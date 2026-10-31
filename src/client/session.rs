//! [`Client`]: one agent's signed, end-to-end encrypted rooms.
//!
//! Each secure room is an MLS group (RFC 9420, ciphersuite
//! `MLS_128_DHKEMX25519_AES128GCM_SHA256_Ed25519`) whose members' signature
//! keys are their mentischat Ed25519 identities. The daemon relays opaque
//! bytes in the room's log:
//!
//! ```text
//! mls/1 commit <epoch> <hex>   a Commit made in <epoch>; one per epoch
//! mls/1 app    <epoch> <hex>   an application message
//! ```
//!
//! and hands each invitee its Welcome out of band (`welcome` op). Every
//! message is also an ordinary signed send, so the daemon can refuse
//! non-members and replays without understanding any of it.
//!
//! Reads go through the room's retained history, not `check_inbox`: history
//! does not consume anything, so a client that dies mid-sync repeats the sync
//! instead of losing a Commit and falling out of the group.

use std::collections::{BTreeMap, HashMap};

use openmls::prelude::tls_codec::{Deserialize as TlsDeserialize, Serialize as TlsSerialize};
use openmls::prelude::*;
use openmls::treesync::LeafNodeParameters;
use openmls_basic_credential::SignatureKeyPair;
use openmls_rust_crypto::OpenMlsRustCrypto;
use openmls_traits::types::SignatureScheme;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use super::http::query_escape;
use super::identity::write_private;
use super::{ClientError, Daemon, Identity};
use crate::signed::{
    hex_decode_vec, hex_encode, key_packages_payload, register_key_payload, PublicKey, SignedOp,
};

/// The one ciphersuite: X25519 HPKE, AES-128-GCM, SHA-256, Ed25519.
const SUITE: Ciphersuite = Ciphersuite::MLS_128_DHKEMX25519_AES128GCM_SHA256_Ed25519;
/// Past epochs a member can still decrypt from, for an application message
/// that raced a Commit in the log.
const PAST_EPOCHS: usize = 3;
/// Plaintext messages kept per room for `history`.
const KEPT_MESSAGES: usize = 1024;
/// How many times a Commit that lost the epoch race is re-made.
const COMMIT_RETRIES: usize = 4;
/// KeyPackages the client keeps on offer.
pub const KEY_PACKAGE_STOCK: usize = 8;

/// When the client refreshes its own key in a room with an MLS self-update
/// (an empty Commit that replaces the member's leaf key and ratchets the group
/// secret), which is what restores confidentiality after a compromise of its
/// state: whoever stole an old key can read nothing sent after the next update.
///
/// A room is due when the client has sent `messages` messages, or `interval`
/// has passed, since its last update, and right after it joins (a joining
/// member may have been added with a reused last-resort KeyPackage). A zero
/// value disables that trigger.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
/// use mentischat::client::UpdatePolicy;
///
/// let policy = UpdatePolicy::default();
/// assert_eq!(policy.messages, 100);
/// assert_eq!(policy.interval, Duration::from_secs(30 * 60));
/// assert!(!UpdatePolicy { messages: 0, interval: Duration::ZERO }.enabled());
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UpdatePolicy {
    /// Update after this many sent messages; 0 for never by count.
    pub messages: u64,
    /// Update after this long; zero for never by time.
    pub interval: std::time::Duration,
}

impl Default for UpdatePolicy {
    /// Every 100 messages or 30 minutes.
    fn default() -> Self {
        Self {
            messages: 100,
            interval: std::time::Duration::from_secs(30 * 60),
        }
    }
}

impl UpdatePolicy {
    /// Whether either trigger is on.
    pub fn enabled(&self) -> bool {
        self.messages > 0 || !self.interval.is_zero()
    }
}

/// A message the client decrypted, as the harness sees it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Received {
    /// The daemon's message id.
    pub id: u64,
    /// The sender, as authenticated by its MLS credential.
    pub from: String,
    /// The room.
    pub room: String,
    /// The plaintext.
    pub body: String,
    /// UTC RFC 3339 time the daemon accepted it.
    pub created_at: String,
}

/// What the client keeps about one room besides the MLS group.
#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(default)]
struct RoomLog {
    /// A Commit this client needs fell out of retention: it must be re-added.
    behind: bool,
    /// The daemon showed an older room than this client had seen; the room is
    /// not used until the user accepts it.
    rolled_back: bool,
    /// Highest MLS epoch this client has held in this room.
    max_epoch: u64,
    /// Highest counter the daemon accepted from this client in this room.
    counter_floor: u64,
    /// A fresh self-update is due as soon as possible (just joined).
    update_due: bool,
    /// Messages this client has sent since its last self-update.
    sent_since_update: u64,
    /// Unix milliseconds of the last self-update, or of joining; 0 = unset.
    last_update_ms: u64,
    /// Unix milliseconds a re-add was last requested, to throttle requests.
    readd_requested_ms: u64,
    /// Highest daemon message id already processed.
    last_id: u64,
    /// This client created the room (and so may remove members).
    owner: bool,
    /// A Commit removed this client, or it left: the group is dead.
    ended: bool,
    /// Decrypted messages, oldest first, with whether the harness has them.
    entries: Vec<Entry>,
}

#[derive(Debug, Serialize, Deserialize)]
struct Entry {
    message: Received,
    delivered: bool,
}

/// The persisted state: OpenMLS's key-value storage and the room logs.
#[derive(Debug, Default, Serialize, Deserialize)]
struct Saved {
    storage: Vec<(String, String)>,
    rooms: BTreeMap<String, RoomLog>,
}

/// One agent's keys, MLS groups and view of the daemon.
pub struct Client {
    identity: Identity,
    daemon: Daemon,
    provider: OpenMlsRustCrypto,
    signer: SignatureKeyPair,
    credential: CredentialWithKey,
    rooms: BTreeMap<String, RoomLog>,
    last_counter: u64,
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Client({:?})", self.identity)
    }
}

fn mls<E: std::fmt::Display>(what: &'static str) -> impl FnOnce(E) -> ClientError {
    move |e| ClientError::Mls(format!("{what}: {e}"))
}

fn folded(room: &str) -> String {
    room.to_ascii_lowercase()
}

fn micros_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_micros() as u64)
}

impl Client {
    /// Open `identity`'s client against `daemon`, restoring its MLS state from
    /// `state.json` if there is one.
    ///
    /// # Errors
    ///
    /// [`ClientError::Storage`] when the state file cannot be read or parsed.
    pub fn open(identity: Identity, daemon: Daemon) -> Result<Self, ClientError> {
        let provider = OpenMlsRustCrypto::default();
        let path = identity.dir().join("state.json");
        let saved: Saved = match std::fs::read(&path) {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .map_err(|e| ClientError::Storage(format!("{}: {e}", path.display())))?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Saved::default(),
            Err(e) => return Err(ClientError::Storage(format!("{}: {e}", path.display()))),
        };
        {
            let mut values = provider
                .storage()
                .values
                .write()
                .map_err(|_| ClientError::Storage("storage lock poisoned".into()))?;
            for (k, v) in saved.storage {
                let (Ok(k), Ok(v)) = (hex_decode_vec(&k), hex_decode_vec(&v)) else {
                    return Err(ClientError::Storage(format!("{}: bad hex", path.display())));
                };
                values.insert(k, v);
            }
        }
        let public = identity.public();
        let signer = SignatureKeyPair::from_raw(
            SignatureScheme::ED25519,
            identity.seed().to_vec(),
            public.as_bytes().to_vec(),
        );
        let credential = CredentialWithKey {
            credential: BasicCredential::new(identity.agent().as_bytes().to_vec()).into(),
            signature_key: public.as_bytes().to_vec().into(),
        };
        Ok(Self {
            identity,
            daemon,
            provider,
            signer,
            credential,
            rooms: saved.rooms,
            last_counter: micros_now(),
        })
    }

    /// The agent name.
    pub fn agent(&self) -> &str {
        self.identity.agent()
    }

    /// The agent's public key.
    pub fn public_key(&self) -> PublicKey {
        self.identity.public()
    }

    /// Whether this client created `room` (and so may remove members and is
    /// the one asked to re-add members that fell behind).
    pub fn owns(&self, room: &str) -> bool {
        self.rooms.get(&folded(room)).is_some_and(|r| r.owner)
    }

    /// True at most once per `every` for `room`: whether a re-add request
    /// should be sent now. Records the request time.
    pub fn readd_due(&mut self, room: &str, every: std::time::Duration) -> bool {
        let now = micros_now() / 1000;
        let Some(log) = self.rooms.get_mut(&folded(room)) else {
            return false;
        };
        if now.saturating_sub(log.readd_requested_ms) < every.as_millis() as u64 {
            return false;
        }
        log.readd_requested_ms = now;
        self.persist().is_ok()
    }

    /// The name of the member whose key is the room's owner key, as the daemon
    /// lists it; `None` when the owner has left.
    ///
    /// # Errors
    ///
    /// [`ClientError::Daemon`].
    pub async fn owner_name(&self, room: &str) -> Result<Option<String>, ClientError> {
        let info = self.room_info(room).await?;
        let Some(owner) = info["owner"].as_str() else {
            return Ok(None);
        };
        Ok(info["members"]
            .as_array()
            .into_iter()
            .flatten()
            .find(|m| m["publicKey"] == owner)
            .and_then(|m| m["agent"].as_str().map(str::to_string)))
    }

    /// Whether `room` is one of this client's secure rooms.
    pub fn is_secure(&self, room: &str) -> bool {
        self.rooms.contains_key(&folded(room))
    }

    /// The names of the secure rooms this client has state for.
    pub fn secure_rooms(&self) -> Vec<String> {
        self.rooms.keys().cloned().collect()
    }

    fn persist(&self) -> Result<(), ClientError> {
        let values = self
            .provider
            .storage()
            .values
            .read()
            .map_err(|_| ClientError::Storage("storage lock poisoned".into()))?;
        let saved = serde_json::json!({
            "storage": values.iter().map(|(k, v)| (hex_encode(k), hex_encode(v))).collect::<Vec<_>>(),
            "rooms": self.rooms,
        });
        write_private(
            &self.identity.dir().join("state.json"),
            saved.to_string().as_bytes(),
        )
    }

    /// A counter above every one this client has used: microseconds since the
    /// epoch, forced strictly increasing, so it needs no state of its own.
    fn next_counter(&mut self) -> u64 {
        self.last_counter = micros_now().max(self.last_counter + 1);
        self.last_counter
    }

    // ---- daemon plumbing ----

    /// Register the name on the roster. `reclaim` takes it back if held.
    ///
    /// # Errors
    ///
    /// [`ClientError::Daemon`], e.g. `name_taken`.
    pub async fn register(&self, reclaim: bool) -> Result<Value, ClientError> {
        let body = json!({ "agent": self.agent(), "reclaim": reclaim });
        self.daemon.call("POST", "/agents", Some(&body)).await
    }

    /// Bind this identity's key to the name and make sure
    /// [`KEY_PACKAGE_STOCK`] KeyPackages are on offer. Idempotent.
    ///
    /// # Errors
    ///
    /// [`ClientError::Daemon`] (`key_conflict` when the name is bound to
    /// another key), or an MLS failure building KeyPackages.
    pub async fn enroll(&mut self) -> Result<(), ClientError> {
        let key = self.identity.public();
        let sig = self
            .identity
            .keypair()
            .sign(&register_key_payload(self.agent(), &key));
        let body = json!({ "public_key": key.to_hex(), "signature": sig.to_hex() });
        let path = format!("/agents/{}/key", self.agent());
        self.daemon.call("PUT", &path, Some(&body)).await?;
        self.publish_key_packages(KEY_PACKAGE_STOCK).await
    }

    /// Generate and publish `n` fresh KeyPackages.
    ///
    /// # Errors
    ///
    /// An MLS failure, or [`ClientError::Daemon`].
    pub async fn publish_key_packages(&mut self, n: usize) -> Result<(), ClientError> {
        let packages = (0..n)
            .map(|_| {
                let bundle = KeyPackage::builder()
                    .build(SUITE, &self.provider, &self.signer, self.credential.clone())
                    .map_err(mls("key package"))?;
                bundle
                    .key_package()
                    .tls_serialize_detached()
                    .map_err(mls("serialize key package"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let last_resort = KeyPackage::builder()
            .mark_as_last_resort()
            .leaf_node_capabilities(Capabilities::new(
                None,
                None,
                Some(&[ExtensionType::LastResort]),
                None,
                None,
            ))
            .build(SUITE, &self.provider, &self.signer, self.credential.clone())
            .map_err(mls("last-resort key package"))?
            .key_package()
            .tls_serialize_detached()
            .map_err(mls("serialize key package"))?;
        let counter = self.next_counter();
        let sig = self.identity.keypair().sign(&key_packages_payload(
            self.agent(),
            counter,
            &packages,
            Some(&last_resort),
        ));
        let body = json!({
            "counter": counter,
            "key_packages": packages.iter().map(|p| hex_encode(p)).collect::<Vec<_>>(),
            "last_resort": hex_encode(&last_resort),
            "signature": sig.to_hex(),
        });
        let path = format!("/agents/{}/keypackages", self.agent());
        self.daemon.call("PUT", &path, Some(&body)).await?;
        self.persist()
    }

    /// Sign and submit one operation, retrying once with a higher counter if
    /// the daemon says this client's counter fell behind.
    async fn post_op(&mut self, room: &str, op: SignedOp) -> Result<Value, ClientError> {
        for attempt in 0..2 {
            let counter = self.next_counter();
            let payload = op.payload(room, self.agent(), counter);
            let sig = self.identity.keypair().sign(&payload);
            let mut body = json!({
                "op": op.kind().as_str(), "agent": self.agent(), "room": room,
                "counter": counter, "signature": sig.to_hex(),
            });
            match &op {
                SignedOp::Create { description } => body["description"] = json!(description),
                SignedOp::Invite {
                    invitee,
                    invitee_key,
                } => {
                    body["invitee"] = json!(invitee);
                    body["invitee_key"] = json!(invitee_key.to_hex());
                }
                SignedOp::Remove { target } => body["target"] = json!(target),
                SignedOp::Send { body: text } => body["body"] = json!(text),
                SignedOp::Welcome { target, welcome } => {
                    body["target"] = json!(target);
                    body["welcome"] = json!(hex_encode(welcome));
                }
                SignedOp::Join | SignedOp::Leave => {}
            }
            match self.daemon.call("POST", "/signed/ops", Some(&body)).await {
                Err(ClientError::Daemon { code, body, .. })
                    if code == "stale_counter" && attempt == 0 =>
                {
                    let last = body.get("last").and_then(Value::as_u64).unwrap_or(0);
                    self.last_counter = self.last_counter.max(last);
                }
                other => {
                    if other.is_ok() {
                        if let Some(log) = self.rooms.get_mut(&folded(room)) {
                            log.counter_floor = log.counter_floor.max(counter);
                        }
                    }
                    return other;
                }
            }
        }
        Err(ClientError::Usage("counter retry exhausted".into()))
    }

    async fn room_info(&self, room: &str) -> Result<Value, ClientError> {
        let path = format!(
            "/signed/rooms/{}?agent={}",
            query_escape(room),
            query_escape(self.agent())
        );
        self.daemon.call("GET", &path, None).await
    }

    // ---- groups ----

    fn group(&self, room: &str) -> Result<MlsGroup, ClientError> {
        let id = GroupId::from_slice(folded(room).as_bytes());
        MlsGroup::load(self.provider.storage(), &id)
            .map_err(mls("load group"))?
            .ok_or_else(|| ClientError::Usage(format!("no secure room {room} on this client")))
    }

    fn live_group(&self, room: &str) -> Result<MlsGroup, ClientError> {
        if self.rooms.get(&folded(room)).is_some_and(|l| l.rolled_back) {
            return Err(self.rolled_back_error(room, "flagged earlier"));
        }
        let group = self.group(room)?;
        if group.is_active() {
            Ok(group)
        } else {
            Err(ClientError::Usage(format!(
                "this client is no longer in secure room {room}"
            )))
        }
    }

    /// Create the secure room `room` (signed, private, end-to-end encrypted)
    /// with this client as its first member and owner.
    ///
    /// # Errors
    ///
    /// [`ClientError::Daemon`] (`room_exists`, `key_required`), or an MLS failure.
    pub async fn create_room(&mut self, room: &str, description: &str) -> Result<(), ClientError> {
        let op = SignedOp::Create {
            description: description.to_string(),
        };
        self.post_op(room, op).await?;
        let config = MlsGroupCreateConfig::builder()
            .ciphersuite(SUITE)
            .use_ratchet_tree_extension(true)
            .max_past_epochs(PAST_EPOCHS)
            .build();
        MlsGroup::new_with_group_id(
            &self.provider,
            &self.signer,
            &config,
            GroupId::from_slice(folded(room).as_bytes()),
            self.credential.clone(),
        )
        .map_err(mls("create group"))?;
        self.rooms.insert(
            folded(room),
            RoomLog {
                owner: true,
                ..RoomLog::default()
            },
        );
        self.persist()
    }

    /// Add `target` to the room: invite it, fetch one of its KeyPackages,
    /// check it is really `target`'s key, commit, and leave it a Welcome. The
    /// target then calls [`join_room`](Self::join_room).
    ///
    /// # Errors
    ///
    /// [`ClientError::Daemon`] (`no_key_package`, `key_required`, `stale_epoch`
    /// after several races), [`ClientError::Mls`], or
    /// [`ClientError::Usage`] when the KeyPackage's identity or key is not the
    /// target's registered one.
    pub async fn add_member(&mut self, room: &str, target: &str) -> Result<(), ClientError> {
        self.live_group(room)?;
        let target_key = self.registered_key(target).await?;
        let invite = SignedOp::Invite {
            invitee: target.to_string(),
            invitee_key: target_key,
        };
        self.post_op(room, invite).await?;
        self.commit_add(room, target, &target_key).await
    }

    /// Take a KeyPackage for `target`, Commit its addition and leave it the
    /// Welcome. The daemon must already list `target` as invited or as a member.
    async fn commit_add(
        &mut self,
        room: &str,
        target: &str,
        target_key: &PublicKey,
    ) -> Result<(), ClientError> {
        let package = self.take_key_package(target, target_key).await?;
        let mut welcome = None;
        for _ in 0..COMMIT_RETRIES {
            self.sync(room).await?;
            let mut group = self.live_group(room)?;
            let (commit, w, _) = group
                .add_members(&self.provider, &self.signer, std::slice::from_ref(&package))
                .map_err(mls("add member"))?;
            if self.send_commit(room, &mut group, commit).await? {
                welcome = Some(w.to_bytes().map_err(mls("serialize welcome"))?);
                break;
            }
        }
        let welcome = welcome.ok_or_else(|| ClientError::Daemon {
            status: 409,
            code: "stale_epoch".into(),
            body: json!({ "error": "stale_epoch" }),
        })?;
        let op = SignedOp::Welcome {
            target: target.to_string(),
            welcome,
        };
        self.post_op(room, op).await?;
        self.persist()
    }

    /// Add a member that is already in the room back into its MLS group: the
    /// recovery for a client that fell behind (see [`ClientError::Behind`]).
    /// Commits the removal of the member's old leaf if it has one, then adds it
    /// again from a fresh KeyPackage and leaves it a Welcome, which it picks up
    /// with [`join_room`](Self::join_room). Any member may do it; the proxy has
    /// the owner do it on request.
    ///
    /// # Errors
    ///
    /// [`ClientError::Usage`] when `target` is not a member of the daemon's room;
    /// otherwise the errors of [`add_member`](Self::add_member).
    pub async fn readd_member(&mut self, room: &str, target: &str) -> Result<(), ClientError> {
        self.live_group(room)?;
        let info = self.room_info(room).await?;
        let listed = info["members"].as_array().into_iter().flatten().any(|m| {
            m["agent"]
                .as_str()
                .is_some_and(|n| n.eq_ignore_ascii_case(target))
        });
        if !listed {
            return Err(ClientError::Usage(format!(
                "{target} is not a member of {room}"
            )));
        }
        let target_key = self.registered_key(target).await?;
        self.commit_removal(room, target).await?;
        self.commit_add(room, target, &target_key).await
    }

    /// Refresh this client's own key in the group with an MLS self-update, an
    /// empty Commit: after it, a key stolen before cannot decrypt anything sent
    /// later (post-compromise security).
    ///
    /// # Errors
    ///
    /// The MLS and daemon errors of [`add_member`](Self::add_member).
    pub async fn self_update(&mut self, room: &str) -> Result<(), ClientError> {
        for _ in 0..COMMIT_RETRIES {
            self.sync(room).await?;
            let mut group = self.live_group(room)?;
            let bundle = group
                .self_update(&self.provider, &self.signer, LeafNodeParameters::default())
                .map_err(mls("self update"))?;
            let (commit, _, _) = bundle.into_contents();
            if self.send_commit(room, &mut group, commit).await? {
                if let Some(log) = self.rooms.get_mut(&folded(room)) {
                    log.sent_since_update = 0;
                    log.update_due = false;
                    log.last_update_ms = micros_now() / 1000;
                }
                return self.persist();
            }
        }
        Err(ClientError::Daemon {
            status: 409,
            code: "stale_epoch".into(),
            body: json!({ "error": "stale_epoch" }),
        })
    }

    /// Run [`self_update`](Self::self_update) if `policy` says the room is due.
    /// Returns whether it did.
    ///
    /// # Errors
    ///
    /// As [`self_update`](Self::self_update); nothing when not due.
    pub async fn maybe_self_update(
        &mut self,
        room: &str,
        policy: &UpdatePolicy,
    ) -> Result<bool, ClientError> {
        let now_ms = micros_now() / 1000;
        let Some(log) = self.rooms.get_mut(&folded(room)) else {
            return Ok(false);
        };
        if log.ended || log.behind || !policy.enabled() {
            return Ok(false);
        }
        if log.last_update_ms == 0 {
            log.last_update_ms = now_ms;
        }
        let by_count = policy.messages > 0 && log.sent_since_update >= policy.messages;
        let by_time = !policy.interval.is_zero()
            && now_ms.saturating_sub(log.last_update_ms) >= policy.interval.as_millis() as u64;
        if !(log.update_due || by_count || by_time) {
            return Ok(false);
        }
        self.self_update(room).await?;
        Ok(true)
    }

    /// Send `commit` for `group` as a signed Commit message. On success the
    /// pending Commit is merged and `true` returned; if another member's Commit
    /// won the epoch (`stale_epoch`) the pending Commit is discarded and `false`
    /// returned, for the caller to re-sync and try again.
    async fn send_commit(
        &mut self,
        room: &str,
        group: &mut MlsGroup,
        commit: MlsMessageOut,
    ) -> Result<bool, ClientError> {
        let epoch = group.epoch().as_u64();
        let bytes = commit.to_bytes().map_err(mls("serialize commit"))?;
        let body = format!("mls/1 commit {epoch} {}", hex_encode(&bytes));
        match self.post_op(room, SignedOp::Send { body }).await {
            Ok(_) => {
                group
                    .merge_pending_commit(&self.provider)
                    .map_err(mls("merge own commit"))?;
                Ok(true)
            }
            Err(ClientError::Daemon { code, .. }) if code == "stale_epoch" => {
                group
                    .clear_pending_commit(self.provider.storage())
                    .map_err(mls("discard commit"))?;
                Ok(false)
            }
            Err(e) => {
                group
                    .clear_pending_commit(self.provider.storage())
                    .map_err(mls("discard commit"))?;
                Err(e)
            }
        }
    }

    async fn registered_key(&self, agent: &str) -> Result<PublicKey, ClientError> {
        let v = self
            .daemon
            .call("GET", &format!("/agents/{}/key", query_escape(agent)), None)
            .await?;
        v.get("publicKey")
            .and_then(Value::as_str)
            .and_then(|k| k.parse().ok())
            .ok_or_else(|| ClientError::Usage(format!("{agent} has no key bound")))
    }

    /// Take one of `agent`'s KeyPackages and check that its credential names
    /// `agent` and its signature key is `expected`, the key the daemon has
    /// bound to that name.
    async fn take_key_package(
        &self,
        agent: &str,
        expected: &PublicKey,
    ) -> Result<KeyPackage, ClientError> {
        let path = format!("/agents/{}/keypackages/take", query_escape(agent));
        let body = json!({ "requester": self.agent() });
        let v = self.daemon.call("POST", &path, Some(&body)).await?;
        let bytes = v
            .get("keyPackage")
            .and_then(Value::as_str)
            .and_then(|h| hex_decode_vec(h).ok())
            .ok_or_else(|| ClientError::Usage("daemon sent no key package".into()))?;
        let package = KeyPackageIn::tls_deserialize_exact(bytes)
            .map_err(mls("parse key package"))?
            .validate(self.provider.crypto(), ProtocolVersion::Mls10)
            .map_err(mls("validate key package"))?;
        let leaf = package.leaf_node();
        let named = leaf.credential().serialized_content();
        if !named.eq_ignore_ascii_case(agent.as_bytes())
            || leaf.signature_key().as_slice() != expected.as_bytes()
        {
            return Err(ClientError::Usage(format!(
                "key package is not {agent}'s registered identity"
            )));
        }
        Ok(package)
    }

    /// Join a room this client was invited to: fetch its Welcome, join the MLS
    /// group, and tell the daemon with a signed `join`.
    ///
    /// # Errors
    ///
    /// [`ClientError::Daemon`] (`room_not_found` when not invited),
    /// [`ClientError::Usage`] when no Welcome is waiting yet, or an MLS failure.
    pub async fn join_room(&mut self, room: &str) -> Result<(), ClientError> {
        let path = format!(
            "/signed/rooms/{}/welcome?agent={}",
            query_escape(room),
            query_escape(self.agent())
        );
        let v = self.daemon.call("GET", &path, None).await?;
        let bytes = v
            .get("welcome")
            .and_then(Value::as_str)
            .and_then(|h| hex_decode_vec(h).ok())
            .ok_or_else(|| ClientError::Usage(format!("no welcome waiting in {room} yet")))?;
        let welcome = match MlsMessageIn::tls_deserialize_exact(bytes)
            .map_err(mls("parse welcome"))?
            .extract()
        {
            MlsMessageBodyIn::Welcome(welcome) => welcome,
            _ => return Err(ClientError::Mls("not a welcome".into())),
        };
        // Rejoining a room this client already has a group for (it fell behind
        // or was removed) replaces that group with the one in the Welcome.
        if let Some(log) = self.rooms.get(&folded(room)) {
            if !(log.behind || log.ended) {
                return Err(ClientError::Usage(format!(
                    "already in secure room {room}; nothing to rejoin"
                )));
            }
            if let Ok(mut old) = self.group(room) {
                old.delete(self.provider.storage())
                    .map_err(mls("drop old group"))?;
            }
        }
        let config = MlsGroupJoinConfig::builder()
            .use_ratchet_tree_extension(true)
            .max_past_epochs(PAST_EPOCHS)
            .build();
        StagedWelcome::new_from_welcome(&self.provider, &config, welcome, None)
            .map_err(mls("stage welcome"))?
            .into_group(&self.provider)
            .map_err(mls("join group"))?;
        let joined_epoch = self.group(room)?.epoch().as_u64();
        let log = self.rooms.entry(folded(room)).or_default();
        if joined_epoch < log.max_epoch {
            // A Welcome older than a group this client already held: the daemon
            // is serving a rolled-back room.
            log.rolled_back = true;
            let detail = format!(
                "welcome is for epoch {joined_epoch}, this client held {}",
                log.max_epoch
            );
            self.persist()?;
            return Err(self.rolled_back_error(room, &detail));
        }
        log.max_epoch = joined_epoch;
        log.ended = false;
        log.behind = false;
        log.update_due = true;
        self.persist()?;
        self.post_op(room, SignedOp::Join).await?;
        Ok(())
    }

    /// Remove `target` from the room (owner only): commit its removal from the
    /// MLS group first, so it cannot read what follows, then remove it from the
    /// daemon's membership.
    ///
    /// # Errors
    ///
    /// [`ClientError::Usage`] when this client is not the owner or `target` is
    /// not in the group; the daemon and MLS errors of [`add_member`](Self::add_member).
    pub async fn remove_member(&mut self, room: &str, target: &str) -> Result<(), ClientError> {
        if !self.rooms.get(&folded(room)).is_some_and(|r| r.owner) {
            return Err(ClientError::Usage(
                "only the room's owner removes members".into(),
            ));
        }
        self.commit_removal(room, target).await?;
        let op = SignedOp::Remove {
            target: target.to_string(),
        };
        self.post_op(room, op).await?;
        Ok(())
    }

    /// Commit `target`'s removal from the MLS group only, leaving the daemon's
    /// membership alone ([`remove_member`](Self::remove_member) does both, in
    /// that order). A target not in the group is already done.
    ///
    /// # Errors
    ///
    /// The MLS and daemon errors of [`add_member`](Self::add_member).
    pub async fn commit_removal(&mut self, room: &str, target: &str) -> Result<(), ClientError> {
        for _ in 0..COMMIT_RETRIES {
            self.sync(room).await?;
            let mut group = self.live_group(room)?;
            let Some(index) = group
                .members()
                .find(|m| {
                    m.credential
                        .serialized_content()
                        .eq_ignore_ascii_case(target.as_bytes())
                })
                .map(|m| m.index)
            else {
                return Ok(());
            };
            let (commit, _, _) = group
                .remove_members(&self.provider, &self.signer, &[index])
                .map_err(mls("remove member"))?;
            if self.send_commit(room, &mut group, commit).await? {
                return self.persist();
            }
        }
        Err(ClientError::Daemon {
            status: 409,
            code: "stale_epoch".into(),
            body: json!({ "error": "stale_epoch" }),
        })
    }

    /// Owner only: remove from the MLS group every member the daemon no longer
    /// lists (members that left or were dropped), so they lose access to what
    /// follows. Invitees that have not joined yet are kept.
    ///
    /// # Errors
    ///
    /// As [`remove_member`](Self::remove_member).
    pub async fn prune(&mut self, room: &str) -> Result<Vec<String>, ClientError> {
        if !self.rooms.get(&folded(room)).is_some_and(|r| r.owner) {
            return Ok(Vec::new());
        }
        self.sync(room).await?;
        let info = self.room_info(room).await?;
        let listed: Vec<String> = ["members", "invited"]
            .iter()
            .flat_map(|k| info[*k].as_array().into_iter().flatten())
            .filter_map(|m| m.get("agent").or(Some(m)).and_then(Value::as_str))
            .map(str::to_ascii_lowercase)
            .collect();
        let group = self.live_group(room)?;
        let stale: Vec<String> = group
            .members()
            .filter_map(|m| String::from_utf8(m.credential.serialized_content().to_vec()).ok())
            .filter(|n| {
                !n.eq_ignore_ascii_case(self.agent()) && !listed.contains(&n.to_ascii_lowercase())
            })
            .collect();
        for name in &stale {
            self.commit_removal(room, name).await?;
        }
        Ok(stale)
    }

    /// Leave the room: a signed `leave` at the daemon and the local group is
    /// closed. The owner's next [`prune`](Self::prune) takes the departed
    /// leaf out of the MLS group.
    ///
    /// # Errors
    ///
    /// [`ClientError::Daemon`].
    pub async fn leave_room(&mut self, room: &str) -> Result<(), ClientError> {
        self.group(room)?;
        self.post_op(room, SignedOp::Leave).await?;
        if let Some(log) = self.rooms.get_mut(&folded(room)) {
            log.ended = true;
        }
        self.persist()
    }

    /// Encrypt `text` for the room and send it as a signed message.
    ///
    /// # Errors
    ///
    /// [`ClientError::Usage`] when the group is gone, an MLS failure, or a
    /// [`ClientError::Daemon`] rejection.
    pub async fn send(&mut self, room: &str, text: &str) -> Result<Received, ClientError> {
        let mut group = self.live_group(room)?;
        let out = group
            .create_message(&self.provider, &self.signer, text.as_bytes())
            .map_err(mls("encrypt"))?;
        let bytes = out.to_bytes().map_err(mls("serialize message"))?;
        let body = format!(
            "mls/1 app {} {}",
            group.epoch().as_u64(),
            hex_encode(&bytes)
        );
        let receipt = self.post_op(room, SignedOp::Send { body }).await?;
        let sent = Received {
            id: receipt["id"]
                .as_str()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0),
            from: self.agent().to_string(),
            room: room.to_string(),
            body: text.to_string(),
            created_at: receipt["createdAt"].as_str().unwrap_or("").to_string(),
        };
        self.log(room, sent.clone(), true);
        if let Some(log) = self.rooms.get_mut(&folded(room)) {
            log.sent_since_update += 1;
        }
        self.persist()?;
        Ok(sent)
    }

    fn log(&mut self, room: &str, message: Received, delivered: bool) {
        if let Some(log) = self.rooms.get_mut(&folded(room)) {
            log.entries.push(Entry { message, delivered });
            let excess = log.entries.len().saturating_sub(KEPT_MESSAGES);
            log.entries.drain(..excess);
        }
    }

    /// The room's decrypted messages the harness has not seen, oldest first,
    /// now marked seen.
    pub fn take_unread(&mut self, room: &str) -> Result<Vec<Received>, ClientError> {
        let Some(log) = self.rooms.get_mut(&folded(room)) else {
            return Ok(Vec::new());
        };
        let fresh: Vec<Received> = log
            .entries
            .iter_mut()
            .filter(|e| !e.delivered)
            .map(|e| {
                e.delivered = true;
                e.message.clone()
            })
            .collect();
        if !fresh.is_empty() {
            self.persist()?;
        }
        Ok(fresh)
    }

    /// Every decrypted message the client still holds for the room, oldest
    /// first (the daemon's ciphertext cannot be read twice, so this is the
    /// room's history).
    pub fn history(&self, room: &str) -> Vec<Received> {
        self.rooms
            .get(&folded(room))
            .map(|l| l.entries.iter().map(|e| e.message.clone()).collect())
            .unwrap_or_default()
    }

    /// Fetch what the room's retained history holds past what this client has
    /// processed and apply it in order: Commits advance the group, application
    /// messages are decrypted into the room's log. Returns how many messages
    /// were decrypted.
    ///
    /// # Errors
    ///
    /// [`ClientError::Mls`] when a Commit this client needs is missing (it fell
    /// behind past the daemon's retention and must be re-added),
    /// [`ClientError::Daemon`].
    pub async fn sync(&mut self, room: &str) -> Result<usize, ClientError> {
        let log = self
            .rooms
            .get(&folded(room))
            .ok_or_else(|| ClientError::Usage(format!("no secure room {room} on this client")))?;
        if log.ended {
            return Ok(0);
        }
        if log.behind {
            return Err(ClientError::Behind {
                room: room.to_string(),
            });
        }
        self.check_rollback(room).await?;
        let log = self
            .rooms
            .get(&folded(room))
            .ok_or_else(|| ClientError::Usage(format!("no secure room {room} on this client")))?;
        let mut pending = self.fetch_since(room, log.last_id).await?;
        pending.sort_by_key(|m| m.id);
        let mut decrypted = 0;
        for message in pending {
            match self.apply(room, &message).await {
                Ok(read) => decrypted += usize::from(read),
                Err(e @ ClientError::Behind { .. }) => {
                    if let Some(log) = self.rooms.get_mut(&folded(room)) {
                        log.behind = true;
                    }
                    self.persist()?;
                    return Err(e);
                }
                Err(e) => return Err(e),
            }
            if let Some(log) = self.rooms.get_mut(&folded(room)) {
                log.last_id = message.id;
            }
        }
        self.persist()?;
        Ok(decrypted)
    }

    /// Compare the daemon's account of the room with what this client has seen,
    /// and stop using the room if the daemon's is older: its epoch is below the
    /// group's (Commits this client already applied are gone), or it has
    /// accepted fewer of this client's operations than this client got
    /// accepted. A daemon restored from an older snapshot, or with a truncated
    /// journal, shows exactly this. This cannot see an omission that leaves both
    /// numbers intact, such as a dropped `leave` of someone else.
    ///
    /// # Errors
    ///
    /// [`ClientError::RolledBack`] (and it keeps returning it until
    /// [`accept_rollback`](Self::accept_rollback)), [`ClientError::Daemon`].
    pub async fn check_rollback(&mut self, room: &str) -> Result<(), ClientError> {
        let key = folded(room);
        if self.rooms.get(&key).is_some_and(|l| l.rolled_back) {
            return Err(self.rolled_back_error(room, "flagged earlier"));
        }
        let info = self.room_info(room).await?;
        let group_epoch = self.group(room)?.epoch().as_u64();
        let daemon_epoch = info["epoch"].as_u64().unwrap_or(0);
        let theirs = info["yourCounter"].as_u64();
        let log = self
            .rooms
            .get_mut(&key)
            .ok_or_else(|| ClientError::Usage(format!("no secure room {room} on this client")))?;
        log.max_epoch = log.max_epoch.max(group_epoch);
        let detail = if daemon_epoch < log.max_epoch {
            Some(format!(
                "daemon is at epoch {daemon_epoch}, this client has held epoch {}",
                log.max_epoch
            ))
        } else if log.counter_floor > 0 && theirs.is_none_or(|c| c < log.counter_floor) {
            Some(format!(
                "daemon has accepted up to counter {}, this client got {} accepted",
                theirs.unwrap_or(0),
                log.counter_floor
            ))
        } else {
            None
        };
        if let Some(detail) = detail {
            log.rolled_back = true;
            self.persist()?;
            return Err(self.rolled_back_error(room, &detail));
        }
        self.persist()
    }

    fn rolled_back_error(&self, room: &str, detail: &str) -> ClientError {
        ClientError::RolledBack {
            room: room.to_string(),
            detail: detail.to_string(),
        }
    }

    /// Resume using a room [`check_rollback`](Self::check_rollback) stopped,
    /// after the user has decided the daemon's older state is acceptable (for
    /// example they restored it on purpose). The client's own high-water marks
    /// are reset to what the daemon now shows. If the client's group is ahead
    /// of the daemon's room it can no longer follow it and is marked as having
    /// fallen behind, to be re-added (see [`ClientError::Behind`]).
    ///
    /// # Errors
    ///
    /// [`ClientError::Usage`] for an unknown room, [`ClientError::Daemon`].
    pub async fn accept_rollback(&mut self, room: &str) -> Result<(), ClientError> {
        let info = self.room_info(room).await?;
        let group_epoch = self.group(room)?.epoch().as_u64();
        let log = self
            .rooms
            .get_mut(&folded(room))
            .ok_or_else(|| ClientError::Usage(format!("no secure room {room} on this client")))?;
        log.rolled_back = false;
        log.max_epoch = info["epoch"].as_u64().unwrap_or(0);
        // A group ahead of the daemon's room cannot commit or follow it any
        // more: it needs a re-add, like a client that fell behind.
        log.behind = group_epoch > log.max_epoch;
        log.counter_floor = info["yourCounter"].as_u64().unwrap_or(0);
        self.persist()
    }

    /// The room's retained messages with an id above `after`, from newest page
    /// back to the first one at or below it.
    async fn fetch_since(&self, room: &str, after: u64) -> Result<Vec<Raw>, ClientError> {
        let mut out = Vec::new();
        let mut before: Option<i64> = None;
        loop {
            let mut path = format!(
                "/history/{}?room={}",
                query_escape(self.agent()),
                query_escape(room)
            );
            if let Some(b) = before {
                path.push_str(&format!("&before={b}"));
            }
            let page = self.daemon.call("GET", &path, None).await?;
            let messages = page["messages"].as_array().cloned().unwrap_or_default();
            let mut reached = false;
            for m in &messages {
                let raw = Raw::from_wire(m);
                if raw.id <= after {
                    reached = true;
                } else {
                    out.push(raw);
                }
            }
            match page["next"].as_i64() {
                Some(next) if !reached => before = Some(next),
                _ => return Ok(out),
            }
        }
    }

    /// Apply one retained message; `true` when it decrypted into a message.
    async fn apply(&mut self, room: &str, message: &Raw) -> Result<bool, ClientError> {
        if message.from.eq_ignore_ascii_case(self.agent()) {
            return Ok(false);
        }
        let Ok(env) = crate::signed::Envelope::parse(&message.body) else {
            return Ok(false);
        };
        let mut parts = env.body.splitn(4, ' ');
        let (Some("mls/1"), Some(kind), Some(epoch), Some(hex)) =
            (parts.next(), parts.next(), parts.next(), parts.next())
        else {
            return Ok(false);
        };
        let (Ok(epoch), Ok(bytes)) = (epoch.parse::<u64>(), hex_decode_vec(hex)) else {
            return Ok(false);
        };
        let mut group = self.live_group(room)?;
        let protocol = match MlsMessageIn::tls_deserialize_exact(bytes)
            .ok()
            .and_then(|m| m.try_into_protocol_message().ok())
        {
            Some(p) => p,
            None => return Ok(false),
        };
        match kind {
            "commit" => {
                self.apply_commit(room, &mut group, protocol, epoch, &message.from)
                    .await
            }
            "app" => self.apply_app(room, &mut group, protocol, message),
            _ => Ok(false),
        }
    }

    async fn apply_commit(
        &mut self,
        room: &str,
        group: &mut MlsGroup,
        protocol: ProtocolMessage,
        epoch: u64,
        from: &str,
    ) -> Result<bool, ClientError> {
        let current = group.epoch().as_u64();
        if epoch < current {
            return Ok(false);
        }
        if epoch > current {
            return Err(ClientError::Behind {
                room: room.to_string(),
            });
        }
        let processed = group
            .process_message(&self.provider, protocol)
            .map_err(mls("process commit"))?;
        check_sender(&processed, from)?;
        let ProcessedMessageContent::StagedCommitMessage(staged) = processed.into_content() else {
            return Ok(false);
        };
        for add in staged.add_proposals() {
            let leaf = add.add_proposal().key_package().leaf_node();
            let name = String::from_utf8_lossy(leaf.credential().serialized_content()).to_string();
            self.check_added_member(&name, leaf.signature_key().as_slice())
                .await?;
        }
        let removed_self = staged.self_removed();
        group
            .merge_staged_commit(&self.provider, *staged)
            .map_err(mls("merge commit"))?;
        if removed_self {
            if let Some(log) = self.rooms.get_mut(&folded(room)) {
                log.ended = true;
            }
        }
        Ok(false)
    }

    /// Refuse to merge a Commit that adds someone whose key is not the one the
    /// daemon has bound to that name: the adder vouches for identities, but
    /// every member checks.
    async fn check_added_member(&self, name: &str, key: &[u8]) -> Result<(), ClientError> {
        let path = format!("/agents/{}/key", query_escape(name));
        let found = self.daemon.call("GET", &path, None).await?;
        let bound = found.get("publicKey").and_then(Value::as_str);
        if bound == Some(hex_encode(key).as_str()) {
            Ok(())
        } else {
            Err(ClientError::Mls(
                "a commit adds a member whose key is not its registered identity".into(),
            ))
        }
    }

    fn apply_app(
        &mut self,
        room: &str,
        group: &mut MlsGroup,
        protocol: ProtocolMessage,
        raw: &Raw,
    ) -> Result<bool, ClientError> {
        let Ok(processed) = group.process_message(&self.provider, protocol) else {
            // Older than this client's epochs, or not for it: unreadable.
            return Ok(false);
        };
        check_sender(&processed, &raw.from)?;
        let ProcessedMessageContent::ApplicationMessage(app) = processed.into_content() else {
            return Ok(false);
        };
        let body = String::from_utf8_lossy(&app.into_bytes()).to_string();
        let received = Received {
            id: raw.id,
            from: raw.from.clone(),
            room: room.to_string(),
            body,
            created_at: raw.created_at.clone(),
        };
        self.log(room, received, false);
        Ok(true)
    }

    /// Try to read one room message body the way [`sync`](Self::sync) would,
    /// without asking the daemon: for a message obtained some other way (a
    /// log, a captured wire body). `Some(plaintext)` when this client's group
    /// can decrypt it, `None` when it cannot (wrong epoch, not for this
    /// group, removed). A message that decrypts is not added to the room's log.
    ///
    /// # Errors
    ///
    /// [`ClientError::Usage`] when there is no live group for `room`, and
    /// [`ClientError::Mls`] when the sender's authenticated identity is not
    /// `from`.
    pub fn try_decrypt(
        &mut self,
        room: &str,
        from: &str,
        wire_body: &str,
    ) -> Result<Option<String>, ClientError> {
        let Ok(env) = crate::signed::Envelope::parse(wire_body) else {
            return Ok(None);
        };
        let mut parts = env.body.splitn(4, ' ');
        let (Some("mls/1"), Some("app"), Some(_), Some(hex)) =
            (parts.next(), parts.next(), parts.next(), parts.next())
        else {
            return Ok(None);
        };
        let Some(protocol) = hex_decode_vec(hex)
            .ok()
            .and_then(|b| MlsMessageIn::tls_deserialize_exact(b).ok())
            .and_then(|m| m.try_into_protocol_message().ok())
        else {
            return Ok(None);
        };
        let mut group = self.live_group(room)?;
        let Ok(processed) = group.process_message(&self.provider, protocol) else {
            return Ok(None);
        };
        check_sender(&processed, from)?;
        Ok(match processed.into_content() {
            ProcessedMessageContent::ApplicationMessage(app) => {
                Some(String::from_utf8_lossy(&app.into_bytes()).to_string())
            }
            _ => None,
        })
    }

    /// Epoch and member names of the room's MLS group, for diagnostics.
    pub fn group_members(&self, room: &str) -> Result<(u64, Vec<String>), ClientError> {
        let group = self.group(room)?;
        let names = group
            .members()
            .map(|m| String::from_utf8_lossy(m.credential.serialized_content()).to_string())
            .collect();
        Ok((group.epoch().as_u64(), names))
    }

    /// Per-room counts the proxy reports: messages waiting for the harness.
    pub fn unread_counts(&self) -> HashMap<String, usize> {
        self.rooms
            .iter()
            .map(|(k, l)| (k.clone(), l.entries.iter().filter(|e| !e.delivered).count()))
            .collect()
    }
}

/// A retained room message as the daemon's history returns it.
#[derive(Debug)]
struct Raw {
    id: u64,
    from: String,
    body: String,
    created_at: String,
}

impl Raw {
    fn from_wire(m: &Value) -> Self {
        let text = |k: &str| m.get(k).and_then(Value::as_str).unwrap_or("").to_string();
        Self {
            id: m["id"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0),
            from: text("from"),
            body: text("body"),
            created_at: text("createdAt"),
        }
    }
}

/// The MLS credential that authenticated `processed` must be the name the
/// daemon says sent it.
fn check_sender(processed: &ProcessedMessage, from: &str) -> Result<(), ClientError> {
    let identity = processed.credential().serialized_content();
    if identity.eq_ignore_ascii_case(from.as_bytes()) {
        Ok(())
    } else {
        Err(ClientError::Mls(format!(
            "message from {from} is authenticated as another member"
        )))
    }
}
