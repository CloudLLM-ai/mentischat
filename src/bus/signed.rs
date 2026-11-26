//! Signed rooms: key binding, pinned membership, replay counters.
//!
//! A signed room is an ordinary room (same log, cursors and caps) whose
//! [`RoomState`] also holds a [`SignedState`]: who is admin, the key pinned
//! for each member and each pending invitation, the bans, and one counter
//! watermark per key. Every operation that changes it, and every send, arrives with an
//! Ed25519 signature the daemon checks (see [`crate::signed`]); the plain
//! tools and routes are refused with `signature_required`.
//!
//! Locks: the signature is checked with no lock held (it is the slow part);
//! counters, pins and membership are checked and changed under the room's
//! lock, so a check and the append it permits are one critical section. The
//! agent lock is never taken under a room's here, and the room lock order of
//! `rooms.rs` is unchanged.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::Arc;

use serde::Serialize;

use super::rooms::{not_in_room, Room, RoomState};
use super::signed_persist::OpRec;
use super::state::{Agent, Key, State};
use super::{
    validate_body, validate_description, Bus, LeaveOutcome, MailErr, Reach, RoomInfo, SendReceipt,
    MAX_ROOMS_PER_AGENT, MAX_ROOM_MEMBERS,
};
use crate::names::{AgentName, RoomName};
use crate::signed::{register_key_payload, Envelope, PublicKey, Signature, SignedOp};

/// Most distinct keys a signed room remembers a counter for: the live members
/// and invitations plus the departed, so a rejoin cannot replay old counters.
pub(super) const MAX_WATERMARKS: usize = 4 * MAX_ROOM_MEMBERS;

/// What a send must prove to enter a room.
pub(super) enum SendGate<'a> {
    /// A plain `send_message`: only an unsigned room takes it.
    Unsigned,
    /// A verified signed send by the holder of `key`: only a signed room
    /// whose pinned key for the sender is `key`, and with a fresh `counter`.
    Signed {
        /// The key the signature verified under.
        key: &'a PublicKey,
        /// The counter that was signed.
        counter: u64,
        /// The epoch the body commits from when it is an MLS Commit
        /// message (see [`crate::signed::mls_commit_epoch`]).
        commit_epoch: Option<u64>,
        /// The journal record of a Commit (`Some` exactly when `commit_epoch` is).
        entry: Option<Box<OpRec>>,
        /// When it arrived, for a public room's rate limit.
        now_ms: u64,
    },
}

/// Most bans a signed room keeps.
pub const MAX_BANS: usize = 4 * MAX_ROOM_MEMBERS;

/// Most messages one participant's key may send to a public room per
/// [`RATE_WINDOW_MS`]. Admins are not limited.
pub const PUBLIC_SENDS_PER_MINUTE: usize = 60;
/// Most times one key may join a public room per [`RATE_WINDOW_MS`]: a key
/// cannot churn in and out.
pub const PUBLIC_JOINS_PER_KEY: usize = 3;
/// Most joins a public room takes from everyone together per
/// [`RATE_WINDOW_MS`].
pub const PUBLIC_JOINS_PER_MINUTE: usize = 30;
/// Width of the public-room rate windows.
pub const RATE_WINDOW_MS: u64 = 60_000;

/// Sliding-window counters of a public room: what the room took in the last
/// [`RATE_WINDOW_MS`]. Live state only; it is not saved or replayed.
#[derive(Debug, Default)]
pub(super) struct Limits {
    /// When each key's recent sends landed, oldest first.
    sends: HashMap<PublicKey, VecDeque<u64>>,
    /// When each key's recent joins landed, oldest first.
    joins: HashMap<PublicKey, VecDeque<u64>>,
    /// When the room's recent joins landed, oldest first.
    room_joins: VecDeque<u64>,
}

/// `Err(retry_after_ms)` when `times` already holds `max` events within the
/// window ending at `now`; drops the events that fell out of it.
fn window(times: &mut VecDeque<u64>, now: u64, max: usize) -> Result<(), u64> {
    while times
        .front()
        .is_some_and(|at| now.saturating_sub(*at) >= RATE_WINDOW_MS)
    {
        times.pop_front();
    }
    match times.front() {
        Some(oldest) if times.len() >= max => {
            Err((oldest + RATE_WINDOW_MS).saturating_sub(now).max(1))
        }
        _ => Ok(()),
    }
}

impl Limits {
    /// Whether `key` may send now.
    fn check_send(&mut self, key: PublicKey, now: u64) -> Result<(), MailErr> {
        let times = self.sends.entry(key).or_default();
        let verdict = window(times, now, PUBLIC_SENDS_PER_MINUTE);
        if times.is_empty() {
            self.sends.remove(&key);
        }
        verdict.map_err(|retry_after_ms| MailErr::RateLimited { retry_after_ms })
    }

    /// Count a send by `key` that was admitted.
    fn note_send(&mut self, key: PublicKey, now: u64) {
        self.sends.entry(key).or_default().push_back(now);
    }

    /// Whether `key` may join now, by its own rate and the room's.
    fn check_join(&mut self, key: PublicKey, now: u64) -> Result<(), MailErr> {
        let times = self.joins.entry(key).or_default();
        let own = window(times, now, PUBLIC_JOINS_PER_KEY);
        if times.is_empty() {
            self.joins.remove(&key);
        }
        own.and_then(|()| window(&mut self.room_joins, now, PUBLIC_JOINS_PER_MINUTE))
            .map_err(|retry_after_ms| MailErr::RateLimited { retry_after_ms })
    }

    /// Count a join by `key`.
    fn note_join(&mut self, key: PublicKey, now: u64) {
        self.joins.entry(key).or_default().push_back(now);
        self.room_joins.push_back(now);
    }
}

/// The extra state of a signed room.
///
/// # Roles
///
/// Every member is an admin or a participant. The founder starts as the only
/// admin; admins promote, demote, invite, remove, ban and unban. When no member
/// is an admin (the last one left, was removed or dropped off the roster), the
/// member that has been in the room longest is the admin, so a room is never
/// left without one. That rule is computed from the join order, not stored, so
/// a replayed journal gives every member the same role it had.
#[derive(Debug)]
pub(super) struct SignedState {
    /// Listed and open to any agent with a bound key that is not banned.
    pub(super) public: bool,
    /// Members made admin, by folded name: the founder and those promoted.
    pub(super) admins: HashSet<String>,
    /// The key pinned for each member, by folded name.
    pub(super) pins: HashMap<String, PublicKey>,
    /// When each member joined (an increasing number), by folded name.
    pub(super) seats: HashMap<String, u64>,
    /// The next join number.
    next_seat: u64,
    /// Banned names, by folded name, with the key banned with each.
    pub(super) banned: BTreeMap<String, Option<PublicKey>>,
    /// The key each pending invitation pinned, by folded name.
    pub(super) invitations: HashMap<String, PublicKey>,
    /// Highest counter each key has signed in this room.
    pub(super) watermarks: HashMap<PublicKey, u64>,
    /// How many Commit messages the room has accepted: the epoch a member's
    /// next Commit must name. Starts at 0.
    pub(super) epoch: u64,
    /// MLS Welcomes waiting for invitees, by folded name.
    pub(super) welcomes: HashMap<String, Vec<u8>>,
    /// The signed operations that built the room, in order, for the snapshot.
    pub(super) journal: Vec<OpRec>,
    /// Set when the journal outgrew its cap: the room is not saved.
    pub(super) overflow: bool,
    /// Public-room rate limits; unused in a private room.
    limits: Limits,
}

/// Why [`SignedState::manage`] refused an operation, before it is put in
/// the room's words.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Deny {
    /// The signer is not an admin.
    NotAdmin,
    /// The target is not a member (or, for `remove`, an invitee; for `demote`,
    /// an admin; for `unban`, banned).
    NoTarget,
    /// An admin acting on itself where that is not allowed (remove, ban).
    SelfTarget,
    /// The key a `ban` names is not the key pinned for the target.
    WrongKey,
    /// The room already keeps [`MAX_BANS`] bans.
    Full,
}

impl Deny {
    /// The error a caller gets.
    fn into_err(self, signer: &str, target: &str, room: &str) -> MailErr {
        match self {
            Self::NotAdmin => MailErr::NotAnAdmin {
                agent: signer.to_string(),
                room: room.to_string(),
            },
            Self::NoTarget | Self::SelfTarget => MailErr::NotAMember {
                agent: target.to_string(),
                room: room.to_string(),
            },
            Self::WrongKey => MailErr::KeyConflict {
                agent: target.to_string(),
            },
            Self::Full => MailErr::RoomFull {
                room: room.to_string(),
                members: MAX_BANS as i64,
            },
        }
    }
}

impl SignedState {
    /// A room founded by `founder` (folded name) at `counter`.
    fn founded(founder: &str, key: PublicKey, public: bool, counter: u64, entry: &OpRec) -> Self {
        let mut state = Self::empty();
        state.public = public;
        state.seat(founder, key);
        state.admins.insert(founder.to_string());
        state.watermarks.insert(key, counter);
        state.record(entry);
        state
    }

    /// A room with nothing in it, the starting point of a replay.
    pub(super) fn empty() -> Self {
        Self {
            public: false,
            admins: HashSet::new(),
            pins: HashMap::new(),
            seats: HashMap::new(),
            next_seat: 0,
            banned: BTreeMap::new(),
            invitations: HashMap::new(),
            watermarks: HashMap::new(),
            epoch: 0,
            welcomes: HashMap::new(),
            journal: Vec::new(),
            overflow: false,
            limits: Limits::default(),
        }
    }

    /// Pin `key` for the member `name` (folded), seated after everyone else.
    pub(super) fn seat(&mut self, name: &str, key: PublicKey) {
        self.pins.insert(name.to_string(), key);
        self.seats.insert(name.to_string(), self.next_seat);
        self.next_seat += 1;
    }

    /// Forget the member `name` (folded): its pin, its seat and its role.
    pub(super) fn unseat(&mut self, name: &str) {
        self.pins.remove(name);
        self.seats.remove(name);
        self.admins.remove(name);
    }

    /// Whether the member `name` (folded) is an admin: one made admin, or,
    /// while no member is, the member seated first.
    pub(super) fn is_admin(&self, name: &str) -> bool {
        if !self.pins.contains_key(name) {
            return false;
        }
        if self.admins.iter().any(|a| self.pins.contains_key(a)) {
            return self.admins.contains(name);
        }
        self.seats
            .iter()
            .filter(|(n, _)| self.pins.contains_key(n.as_str()))
            .min_by_key(|(n, seat)| (**seat, n.as_str()))
            .is_some_and(|(n, _)| n == name)
    }

    /// Whether `name` (folded), or `key`, is banned.
    pub(super) fn is_banned(&self, name: &str, key: Option<PublicKey>) -> bool {
        self.banned.contains_key(name)
            || key.is_some_and(|k| self.banned.values().any(|b| *b == Some(k)))
    }

    /// Apply an admin operation (`remove`, `promote`, `demote`, `ban`,
    /// `unban`) by the member `signer` (folded) to this state, or say why not.
    /// The same rules for the live daemon and for a replayed journal. Returns
    /// the folded target, which the caller takes out of the room's member and
    /// invitee lists for `remove` and `ban`.
    pub(super) fn manage(&mut self, signer: &str, op: &SignedOp) -> Result<String, Deny> {
        let target = op
            .target()
            .and_then(|t| AgentName::parse(t).ok())
            .map(|t| t.folded())
            .ok_or(Deny::NoTarget)?;
        if !self.is_admin(signer) {
            return Err(Deny::NotAdmin);
        }
        let member = self.pins.contains_key(&target);
        let invited = self.invitations.contains_key(&target);
        match op {
            SignedOp::Remove { .. } => {
                if target == signer {
                    return Err(Deny::SelfTarget);
                }
                if !member && !invited {
                    return Err(Deny::NoTarget);
                }
                self.evict(&target);
            }
            SignedOp::Promote { .. } => {
                if !member {
                    return Err(Deny::NoTarget);
                }
                self.admins.insert(target.clone());
            }
            SignedOp::Demote { .. } => {
                if !self.admins.remove(&target) {
                    return Err(Deny::NoTarget);
                }
            }
            SignedOp::Ban { target_key, .. } => {
                if target == signer {
                    return Err(Deny::SelfTarget);
                }
                let pinned = self
                    .pins
                    .get(&target)
                    .or_else(|| self.invitations.get(&target));
                if pinned.is_some_and(|k| Some(*k) != *target_key) {
                    return Err(Deny::WrongKey);
                }
                if !self.banned.contains_key(&target) && self.banned.len() >= MAX_BANS {
                    return Err(Deny::Full);
                }
                self.evict(&target);
                self.banned.insert(target.clone(), *target_key);
            }
            SignedOp::Unban { .. } => {
                if self.banned.remove(&target).is_none() {
                    return Err(Deny::NoTarget);
                }
            }
            _ => return Err(Deny::NoTarget),
        }
        Ok(target)
    }

    /// Take `name` (folded) out as a member or invitee, with its Welcome.
    fn evict(&mut self, name: &str) {
        self.unseat(name);
        self.invitations.remove(name);
        self.welcomes.remove(name);
        self.forget_welcomes(name);
    }

    /// Whether `counter` from `key` is above the key's watermark (the first
    /// counter a key uses may be anything, including 0), and the room has
    /// space to remember a new key.
    fn check(&self, room: &Room, key: PublicKey, counter: u64) -> Result<(), MailErr> {
        match self.watermarks.get(&key) {
            Some(&last) if counter <= last => Err(MailErr::StaleCounter {
                room: room.name.to_string(),
                last,
            }),
            Some(_) => Ok(()),
            None if self.watermarks.len() >= MAX_WATERMARKS => Err(MailErr::RoomFull {
                room: room.name.to_string(),
                members: self.pins.len() as i64,
            }),
            None => Ok(()),
        }
    }

    /// [`check`](Self::check), then record `counter` as the key's watermark.
    fn bump(&mut self, room: &Room, key: PublicKey, counter: u64) -> Result<(), MailErr> {
        self.check(room, key, counter)?;
        self.watermarks.insert(key, counter);
        Ok(())
    }
}

impl RoomState {
    /// Fail a plain (unsigned) operation on a signed room, but only for those
    /// who may know the room exists: its members and invitees. Anyone else
    /// gets the `room_not_found` they would get from a private room.
    pub(super) fn refuse_unsigned(&self, room: &Room, agent: &Agent) -> Result<(), MailErr> {
        if self.signed.is_none() {
            return Ok(());
        }
        let key = Key::fold(&agent.name);
        let public = self.signed.as_ref().is_some_and(|s| s.public);
        if public || self.members.contains_key(key.as_str()) || self.invited.contains(key.as_str())
        {
            Err(MailErr::SignatureRequired {
                room: room.name.to_string(),
            })
        } else {
            Err(MailErr::RoomNotFound {
                room: room.name.to_string(),
            })
        }
    }

    /// Check a send against the room's mode, under the room's lock and after
    /// membership: a plain send needs an unsigned room; a signed send needs
    /// the sender's pinned key, a fresh counter and, for an MLS Commit, the
    /// room's current epoch. Changes nothing: [`record_send`](Self::record_send)
    /// applies it once the message is certain to be stored.
    pub(super) fn admit_send(
        &mut self,
        room: &Room,
        sender: &Agent,
        gate: &SendGate<'_>,
    ) -> Result<(), MailErr> {
        match gate {
            SendGate::Unsigned => self.refuse_unsigned(room, sender),
            SendGate::Signed {
                key,
                counter,
                commit_epoch,
                now_ms,
                ..
            } => {
                let name = Key::fold(&sender.name);
                let signed = self.signed.as_mut().ok_or_else(|| MailErr::RoomNotFound {
                    room: room.name.to_string(),
                })?;
                if signed.pins.get(name.as_str()) != Some(*key) {
                    return Err(MailErr::InvalidSignature);
                }
                signed.check(room, **key, *counter)?;
                if signed.public && !signed.is_admin(name.as_str()) {
                    signed.limits.check_send(**key, *now_ms)?;
                }
                match commit_epoch {
                    Some(epoch) if *epoch != signed.epoch => Err(MailErr::StaleEpoch {
                        room: room.name.to_string(),
                        epoch: signed.epoch,
                    }),
                    _ => Ok(()),
                }
            }
        }
    }

    /// Apply a send that [`admit_send`](Self::admit_send) admitted and that
    /// has been stored: consume the counter, advance the epoch on a Commit.
    pub(super) fn record_send(&mut self, gate: &SendGate<'_>) {
        if let (
            SendGate::Signed {
                key,
                counter,
                commit_epoch,
                entry,
                now_ms,
            },
            Some(signed),
        ) = (gate, self.signed.as_mut())
        {
            signed.watermarks.insert(**key, *counter);
            if signed.public {
                signed.limits.note_send(**key, *now_ms);
            }
            if commit_epoch.is_some() {
                signed.epoch += 1;
                if let Some(entry) = entry {
                    signed.record(entry);
                }
            }
        }
    }

    /// The room must be live and signed, and `agent` a member whose pinned
    /// key is `key`; otherwise the error it would get from any private room.
    fn require_signed_member(
        &self,
        room: &Room,
        agent: &Agent,
        asked: &RoomName,
        key: PublicKey,
    ) -> Result<(), MailErr> {
        let name = Key::fold(&agent.name);
        let Some(signed) = self.signed.as_ref().filter(|_| !self.retired) else {
            return Err(not_a_signed_room(asked));
        };
        match signed.pins.get(name.as_str()) {
            Some(pinned) if *pinned == key && self.members.contains_key(name.as_str()) => Ok(()),
            Some(_) => Err(MailErr::InvalidSignature),
            None => Err(not_in_room(room, self, agent, asked)),
        }
    }

    /// `agent`, a member of this signed room, must be an admin.
    fn require_admin(&self, room: &Room, agent: &Agent) -> Result<(), MailErr> {
        let name = Key::fold(&agent.name);
        if self
            .signed
            .as_ref()
            .is_some_and(|s| s.is_admin(name.as_str()))
        {
            Ok(())
        } else {
            Err(MailErr::NotAnAdmin {
                agent: agent.name.to_string(),
                room: room.name.to_string(),
            })
        }
    }

    /// Forget the key pinned for a member that left the room, and its role.
    pub(super) fn drop_pin(&mut self, folded: &str) {
        if let Some(signed) = self.signed.as_mut() {
            signed.unseat(folded);
        }
    }

    /// Take a member out of a signed room without a signature of its own (it
    /// left the roster: deregistered or idle), and journal that it went, so a
    /// replay seats the same members in the same order.
    pub(super) fn drop_unsigned(&mut self, folded: &str) {
        if let Some(signed) = self.signed.as_mut() {
            if signed.pins.contains_key(folded) {
                signed.unseat(folded);
                signed.record(&OpRec::dropped(folded));
            }
        }
    }

    /// Forget the key pinned by an invitation that will not be used.
    pub(super) fn drop_invitation(&mut self, folded: &str) {
        if let Some(signed) = self.signed.as_mut() {
            signed.invitations.remove(folded);
            signed.welcomes.remove(folded);
        }
    }
}

/// A key bound to a roster name.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct KeyBinding {
    /// The roster name, spelled as the roster stores it.
    pub agent: String,
    /// The bound Ed25519 public key, 64 lowercase hex digits.
    pub public_key: String,
}

/// A member's role in a signed room.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    /// Invites, removes, bans, unbans, promotes and demotes.
    Admin,
    /// Sends and reads.
    Participant,
}

/// One member of a signed room with the key its operations must verify under.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SignedMember {
    /// The member's name.
    pub agent: String,
    /// Its pinned public key, 64 lowercase hex digits.
    pub public_key: String,
    /// Admin or participant.
    pub role: Role,
}

/// A ban in a signed room.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Ban {
    /// The banned name, folded to lowercase.
    pub agent: String,
    /// The key banned with it, 64 lowercase hex digits, when it had one.
    pub public_key: Option<String>,
}

/// A signed room's membership as the daemon enforces it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SignedRoomInfo {
    /// The room's name.
    pub room: String,
    /// Listed and open to any agent with a key that is not banned; private
    /// rooms take only the invited.
    pub public: bool,
    /// The admins' names, sorted.
    pub admins: Vec<String>,
    /// How many MLS Commits the room has accepted: the epoch the next one
    /// must name.
    pub epoch: u64,
    /// Members sorted by name, each with its pinned key.
    pub members: Vec<SignedMember>,
    /// Names invited and not yet joined, sorted: a client pruning departed
    /// members must not mistake an invitee for one.
    pub invited: Vec<String>,
    /// The bans, sorted by name.
    pub banned: Vec<Ban>,
    /// The highest counter the daemon has accepted from the asking member's key
    /// in this room. A client remembers the highest counter the daemon accepted
    /// from it; a lower answer means the room was rolled back.
    pub your_counter: Option<u64>,
}

/// Most KeyPackages the daemon keeps for one agent; publishing more drops the
/// oldest.
pub const MAX_KEY_PACKAGES: usize = 16;
/// Largest serialized KeyPackage the daemon stores (a real one is a few
/// hundred bytes).
pub const MAX_KEY_PACKAGE_BYTES: usize = 1024;
/// Largest MLS Welcome the daemon stores for one invitee.
pub const MAX_WELCOME_BYTES: usize = 64 * 1024;
/// Most Welcome bytes one room holds at a time, over all its invitees.
const MAX_ROOM_WELCOME_BYTES: usize = 1024 * 1024;

/// Width of the KeyPackage request rate-limit window.
const TAKE_WINDOW_MS: u64 = 60_000;
/// Most KeyPackages one requester may take from one agent per minute.
pub const TAKES_PER_REQUESTER: usize = 8;
/// Most KeyPackages anyone together may take from one agent per minute.
pub const TAKES_PER_MINUTE: usize = 24;

/// A KeyPackage handed out for an add.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyPackageOffer {
    /// The serialized KeyPackage.
    pub package: Vec<u8>,
    /// True when it is the agent's last-resort package, which stays on offer.
    pub last_resort: bool,
}

/// The KeyPackages an agent has on offer, after a publish.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct KeyPackageStock {
    /// The agent.
    pub agent: String,
    /// How many ordinary packages are available now.
    pub available: usize,
    /// Whether a last-resort package is on offer.
    pub last_resort: bool,
}

/// A signed operation as the REST layer parsed it.
#[derive(Debug, Clone)]
pub struct SignedRequest {
    /// The acting agent.
    pub agent: AgentName,
    /// The room it acts on.
    pub room: RoomName,
    /// The signed counter.
    pub counter: u64,
    /// What it does.
    pub op: SignedOp,
    /// The signature over [`SignedOp::payload`].
    pub signature: Signature,
}

/// What a signed operation produced.
#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum SignedOutcome {
    /// `create`, `invite`, `join` and `remove` answer with the room.
    Room(RoomInfo),
    /// `leave` answers like `leave_room`.
    Left(LeaveOutcome),
    /// `send` answers with the receipt.
    Sent(SendReceipt),
}

impl Bus {
    /// Bind `key` to `agent` (trust on first use). `signature` signs
    /// [`register_key_payload`], proving the caller holds the key.
    ///
    /// With a key registry attached ([`Bus::with_key_registry`]) the binding
    /// is recorded there before it applies, and a name the registry already
    /// binds to another key fails `key_conflict` even if this registration
    /// has none yet.
    ///
    /// # Errors
    ///
    /// `unregistered`; `invalid_signature`; `key_conflict` when the name is
    /// bound to a different key (binding the same key again is fine);
    /// `shutting_down`.
    pub async fn register_key(
        &self,
        agent: &AgentName,
        key: PublicKey,
        signature: &Signature,
    ) -> Result<KeyBinding, MailErr> {
        let (state, found) = self.active(agent)?;
        key.verify(&register_key_payload(agent.as_str(), &key), signature)
            .map_err(|_| MailErr::InvalidSignature)?;
        let conflict = || MailErr::KeyConflict {
            agent: found.name.to_string(),
        };
        if found.inner.lock().key.is_some_and(|bound| bound != key) {
            return Err(conflict());
        }
        // The registry decides first, atomically, and durably (outside the
        // agent's lock: it fsyncs); then the binding applies in memory.
        if let Some(ring) = &state.keyring {
            match ring.bind(&found.name, key, *signature) {
                Ok(true) => {}
                Ok(false) => return Err(conflict()),
                Err(e) => super::persist(Err(e)),
            }
        }
        let mut st = found.inner.lock();
        match st.key {
            Some(bound) if bound != key => Err(conflict()),
            _ => {
                st.key = Some(key);
                st.key_proof = Some(*signature);
                Ok(KeyBinding {
                    agent: found.name.to_string(),
                    public_key: key.to_hex(),
                })
            }
        }
    }

    /// The key bound to `agent`, if any. Reads the roster only: it does not
    /// refresh anyone's `last_seen`.
    ///
    /// # Errors
    ///
    /// `unregistered`; `shutting_down`.
    pub async fn agent_key(&self, agent: &AgentName) -> Result<Option<KeyBinding>, MailErr> {
        let state = self.open()?;
        let found = state.agent(agent.as_str(), Key::fold(agent.as_str()).as_str())?;
        let key = found.inner.lock().key;
        Ok(key.map(|k| KeyBinding {
            agent: found.name.to_string(),
            public_key: k.to_hex(),
        }))
    }

    /// Apply one signed room operation after verifying its signature under
    /// the key bound to the acting agent.
    ///
    /// # Errors
    ///
    /// `unregistered`; `key_required`; `invalid_signature`; `stale_counter`;
    /// `room_not_found` (also for a room that is not signed, or a private one
    /// the agent may not know); `not_a_member`; `room_exists`; the room caps;
    /// `invalid_body` and `storage_full` for a send; `shutting_down`.
    pub async fn signed_op(&self, req: SignedRequest) -> Result<SignedOutcome, MailErr> {
        let (state, agent) = self.active(&req.agent)?;
        let key = agent.inner.lock().key.ok_or_else(|| MailErr::KeyRequired {
            agent: agent.name.to_string(),
        })?;
        let payload = req
            .op
            .payload(req.room.as_str(), req.agent.as_str(), req.counter);
        key.verify(&payload, &req.signature)
            .map_err(|_| MailErr::InvalidSignature)?;
        let (room, counter) = (&req.room, req.counter);
        // The journal record, built for every operation but a plain message
        // (only Commits among sends are journaled), so ordinary sends copy nothing.
        let entry = match &req.op {
            SignedOp::Send { body } => crate::signed::mls_commit_epoch(body)
                .map(|_| OpRec::new(req.agent.as_str(), &key, counter, &req.signature, &req.op)),
            op => Some(OpRec::new(
                req.agent.as_str(),
                &key,
                counter,
                &req.signature,
                op,
            )),
        };
        let need = || entry.as_ref().ok_or(MailErr::InvalidBody);
        match req.op.clone() {
            SignedOp::Create {
                description,
                public,
            } => {
                let description =
                    Some(validate_description(description)?).filter(|d| !d.is_empty());
                state
                    .signed_create(&agent, room, (key, counter), public, description, need()?)
                    .map(SignedOutcome::Room)
            }
            SignedOp::Invite {
                invitee,
                invitee_key,
            } => {
                let invitee = AgentName::parse(&invitee)?;
                let invitee =
                    state.agent(invitee.as_str(), Key::fold(invitee.as_str()).as_str())?;
                let bound = invitee.inner.lock().key;
                match bound {
                    None => Err(MailErr::KeyRequired {
                        agent: invitee.name.to_string(),
                    }),
                    Some(bound) if bound != invitee_key => Err(MailErr::KeyConflict {
                        agent: invitee.name.to_string(),
                    }),
                    Some(_) => state.signed_invite(
                        &agent,
                        room,
                        key,
                        counter,
                        (&invitee, invitee_key),
                        need()?,
                    ),
                }
                .map(SignedOutcome::Room)
            }
            SignedOp::Join => state
                .signed_join(&agent, room, (key, counter), self.now_ms(), need()?)
                .map(SignedOutcome::Room),
            SignedOp::Leave => state
                .signed_leave(&agent, room, key, counter, need()?)
                .map(SignedOutcome::Left),
            SignedOp::Remove { .. }
            | SignedOp::Promote { .. }
            | SignedOp::Demote { .. }
            | SignedOp::Unban { .. } => state
                .signed_manage(&agent, room, (key, counter), &req.op, need()?)
                .map(SignedOutcome::Room),
            SignedOp::Ban {
                ref target,
                target_key,
            } => {
                let name = AgentName::parse(target)?;
                // A name on the roster is banned with the key bound to it; a
                // name that is not can be banned ahead of time, as signed.
                if let Ok(found) = state.agent(name.as_str(), &name.folded()) {
                    if found.inner.lock().key != target_key {
                        return Err(MailErr::KeyConflict {
                            agent: found.name.to_string(),
                        });
                    }
                }
                state
                    .signed_manage(&agent, room, (key, counter), &req.op, need()?)
                    .map(SignedOutcome::Room)
            }
            SignedOp::Welcome { target, welcome } => {
                let target = AgentName::parse(&target)?;
                state
                    .signed_store_welcome(&agent, room, key, counter, (&target, welcome), need()?)
                    .map(SignedOutcome::Room)
            }
            SignedOp::Send { body } => {
                validate_body(&body)?;
                let envelope = Envelope::encode(counter, &key, &req.signature, &body);
                let json_len = super::log::json_len(&envelope);
                let commit_epoch = crate::signed::mls_commit_epoch(&body);
                let (msg, heard) = state.send_room(
                    &agent,
                    room,
                    envelope.into_boxed_str(),
                    json_len,
                    self.now_ms(),
                    SendGate::Signed {
                        key: &key,
                        counter,
                        commit_epoch,
                        entry: entry.clone().map(Box::new),
                        now_ms: self.now_ms(),
                    },
                )?;
                self.traffic.sent(&msg, super::traffic::Body::Signed);
                Ok(SignedOutcome::Sent(SendReceipt::new(
                    &msg,
                    Reach::Room(heard),
                )))
            }
        }
    }

    /// Offer `packages` (serialized MLS KeyPackages, opaque here) and
    /// optionally a `last_resort` KeyPackage (RFC 9420 section 16.8) for others
    /// to add `agent` with. `signature` signs
    /// [`key_packages_payload`](crate::signed::key_packages_payload) under the
    /// agent's bound key, with a `counter` above its previous publish. The
    /// directory keeps at most [`MAX_KEY_PACKAGES`] ordinary packages, dropping
    /// the oldest, and one last-resort package, replaced by each publish that
    /// carries one.
    ///
    /// # Errors
    ///
    /// `unregistered`; `key_required`; `invalid_signature`; `stale_counter`
    /// (carrying the agent's own previous counter); `invalid_body` for an
    /// empty publish, more than [`MAX_KEY_PACKAGES`] packages, or one over
    /// [`MAX_KEY_PACKAGE_BYTES`]; `shutting_down`.
    pub async fn publish_key_packages(
        &self,
        agent: &AgentName,
        counter: u64,
        packages: Vec<Vec<u8>>,
        last_resort: Option<Vec<u8>>,
        signature: &Signature,
    ) -> Result<KeyPackageStock, MailErr> {
        let sized = |p: &Vec<u8>| p.is_empty() || p.len() > MAX_KEY_PACKAGE_BYTES;
        if (packages.is_empty() && last_resort.is_none())
            || packages.len() > MAX_KEY_PACKAGES
            || packages.iter().any(sized)
            || last_resort.as_ref().is_some_and(sized)
        {
            return Err(MailErr::InvalidBody);
        }
        let (_, found) = self.active(agent)?;
        let key = found.inner.lock().key.ok_or_else(|| MailErr::KeyRequired {
            agent: found.name.to_string(),
        })?;
        let payload = crate::signed::key_packages_payload(
            agent.as_str(),
            counter,
            &packages,
            last_resort.as_deref(),
        );
        key.verify(&payload, signature)
            .map_err(|_| MailErr::InvalidSignature)?;
        let mut st = found.inner.lock();
        if counter <= st.key_counter {
            return Err(MailErr::StaleCounter {
                room: String::new(),
                last: st.key_counter,
            });
        }
        st.key_counter = counter;
        drop(st);
        if let Some(ring) = &self.open()?.keyring {
            super::persist(ring.raise_counter(&agent.folded(), counter));
        }
        let mut st = found.inner.lock();
        st.key_packages.extend(packages);
        while st.key_packages.len() > MAX_KEY_PACKAGES {
            st.key_packages.pop_front();
        }
        if last_resort.is_some() {
            st.last_resort = last_resort;
        }
        Ok(KeyPackageStock {
            agent: found.name.to_string(),
            available: st.key_packages.len(),
            last_resort: st.last_resort.is_some(),
        })
    }

    /// Hand `requester` one of `agent`'s KeyPackages: the oldest ordinary one,
    /// which is then forgotten, or, once those are gone, the agent's
    /// last-resort package, which is never used up. Anyone registered may ask;
    /// a KeyPackage is public. Requests are rate limited so one local process
    /// cannot drain an agent: at most [`TAKES_PER_REQUESTER`] a minute from
    /// one requester and [`TAKES_PER_MINUTE`] from everyone together.
    ///
    /// # Errors
    ///
    /// `unregistered` (for `agent` or `requester`); `rate_limited`;
    /// `no_key_package` when the agent has neither ordinary nor last-resort
    /// packages; `shutting_down`.
    pub async fn take_key_package(
        &self,
        agent: &AgentName,
        requester: &AgentName,
    ) -> Result<KeyPackageOffer, MailErr> {
        let state = self.open()?;
        let found = state.agent(agent.as_str(), Key::fold(agent.as_str()).as_str())?;
        state.agent(requester.as_str(), Key::fold(requester.as_str()).as_str())?;
        let now = self.now_ms();
        let who = requester.folded();
        let mut st = found.inner.lock();
        while st
            .package_takes
            .front()
            .is_some_and(|(at, _)| now.saturating_sub(*at) >= TAKE_WINDOW_MS)
        {
            st.package_takes.pop_front();
        }
        let mine = st.package_takes.iter().filter(|(_, w)| *w == who).count();
        if mine >= TAKES_PER_REQUESTER || st.package_takes.len() >= TAKES_PER_MINUTE {
            let oldest = st
                .package_takes
                .iter()
                .find(|(_, w)| mine < TAKES_PER_REQUESTER || *w == who);
            let at = oldest.map_or(now, |(at, _)| *at);
            return Err(MailErr::RateLimited {
                retry_after_ms: (at + TAKE_WINDOW_MS).saturating_sub(now).max(1),
            });
        }
        let (package, last_resort) = match st.key_packages.pop_front() {
            Some(p) => (p, false),
            None => match st.last_resort.clone() {
                Some(p) => (p, true),
                None => {
                    return Err(MailErr::NoKeyPackage {
                        agent: found.name.to_string(),
                    })
                }
            },
        };
        st.package_takes.push_back((now, who));
        Ok(KeyPackageOffer {
            package,
            last_resort,
        })
    }

    /// The MLS Welcome waiting for `agent` in signed room `room`, if its
    /// inviter left one. Only an invited agent may read it; everyone else
    /// gets `room_not_found`. The Welcome stays until the agent joins.
    ///
    /// # Errors
    ///
    /// `unregistered`; `room_not_found`; `shutting_down`.
    pub async fn signed_welcome(
        &self,
        agent: &AgentName,
        room: &RoomName,
    ) -> Result<Option<Vec<u8>>, MailErr> {
        let (state, agent) = self.active(agent)?;
        state.signed_welcome(&agent, room)
    }

    /// The members of signed room `room` with their pinned keys, for a
    /// member: what a reader checks envelopes against.
    ///
    /// # Errors
    ///
    /// `unregistered`; `room_not_found` (also for a room that is not signed);
    /// `not_a_member`; `shutting_down`.
    pub async fn signed_room_info(
        &self,
        agent: &AgentName,
        room: &RoomName,
    ) -> Result<SignedRoomInfo, MailErr> {
        let (state, agent) = self.active(agent)?;
        state.signed_room_info(&agent, room)
    }
}

impl State {
    fn signed_create(
        &self,
        agent: &Arc<Agent>,
        room: &RoomName,
        (key, counter): (PublicKey, u64),
        public: bool,
        description: Option<String>,
        entry: &OpRec,
    ) -> Result<RoomInfo, MailErr> {
        let founder = Key::fold(&agent.name);
        let signed = SignedState::founded(founder.as_str(), key, public, counter, entry);
        self.create_room_with(agent, room, !public, description, Some(signed))
    }

    /// The signed state of a live signed room; anything else is
    /// `room_not_found`, so a signed operation cannot probe plain rooms.
    fn signed_room(&self, room: &RoomName) -> Result<Arc<Room>, MailErr> {
        self.live_room(room)
    }

    fn signed_invite(
        &self,
        agent: &Arc<Agent>,
        room: &RoomName,
        key: PublicKey,
        counter: u64,
        (invitee, invitee_key): (&Arc<Agent>, PublicKey),
        entry: &OpRec,
    ) -> Result<RoomInfo, MailErr> {
        let found = self.signed_room(room)?;
        let mut st = found.inner.lock();
        let invitee_name = Key::fold(&invitee.name);
        st.require_signed_member(&found, agent, room, key)?;
        st.require_admin(&found, agent)?;
        if st
            .signed
            .as_ref()
            .is_some_and(|s| s.is_banned(invitee_name.as_str(), Some(invitee_key)))
        {
            return Err(MailErr::Banned {
                agent: invitee.name.to_string(),
                room: found.name.to_string(),
            });
        }
        if !st.members.contains_key(invitee_name.as_str()) {
            let already = st.invited.contains(invitee_name.as_str());
            if !already && st.members.len() + st.invited.len() >= MAX_ROOM_MEMBERS {
                return Err(MailErr::RoomFull {
                    room: found.name.to_string(),
                    members: st.members.len() as i64,
                });
            }
            let signed = st.signed.as_mut().ok_or_else(|| not_a_signed_room(room))?;
            signed.bump(&found, key, counter)?;
            signed.record(entry);
            signed
                .invitations
                .insert(invitee_name.as_str().to_string(), invitee_key);
            st.invited.insert(invitee_name.as_str().to_string());
        }
        let info = st.info(&found);
        drop(st);
        let mut inv = invitee.inner.lock();
        if inv.invited_to.len() < MAX_ROOMS_PER_AGENT {
            inv.invited_to
                .insert(Key::fold(&found.name).as_str().to_string());
        }
        Ok(info)
    }

    fn signed_join(
        &self,
        agent: &Arc<Agent>,
        room: &RoomName,
        (key, counter): (PublicKey, u64),
        now_ms: u64,
        entry: &OpRec,
    ) -> Result<RoomInfo, MailErr> {
        let found = self.signed_room(room)?;
        if agent.inner.lock().rooms.len() >= MAX_ROOMS_PER_AGENT {
            return Err(MailErr::TooManyRooms {
                agent: agent.name.to_string(),
                rooms: MAX_ROOMS_PER_AGENT as i64,
            });
        }
        let name = Key::fold(&agent.name);
        let info = {
            let mut st = found.inner.lock();
            let member = st.member_for(agent);
            if st.retired || st.signed.is_none() {
                return Err(not_a_signed_room(room));
            }
            if st.members.contains_key(name.as_str()) {
                // A member picking up a Welcome (a re-add) is done with it.
                let pinned = st
                    .signed
                    .as_ref()
                    .and_then(|s| s.pins.get(name.as_str()).copied());
                if pinned != Some(key) {
                    return Err(MailErr::InvalidSignature);
                }
                let signed = st.signed.as_mut().ok_or_else(|| not_a_signed_room(room))?;
                signed.bump(&found, key, counter)?;
                signed.welcomes.remove(name.as_str());
                signed.forget_welcomes(name.as_str());
            } else {
                let signed = st.signed.as_ref().ok_or_else(|| not_a_signed_room(room))?;
                let pinned = signed.invitations.get(name.as_str()).copied();
                if signed.is_banned(name.as_str(), Some(key)) {
                    // A private room still does not admit it exists.
                    return Err(if signed.public || pinned.is_some() {
                        MailErr::Banned {
                            agent: agent.name.to_string(),
                            room: found.name.to_string(),
                        }
                    } else {
                        not_a_signed_room(room)
                    });
                }
                match pinned {
                    // A public room takes any agent under its bound key, the
                    // key the signature was checked against.
                    None if signed.public => {}
                    None => return Err(not_a_signed_room(room)),
                    Some(pinned) if pinned != key => return Err(MailErr::InvalidSignature),
                    Some(_) => {}
                }
                if st.members.len() >= MAX_ROOM_MEMBERS {
                    return Err(MailErr::RoomFull {
                        room: found.name.to_string(),
                        members: st.members.len() as i64,
                    });
                }
                let signed = st.signed.as_mut().ok_or_else(|| not_a_signed_room(room))?;
                signed.check(&found, key, counter)?;
                // An invited agent was let in by an admin; only open joins count.
                let open = signed.public && pinned.is_none();
                if open {
                    signed.limits.check_join(key, now_ms)?;
                    signed.limits.note_join(key, now_ms);
                }
                signed.bump(&found, key, counter)?;
                signed.record(entry);
                signed.forget_welcomes(name.as_str());
                signed.invitations.remove(name.as_str());
                signed.welcomes.remove(name.as_str());
                signed.seat(name.as_str(), key);
                st.invited.remove(name.as_str());
                st.members.insert(name.as_str().to_string(), member);
            }
            st.info(&found)
        };
        self.remember_room(agent, &found);
        Ok(info)
    }

    fn signed_leave(
        &self,
        agent: &Arc<Agent>,
        room: &RoomName,
        key: PublicKey,
        counter: u64,
        entry: &OpRec,
    ) -> Result<LeaveOutcome, MailErr> {
        let found = self.signed_room(room)?;
        let name = Key::fold(&agent.name);
        let emptied = {
            let mut st = found.inner.lock();
            st.require_signed_member(&found, agent, room, key)?;
            let signed = st.signed.as_mut().ok_or_else(|| not_a_signed_room(room))?;
            signed.bump(&found, key, counter)?;
            signed.record(entry);
            st.drop_pin(name.as_str());
            st.members.remove(name.as_str());
            st.members.is_empty()
        };
        agent.inner.lock().rooms.retain(|r| !Arc::ptr_eq(r, &found));
        if emptied {
            self.reap_room(&found);
        }
        Ok(LeaveOutcome {
            room: found.name.to_string(),
            deleted: emptied,
        })
    }

    /// An admin operation: `remove`, `promote`, `demote`, `ban` or `unban`
    /// (see [`SignedState::manage`]).
    fn signed_manage(
        &self,
        agent: &Arc<Agent>,
        room: &RoomName,
        (key, counter): (PublicKey, u64),
        op: &SignedOp,
        entry: &OpRec,
    ) -> Result<RoomInfo, MailErr> {
        let found = self.signed_room(room)?;
        let name = Key::fold(&agent.name);
        let shown = op.target().unwrap_or_default().to_string();
        let (info, evicted) = {
            let mut st = found.inner.lock();
            st.require_signed_member(&found, agent, room, key)?;
            let signed = st.signed.as_mut().ok_or_else(|| not_a_signed_room(room))?;
            signed.check(&found, key, counter)?;
            let target = signed
                .manage(name.as_str(), op)
                .map_err(|deny| deny.into_err(&agent.name, &shown, &found.name))?;
            signed.watermarks.insert(key, counter);
            signed.record(entry);
            let evicted = matches!(op, SignedOp::Remove { .. } | SignedOp::Ban { .. });
            if evicted {
                st.invited.remove(target.as_str());
                st.members.remove(target.as_str());
            }
            (st.info(&found), evicted.then_some(target))
        };
        if let Some(target) = evicted {
            if let Ok(gone) = self.agent(&target, &target) {
                let mut gone = gone.inner.lock();
                gone.rooms.retain(|r| !Arc::ptr_eq(r, &found));
                gone.invited_to.remove(Key::fold(&found.name).as_str());
            }
        }
        Ok(info)
    }

    fn signed_store_welcome(
        &self,
        agent: &Arc<Agent>,
        room: &RoomName,
        key: PublicKey,
        counter: u64,
        (target, welcome): (&AgentName, Vec<u8>),
        entry: &OpRec,
    ) -> Result<RoomInfo, MailErr> {
        if welcome.is_empty() || welcome.len() > MAX_WELCOME_BYTES {
            return Err(MailErr::InvalidBody);
        }
        let found = self.signed_room(room)?;
        let target_key = Key::fold(target.as_str());
        let mut st = found.inner.lock();
        st.require_signed_member(&found, agent, room, key)?;
        let is_member = st.members.contains_key(target_key.as_str());
        let signed = st.signed.as_mut().ok_or_else(|| not_a_signed_room(room))?;
        if !is_member && !signed.invitations.contains_key(target_key.as_str()) {
            return Err(MailErr::NotAMember {
                agent: target.to_string(),
                room: found.name.to_string(),
            });
        }
        let held: usize = signed
            .welcomes
            .iter()
            .filter(|(name, _)| name.as_str() != target_key.as_str())
            .map(|(_, w)| w.len())
            .sum();
        if held + welcome.len() > MAX_ROOM_WELCOME_BYTES {
            return Err(MailErr::InvalidBody);
        }
        signed.bump(&found, key, counter)?;
        signed.forget_welcomes(target_key.as_str());
        signed.record(entry);
        signed
            .welcomes
            .insert(target_key.as_str().to_string(), welcome);
        Ok(st.info(&found))
    }

    fn signed_welcome(&self, agent: &Agent, room: &RoomName) -> Result<Option<Vec<u8>>, MailErr> {
        let found = self.signed_room(room)?;
        let st = found.inner.lock();
        let name = Key::fold(&agent.name);
        let signed = st
            .signed
            .as_ref()
            .filter(|_| !st.retired)
            .ok_or_else(|| not_a_signed_room(room))?;
        if !signed.invitations.contains_key(name.as_str())
            && !st.members.contains_key(name.as_str())
        {
            return Err(not_a_signed_room(room));
        }
        Ok(signed.welcomes.get(name.as_str()).cloned())
    }

    fn signed_room_info(&self, agent: &Agent, room: &RoomName) -> Result<SignedRoomInfo, MailErr> {
        let found = self.signed_room(room)?;
        let st = found.inner.lock();
        let Some(signed) = st.signed.as_ref() else {
            return Err(not_a_signed_room(room));
        };
        if st.retired || st.member(agent).is_none() {
            return Err(not_in_room(&found, &st, agent, room));
        }
        let mut members: Vec<SignedMember> = st
            .members
            .iter()
            .filter_map(|(folded, m)| {
                signed.pins.get(folded).map(|k| SignedMember {
                    agent: m.name.to_string(),
                    public_key: k.to_hex(),
                    role: if signed.is_admin(folded) {
                        Role::Admin
                    } else {
                        Role::Participant
                    },
                })
            })
            .collect();
        members.sort_unstable_by(|a, b| {
            a.agent
                .to_ascii_lowercase()
                .cmp(&b.agent.to_ascii_lowercase())
        });
        let mut invited: Vec<String> = signed.invitations.keys().cloned().collect();
        invited.sort_unstable();
        let admins = members
            .iter()
            .filter(|m| m.role == Role::Admin)
            .map(|m| m.agent.clone())
            .collect();
        let banned = signed
            .banned
            .iter()
            .map(|(name, key)| Ban {
                agent: name.clone(),
                public_key: key.map(|k| k.to_hex()),
            })
            .collect();
        Ok(SignedRoomInfo {
            room: found.name.to_string(),
            public: signed.public,
            admins,
            epoch: signed.epoch,
            members,
            invited,
            banned,
            your_counter: signed
                .pins
                .get(Key::fold(&agent.name).as_str())
                .and_then(|k| signed.watermarks.get(k).copied()),
        })
    }
}

fn not_a_signed_room(room: &RoomName) -> MailErr {
    MailErr::RoomNotFound {
        room: room.to_string(),
    }
}
