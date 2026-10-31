//! Signed rooms: key binding, pinned membership, replay counters.
//!
//! A signed room is an ordinary room (same log, cursors and caps) whose
//! [`RoomState`] also holds a [`SignedState`]: the owner's key, the key pinned
//! for each member and each pending invitation, and one counter watermark per
//! key. Every operation that changes it, and every send, arrives with an
//! Ed25519 signature the daemon checks (see [`crate::signed`]); the plain
//! tools and routes are refused with `signature_required`.
//!
//! Locks: the signature is checked with no lock held (it is the slow part);
//! counters, pins and membership are checked and changed under the room's
//! lock, so a check and the append it permits are one critical section. The
//! agent lock is never taken under a room's here, and the room lock order of
//! `rooms.rs` is unchanged.

use std::collections::HashMap;
use std::sync::Arc;

use serde::Serialize;

use super::rooms::{not_in_room, Room, RoomState};
use super::state::{Agent, Key, State};
use super::{
    validate_body, validate_description, Bus, LeaveOutcome, MailErr, RoomInfo, SendReceipt,
    MAX_ROOMS_PER_AGENT, MAX_ROOM_MEMBERS,
};
use crate::names::{AgentName, RoomName};
use crate::signed::{register_key_payload, Envelope, PublicKey, Signature, SignedOp};

/// Most distinct keys a signed room remembers a counter for: the live members
/// and invitations plus the departed, so a rejoin cannot replay old counters.
const MAX_WATERMARKS: usize = 4 * MAX_ROOM_MEMBERS;

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
    },
}

/// The extra state of a signed room.
#[derive(Debug)]
pub(super) struct SignedState {
    /// The founder's key while the founder is a member; only the owner may
    /// remove others.
    owner: Option<PublicKey>,
    /// The key pinned for each member, by folded name.
    pins: HashMap<String, PublicKey>,
    /// The key each pending invitation pinned, by folded name.
    invitations: HashMap<String, PublicKey>,
    /// Highest counter each key has signed in this room.
    watermarks: HashMap<PublicKey, u64>,
    /// How many Commit messages the room has accepted: the epoch a member's
    /// next Commit must name. Starts at 0.
    epoch: u64,
    /// MLS Welcomes waiting for invitees, by folded name.
    welcomes: HashMap<String, Vec<u8>>,
}

impl SignedState {
    /// A room founded by `owner` (folded name `name`) at `counter`.
    fn founded(name: &str, owner: PublicKey, counter: u64) -> Self {
        Self {
            owner: Some(owner),
            pins: HashMap::from([(name.to_string(), owner)]),
            invitations: HashMap::new(),
            watermarks: HashMap::from([(owner, counter)]),
            epoch: 0,
            welcomes: HashMap::new(),
        }
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
        if self.members.contains_key(key.as_str()) || self.invited.contains(key.as_str()) {
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
        &self,
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
            } => {
                let name = Key::fold(&sender.name);
                let signed = self.signed.as_ref().ok_or_else(|| MailErr::RoomNotFound {
                    room: room.name.to_string(),
                })?;
                if signed.pins.get(name.as_str()) != Some(*key) {
                    return Err(MailErr::InvalidSignature);
                }
                signed.check(room, **key, *counter)?;
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
            },
            Some(signed),
        ) = (gate, self.signed.as_mut())
        {
            signed.watermarks.insert(**key, *counter);
            if commit_epoch.is_some() {
                signed.epoch += 1;
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

    /// Forget the key pinned for a member that left the room.
    pub(super) fn drop_pin(&mut self, folded: &str) {
        if let Some(signed) = self.signed.as_mut() {
            if let Some(key) = signed.pins.remove(folded) {
                if signed.owner == Some(key) {
                    signed.owner = None;
                }
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

/// One member of a signed room with the key its operations must verify under.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SignedMember {
    /// The member's name.
    pub agent: String,
    /// Its pinned public key, 64 lowercase hex digits.
    pub public_key: String,
}

/// A signed room's membership as the daemon enforces it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SignedRoomInfo {
    /// The room's name.
    pub room: String,
    /// The owner's key while the owner is a member; only it may remove.
    pub owner: Option<String>,
    /// How many MLS Commits the room has accepted: the epoch the next one
    /// must name.
    pub epoch: u64,
    /// Members sorted by name, each with its pinned key.
    pub members: Vec<SignedMember>,
    /// Names invited and not yet joined, sorted: a client pruning departed
    /// members must not mistake an invitee for one.
    pub invited: Vec<String>,
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
        let (_, found) = self.active(agent)?;
        key.verify(&register_key_payload(agent.as_str(), &key), signature)
            .map_err(|_| MailErr::InvalidSignature)?;
        let mut st = found.inner.lock();
        match st.key {
            Some(bound) if bound != key => Err(MailErr::KeyConflict {
                agent: found.name.to_string(),
            }),
            _ => {
                st.key = Some(key);
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
        match req.op {
            SignedOp::Create { description } => {
                let description =
                    Some(validate_description(description)?).filter(|d| !d.is_empty());
                state
                    .signed_create(&agent, room, key, counter, description)
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
                    Some(_) => {
                        state.signed_invite(&agent, room, key, counter, &invitee, invitee_key)
                    }
                }
                .map(SignedOutcome::Room)
            }
            SignedOp::Join => state
                .signed_join(&agent, room, key, counter)
                .map(SignedOutcome::Room),
            SignedOp::Leave => state
                .signed_leave(&agent, room, key, counter)
                .map(SignedOutcome::Left),
            SignedOp::Remove { target } => {
                let target = AgentName::parse(&target)?;
                state
                    .signed_remove(&agent, room, key, counter, &target)
                    .map(SignedOutcome::Room)
            }
            SignedOp::Welcome { target, welcome } => {
                let target = AgentName::parse(&target)?;
                state
                    .signed_store_welcome(&agent, room, key, counter, &target, welcome)
                    .map(SignedOutcome::Room)
            }
            SignedOp::Send { body } => {
                validate_body(&body)?;
                let envelope = Envelope::encode(counter, &key, &req.signature, &body);
                let json_len = super::log::json_len(&envelope);
                let commit_epoch = crate::signed::mls_commit_epoch(&body);
                let msg = state.send_room(
                    &agent,
                    room,
                    envelope.into_boxed_str(),
                    json_len,
                    self.now_ms(),
                    SendGate::Signed {
                        key: &key,
                        counter,
                        commit_epoch,
                    },
                )?;
                Ok(SignedOutcome::Sent(SendReceipt::from(&*msg)))
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
        key: PublicKey,
        counter: u64,
        description: Option<String>,
    ) -> Result<RoomInfo, MailErr> {
        let signed = SignedState::founded(Key::fold(&agent.name).as_str(), key, counter);
        self.create_room_with(agent, room, true, description, Some(signed))
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
        invitee: &Arc<Agent>,
        invitee_key: PublicKey,
    ) -> Result<RoomInfo, MailErr> {
        let found = self.signed_room(room)?;
        let mut st = found.inner.lock();
        let invitee_name = Key::fold(&invitee.name);
        st.require_signed_member(&found, agent, room, key)?;
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
        key: PublicKey,
        counter: u64,
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
            if !st.members.contains_key(name.as_str()) {
                let pinned = st
                    .signed
                    .as_ref()
                    .and_then(|s| s.invitations.get(name.as_str()).copied());
                match pinned {
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
                signed.bump(&found, key, counter)?;
                signed.invitations.remove(name.as_str());
                signed.welcomes.remove(name.as_str());
                signed.pins.insert(name.as_str().to_string(), key);
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
    ) -> Result<LeaveOutcome, MailErr> {
        let found = self.signed_room(room)?;
        let name = Key::fold(&agent.name);
        let emptied = {
            let mut st = found.inner.lock();
            st.require_signed_member(&found, agent, room, key)?;
            let signed = st.signed.as_mut().ok_or_else(|| not_a_signed_room(room))?;
            signed.bump(&found, key, counter)?;
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

    fn signed_remove(
        &self,
        agent: &Arc<Agent>,
        room: &RoomName,
        key: PublicKey,
        counter: u64,
        target: &AgentName,
    ) -> Result<RoomInfo, MailErr> {
        let found = self.signed_room(room)?;
        let name = Key::fold(&agent.name);
        let target_key = Key::fold(target.as_str());
        let info = {
            let mut st = found.inner.lock();
            st.require_signed_member(&found, agent, room, key)?;
            let is_owner = st.signed.as_ref().is_some_and(|s| s.owner == Some(key));
            // Only the owner removes others; it removes itself by leaving.
            if !is_owner || target_key.as_str() == name.as_str() {
                return Err(MailErr::NotAMember {
                    agent: agent.name.to_string(),
                    room: found.name.to_string(),
                });
            }
            if !st.members.contains_key(target_key.as_str())
                && !st.invited.contains(target_key.as_str())
            {
                return Err(MailErr::NotAMember {
                    agent: target.to_string(),
                    room: found.name.to_string(),
                });
            }
            let signed = st.signed.as_mut().ok_or_else(|| not_a_signed_room(room))?;
            signed.bump(&found, key, counter)?;
            st.drop_pin(target_key.as_str());
            st.drop_invitation(target_key.as_str());
            st.invited.remove(target_key.as_str());
            st.members.remove(target_key.as_str());
            st.info(&found)
        };
        if let Ok(gone) = self.agent(target.as_str(), target_key.as_str()) {
            gone.inner.lock().rooms.retain(|r| !Arc::ptr_eq(r, &found));
        }
        Ok(info)
    }

    fn signed_store_welcome(
        &self,
        agent: &Arc<Agent>,
        room: &RoomName,
        key: PublicKey,
        counter: u64,
        target: &AgentName,
        welcome: Vec<u8>,
    ) -> Result<RoomInfo, MailErr> {
        if welcome.is_empty() || welcome.len() > MAX_WELCOME_BYTES {
            return Err(MailErr::InvalidBody);
        }
        let found = self.signed_room(room)?;
        let target_key = Key::fold(target.as_str());
        let mut st = found.inner.lock();
        st.require_signed_member(&found, agent, room, key)?;
        let signed = st.signed.as_mut().ok_or_else(|| not_a_signed_room(room))?;
        if !signed.invitations.contains_key(target_key.as_str()) {
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
        if !signed.invitations.contains_key(name.as_str()) {
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
        Ok(SignedRoomInfo {
            room: found.name.to_string(),
            owner: signed.owner.map(|k| k.to_hex()),
            epoch: signed.epoch,
            members,
            invited,
        })
    }
}

fn not_a_signed_room(room: &RoomName) -> MailErr {
    MailErr::RoomNotFound {
        room: room.to_string(),
    }
}
