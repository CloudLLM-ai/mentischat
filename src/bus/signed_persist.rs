//! Persistence of signed rooms: the signed records themselves, replayed and
//! re-verified on restore.
//!
//! A signed room's state (members, pinned keys, owner, invitations, pending
//! Welcomes, epoch) is not saved as such. What is saved is the **journal**, the
//! signed operations that built it, each with the signer's key and signature,
//! plus the counter watermarks. On restore the journal is replayed from an empty
//! room through the same rules the live daemon applies, and every signature is
//! checked again, so a snapshot edited on disk cannot add a member, change an
//! owner, move an epoch or raise a pinned key: the first record that does not
//! verify or does not follow from the ones before rejects that room (and only
//! that room).
//!
//! The journal holds the membership operations, the Commit sends (which carry
//! the epoch chain; the epoch is rebuilt by counting them from 0) and the latest
//! Welcome per invitee. Ordinary messages are not journaled; their envelopes
//! are in the room's retained log, and each is re-verified too. Counter
//! watermarks are saved and restored as the maximum of what was saved and what
//! the verified records show, so a replay cannot be reopened by lowering one
//! number in the file unless every message that raised it has also left the
//! retained log (the residual risk, noted in `docs/SIGNED_ROOMS.md`).
//!
//! A room whose journal outgrows [`MAX_JOURNAL`] records is not saved at all,
//! the same fail-closed rule as before persistence existed: it ends with the
//! process rather than coming back partly.

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use super::signed::{SignedState, MAX_WATERMARKS};
use super::{validate_description, MAX_BODY_BYTES, MAX_ROOM_MEMBERS};
use crate::names::AgentName;
use crate::signed::{
    hex_decode_vec, hex_encode, mls_commit_epoch, Envelope, OpKind, PublicKey, Signature, SignedOp,
};

/// Most journal records a signed room keeps; past it the room is not saved.
pub(super) const MAX_JOURNAL: usize = 8192;
/// Most Welcome bytes a room holds (see `signed.rs`).
const MAX_ROOM_WELCOME_BYTES: usize = 1024 * 1024;

/// One signed operation as saved: who, under which key, at which counter, with
/// which signature, and the operation's own fields.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct OpRec {
    /// The signer's roster name.
    pub agent: String,
    /// The signer's public key, hex.
    pub key: String,
    /// The signed counter.
    pub counter: u64,
    /// The signature, hex.
    pub signature: String,
    /// The operation: `create`, `invite`, `join`, `leave`, `remove`, `welcome`
    /// or `send` (a Commit).
    pub kind: String,
    pub description: Option<String>,
    pub invitee: Option<String>,
    pub invitee_key: Option<String>,
    pub target: Option<String>,
    /// A Welcome, hex.
    pub welcome: Option<String>,
    /// A Commit message body.
    pub body: Option<String>,
}

impl OpRec {
    /// The record of `op` signed by `agent` (key `key`) at `counter`.
    pub(super) fn new(
        agent: &str,
        key: &PublicKey,
        counter: u64,
        signature: &Signature,
        op: &SignedOp,
    ) -> Self {
        let mut rec = Self {
            agent: agent.to_string(),
            key: key.to_hex(),
            counter,
            signature: signature.to_hex(),
            kind: op.kind().as_str().to_string(),
            description: None,
            invitee: None,
            invitee_key: None,
            target: None,
            welcome: None,
            body: None,
        };
        match op {
            SignedOp::Create { description } => rec.description = Some(description.clone()),
            SignedOp::Invite {
                invitee,
                invitee_key,
            } => {
                rec.invitee = Some(invitee.clone());
                rec.invitee_key = Some(invitee_key.to_hex());
            }
            SignedOp::Join | SignedOp::Leave => {}
            SignedOp::Remove { target } => rec.target = Some(target.clone()),
            SignedOp::Welcome { target, welcome } => {
                rec.target = Some(target.clone());
                rec.welcome = Some(hex_encode(welcome));
            }
            SignedOp::Send { body } => rec.body = Some(body.clone()),
        }
        rec
    }

    /// The operation, key and signature this record holds, or `None` when a
    /// field is missing or malformed.
    fn decode(&self) -> Option<(SignedOp, PublicKey, Signature)> {
        let key = self.key.parse().ok()?;
        let signature = self.signature.parse().ok()?;
        let kind: OpKind = self.kind.parse().ok()?;
        let text = |f: &Option<String>| f.clone();
        let op = match kind {
            OpKind::Create => SignedOp::Create {
                description: text(&self.description)?,
            },
            OpKind::Invite => SignedOp::Invite {
                invitee: text(&self.invitee)?,
                invitee_key: self.invitee_key.as_deref()?.parse().ok()?,
            },
            OpKind::Join => SignedOp::Join,
            OpKind::Leave => SignedOp::Leave,
            OpKind::Remove => SignedOp::Remove {
                target: text(&self.target)?,
            },
            OpKind::Welcome => SignedOp::Welcome {
                target: text(&self.target)?,
                welcome: hex_decode_vec(self.welcome.as_deref()?).ok()?,
            },
            OpKind::Send => SignedOp::Send {
                body: text(&self.body)?,
            },
            OpKind::RegisterKey | OpKind::PublishKeyPackages => return None,
        };
        Some((op, key, signature))
    }
}

/// A signed room as saved.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct SignedRec {
    /// The signed operations that built the room, in order.
    pub journal: Vec<OpRec>,
    /// Highest counter each key has signed, `(key hex, counter)`.
    pub watermarks: Vec<(String, u64)>,
}

/// A signed room rebuilt from its record.
pub(super) struct Replayed {
    /// The rebuilt state; the caller trims it to the members it seats.
    pub state: SignedState,
    /// The description the room was created with.
    pub description: Option<String>,
}

impl SignedState {
    /// The record to save, or `None` when the journal overflowed.
    pub(super) fn to_rec(&self) -> Option<SignedRec> {
        if self.overflow {
            return None;
        }
        let mut watermarks: Vec<(String, u64)> = self
            .watermarks
            .iter()
            .map(|(k, c)| (k.to_hex(), *c))
            .collect();
        watermarks.sort_unstable();
        Some(SignedRec {
            journal: self.journal.clone(),
            watermarks,
        })
    }

    /// Append `entry` to the journal, or mark the room too long to save.
    pub(super) fn record(&mut self, entry: &OpRec) {
        if self.journal.len() >= MAX_JOURNAL {
            self.overflow = true;
        } else {
            self.journal.push(entry.clone());
        }
    }

    /// Drop the journaled Welcomes for `target` (folded): they are superseded or
    /// consumed, and the state they would rebuild is gone.
    pub(super) fn forget_welcomes(&mut self, target: &str) {
        self.journal.retain(|r| {
            !(r.kind == "welcome"
                && r.target
                    .as_deref()
                    .is_some_and(|t| t.eq_ignore_ascii_case(target)))
        });
    }
}

/// Rebuild the room `room` from `rec`, verifying everything, or say why it
/// cannot be trusted. `retained` is the room's retained messages as
/// `(sender, body)`; each must be a validly signed envelope by a key the
/// journal knows.
pub(super) fn replay(
    room: &str,
    rec: &SignedRec,
    retained: &[(&str, &str)],
) -> Result<Replayed, &'static str> {
    if rec.journal.len() > MAX_JOURNAL {
        return Err("journal too long");
    }
    let mut state = SignedState::empty();
    let mut description = None;
    let mut seen_keys: HashSet<PublicKey> = HashSet::new();
    for (index, record) in rec.journal.iter().enumerate() {
        let (op, key, signature) = record.decode().ok_or("malformed record")?;
        let agent = AgentName::parse(&record.agent).map_err(|_| "bad agent name")?;
        let payload = op.payload(room, agent.as_str(), record.counter);
        key.verify(&payload, &signature)
            .map_err(|_| "signature does not verify")?;
        if state
            .watermarks
            .get(&key)
            .is_some_and(|&last| record.counter <= last)
        {
            return Err("counter does not increase");
        }
        let name = agent.folded();
        if let SignedOp::Create { description: d } = &op {
            if index != 0 {
                return Err("create is not first");
            }
            description = Some(validate_description(d.clone()).map_err(|_| "bad description")?)
                .filter(|d| !d.is_empty());
            state.owner = Some(key);
            state.pins.insert(name, key);
        } else if index == 0 {
            return Err("first record is not create");
        } else {
            apply(&mut state, &name, &op, key)?;
        }
        state.watermarks.insert(key, record.counter);
        seen_keys.insert(key);
        state.journal.push(record.clone());
        if state.watermarks.len() > MAX_WATERMARKS
            || state.pins.len() + state.invitations.len() > MAX_ROOM_MEMBERS
        {
            return Err("room over its limits");
        }
    }
    if state.pins.is_empty() {
        return Err("room has no members");
    }
    verify_retained(room, retained, &seen_keys, &mut state)?;
    merge_saved_watermarks(&mut state, &rec.watermarks)?;
    Ok(Replayed { state, description })
}

/// The rules of each operation after the first, as the live daemon applies them.
fn apply(
    state: &mut SignedState,
    name: &str,
    op: &SignedOp,
    key: PublicKey,
) -> Result<(), &'static str> {
    let pinned = state.pins.get(name) == Some(&key);
    match op {
        SignedOp::Create { .. } => Err("second create"),
        SignedOp::Invite {
            invitee,
            invitee_key,
        } => {
            let invitee = AgentName::parse(invitee)
                .map_err(|_| "bad invitee")?
                .folded();
            if !pinned || state.pins.contains_key(&invitee) {
                return Err("invite by a non-member or of a member");
            }
            state.invitations.insert(invitee, *invitee_key);
            Ok(())
        }
        SignedOp::Join => {
            if state.pins.contains_key(name) || state.invitations.get(name) != Some(&key) {
                return Err("join without a matching invitation");
            }
            state.invitations.remove(name);
            state.welcomes.remove(name);
            state.pins.insert(name.to_string(), key);
            Ok(())
        }
        SignedOp::Leave => {
            if !pinned {
                return Err("leave by a non-member");
            }
            state.pins.remove(name);
            if state.owner == Some(key) {
                state.owner = None;
            }
            Ok(())
        }
        SignedOp::Remove { target } => {
            let target = AgentName::parse(target).map_err(|_| "bad target")?.folded();
            let known = state.pins.contains_key(&target) || state.invitations.contains_key(&target);
            if !pinned || state.owner != Some(key) || target == name || !known {
                return Err("remove not allowed");
            }
            state.pins.remove(&target);
            state.invitations.remove(&target);
            state.welcomes.remove(&target);
            Ok(())
        }
        SignedOp::Welcome { target, welcome } => {
            let target = AgentName::parse(target).map_err(|_| "bad target")?.folded();
            let known = state.pins.contains_key(&target) || state.invitations.contains_key(&target);
            if !pinned || !known || welcome.is_empty() {
                return Err("welcome not allowed");
            }
            state.welcomes.insert(target, welcome.clone());
            let held: usize = state.welcomes.values().map(Vec::len).sum();
            if held > MAX_ROOM_WELCOME_BYTES {
                return Err("welcomes over their limit");
            }
            Ok(())
        }
        SignedOp::Send { body } => {
            let epoch = mls_commit_epoch(body);
            if !pinned || epoch != Some(state.epoch) || body.len() > MAX_BODY_BYTES {
                return Err("commit out of chain");
            }
            state.epoch += 1;
            Ok(())
        }
    }
}

/// Every retained message must be an envelope whose signature verifies for
/// its sender and room, made by a key that appears in the journal; its counter
/// raises the key's watermark.
fn verify_retained(
    room: &str,
    retained: &[(&str, &str)],
    known: &HashSet<PublicKey>,
    state: &mut SignedState,
) -> Result<(), &'static str> {
    for (from, body) in retained {
        let env = Envelope::parse(body).map_err(|_| "retained message is not an envelope")?;
        if !known.contains(&env.key) {
            return Err("retained message by an unknown key");
        }
        env.verify(room, from)
            .map_err(|_| "retained message signature does not verify")?;
        let mark = state.watermarks.entry(env.key).or_insert(env.counter);
        *mark = (*mark).max(env.counter);
    }
    Ok(())
}

/// Fold the saved watermarks in, never lowering what the records proved.
fn merge_saved_watermarks(
    state: &mut SignedState,
    saved: &[(String, u64)],
) -> Result<(), &'static str> {
    let mut merged: HashMap<PublicKey, u64> = HashMap::new();
    for (key, counter) in saved {
        let key: PublicKey = key.parse().map_err(|_| "bad watermark key")?;
        merged.insert(key, *counter);
    }
    for (key, counter) in merged {
        let mark = state.watermarks.entry(key).or_insert(counter);
        *mark = (*mark).max(counter);
    }
    if state.watermarks.len() > MAX_WATERMARKS {
        return Err("too many watermarks");
    }
    Ok(())
}
