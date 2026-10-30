//! The mailbox state and every operation on it. Nothing here locks, waits or
//! does I/O: [`crate::bus::Bus`] takes its lock, calls one of these, and
//! releases the lock before it builds a response.
//!
//! The pieces, and who owns what:
//!
//! - [`Agent`]: a roster entry and its unread queue (`Arc<Msg>` in send
//!   order), so a pull and the per-recipient cap are O(1).
//! - [`Convo`]: the log of everything two agents exchanged, found by the
//!   pair's folded names. History reads these, newest first.
//! - `read_q`: every read message in the order it was read, so the oldest
//!   read mail (to prune at the global cap) and the expired mail (to sweep)
//!   are both at the front.
//! - `holds`: released names, with what their previous holder needs to
//!   resume them.
//!
//! Names are folded (ASCII-lowercased) for every key and compared without
//! regard to case; entries keep the spelling they were registered with.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use super::log::{Convo, Msg};
use super::park::Signal;
use super::{MailErr, MAX_BATCH, MAX_PENDING_PER_AGENT, MAX_RETURN_BYTES, MAX_TOTAL_MESSAGES};
use crate::errors::TakenReason;

/// A released name and what its previous holder needs to resume it.
#[derive(Debug, Clone)]
pub(super) struct Hold {
    /// Milliseconds since the Unix epoch when anyone may take the name.
    pub until_ms: u64,
    /// The previous registration's start; a reclaim keeps it, so the agent's
    /// sent mail is still in its `history`.
    pub registered_ms: u64,
}

/// A roster entry.
#[derive(Debug)]
pub(super) struct Agent {
    /// The name as registered.
    pub name: Arc<str>,
    pub provider: Option<String>,
    pub description: Option<String>,
    /// Start of this registration; `history` shows only mail created since.
    pub registered_ms: u64,
    pub last_seen_ms: u64,
    /// Unread mail addressed to this agent, oldest first.
    pub unread: VecDeque<Arc<Msg>>,
    /// What a send pokes and a pull parks on.
    pub signal: Arc<Signal>,
}

impl Agent {
    pub(super) fn new(
        name: Arc<str>,
        provider: Option<String>,
        description: Option<String>,
        registered_ms: u64,
        now_ms: u64,
    ) -> Self {
        Self {
            name,
            provider,
            description,
            registered_ms,
            last_seen_ms: now_ms,
            unread: VecDeque::new(),
            signal: Arc::new(Signal::default()),
        }
    }
}

/// The whole mailbox. See the module docs.
#[derive(Debug, Default)]
pub(super) struct State {
    /// The roster by folded name.
    pub agents: HashMap<String, Agent>,
    /// Released names by folded name.
    pub holds: HashMap<String, Hold>,
    /// Conversations by [`convo_key`].
    pub convos: HashMap<String, Convo>,
    /// Read messages in read order. May hold dead entries until they reach
    /// the front or the owner leaves.
    pub read_q: VecDeque<Arc<Msg>>,
    /// Live messages, read and unread.
    pub total: usize,
    /// Live unread messages.
    pub unread: usize,
    /// Id of the next message.
    pub next_id: u64,
    /// Messages accepted since the daemon started (or the snapshot it was
    /// restored from was written).
    pub messages_sent: u64,
    /// Set once the bus is closed; nothing is accepted after.
    pub closed: bool,
}

/// The key of the conversation between two agents: their folded names, the
/// smaller first. Names never hold a newline, so the pair cannot be confused.
pub(super) fn convo_key(a: &str, b: &str) -> String {
    let (lo, hi) = if a <= b { (a, b) } else { (b, a) };
    format!("{lo}\n{hi}")
}

/// True when `key` is a conversation `folded` takes part in.
fn convo_has(key: &str, folded: &str) -> bool {
    key.split('\n').any(|side| side == folded)
}

/// How many of `sizes`, in order, fit [`MAX_BATCH`] and [`MAX_RETURN_BYTES`].
/// The first always fits: progress must be possible.
///
/// Size is each message's serialized JSON, not its raw body: escaping can
/// make a body several times longer on the wire (a control character becomes
/// six bytes), and the wire size is what has to stay under client truncation
/// limits.
pub(super) fn fitting(sizes: impl Iterator<Item = usize>) -> usize {
    let mut take = 0usize;
    let mut bytes = 0usize;
    for size in sizes.take(MAX_BATCH as usize) {
        if take > 0 && bytes + size > MAX_RETURN_BYTES {
            break;
        }
        bytes += size;
        take += 1;
    }
    take
}

impl State {
    /// A mailbox with nothing in it.
    pub(super) fn new() -> Self {
        Self {
            next_id: 1,
            ..Self::default()
        }
    }

    /// Roster names, sorted without regard to case.
    pub(super) fn roster_sorted(&self) -> Vec<String> {
        self.agents_sorted().map(|a| a.name.to_string()).collect()
    }

    /// Roster entries, sorted by folded name.
    pub(super) fn agents_sorted(&self) -> impl Iterator<Item = &Agent> {
        let mut keyed: Vec<(&String, &Agent)> = self.agents.iter().collect();
        keyed.sort_unstable_by(|a, b| a.0.cmp(b.0));
        keyed.into_iter().map(|(_, agent)| agent)
    }

    /// The `unregistered` error for `name`, with the roster.
    pub(super) fn unregistered(&self, name: &str) -> MailErr {
        MailErr::Unregistered {
            agent: name.to_string(),
            known: self.roster_sorted(),
        }
    }

    /// `name_taken` for `agent`, with free names to pick instead.
    pub(super) fn name_taken(&self, agent: String, reason: TakenReason, now_ms: u64) -> MailErr {
        MailErr::NameTaken {
            agent,
            reason,
            suggestions: self.free_suggestions(now_ms),
        }
    }

    /// Suggested names that are neither on the roster nor held at `now_ms`.
    pub(super) fn free_suggestions(&self, now_ms: u64) -> Vec<String> {
        let held = self
            .holds
            .iter()
            .filter(|(_, h)| h.until_ms > now_ms)
            .map(|(key, _)| key);
        let taken: HashSet<&String> = self.agents.keys().chain(held).collect();
        crate::names::suggestions(|n| taken.contains(&n.to_ascii_lowercase()))
    }

    /// Accept `body` from `from` for `to`, which must both be registered
    /// (`from` is checked first). Enforces the per-recipient and global
    /// caps, makes room at the global cap by deleting the oldest read
    /// message, refreshes the sender's `last_seen`, and wakes the oldest pull
    /// parked on the recipient. Nothing changes when it fails.
    ///
    /// `body_json_len` is the body's JSON-escaped size, measured by the
    /// caller before taking the lock.
    pub(super) fn send(
        &mut self,
        from: &str,
        to: &str,
        body: Box<str>,
        body_json_len: usize,
        now_ms: u64,
    ) -> Result<Arc<Msg>, MailErr> {
        let (from_key, to_key) = (from.to_ascii_lowercase(), to.to_ascii_lowercase());
        let Some(sender) = self.agents.get(&from_key) else {
            return Err(self.unregistered(from));
        };
        let from_name = sender.name.clone();
        let Some(recipient) = self.agents.get(&to_key) else {
            return Err(self.unregistered(to));
        };
        let pending = recipient.unread.len();
        if pending >= MAX_PENDING_PER_AGENT as usize {
            return Err(MailErr::QueueFull {
                to: recipient.name.to_string(),
                pending: pending as i64,
            });
        }
        if self.unread >= MAX_TOTAL_MESSAGES as usize {
            return Err(MailErr::StorageFull {
                pending: self.unread as i64,
            });
        }
        let to_name = recipient.name.clone();
        // Unread mail is under the cap here, so there is read mail to prune.
        if self.total >= MAX_TOTAL_MESSAGES as usize {
            self.prune_oldest_read();
        }
        let id = self.next_id;
        self.next_id += 1;
        let msg = Arc::new(Msg::new(
            id,
            from_name,
            to_name,
            body,
            body_json_len,
            now_ms,
        ));
        self.append(&from_key, &to_key, &msg, now_ms);
        Ok(msg)
    }

    /// File `msg` in its conversation, queue it unread for its recipient,
    /// count it, wake a parked pull, and stamp the sender's `last_seen`
    /// (sending is activity: an agent that only sends must not age off the
    /// roster). Both agents are on the roster.
    fn append(&mut self, from_key: &str, to_key: &str, msg: &Arc<Msg>, now_ms: u64) {
        self.convos
            .entry(convo_key(from_key, to_key))
            .or_default()
            .push(msg.clone());
        self.total += 1;
        self.unread += 1;
        self.messages_sent += 1;
        if let Some(recipient) = self.agents.get_mut(to_key) {
            recipient.unread.push_back(msg.clone());
            if recipient.signal.parked() > 0 {
                recipient.signal.notify.notify_one();
            }
        }
        if let Some(sender) = self.agents.get_mut(from_key) {
            sender.last_seen_ms = now_ms;
        }
    }

    /// Delete the message that has been read longest.
    fn prune_oldest_read(&mut self) {
        while let Some(oldest) = self.read_q.pop_front() {
            if oldest.kill() {
                self.total -= 1;
                return;
            }
        }
    }

    /// Stamp the agent under `key` (a folded name) as seen at `now_ms`.
    /// False when it is not on the roster.
    pub(super) fn touch(&mut self, key: &str, now_ms: u64) -> bool {
        self.agents
            .get_mut(key)
            .map(|agent| agent.last_seen_ms = now_ms)
            .is_some()
    }

    /// Take the next batch of unread mail for the agent under `key` (a
    /// folded name), marking it read at `now_ms`; `None` when nothing is
    /// unread or the agent is not on the roster. Returns the batch and how
    /// many unread messages remain.
    ///
    /// Up to [`MAX_BATCH`] messages within [`MAX_RETURN_BYTES`] of
    /// serialized JSON, the first always included. Taking and marking happen
    /// in this one call, so a pull hands each message out exactly once.
    pub(super) fn take(&mut self, key: &str, now_ms: u64) -> Option<(Vec<Arc<Msg>>, usize)> {
        let agent = self.agents.get_mut(key)?;
        let take = fitting(agent.unread.iter().map(|m| m.page_len()));
        if take == 0 {
            return None;
        }
        let batch: Vec<Arc<Msg>> = agent.unread.drain(..take).collect();
        let remaining = agent.unread.len();
        for msg in &batch {
            msg.read_ms
                .store(now_ms, std::sync::atomic::Ordering::Relaxed);
        }
        self.read_q.extend(batch.iter().cloned());
        self.unread -= take;
        Some((batch, remaining))
    }

    /// Delete read mail read before `cutoff_ms`. Returns how many.
    pub(super) fn expire_read(&mut self, cutoff_ms: u64) -> i64 {
        let mut expired = 0;
        while let Some(front) = self.read_q.front() {
            if !front.is_dead() && front.read_at_ms().is_some_and(|ms| ms >= cutoff_ms) {
                break;
            }
            if let Some(msg) = self.read_q.pop_front() {
                if msg.kill() {
                    self.total -= 1;
                    expired += 1;
                }
            }
        }
        expired
    }

    /// Remove the agent under `key` from the roster with all the mail
    /// addressed to it, read or not, wake its parked pulls (they fail
    /// `unregistered`) and hold the name until `until_ms`. Messages it sent
    /// stay with their recipients. Returns the agent and how many messages
    /// were discarded; `None` when it is not on the roster.
    pub(super) fn remove_agent(&mut self, key: &str, until_ms: u64) -> Option<(Agent, i64)> {
        let agent = self.agents.remove(key)?;
        let discarded = self.discard_mail_to(key);
        self.unread -= agent.unread.len();
        self.total -= discarded;
        self.holds.insert(
            key.to_string(),
            Hold {
                until_ms,
                registered_ms: agent.registered_ms,
            },
        );
        agent.signal.notify.notify_waiters();
        Some((agent, discarded as i64))
    }

    /// Kill every live message addressed to the folded name `key` and drop
    /// the entries that leaves behind. Returns how many it killed.
    fn discard_mail_to(&mut self, key: &str) -> usize {
        let mut discarded = 0;
        let mut read_discarded = false;
        for (_, convo) in self.convos.iter_mut().filter(|(k, _)| convo_has(k, key)) {
            for msg in convo.live().filter(|m| m.to.eq_ignore_ascii_case(key)) {
                if msg.kill() {
                    discarded += 1;
                    read_discarded |= msg.read_at_ms().is_some();
                }
            }
            convo.compact();
        }
        self.convos.retain(|_, convo| !convo.is_empty());
        if read_discarded {
            self.read_q.retain(|m| !m.is_dead());
        }
        discarded
    }

    /// Drop dead entries from every conversation, and the conversations left
    /// empty.
    pub(super) fn compact_convos(&mut self) {
        self.convos.retain(|_, convo| {
            convo.compact();
            !convo.is_empty()
        });
        self.read_q.retain(|m| !m.is_dead());
    }

    /// Drop everything: roster, mail, holds. Wakes every parked pull (they
    /// fail `unregistered`, or `shutting_down` once the bus is closed).
    /// Returns `(messages, agents)` removed.
    pub(super) fn clear(&mut self) -> (i64, i64) {
        let counts = (self.total as i64, self.agents.len() as i64);
        for agent in self.agents.values() {
            agent.signal.notify.notify_waiters();
        }
        self.agents.clear();
        self.holds.clear();
        self.convos.clear();
        self.read_q.clear();
        self.total = 0;
        self.unread = 0;
        counts
    }
}
