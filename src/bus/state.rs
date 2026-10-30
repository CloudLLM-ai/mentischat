//! The mailbox state and every operation on it. Nothing here waits or does
//! I/O: [`crate::bus::Bus`] calls one of these, and builds its response from
//! the `Arc`'d results after every lock is released.
//!
//! The pieces, and the lock that guards each:
//!
//! - **Roster** (a [`Sharded`] map of `RwLock`s): agents by folded name. The
//!   hot path read-locks one shard to turn a name into an [`Agent`] handle, so
//!   agents that hash apart never touch the same lock word.
//! - **`holds`** (`Mutex`): released names. Registering, removing and
//!   sweeping agents all take it first, which serializes membership changes
//!   (the roster-full check, a name's hold) without involving the hot path.
//! - **[`Agent`]** (one `Mutex` each): its profile, its unread queue
//!   (`Arc<Msg>` in send order, so a pull and the per-recipient cap are O(1))
//!   and its read queue. Its [`Signal`] is lock-free.
//! - **Conversations** (a [`Sharded`] map of [`Convo`], one `Mutex` each): the
//!   log of everything two agents exchanged, found by the pair's folded
//!   names. History reads these, newest first. A room log (later) is the same
//!   shape.
//! - **Read queues** (inside each agent's lock): the mail an agent has read,
//!   in the order it was read, with the read time and id of the front
//!   published in atomics, so the oldest read mail anywhere (pruned at the
//!   global cap) is found by comparing those, and expired mail (swept) is at
//!   each front.
//! - **Counters** (padded atomics): live messages, live unread messages and
//!   the next message id carry the global caps and the order. Messages sent
//!   are counted per sending agent under the lock the send takes anyway.
//!
//! # Lock order
//!
//! Every path takes locks in this order, skipping the ones it does not need,
//! and never holds a lock across an `.await`:
//!
//! 1. `holds`
//! 2. a roster shard
//! 3. an agent
//! 4. a conversation-map shard
//! 5. a conversation log
//!
//! Two agents are never locked at once, nor two conversation logs, nor two
//! shards of one map. A send locks its recipient, then the conversation, and
//! touches the sender (and prunes at the global cap, which locks whichever
//! agent has read longest) only after releasing the recipient. A message's id
//! is taken under its conversation's lock, so every log is sorted by id, and
//! under its recipient's lock, so every unread queue is too.
//!
//! Names are folded (ASCII-lowercased) for every key and compared without
//! regard to case; entries keep the spelling they were registered with.

use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::{BuildHasher, Hasher};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering::Relaxed};
use std::sync::Arc;

use parking_lot::{Mutex, RwLock};

use super::log::{Convo, Msg};
use super::park::Signal;
use super::{MailErr, MAX_BATCH, MAX_PENDING_PER_AGENT, MAX_RETURN_BYTES, MAX_TOTAL_MESSAGES};
use crate::errors::TakenReason;

/// Shards per map. Sixteen is enough that a handful of busy conversations
/// rarely share one, and few enough that walking every shard stays cheap.
const SHARDS: usize = 16;

/// A value alone on its cache lines, so a lock or counter written by one
/// thread does not slow reads of its neighbors.
#[derive(Debug, Default)]
#[repr(align(128))]
pub(super) struct Padded<T>(pub T);

/// FNV-1a: a hash that costs a few cycles on the short names that key every
/// map here. The names are roster names a local process chose, capped at 256,
/// so there is nothing for a collision attack to gain and no reason to pay for
/// SipHash on every lookup.
#[derive(Debug, Default, Clone, Copy)]
pub(super) struct Fnv;

/// The state of one [`Fnv`] hash.
#[derive(Debug)]
pub(super) struct FnvHasher(u64);

impl BuildHasher for Fnv {
    type Hasher = FnvHasher;

    fn build_hasher(&self) -> FnvHasher {
        FnvHasher(0xcbf2_9ce4_8422_2325)
    }
}

impl Hasher for FnvHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.0 = (self.0 ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3);
        }
    }
}

type Shard<V> = RwLock<HashMap<String, V, Fnv>>;

/// A map split by key hash into independently locked shards.
pub(super) struct Sharded<V> {
    shards: Box<[Padded<Shard<V>>]>,
}

impl<V: Clone> Sharded<V> {
    fn new() -> Self {
        Self {
            shards: (0..SHARDS).map(|_| Padded(RwLock::default())).collect(),
        }
    }

    fn shard(&self, key: &str) -> &Shard<V> {
        // The map inside hashes the same way, so mix the high bits in: the
        // low ones pick a slot there.
        let h = Fnv.hash_one(key);
        &self.shards[(h >> 32) as usize % SHARDS].0
    }

    /// The value under `key`.
    pub(super) fn get(&self, key: &str) -> Option<V> {
        self.shard(key).read().get(key).cloned()
    }

    /// The value under `key`, made by `make` and inserted if there is none.
    pub(super) fn get_or_insert_with(&self, key: &str, make: impl FnOnce() -> V) -> V {
        if let Some(found) = self.get(key) {
            return found;
        }
        self.shard(key)
            .write()
            .entry(key.to_string())
            .or_insert_with(make)
            .clone()
    }

    pub(super) fn insert(&self, key: String, value: V) {
        self.shard(&key).write().insert(key, value);
    }

    pub(super) fn remove(&self, key: &str) -> Option<V> {
        self.shard(key).write().remove(key)
    }

    /// Every entry, one shard at a time (so not a consistent cut).
    pub(super) fn entries(&self) -> Vec<(String, V)> {
        self.shards
            .iter()
            .flat_map(|s| {
                let map = s.0.read();
                map.iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    /// Every value whose key `keep` accepts.
    pub(super) fn values_where(&self, keep: impl Fn(&str) -> bool) -> Vec<V> {
        self.shards
            .iter()
            .flat_map(|s| {
                let map = s.0.read();
                map.iter()
                    .filter(|(k, _)| keep(k))
                    .map(|(_, v)| v.clone())
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    /// The value with the smallest `rank`, ignoring those that rank `None`.
    /// Walks the shards without copying anything but the winner.
    pub(super) fn min_by_rank<R: Ord>(&self, rank: impl Fn(&V) -> Option<R>) -> Option<V> {
        let mut best: Option<(R, V)> = None;
        for shard in self.shards.iter() {
            for value in shard.0.read().values() {
                let Some(r) = rank(value) else { continue };
                if best.as_ref().is_none_or(|(b, _)| r < *b) {
                    best = Some((r, value.clone()));
                }
            }
        }
        best.map(|(_, v)| v)
    }

    pub(super) fn len(&self) -> usize {
        self.shards.iter().map(|s| s.0.read().len()).sum()
    }

    /// Remove every entry `keep` rejects, shard by shard.
    pub(super) fn retain(&self, mut keep: impl FnMut(&V) -> bool) {
        for shard in self.shards.iter() {
            shard.0.write().retain(|_, v| keep(v));
        }
    }

    /// Remove and return every value.
    pub(super) fn drain(&self) -> Vec<V> {
        self.shards
            .iter()
            .flat_map(|s| s.0.write().drain().map(|(_, v)| v).collect::<Vec<_>>())
            .collect()
    }
}

/// A released name and what its previous holder needs to resume it.
#[derive(Debug, Clone)]
pub(super) struct Hold {
    /// Milliseconds since the Unix epoch when anyone may take the name.
    pub until_ms: u64,
    /// The previous registration's start; a reclaim keeps it, so the agent's
    /// sent mail is still in its `history`.
    pub registered_ms: u64,
}

/// What changes about an agent while it is on the roster.
#[derive(Debug)]
pub(super) struct AgentState {
    pub provider: Option<String>,
    pub description: Option<String>,
    /// Start of this registration; `history` shows only mail created since.
    pub registered_ms: u64,
    pub last_seen_ms: u64,
    /// Unread mail addressed to this agent, oldest first.
    pub unread: VecDeque<Arc<Msg>>,
    /// Mail this agent has read, in the order it was read.
    pub read: VecDeque<Arc<Msg>>,
    /// Messages this agent has sent.
    pub sent: u64,
    /// Set once the agent has left the roster. A send that looked the agent
    /// up before that sees it here and fails `unregistered`.
    pub removed: bool,
}

/// A roster entry.
#[derive(Debug)]
pub(super) struct Agent {
    /// The name as registered.
    pub name: Arc<str>,
    /// Which registration this is; mail carries it (see `Msg::to_gen`).
    pub gen: u64,
    /// What a send pokes and a pull parks on.
    pub signal: Arc<Signal>,
    /// Read time and id of the front of the read queue, [`NEVER`] when it is
    /// empty. Written under the agent's lock, readable without it.
    pub oldest_read: (AtomicU64, AtomicU64),
    pub inner: Mutex<AgentState>,
}

/// [`Agent::oldest_read`] of an agent with no read mail.
const NEVER: u64 = u64::MAX;

impl Agent {
    pub(super) fn new(
        name: Arc<str>,
        gen: u64,
        provider: Option<String>,
        description: Option<String>,
        registered_ms: u64,
        now_ms: u64,
    ) -> Self {
        Self {
            name,
            gen,
            signal: Arc::new(Signal::default()),
            oldest_read: (AtomicU64::new(NEVER), AtomicU64::new(NEVER)),
            inner: Mutex::new(AgentState {
                provider,
                description,
                registered_ms,
                last_seen_ms: now_ms,
                unread: VecDeque::new(),
                read: VecDeque::new(),
                sent: 0,
                removed: false,
            }),
        }
    }

    /// Publish the read time of the front of `st.read`; call after changing
    /// the queue, still holding the lock `st` came from.
    pub(super) fn publish_oldest_read(&self, st: &AgentState) {
        let front = st.read.front();
        let read_ms = front.and_then(|m| m.read_at_ms());
        self.oldest_read.0.store(read_ms.unwrap_or(NEVER), Relaxed);
        self.oldest_read
            .1
            .store(front.map_or(NEVER, |m| m.id), Relaxed);
    }
}

/// Roster entries sorted by folded name.
pub(super) fn sorted(mut entries: Vec<(String, Arc<Agent>)>) -> Vec<Arc<Agent>> {
    entries.sort_unstable_by(|a, b| a.0.cmp(&b.0));
    entries.into_iter().map(|(_, agent)| agent).collect()
}

/// Longest folded name: [`crate::names::AgentName`] caps names at 63 bytes.
const NAME_MAX: usize = 63;

/// A folded name or a conversation key built on the stack, so the hot path
/// looks names up without allocating.
#[derive(Debug, Clone, Copy)]
pub(super) struct Key {
    buf: [u8; 2 * NAME_MAX + 1],
    len: usize,
}

impl Key {
    /// The folded (ASCII-lowercased) form of a validated name.
    pub(super) fn fold(name: &str) -> Self {
        let mut key = Self {
            buf: [0; 2 * NAME_MAX + 1],
            len: name.len().min(NAME_MAX),
        };
        key.buf[..key.len].copy_from_slice(&name.as_bytes()[..key.len]);
        key.buf[..key.len].make_ascii_lowercase();
        key
    }

    /// The key as text.
    pub(super) fn as_str(&self) -> &str {
        // Only ASCII-lowercased prefixes of valid UTF-8 names are stored, and
        // names are ASCII, so the cut never splits a character.
        std::str::from_utf8(&self.buf[..self.len]).unwrap_or_default()
    }
}

/// The key of the conversation between two agents: their folded names, the
/// smaller first. Names never hold a newline, so the pair cannot be confused.
pub(super) fn convo_key(a: &Key, b: &Key) -> Key {
    let (lo, hi) = if a.as_str() <= b.as_str() {
        (a, b)
    } else {
        (b, a)
    };
    let mut key = Key {
        buf: [0; 2 * NAME_MAX + 1],
        len: lo.len + 1 + hi.len,
    };
    key.buf[..lo.len].copy_from_slice(&lo.buf[..lo.len]);
    key.buf[lo.len] = b'\n';
    key.buf[lo.len + 1..key.len].copy_from_slice(&hi.buf[..hi.len]);
    key
}

/// True when `key` is a conversation `folded` takes part in.
pub(super) fn convo_has(key: &str, folded: &str) -> bool {
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

/// A message on its way: what `send` hands down once both names resolve.
struct Outgoing<'a> {
    /// The conversation it belongs to, see [`convo_key`].
    key: &'a str,
    body: Box<str>,
    /// The body's JSON-escaped size.
    body_json_len: usize,
    now_ms: u64,
}

/// Mail taken by a pull: the batch and how many unread messages remain.
pub(super) type Batch = (Vec<Arc<Msg>>, usize);

/// The whole mailbox. See the module docs for what each lock guards and the
/// order to take them in.
pub(super) struct State {
    /// Agents by folded name.
    pub roster: Sharded<Arc<Agent>>,
    /// Released names by folded name; also the lock that serializes changes
    /// to the roster's membership.
    pub holds: Mutex<HashMap<String, Hold>>,
    /// Conversations by [`convo_key`].
    pub convos: Sharded<Arc<Convo>>,
    /// Live messages, read and unread.
    pub total: Padded<AtomicUsize>,
    /// Live unread messages.
    pub unread: Padded<AtomicUsize>,
    /// Id of the next message.
    pub next_id: Padded<AtomicU64>,
    /// Generation of the next registration.
    pub next_gen: AtomicU64,
    /// Messages sent by agents that have since left the roster (or by the
    /// run a snapshot came from); the live ones count their own.
    pub sent_by_gone: AtomicU64,
    /// Set once the bus is closed; nothing is accepted after.
    pub closed: AtomicBool,
}

impl State {
    /// A mailbox with nothing in it.
    pub(super) fn new() -> Self {
        Self {
            roster: Sharded::new(),
            holds: Mutex::default(),
            convos: Sharded::new(),
            total: Padded(AtomicUsize::new(0)),
            unread: Padded(AtomicUsize::new(0)),
            next_id: Padded(AtomicU64::new(1)),
            next_gen: AtomicU64::new(1),
            sent_by_gone: AtomicU64::new(0),
            closed: AtomicBool::new(false),
        }
    }

    /// Roster entries sorted by folded name.
    pub(super) fn agents_sorted(&self) -> Vec<Arc<Agent>> {
        sorted(self.roster.entries())
    }

    /// Roster names, sorted without regard to case.
    pub(super) fn names_sorted(&self) -> Vec<String> {
        self.agents_sorted()
            .iter()
            .map(|a| a.name.to_string())
            .collect()
    }

    /// The `unregistered` error for `name`, with the roster. Takes roster
    /// locks: call it with none of the later locks held.
    pub(super) fn unregistered(&self, name: &str) -> MailErr {
        MailErr::Unregistered {
            agent: name.to_string(),
            known: self.names_sorted(),
        }
    }

    /// `name_taken` for `agent`, with free names to pick instead. `holds` is
    /// the caller's guard.
    pub(super) fn name_taken(
        &self,
        holds: &HashMap<String, Hold>,
        agent: String,
        reason: TakenReason,
        now_ms: u64,
    ) -> MailErr {
        MailErr::NameTaken {
            agent,
            reason,
            suggestions: self.free_suggestions(holds, now_ms),
        }
    }

    /// Suggested names that are neither on the roster nor held at `now_ms`.
    pub(super) fn free_suggestions(
        &self,
        holds: &HashMap<String, Hold>,
        now_ms: u64,
    ) -> Vec<String> {
        let held = holds
            .iter()
            .filter(|(_, h)| h.until_ms > now_ms)
            .map(|(key, _)| key.clone());
        let taken: HashSet<String> = self
            .roster
            .entries()
            .into_iter()
            .map(|(key, _)| key)
            .chain(held)
            .collect();
        crate::names::suggestions(|n| taken.contains(&n.to_ascii_lowercase()))
    }

    /// The agent under `key` (a folded name), or `unregistered` for `name`.
    pub(super) fn agent(&self, name: &str, key: &str) -> Result<Arc<Agent>, MailErr> {
        self.roster.get(key).ok_or_else(|| self.unregistered(name))
    }

    /// Messages accepted since the daemon started.
    pub(super) fn messages_sent(&self) -> u64 {
        let live: u64 = self
            .agents_sorted()
            .iter()
            .map(|a| a.inner.lock().sent)
            .sum();
        live + self.sent_by_gone.load(Relaxed)
    }

    /// Accept `body` from `from` for `to`, which must both be registered
    /// (`from` is checked first). Enforces the per-recipient and global
    /// caps, makes room at the global cap by deleting the oldest read
    /// message, refreshes the sender's `last_seen`, and wakes the oldest pull
    /// parked on the recipient. Nothing changes when it fails.
    ///
    /// `body_json_len` is the body's JSON-escaped size, measured by the
    /// caller before taking any lock.
    pub(super) fn send(
        &self,
        from: &str,
        to: &str,
        body: Box<str>,
        body_json_len: usize,
        now_ms: u64,
    ) -> Result<Arc<Msg>, MailErr> {
        let (from_key, to_key) = (Key::fold(from), Key::fold(to));
        let sender = self.agent(from, from_key.as_str())?;
        let recipient = self.agent(to, to_key.as_str())?;
        let key = convo_key(&from_key, &to_key);
        let delivered = {
            let mut to_state = recipient.inner.lock();
            if to_state.removed {
                None
            } else {
                let out = Outgoing {
                    key: key.as_str(),
                    body,
                    body_json_len,
                    now_ms,
                };
                Some(self.deliver(&mut to_state, &sender, &recipient, out))
            }
        };
        // The roster is only read once the recipient's lock is released: the
        // roster comes first in the lock order.
        let msg = match delivered {
            Some(result) => result?,
            None => return Err(self.unregistered(to)),
        };
        // The live count is raised once the message is filed; at the cap the
        // oldest read message makes room. Unread mail is under the cap, so
        // there is read mail to prune.
        if self.total.0.fetch_add(1, Relaxed) >= MAX_TOTAL_MESSAGES as usize {
            self.prune_oldest_read();
        }
        // Sending is activity: an agent that only sends must not age off the
        // roster.
        let mut from_state = sender.inner.lock();
        if from_state.removed {
            self.sent_by_gone.fetch_add(1, Relaxed);
        } else {
            from_state.last_seen_ms = now_ms;
            from_state.sent += 1;
        }
        Ok(msg)
    }

    /// Queue a message for `recipient`, whose state `to_state` is locked by the
    /// caller: check its queue, count the message, file it in the
    /// conversation `key` and wake a parked pull. Nothing changes on failure.
    fn deliver(
        &self,
        to_state: &mut AgentState,
        sender: &Agent,
        recipient: &Agent,
        out: Outgoing<'_>,
    ) -> Result<Arc<Msg>, MailErr> {
        let pending = to_state.unread.len();
        if pending >= MAX_PENDING_PER_AGENT as usize {
            return Err(MailErr::QueueFull {
                to: recipient.name.to_string(),
                pending: pending as i64,
            });
        }
        self.reserve()?;
        let msg = self.file(out, sender, recipient);
        to_state.unread.push_back(msg.clone());
        if recipient.signal.parked() > 0 {
            recipient.signal.notify.notify_one();
        }
        Ok(msg)
    }

    /// Count one more unread message, failing `storage_full` when every slot
    /// is unread.
    fn reserve(&self) -> Result<(), MailErr> {
        let mut unread = self.unread.0.load(Relaxed);
        loop {
            if unread >= MAX_TOTAL_MESSAGES as usize {
                return Err(MailErr::StorageFull {
                    pending: unread as i64,
                });
            }
            match self
                .unread
                .0
                .compare_exchange_weak(unread, unread + 1, Relaxed, Relaxed)
            {
                Ok(_) => break,
                Err(now) => unread = now,
            }
        }
        Ok(())
    }

    /// Delete the message that has been read longest, comparing the read
    /// times the agents publish and locking only the one that holds it.
    /// Retries when it lost a race for that message to another prune or a
    /// sweep.
    fn prune_oldest_read(&self) {
        loop {
            let oldest = self.roster.min_by_rank(|a| {
                let read_ms = a.oldest_read.0.load(Relaxed);
                (read_ms != NEVER).then(|| (read_ms, a.oldest_read.1.load(Relaxed)))
            });
            let Some(agent) = oldest else { return };
            let mut st = agent.inner.lock();
            let killed = st.read.pop_front().is_some_and(|m| m.kill());
            agent.publish_oldest_read(&st);
            if killed {
                self.total.0.fetch_sub(1, Relaxed);
                return;
            }
        }
    }

    /// Give a message its id and file it in the conversation `out.key`, creating
    /// the conversation on first use.
    fn file(&self, out: Outgoing<'_>, sender: &Agent, recipient: &Agent) -> Arc<Msg> {
        let Outgoing {
            key,
            body,
            body_json_len,
            now_ms,
        } = out;
        loop {
            let convo = self.convo(key);
            let mut log = convo.log.lock();
            if log.retired {
                continue;
            }
            let id = self.next_id.0.fetch_add(1, Relaxed);
            let msg = Arc::new(Msg::new(
                id,
                sender.name.clone(),
                recipient.name.clone(),
                recipient.gen,
                body,
                body_json_len,
                now_ms,
            ));
            log.push(msg.clone());
            return msg;
        }
    }

    /// The conversation under `key`, created if need be.
    fn convo(&self, key: &str) -> Arc<Convo> {
        self.convos.get_or_insert_with(key, Arc::default)
    }

    /// Stamp `agent` as seen at `now_ms`, then take its next batch of unread
    /// mail, marking it read at `now_ms`; `None` when nothing is unread.
    /// `unregistered` when the agent has left.
    ///
    /// Up to [`MAX_BATCH`] messages within [`MAX_RETURN_BYTES`] of
    /// serialized JSON, the first always included. Taking and marking happen
    /// under the agent's lock in this one call, so a pull hands each message
    /// out exactly once.
    pub(super) fn pull(&self, agent: &Agent, now_ms: u64) -> Option<Option<Batch>> {
        let mut st = agent.inner.lock();
        if st.removed {
            return None;
        }
        st.last_seen_ms = now_ms;
        let take = fitting(st.unread.iter().map(|m| m.page_len()));
        if take == 0 {
            return Some(None);
        }
        let batch: Vec<Arc<Msg>> = st.unread.drain(..take).collect();
        let remaining = st.unread.len();
        for msg in &batch {
            msg.read_ms.store(now_ms, Relaxed);
        }
        st.read.extend(batch.iter().cloned());
        agent.publish_oldest_read(&st);
        self.unread.0.fetch_sub(take, Relaxed);
        Some(Some((batch, remaining)))
    }

    /// Delete read mail read before `cutoff_ms`. Returns how many.
    pub(super) fn expire_read(&self, cutoff_ms: u64) -> i64 {
        let mut expired = 0;
        for (_, agent) in self.roster.entries() {
            if agent.oldest_read.0.load(Relaxed) >= cutoff_ms {
                continue;
            }
            let mut st = agent.inner.lock();
            while let Some(front) = st.read.front() {
                if front.read_at_ms().is_some_and(|ms| ms >= cutoff_ms) {
                    break;
                }
                if st.read.pop_front().is_some_and(|m| m.kill()) {
                    self.total.0.fetch_sub(1, Relaxed);
                    expired += 1;
                }
            }
            agent.publish_oldest_read(&st);
        }
        expired
    }

    /// Remove the agent under `key` from the roster with all the mail
    /// addressed to it, read or not, wake its parked pulls (they fail
    /// `unregistered`) and hold the name until `until_ms`. Messages it sent
    /// stay with their recipients. Returns the agent and how many messages
    /// were discarded; `None` when it is not on the roster.
    pub(super) fn remove_agent(&self, key: &str, until_ms: u64) -> Option<(Arc<Agent>, i64)> {
        let mut holds = self.holds.lock();
        let agent = self.roster.remove(key)?;
        let queued = {
            let mut st = agent.inner.lock();
            st.removed = true;
            st.read.clear();
            agent.publish_oldest_read(&st);
            self.sent_by_gone.fetch_add(st.sent, Relaxed);
            holds.insert(
                key.to_string(),
                Hold {
                    until_ms,
                    registered_ms: st.registered_ms,
                },
            );
            std::mem::take(&mut st.unread).len()
        };
        drop(holds);
        self.unread.0.fetch_sub(queued, Relaxed);
        let discarded = self.discard_mail_to(key, agent.gen);
        self.total.0.fetch_sub(discarded, Relaxed);
        agent.signal.notify.notify_waiters();
        Some((agent, discarded as i64))
    }

    /// Kill every live message addressed to registration `gen` of the folded
    /// name `key`, and tidy what that leaves behind. Returns how many it
    /// killed.
    fn discard_mail_to(&self, key: &str, gen: u64) -> usize {
        let convos = self.convos.values_where(|k| convo_has(k, key));
        let mut discarded = 0;
        for convo in convos {
            let mut log = convo.log.lock();
            discarded += log
                .live()
                .filter(|m| m.to_gen == gen)
                .filter(|m| m.kill())
                .count();
            log.compact();
        }
        self.reap_empty_convos();
        discarded
    }

    /// Remove the conversations that hold no live message.
    fn reap_empty_convos(&self) {
        self.convos.retain(|convo| {
            let mut log = convo.log.lock();
            log.retired = log.is_empty();
            !log.retired
        });
    }

    /// Drop dead entries from every conversation, and the conversations left
    /// empty.
    pub(super) fn compact_convos(&self) {
        self.convos.retain(|convo| {
            let mut log = convo.log.lock();
            log.compact();
            log.retired = log.is_empty();
            !log.retired
        });
    }

    /// Wake every parked pull: the agents are gone, or the bus closed.
    pub(super) fn wake_all(&self) {
        for (_, agent) in self.roster.entries() {
            agent.signal.notify.notify_waiters();
        }
    }

    /// Drop everything: roster, mail, holds. Wakes every parked pull (they
    /// fail `unregistered`, or `shutting_down` once the bus is closed).
    /// Returns `(messages, agents)` removed.
    pub(super) fn clear(&self) -> (i64, i64) {
        let mut holds = self.holds.lock();
        let agents = self.roster.drain();
        let counts = (self.total.0.load(Relaxed) as i64, agents.len() as i64);
        for agent in agents {
            let mut st = agent.inner.lock();
            st.removed = true;
            self.sent_by_gone.fetch_add(st.sent, Relaxed);
            drop(st);
            agent.signal.notify.notify_waiters();
        }
        holds.clear();
        for convo in self.convos.drain() {
            convo.log.lock().retired = true;
        }
        self.total.0.store(0, Relaxed);
        self.unread.0.store(0, Relaxed);
        counts
    }
}
