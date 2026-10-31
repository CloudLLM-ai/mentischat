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
//! - **Rooms** (a [`Sharded`] map of [`Room`], one `Mutex` each): see
//!   `rooms.rs`. An agent keeps a list of the rooms it is in so a pull knows
//!   where to look.
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
//! 4. a room-map shard, or a conversation-map shard
//! 5. a room, or a conversation log
//! 6. leaf locks, held with nothing else taken: an agent's hot-room set
//!    (`Signal::hot`, which a room's send fills under the room's lock) and a
//!    conversation-index set (`convos.rs`)
//!
//! Two agents are never locked at once, nor two conversation logs, nor two
//! rooms, nor two shards of one map. A send locks its recipient, then the conversation, and
//! touches the sender (and prunes at the global cap, which locks whichever
//! agent has read longest) only after releasing the recipient. A message's id
//! is taken under its conversation's lock, so every log is sorted by id, and
//! under its recipient's lock, so every unread queue is too.
//!
//! Names are folded (ASCII-lowercased) for every key and compared without
//! regard to case; entries keep the spelling they were registered with.

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::ops::Bound;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering::Relaxed};
use std::sync::Arc;

use parking_lot::{Mutex, RwLock};

use super::convos::ConvoMap;
use super::log::{Convo, Msg, Target};
use super::park::{Hot, Signal};
use super::rooms::Room;
use super::sharded::{Padded, Sharded};
use super::{
    MailErr, MAX_BATCH, MAX_LIMITED_PAGE_BYTES, MAX_PAGE, MAX_PENDING_PER_AGENT, MAX_RETURN_BYTES,
    MAX_TOTAL_MESSAGES,
};
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

/// A message pushed to a stream and awaiting its acknowledgement.
#[derive(Debug)]
pub(super) struct Leased {
    pub msg: Arc<Msg>,
    /// The room it came from; `None` for direct mail.
    pub room: Option<Arc<Room>>,
}

impl AgentState {
    /// Direct messages leased: still unread, so still counted against the
    /// recipient's queue.
    pub(super) fn leased_direct(&self) -> usize {
        self.leased.iter().filter(|l| l.room.is_none()).count()
    }
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
    /// The rooms this agent is in (a hint: each room's member list decides).
    pub rooms: Vec<Arc<Room>>,
    /// Messages a stream has pushed to this agent and not had acknowledged
    /// yet, in the order they were pushed. Direct mail leaves `unread` for
    /// here (and still counts as unread); a room message stays counted as
    /// taken by the member's cursor, which a release winds back.
    pub leased: Vec<Leased>,
    /// Serialized size of everything in `leased`.
    pub leased_bytes: usize,
    /// Which stream owns the leases; a new stream takes over by raising it.
    pub stream_epoch: u64,
    /// Folded names of rooms this agent was invited to (a hint, bounded, so
    /// `list_rooms` can show them without searching every room; the room's
    /// own invitation list decides).
    pub invited_to: std::collections::HashSet<String>,
    /// Set once the agent has left the roster. A send that looked the agent
    /// up before that sees it here and fails `unregistered`.
    pub removed: bool,
    /// The Ed25519 key bound to this registration (trust on first use), for
    /// signed rooms. Dies with the roster entry; never snapshotted.
    pub key: Option<crate::signed::PublicKey>,
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
    /// Whether the agent is in `State::readers`. Changed only under the
    /// agent's lock.
    pub indexed: AtomicBool,
    /// Woken when a new stream takes over from the current one.
    pub takeover: tokio::sync::Notify,
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
            indexed: AtomicBool::new(false),
            takeover: tokio::sync::Notify::new(),
            inner: Mutex::new(AgentState {
                provider,
                description,
                registered_ms,
                last_seen_ms: now_ms,
                unread: VecDeque::new(),
                read: VecDeque::new(),
                sent: 0,
                rooms: Vec::new(),
                leased: Vec::new(),
                leased_bytes: 0,
                stream_epoch: 0,
                invited_to: Default::default(),
                removed: false,
                key: None,
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

/// The values of `entries` ordered by their (folded) keys.
pub(super) fn sorted_by_key<T>(mut entries: Vec<(String, T)>) -> Vec<T> {
    entries.sort_unstable_by(|a, b| a.0.cmp(&b.0));
    entries.into_iter().map(|(_, value)| value).collect()
}

/// Longest folded name: [`crate::names::AgentName`] caps names at this.
const NAME_MAX: usize = crate::names::MAX_NAME_LEN;

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

/// How big a page of messages is: how many, and how many bytes of their JSON.
#[derive(Debug, Clone, Copy)]
pub(super) struct Page {
    pub items: usize,
    pub bytes: usize,
}

impl Page {
    /// The contract's page: [`MAX_BATCH`] messages within
    /// [`MAX_RETURN_BYTES`].
    pub(super) const DEFAULT: Self = Self {
        items: MAX_BATCH as usize,
        bytes: MAX_RETURN_BYTES,
    };

    /// A page a caller asked for: `limit` messages (1 to [`MAX_PAGE`]) within
    /// [`MAX_LIMITED_PAGE_BYTES`]. `None` is [`Page::DEFAULT`].
    pub(super) fn limited(limit: Option<usize>) -> Self {
        limit.map_or(Self::DEFAULT, |n| Self {
            items: n.clamp(1, MAX_PAGE),
            bytes: MAX_LIMITED_PAGE_BYTES,
        })
    }
}

/// How many of `sizes`, in order, fit `page`.
/// The first always fits: progress must be possible.
///
/// Size is each message's serialized JSON, not its raw body: escaping can
/// make a body several times longer on the wire (a control character becomes
/// six bytes), and the wire size is what has to stay under client truncation
/// limits.
pub(super) fn fitting(sizes: impl Iterator<Item = usize>, page: Page) -> usize {
    let mut take = 0usize;
    let mut bytes = 0usize;
    for size in sizes.take(page.items) {
        if take > 0 && bytes + size > page.bytes {
            break;
        }
        bytes += size;
        take += 1;
    }
    take
}

/// A history page from `rows`, the newest messages first (one more than a
/// page, if there are more): trims them to what fits [`Page::DEFAULT`], the
/// cursor for the next (older) page, and the page oldest first.
pub(super) fn page_from_newest(mut rows: Vec<Arc<Msg>>) -> (Vec<Arc<Msg>>, Option<i64>) {
    let take = fitting(rows.iter().map(|m| m.page_len()), Page::DEFAULT);
    let next = (rows.len() > take).then(|| rows[take - 1].id as i64);
    rows.truncate(take);
    rows.reverse();
    (rows, next)
}

/// How [`State::take_merged`] hands out what it picked.
#[derive(Debug, Clone, Copy)]
pub(super) enum Take {
    /// A pull: direct mail is marked read at this time (milliseconds since
    /// the Unix epoch).
    Read(u64),
    /// A stream push: out of the queue but unread, until acked.
    Lease,
}

/// What one pull found.
pub(super) enum Pulled {
    /// The agent has left the roster.
    Gone,
    /// The agent is there and nothing is unread.
    Nothing,
    /// A batch of unread mail.
    Batch(Batch),
}

/// Lease `chosen` (candidates in id order, tagged with the index of their
/// room in `rooms`, or `None` for direct mail): the first `direct` unread
/// direct messages move to the lease list, room messages are leased as they
/// are.
fn lease_chosen(
    st: &mut AgentState,
    chosen: &[(Arc<Msg>, Option<usize>)],
    rooms: &[Arc<Room>],
    direct: usize,
) {
    let moved: Vec<Arc<Msg>> = st.unread.drain(..direct).collect();
    for msg in moved {
        st.leased_bytes += msg.page_len();
        st.leased.push(Leased { msg, room: None });
    }
    for (msg, room) in chosen {
        if let Some(i) = room {
            st.leased_bytes += msg.page_len();
            st.leased.push(Leased {
                msg: msg.clone(),
                room: Some(rooms[*i].clone()),
            });
        }
    }
}

/// A message on its way: what `send` hands down once both names resolve.
struct Outgoing<'a> {
    /// The conversation it belongs to, see [`convo_key`].
    key: &'a str,
    /// The body, taken once the message is filed.
    body: &'a mut Option<Box<str>>,
    /// The body's JSON-escaped size.
    body_json_len: usize,
    now_ms: u64,
}

/// Why `deliver` did not queue a message.
enum Refused {
    /// A contract error: the send fails with it.
    Error(MailErr),
    /// The byte budget is full: delete the oldest read message and retry.
    NoRoom,
}

/// How many roster names an `unregistered` error lists.
pub(super) const MAX_KNOWN: usize = 256;

/// Mail taken by a pull.
pub(super) struct Batch {
    /// Direct and room messages together, oldest first.
    pub messages: Vec<Arc<Msg>>,
    /// Unread messages left, in direct mail and every room.
    pub remaining: usize,
    /// Room messages lost to rooms filling up since the last pull.
    pub skipped: u64,
}

/// The whole mailbox. See the module docs for what each lock guards and the
/// order to take them in.
pub(super) struct State {
    /// Agents by folded name.
    pub roster: Sharded<Arc<Agent>>,
    /// Released names by folded name; also the lock that serializes changes
    /// to the roster's membership.
    pub holds: Mutex<HashMap<String, Hold>>,
    /// Folded roster names in order, for paging and for the `known` list
    /// without sorting the roster. Changed under `holds`.
    pub names: RwLock<BTreeSet<String>>,
    /// Agents that have read mail, by folded name: the only ones a prune or
    /// an expiry has to look at (at most [`MAX_TOTAL_MESSAGES`]). Entries
    /// are added, and removed, under the agent's lock.
    pub readers: Sharded<Arc<Agent>>,
    /// Folded names of the public rooms in order, so `list_rooms` pages through
    /// them without visiting private rooms it cannot show.
    pub public_names: RwLock<BTreeSet<String>>,
    /// Conversations by [`convo_key`].
    pub convos: ConvoMap,
    /// Rooms by folded name.
    pub rooms: Sharded<Arc<Room>>,
    /// How many messages a room keeps.
    pub room_capacity: usize,
    /// The recipient of a room message, which has none.
    pub no_agent: Arc<str>,
    /// Live messages, read and unread.
    pub total: Padded<AtomicUsize>,
    /// Live unread messages.
    pub unread: Padded<AtomicUsize>,
    /// Bytes of message bodies held, direct and room.
    pub bytes: Padded<AtomicUsize>,
    /// What `bytes` may reach.
    pub byte_budget: usize,
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
            names: RwLock::default(),
            readers: Sharded::new(),
            public_names: RwLock::default(),
            convos: ConvoMap::new(),
            rooms: Sharded::new(),
            room_capacity: super::DEFAULT_ROOM_CAPACITY,
            no_agent: "".into(),
            total: Padded(AtomicUsize::new(0)),
            unread: Padded(AtomicUsize::new(0)),
            bytes: Padded(AtomicUsize::new(0)),
            byte_budget: super::MAX_RETAINED_BYTES,
            next_id: Padded(AtomicU64::new(1)),
            next_gen: AtomicU64::new(1),
            sent_by_gone: AtomicU64::new(0),
            closed: AtomicBool::new(false),
        }
    }

    /// Roster entries sorted by folded name. Walks the whole roster: for
    /// snapshots, not for requests.
    pub(super) fn agents_sorted(&self) -> Vec<Arc<Agent>> {
        sorted_by_key(self.roster.entries())
    }

    /// Put `agent` on the roster under `key` (a folded name). The caller
    /// holds `holds`.
    pub(super) fn add_agent(&self, key: String, agent: Arc<Agent>) {
        self.names.write().insert(key.clone());
        self.roster.insert(key, agent);
    }

    /// Up to `limit` roster entries after `after` (a folded name; from the
    /// start for `None`), in name order, and whether more follow. O(limit),
    /// however large the roster.
    pub(super) fn agents_page(&self, after: Option<&str>, limit: usize) -> (Vec<Arc<Agent>>, bool) {
        let lower = after.map_or(Bound::Unbounded, Bound::Excluded);
        let mut keys: Vec<String> = self
            .names
            .read()
            .range::<str, _>((lower, Bound::Unbounded))
            .take(limit + 1)
            .cloned()
            .collect();
        let more = keys.len() > limit;
        keys.truncate(limit);
        let agents = keys.iter().filter_map(|k| self.roster.get(k)).collect();
        (agents, more)
    }

    /// The `unregistered` error for `name`, with the first
    /// [`MAX_KNOWN`] names on the roster. Takes roster locks: call it with none
    /// of the later locks held.
    pub(super) fn unregistered(&self, name: &str) -> MailErr {
        let (agents, _) = self.agents_page(None, MAX_KNOWN);
        MailErr::Unregistered {
            agent: name.to_string(),
            known: agents.iter().map(|a| a.name.to_string()).collect(),
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

    /// Suggested names that are neither on the roster nor held at `now_ms`:
    /// a lookup per suggestion, not a pass over the roster.
    pub(super) fn free_suggestions(
        &self,
        holds: &HashMap<String, Hold>,
        now_ms: u64,
    ) -> Vec<String> {
        crate::names::suggestions(|n| {
            let key = n.to_ascii_lowercase();
            self.roster.get(&key).is_some() || holds.get(&key).is_some_and(|h| h.until_ms > now_ms)
        })
    }

    /// The agent under `key` (a folded name), or `unregistered` for `name`.
    pub(super) fn agent(&self, name: &str, key: &str) -> Result<Arc<Agent>, MailErr> {
        self.roster.get(key).ok_or_else(|| self.unregistered(name))
    }

    /// Messages accepted since the daemon started.
    pub(super) fn messages_sent(&self) -> u64 {
        // An agent removed since the snapshot already folded its count into
        // `sent_by_gone`, under the same lock.
        let mut live = 0;
        self.roster.for_each(|a| {
            let st = a.inner.lock();
            if !st.removed {
                live += st.sent;
            }
        });
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
        let mut body = Some(body);
        let (msg, at_cap) = loop {
            let delivered = {
                let mut to_state = recipient.inner.lock();
                if to_state.removed {
                    None
                } else {
                    let out = Outgoing {
                        key: key.as_str(),
                        body: &mut body,
                        body_json_len,
                        now_ms,
                    };
                    Some(self.deliver(&mut to_state, &sender, &recipient, out))
                }
            };
            // The roster is only read, and another agent only locked (to
            // prune), once the recipient's lock is released.
            match delivered {
                None => return Err(self.unregistered(to)),
                Some(Ok(done)) => break done,
                Some(Err(Refused::Error(e))) => return Err(e),
                Some(Err(Refused::NoRoom)) => {
                    if !self.prune_oldest_read() && !self.evict_from_rooms() {
                        return Err(MailErr::StorageFull {
                            pending: self.unread.0.load(Relaxed) as i64,
                        });
                    }
                }
            }
        };
        // The live count was raised with the message; at the cap the oldest
        // read message makes room. Unread mail is under the cap, so there is
        // read mail to prune.
        if at_cap {
            self.prune_oldest_read();
        }
        // Sending is activity: an agent that only sends must not age off the
        // roster.
        self.note_sent(&sender, now_ms);
        Ok(msg)
    }

    /// Count a send against `sender` and mark it seen: sending is activity.
    /// A sender that left the roster meanwhile is counted in `sent_by_gone`
    /// so [`State::messages_sent`] stays exact.
    pub(super) fn note_sent(&self, sender: &Agent, now_ms: u64) {
        let mut from_state = sender.inner.lock();
        if from_state.removed {
            self.sent_by_gone.fetch_add(1, Relaxed);
        } else {
            from_state.last_seen_ms = now_ms;
            from_state.sent += 1;
        }
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
    ) -> Result<(Arc<Msg>, bool), Refused> {
        let pending = to_state.unread.len() + to_state.leased_direct();
        if pending >= MAX_PENDING_PER_AGENT as usize {
            return Err(Refused::Error(MailErr::QueueFull {
                to: recipient.name.to_string(),
                pending: pending as i64,
            }));
        }
        let len = out
            .body
            .as_ref()
            .map_or(0, |b| b.len() + super::MESSAGE_OVERHEAD_BYTES);
        if !self.try_reserve_bytes(len) {
            return Err(Refused::NoRoom);
        }
        let at_cap = self.reserve().map_err(|e| {
            self.release_bytes(len);
            Refused::Error(e)
        })?;
        let msg = self.file(out, sender, recipient);
        to_state.unread.push_back(msg.clone());
        if recipient.signal.parked() > 0 {
            recipient.signal.notify.notify_one();
        }
        Ok((msg, at_cap))
    }

    /// Count one more unread and one more live message, failing
    /// `storage_full` when every slot is unread. Returns true when the live
    /// count was already at the cap, so the caller prunes the oldest read
    /// message once it has released the recipient's lock.
    ///
    /// Both counts are raised under the recipient's lock, so `clear` and
    /// `remove_agent` (which lock the agent first) never see a message that
    /// is filed but not counted.
    fn reserve(&self) -> Result<bool, MailErr> {
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
        Ok(self.total.0.fetch_add(1, Relaxed) >= MAX_TOTAL_MESSAGES as usize)
    }

    /// Count `n` more body bytes, unless that passes the budget.
    pub(super) fn try_reserve_bytes(&self, n: usize) -> bool {
        let mut held = self.bytes.0.load(Relaxed);
        loop {
            if held.saturating_add(n) > self.byte_budget {
                return false;
            }
            match self
                .bytes
                .0
                .compare_exchange_weak(held, held + n, Relaxed, Relaxed)
            {
                Ok(_) => return true,
                Err(now) => held = now,
            }
        }
    }

    /// Give `n` body bytes back (never below zero).
    pub(super) fn release_bytes(&self, n: usize) {
        let mut held = self.bytes.0.load(Relaxed);
        while let Err(now) =
            self.bytes
                .0
                .compare_exchange_weak(held, held.saturating_sub(n), Relaxed, Relaxed)
        {
            held = now;
        }
    }

    /// Delete the message that has been read longest, comparing the read
    /// times the agents publish and locking only the one that holds it.
    /// Retries when it lost a race for that message to another prune or a
    /// sweep. False when no read message is left to delete.
    fn prune_oldest_read(&self) -> bool {
        loop {
            let oldest = self.readers.min_by_rank(|a| {
                let read_ms = a.oldest_read.0.load(Relaxed);
                (read_ms != NEVER).then(|| (read_ms, a.oldest_read.1.load(Relaxed)))
            });
            let Some(agent) = oldest else { return false };
            let mut st = agent.inner.lock();
            let killed = st.read.pop_front().filter(|m| m.kill());
            agent.publish_oldest_read(&st);
            if let Some(msg) = killed {
                self.total.0.fetch_sub(1, Relaxed);
                self.release_bytes(msg.cost());
                return true;
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
        let body = body.take().unwrap_or_default();
        loop {
            let convo = self.convo(key);
            let mut log = convo.log.lock();
            if log.retired {
                drop(log);
                self.convos.remove_if(key, |c| Arc::ptr_eq(c, &convo));
                continue;
            }
            let id = self.next_id.0.fetch_add(1, Relaxed);
            let msg = Arc::new(Msg::new(
                id,
                sender.name.clone(),
                Target {
                    to: recipient.name.clone(),
                    to_gen: recipient.gen,
                    room: None,
                },
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
    /// Up to `page.items` messages within `page.bytes` of serialized JSON,
    /// the first always included. Taking and marking happen
    /// under the agent's lock in this one call, so a pull hands each message
    /// out exactly once.
    pub(super) fn pull(&self, agent: &Arc<Agent>, now_ms: u64, page: Page) -> Pulled {
        let mut st = agent.inner.lock();
        if st.removed {
            return Pulled::Gone;
        }
        st.last_seen_ms = now_ms;
        let hot = agent.signal.take_hot();
        let batch = if !hot.all && hot.rooms.is_empty() {
            self.take_direct(agent, &mut st, now_ms, page)
                .map(|messages| Batch {
                    remaining: st.unread.len(),
                    messages,
                    skipped: 0,
                })
        } else {
            self.take_merged(agent, &mut st, hot, Take::Read(now_ms), page)
        };
        batch.map_or(Pulled::Nothing, Pulled::Batch)
    }

    /// Mark the first `n` (at most) of the agent's unread direct mail read at
    /// `now_ms` and hand it out, oldest first, within the batch caps.
    fn take_direct(
        &self,
        agent: &Arc<Agent>,
        st: &mut AgentState,
        now_ms: u64,
        page: Page,
    ) -> Option<Vec<Arc<Msg>>> {
        let take = fitting(st.unread.iter().map(|m| m.page_len()), page);
        if take == 0 {
            return None;
        }
        Some(self.read_first(agent, st, take, now_ms))
    }

    /// Move the first `take` unread messages to the read queue.
    fn read_first(
        &self,
        agent: &Arc<Agent>,
        st: &mut AgentState,
        take: usize,
        now_ms: u64,
    ) -> Vec<Arc<Msg>> {
        let batch: Vec<Arc<Msg>> = st.unread.drain(..take).collect();
        self.mark_read(agent, st, &batch, now_ms);
        batch
    }

    /// Mark direct `batch` (already out of the unread queue) read at `now_ms`:
    /// stamp it, queue it in read order and index the agent as a reader.
    pub(super) fn mark_read(
        &self,
        agent: &Arc<Agent>,
        st: &mut AgentState,
        batch: &[Arc<Msg>],
        now_ms: u64,
    ) {
        for msg in batch {
            msg.read_ms.store(now_ms, Relaxed);
        }
        st.read.extend(batch.iter().cloned());
        agent.publish_oldest_read(st);
        if !agent.indexed.swap(true, Relaxed) {
            self.readers
                .insert(Key::fold(&agent.name).as_str().to_string(), agent.clone());
        }
        self.unread.0.fetch_sub(batch.len(), Relaxed);
    }

    /// A pull for an agent that is in rooms: the oldest unread messages of
    /// its direct mail and every room, merged by id, within the batch caps.
    /// Direct mail is marked read; each room's cursor advances past what was
    /// taken, and its skipped count is reported and reset.
    pub(super) fn take_merged(
        &self,
        agent: &Arc<Agent>,
        st: &mut AgentState,
        hot: Hot,
        how: Take,
        page: Page,
    ) -> Option<Batch> {
        let rooms = self.hot_rooms(st, hot);
        let rooms = rooms.as_slice();
        let waiting: Vec<_> = rooms
            .iter()
            .enumerate()
            .filter_map(|(i, room)| room.waiting(agent, page.items).map(|w| (i, w)))
            .collect();
        if waiting.len() < rooms.len() {
            let member_of: Vec<&Arc<Room>> = waiting.iter().map(|(i, _)| &rooms[*i]).collect();
            st.rooms.retain(|r| {
                member_of.iter().any(|m| Arc::ptr_eq(m, r))
                    || !rooms.iter().any(|m| Arc::ptr_eq(m, r))
            });
        }
        let mut candidates: Vec<(Arc<Msg>, Option<usize>)> = st
            .unread
            .iter()
            .take(page.items)
            .map(|m| (m.clone(), None))
            .chain(
                waiting
                    .iter()
                    .flat_map(|(i, w)| w.messages.iter().map(move |m| (m.clone(), Some(*i)))),
            )
            .collect();
        candidates.sort_unstable_by_key(|(m, _)| m.id);
        let take = fitting(candidates.iter().map(|(m, _)| m.page_len()), page);
        if take == 0 && waiting.iter().all(|(_, w)| w.skipped == 0) {
            for (i, w) in &waiting {
                if w.unread > 0 {
                    agent.signal.mark_room(&rooms[*i].name);
                }
            }
            return None;
        }
        let chosen = &candidates[..take];
        let direct = chosen.iter().filter(|(_, room)| room.is_none()).count();
        match how {
            Take::Read(now_ms) => {
                self.read_first(agent, st, direct, now_ms);
            }
            Take::Lease => lease_chosen(st, chosen, rooms, direct),
        }
        let (mut remaining, mut skipped) = (st.unread.len(), 0);
        for (i, w) in &waiting {
            let ids: Vec<u64> = chosen
                .iter()
                .filter(|(_, room)| *room == Some(*i))
                .map(|(m, _)| m.id)
                .collect();
            let left = if ids.is_empty() && w.skipped == 0 {
                w.unread
            } else {
                let (unread, lost) = rooms[*i].commit(agent, &ids);
                skipped += lost;
                unread
            };
            if left > 0 {
                agent.signal.mark_room(&rooms[*i].name);
            }
            remaining += left;
        }
        Some(Batch {
            messages: chosen.iter().map(|(m, _)| m.clone()).collect(),
            remaining,
            skipped,
        })
    }

    /// The rooms a pull has to look at: all the agent is in, or those
    /// named in `hot` that exist (once each).
    fn hot_rooms(&self, st: &AgentState, hot: Hot) -> Vec<Arc<Room>> {
        if hot.all {
            return st.rooms.clone();
        }
        let mut rooms: Vec<Arc<Room>> = hot
            .rooms
            .iter()
            .filter_map(|name| self.rooms.get(Key::fold(name).as_str()))
            .collect();
        rooms.sort_unstable_by_key(|r| Arc::as_ptr(r) as usize);
        rooms.dedup_by(|a, b| Arc::ptr_eq(a, b));
        rooms
    }

    /// Delete read mail read before `cutoff_ms`. Returns how many.
    pub(super) fn expire_read(&self, cutoff_ms: u64) -> i64 {
        let mut expired = 0;
        for (key, agent) in self.readers.entries() {
            if agent.oldest_read.0.load(Relaxed) == NEVER {
                // Drained since it was indexed: drop it from the index,
                // unless a pull refilled it first.
                let st = agent.inner.lock();
                if st.read.is_empty() {
                    agent.indexed.store(false, Relaxed);
                    self.readers.remove_if(&key, |a| Arc::ptr_eq(a, &agent));
                }
                continue;
            }
            if agent.oldest_read.0.load(Relaxed) >= cutoff_ms {
                continue;
            }
            let mut st = agent.inner.lock();
            while let Some(front) = st.read.front() {
                if front.read_at_ms().is_some_and(|ms| ms >= cutoff_ms) {
                    break;
                }
                if let Some(msg) = st.read.pop_front().filter(|m| m.kill()) {
                    self.total.0.fetch_sub(1, Relaxed);
                    self.release_bytes(msg.cost());
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
        self.names.write().remove(key);
        let rooms;
        let queued = {
            let mut st = agent.inner.lock();
            st.removed = true;
            st.read.clear();
            agent.publish_oldest_read(&st);
            agent.indexed.store(false, Relaxed);
            self.readers.remove(key);
            rooms = std::mem::take(&mut st.rooms);
            self.sent_by_gone.fetch_add(st.sent, Relaxed);
            holds.insert(
                key.to_string(),
                Hold {
                    until_ms,
                    registered_ms: st.registered_ms,
                },
            );
            let direct = st.leased_direct();
            st.leased.clear();
            st.leased_bytes = 0;
            std::mem::take(&mut st.unread).len() + direct
        };
        drop(holds);
        self.leave_all_rooms(&agent, rooms);
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
        let convos = self.convos.of_agent(key);
        let (mut discarded, mut freed) = (0, 0);
        for (convo_key, convo) in convos {
            let mut log = convo.log.lock();
            for msg in log.live().filter(|m| m.to_gen == gen && m.kill()) {
                discarded += 1;
                freed += msg.cost();
            }
            log.compact();
            log.retired = log.is_empty();
            let gone = log.retired;
            drop(log);
            if gone {
                self.convos
                    .remove_if(&convo_key, |c| Arc::ptr_eq(c, &convo));
            }
        }
        self.release_bytes(freed);
        discarded
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
        self.roster
            .for_each(|agent| agent.signal.notify.notify_waiters());
    }

    /// Drop everything: roster, mail, holds. Wakes every parked pull (they
    /// fail `unregistered`, or `shutting_down` once the bus is closed).
    /// Returns `(messages, agents)` removed.
    pub(super) fn clear(&self) -> (i64, i64) {
        let mut holds = self.holds.lock();
        let agents = self.roster.drain();
        self.names.write().clear();
        self.readers.drain();
        self.public_names.write().clear();
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
        for room in self.rooms.drain() {
            room.inner.lock().retired = true;
        }
        self.total.0.store(0, Relaxed);
        self.unread.0.store(0, Relaxed);
        self.bytes.0.store(0, Relaxed);
        counts
    }
}
