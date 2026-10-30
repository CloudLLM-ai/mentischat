//! The mailbox: roster, caps, read marking, blocking pulls, history, held
//! names, and expiry of read mail and idle agents.
//!
//! Everything lives in memory in one `State`, split so that independent
//! conversations never wait on each other: a roster `RwLock` (read-locked on
//! the hot path), one `Mutex` per agent for its unread queue, one per
//! conversation log, a small one for the order mail was read in, and atomics
//! for the global counts. Each operation is a few short synchronous critical
//! sections (no I/O, no `.await`), taken in the order `state.rs` documents,
//! and responses are built after every lock is released.
//!
//! Mail between two agents is a conversation log of `Arc`'d messages; each
//! recipient keeps a queue of its unread ones, so pulling, and the caps
//! on unread mail, never scan anything. A blocking pull waits on its agent's
//! [`tokio::sync::Notify`] rather than on a lock: it enables the wake
//! first, checks the inbox, then waits, so a send that lands in between
//! cannot be missed.
//!
//! Names compare without regard to ASCII case: the roster, held names and
//! conversations are keyed by [`AgentName::folded`]. Responses use the
//! spelling the roster stores.
//!
//! Nothing is written to disk unless the daemon runs with
//! `MENTISCHAT_PERSIST=1`: then [`Bus::close`] writes a snapshot of the
//! mailbox and the next [`Bus::new`] reads it back and deletes it.

use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use tokio::time::Instant;

use crate::errors::TakenReason;
use crate::names::{AgentName, RoomName};

mod clock;
mod delivery;
mod log;
mod model;
mod park;
mod rooms;
mod snapshot;
mod state;
mod stream;

pub use clock::{now_rfc3339, rfc3339};
pub use model::{
    parse_duration, AgentOverview, DiscardOutcome, HistoryPage, InboxPage, Message, Overview,
    RegisterOutcome, Registration, Retention, RosterEntry, RosterListing, SendReceipt, Stats,
    SweepOutcome, MAX_BATCH, MAX_BODY_BYTES, MAX_DESCRIPTION_BYTES, MAX_PENDING_PER_AGENT,
    MAX_RETURN_BYTES, MAX_ROSTER, MAX_TOTAL_MESSAGES, MAX_WAITERS_PER_AGENT, MAX_WAIT_MS,
};
pub use model::{
    LeaveOutcome, RoomEntry, RoomInfo, RoomListing, DEFAULT_ROOM_CAPACITY, MAX_RETAINED_BYTES,
    MAX_ROOMS, MESSAGE_OVERHEAD_BYTES,
};
pub use model::{
    MAX_LIMITED_PAGE_BYTES, MAX_OVERVIEW_AGENTS, MAX_PAGE, MAX_ROOMS_PER_AGENT, MAX_ROOM_MEMBERS,
    MAX_STREAM_BYTES, MAX_STREAM_IN_FLIGHT,
};
pub use snapshot::{remove_snapshot, SnapshotError};
pub use stream::{Push, StreamSession};

use clock::{now_ms, rfc3339_ms, unix_ms};
use delivery::{history_page, history_rows};
use log::{json_len, Msg};
use park::{ParkGuard, Signal};
use state::{Agent, Key, Page, State};

type MailErr = crate::errors::MailboxError;

/// What one pass over a pull's inbox found.
enum Pull {
    /// A page of mail, or an empty page because the deadline passed.
    Ready(Taken),
    /// The agent left and came back under a new entry: wait on its signal
    /// instead.
    Retarget(Arc<Signal>),
    /// Nothing yet: wait, counted as parked until the guard drops.
    Park(ParkGuard),
}

/// Mail taken for a pull, still `Arc`'d: the wire strings are built after
/// the lock is released.
struct Taken {
    agent: Arc<str>,
    messages: Vec<Arc<Msg>>,
    remaining: usize,
    skipped: u64,
}

impl Taken {
    /// True when the pull has something to report, mail or lost messages.
    fn delivers(&self) -> bool {
        !self.messages.is_empty() || self.skipped > 0
    }

    fn into_page(self) -> InboxPage {
        InboxPage {
            agent: self.agent.to_string(),
            messages: self.messages.iter().map(|m| m.to_wire()).collect(),
            remaining: self.remaining as i64,
            skipped: self.skipped,
        }
    }

    fn empty(agent: Arc<str>) -> Self {
        Self {
            agent,
            messages: Vec::new(),
            remaining: 0,
            skipped: 0,
        }
    }
}

/// Shared mailbox handle; wrap in [`std::sync::Arc`] to share across HTTP/MCP layers.
pub struct Bus {
    state: State,
    retention: Retention,
    /// Where [`close`](Self::close) writes its snapshot and [`new`](Self::new)
    /// looks for one.
    data_dir: PathBuf,
}

impl Bus {
    /// An empty mailbox with the default [`Retention`], restored from the
    /// snapshot under `data_dir` if a clean stop left one. Nothing is
    /// written to `data_dir` until [`close`](Self::close).
    ///
    /// # Errors
    ///
    /// [`SnapshotError`] when a snapshot exists but cannot be read; it is
    /// left in place.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let bus = mentischat::bus::Bus::new(std::path::Path::new("/tmp/mentischat_demo"))?;
    /// bus.register(&mentischat::names::AgentName::parse("Vega")?).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn new(data_dir: &Path) -> Result<Self, SnapshotError> {
        Self::with_retention(data_dir, Retention::default())
    }

    /// Like [`new`](Self::new), expiring read mail and idle agents and
    /// holding released names per `retention`.
    pub fn with_retention(data_dir: &Path, retention: Retention) -> Result<Self, SnapshotError> {
        let state = snapshot::restore(data_dir)?.unwrap_or_else(State::new);
        Ok(Self {
            state,
            retention,
            data_dir: data_dir.to_path_buf(),
        })
    }

    /// Keep the last `capacity` messages of each room instead of
    /// [`DEFAULT_ROOM_CAPACITY`]; at least 1.
    pub fn with_room_capacity(mut self, capacity: usize) -> Self {
        self.state.room_capacity = capacity.max(1);
        self
    }

    /// Hold at most `bytes` of message bodies instead of
    /// [`MAX_RETAINED_BYTES`].
    pub fn with_byte_budget(mut self, bytes: usize) -> Self {
        self.state.byte_budget = bytes;
        self
    }

    /// How long read mail and idle agents are kept, and released names held.
    pub fn retention(&self) -> Retention {
        self.retention
    }

    /// The state, failing `shutting_down` once the bus has been closed.
    fn open(&self) -> Result<&State, MailErr> {
        if self.state.closed.load(Ordering::Relaxed) {
            Err(MailErr::ShuttingDown)
        } else {
            Ok(&self.state)
        }
    }

    /// Claim `name` with default options. See
    /// [`register_with`](Self::register_with).
    pub async fn register(&self, name: &AgentName) -> Result<RegisterOutcome, MailErr> {
        self.register_with(name, Registration::default()).await
    }

    /// Claim `name` on the roster.
    ///
    /// A free name is created. A name another agent has registered, or one
    /// held after it left the roster, fails `name_taken` with free
    /// suggestions, unless `opts.reclaim` is set: then a registered name has
    /// its `last_seen`, provider and description refreshed and keeps its
    /// mail (anyone using it shares the inbox), and a held name is created
    /// again with its previous registration's history.
    ///
    /// # Errors
    ///
    /// `shutting_down` once the bus is closed; `name_taken`; `roster_full`
    /// when a new name would exceed [`MAX_ROSTER`]; `invalid_description`.
    pub async fn register_with(
        &self,
        name: &AgentName,
        opts: Registration,
    ) -> Result<RegisterOutcome, MailErr> {
        let description = opts.description.map(validate_description).transpose()?;
        let provider = opts.provider.map(|p| p.as_str().to_string());
        let state = self.open()?;
        let now = now_ms();
        let key = name.folded();
        // Membership changes are serialized by `holds`.
        let mut holds = state.holds.lock();
        if let Some(agent) = state.roster.get(&key) {
            if opts.reclaim {
                return Ok(refresh_agent(&agent, now, provider, description));
            }
            let stored = agent.name.to_string();
            return Err(state.name_taken(&holds, stored, TakenReason::Registered, now));
        }
        let hold = holds.get(&key).filter(|h| h.until_ms > now).cloned();
        if let (Some(h), false) = (&hold, opts.reclaim) {
            let reason = TakenReason::Held {
                until: rfc3339_ms(h.until_ms),
            };
            return Err(state.name_taken(&holds, name.to_string(), reason, now));
        }
        let agents = state.roster.len();
        if agents >= MAX_ROSTER as usize {
            return Err(MailErr::RosterFull {
                agents: agents as i64,
            });
        }
        let registered_ms = hold.map_or(now, |h| h.registered_ms);
        let description = description.filter(|d| !d.is_empty());
        let gen = state.next_gen.fetch_add(1, Ordering::Relaxed);
        let agent = Agent::new(
            name.as_str().into(),
            gen,
            provider,
            description,
            registered_ms,
            now,
        );
        let outcome = register_outcome(&agent, true);
        state.add_agent(key.clone(), Arc::new(agent));
        holds.remove(&key);
        Ok(outcome)
    }

    /// Leave the roster, deleting the mail addressed to this name (read or
    /// not) and waking its parked pulls (they fail `unregistered`). Messages
    /// it sent stay with their recipients. The name is then held for
    /// [`Retention::name_hold`].
    pub async fn deregister(&self, name: &AgentName) -> Result<DiscardOutcome, MailErr> {
        let state = self.open()?;
        let until_ms = now_ms() + self.retention.name_hold.as_millis() as u64;
        match state.remove_agent(&name.folded(), until_ms) {
            Some((agent, discarded)) => Ok(DiscardOutcome {
                agent: agent.name.to_string(),
                discarded,
            }),
            None => Err(state.unregistered(name.as_str())),
        }
    }

    /// Current roster sorted by name ascending, with each agent's provider,
    /// description and unread count, plus free names to suggest. Read-only:
    /// does not touch `last_seen`.
    pub async fn list_agents(&self) -> Result<RosterListing, MailErr> {
        self.list_agents_page(None, MAX_PAGE).await
    }

    /// One page of [`list_agents`](Self::list_agents): up to `limit` (at most
    /// [`MAX_PAGE`], at least 1) agents after `after`, which is the `next` of
    /// the previous page. Costs O(limit), however large the roster, and
    /// `next` is present when more follow.
    pub async fn list_agents_page(
        &self,
        after: Option<&AgentName>,
        limit: usize,
    ) -> Result<RosterListing, MailErr> {
        let state = self.open()?;
        let after = after.map(AgentName::folded);
        let (page, more) = state.agents_page(after.as_deref(), limit.clamp(1, MAX_PAGE));
        let rows: Vec<_> = page.iter().map(|a| roster_row(state, a)).collect();
        let next = more.then(|| rows.last().map(|r| r.name.clone())).flatten();
        let suggestions = state.free_suggestions(&state.holds.lock(), now_ms());
        let agents = rows
            .into_iter()
            .map(|row| RosterEntry {
                name: row.name,
                provider: row.provider,
                description: row.description,
                last_seen: rfc3339_ms(row.last_seen_ms),
                queued: row.queued,
            })
            .collect();
        Ok(RosterListing {
            agents,
            next,
            suggestions,
        })
    }

    /// The first [`MAX_OVERVIEW_AGENTS`] agents of the roster, with each
    /// agent's provider, description, unread count and last-seen age, plus the number of messages sent since the daemon
    /// started, for the operator terminal view.
    ///
    /// Shares its rows with [`list_agents`](Self::list_agents); this shape
    /// trades the wire timestamp for an age the view can print.
    ///
    /// Read-only: does not deliver anything or touch `last_seen`.
    ///
    /// # Errors
    ///
    /// `shutting_down` once the bus is closed.
    pub async fn overview(&self) -> Result<Overview, MailErr> {
        let now = now_ms();
        let state = self.open()?;
        let agents = state
            .agents_page(None, MAX_OVERVIEW_AGENTS)
            .0
            .iter()
            .map(|a| roster_row(state, a))
            .map(|row| AgentOverview {
                name: row.name,
                provider: row.provider,
                description: row.description,
                seen_secs_ago: (now / 1000).saturating_sub(row.last_seen_ms / 1000) as i64,
                queued: row.queued,
            })
            .collect();
        Ok(Overview {
            agents,
            agents_total: state.roster.len(),
            messages_sent: state.messages_sent(),
        })
    }

    /// Queue `body` for registered `to`. Validates body size/content and both
    /// roster entries (`from` first). At [`MAX_TOTAL_MESSAGES`] messages the
    /// oldest read message is deleted to make room. Refreshes the sender's
    /// `last_seen`, wakes the oldest parked pull only, and counts toward
    /// [`Overview::messages_sent`].
    pub async fn send(
        &self,
        from: &AgentName,
        to: &AgentName,
        body: String,
    ) -> Result<SendReceipt, MailErr> {
        if body.len() > MAX_BODY_BYTES || body.trim().is_empty() {
            return Err(MailErr::InvalidBody);
        }
        let body_json_len = json_len(&body);
        let body = body.into_boxed_str();
        let msg = self
            .open()?
            .send(from.as_str(), to.as_str(), body, body_json_len, now_ms())?;
        Ok(SendReceipt {
            id: msg.id.to_string(),
            from: msg.from.to_string(),
            to: msg.to.to_string(),
            room: msg.room.as_ref().map(ToString::to_string),
            created_at: rfc3339_ms(msg.created_ms),
        })
    }

    /// Pull (and optionally block for) the next batch of unread mail for
    /// `name`. Returned messages are marked read: `check_inbox` hands each
    /// one out once, and [`history`](Self::history) shows it again until
    /// [`Retention::read_ttl`] after it was read.
    ///
    /// Parks only when the inbox is empty and `wait_ms > 0`; timeout is an
    /// empty success. Dropping the returned future (the client went away)
    /// frees its parked slot and never swallows a wake meant for another
    /// pull of the same name.
    ///
    /// Fails [`crate::errors::MailboxError::TooManyWaiters`] when
    /// [`MAX_WAITERS_PER_AGENT`] pulls are already parked for this name.
    pub async fn check_inbox(&self, name: &AgentName, wait_ms: u32) -> Result<InboxPage, MailErr> {
        self.check_inbox_limit(name, wait_ms, None).await
    }

    /// [`check_inbox`](Self::check_inbox) with a page size: `Some(n)` returns
    /// up to `n` messages (1 to [`MAX_PAGE`]) within
    /// [`MAX_LIMITED_PAGE_BYTES`], the first always included; `None` is the
    /// usual page of [`MAX_BATCH`] messages within [`MAX_RETURN_BYTES`].
    pub async fn check_inbox_limit(
        &self,
        name: &AgentName,
        wait_ms: u32,
        limit: Option<usize>,
    ) -> Result<InboxPage, MailErr> {
        let page = Page::limited(limit);
        let (taken, mut signal) = self.pull_now(name, page)?;
        if wait_ms == 0 || taken.delivers() {
            return Ok(taken.into_page());
        }
        // Park until mail arrives or the deadline passes. A wake can find
        // nothing to deliver (another pull under the same name took it, or a
        // cancelled pull handed its wake on); that pull parks again for the
        // time left instead of returning empty early.
        let deadline = Instant::now() + Duration::from_millis(u64::from(wait_ms));
        loop {
            let retarget = {
                // Enabled before the inbox is checked: a send between the
                // check and the wait wakes this, so it is never missed.
                let notified = signal.notify.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                match self.pull_wait(name, &signal, deadline, page)? {
                    Pull::Ready(taken) => return Ok(taken.into_page()),
                    Pull::Retarget(fresh) => Some(fresh),
                    Pull::Park(_slot) => {
                        // Woken or timed out, the next pass takes whatever
                        // arrived and decides whether the deadline passed.
                        let _ = tokio::time::timeout_at(deadline, notified).await;
                        None
                    }
                }
            };
            if let Some(fresh) = retarget {
                signal = fresh;
            }
        }
    }

    /// Take `name`'s next batch of unread mail now (an empty one when there
    /// is none), and hand back the agent's signal for a pull that goes on to
    /// wait.
    fn pull_now(&self, name: &AgentName, page: Page) -> Result<(Taken, Arc<Signal>), MailErr> {
        let state = self.open()?;
        let agent = state.agent(name.as_str(), Key::fold(name.as_str()).as_str())?;
        let taken = self.take(state, &agent, name, page)?;
        Ok((
            taken.unwrap_or_else(|| Taken::empty(agent.name.clone())),
            agent.signal.clone(),
        ))
    }

    /// Stamp `agent` as seen and take its next batch, if any;
    /// `unregistered` when it left in the meantime.
    fn take(
        &self,
        state: &State,
        agent: &Arc<Agent>,
        name: &AgentName,
        page: Page,
    ) -> Result<Option<Taken>, MailErr> {
        match state.pull(agent, now_ms(), page) {
            None => Err(state.unregistered(name.as_str())),
            Some(batch) => Ok(batch.map(|batch| Taken {
                agent: agent.name.clone(),
                messages: batch.messages,
                remaining: batch.remaining,
                skipped: batch.skipped,
            })),
        }
    }

    /// One pass of a waiting pull: take mail if there is any, else say the
    /// deadline passed, else count the pull as parked.
    ///
    /// Fails `unregistered` when the agent has left, `shutting_down` once the
    /// bus is closed, and `too_many_waiters` when [`MAX_WAITERS_PER_AGENT`]
    /// pulls are parked already.
    fn pull_wait(
        &self,
        name: &AgentName,
        signal: &Arc<Signal>,
        deadline: Instant,
        page: Page,
    ) -> Result<Pull, MailErr> {
        let state = self.open()?;
        // The agent was waiting all along; taking also records when the pull
        // ended.
        let agent = state.agent(name.as_str(), Key::fold(name.as_str()).as_str())?;
        if !Arc::ptr_eq(&agent.signal, signal) {
            return Ok(Pull::Retarget(agent.signal.clone()));
        }
        // Counted as parked before the inbox is looked at, so a send that
        // lands in between wakes this pull (see `ParkGuard::try_new`).
        let slot = ParkGuard::try_new(signal.clone(), MAX_WAITERS_PER_AGENT);
        if let Some(taken) = self.take(state, &agent, name, page)? {
            return Ok(Pull::Ready(taken));
        }
        if Instant::now() >= deadline {
            return Ok(Pull::Ready(Taken::empty(agent.name.clone())));
        }
        match slot {
            Ok(slot) => Ok(Pull::Park(slot)),
            Err(waiting) => Err(MailErr::TooManyWaiters {
                agent: agent.name.to_string(),
                waiting,
            }),
        }
    }

    /// One page of `name`'s history since it registered: messages it sent
    /// (read or not, with `readAt` when the recipient has read them) and
    /// messages it has read. Unread incoming mail is left to
    /// [`check_inbox`](Self::check_inbox). `with` keeps only mail exchanged
    /// with that agent; `before` (a previous page's `next`) continues with
    /// older messages.
    ///
    /// Up to [`MAX_BATCH`] messages within [`MAX_RETURN_BYTES`] of message
    /// JSON, the newest that fit, returned oldest first. Refreshes
    /// `last_seen`; changes nothing else.
    ///
    /// # Errors
    ///
    /// `unregistered` when `name` is not on the roster; `shutting_down`.
    pub async fn history(
        &self,
        name: &AgentName,
        with: Option<&AgentName>,
        before: Option<i64>,
    ) -> Result<HistoryPage, MailErr> {
        let state = self.open()?;
        let agent = state.agent(name.as_str(), Key::fold(name.as_str()).as_str())?;
        {
            let mut st = agent.inner.lock();
            if st.removed {
                drop(st);
                return Err(state.unregistered(name.as_str()));
            }
            st.last_seen_ms = now_ms();
        }
        let with = with.map(AgentName::folded);
        let (rows, next) = history_rows(state, &agent, with.as_deref(), before);
        Ok(history_page(&agent.name, &rows, next))
    }

    /// Create the room `room` with `agent` as its first member. A private
    /// room is invite-only and left out of other agents' `list_rooms`; that
    /// is etiquette between cooperating local agents, not a security
    /// boundary, since any local process can claim any name.
    ///
    /// # Errors
    ///
    /// `unregistered`; `room_exists`; `rooms_full` at [`MAX_ROOMS`];
    /// `invalid_description`; `shutting_down`.
    pub async fn create_room(
        &self,
        agent: &AgentName,
        room: &RoomName,
        private: bool,
        description: Option<String>,
    ) -> Result<RoomInfo, MailErr> {
        let description = description
            .map(validate_description)
            .transpose()?
            .filter(|d| !d.is_empty());
        let (state, agent) = self.active(agent)?;
        state.create_room(&agent, room, private, description)
    }

    /// Join `room`: a public room takes anyone, a private one only an agent
    /// that was invited. Joining a room you are in changes nothing. New
    /// members start at the head of the room: `check_inbox` brings what is
    /// sent from now on, and `history` with `room` shows what the room still
    /// holds.
    ///
    /// # Errors
    ///
    /// `unregistered`; `room_not_found` (also for a private room the agent was
    /// not invited to); `shutting_down`.
    pub async fn join_room(&self, agent: &AgentName, room: &RoomName) -> Result<RoomInfo, MailErr> {
        let (state, agent) = self.active(agent)?;
        state.join_room(&agent, room)
    }

    /// Let `invitee` join `room`, which only a member may do. The invitee
    /// then joins with [`join_room`](Self::join_room); the invitation is the
    /// only way into a private room.
    ///
    /// # Errors
    ///
    /// `unregistered` (for `agent` or `invitee`); `room_not_found`;
    /// `not_a_member`; `shutting_down`.
    pub async fn invite(
        &self,
        agent: &AgentName,
        room: &RoomName,
        invitee: &AgentName,
    ) -> Result<RoomInfo, MailErr> {
        let (state, agent) = self.active(agent)?;
        let invitee = state.agent(invitee.as_str(), Key::fold(invitee.as_str()).as_str())?;
        state.invite(&agent, room, &invitee)
    }

    /// Leave `room`; the room is deleted with its last member.
    ///
    /// # Errors
    ///
    /// `unregistered`; `room_not_found`; `not_a_member`; `shutting_down`.
    pub async fn leave_room(
        &self,
        agent: &AgentName,
        room: &RoomName,
    ) -> Result<LeaveOutcome, MailErr> {
        let (state, agent) = self.active(agent)?;
        let deleted = state.leave_room(&agent, room)?;
        Ok(LeaveOutcome {
            room: room.to_string(),
            deleted,
        })
    }

    /// The rooms `agent` can see: every public room, and the private ones it
    /// is in or was invited to, sorted by name.
    pub async fn list_rooms(&self, agent: &AgentName) -> Result<RoomListing, MailErr> {
        self.list_rooms_page(agent, None, MAX_PAGE).await
    }

    /// One page of [`list_rooms`](Self::list_rooms): up to `limit` (at most
    /// [`MAX_PAGE`], at least 1) rooms after `after`, the `next` of the
    /// previous page.
    pub async fn list_rooms_page(
        &self,
        agent: &AgentName,
        after: Option<&RoomName>,
        limit: usize,
    ) -> Result<RoomListing, MailErr> {
        let (state, agent) = self.active(agent)?;
        let after = after.map(RoomName::folded);
        let (rooms, more) = state.list_rooms(&agent, after.as_deref(), limit.clamp(1, MAX_PAGE));
        let next = more.then(|| rooms.last().map(|r| r.room.clone())).flatten();
        Ok(RoomListing { rooms, next })
    }

    /// Send `body` to every other member of `room`, which `from` must be in.
    /// Validates the body like [`send`](Self::send). A room keeps its last
    /// messages ([`DEFAULT_ROOM_CAPACITY`]); a member that falls further
    /// behind than that is told how many it missed (`skipped`).
    ///
    /// # Errors
    ///
    /// `invalid_body`; `unregistered`; `room_not_found`; `not_a_member`;
    /// `shutting_down`.
    pub async fn send_room(
        &self,
        from: &AgentName,
        room: &RoomName,
        body: String,
    ) -> Result<SendReceipt, MailErr> {
        if body.len() > MAX_BODY_BYTES || body.trim().is_empty() {
            return Err(MailErr::InvalidBody);
        }
        let body_json_len = json_len(&body);
        let (state, sender) = self.active(from)?;
        let msg = state.send_room(
            &sender,
            room,
            body.into_boxed_str(),
            body_json_len,
            now_ms(),
        )?;
        Ok(SendReceipt {
            id: msg.id.to_string(),
            from: msg.from.to_string(),
            to: String::new(),
            room: msg.room.as_ref().map(ToString::to_string),
            created_at: rfc3339_ms(msg.created_ms),
        })
    }

    /// One page of what `room` still holds, for a member: the newest
    /// messages (older than `before`, a previous page's `next`), up to
    /// [`MAX_BATCH`] within [`MAX_RETURN_BYTES`], oldest first. Reads
    /// everything in the room, including what the agent has not taken with
    /// `check_inbox`; changes nothing but `last_seen`.
    ///
    /// # Errors
    ///
    /// `unregistered`; `room_not_found`; `not_a_member`; `shutting_down`.
    pub async fn room_history(
        &self,
        agent: &AgentName,
        room: &RoomName,
        before: Option<i64>,
    ) -> Result<HistoryPage, MailErr> {
        let (state, agent) = self.active(agent)?;
        let (rows, next) = state.room_history(&agent, room, before)?;
        Ok(history_page(&agent.name, &rows, next))
    }

    /// The open state and the agent `name`, refreshed as seen now:
    /// `unregistered` when it is not on the roster.
    fn active(&self, name: &AgentName) -> Result<(&State, Arc<Agent>), MailErr> {
        let state = self.open()?;
        let agent = state.agent(name.as_str(), Key::fold(name.as_str()).as_str())?;
        {
            let mut st = agent.inner.lock();
            if st.removed {
                drop(st);
                return Err(state.unregistered(name.as_str()));
            }
            st.last_seen_ms = now_ms();
        }
        Ok((state, agent))
    }

    /// Delete mail read longer than [`Retention::read_ttl`] ago, and agents
    /// with no activity within [`Retention::agent_ttl`] (with the mail
    /// addressed to them), as of now. Unread mail is never expired. The
    /// daemon runs this on a timer; see [`sweep_at`](Self::sweep_at).
    pub async fn sweep(&self) -> Result<SweepOutcome, MailErr> {
        self.sweep_at(SystemTime::now()).await
    }

    /// [`sweep`](Self::sweep) as of `now` (injectable for tests).
    ///
    /// An agent with a pull parked right now is never dropped, however old
    /// its `last_seen`. A dropped agent's name is held for
    /// [`Retention::name_hold`] from `now`, and holds that ended by `now`
    /// are forgotten.
    pub async fn sweep_at(&self, now: SystemTime) -> Result<SweepOutcome, MailErr> {
        let state = self.open()?;
        let now_ms = unix_ms(now);
        let cutoff = |ttl: Duration| now_ms.saturating_sub(ttl.as_millis() as u64);
        let expired_read = state.expire_read(cutoff(self.retention.read_ttl));
        let idle_since = cutoff(self.retention.agent_ttl);
        let mut stale: Vec<String> = state
            .roster
            .entries()
            .into_iter()
            .filter(|(_, a)| a.signal.parked() == 0 && a.inner.lock().last_seen_ms < idle_since)
            .map(|(key, _)| key)
            .collect();
        stale.sort_unstable();
        state.holds.lock().retain(|_, h| h.until_ms > now_ms);
        let until_ms = now_ms + self.retention.name_hold.as_millis() as u64;
        let mut out = SweepOutcome {
            expired_read,
            ..SweepOutcome::default()
        };
        for key in stale {
            if let Some((agent, discarded)) = state.remove_agent(&key, until_ms) {
                out.discarded += discarded;
                out.dropped_agents.push(agent.name.to_string());
            }
        }
        if !out.is_empty() {
            state.compact_convos();
        }
        state.prune_invites();
        Ok(out)
    }

    /// Roster size and unread and read message counts, for `/health`. Does
    /// not deliver anything or touch `last_seen`.
    pub async fn stats(&self) -> Result<Stats, MailErr> {
        let state = self.open()?;
        let unread = state.unread.0.load(Ordering::Relaxed);
        Ok(Stats {
            agents: state.roster.len() as i64,
            unread: unread as i64,
            read: state.total.0.load(Ordering::Relaxed).saturating_sub(unread) as i64,
        })
    }

    /// Delete every agent, message and held name. The daemon runs this at
    /// startup, which also clears whatever a persisted run left. Parked pulls
    /// fail `unregistered`. Returns `(messages, agents)` removed.
    pub async fn clear(&self) -> Result<(i64, i64), MailErr> {
        Ok(self.open()?.clear())
    }

    /// Mark the bus closed: every parked waiter fails `shutting_down` and no
    /// further operations are accepted. Then discard the whole mailbox,
    /// roster and every message, read or not, so nothing outlives the server
    /// run. Nothing was ever written to disk, so there is nothing to scrub.
    ///
    /// Never fails; it returns a `Result` to keep the signature of the
    /// engine it replaced.
    pub async fn shutdown(&self) -> Result<(), MailErr> {
        self.state.closed.store(true, Ordering::Relaxed);
        self.state.release_all_streams();
        self.state.clear();
        Ok(())
    }

    /// Mark the bus closed like [`shutdown`](Self::shutdown) but keep the
    /// roster and mail: write them to `{data_dir}/snapshot.json`
    /// (`MENTISCHAT_PERSIST=1`), where the next [`Bus::new`] finds them. A
    /// failure to write is logged and loses the mailbox, which is all a
    /// crash would have done.
    pub async fn close(&self) {
        self.state.closed.store(true, Ordering::Relaxed);
        self.state.wake_all();
        // Leases go back before the snapshot is taken, so what streams had in
        // flight is in it as unread mail.
        self.state.release_all_streams();
        let snapshot = snapshot::capture(&self.state);
        if let Err(e) = snapshot::save(&self.data_dir, &snapshot) {
            tracing::error!(error = %e, "could not write the mailbox snapshot");
        }
    }
}

/// Trims a register description and enforces [`MAX_DESCRIPTION_BYTES`] and
/// no control characters; `""` stays `""` (it clears the stored one).
fn validate_description(raw: String) -> Result<String, MailErr> {
    let trimmed = raw.trim();
    if trimmed.len() > MAX_DESCRIPTION_BYTES || trimmed.chars().any(char::is_control) {
        return Err(MailErr::InvalidDescription);
    }
    Ok(trimmed.to_string())
}

/// The outcome of registering `agent`, as stored.
fn register_outcome(agent: &Agent, created: bool) -> RegisterOutcome {
    let st = agent.inner.lock();
    RegisterOutcome {
        agent: agent.name.to_string(),
        created,
        last_seen: rfc3339_ms(st.last_seen_ms),
        provider: st.provider.clone(),
        description: st.description.clone(),
    }
}

/// Refresh a registered agent's `last_seen`, and its provider and
/// description when given (`None` keeps the stored value; an empty
/// description clears it).
fn refresh_agent(
    agent: &Agent,
    now: u64,
    provider: Option<String>,
    description: Option<String>,
) -> RegisterOutcome {
    {
        let mut st = agent.inner.lock();
        st.last_seen_ms = now;
        if provider.is_some() {
            st.provider = provider;
        }
        if let Some(description) = description {
            st.description = Some(description).filter(|d| !d.is_empty());
        }
    }
    register_outcome(agent, false)
}

/// One roster row with everything `list_agents` and `overview` need.
struct RosterRow {
    name: String,
    provider: Option<String>,
    description: Option<String>,
    last_seen_ms: u64,
    queued: i64,
}

fn roster_row(state: &State, agent: &Agent) -> RosterRow {
    let st = agent.inner.lock();
    RosterRow {
        name: agent.name.to_string(),
        provider: st.provider.clone(),
        description: st.description.clone(),
        last_seen_ms: st.last_seen_ms,
        queued: (st.unread.len() + st.leased_direct() + state.room_unread(agent, &st.rooms)) as i64,
    }
}
