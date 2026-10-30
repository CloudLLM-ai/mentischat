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
use crate::names::AgentName;

mod clock;
mod delivery;
mod log;
mod model;
mod park;
mod snapshot;
mod state;

pub use clock::{now_rfc3339, rfc3339};
pub use model::{
    parse_duration, AgentOverview, DiscardOutcome, HistoryPage, InboxPage, Message, Overview,
    RegisterOutcome, Registration, Retention, RosterEntry, RosterListing, SendReceipt, Stats,
    SweepOutcome, MAX_BATCH, MAX_BODY_BYTES, MAX_DESCRIPTION_BYTES, MAX_PENDING_PER_AGENT,
    MAX_RETURN_BYTES, MAX_ROSTER, MAX_TOTAL_MESSAGES, MAX_WAITERS_PER_AGENT, MAX_WAIT_MS,
};
pub use snapshot::{remove_snapshot, SnapshotError};

use clock::{now_ms, rfc3339_ms, unix_ms};
use delivery::{history_page, history_rows};
use log::{json_len, Msg};
use park::{ParkGuard, Signal};
use state::{Agent, Key, State};

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
}

impl Taken {
    fn into_page(self) -> InboxPage {
        InboxPage {
            agent: self.agent.to_string(),
            messages: self.messages.iter().map(|m| m.to_wire()).collect(),
            remaining: self.remaining as i64,
        }
    }

    fn empty(agent: Arc<str>) -> Self {
        Self {
            agent,
            messages: Vec::new(),
            remaining: 0,
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
        state.roster.insert(key.clone(), Arc::new(agent));
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
        let state = self.open()?;
        let rows: Vec<_> = state
            .agents_sorted()
            .iter()
            .map(|a| roster_row(a))
            .collect();
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
            suggestions,
        })
    }

    /// The roster with each agent's provider, description, unread count and
    /// last-seen age, plus the number of messages sent since the daemon
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
            .agents_sorted()
            .iter()
            .map(|a| roster_row(a))
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
        let (taken, mut signal) = self.pull_now(name)?;
        if wait_ms == 0 || !taken.messages.is_empty() {
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
                match self.pull_wait(name, &signal, deadline)? {
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
    fn pull_now(&self, name: &AgentName) -> Result<(Taken, Arc<Signal>), MailErr> {
        let state = self.open()?;
        let agent = state.agent(name.as_str(), Key::fold(name.as_str()).as_str())?;
        let taken = self.take(state, &agent, name)?;
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
    ) -> Result<Option<Taken>, MailErr> {
        match state.pull(agent, now_ms()) {
            None => Err(state.unregistered(name.as_str())),
            Some(batch) => Ok(batch.map(|(messages, remaining)| Taken {
                agent: agent.name.clone(),
                messages,
                remaining,
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
    ) -> Result<Pull, MailErr> {
        let state = self.open()?;
        // The agent was waiting all along; taking also records when the pull
        // ended.
        let agent = state.agent(name.as_str(), Key::fold(name.as_str()).as_str())?;
        if !Arc::ptr_eq(&agent.signal, signal) {
            return Ok(Pull::Retarget(agent.signal.clone()));
        }
        if let Some(taken) = self.take(state, &agent, name)? {
            return Ok(Pull::Ready(taken));
        }
        if Instant::now() >= deadline {
            return Ok(Pull::Ready(Taken::empty(agent.name.clone())));
        }
        // Raised only here, so a burst of pulls can overshoot the cap by the
        // number racing; the guard counts every one that parks.
        let waiting = signal.parked();
        if waiting >= MAX_WAITERS_PER_AGENT {
            return Err(MailErr::TooManyWaiters {
                agent: agent.name.to_string(),
                waiting,
            });
        }
        Ok(Pull::Park(ParkGuard::new(signal.clone())))
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

fn roster_row(agent: &Agent) -> RosterRow {
    let st = agent.inner.lock();
    RosterRow {
        name: agent.name.to_string(),
        provider: st.provider.clone(),
        description: st.description.clone(),
        last_seen_ms: st.last_seen_ms,
        queued: st.unread.len() as i64,
    }
}
