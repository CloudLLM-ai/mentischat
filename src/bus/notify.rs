//! Notify sockets: a read-only way to learn that mail is waiting, so an idle
//! agent can be woken without anything being delivered to it.
//!
//! A [`NotifySession`] is one open `GET /notify` socket. It is *not* a
//! stream: it never leases, reads, acknowledges or marks anything, so mail
//! stays unread for `check_inbox`, which still hands each message out
//! exactly once. All it does is
//!
//! - count as something attending to the agent's mail: a send to the agent
//!   reports `notified` in its receipt, and the idle sweep leaves the agent
//!   on the roster, as for a parked pull;
//! - hear every send to the agent (through a [`tokio::sync::watch`] counter
//!   in the agent's signal, so it never takes a wake away from a parked pull
//!   or a stream, which share a [`tokio::sync::Notify`] that wakes only
//!   one);
//! - summarize what is waiting ([`Bus::notify_digest`]): the count, who from,
//!   which rooms, and the newest id. Never a body, so secure-room ciphertext
//!   and the delivery contract are untouched.
//!
//! Up to [`MAX_NOTIFY_PER_AGENT`] sockets may be open for one name; they
//! coexist with each other, with a stream and with parked pulls.

use std::sync::Arc;

use tokio::sync::watch;

use super::park::WatchGuard;
use super::state::{Agent, Key, State};
use super::{Bus, MailErr, MAX_DIGEST_NAMES, MAX_NOTIFY_PER_AGENT};
use crate::names::AgentName;

/// What is waiting for an agent in `check_inbox`, as a notify frame reports
/// it: a summary, never message bodies.
///
/// Counts what `check_inbox` would return now. Mail a stream has leased is
/// not counted (the stream's consumer has it), and mail the agent sent itself
/// never is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MailDigest {
    /// Unread messages, direct and room together. Exact.
    pub unread: usize,
    /// Up to [`MAX_DIGEST_NAMES`] distinct senders, direct mail first, then
    /// room mail by room.
    pub from: Vec<String>,
    /// Up to [`MAX_DIGEST_NAMES`] names of rooms with unread mail, as the
    /// rooms were created.
    pub rooms: Vec<String>,
    /// Id of the newest unread message (0 when `unread` is 0). Two digests
    /// with the same `newest` and no `check_inbox` between them describe the
    /// same mail, which is how a socket avoids telling the agent twice.
    pub newest: u64,
}

/// One open notify socket for an agent. Dropping it, however the socket ends,
/// frees its slot and stops it counting as attending to the agent's mail.
#[derive(Debug)]
pub struct NotifySession {
    agent: Arc<Agent>,
    _watch: WatchGuard,
}

impl NotifySession {
    /// The agent's name as registered.
    pub fn name(&self) -> &str {
        &self.agent.name
    }

    /// A receiver that resolves ([`watch::Receiver::changed`]) when something
    /// happened to the agent since it last looked: mail landed, the agent
    /// left the roster, or the bus closed. Several events between two looks
    /// resolve once, which is the first level of coalescing. It starts out
    /// having seen everything so far, so take it *before* the first
    /// [`Bus::notify_digest`]: a send in between is then not missed.
    pub fn events(&self) -> watch::Receiver<u64> {
        self.agent.signal.subscribe()
    }
}

impl Bus {
    /// Open a notify socket for `name`.
    ///
    /// Counts as attending to the agent's mail from now on. Takes nothing
    /// from the inbox. Also marks the agent as seen, so the idle sweep
    /// cannot drop it between sockets (see [`Bus::notify_touch`]).
    ///
    /// # Errors
    ///
    /// `unregistered`; `shutting_down`; `too_many_waiters` when
    /// [`MAX_NOTIFY_PER_AGENT`] notify sockets are already open for this name.
    pub fn open_notify(&self, name: &AgentName) -> Result<NotifySession, MailErr> {
        let state = self.open()?;
        let agent = state.agent(name.as_str(), Key::fold(name.as_str()).as_str())?;
        let watch =
            WatchGuard::try_new(agent.signal.clone(), MAX_NOTIFY_PER_AGENT).map_err(|open| {
                MailErr::TooManyWaiters {
                    agent: agent.name.to_string(),
                    waiting: open,
                }
            })?;
        self.notify_touch_agent(&agent);
        Ok(NotifySession {
            agent,
            _watch: watch,
        })
    }

    /// Summarize the mail waiting for `session`'s agent without taking,
    /// leasing, reading or marking any of it.
    ///
    /// # Errors
    ///
    /// `shutting_down`; `unregistered` when the agent has left the roster.
    pub fn notify_digest(&self, session: &NotifySession) -> Result<MailDigest, MailErr> {
        let state = self.open()?;
        state
            .digest(&session.agent)
            .ok_or_else(|| state.unregistered(&session.agent.name))
    }

    /// Mark `session`'s agent as seen now. A socket calls this on every ping
    /// so that, when it closes, the agent's last-seen time is recent and the
    /// idle sweep does not drop the agent in the gap before it re-arms.
    pub fn notify_touch(&self, session: &NotifySession) {
        self.notify_touch_agent(&session.agent);
    }

    fn notify_touch_agent(&self, agent: &Agent) {
        let mut st = agent.inner.lock();
        if !st.removed {
            st.last_seen_ms = self.now_ms();
        }
    }
}

impl State {
    /// [`MailDigest`] of `agent`'s unread mail; `None` when it has left.
    ///
    /// Locks the agent, copies what it needs, and releases it before looking
    /// at any room (a room's lock is taken after an agent's, never held
    /// across another).
    fn digest(&self, agent: &Arc<Agent>) -> Option<MailDigest> {
        let (direct, rooms) = {
            let st = agent.inner.lock();
            if st.removed {
                return None;
            }
            let direct: Vec<(u64, Arc<str>)> =
                st.unread.iter().map(|m| (m.id, m.from.clone())).collect();
            (direct, st.rooms.clone())
        };
        let mut digest = MailDigest {
            unread: direct.len(),
            from: Vec::new(),
            rooms: Vec::new(),
            newest: direct.last().map_or(0, |(id, _)| *id),
        };
        let add_sender = |from: &mut Vec<String>, sender: &str| {
            if from.len() < MAX_DIGEST_NAMES && !from.iter().any(|f| f.eq_ignore_ascii_case(sender))
            {
                from.push(sender.to_string());
            }
        };
        for (_, from) in &direct {
            add_sender(&mut digest.from, from);
        }
        for room in rooms {
            let Some(seen) = room.digest(agent) else {
                continue;
            };
            digest.unread += seen.unread;
            digest.newest = digest.newest.max(seen.newest);
            if digest.rooms.len() < MAX_DIGEST_NAMES {
                digest.rooms.push(room.name.to_string());
            }
            for sender in &seen.senders {
                add_sender(&mut digest.from, sender);
            }
        }
        Some(digest)
    }
}
