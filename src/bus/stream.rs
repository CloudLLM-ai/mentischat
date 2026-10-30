//! Streams: a long-lived delivery path for one agent, used by the WebSocket
//! route. Mail is pushed as it lands instead of waiting for a pull, without
//! weakening "each message is handed out once".
//!
//! A pushed message is *leased*, not read: direct mail moves from the agent's
//! unread queue to its lease list (still unread, so `history` shows it as
//! the recipient's unread mail and the per-name cap still counts it), and a
//! room message is taken by the member's cursor with its id remembered.
//! `check_inbox` never sees leased mail. An acknowledgement (cumulative, by
//! id) marks the direct mail read at the ack time and forgets the leases.
//! When the stream ends, however it ends, what was not acknowledged goes
//! back: direct mail to the front of the unread queue in order, room mail by
//! winding the cursor back (messages the room pushed out meanwhile count as
//! `skipped`). At most [`MAX_STREAM_IN_FLIGHT`] messages and
//! [`MAX_STREAM_BYTES`] bytes are in flight, so a slow reader costs a bounded
//! amount and pushing pauses until acks free the window.
//!
//! A stream counts as a parked pull for as long as it lives: sends wake it
//! through the agent's `Notify`, the idle sweep leaves its agent alone, and,
//! as for pulls, it is counted before it looks for mail so no send is missed.
//! A new stream for the same agent takes over: it takes the old stream's
//! leases back first and the old stream's later calls do nothing.

use std::sync::Arc;

use tokio::sync::futures::Notified;

use super::clock::now_ms;
use super::park::ParkGuard;
use super::state::{Agent, AgentState, Key, Page, State, Take};
use super::{Bus, MailErr, Message, MAX_STREAM_BYTES, MAX_STREAM_IN_FLIGHT};
use crate::names::AgentName;

/// One open stream. Dropping it returns whatever it had not had acknowledged.
pub struct StreamSession {
    agent: Arc<Agent>,
    epoch: u64,
    _parked: ParkGuard,
}

/// What a push carries.
#[derive(Debug)]
pub struct Push {
    /// Direct and room messages together, oldest first by id, none read yet.
    pub messages: Vec<Message>,
    /// Room messages lost to rooms filling up since the stream last heard.
    pub skipped: u64,
}

impl StreamSession {
    /// The agent's name as registered.
    pub fn name(&self) -> &str {
        &self.agent.name
    }

    /// Completes when mail may have landed. Create and `enable` it before
    /// calling [`Bus::stream_push`], so a send in between is not missed.
    pub fn wake(&self) -> Notified<'_> {
        self.agent.signal.notify.notified()
    }

    /// Completes when a newer stream takes over. Create and `enable` it before
    /// checking [`taken_over`](Self::taken_over).
    pub fn takeover(&self) -> Notified<'_> {
        self.agent.takeover.notified()
    }

    /// True once a newer stream for the same agent has taken over.
    pub fn taken_over(&self) -> bool {
        self.agent.inner.lock().stream_epoch != self.epoch
    }
}

impl Drop for StreamSession {
    fn drop(&mut self) {
        let mut st = self.agent.inner.lock();
        if st.stream_epoch == self.epoch {
            release(&self.agent, &mut st);
        }
    }
}

/// Return every lease of `agent` (its state `st` locked by the caller): direct
/// mail to the front of the unread queue in id order, room mail by rewinding
/// the member's cursor.
fn release(agent: &Agent, st: &mut AgentState) {
    let leased = std::mem::take(&mut st.leased);
    st.leased_bytes = 0;
    let mut direct = Vec::new();
    let mut rooms: Vec<(Arc<super::rooms::Room>, Vec<u64>)> = Vec::new();
    for lease in leased {
        match lease.room {
            None => direct.push(lease.msg),
            Some(room) => match rooms.iter_mut().find(|(r, _)| Arc::ptr_eq(r, &room)) {
                Some((_, ids)) => ids.push(lease.msg.id),
                None => rooms.push((room, vec![lease.msg.id])),
            },
        }
    }
    direct.sort_unstable_by_key(|m| m.id);
    for msg in direct.into_iter().rev() {
        st.unread.push_front(msg);
    }
    for (room, mut ids) in rooms {
        ids.sort_unstable();
        room.rewind(agent, &ids);
    }
}

impl State {
    /// Start a stream for `agent`, taking the leases of any stream it had.
    /// Returns the new stream's epoch.
    fn stream_open(&self, agent: &Agent) -> Option<u64> {
        let epoch = {
            let mut st = agent.inner.lock();
            if st.removed {
                return None;
            }
            release(agent, &mut st);
            st.stream_epoch += 1;
            st.stream_epoch
        };
        agent.takeover.notify_waiters();
        Some(epoch)
    }

    /// Lease the next messages for the stream `epoch` of `agent` within what
    /// is left of the in-flight window. `Err(())` when the agent has left.
    fn stream_lease(
        &self,
        agent: &Arc<Agent>,
        epoch: u64,
    ) -> Result<Option<super::state::Batch>, ()> {
        let mut st = agent.inner.lock();
        if st.removed {
            return Err(());
        }
        let items = MAX_STREAM_IN_FLIGHT.saturating_sub(st.leased.len());
        let bytes = MAX_STREAM_BYTES.saturating_sub(st.leased_bytes);
        if st.stream_epoch != epoch || items == 0 || bytes == 0 {
            return Ok(None);
        }
        let now = now_ms();
        st.last_seen_ms = now;
        let rooms = st.rooms.clone();
        Ok(self.take_merged(agent, &mut st, &rooms, Take::Lease, Page { items, bytes }))
    }

    /// Acknowledge everything leased to the stream `epoch` with an id up to
    /// `up_to`: direct mail becomes read now. Returns how many.
    fn stream_ack(&self, agent: &Arc<Agent>, epoch: u64, up_to: u64) -> usize {
        let mut st = agent.inner.lock();
        if st.removed || st.stream_epoch != epoch {
            return 0;
        }
        let now = now_ms();
        st.last_seen_ms = now;
        let (acked, kept): (Vec<_>, Vec<_>) = std::mem::take(&mut st.leased)
            .into_iter()
            .partition(|l| l.msg.id <= up_to);
        st.leased_bytes = kept.iter().map(|l| l.msg.page_len()).sum();
        st.leased = kept;
        let direct: Vec<Arc<super::log::Msg>> = acked
            .iter()
            .filter(|l| l.room.is_none())
            .map(|l| l.msg.clone())
            .collect();
        self.mark_read(agent, &mut st, &direct, now);
        acked.len()
    }

    /// End every stream: take back all leases and invalidate the streams, so a
    /// snapshot written next holds the mail they had in flight.
    pub(super) fn release_all_streams(&self) {
        self.roster.for_each(|agent| {
            let mut st = agent.inner.lock();
            if st.stream_epoch != 0 || !st.leased.is_empty() {
                release(agent, &mut st);
                st.stream_epoch += 1;
            }
            drop(st);
            agent.takeover.notify_waiters();
        });
    }
}

impl Bus {
    /// True once the bus has been closed or shut down.
    pub fn is_closed(&self) -> bool {
        self.state.closed.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Fail `unregistered` unless `name` is on the roster: the check a stream
    /// route makes before it upgrades the connection.
    pub fn check_registered(&self, name: &AgentName) -> Result<(), MailErr> {
        let state = self.open()?;
        state
            .agent(name.as_str(), Key::fold(name.as_str()).as_str())
            .map(|_| ())
    }

    /// Open a stream for `name`: its sends and pushes go through the returned
    /// session, and a stream it already had is taken over (its leases are
    /// returned first). Counts as a parked pull for as long as it lives.
    ///
    /// # Errors
    ///
    /// `unregistered`; `shutting_down`.
    pub fn open_stream(&self, name: &AgentName) -> Result<StreamSession, MailErr> {
        let state = self.open()?;
        let agent = state.agent(name.as_str(), Key::fold(name.as_str()).as_str())?;
        let parked = ParkGuard::held(agent.signal.clone());
        let epoch = state
            .stream_open(&agent)
            .ok_or_else(|| state.unregistered(name.as_str()))?;
        Ok(StreamSession {
            agent,
            epoch,
            _parked: parked,
        })
    }

    /// Lease and return the next messages for `session`, up to the room left
    /// in its in-flight window; `None` when there is nothing to push (or the
    /// window is full, or the session was taken over).
    ///
    /// # Errors
    ///
    /// `shutting_down`; `unregistered` when the agent has left.
    pub fn stream_push(&self, session: &StreamSession) -> Result<Option<Push>, MailErr> {
        let state = self.open()?;
        let leased = state
            .stream_lease(&session.agent, session.epoch)
            .map_err(|()| state.unregistered(&session.agent.name))?;
        Ok(leased.map(|batch| Push {
            messages: batch.messages.iter().map(|m| m.to_wire()).collect(),
            skipped: batch.skipped,
        }))
    }

    /// Acknowledge, cumulatively, every message pushed to `session` with an
    /// id up to `up_to`: direct mail is marked read now, and the window frees
    /// for more. Returns how many messages that acknowledged.
    ///
    /// # Errors
    ///
    /// `shutting_down`.
    pub fn stream_ack(&self, session: &StreamSession, up_to: u64) -> Result<usize, MailErr> {
        let state = self.open()?;
        Ok(state.stream_ack(&session.agent, session.epoch, up_to))
    }
}
