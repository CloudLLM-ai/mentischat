//! Parked `check_inbox` pulls: one [`Signal`] per agent, a [`Notify`] senders
//! poke and a count of the pulls waiting on it.

use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::sync::Arc;

use tokio::sync::Notify;

/// What a send wakes. Lives as long as its agent is on the roster; a pull
/// holds an [`Arc`] to it so it can wait without holding the bus lock.
#[derive(Debug, Default)]
pub(super) struct Signal {
    /// Woken with `notify_one` per send (the oldest parked pull only) and
    /// with `notify_waiters` when the agent leaves or the bus closes. A pull
    /// creates and enables its `Notified` before it checks the inbox, so a
    /// send that lands in between is never missed; a `notify_one` with no
    /// one waiting is kept as a permit for the next pull.
    pub notify: Notify,
    /// Pulls parked right now. Raised under the bus lock (so the cap holds)
    /// and lowered by [`ParkGuard`] from wherever the pull ends.
    parked: AtomicUsize,
}

impl Signal {
    /// Pulls parked right now.
    pub(super) fn parked(&self) -> usize {
        self.parked.load(Relaxed)
    }
}

/// One parked pull. Dropping it, however the pull ends (woken, timed out,
/// or its client went away and the future was dropped), frees its slot
/// toward `MAX_WAITERS_PER_AGENT` and stops it counting as parked.
///
/// A pull dropped after a send had woken it does not swallow that wake:
/// `Notify` hands a `notify_one` that reached a dropped `Notified` to the
/// next waiter, or keeps it as a permit when there is none.
#[derive(Debug)]
pub(super) struct ParkGuard {
    signal: Arc<Signal>,
}

impl ParkGuard {
    /// Count a pull as parked on `signal`.
    pub(super) fn new(signal: Arc<Signal>) -> Self {
        signal.parked.fetch_add(1, Relaxed);
        Self { signal }
    }
}

impl Drop for ParkGuard {
    fn drop(&mut self) {
        self.signal.parked.fetch_sub(1, Relaxed);
    }
}
