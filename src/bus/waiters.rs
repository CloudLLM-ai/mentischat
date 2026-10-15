//! Parked `check_inbox` pulls: a waiter parks on a one-shot channel under the
//! bus lock and is woken by `send`, so the send/timeout race cannot lose mail.

use std::collections::{HashMap, VecDeque};

use tokio::sync::oneshot;

use super::{Inner, MailErr, MAX_WAITERS_PER_AGENT};

pub(super) struct Waiter {
    id: u64,
    tx: oneshot::Sender<()>,
}

pub(super) type WaiterMap = HashMap<String, VecDeque<Waiter>>;

/// Deliver one wake to `q`, discarding canceled waiters until a live
/// receiver takes the signal (or the queue empties).
pub(super) fn wake_one(q: &mut VecDeque<Waiter>) {
    while let Some(w) = q.pop_front() {
        if w.tx.send(()).is_ok() {
            return;
        }
    }
}

/// Park a fresh waiter for `name`, enforcing the per-name cap.
///
/// Returns `(waiter id, receiver)`; the caller awaits the receiver outside
/// the bus lock and must later call [`unpark`] with the id.
///
/// A pull whose client disconnected is dropped mid-await and never reaches
/// [`unpark`]; its closed sender is pruned here so abandoned pulls cannot
/// fill the cap and lock the name out of blocking pulls.
pub(super) fn try_park(g: &mut Inner, name: &str) -> Result<(u64, oneshot::Receiver<()>), MailErr> {
    if let Some(q) = g.waiters.get_mut(name) {
        q.retain(|w| !w.tx.is_closed());
    }
    let waiting = g.waiters.get(name).map_or(0, VecDeque::len);
    if waiting >= MAX_WAITERS_PER_AGENT {
        return Err(MailErr::TooManyWaiters {
            agent: name.to_string(),
            waiting,
        });
    }
    let id = g.next_waiter_id;
    g.next_waiter_id += 1;
    let (tx, rx) = oneshot::channel();
    g.waiters
        .entry(name.to_string())
        .or_default()
        .push_back(Waiter { id, tx });
    Ok((id, rx))
}

/// Remove a settled waiter (woken, timed out, or canceled) and drop its
/// queue when empty so idle keys cannot accumulate.
pub(super) fn unpark(g: &mut Inner, name: &str, id: u64) {
    if let Some(q) = g.waiters.get_mut(name) {
        q.retain(|w| w.id != id);
        if q.is_empty() {
            g.waiters.remove(name);
        }
    }
}
