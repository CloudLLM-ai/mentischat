//! The mailbox: roster, caps, ack/batch delivery, and blocking pulls.
//!
//! All state sits behind a single [`tokio::sync::Mutex`]; sqlite writes run
//! inside the lock so waiters and rows cannot tear. The lock is **never**
//! held across the park await — a waiter parks on a one-shot channel and
//! re-acquires to deliver, which is what makes the send/timeout race safe.

use std::collections::{HashMap, VecDeque};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::params;
use serde::Serialize;
use tokio::sync::{oneshot, Mutex};

pub use crate::store::StoreError;

type MailErr = crate::errors::MailboxError;

/// Maximum UTF-8 byte length of a message body.
pub const MAX_BODY_BYTES: usize = 64 * 1024;
/// Per-recipient cap on unacked rows (`pending` + `in_flight`).
pub const MAX_PENDING_PER_AGENT: i64 = 1024;
/// Global cap on rows in `messages`.
pub const MAX_TOTAL_MESSAGES: i64 = 8192;
/// Max messages returned by one `check_inbox`.
pub const MAX_BATCH: i64 = 8;
/// Max summed body bytes returned by one `check_inbox` (first message always included).
pub const MAX_RETURN_BYTES: usize = 16 * 1024;
/// Upper bound of `wait_ms`.
pub const MAX_WAIT_MS: u32 = 30_000;
/// Max parked pulls per name; an additional park fails.
pub const MAX_WAITERS_PER_AGENT: usize = 8;
/// Cap on roster rows — bounds the number of parked pulls (8 per name).
pub const MAX_ROSTER: i64 = 256;
/// Cap on ids accepted in one `ack_ids` array.
pub const MAX_ACK_IDS: usize = MAX_PENDING_PER_AGENT as usize;

/// A delivered or queued mail row (wire shape).
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Message {
    /// Unique id (uuid v4).
    pub id: String,
    /// Sender roster name.
    pub from: String,
    /// Recipient roster name.
    pub to: String,
    /// Body exactly as stored (never trimmed).
    pub body: String,
    /// UTC RFC 3339 creation timestamp.
    pub created_at: String,
}

/// Send receipt handed back to the sender.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SendReceipt {
    /// Id of the queued message.
    pub id: String,
    /// Echoed sender name.
    pub from: String,
    /// Echoed recipient name.
    pub to: String,
    /// UTC RFC 3339 creation timestamp.
    pub created_at: String,
}

/// Register outcome.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RegisterOutcome {
    /// The registered name.
    pub agent: String,
    /// True only when the name was not already on the roster.
    pub created: bool,
    /// Fresh last-seen timestamp.
    pub last_seen: String,
}

/// Roster entry.
#[derive(Debug, Clone, Serialize)]
pub struct RosterEntry {
    /// Agent name.
    pub name: String,
    /// UTC RFC 3339 timestamp of the last register / check_inbox.
    #[serde(rename = "lastSeen")]
    pub last_seen: String,
}

/// One roster row as the terminal view shows it: who is registered, how
/// recently it was seen, and how much undelivered mail it holds.
///
/// Built by [`Bus::overview`]. Read-only: producing it never touches
/// `last_seen` or message state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentOverview {
    /// Agent name.
    pub name: String,
    /// Whole seconds since the last `register` / `check_inbox`, never negative.
    pub seen_secs_ago: i64,
    /// Undelivered messages addressed to this agent (`pending` plus `in_flight`).
    pub queued: i64,
}

/// The whole picture the terminal view needs in one read: the roster and how
/// many messages have been sent since the daemon started.
///
/// Built by [`Bus::overview`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Overview {
    /// Registered agents, sorted by name.
    pub agents: Vec<AgentOverview>,
    /// Messages accepted by `send` since this daemon process started. Acked,
    /// discarded and wiped mail still counts; the number never goes down and
    /// restarts at 0 with the process.
    pub messages_sent: u64,
}

/// Deregister outcome.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DiscardOutcome {
    /// The removed name.
    pub agent: String,
    /// Undelivered inbox messages dropped with it.
    pub discarded: i64,
}

/// One `check_inbox` result page.
#[derive(Debug, Serialize)]
pub struct InboxPage {
    /// Pulled name.
    pub agent: String,
    /// Delivered messages (at-least-once; ack by id on a later pull).
    pub messages: Vec<Message>,
    /// Unacked rows not included in this page (leftover `in_flight` +
    /// still-`pending`); drain while `> 0`.
    pub remaining: i64,
}

struct Waiter {
    id: u64,
    tx: oneshot::Sender<()>,
}

type WaiterMap = HashMap<String, VecDeque<Waiter>>;

struct Inner {
    conn: rusqlite::Connection,
    waiters: WaiterMap,
    next_waiter_id: u64,
    messages_sent: u64,
    closed: bool,
}

/// Shared mailbox handle; wrap in [`std::sync::Arc`] to share across HTTP/MCP layers.
pub struct Bus {
    inner: Mutex<Inner>,
}

impl Bus {
    /// Open (creating if needed) the store under `data_dir`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let bus = mentischat::bus::Bus::new(std::path::Path::new("/tmp/mentischat_demo"))?;
    /// bus.register(&mentischat::names::AgentName::parse("demo")?).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn new(data_dir: &Path) -> Result<Self, StoreError> {
        Ok(Self {
            inner: Mutex::new(Inner {
                conn: crate::store::open(data_dir)?,
                waiters: HashMap::new(),
                next_waiter_id: 0,
                messages_sent: 0,
                closed: false,
            }),
        })
    }

    /// Claim `name` on the roster. Idempotent: refreshes `last_seen` and keeps
    /// any pending queue.
    ///
    /// # Errors
    ///
    /// `shutting_down` once the bus is closed; `roster_full` when a new name
    /// would exceed [`MAX_ROSTER`]; `storage_error` on a sqlite failure.
    pub async fn register(
        &self,
        name: &crate::names::AgentName,
    ) -> Result<RegisterOutcome, MailErr> {
        let now = now_rfc3339();
        let g = self.inner.lock().await;
        if g.closed {
            return Err(MailErr::ShuttingDown);
        }
        let exists: bool = g
            .conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM roster WHERE name = ?1)",
                params![name.as_str()],
                |r| r.get(0),
            )
            .map_err(sqlite_storage)?;
        if exists {
            g.conn
                .execute(
                    "UPDATE roster SET last_seen = ?2 WHERE name = ?1",
                    params![name.as_str(), now],
                )
                .map_err(sqlite_storage)?;
        } else {
            let agents: i64 = g
                .conn
                .query_row("SELECT COUNT(*) FROM roster", [], |r| r.get(0))
                .map_err(sqlite_storage)?;
            if agents >= MAX_ROSTER {
                return Err(MailErr::RosterFull { agents });
            }
            g.conn
                .execute(
                    "INSERT INTO roster (name, last_seen) VALUES (?1, ?2)",
                    params![name.as_str(), now],
                )
                .map_err(sqlite_storage)?;
        }
        drop(g);
        Ok(RegisterOutcome {
            agent: name.to_string(),
            created: !exists,
            last_seen: now,
        })
    }

    /// Leave the roster, discarding this name's undelivered inbox and waking
    /// its parked pulls (they fail `unregistered`). Messages it sent to other
    /// names stay.
    pub async fn deregister(
        &self,
        name: &crate::names::AgentName,
    ) -> Result<DiscardOutcome, MailErr> {
        let mut g = self.inner.lock().await;
        if g.closed {
            return Err(MailErr::ShuttingDown);
        }
        ensure_registered(&g, name.as_str())?;
        let discarded: i64 = g
            .conn
            .query_row(
                "SELECT COUNT(*) FROM messages WHERE recipient = ?1",
                params![name.as_str()],
                |r| r.get(0),
            )
            .map_err(sqlite_storage)?;
        {
            let tx = g.conn.transaction().map_err(sqlite_storage)?;
            tx.execute(
                "DELETE FROM messages WHERE recipient = ?1",
                params![name.as_str()],
            )
            .map_err(sqlite_storage)?;
            tx.execute("DELETE FROM roster WHERE name = ?1", params![name.as_str()])
                .map_err(sqlite_storage)?;
            tx.commit().map_err(sqlite_storage)?;
        }
        // Dropping senders wakes those pulls; they re-lock, see the name is
        // gone, and fail unregistered.
        g.waiters.remove(name.as_str());
        Ok(DiscardOutcome {
            agent: name.to_string(),
            discarded,
        })
    }

    /// Current roster sorted by name ascending.
    pub async fn list_agents(&self) -> Result<Vec<RosterEntry>, MailErr> {
        let g = self.inner.lock().await;
        if g.closed {
            return Err(MailErr::ShuttingDown);
        }
        let mut stmt = g
            .conn
            .prepare("SELECT name, last_seen FROM roster ORDER BY name ASC")
            .map_err(sqlite_storage)?;
        let rows = stmt
            .query_map([], |r| {
                Ok(RosterEntry {
                    name: r.get(0)?,
                    last_seen: r.get(1)?,
                })
            })
            .map_err(sqlite_storage)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(sqlite_storage)?;
        Ok(rows)
    }

    /// The roster with each agent's queue depth and last-seen age, plus the
    /// number of messages sent since the daemon started, for the operator
    /// terminal view.
    ///
    /// Read-only: does not promote, ack, or touch `last_seen`. A `last_seen`
    /// that SQLite cannot parse reads as 0 seconds ago rather than failing the
    /// whole view.
    ///
    /// # Errors
    ///
    /// `shutting_down` once the bus is closed; `storage_error` on a sqlite
    /// failure.
    pub async fn overview(&self) -> Result<Overview, MailErr> {
        let g = self.inner.lock().await;
        if g.closed {
            return Err(MailErr::ShuttingDown);
        }
        let mut stmt = g
            .conn
            .prepare(
                "SELECT r.name,
                        MAX(0, COALESCE(CAST(strftime('%s', 'now') AS INTEGER)
                                        - CAST(strftime('%s', r.last_seen) AS INTEGER), 0)),
                        (SELECT COUNT(*) FROM messages m WHERE m.recipient = r.name)
                 FROM roster r ORDER BY r.name ASC",
            )
            .map_err(sqlite_storage)?;
        let agents = stmt
            .query_map([], |r| {
                Ok(AgentOverview {
                    name: r.get(0)?,
                    seen_secs_ago: r.get(1)?,
                    queued: r.get(2)?,
                })
            })
            .map_err(sqlite_storage)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(sqlite_storage)?;
        Ok(Overview {
            agents,
            messages_sent: g.messages_sent,
        })
    }

    /// Queue `body` for registered `to`. Validates body size/content and both
    /// roster entries (`from` first). Wakes the oldest parked pull only, and
    /// counts toward [`Overview::messages_sent`].
    pub async fn send(
        &self,
        from: &crate::names::AgentName,
        to: &crate::names::AgentName,
        body: String,
    ) -> Result<SendReceipt, MailErr> {
        if body.len() > MAX_BODY_BYTES || body.trim().is_empty() {
            return Err(MailErr::InvalidBody);
        }
        let created_at = now_rfc3339();
        let mut g = self.inner.lock().await;
        if g.closed {
            return Err(MailErr::ShuttingDown);
        }
        ensure_registered(&g, from.as_str())?;
        ensure_registered(&g, to.as_str())?;

        let n_to: i64 = g
            .conn
            .query_row(
                "SELECT COUNT(*) FROM messages WHERE recipient = ?1",
                params![to.as_str()],
                |r| r.get(0),
            )
            .map_err(sqlite_storage)?;
        if n_to >= MAX_PENDING_PER_AGENT {
            return Err(MailErr::QueueFull {
                to: to.to_string(),
                pending: n_to,
            });
        }
        let n_all: i64 = g
            .conn
            .query_row("SELECT COUNT(*) FROM messages", [], |r| r.get(0))
            .map_err(sqlite_storage)?;
        if n_all >= MAX_TOTAL_MESSAGES {
            return Err(MailErr::StorageFull { pending: n_all });
        }

        let id = uuid::Uuid::new_v4().to_string();
        let seq: i64 = g
            .conn
            .query_row("SELECT COALESCE(MAX(seq), 0) + 1 FROM messages", [], |r| {
                r.get(0)
            })
            .map_err(sqlite_storage)?;
        g.conn
            .execute(
                "INSERT INTO messages (id, sender, recipient, body, created_at, seq, state)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'pending')",
                params![id, from.as_str(), to.as_str(), body, created_at, seq],
            )
            .map_err(sqlite_storage)?;

        g.messages_sent += 1;
        if let Some(q) = g.waiters.get_mut(to.as_str()) {
            wake_one(q);
        }
        drop(g);
        Ok(SendReceipt {
            id,
            from: from.to_string(),
            to: to.to_string(),
            created_at,
        })
    }

    /// Pull (and optionally block for) the next batch for `name`.
    ///
    /// Applies `ack_ids` first (deleting matching `in_flight` rows owned by
    /// this name), then delivers per batch+ack rules. Parks only when both
    /// states are empty and `wait_ms > 0`; timeout is an empty success.
    ///
    /// Fails [`crate::errors::MailboxError::TooManyWaiters`] when
    /// [`MAX_WAITERS_PER_AGENT`] pulls are already parked for this name.
    pub async fn check_inbox(
        &self,
        name: &crate::names::AgentName,
        wait_ms: u32,
        ack_ids: Vec<String>,
    ) -> Result<InboxPage, MailErr> {
        let mut g = self.inner.lock().await;
        if g.closed {
            return Err(MailErr::ShuttingDown);
        }
        ensure_registered(&g, name.as_str())?;
        apply_acks(&mut g, name.as_str(), &ack_ids)?;

        if let Some(page) = deliver_locked(&mut g, name.as_str())? {
            return Ok(page);
        }
        if wait_ms == 0 {
            return Ok(empty_page(name));
        }

        // Park under the mutex: register the waiter before releasing so a
        // concurrent send cannot land between "inbox empty" and "waiting".
        let (waiter_id, rx) = try_park(&mut g, name.as_str())?;
        drop(g);

        let _ =
            tokio::time::timeout(std::time::Duration::from_millis(u64::from(wait_ms)), rx).await;

        let mut g = self.inner.lock().await;
        unpark(&mut g, name.as_str(), waiter_id);
        if g.closed {
            return Err(MailErr::ShuttingDown);
        }
        ensure_registered(&g, name.as_str())?;
        Ok(deliver_locked(&mut g, name.as_str())?.unwrap_or_else(|| empty_page(name)))
    }

    /// Acknowledge-and-delete only (REST `POST /inboxes/{agent}/ack`).
    /// Returns how many matching `in_flight` rows owned by `name` were
    /// removed. Unknown ids are ignored (idempotent). Delivers nothing.
    pub async fn ack(
        &self,
        name: &crate::names::AgentName,
        ack_ids: Vec<String>,
    ) -> Result<i64, MailErr> {
        let mut g = self.inner.lock().await;
        if g.closed {
            return Err(MailErr::ShuttingDown);
        }
        ensure_registered(&g, name.as_str())?;
        if ack_ids.is_empty() {
            return Ok(0);
        }
        let mut acked = 0i64;
        let tx = g.conn.transaction().map_err(sqlite_storage)?;
        {
            let mut stmt = tx
                .prepare(
                    "DELETE FROM messages
                     WHERE id = ?1 AND recipient = ?2 AND state = 'in_flight'",
                )
                .map_err(sqlite_storage)?;
            for id in &ack_ids {
                acked += stmt
                    .execute(params![id, name.as_str()])
                    .map_err(sqlite_storage)? as i64;
            }
        }
        tx.commit().map_err(sqlite_storage)?;
        Ok(acked)
    }

    /// Roster size and total message row count (both states), for `/health`.
    /// Does not promote, ack, or touch `last_seen`.
    pub async fn stats(&self) -> Result<(i64, i64), MailErr> {
        let g = self.inner.lock().await;
        if g.closed {
            return Err(MailErr::ShuttingDown);
        }
        let agents: i64 = g
            .conn
            .query_row("SELECT COUNT(*) FROM roster", [], |r| r.get(0))
            .map_err(sqlite_storage)?;
        let pending: i64 = g
            .conn
            .query_row("SELECT COUNT(*) FROM messages", [], |r| r.get(0))
            .map_err(sqlite_storage)?;
        Ok((agents, pending))
    }

    /// Mark the bus closed: every parked waiter fails `shutting_down` and no
    /// further operations are accepted. Then discard the whole mailbox —
    /// roster and every pending/in_flight row — in one transaction, so a
    /// restart starts completely clean (a bulk deregister of everyone).
    /// Committed rows only survive a crash, never a graceful shutdown.
    ///
    /// Returns `Err` when the wipe transaction fails (rolled back); the bus
    /// is still closed either way.
    pub async fn shutdown(&self) -> Result<(), MailErr> {
        let mut g = self.inner.lock().await;
        g.closed = true;
        g.waiters.clear(); // dropping senders wakes all parked pulls
        let wipe_result = g.conn.transaction().and_then(|tx| {
            tx.execute("DELETE FROM messages", [])?;
            tx.execute("DELETE FROM roster", [])?;
            tx.commit()
        });
        if let Err(e) = wipe_result {
            tracing::error!(
                error = %e,
                "shutdown: failed to discard mailbox transactionally; \
                 roster or messages may persist on disk"
            );
            return Err(sqlite_storage(e));
        }
        Ok(())
    }
}

/// Deliver one wake to `q`, discarding canceled waiters until a live
/// receiver takes the signal (or the queue empties).
fn wake_one(q: &mut VecDeque<Waiter>) {
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
fn try_park(g: &mut Inner, name: &str) -> Result<(u64, oneshot::Receiver<()>), MailErr> {
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
fn unpark(g: &mut Inner, name: &str, id: u64) {
    if let Some(q) = g.waiters.get_mut(name) {
        q.retain(|w| w.id != id);
        if q.is_empty() {
            g.waiters.remove(name);
        }
    }
}

fn empty_page(name: &crate::names::AgentName) -> InboxPage {
    InboxPage {
        agent: name.to_string(),
        messages: Vec::new(),
        remaining: 0,
    }
}

fn sqlite_storage(e: rusqlite::Error) -> MailErr {
    tracing::error!(error = %e, "sqlite error");
    MailErr::StorageError
}

fn ensure_registered(g: &Inner, name: &str) -> Result<(), MailErr> {
    let exists: bool = g
        .conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM roster WHERE name = ?1)",
            params![name],
            |r| r.get(0),
        )
        .map_err(sqlite_storage)?;
    if exists {
        return Ok(());
    }
    Err(MailErr::Unregistered {
        agent: name.to_string(),
        known: roster_sorted(g)?,
    })
}

fn roster_sorted(g: &Inner) -> Result<Vec<String>, MailErr> {
    let mut stmt = g
        .conn
        .prepare("SELECT name FROM roster ORDER BY name ASC")
        .map_err(sqlite_storage)?;
    let names = stmt
        .query_map([], |r| r.get::<_, String>(0))
        .map_err(sqlite_storage)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(sqlite_storage)?;
    Ok(names)
}

fn apply_acks(g: &mut Inner, name: &str, ack_ids: &[String]) -> Result<(), MailErr> {
    if !ack_ids.is_empty() {
        let tx = g.conn.transaction().map_err(sqlite_storage)?;
        {
            let mut stmt = tx
                .prepare(
                    "DELETE FROM messages
                     WHERE id = ?1 AND recipient = ?2 AND state = 'in_flight'",
                )
                .map_err(sqlite_storage)?;
            for id in ack_ids {
                stmt.execute(params![id, name]).map_err(sqlite_storage)?;
            }
        }
        tx.commit().map_err(sqlite_storage)?;
    }
    touch_last_seen(g, name)
}

fn touch_last_seen(g: &mut Inner, name: &str) -> Result<(), MailErr> {
    g.conn
        .execute(
            "UPDATE roster SET last_seen = ?2 WHERE name = ?1",
            params![name, now_rfc3339()],
        )
        .map_err(sqlite_storage)
        .map(|_| ())
}

/// Deliver per batch+ack rules; `None` means both states are empty.
///
/// Redeliver unacked `in_flight` FIFO within the same batch caps as a fresh
/// promote; else promote up to `MAX_BATCH` pending rows within
/// `MAX_RETURN_BYTES` of body bytes (the first message always rides even when
/// oversized — progress must be possible).
fn deliver_locked(g: &mut Inner, name: &str) -> Result<Option<InboxPage>, MailErr> {
    let in_flight = load_rows(g, name, "in_flight", None)?;
    if !in_flight.is_empty() {
        let batch = take_batch(&in_flight);
        return page(g, name, batch).map(Some);
    }

    let candidates = load_rows(g, name, "pending", Some(MAX_BATCH))?;
    if candidates.is_empty() {
        return Ok(None);
    }
    let batch = take_batch(&candidates);
    for msg in &batch {
        g.conn
            .execute(
                "UPDATE messages SET state = 'in_flight' WHERE id = ?1",
                params![msg.id],
            )
            .map_err(sqlite_storage)?;
    }
    page(g, name, batch).map(Some)
}

/// Select a prefix of `candidates` (already seq-ordered) that fits
/// [`MAX_BATCH`] and [`MAX_RETURN_BYTES`]. The first message is always kept.
fn take_batch(candidates: &[Message]) -> Vec<Message> {
    let mut take = 0usize;
    let mut bytes = 0usize;
    for msg in candidates.iter().take(MAX_BATCH as usize) {
        if take > 0 && bytes + msg.body.len() > MAX_RETURN_BYTES {
            break;
        }
        bytes += msg.body.len();
        take += 1;
    }
    candidates[..take].to_vec()
}

fn page(g: &mut Inner, name: &str, messages: Vec<Message>) -> Result<InboxPage, MailErr> {
    // Unacked rows not in this page (leftover in_flight + still-pending).
    let total: i64 = g
        .conn
        .query_row(
            "SELECT COUNT(*) FROM messages WHERE recipient = ?1",
            params![name],
            |r| r.get(0),
        )
        .map_err(sqlite_storage)?;
    let remaining = (total - messages.len() as i64).max(0);
    touch_last_seen(g, name)?;
    Ok(InboxPage {
        agent: name.to_string(),
        messages,
        remaining,
    })
}

fn load_rows(
    g: &Inner,
    name: &str,
    state: &str,
    limit: Option<i64>,
) -> Result<Vec<Message>, MailErr> {
    let map = |r: &rusqlite::Row| {
        Ok(Message {
            id: r.get(0)?,
            from: r.get(1)?,
            to: name.to_string(),
            body: r.get(2)?,
            created_at: r.get(3)?,
        })
    };
    let mut stmt = g
        .conn
        .prepare(
            "SELECT id, sender, body, created_at FROM messages
             WHERE recipient = ?1 AND state = ?2 ORDER BY seq ASC LIMIT ?3",
        )
        .map_err(sqlite_storage)?;
    let rows = stmt
        .query_map(params![name, state, limit.unwrap_or(i64::MAX)], map)
        .map_err(sqlite_storage)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(sqlite_storage)?;
    Ok(rows)
}

/// Current time as UTC RFC 3339 with millisecond precision.
pub fn now_rfc3339() -> String {
    let d = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let secs = d.as_secs();
    let millis = d.subsec_millis();
    let (year, month, day) = civil_from_days((secs / 86_400) as i64);
    let sod = secs % 86_400;
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z",
        year,
        month,
        day,
        sod / 3600,
        (sod % 3600) / 60,
        sod % 60,
        millis
    )
}

/// Howard Hinnant's days-from-civil inverse: epoch day -> (y, m, d).
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m as u32, d as u32)
}
