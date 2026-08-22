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
    /// Pending rows still waiting after this call; drain while `> 0`.
    pub remaining: i64,
}

struct Waiter {
    id: u64,
    tx: oneshot::Sender<()>,
}

struct Inner {
    conn: rusqlite::Connection,
    waiters: HashMap<String, VecDeque<Waiter>>,
    next_waiter_id: u64,
    closed: bool,
}

/// Shared mailbox handle; wrap in [`Arc`] to share across HTTP/MCP layers.
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
    /// let bus = llm_chat::bus::Bus::new(std::path::Path::new("/tmp/llm_chat_demo"))?;
    /// bus.register(&llm_chat::names::AgentName::parse("demo")?).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn new(data_dir: &Path) -> Result<Self, StoreError> {
        Ok(Self {
            inner: Mutex::new(Inner {
                conn: crate::store::open(data_dir)?,
                waiters: HashMap::new(),
                next_waiter_id: 0,
                closed: false,
            }),
        })
    }

    /// Claim `name` on the roster. Idempotent: refreshes `last_seen`, keeps
    /// any pending queue.
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
            return Ok(RegisterOutcome {
                agent: name.to_string(),
                created: false,
                last_seen: now,
            });
        }
        g.conn
            .execute(
                "INSERT INTO roster (name, last_seen) VALUES (?1, ?2)",
                params![name.as_str(), now],
            )
            .map_err(sqlite_storage)?;
        Ok(RegisterOutcome {
            agent: name.to_string(),
            created: true,
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
        g.conn
            .execute(
                "DELETE FROM messages WHERE recipient = ?1",
                params![name.as_str()],
            )
            .map_err(sqlite_storage)?;
        // Dropping senders wakes those pulls; they re-lock, see the name is
        // gone, and fail unregistered.
        g.waiters.remove(name.as_str());
        g.conn
            .execute("DELETE FROM roster WHERE name = ?1", params![name.as_str()])
            .map_err(sqlite_storage)?;
        Ok(DiscardOutcome {
            agent: name.to_string(),
            discarded,
        })
    }

    /// Current roster sorted by name ascending.
    pub async fn list_agents(&self) -> Result<Vec<RosterEntry>, MailErr> {
        let g = self.inner.lock().await;
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

    /// Queue `body` for registered `to`. Validates body size/content and both
    /// roster entries (`from` first). Wakes the oldest parked pull only.
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
            .map_err(|_| MailErr::StorageFull { pending: n_all })?;

        if let Some(q) = g.waiters.get_mut(to.as_str()) {
            if let Some(w) = q.pop_front() {
                let _ = w.tx.send(());
            }
        }
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
    /// Fails [`MailboxError::TooManyWaiters`] when
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

        let parked_len = g.waiters.entry(name.to_string()).or_default().len();
        if parked_len >= MAX_WAITERS_PER_AGENT {
            return Err(MailErr::TooManyWaiters {
                agent: name.to_string(),
                waiting: parked_len,
            });
        }

        // Park under the mutex: register the waiter before releasing so a
        // concurrent send cannot land between "inbox empty" and "waiting".
        let waiter_id = g.next_waiter_id;
        g.next_waiter_id += 1;
        let (tx, rx) = oneshot::channel();
        g.waiters
            .get_mut(name.as_str())
            .expect("entry created above")
            .push_back(Waiter { id: waiter_id, tx });
        drop(g);

        let _ =
            tokio::time::timeout(std::time::Duration::from_millis(u64::from(wait_ms)), rx).await;

        let mut g = self.inner.lock().await;
        if let Some(q) = g.waiters.get_mut(name.as_str()) {
            q.retain(|w| w.id != waiter_id);
        }
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
        let g = self.inner.lock().await;
        if g.closed {
            return Err(MailErr::ShuttingDown);
        }
        ensure_registered(&g, name.as_str())?;
        let mut acked = 0i64;
        for id in &ack_ids {
            acked += g
                .conn
                .execute(
                    "DELETE FROM messages WHERE id = ?1 AND recipient = ?2 AND state = 'in_flight'",
                    params![id, name.as_str()],
                )
                .map_err(sqlite_storage)? as i64;
        }
        Ok(acked)
    }

    /// Roster size and total message row count (both states), for `/health`.
    /// Does not promote, ack, or touch `last_seen`.
    pub async fn stats(&self) -> Result<(i64, i64), MailErr> {
        let g = self.inner.lock().await;
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
    /// further operations are accepted. Committed rows remain on disk.
    pub async fn shutdown(&self) {
        let mut g = self.inner.lock().await;
        g.closed = true;
        g.waiters.clear(); // dropping senders wakes all parked pulls
    }
}

fn empty_page(name: &crate::names::AgentName) -> InboxPage {
    InboxPage {
        agent: name.to_string(),
        messages: Vec::new(),
        remaining: 0,
    }
}

fn sqlite_storage(_e: rusqlite::Error) -> MailErr {
    MailErr::StorageFull { pending: -1 }
}

fn ensure_registered(g: &Inner, name: &str) -> Result<(), MailErr> {
    let known = roster_sorted(g);
    if known.iter().any(|n| n == name) {
        return Ok(());
    }
    Err(MailErr::Unregistered {
        agent: name.to_string(),
        known,
    })
}

fn roster_sorted(g: &Inner) -> Vec<String> {
    let mut stmt = match g.conn.prepare("SELECT name FROM roster ORDER BY name ASC") {
        Ok(s) => s,
        Err(_) => return Vec::new(),
    };
    stmt.query_map([], |r| r.get::<_, String>(0))
        .map(|rows| rows.filter_map(Result::ok).collect())
        .unwrap_or_default()
}

fn apply_acks(g: &mut Inner, name: &str, ack_ids: &[String]) -> Result<(), MailErr> {
    for id in ack_ids {
        g.conn
            .execute(
                "DELETE FROM messages WHERE id = ?1 AND recipient = ?2 AND state = 'in_flight'",
                params![id, name],
            )
            .map_err(sqlite_storage)?;
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
/// Redeliver unacked `in_flight` FIFO; else promote up to `MAX_BATCH`
/// pending rows within `MAX_RETURN_BYTES` of body bytes (the first message
/// always rides even when oversized — progress must be possible).
fn deliver_locked(g: &mut Inner, name: &str) -> Result<Option<InboxPage>, MailErr> {
    let in_flight = load_rows(g, name, "in_flight", None)?;
    if !in_flight.is_empty() {
        return page(g, name, in_flight).map(Some);
    }

    let candidates = load_rows(g, name, "pending", Some(MAX_BATCH))?;
    if candidates.is_empty() {
        return Ok(None);
    }
    let mut take = 0usize;
    let mut bytes = 0usize;
    for msg in &candidates {
        if take > 0 && bytes + msg.body.len() > MAX_RETURN_BYTES {
            break;
        }
        bytes += msg.body.len();
        take += 1;
    }
    for msg in &candidates[..take] {
        g.conn
            .execute(
                "UPDATE messages SET state = 'in_flight' WHERE id = ?1",
                params![msg.id],
            )
            .map_err(sqlite_storage)?;
    }
    page(g, name, candidates[..take].to_vec()).map(Some)
}

fn page(g: &mut Inner, name: &str, messages: Vec<Message>) -> Result<InboxPage, MailErr> {
    let remaining: i64 = g
        .conn
        .query_row(
            "SELECT COUNT(*) FROM messages WHERE recipient = ?1 AND state = 'pending'",
            params![name],
            |r| r.get(0),
        )
        .map_err(sqlite_storage)?;
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
