//! The mailbox: roster, caps, ack/batch delivery, and blocking pulls.
//!
//! All state sits behind a single [`tokio::sync::Mutex`]; sqlite writes run
//! inside the lock so waiters and rows cannot tear. The lock is **never**
//! held across the park await — a waiter parks on a one-shot channel and
//! re-acquires to deliver, which is what makes the send/timeout race safe.

use std::collections::HashMap;
use std::path::Path;

use rusqlite::params;
use tokio::sync::{Mutex, MutexGuard};

mod clock;
mod delivery;
mod model;
mod waiters;

pub use crate::store::StoreError;
pub use clock::now_rfc3339;
pub use model::{
    AgentOverview, DiscardOutcome, InboxPage, Message, Overview, RegisterOutcome, Registration,
    RosterEntry, SendReceipt, MAX_ACK_IDS, MAX_BATCH, MAX_BODY_BYTES, MAX_DESCRIPTION_BYTES,
    MAX_PENDING_PER_AGENT, MAX_RETURN_BYTES, MAX_ROSTER, MAX_TOTAL_MESSAGES, MAX_WAITERS_PER_AGENT,
    MAX_WAIT_MS,
};

use delivery::{delete_acked, deliver_locked, empty_page, touch_last_seen};
use waiters::{try_park, unpark, wake_one, WaiterMap};

type MailErr = crate::errors::MailboxError;

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

    /// Locks the bus, failing `shutting_down` once it has been closed.
    async fn lock_open(&self) -> Result<MutexGuard<'_, Inner>, MailErr> {
        let g = self.inner.lock().await;
        if g.closed {
            Err(MailErr::ShuttingDown)
        } else {
            Ok(g)
        }
    }

    /// Claim `name` on the roster with default options: keep any stored
    /// description and join the name if it is already registered. See
    /// [`register_with`](Self::register_with).
    pub async fn register(
        &self,
        name: &crate::names::AgentName,
    ) -> Result<RegisterOutcome, MailErr> {
        self.register_with(name, Registration::default()).await
    }

    /// Claim `name` on the roster. Idempotent unless `opts.exclusive`:
    /// refreshes `last_seen`, applies `opts.description`, and keeps any
    /// pending queue.
    ///
    /// # Errors
    ///
    /// `shutting_down` once the bus is closed; `name_taken` when
    /// `opts.exclusive` and the name is already on the roster; `roster_full`
    /// when a new name would exceed [`MAX_ROSTER`]; `storage_error` on a sqlite
    /// failure.
    pub async fn register_with(
        &self,
        name: &crate::names::AgentName,
        opts: Registration,
    ) -> Result<RegisterOutcome, MailErr> {
        let description = opts.description.map(validate_description).transpose()?;
        let g = self.lock_open().await?;
        // Stamped under the lock so concurrent registers cannot write last_seen
        // out of order.
        let now = now_rfc3339();
        let exists = is_registered(&g, name.as_str())?;
        let description: Option<String> = if exists {
            if opts.exclusive {
                return Err(MailErr::NameTaken {
                    agent: name.to_string(),
                    known: roster_sorted(&g)?,
                });
            }
            // A NULL ?3 keeps the stored description; '' clears it.
            g.conn
                .query_row(
                    "UPDATE roster
                        SET last_seen = ?2,
                            description = CASE WHEN ?3 IS NULL THEN description
                                               ELSE NULLIF(?3, '') END
                      WHERE name = ?1
                  RETURNING description",
                    params![name.as_str(), now, description],
                    |r| r.get(0),
                )
                .map_err(sqlite_storage)?
        } else {
            let agents: i64 = g
                .conn
                .query_row("SELECT COUNT(*) FROM roster", [], |r| r.get(0))
                .map_err(sqlite_storage)?;
            if agents >= MAX_ROSTER {
                return Err(MailErr::RosterFull { agents });
            }
            g.conn
                .query_row(
                    "INSERT INTO roster (name, last_seen, description)
                     VALUES (?1, ?2, NULLIF(?3, ''))
                     RETURNING description",
                    params![name.as_str(), now, description],
                    |r| r.get(0),
                )
                .map_err(sqlite_storage)?
        };
        drop(g);
        Ok(RegisterOutcome {
            agent: name.to_string(),
            created: !exists,
            last_seen: now,
            description,
        })
    }

    /// Leave the roster, discarding this name's undelivered inbox and waking
    /// its parked pulls (they fail `unregistered`). Messages it sent to other
    /// names stay.
    pub async fn deregister(
        &self,
        name: &crate::names::AgentName,
    ) -> Result<DiscardOutcome, MailErr> {
        let mut g = self.lock_open().await?;
        ensure_registered(&g, name.as_str())?;
        let discarded = {
            let tx = g.conn.transaction().map_err(sqlite_storage)?;
            let discarded = tx
                .execute(
                    "DELETE FROM messages WHERE recipient = ?1",
                    params![name.as_str()],
                )
                .map_err(sqlite_storage)?;
            tx.execute("DELETE FROM roster WHERE name = ?1", params![name.as_str()])
                .map_err(sqlite_storage)?;
            tx.commit().map_err(sqlite_storage)?;
            discarded as i64
        };
        // Dropping senders wakes those pulls; they re-lock, see the name is
        // gone, and fail unregistered.
        g.waiters.remove(name.as_str());
        Ok(DiscardOutcome {
            agent: name.to_string(),
            discarded,
        })
    }

    /// Current roster sorted by name ascending, with each agent's description
    /// and queue depth. Read-only: does not touch `last_seen`.
    pub async fn list_agents(&self) -> Result<Vec<RosterEntry>, MailErr> {
        let g = self.lock_open().await?;
        Ok(roster_rows(&g)?
            .into_iter()
            .map(|row| RosterEntry {
                name: row.name,
                description: row.description,
                last_seen: row.last_seen,
                queued: row.queued,
            })
            .collect())
    }

    /// The roster with each agent's description, queue depth and last-seen
    /// age, plus the number of messages sent since the daemon started, for the
    /// operator terminal view.
    ///
    /// Shares its query with [`list_agents`](Self::list_agents); this shape
    /// trades the wire timestamp for an age the view can print.
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
        let g = self.lock_open().await?;
        let agents = roster_rows(&g)?
            .into_iter()
            .map(|row| AgentOverview {
                name: row.name,
                description: row.description,
                seen_secs_ago: row.seen_secs_ago,
                queued: row.queued,
            })
            .collect();
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
        let mut g = self.lock_open().await?;
        // Stamped under the lock so createdAt order matches seq order.
        let created_at = now_rfc3339();
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
        let mut g = self.lock_open().await?;
        ensure_registered(&g, name.as_str())?;
        delete_acked(&mut g, name.as_str(), &ack_ids)?;
        touch_last_seen(&mut g, name.as_str())?;

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
        // The agent was waiting all along; record when the pull ended.
        touch_last_seen(&mut g, name.as_str())?;
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
        let mut g = self.lock_open().await?;
        ensure_registered(&g, name.as_str())?;
        delete_acked(&mut g, name.as_str(), &ack_ids)
    }

    /// Roster size and total message row count (both states), for `/health`.
    /// Does not promote, ack, or touch `last_seen`.
    pub async fn stats(&self) -> Result<(i64, i64), MailErr> {
        let g = self.lock_open().await?;
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

fn sqlite_storage(e: rusqlite::Error) -> MailErr {
    tracing::error!(error = %e, "sqlite error");
    MailErr::StorageError
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

fn is_registered(g: &Inner, name: &str) -> Result<bool, MailErr> {
    g.conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM roster WHERE name = ?1)",
            params![name],
            |r| r.get(0),
        )
        .map_err(sqlite_storage)
}

fn ensure_registered(g: &Inner, name: &str) -> Result<(), MailErr> {
    if is_registered(g, name)? {
        return Ok(());
    }
    Err(MailErr::Unregistered {
        agent: name.to_string(),
        known: roster_sorted(g)?,
    })
}

/// One roster row with everything `list_agents` and `overview` need.
struct RosterRow {
    name: String,
    description: Option<String>,
    last_seen: String,
    seen_secs_ago: i64,
    queued: i64,
}

fn roster_rows(g: &Inner) -> Result<Vec<RosterRow>, MailErr> {
    let mut stmt = g
        .conn
        .prepare(
            "SELECT r.name, r.description, r.last_seen,
                    MAX(0, COALESCE(CAST(strftime('%s', 'now') AS INTEGER)
                                    - CAST(strftime('%s', r.last_seen) AS INTEGER), 0)),
                    (SELECT COUNT(*) FROM messages m WHERE m.recipient = r.name)
             FROM roster r ORDER BY r.name ASC",
        )
        .map_err(sqlite_storage)?;
    let rows = stmt
        .query_map([], |r| {
            Ok(RosterRow {
                name: r.get(0)?,
                description: r.get(1)?,
                last_seen: r.get(2)?,
                seen_secs_ago: r.get(3)?,
                queued: r.get(4)?,
            })
        })
        .map_err(sqlite_storage)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(sqlite_storage)?;
    Ok(rows)
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
