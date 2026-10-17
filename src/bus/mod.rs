//! The mailbox: roster, caps, delete-on-read delivery, blocking pulls, and
//! expiry of unread mail and idle agents.
//!
//! All state sits behind a single [`tokio::sync::Mutex`]; sqlite writes run
//! inside the lock so waiters and rows cannot tear. The lock is **never**
//! held across the park await — a waiter parks on a one-shot channel and
//! re-acquires to deliver, which is what makes the send/timeout race safe.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use rusqlite::params;
use tokio::sync::{Mutex, MutexGuard};

mod clock;
mod delivery;
mod model;
mod waiters;

pub use crate::store::StoreError;
pub use clock::{now_rfc3339, rfc3339};
pub use model::{
    parse_duration, AgentOverview, DiscardOutcome, InboxPage, Message, Overview, RegisterOutcome,
    Registration, Retention, RosterEntry, SendReceipt, SweepOutcome, MAX_BATCH, MAX_BODY_BYTES,
    MAX_DESCRIPTION_BYTES, MAX_PENDING_PER_AGENT, MAX_RETURN_BYTES, MAX_ROSTER, MAX_TOTAL_MESSAGES,
    MAX_WAITERS_PER_AGENT, MAX_WAIT_MS,
};

use delivery::{empty_page, take_locked, touch_last_seen};
use waiters::{has_live_waiter, try_park, unpark, wake_one, ParkGuard, WaiterMap};

type MailErr = crate::errors::MailboxError;

struct Inner {
    conn: rusqlite::Connection,
    waiters: WaiterMap,
    next_waiter_id: u64,
    /// Delivery order for the next message; strictly increasing for the life
    /// of the process, seeded past every row already on disk.
    next_seq: i64,
    messages_sent: u64,
    /// Highest `{provider}_{n}` number registered this run, per provider.
    /// Provider registration never goes back below it, so a freed name is
    /// not handed to a new agent while its old client may still use it.
    issued: HashMap<String, u64>,
    closed: bool,
}

/// Shared mailbox handle; wrap in [`std::sync::Arc`] to share across HTTP/MCP layers.
pub struct Bus {
    // Arc so a parked pull dropped mid-wait can settle its waiter from Drop.
    inner: Arc<Mutex<Inner>>,
    /// A second connection that truncates the write-ahead log on a blocking
    /// thread, outside the bus lock.
    wal: Arc<std::sync::Mutex<rusqlite::Connection>>,
    retention: Retention,
}

impl Bus {
    /// Open (creating if needed) the store under `data_dir` with the default
    /// [`Retention`].
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
        Self::with_retention(data_dir, Retention::default())
    }

    /// Open the store under `data_dir`, expiring unread mail and idle agents
    /// per `retention` whenever [`sweep`](Self::sweep) runs.
    pub fn with_retention(data_dir: &Path, retention: Retention) -> Result<Self, StoreError> {
        let conn = crate::store::open(data_dir)?;
        let wal = crate::store::open_side(data_dir)?;
        let next_seq: i64 = conn
            .query_row("SELECT COALESCE(MAX(seq), 0) + 1 FROM messages", [], |r| {
                r.get(0)
            })
            .map_err(StoreError::Sqlite)?;
        Ok(Self {
            inner: Arc::new(Mutex::new(Inner {
                conn,
                waiters: HashMap::new(),
                next_waiter_id: 0,
                next_seq,
                messages_sent: 0,
                issued: HashMap::new(),
                closed: false,
            })),
            wal: Arc::new(std::sync::Mutex::new(wal)),
            retention,
        })
    }

    /// How long unread mail and idle agents are kept.
    pub fn retention(&self) -> Retention {
        self.retention
    }

    /// Truncate the write-ahead log over the second connection on a blocking
    /// thread, so its disk sync runs neither on an async worker nor under the
    /// bus lock. Callers await it, so deleted text is off disk before they
    /// return.
    async fn scrub_wal(&self) {
        let wal = self.wal.clone();
        let done = tokio::task::spawn_blocking(move || {
            truncate_wal(
                &wal.lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner),
            );
        })
        .await;
        if let Err(e) = done {
            tracing::warn!(error = %e, "could not run the write-ahead log truncation");
        }
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
    /// description. See [`register_with`](Self::register_with).
    pub async fn register(
        &self,
        name: &crate::names::AgentName,
    ) -> Result<RegisterOutcome, MailErr> {
        self.register_with(name, Registration::default()).await
    }

    /// Claim `name` on the roster. Idempotent: an existing name has its
    /// `last_seen` refreshed and `opts.description` applied, and keeps any
    /// pending queue (so a second caller of the same name shares its inbox).
    ///
    /// # Errors
    ///
    /// `shutting_down` once the bus is closed; `roster_full` when a new name
    /// would exceed [`MAX_ROSTER`]; `invalid_description`; `storage_error` on
    /// a sqlite failure.
    pub async fn register_with(
        &self,
        name: &crate::names::AgentName,
        opts: Registration,
    ) -> Result<RegisterOutcome, MailErr> {
        let description = opts.description.map(validate_description).transpose()?;
        let mut g = self.lock_open().await?;
        // Stamped under the lock so concurrent registers cannot write last_seen
        // out of order.
        let now = now_rfc3339();
        let exists = is_registered(&g, name.as_str())?;
        let description: Option<String> = if exists {
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
            insert_agent(&mut g, name.as_str(), &now, description)?
        };
        drop(g);
        Ok(RegisterOutcome {
            agent: name.to_string(),
            created: !exists,
            last_seen: now,
            description,
        })
    }

    /// Claim the next `{provider}_{n}` (`claude_code_1`, then `_2`, ...) as a
    /// new roster entry: one past the highest number registered for that
    /// provider this run or on the roster. Numbers are not reused within a
    /// run, so a client still holding a freed name fails `unregistered`
    /// instead of reading a new agent's inbox. The name is chosen under the
    /// roster lock, so concurrent callers always get distinct names.
    ///
    /// # Errors
    ///
    /// `shutting_down` once the bus is closed; `roster_full` at
    /// [`MAX_ROSTER`] agents; `invalid_description`; `invalid_name` when the
    /// provider's numbers are exhausted (an explicit `{provider}_{u64::MAX}`
    /// was registered); `storage_error` on a sqlite failure.
    pub async fn register_provider(
        &self,
        provider: &crate::names::Provider,
        opts: Registration,
    ) -> Result<RegisterOutcome, MailErr> {
        let description = opts.description.map(validate_description).transpose()?;
        let mut g = self.lock_open().await?;
        let now = now_rfc3339();
        let name = next_provider_name(&roster_sorted(&g)?, provider.as_str(), &g.issued)
            .ok_or_else(|| MailErr::InvalidName {
                agent: provider.as_str().to_string(),
            })?;
        let description = insert_agent(&mut g, &name, &now, description)?;
        drop(g);
        Ok(RegisterOutcome {
            agent: name,
            created: true,
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
        drop(g);
        if discarded > 0 {
            self.scrub_wal().await;
        }
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
    /// Read-only: does not deliver anything or touch `last_seen`. A `last_seen`
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
    /// roster entries (`from` first). Refreshes the sender's `last_seen`,
    /// wakes the oldest parked pull only, and counts toward
    /// [`Overview::messages_sent`].
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

        // Both caps from one scan of the (small, capped) table.
        let (n_to, n_all): (i64, i64) = g
            .conn
            .query_row(
                "SELECT COALESCE(SUM(recipient = ?1), 0), COUNT(*) FROM messages",
                params![to.as_str()],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .map_err(sqlite_storage)?;
        if n_to >= MAX_PENDING_PER_AGENT {
            return Err(MailErr::QueueFull {
                to: to.to_string(),
                pending: n_to,
            });
        }
        if n_all >= MAX_TOTAL_MESSAGES {
            return Err(MailErr::StorageFull { pending: n_all });
        }

        let id = uuid::Uuid::new_v4().to_string();
        let seq = g.next_seq;
        // One transaction: a failure stores nothing, so the caller's error is
        // the whole truth and a retry cannot duplicate the message.
        let tx = g.conn.transaction().map_err(sqlite_storage)?;
        tx.execute(
            "INSERT INTO messages (id, sender, recipient, body, created_at, seq)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![id, from.as_str(), to.as_str(), body, created_at, seq],
        )
        .map_err(sqlite_storage)?;
        // Sending is activity too: an agent that only sends must not age off
        // the roster.
        tx.execute(
            "UPDATE roster SET last_seen = ?2 WHERE name = ?1",
            params![from.as_str(), created_at],
        )
        .map_err(sqlite_storage)?;
        tx.commit().map_err(sqlite_storage)?;
        g.next_seq += 1;
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

    /// Pull (and optionally block for) the next batch for `name`. Returned
    /// messages are deleted, and the write-ahead log is truncated before this
    /// returns, so their text is off disk by the time the caller has it.
    ///
    /// Parks only when the inbox is empty and `wait_ms > 0`; timeout is an
    /// empty success.
    ///
    /// Fails [`crate::errors::MailboxError::TooManyWaiters`] when
    /// [`MAX_WAITERS_PER_AGENT`] pulls are already parked for this name.
    pub async fn check_inbox(
        &self,
        name: &crate::names::AgentName,
        wait_ms: u32,
    ) -> Result<InboxPage, MailErr> {
        let page = self.pull(name, wait_ms).await?;
        if !page.messages.is_empty() {
            self.scrub_wal().await;
        }
        Ok(page)
    }

    /// [`check_inbox`](Self::check_inbox) without the log truncation.
    async fn pull(
        &self,
        name: &crate::names::AgentName,
        wait_ms: u32,
    ) -> Result<InboxPage, MailErr> {
        let mut g = self.lock_open().await?;
        ensure_registered(&g, name.as_str())?;
        touch_last_seen(&mut g, name.as_str())?;

        if let Some(page) = take_locked(&mut g, name.as_str())? {
            return Ok(page);
        }
        if wait_ms == 0 {
            return Ok(empty_page(name));
        }

        // Park until mail arrives or the deadline passes. A wake can find
        // nothing to deliver (another pull under the same name took it, or a
        // cancelled waiter handed its wake on); that waiter parks again for
        // the time left instead of returning empty early.
        let deadline = tokio::time::Instant::now() + Duration::from_millis(u64::from(wait_ms));
        loop {
            // Park under the mutex: register the waiter before releasing so a
            // concurrent send cannot land between "inbox empty" and "waiting".
            let (waiter_id, rx) = try_park(&mut g, name.as_str())?;
            drop(g);

            let mut guard = ParkGuard::new(self.inner.clone(), name.as_str(), waiter_id, rx);
            guard.wait_until(deadline).await;

            g = self.inner.lock().await;
            guard.settle();
            unpark(&mut g, name.as_str(), waiter_id);
            if g.closed {
                return Err(MailErr::ShuttingDown);
            }
            ensure_registered(&g, name.as_str())?;
            // The agent was waiting all along; record when the pull ended.
            touch_last_seen(&mut g, name.as_str())?;
            if let Some(page) = take_locked(&mut g, name.as_str())? {
                return Ok(page);
            }
            if tokio::time::Instant::now() >= deadline {
                return Ok(empty_page(name));
            }
        }
    }

    /// Expire unread mail older than [`Retention::message_ttl`] and drop
    /// agents with no `register`, `send` or `check_inbox` within
    /// [`Retention::agent_ttl`] (with their unread inbox), as of now. The daemon runs this on a timer; see
    /// [`sweep_at`](Self::sweep_at).
    pub async fn sweep(&self) -> Result<SweepOutcome, MailErr> {
        self.sweep_at(SystemTime::now()).await
    }

    /// [`sweep`](Self::sweep) as of `now` (injectable for tests).
    ///
    /// An agent with a pull parked right now is never dropped, however old
    /// its `last_seen`. When anything was deleted the write-ahead log is
    /// truncated so the removed text does not linger on disk.
    pub async fn sweep_at(&self, now: SystemTime) -> Result<SweepOutcome, MailErr> {
        let mut g = self.lock_open().await?;
        let cutoff =
            |ttl: Duration| rfc3339(now.checked_sub(ttl).unwrap_or(SystemTime::UNIX_EPOCH));
        let (message_cutoff, agent_cutoff) = (
            cutoff(self.retention.message_ttl),
            cutoff(self.retention.agent_ttl),
        );
        let stale: Vec<String> = {
            let mut stmt = g
                .conn
                .prepare("SELECT name FROM roster WHERE last_seen < ?1 ORDER BY name")
                .map_err(sqlite_storage)?;
            let names = stmt
                .query_map(params![agent_cutoff], |r| r.get::<_, String>(0))
                .map_err(sqlite_storage)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(sqlite_storage)?;
            names
        };
        let dropped: Vec<String> = stale
            .into_iter()
            .filter(|n| !has_live_waiter(&g.waiters, n))
            .collect();

        let (expired, discarded) = {
            let tx = g.conn.transaction().map_err(sqlite_storage)?;
            let expired = tx
                .execute(
                    "DELETE FROM messages WHERE created_at < ?1",
                    params![message_cutoff],
                )
                .map_err(sqlite_storage)? as i64;
            let mut discarded = 0i64;
            for name in &dropped {
                discarded += tx
                    .execute("DELETE FROM messages WHERE recipient = ?1", params![name])
                    .map_err(sqlite_storage)? as i64;
                tx.execute("DELETE FROM roster WHERE name = ?1", params![name])
                    .map_err(sqlite_storage)?;
            }
            tx.commit().map_err(sqlite_storage)?;
            (expired, discarded)
        };
        for name in &dropped {
            g.waiters.remove(name);
        }
        let out = SweepOutcome {
            expired,
            discarded,
            dropped_agents: dropped,
        };
        drop(g);
        if !out.is_empty() {
            self.scrub_wal().await;
        }
        Ok(out)
    }

    /// Roster size and unread message count, for `/health`. Does not deliver
    /// anything or touch `last_seen`.
    pub async fn stats(&self) -> Result<(i64, i64), MailErr> {
        let g = self.lock_open().await?;
        g.conn
            .query_row(
                "SELECT (SELECT COUNT(*) FROM roster), (SELECT COUNT(*) FROM messages)",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .map_err(sqlite_storage)
    }

    /// Delete every agent and message, then compact the database file and
    /// truncate the write-ahead log so nothing is left on disk. The daemon
    /// runs this at startup, which also clears whatever a crashed run left.
    /// Returns `(messages, agents)` removed.
    pub async fn clear(&self) -> Result<(i64, i64), MailErr> {
        let mut g = self.lock_open().await?;
        // Parked pulls fail unregistered: their names are gone.
        g.waiters.clear();
        clear_locked(&mut g)
    }

    /// Mark the bus closed: every parked waiter fails `shutting_down` and no
    /// further operations are accepted. Then discard the whole mailbox —
    /// roster and every unread message — and scrub it from disk as
    /// [`clear`](Self::clear) does, so nothing outlives the server run.
    ///
    /// Returns `Err` when the wipe transaction fails (rolled back); the bus
    /// is still closed either way.
    pub async fn shutdown(&self) -> Result<(), MailErr> {
        let mut g = self.inner.lock().await;
        g.closed = true;
        g.waiters.clear(); // dropping senders wakes all parked pulls
        clear_locked(&mut g).map(|_| ()).inspect_err(|_| {
            tracing::error!(
                "shutdown: failed to discard mailbox transactionally; \
                 roster or messages may persist on disk"
            );
        })
    }

    /// Mark the bus closed like [`shutdown`](Self::shutdown) but keep the
    /// roster and unread mail on disk (`MENTISCHAT_PERSIST=1`).
    pub async fn close(&self) {
        let mut g = self.inner.lock().await;
        g.closed = true;
        g.waiters.clear();
    }
}

fn clear_locked(g: &mut Inner) -> Result<(i64, i64), MailErr> {
    let tx = g.conn.transaction().map_err(sqlite_storage)?;
    let messages = tx
        .execute("DELETE FROM messages", [])
        .map_err(sqlite_storage)? as i64;
    let agents = tx
        .execute("DELETE FROM roster", [])
        .map_err(sqlite_storage)? as i64;
    tx.commit().map_err(sqlite_storage)?;
    // Best effort: the rows are gone either way; these shrink the file and
    // drop the old pages and log frames that held them.
    if let Err(e) = g.conn.execute_batch("VACUUM;") {
        tracing::warn!(error = %e, "could not compact the database");
    }
    truncate_wal(&g.conn);
    Ok((messages, agents))
}

/// Checkpoint the write-ahead log into the database and truncate it to zero
/// bytes, so deleted message text is not left behind in the `-wal` file
/// (`secure_delete` already zeroes it in the database pages). Best effort: a
/// failure is logged, never surfaced, because the rows are already gone.
pub(super) fn truncate_wal(conn: &rusqlite::Connection) {
    if let Err(e) = conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(())) {
        tracing::warn!(error = %e, "could not truncate the write-ahead log");
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

/// Insert a new roster row, refusing past [`MAX_ROSTER`], and record its
/// number if it has the `{provider}_{n}` shape; returns the stored
/// description (`None` for an empty one).
fn insert_agent(
    g: &mut Inner,
    name: &str,
    now: &str,
    description: Option<String>,
) -> Result<Option<String>, MailErr> {
    let agents: i64 = g
        .conn
        .query_row("SELECT COUNT(*) FROM roster", [], |r| r.get(0))
        .map_err(sqlite_storage)?;
    if agents >= MAX_ROSTER {
        return Err(MailErr::RosterFull { agents });
    }
    let stored = g
        .conn
        .query_row(
            "INSERT INTO roster (name, last_seen, description)
             VALUES (?1, ?2, NULLIF(?3, ''))
             RETURNING description",
            params![name, now, description],
            |r| r.get(0),
        )
        .map_err(sqlite_storage)?;
    if let Some((stem, n)) = numbered(name) {
        let high = g.issued.entry(stem.to_string()).or_default();
        *high = (*high).max(n);
    }
    Ok(stored)
}

/// `{provider}_{n}` one past the highest `n` in `issued` or on the roster
/// for `provider`; `None` when that would overflow `u64`.
fn next_provider_name(
    roster: &[String],
    provider: &str,
    issued: &HashMap<String, u64>,
) -> Option<String> {
    let highest = roster
        .iter()
        .filter_map(|name| numbered(name))
        .filter(|(stem, _)| *stem == provider)
        .map(|(_, n)| n)
        .chain(issued.get(provider).copied())
        .max()
        .unwrap_or(0);
    Some(format!("{provider}_{}", highest.checked_add(1)?))
}

/// Split `claude_code_3` into (`claude_code`, 3). Only a canonical positive
/// suffix counts: `claude_code_01` and `claude_code_0` do not.
fn numbered(name: &str) -> Option<(&str, u64)> {
    let (stem, suffix) = name.rsplit_once('_')?;
    let n: u64 = suffix.parse().ok()?;
    (n > 0 && n.to_string() == suffix).then_some((stem, n))
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
