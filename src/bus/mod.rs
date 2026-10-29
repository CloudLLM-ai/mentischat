//! The mailbox: roster, caps, read marking, blocking pulls, history, held
//! names, and expiry of read mail and idle agents.
//!
//! All state sits behind a single [`tokio::sync::Mutex`]; sqlite writes run
//! inside the lock so waiters and rows cannot tear. The lock is **never**
//! held across the park await — a waiter parks on a one-shot channel and
//! re-acquires to deliver, which is what makes the send/timeout race safe.
//!
//! Names compare without regard to ASCII case: the roster and message columns
//! use `COLLATE NOCASE`, and in-memory maps (waiters, held names) are keyed
//! by [`AgentName::folded`]. Responses use the spelling the roster stores.

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use rusqlite::{params, OptionalExtension};
use tokio::sync::{Mutex, MutexGuard};

use crate::errors::TakenReason;
use crate::names::AgentName;

mod clock;
mod delivery;
mod model;
mod waiters;

pub use crate::store::StoreError;
pub use clock::{now_rfc3339, rfc3339};
pub use model::{
    parse_duration, AgentOverview, DiscardOutcome, HistoryPage, InboxPage, Message, Overview,
    RegisterOutcome, Registration, Retention, RosterEntry, RosterListing, SendReceipt, Stats,
    SweepOutcome, MAX_BATCH, MAX_BODY_BYTES, MAX_DESCRIPTION_BYTES, MAX_PENDING_PER_AGENT,
    MAX_RETURN_BYTES, MAX_ROSTER, MAX_TOTAL_MESSAGES, MAX_WAITERS_PER_AGENT, MAX_WAIT_MS,
};

use delivery::{empty_page, history_locked, take_locked, touch_last_seen};
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
    /// Names that left the roster recently, by folded name. Only `reclaim`
    /// takes one before its hold ends, so a client still using the old name
    /// cannot read a new agent's mail.
    held: HashMap<String, Hold>,
    closed: bool,
}

/// A released name and what its previous holder needs to resume it.
#[derive(Debug, Clone)]
struct Hold {
    until: SystemTime,
    /// The previous registration's start; a reclaim keeps it, so the agent's
    /// sent mail is still in its `history`.
    registered_at: String,
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
    /// bus.register(&mentischat::names::AgentName::parse("Vega")?).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn new(data_dir: &Path) -> Result<Self, StoreError> {
        Self::with_retention(data_dir, Retention::default())
    }

    /// Open the store under `data_dir`, expiring read mail and idle agents and
    /// holding released names per `retention`.
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
                held: HashMap::new(),
                closed: false,
            })),
            wal: Arc::new(std::sync::Mutex::new(wal)),
            retention,
        })
    }

    /// How long read mail and idle agents are kept, and released names held.
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
    /// when a new name would exceed [`MAX_ROSTER`]; `invalid_description`;
    /// `storage_error` on a sqlite failure.
    pub async fn register_with(
        &self,
        name: &AgentName,
        opts: Registration,
    ) -> Result<RegisterOutcome, MailErr> {
        let description = opts.description.map(validate_description).transpose()?;
        let provider = opts.provider.map(|p| p.as_str().to_string());
        let mut g = self.lock_open().await?;
        // Stamped under the lock so concurrent registers cannot write last_seen
        // out of order.
        let now = SystemTime::now();
        let stamp = rfc3339(now);
        if let Some(stored) = registered_name(&g, name.as_str())? {
            if !opts.reclaim {
                return Err(name_taken(&g, stored, TakenReason::Registered, now));
            }
            let (provider, description) =
                refresh_agent(&g, &stored, &stamp, provider, description)?;
            return Ok(RegisterOutcome {
                agent: stored,
                created: false,
                last_seen: stamp,
                provider,
                description,
            });
        }
        let key = name.folded();
        let hold = g.held.get(&key).filter(|h| h.until > now).cloned();
        if let (Some(h), false) = (&hold, opts.reclaim) {
            let reason = TakenReason::Held {
                until: rfc3339(h.until),
            };
            return Err(name_taken(&g, name.to_string(), reason, now));
        }
        let registered_at = hold.map_or_else(|| stamp.clone(), |h| h.registered_at);
        let (provider, description) = insert_agent(
            &g,
            name.as_str(),
            &stamp,
            &registered_at,
            provider,
            description,
        )?;
        g.held.remove(&key);
        Ok(RegisterOutcome {
            agent: name.to_string(),
            created: true,
            last_seen: stamp,
            provider,
            description,
        })
    }

    /// Leave the roster, deleting the mail addressed to this name (read or
    /// not) and waking its parked pulls (they fail `unregistered`). Messages
    /// it sent stay with their recipients. The name is then held for
    /// [`Retention::name_hold`].
    pub async fn deregister(&self, name: &AgentName) -> Result<DiscardOutcome, MailErr> {
        let mut g = self.lock_open().await?;
        let stored = resolve(&g, name.as_str())?;
        let (discarded, registered_at) = {
            let tx = g.conn.transaction().map_err(sqlite_storage)?;
            let discarded = tx
                .execute("DELETE FROM messages WHERE recipient = ?1", params![stored])
                .map_err(sqlite_storage)? as i64;
            let registered_at: String = tx
                .query_row(
                    "DELETE FROM roster WHERE name = ?1 RETURNING registered_at",
                    params![stored],
                    |r| r.get(0),
                )
                .map_err(sqlite_storage)?;
            tx.commit().map_err(sqlite_storage)?;
            (discarded, registered_at)
        };
        let until = SystemTime::now() + self.retention.name_hold;
        g.held.insert(
            name.folded(),
            Hold {
                until,
                registered_at,
            },
        );
        // Dropping senders wakes those pulls; they re-lock, see the name is
        // gone, and fail unregistered.
        g.waiters.remove(&name.folded());
        drop(g);
        if discarded > 0 {
            self.scrub_wal().await;
        }
        Ok(DiscardOutcome {
            agent: stored,
            discarded,
        })
    }

    /// Current roster sorted by name ascending, with each agent's provider,
    /// description and unread count, plus free names to suggest. Read-only:
    /// does not touch `last_seen`.
    pub async fn list_agents(&self) -> Result<RosterListing, MailErr> {
        let g = self.lock_open().await?;
        let agents = roster_rows(&g)?
            .into_iter()
            .map(|row| RosterEntry {
                name: row.name,
                provider: row.provider,
                description: row.description,
                last_seen: row.last_seen,
                queued: row.queued,
            })
            .collect();
        Ok(RosterListing {
            agents,
            suggestions: free_suggestions(&g, SystemTime::now())?,
        })
    }

    /// The roster with each agent's provider, description, unread count and
    /// last-seen age, plus the number of messages sent since the daemon
    /// started, for the operator terminal view.
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
                provider: row.provider,
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
    /// roster entries (`from` first). At [`MAX_TOTAL_MESSAGES`] rows the
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
        let mut g = self.lock_open().await?;
        // Stamped under the lock so createdAt order matches seq order.
        let created_at = now_rfc3339();
        let sender = resolve(&g, from.as_str())?;
        let recipient = resolve(&g, to.as_str())?;

        // All three counts from one scan of the (small, capped) table.
        let (n_to, n_unread, n_all): (i64, i64, i64) = g
            .conn
            .query_row(
                "SELECT COALESCE(SUM(recipient = ?1 AND read_at IS NULL), 0),
                        COALESCE(SUM(read_at IS NULL), 0),
                        COUNT(*)
                   FROM messages",
                params![recipient],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .map_err(sqlite_storage)?;
        if n_to >= MAX_PENDING_PER_AGENT {
            return Err(MailErr::QueueFull {
                to: recipient,
                pending: n_to,
            });
        }
        if n_unread >= MAX_TOTAL_MESSAGES {
            return Err(MailErr::StorageFull { pending: n_unread });
        }
        // Read rows fill the gap between the unread count and the cap, so
        // there are always enough of them to prune.
        let prune = (n_all - MAX_TOTAL_MESSAGES + 1).max(0);

        let id = uuid::Uuid::new_v4().to_string();
        let seq = g.next_seq;
        // One transaction: a failure stores (and prunes) nothing, so the
        // caller's error is the whole truth and a retry cannot duplicate the
        // message.
        {
            let tx = g.conn.transaction().map_err(sqlite_storage)?;
            if prune > 0 {
                tx.execute(
                    "DELETE FROM messages WHERE id IN (
                       SELECT id FROM messages WHERE read_at IS NOT NULL ORDER BY seq LIMIT ?1)",
                    params![prune],
                )
                .map_err(sqlite_storage)?;
            }
            tx.execute(
                "INSERT INTO messages (id, sender, recipient, body, created_at, seq)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![id, sender, recipient, body, created_at, seq],
            )
            .map_err(sqlite_storage)?;
            // Sending is activity too: an agent that only sends must not age
            // off the roster.
            tx.execute(
                "UPDATE roster SET last_seen = ?2 WHERE name = ?1",
                params![sender, created_at],
            )
            .map_err(sqlite_storage)?;
            tx.commit().map_err(sqlite_storage)?;
        }
        g.next_seq += 1;
        g.messages_sent += 1;
        if let Some(q) = g.waiters.get_mut(&to.folded()) {
            wake_one(q);
        }
        drop(g);
        if prune > 0 {
            self.scrub_wal().await;
        }
        Ok(SendReceipt {
            id,
            from: sender,
            to: recipient,
            created_at,
        })
    }

    /// Pull (and optionally block for) the next batch of unread mail for
    /// `name`. Returned messages are marked read: `check_inbox` hands each
    /// one out once, and [`history`](Self::history) shows it again until
    /// [`Retention::read_ttl`] after it was read.
    ///
    /// Parks only when the inbox is empty and `wait_ms > 0`; timeout is an
    /// empty success.
    ///
    /// Fails [`crate::errors::MailboxError::TooManyWaiters`] when
    /// [`MAX_WAITERS_PER_AGENT`] pulls are already parked for this name.
    pub async fn check_inbox(&self, name: &AgentName, wait_ms: u32) -> Result<InboxPage, MailErr> {
        let key = name.folded();
        let mut g = self.lock_open().await?;
        let stored = resolve(&g, name.as_str())?;
        touch_last_seen(&mut g, &stored)?;

        if let Some(page) = take_locked(&mut g, &stored)? {
            return Ok(page);
        }
        if wait_ms == 0 {
            return Ok(empty_page(&stored));
        }

        // Park until mail arrives or the deadline passes. A wake can find
        // nothing to deliver (another pull under the same name took it, or a
        // cancelled waiter handed its wake on); that waiter parks again for
        // the time left instead of returning empty early.
        let deadline = tokio::time::Instant::now() + Duration::from_millis(u64::from(wait_ms));
        loop {
            // Park under the mutex: register the waiter before releasing so a
            // concurrent send cannot land between "inbox empty" and "waiting".
            let (waiter_id, rx) = try_park(&mut g, &key, &stored)?;
            drop(g);

            let mut guard = ParkGuard::new(self.inner.clone(), &key, waiter_id, rx);
            guard.wait_until(deadline).await;

            g = self.inner.lock().await;
            guard.settle();
            unpark(&mut g, &key, waiter_id);
            if g.closed {
                return Err(MailErr::ShuttingDown);
            }
            let stored = resolve(&g, name.as_str())?;
            // The agent was waiting all along; record when the pull ended.
            touch_last_seen(&mut g, &stored)?;
            if let Some(page) = take_locked(&mut g, &stored)? {
                return Ok(page);
            }
            if tokio::time::Instant::now() >= deadline {
                return Ok(empty_page(&stored));
            }
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
    /// `unregistered` when `name` is not on the roster; `shutting_down`;
    /// `storage_error`.
    pub async fn history(
        &self,
        name: &AgentName,
        with: Option<&AgentName>,
        before: Option<i64>,
    ) -> Result<HistoryPage, MailErr> {
        let mut g = self.lock_open().await?;
        let stored = resolve(&g, name.as_str())?;
        touch_last_seen(&mut g, &stored)?;
        history_locked(&g, &stored, with.map(AgentName::as_str), before)
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
    /// are forgotten. When anything was deleted the write-ahead log is
    /// truncated so the removed text does not linger on disk.
    pub async fn sweep_at(&self, now: SystemTime) -> Result<SweepOutcome, MailErr> {
        let mut g = self.lock_open().await?;
        let cutoff =
            |ttl: Duration| rfc3339(now.checked_sub(ttl).unwrap_or(SystemTime::UNIX_EPOCH));
        let stale: Vec<(String, String)> = {
            let mut stmt = g
                .conn
                .prepare(
                    "SELECT name, registered_at FROM roster WHERE last_seen < ?1 ORDER BY name",
                )
                .map_err(sqlite_storage)?;
            let rows = stmt
                .query_map(params![cutoff(self.retention.agent_ttl)], |r| {
                    Ok((r.get(0)?, r.get(1)?))
                })
                .map_err(sqlite_storage)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(sqlite_storage)?;
            rows
        };
        let dropped: Vec<(String, String)> = stale
            .into_iter()
            .filter(|(n, _)| !has_live_waiter(&g.waiters, &n.to_ascii_lowercase()))
            .collect();

        let (expired_read, discarded) = {
            let tx = g.conn.transaction().map_err(sqlite_storage)?;
            let expired_read = tx
                .execute(
                    "DELETE FROM messages WHERE read_at < ?1",
                    params![cutoff(self.retention.read_ttl)],
                )
                .map_err(sqlite_storage)? as i64;
            let mut discarded = 0i64;
            for (name, _) in &dropped {
                discarded += tx
                    .execute("DELETE FROM messages WHERE recipient = ?1", params![name])
                    .map_err(sqlite_storage)? as i64;
                tx.execute("DELETE FROM roster WHERE name = ?1", params![name])
                    .map_err(sqlite_storage)?;
            }
            tx.commit().map_err(sqlite_storage)?;
            (expired_read, discarded)
        };
        g.held.retain(|_, h| h.until > now);
        for (name, registered_at) in &dropped {
            let key = name.to_ascii_lowercase();
            g.waiters.remove(&key);
            g.held.insert(
                key,
                Hold {
                    until: now + self.retention.name_hold,
                    registered_at: registered_at.clone(),
                },
            );
        }
        let out = SweepOutcome {
            expired_read,
            discarded,
            dropped_agents: dropped.into_iter().map(|(n, _)| n).collect(),
        };
        drop(g);
        if !out.is_empty() {
            self.scrub_wal().await;
        }
        Ok(out)
    }

    /// Roster size and unread and read message counts, for `/health`. Does
    /// not deliver anything or touch `last_seen`.
    pub async fn stats(&self) -> Result<Stats, MailErr> {
        let g = self.lock_open().await?;
        g.conn
            .query_row(
                "SELECT (SELECT COUNT(*) FROM roster),
                        (SELECT COUNT(*) FROM messages WHERE read_at IS NULL),
                        (SELECT COUNT(*) FROM messages WHERE read_at IS NOT NULL)",
                [],
                |r| {
                    Ok(Stats {
                        agents: r.get(0)?,
                        unread: r.get(1)?,
                        read: r.get(2)?,
                    })
                },
            )
            .map_err(sqlite_storage)
    }

    /// Delete every agent, message and held name, then compact the database
    /// file and truncate the write-ahead log so nothing is left on disk. The
    /// daemon runs this at startup, which also clears whatever a crashed run
    /// left. Returns `(messages, agents)` removed.
    pub async fn clear(&self) -> Result<(i64, i64), MailErr> {
        let mut g = self.lock_open().await?;
        // Parked pulls fail unregistered: their names are gone.
        g.waiters.clear();
        clear_locked(&mut g)
    }

    /// Mark the bus closed: every parked waiter fails `shutting_down` and no
    /// further operations are accepted. Then discard the whole mailbox —
    /// roster and every message, read or not — and scrub it from disk as
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
    /// roster and mail on disk (`MENTISCHAT_PERSIST=1`).
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
    g.held.clear();
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

/// The roster's spelling of `name`, or `None` when it is not registered.
fn registered_name(g: &Inner, name: &str) -> Result<Option<String>, MailErr> {
    g.conn
        .query_row(
            "SELECT name FROM roster WHERE name = ?1",
            params![name],
            |r| r.get(0),
        )
        .optional()
        .map_err(sqlite_storage)
}

/// The roster's spelling of `name`, or `unregistered` with the roster.
fn resolve(g: &Inner, name: &str) -> Result<String, MailErr> {
    match registered_name(g, name)? {
        Some(stored) => Ok(stored),
        None => Err(MailErr::Unregistered {
            agent: name.to_string(),
            known: roster_sorted(g)?,
        }),
    }
}

/// `name_taken` for `agent`, with free names to pick instead (or the
/// `storage_error` that stopped them being listed).
fn name_taken(g: &Inner, agent: String, reason: TakenReason, now: SystemTime) -> MailErr {
    match free_suggestions(g, now) {
        Ok(suggestions) => MailErr::NameTaken {
            agent,
            reason,
            suggestions,
        },
        Err(e) => e,
    }
}

/// Suggested names that are neither on the roster nor held at `now`.
fn free_suggestions(g: &Inner, now: SystemTime) -> Result<Vec<String>, MailErr> {
    let taken: HashSet<String> = roster_sorted(g)?
        .into_iter()
        .map(|n| n.to_ascii_lowercase())
        .chain(
            g.held
                .iter()
                .filter(|(_, h)| h.until > now)
                .map(|(k, _)| k.clone()),
        )
        .collect();
    Ok(crate::names::suggestions(|n| {
        taken.contains(&n.to_ascii_lowercase())
    }))
}

/// Insert a new roster row, refusing past [`MAX_ROSTER`]; returns the stored
/// provider and description (`None` for an empty description).
fn insert_agent(
    g: &Inner,
    name: &str,
    now: &str,
    registered_at: &str,
    provider: Option<String>,
    description: Option<String>,
) -> Result<(Option<String>, Option<String>), MailErr> {
    let agents: i64 = g
        .conn
        .query_row("SELECT COUNT(*) FROM roster", [], |r| r.get(0))
        .map_err(sqlite_storage)?;
    if agents >= MAX_ROSTER {
        return Err(MailErr::RosterFull { agents });
    }
    g.conn
        .query_row(
            "INSERT INTO roster (name, provider, description, registered_at, last_seen)
             VALUES (?1, ?2, NULLIF(?3, ''), ?4, ?5)
             RETURNING provider, description",
            params![name, provider, description, registered_at, now],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .map_err(sqlite_storage)
}

/// Refresh a registered agent's `last_seen`, and its provider and
/// description when given (a NULL keeps the stored value; an empty
/// description clears it). Returns the stored provider and description.
fn refresh_agent(
    g: &Inner,
    name: &str,
    now: &str,
    provider: Option<String>,
    description: Option<String>,
) -> Result<(Option<String>, Option<String>), MailErr> {
    g.conn
        .query_row(
            "UPDATE roster
                SET last_seen = ?2,
                    provider = COALESCE(?3, provider),
                    description = CASE WHEN ?4 IS NULL THEN description
                                       ELSE NULLIF(?4, '') END
              WHERE name = ?1
          RETURNING provider, description",
            params![name, now, provider, description],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .map_err(sqlite_storage)
}

/// One roster row with everything `list_agents` and `overview` need.
struct RosterRow {
    name: String,
    provider: Option<String>,
    description: Option<String>,
    last_seen: String,
    seen_secs_ago: i64,
    queued: i64,
}

fn roster_rows(g: &Inner) -> Result<Vec<RosterRow>, MailErr> {
    let mut stmt = g
        .conn
        .prepare(
            "SELECT r.name, r.provider, r.description, r.last_seen,
                    MAX(0, COALESCE(CAST(strftime('%s', 'now') AS INTEGER)
                                    - CAST(strftime('%s', r.last_seen) AS INTEGER), 0)),
                    (SELECT COUNT(*) FROM messages m
                      WHERE m.recipient = r.name AND m.read_at IS NULL)
             FROM roster r ORDER BY r.name ASC",
        )
        .map_err(sqlite_storage)?;
    let rows = stmt
        .query_map([], |r| {
            Ok(RosterRow {
                name: r.get(0)?,
                provider: r.get(1)?,
                description: r.get(2)?,
                last_seen: r.get(3)?,
                seen_secs_ago: r.get(4)?,
                queued: r.get(5)?,
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
