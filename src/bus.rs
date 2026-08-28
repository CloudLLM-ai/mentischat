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
use rusqlite::OptionalExtension;
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
/// Max parked nudge-peek long-polls per name (wrapper restart storms cap).
pub const MAX_NUDGE_WAITERS_PER_AGENT: usize = 2;
/// Global cap on parked wake-session waiters across all unbound ids, so
/// arbitrary UUIDs cannot grow `wake_waiters` without bound.
pub const MAX_WAKE_WAITERS_GLOBAL: usize = 64;
/// Cap on roster rows — bounds waiter amplification (8 inbox + 2 peek per name).
pub const MAX_ROSTER: i64 = 256;
/// Cap on unacked ids echoed by one nudge peek. The wrapper uses them only to
/// build a debounce key, so a stalled agent holding a full queue must not turn
/// every 1 Hz poll into a 1024-uuid response.
pub const MAX_PEEK_IDS: i64 = 64;
/// Cap on ids accepted in one `ack_ids` array.
pub const MAX_ACK_IDS: usize = MAX_PENDING_PER_AGENT as usize;

/// How an idle session is woken. Stored on the roster row as
/// `nudge_transport`; one transport per agent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum NudgeTransport {
    /// `mentis-wake` owns wake via the peek endpoint.
    Wrapper,
    /// No doorbell; use `mentis-wake` or the session cannot be woken idle.
    None,
}

impl NudgeTransport {
    /// Stored column value.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Wrapper => "wrapper",
            Self::None => "none",
        }
    }

    /// Parse a wire value (`register.nudgeTransport`); absent means keep the
    /// existing roster transport on re-register. `terminal` is accepted as a
    /// deprecated alias for `none`.
    pub fn parse(raw: Option<&str>) -> Result<Option<Self>, MailErr> {
        match raw {
            None | Some("") => Ok(None),
            Some("none") | Some("terminal") => Ok(Some(Self::None)),
            Some("wrapper") => Ok(Some(Self::Wrapper)),
            Some(_) => Err(MailErr::InvalidBody),
        }
    }

    /// Read the column back; legacy `terminal` rows map to [`Self::None`].
    fn from_db(raw: String) -> Self {
        match raw.as_str() {
            "wrapper" => Self::Wrapper,
            _ => Self::None,
        }
    }
}

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
    /// Legacy v1 field; always `null` (AppleScript window binding removed).
    pub nudge_window: Option<String>,
    /// How this session is woken while idle.
    pub nudge_transport: NudgeTransport,
}

/// Roster entry.
#[derive(Debug, Clone, Serialize)]
pub struct RosterEntry {
    /// Agent name.
    pub name: String,
    /// UTC RFC 3339 timestamp of the last register / check_inbox.
    #[serde(rename = "lastSeen")]
    pub last_seen: String,
    /// Legacy v1 field; always `null` (AppleScript window binding removed).
    #[serde(rename = "nudgeWindow")]
    pub nudge_window: Option<String>,
    /// How this session is woken while idle.
    #[serde(rename = "nudgeTransport")]
    pub nudge_transport: NudgeTransport,
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

/// One `GET /nudge/{agent}` or `GET /wake/{wake_id}` long-poll result.
/// Read-only: never promotes, acks, or touches `last_seen`.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NudgePeek {
    /// Roster name when [`Self::bound`] is true; empty while waiting for
    /// register.
    pub agent: String,
    /// True once an agent has registered with this wake session's id.
    pub bound: bool,
    /// True when unacked rows exist (or arrived during the wait).
    pub pending: bool,
    /// Unacked message ids (pending + in_flight), seq order. Used by the
    /// wrapper for its own debounce mirror.
    pub ids: Vec<String>,
    /// The single pointer the wrapper injects — always the shared constant.
    pub pointer: String,
}

struct Waiter {
    id: u64,
    tx: oneshot::Sender<()>,
}

impl Waiter {
    /// True once the parked request is gone (client disconnected, task
    /// aborted, or the park timed out and dropped its receiver).
    fn canceled(&self) -> bool {
        self.tx.is_closed()
    }
}

/// Which waiter queue a park targets. Inbox/nudge queues are keyed by roster
/// name; wake queues by unbound session id.
#[derive(Clone, Copy)]
enum WaiterKind<'a> {
    Inbox(&'a str),
    Nudge(&'a str),
    Wake(&'a str),
}

impl<'a> WaiterKind<'a> {
    fn key(self) -> &'a str {
        match self {
            Self::Inbox(k) | Self::Nudge(k) | Self::Wake(k) => k,
        }
    }

    fn cap(self) -> usize {
        match self {
            Self::Inbox(_) => MAX_WAITERS_PER_AGENT,
            Self::Nudge(_) | Self::Wake(_) => MAX_NUDGE_WAITERS_PER_AGENT,
        }
    }
}

type WaiterMap = HashMap<String, VecDeque<Waiter>>;

struct Inner {
    conn: rusqlite::Connection,
    waiters: WaiterMap,
    nudge_waiters: WaiterMap,
    wake_waiters: WaiterMap,
    next_waiter_id: u64,
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
                nudge_waiters: HashMap::new(),
                wake_waiters: HashMap::new(),
                next_waiter_id: 0,
                closed: false,
            }),
        })
    }

    /// Claim `name` on the roster. Idempotent: refreshes `last_seen`, keeps
    /// any pending queue and the existing wake transport.
    pub async fn register(
        &self,
        name: &crate::names::AgentName,
    ) -> Result<RegisterOutcome, MailErr> {
        self.register_with_transport(name, None).await
    }

    /// Register, optionally setting the wake transport and/or binding a
    /// `mentis-wake` session id.
    ///
    /// `transport: None` on re-register refreshes `last_seen` only. On a new
    /// name the default transport is [`NudgeTransport::None`]. When `wake_id`
    /// is present, transport becomes [`NudgeTransport::Wrapper`].
    ///
    /// Session tokens change only as part of an explicit transition: a
    /// supplied `wake_id` replaces any previous token; an explicit
    /// `nudgeTransport` of `none` (or the deprecated `terminal`) clears it;
    /// a plain refresh and an assertion of `wrapper` keep the live binding.
    pub async fn register_with_wake(
        &self,
        name: &crate::names::AgentName,
        transport: Option<NudgeTransport>,
        wake_id: Option<&str>,
    ) -> Result<RegisterOutcome, MailErr> {
        let wake_id = match wake_id {
            Some(w) if !w.is_empty() => Some(crate::nudge::validate_wake_id(w)?),
            _ => None,
        };
        let now = now_rfc3339();
        let mut g = self.inner.lock().await;
        if g.closed {
            return Err(MailErr::ShuttingDown);
        }
        if let Some(wid) = wake_id.as_deref() {
            ensure_wake_id_free(&g, wid, name.as_str())?;
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
            if let Some(wid) = wake_id.as_deref() {
                g.conn
                    .execute(
                        "UPDATE roster SET last_seen = ?2, nudge_transport = 'wrapper',
                         wake_id = ?3 WHERE name = ?1",
                        params![name.as_str(), now, wid],
                    )
                    .map_err(sqlite_storage)?;
            } else if let Some(t) = transport {
                if matches!(t, NudgeTransport::None) {
                    // Explicitly disabling wake also drops the session token
                    // so a stale /wake/{id} poller cannot stay attached.
                    g.conn
                        .execute(
                            "UPDATE roster SET last_seen = ?2, nudge_transport = ?3,
                             wake_id = NULL WHERE name = ?1",
                            params![name.as_str(), now, t.as_str()],
                        )
                        .map_err(sqlite_storage)?;
                } else {
                    g.conn
                        .execute(
                            "UPDATE roster SET last_seen = ?2, nudge_transport = ?3
                             WHERE name = ?1",
                            params![name.as_str(), now, t.as_str()],
                        )
                        .map_err(sqlite_storage)?;
                }
            } else {
                g.conn
                    .execute(
                        "UPDATE roster SET last_seen = ?2 WHERE name = ?1",
                        params![name.as_str(), now],
                    )
                    .map_err(sqlite_storage)?;
            }
            let nudge_transport = roster_transport(&g, name.as_str())?;
            if let Some(wid) = wake_id.as_deref() {
                wake_wake_waiters(&mut g, wid);
            }
            drop(g);
            return Ok(RegisterOutcome {
                agent: name.to_string(),
                created: false,
                last_seen: now,
                nudge_window: None,
                nudge_transport,
            });
        }
        let agents: i64 = g
            .conn
            .query_row("SELECT COUNT(*) FROM roster", [], |r| r.get(0))
            .map_err(sqlite_storage)?;
        if agents >= MAX_ROSTER {
            return Err(MailErr::RosterFull { agents });
        }
        let transport = if wake_id.is_some() {
            NudgeTransport::Wrapper
        } else {
            transport.unwrap_or(NudgeTransport::None)
        };
        g.conn
            .execute(
                "INSERT INTO roster (name, last_seen, nudge_transport, wake_id)
                 VALUES (?1, ?2, ?3, ?4)",
                params![name.as_str(), now, transport.as_str(), wake_id],
            )
            .map_err(sqlite_storage)?;
        if let Some(wid) = wake_id.as_deref() {
            wake_wake_waiters(&mut g, wid);
        }
        drop(g);
        Ok(RegisterOutcome {
            agent: name.to_string(),
            created: true,
            last_seen: now,
            nudge_window: None,
            nudge_transport: transport,
        })
    }

    /// Register, optionally setting the wake transport (no wake session).
    pub async fn register_with_transport(
        &self,
        name: &crate::names::AgentName,
        transport: Option<NudgeTransport>,
    ) -> Result<RegisterOutcome, MailErr> {
        self.register_with_wake(name, transport, None).await
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
        let wake_id: Option<String> = g
            .conn
            .query_row(
                "SELECT wake_id FROM roster WHERE name = ?1",
                params![name.as_str()],
                |r| r.get::<_, Option<String>>(0),
            )
            .map_err(sqlite_storage)?
            .filter(|s| !s.trim().is_empty());
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
        g.nudge_waiters.remove(name.as_str());
        // A wrapper long-polling /wake/{id} for this name would otherwise sit
        // until its wait expires; wake it so it re-reads and sees `unbound`.
        if let Some(wid) = wake_id.as_deref() {
            wake_wake_waiters(&mut g, wid);
        }
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
            .prepare("SELECT name, last_seen, nudge_transport FROM roster ORDER BY name ASC")
            .map_err(sqlite_storage)?;
        let rows = stmt
            .query_map([], |r| {
                Ok(RosterEntry {
                    name: r.get(0)?,
                    last_seen: r.get(1)?,
                    nudge_window: None,
                    nudge_transport: NudgeTransport::from_db(r.get(2)?),
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
            .map_err(sqlite_storage)?;

        if let Some(q) = g.waiters.get_mut(to.as_str()) {
            wake_one(q);
        }
        if let Some(q) = g.nudge_waiters.get_mut(to.as_str()) {
            wake_one(q);
        }
        let wake_id: Option<String> = g
            .conn
            .query_row(
                "SELECT wake_id FROM roster WHERE name = ?1",
                params![to.as_str()],
                |r| r.get::<_, Option<String>>(0),
            )
            .map_err(sqlite_storage)?
            .filter(|s| !s.trim().is_empty());
        if let Some(wid) = wake_id.as_deref() {
            if let Some(q) = g.wake_waiters.get_mut(wid) {
                wake_one(q);
            }
        }
        drop(g);
        Ok(SendReceipt {
            id,
            from: from.to_string(),
            to: to.to_string(),
            created_at,
        })
    }

    /// Read-only nudge long-poll for one name. Returns pending when unacked
    /// rows exist; otherwise parks a lightweight waiter (separate from
    /// [`check_inbox`](Self::check_inbox) waiters) for up to `wait_ms` and
    /// wakes when a `send` lands for the name.
    ///
    /// Never promotes rows to `in_flight`, never acks, and never touches
    /// `last_seen`. Fails [`crate::errors::MailboxError::TooManyWaiters`] when
    /// [`MAX_NUDGE_WAITERS_PER_AGENT`] peeks are already parked for this name.
    pub async fn nudge_peek(
        &self,
        name: &crate::names::AgentName,
        wait_ms: u32,
    ) -> Result<NudgePeek, MailErr> {
        let mut g = self.inner.lock().await;
        if g.closed {
            return Err(MailErr::ShuttingDown);
        }
        ensure_registered(&g, name.as_str())?;
        if let Some(ids) = unacked_ids(&g, name.as_str())? {
            return Ok(peek_pending(name.as_str(), ids));
        }
        if wait_ms == 0 {
            return Ok(peek_empty(name.as_str()));
        }

        // Park under the mutex so a concurrent send cannot land between "no
        // mail" and "waiting".
        let (waiter_id, rx) = try_park(&mut g, WaiterKind::Nudge(name.as_str()))?;
        drop(g);

        let _ =
            tokio::time::timeout(std::time::Duration::from_millis(u64::from(wait_ms)), rx).await;

        let mut g = self.inner.lock().await;
        unpark(&mut g, WaiterKind::Nudge(name.as_str()), waiter_id);
        if g.closed {
            return Err(MailErr::ShuttingDown);
        }
        ensure_registered(&g, name.as_str())?;
        Ok(match unacked_ids(&g, name.as_str())? {
            Some(ids) => peek_pending(name.as_str(), ids),
            None => peek_empty(name.as_str()),
        })
    }

    /// Read-only long-poll for a `mentis-wake` session before or after the
    /// agent registers. Unbound sessions park until `register` supplies the
    /// same `wake_id`.
    pub async fn nudge_peek_by_wake(
        &self,
        wake_id: &str,
        wait_ms: u32,
    ) -> Result<NudgePeek, MailErr> {
        let wake_id = crate::nudge::validate_wake_id(wake_id)?;
        let mut g = self.inner.lock().await;
        if g.closed {
            return Err(MailErr::ShuttingDown);
        }
        if let Some(agent) = agent_by_wake_id(&g, &wake_id)? {
            if let Some(ids) = unacked_ids(&g, &agent)? {
                return Ok(peek_pending(&agent, ids));
            }
            if wait_ms == 0 {
                return Ok(peek_empty(&agent));
            }
            let (waiter_id, rx) = try_park(&mut g, WaiterKind::Nudge(&agent))?;
            drop(g);
            let _ = tokio::time::timeout(std::time::Duration::from_millis(u64::from(wait_ms)), rx)
                .await;
            let mut g = self.inner.lock().await;
            unpark(&mut g, WaiterKind::Nudge(&agent), waiter_id);
            if g.closed {
                return Err(MailErr::ShuttingDown);
            }
            return Ok(match agent_by_wake_id(&g, &wake_id)? {
                Some(agent) => match unacked_ids(&g, &agent)? {
                    Some(ids) => peek_pending(&agent, ids),
                    None => peek_empty(&agent),
                },
                None => peek_unbound(),
            });
        }
        if wait_ms == 0 {
            return Ok(peek_unbound());
        }
        let (waiter_id, rx) = try_park(&mut g, WaiterKind::Wake(&wake_id))?;
        drop(g);
        let _ =
            tokio::time::timeout(std::time::Duration::from_millis(u64::from(wait_ms)), rx).await;
        let mut g = self.inner.lock().await;
        unpark(&mut g, WaiterKind::Wake(&wake_id), waiter_id);
        if g.closed {
            return Err(MailErr::ShuttingDown);
        }
        Ok(match agent_by_wake_id(&g, &wake_id)? {
            Some(agent) => match unacked_ids(&g, &agent)? {
                Some(ids) => peek_pending(&agent, ids),
                None => peek_empty(&agent),
            },
            None => peek_unbound(),
        })
    }

    /// Clear a wake session on `mentis-wake` exit (transport → `none`).
    pub async fn unbind_wake(&self, wake_id: &str) -> Result<(), MailErr> {
        let wake_id = crate::nudge::validate_wake_id(wake_id)?;
        let mut g = self.inner.lock().await;
        if g.closed {
            return Err(MailErr::ShuttingDown);
        }
        let agent = agent_by_wake_id(&g, &wake_id)?;
        g.conn
            .execute(
                "UPDATE roster SET nudge_transport = 'none', wake_id = NULL WHERE wake_id = ?1",
                params![wake_id],
            )
            .map_err(sqlite_storage)?;
        if let Some(a) = agent.as_deref() {
            if let Some(q) = g.nudge_waiters.remove(a) {
                for w in q {
                    let _ = w.tx.send(());
                }
            }
        }
        wake_wake_waiters(&mut g, &wake_id);
        Ok(())
    }

    /// Bind `mentis-wake` as the doorbell for an already-registered name:
    /// sets transport to `wrapper`, clears any Terminal window binding, and
    /// records `wake_id` (generated when absent) as the binding's ownership
    /// token. Returns the effective token; only its holder may unbind **or
    /// re-bind**.
    ///
    /// When a binding already exists, the request must present that same
    /// token (idempotent refresh). A silent mint while bound is `forbidden`
    /// so a peer cannot hijack the doorbell — take over via
    /// [`register_with_wake`](Self::register_with_wake) with a new `wakeId`,
    /// or unbind first.
    pub async fn bind_wrapper(
        &self,
        name: &crate::names::AgentName,
        wake_id: Option<&str>,
    ) -> Result<String, MailErr> {
        let requested = match wake_id {
            Some(w) if !w.trim().is_empty() => Some(crate::nudge::validate_wake_id(w)?),
            _ => None,
        };
        let mut g = self.inner.lock().await;
        if g.closed {
            return Err(MailErr::ShuttingDown);
        }
        ensure_registered(&g, name.as_str())?;
        let current: Option<String> = g
            .conn
            .query_row(
                "SELECT wake_id FROM roster WHERE name = ?1",
                params![name.as_str()],
                |r| r.get::<_, Option<String>>(0),
            )
            .map_err(sqlite_storage)?
            .filter(|s| !s.trim().is_empty());

        let token = match (current.as_deref(), requested.as_deref()) {
            (Some(cur), Some(req)) if cur == req => cur.to_string(),
            (Some(_), _) => {
                return Err(MailErr::Forbidden {
                    agent: name.to_string(),
                });
            }
            (None, Some(req)) => {
                ensure_wake_id_free(&g, req, name.as_str())?;
                req.to_string()
            }
            (None, None) => uuid::Uuid::new_v4().to_string(),
        };

        g.conn
            .execute(
                "UPDATE roster SET nudge_transport = 'wrapper', wake_id = ?2
                 WHERE name = ?1",
                params![name.as_str(), token],
            )
            .map_err(sqlite_storage)?;
        wake_wake_waiters(&mut g, &token);
        Ok(token)
    }

    /// Clear a wrapper binding on clean exit: transport becomes `none` (no
    /// doorbell). Ownership-safe: succeeds only when `wake_id` still owns the
    /// binding, so a stale wrapper cannot clear its replacement's state. An
    /// already-unbound agent is idempotently fine.
    pub async fn unbind_wrapper(
        &self,
        name: &crate::names::AgentName,
        wake_id: &str,
    ) -> Result<(), MailErr> {
        let token = crate::nudge::validate_wake_id(wake_id)?;
        let mut g = self.inner.lock().await;
        if g.closed {
            return Err(MailErr::ShuttingDown);
        }
        ensure_registered(&g, name.as_str())?;
        let row: (String, Option<String>) = g
            .conn
            .query_row(
                "SELECT nudge_transport, wake_id FROM roster WHERE name = ?1",
                params![name.as_str()],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .map_err(sqlite_storage)?;
        match row.1.filter(|s| !s.trim().is_empty()) {
            Some(current) if current == token => {
                g.conn
                    .execute(
                        "UPDATE roster SET nudge_transport = 'none', wake_id = NULL WHERE name = ?1",
                        params![name.as_str()],
                    )
                    .map_err(sqlite_storage)?;
                if let Some(q) = g.nudge_waiters.remove(name.as_str()) {
                    for w in q {
                        let _ = w.tx.send(());
                    }
                }
                wake_wake_waiters(&mut g, &token);
                Ok(())
            }
            // Nothing bound: a second clean exit is a no-op.
            None if NudgeTransport::from_db(row.0) == NudgeTransport::None => Ok(()),
            Some(_) => Err(MailErr::Forbidden {
                agent: name.to_string(),
            }),
            None => Err(MailErr::Forbidden {
                agent: name.to_string(),
            }),
        }
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
        let (waiter_id, rx) = try_park(&mut g, WaiterKind::Inbox(name.as_str()))?;
        drop(g);

        let _ =
            tokio::time::timeout(std::time::Duration::from_millis(u64::from(wait_ms)), rx).await;

        let mut g = self.inner.lock().await;
        unpark(&mut g, WaiterKind::Inbox(name.as_str()), waiter_id);
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
        g.nudge_waiters.clear(); // ...and all parked nudge peeks
        g.wake_waiters.clear();
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

fn roster_transport(g: &Inner, name: &str) -> Result<NudgeTransport, MailErr> {
    let raw: String = g
        .conn
        .query_row(
            "SELECT nudge_transport FROM roster WHERE name = ?1",
            params![name],
            |r| r.get(0),
        )
        .map_err(sqlite_storage)?;
    Ok(NudgeTransport::from_db(raw))
}

fn peek_pending(agent: &str, ids: Vec<String>) -> NudgePeek {
    NudgePeek {
        agent: agent.to_string(),
        bound: true,
        pending: true,
        ids,
        pointer: crate::nudge::POINTER_TEXT.to_string(),
    }
}

fn peek_empty(agent: &str) -> NudgePeek {
    NudgePeek {
        agent: agent.to_string(),
        bound: true,
        pending: false,
        ids: Vec::new(),
        pointer: crate::nudge::POINTER_TEXT.to_string(),
    }
}

fn peek_unbound() -> NudgePeek {
    NudgePeek {
        agent: String::new(),
        bound: false,
        pending: false,
        ids: Vec::new(),
        pointer: crate::nudge::POINTER_TEXT.to_string(),
    }
}

fn agent_by_wake_id(g: &Inner, wake_id: &str) -> Result<Option<String>, MailErr> {
    let agent: Option<String> = g
        .conn
        .query_row(
            "SELECT name FROM roster WHERE wake_id = ?1",
            params![wake_id],
            |r| r.get(0),
        )
        .optional()
        .map_err(sqlite_storage)?;
    Ok(agent)
}

fn ensure_wake_id_free(g: &Inner, wake_id: &str, name: &str) -> Result<(), MailErr> {
    let owner: Option<String> = g
        .conn
        .query_row(
            "SELECT name FROM roster WHERE wake_id = ?1",
            params![wake_id],
            |r| r.get(0),
        )
        .optional()
        .map_err(sqlite_storage)?;
    match owner {
        None => Ok(()),
        Some(o) if o == name => Ok(()),
        Some(_) => Err(MailErr::InvalidBody),
    }
}

fn wake_wake_waiters(g: &mut Inner, wake_id: &str) {
    if let Some(q) = g.wake_waiters.remove(wake_id) {
        for w in q {
            let _ = w.tx.send(());
        }
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

/// Drop every canceled waiter from `map`, removing queues left empty.
fn prune_canceled(map: &mut WaiterMap) {
    map.retain(|_, q| {
        q.retain(|w| !w.canceled());
        !q.is_empty()
    });
}

/// Park a fresh waiter for `kind`, enforcing the per-queue cap plus the
/// global budget on parked wake-session waiters.
///
/// Returns `(waiter id, receiver)`; the caller awaits the receiver outside
/// the bus lock and must later call [`unpark`] with the id.
fn try_park(g: &mut Inner, kind: WaiterKind<'_>) -> Result<(u64, oneshot::Receiver<()>), MailErr> {
    if let WaiterKind::Wake(_) = kind {
        prune_canceled(&mut g.wake_waiters);
        let total: usize = g.wake_waiters.values().map(VecDeque::len).sum();
        if total >= MAX_WAKE_WAITERS_GLOBAL {
            return Err(MailErr::TooManyWaiters {
                agent: kind.key().to_string(),
                waiting: total,
            });
        }
    }
    let key = kind.key();
    let existing_len = match kind {
        WaiterKind::Inbox(_) => g.waiters.get(key).map_or(0, VecDeque::len),
        WaiterKind::Nudge(_) => g.nudge_waiters.get(key).map_or(0, VecDeque::len),
        WaiterKind::Wake(_) => g.wake_waiters.get(key).map_or(0, VecDeque::len),
    };
    if existing_len >= kind.cap() {
        return Err(MailErr::TooManyWaiters {
            agent: key.to_string(),
            waiting: existing_len,
        });
    }
    let id = g.next_waiter_id;
    g.next_waiter_id += 1;
    let (tx, rx) = oneshot::channel();
    let waiter = Waiter { id, tx };
    match kind {
        WaiterKind::Inbox(k) => g.waiters.entry(k.to_string()).or_default(),
        WaiterKind::Nudge(k) => g.nudge_waiters.entry(k.to_string()).or_default(),
        WaiterKind::Wake(k) => g.wake_waiters.entry(k.to_string()).or_default(),
    }
    .push_back(waiter);
    Ok((id, rx))
}

/// Remove a settled waiter (woken, timed out, or canceled) and drop its
/// queue when empty so idle keys cannot accumulate.
fn unpark(g: &mut Inner, kind: WaiterKind<'_>, id: u64) {
    let map: &mut WaiterMap = match kind {
        WaiterKind::Inbox(_) => &mut g.waiters,
        WaiterKind::Nudge(_) => &mut g.nudge_waiters,
        WaiterKind::Wake(_) => &mut g.wake_waiters,
    };
    if let Some(q) = map.get_mut(kind.key()) {
        q.retain(|w| w.id != id);
        if q.is_empty() {
            map.remove(kind.key());
        }
    }
}

fn unacked_ids(g: &Inner, name: &str) -> Result<Option<Vec<String>>, MailErr> {
    let mut stmt = g
        .conn
        .prepare("SELECT id FROM messages WHERE recipient = ?1 ORDER BY seq ASC LIMIT ?2")
        .map_err(sqlite_storage)?;
    let ids: Vec<String> = stmt
        .query_map(params![name, MAX_PEEK_IDS], |r| r.get(0))
        .map_err(sqlite_storage)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(sqlite_storage)?;
    if ids.is_empty() {
        Ok(None)
    } else {
        Ok(Some(ids))
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
