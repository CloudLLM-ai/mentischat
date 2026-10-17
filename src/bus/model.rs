//! Caps, retention and the public value types of the mailbox: wire shapes returned by the
//! tools and REST routes, and the read model behind the terminal view.

use std::time::Duration;

use serde::Serialize;

/// Maximum UTF-8 byte length of a message body.
pub const MAX_BODY_BYTES: usize = 64 * 1024;
/// Per-recipient cap on unread messages.
pub const MAX_PENDING_PER_AGENT: i64 = 1024;
/// Global cap on rows in `messages`.
pub const MAX_TOTAL_MESSAGES: i64 = 8192;
/// Max messages returned by one `check_inbox`.
pub const MAX_BATCH: i64 = 8;
/// Max summed serialized-JSON bytes of the messages returned by one
/// `check_inbox` (first message always included).
pub const MAX_RETURN_BYTES: usize = 16 * 1024;
/// Upper bound of `wait_ms`.
pub const MAX_WAIT_MS: u32 = 30_000;
/// Max parked pulls per name; an additional park fails.
pub const MAX_WAITERS_PER_AGENT: usize = 8;
/// Cap on roster rows — bounds the number of parked pulls (8 per name).
pub const MAX_ROSTER: i64 = 256;
/// Default time an unread message is kept before it expires.
const DEFAULT_MESSAGE_TTL: Duration = Duration::from_secs(60 * 60);
/// Default time an agent stays on the roster without a `register` or
/// `check_inbox`.
const DEFAULT_AGENT_TTL: Duration = Duration::from_secs(24 * 60 * 60);
/// Maximum UTF-8 byte length of an agent's roster description.
pub const MAX_DESCRIPTION_BYTES: usize = 256;

/// Options of a `register` call.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Registration {
    /// `None` keeps the stored description, `Some("")` clears it, and any
    /// other value replaces it. The bus trims it and rejects anything over
    /// [`MAX_DESCRIPTION_BYTES`] or holding control characters with
    /// `invalid_description`.
    pub description: Option<String>,
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
    /// The description now stored for this name, if any.
    pub description: Option<String>,
}

/// Roster entry (wire shape of `list_agents`).
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RosterEntry {
    /// Agent name.
    pub name: String,
    /// What the agent said it is or does at `register`, if anything.
    pub description: Option<String>,
    /// UTC RFC 3339 timestamp of the last register / send / check_inbox.
    pub last_seen: String,
    /// Unread messages addressed to this agent.
    pub queued: i64,
}

/// One roster row as the terminal view shows it: who is registered, what it
/// does, how recently it was seen, and how much undelivered mail it holds.
///
/// Built by [`crate::bus::Bus::overview`]. Read-only: producing it never touches
/// `last_seen` or message state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentOverview {
    /// Agent name.
    pub name: String,
    /// What the agent said it is or does at `register`, if anything.
    pub description: Option<String>,
    /// Whole seconds since the last `register` / `send` / `check_inbox`, never negative.
    pub seen_secs_ago: i64,
    /// Unread messages addressed to this agent.
    pub queued: i64,
}

/// The whole picture the terminal view needs in one read: the roster and how
/// many messages have been sent since the daemon started.
///
/// Built by [`crate::bus::Bus::overview`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Overview {
    /// Registered agents, sorted by name.
    pub agents: Vec<AgentOverview>,
    /// Messages accepted by `send` since this daemon process started. Read,
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
    /// Delivered messages. They are deleted as they are returned: each
    /// message is handed out once.
    pub messages: Vec<Message>,
    /// Unread messages still queued after this page; drain while `> 0`.
    pub remaining: i64,
}

/// How long the mailbox keeps things nobody collects. Read messages are
/// deleted at once; these bound what is left unread.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Retention {
    /// An unread message expires this long after it was sent.
    pub message_ttl: Duration,
    /// An agent drops off the roster (with its unread inbox) after this long
    /// without a `register`, `send_message` or `check_inbox`.
    pub agent_ttl: Duration,
}

impl Default for Retention {
    fn default() -> Self {
        Self {
            message_ttl: DEFAULT_MESSAGE_TTL,
            agent_ttl: DEFAULT_AGENT_TTL,
        }
    }
}

impl Retention {
    /// Build from the `MENTISCHAT_MESSAGE_TTL` / `MENTISCHAT_AGENT_TTL`
    /// values; `None` or empty keeps the default.
    ///
    /// # Errors
    ///
    /// A message naming the variable when a value does not parse (see
    /// [`parse_duration`]).
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    /// use mentischat::bus::Retention;
    ///
    /// let r = Retention::from_settings(Some("15m"), None).unwrap();
    /// assert_eq!(r.message_ttl, Duration::from_secs(900));
    /// assert!(Retention::from_settings(Some("soon"), None).is_err());
    /// ```
    pub fn from_settings(
        message_ttl: Option<&str>,
        agent_ttl: Option<&str>,
    ) -> Result<Self, String> {
        let pick = |var: &str, raw: Option<&str>, default: Duration| match raw {
            None | Some("") => Ok(default),
            Some(v) => parse_duration(v).map_err(|e| format!("{var}={v:?}: {e}")),
        };
        Ok(Self {
            message_ttl: pick("MENTISCHAT_MESSAGE_TTL", message_ttl, DEFAULT_MESSAGE_TTL)?,
            agent_ttl: pick("MENTISCHAT_AGENT_TTL", agent_ttl, DEFAULT_AGENT_TTL)?,
        })
    }

    /// How often the daemon sweeps: a quarter of the shorter TTL, between 1 s
    /// and 60 s, so nothing outlives its TTL by much.
    pub fn sweep_interval(&self) -> Duration {
        (self.message_ttl.min(self.agent_ttl) / 4)
            .clamp(Duration::from_secs(1), Duration::from_secs(60))
    }
}

/// Parse a positive duration: whole seconds (`90`) or a number with one unit
/// suffix, `s`, `m`, `h` or `d` (`30m`, `1h`, `2d`).
///
/// # Errors
///
/// Anything else, zero, or a value too large to represent.
pub fn parse_duration(raw: &str) -> Result<Duration, String> {
    let raw = raw.trim();
    let (digits, unit) = match raw.char_indices().last() {
        Some((i, c)) if c.is_ascii_alphabetic() => (&raw[..i], c),
        _ => (raw, 's'),
    };
    let scale: u64 = match unit {
        's' => 1,
        'm' => 60,
        'h' => 60 * 60,
        'd' => 24 * 60 * 60,
        _ => return Err("unit must be s, m, h or d".into()),
    };
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return Err("expected a whole number with an optional s/m/h/d suffix".into());
    }
    let n: u64 = digits.parse().map_err(|_| "number too large".to_string())?;
    if n == 0 {
        return Err("must be greater than zero".into());
    }
    n.checked_mul(scale)
        .map(Duration::from_secs)
        .ok_or_else(|| "number too large".into())
}

/// What one expiry sweep removed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SweepOutcome {
    /// Unread messages deleted for being older than the message TTL.
    pub expired: i64,
    /// Unread messages deleted with an agent that dropped off the roster.
    pub discarded: i64,
    /// Agents removed for being idle longer than the agent TTL.
    pub dropped_agents: Vec<String>,
}

impl SweepOutcome {
    /// True when the sweep removed nothing.
    pub fn is_empty(&self) -> bool {
        self.expired == 0 && self.discarded == 0 && self.dropped_agents.is_empty()
    }
}
