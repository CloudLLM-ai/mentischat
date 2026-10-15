//! Caps and the public value types of the mailbox: wire shapes returned by the
//! tools and REST routes, and the read model behind the terminal view.

use serde::Serialize;

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
/// Built by [`crate::bus::Bus::overview`]. Read-only: producing it never touches
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
/// Built by [`crate::bus::Bus::overview`].
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
