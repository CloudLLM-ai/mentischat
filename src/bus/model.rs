//! Caps, retention and the public value types of the mailbox: wire shapes returned by the
//! tools and REST routes, and the read model behind the terminal view.

use std::time::Duration;

use serde::Serialize;

/// Maximum UTF-8 byte length of a message body.
pub const MAX_BODY_BYTES: usize = 64 * 1024;
/// Per-recipient cap on unread messages.
pub const MAX_PENDING_PER_AGENT: i64 = 1024;
/// Global cap on messages, read and unread. A send at the cap deletes the
/// oldest read message to make room; when every row is unread it
/// fails `storage_full`.
pub const MAX_TOTAL_MESSAGES: i64 = 8192;
/// Max messages returned by one `check_inbox` or `history` page.
pub const MAX_BATCH: i64 = 8;
/// Max summed serialized-JSON bytes of the messages returned by one
/// `check_inbox` or `history` page (first message always included).
pub const MAX_RETURN_BYTES: usize = 16 * 1024;
/// Upper bound of `wait_ms`.
pub const MAX_WAIT_MS: u32 = 30_000;
/// Max parked pulls per name; an additional park fails.
pub const MAX_WAITERS_PER_AGENT: usize = 8;
/// Cap on roster rows — bounds the number of parked pulls (8 per name).
pub const MAX_ROSTER: i64 = 256;
/// Default number of messages a room keeps. When a room is full a new message
/// pushes out the oldest; a member that had not read it is told how many it
/// missed (`skipped`).
pub const DEFAULT_ROOM_CAPACITY: usize = 1024;
/// Cap on rooms: bounds the memory rooms can hold, as [`MAX_ROSTER`] bounds
/// the roster.
pub const MAX_ROOMS: usize = 256;
/// Default time a read message is kept after it was read.
const DEFAULT_READ_TTL: Duration = Duration::from_secs(24 * 60 * 60);
/// Default time an agent stays on the roster without a `register`,
/// `send_message`, `check_inbox` or `history`.
const DEFAULT_AGENT_TTL: Duration = Duration::from_secs(24 * 60 * 60);
/// Default time a released name is held for its previous holder.
const DEFAULT_NAME_HOLD: Duration = Duration::from_secs(10 * 60);
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
    /// The harness or app the agent runs in. `None` keeps the stored one.
    pub provider: Option<crate::names::Provider>,
    /// Take the name even though it is registered or held: the caller says it
    /// is the agent that held it. Without this a taken name fails
    /// `name_taken`.
    pub reclaim: bool,
}

/// A delivered or queued mail message (wire shape).
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Message {
    /// Unique id: the decimal delivery number, increasing across the life of
    /// the daemon (and across a persisted restart). `history`'s `next` cursor
    /// is the same number.
    pub id: String,
    /// Sender roster name.
    pub from: String,
    /// Recipient roster name. Empty (and left out of the JSON) for a room
    /// message, which carries `room` instead.
    #[serde(skip_serializing_if = "String::is_empty")]
    pub to: String,
    /// The room a room message was sent to; absent from direct mail.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub room: Option<String>,
    /// Body exactly as stored (never trimmed).
    pub body: String,
    /// UTC RFC 3339 creation timestamp.
    pub created_at: String,
    /// UTC RFC 3339 time the recipient read it; `None` while unread.
    pub read_at: Option<String>,
}

/// Send receipt handed back to the sender.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SendReceipt {
    /// Id of the queued message.
    pub id: String,
    /// Echoed sender name.
    pub from: String,
    /// Echoed recipient name; empty (and left out) for a room message.
    #[serde(skip_serializing_if = "String::is_empty")]
    pub to: String,
    /// The room it went to, for a room message.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub room: Option<String>,
    /// UTC RFC 3339 creation timestamp.
    pub created_at: String,
}

/// Register outcome.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RegisterOutcome {
    /// The registered name, spelled as the roster stores it.
    pub agent: String,
    /// True only when the name was not already on the roster.
    pub created: bool,
    /// Fresh last-seen timestamp.
    pub last_seen: String,
    /// The harness or app now stored for this name, if any.
    pub provider: Option<String>,
    /// The description now stored for this name, if any.
    pub description: Option<String>,
}

/// Roster entry.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RosterEntry {
    /// Agent name.
    pub name: String,
    /// The harness or app the agent runs in, if it said.
    pub provider: Option<String>,
    /// What the agent said it is or does at `register`, if anything.
    pub description: Option<String>,
    /// UTC RFC 3339 timestamp of the last register / send / check_inbox /
    /// history.
    pub last_seen: String,
    /// Unread messages addressed to this agent.
    pub queued: i64,
}

/// Wire shape of `list_agents`: the roster and some names free to take.
#[derive(Debug, Clone, Serialize)]
pub struct RosterListing {
    /// Registered agents, sorted by name.
    pub agents: Vec<RosterEntry>,
    /// Up to [`crate::names::SUGGESTION_COUNT`] names from
    /// [`crate::names::SUGGESTED_NAMES`] that are neither registered nor held.
    pub suggestions: Vec<String>,
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
    /// The harness or app the agent runs in, if it said.
    pub provider: Option<String>,
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
    /// Messages accepted by `send` since this daemon process started.
    /// Expired, discarded and wiped mail still counts; the number never goes
    /// down and restarts at 0 with the process.
    pub messages_sent: u64,
}

/// Deregister outcome.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DiscardOutcome {
    /// The removed name.
    pub agent: String,
    /// Messages addressed to it, read or unread, deleted with it.
    pub discarded: i64,
}

/// One `check_inbox` result page.
#[derive(Debug, Serialize)]
pub struct InboxPage {
    /// Pulled name.
    pub agent: String,
    /// Delivered messages, now marked read. Each message is handed out by
    /// `check_inbox` once; `history` shows it again.
    pub messages: Vec<Message>,
    /// Unread messages still queued after this page, direct and room mail
    /// together; drain while `> 0`.
    pub remaining: i64,
    /// Room messages this agent never saw because its rooms were full and
    /// pushed them out first, since its last pull. Absent when none.
    #[serde(skip_serializing_if = "is_zero")]
    pub skipped: u64,
}

fn is_zero(n: &u64) -> bool {
    *n == 0
}

/// One `history` result page: the newest messages older than the cursor,
/// oldest first.
#[derive(Debug, Serialize)]
pub struct HistoryPage {
    /// The agent whose history this is.
    pub agent: String,
    /// Messages it sent (read or not) and messages it has read, oldest first;
    /// for a room, the room's retained messages.
    pub messages: Vec<Message>,
    /// Pass as `before` to get the page of older messages; `None` when there
    /// are none.
    pub next: Option<i64>,
}

/// Wire shape of a room: what `create_room`, `join_room` and `invite` return.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RoomInfo {
    /// The room's name, spelled as it was created.
    pub room: String,
    /// What the room is for, if its creator said.
    pub description: Option<String>,
    /// Invite-only and left out of other agents' `list_rooms`. This is
    /// etiquette between cooperating local agents, not a security boundary:
    /// any local process can claim any agent name.
    pub private: bool,
    /// Members, sorted by name.
    pub members: Vec<String>,
}

/// One row of `list_rooms`.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RoomEntry {
    /// The room's name.
    pub room: String,
    /// What the room is for, if its creator said.
    pub description: Option<String>,
    /// Invite-only. A private room only appears to its members and invitees.
    pub private: bool,
    /// How many agents are in it.
    pub members: usize,
    /// True when the asking agent is a member.
    pub joined: bool,
    /// Unread room messages for the asking agent; 0 unless `joined`.
    pub unread: usize,
}

/// Wire shape of `list_rooms`: public rooms plus the private ones the agent is
/// in or was invited to, sorted by name.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct RoomListing {
    /// The rooms the asking agent can see.
    pub rooms: Vec<RoomEntry>,
}

/// Wire shape of `leave_room`.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct LeaveOutcome {
    /// The room that was left.
    pub room: String,
    /// True when the agent was its last member and the room is gone.
    pub deleted: bool,
}

/// How long the mailbox keeps things, and how long a released name is held.
///
/// Unread mail has no lifetime of its own: it waits until its recipient
/// reads it or leaves the roster, or the daemon restarts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Retention {
    /// A read message is deleted this long after it was read.
    pub read_ttl: Duration,
    /// An agent drops off the roster (with the mail addressed to it) after
    /// this long without a `register`, `send_message`, `check_inbox` or
    /// `history`.
    pub agent_ttl: Duration,
    /// A name that leaves the roster is held this long: only `reclaim` takes
    /// it, so a new agent cannot pick up mail meant for the old one.
    pub name_hold: Duration,
}

impl Default for Retention {
    fn default() -> Self {
        Self {
            read_ttl: DEFAULT_READ_TTL,
            agent_ttl: DEFAULT_AGENT_TTL,
            name_hold: DEFAULT_NAME_HOLD,
        }
    }
}

impl Retention {
    /// Build from `MENTISCHAT_READ_TTL`, `MENTISCHAT_AGENT_TTL` and
    /// `MENTISCHAT_NAME_HOLD` as `setting` returns them (usually [`std::env::var`]); `None` or empty keeps the default.
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
    /// let r = Retention::from_settings(|var| {
    ///     (var == "MENTISCHAT_READ_TTL").then(|| "15m".to_string())
    /// })
    /// .unwrap();
    /// assert_eq!(r.read_ttl, Duration::from_secs(900));
    /// assert_eq!(r.agent_ttl, Duration::from_secs(86_400));
    /// assert!(Retention::from_settings(|_| Some("soon".into())).is_err());
    /// ```
    pub fn from_settings(setting: impl Fn(&str) -> Option<String>) -> Result<Self, String> {
        let pick = |var: &str, default: Duration| match setting(var).as_deref() {
            None | Some("") => Ok(default),
            Some(v) => parse_duration(v).map_err(|e| format!("{var}={v:?}: {e}")),
        };
        Ok(Self {
            read_ttl: pick("MENTISCHAT_READ_TTL", DEFAULT_READ_TTL)?,
            agent_ttl: pick("MENTISCHAT_AGENT_TTL", DEFAULT_AGENT_TTL)?,
            name_hold: pick("MENTISCHAT_NAME_HOLD", DEFAULT_NAME_HOLD)?,
        })
    }

    /// How often the daemon sweeps: a quarter of the shorter TTL, between
    /// 1 s and 60 s, so nothing outlives its TTL by much. Holds are checked
    /// when a name is registered, so they do not set the pace.
    pub fn sweep_interval(&self) -> Duration {
        (self.read_ttl.min(self.agent_ttl) / 4)
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

/// Mailbox counts for `/health`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Stats {
    /// Agents on the roster.
    pub agents: i64,
    /// Messages not yet read.
    pub unread: i64,
    /// Read messages still kept for `history`.
    pub read: i64,
}

/// What one expiry sweep removed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SweepOutcome {
    /// Read messages deleted for having been read longer than the read TTL ago.
    pub expired_read: i64,
    /// Messages addressed to an agent that dropped off the roster, deleted
    /// with it.
    pub discarded: i64,
    /// Agents removed for being idle longer than the agent TTL.
    pub dropped_agents: Vec<String>,
}

impl SweepOutcome {
    /// True when the sweep removed nothing.
    pub fn is_empty(&self) -> bool {
        self.expired_read == 0 && self.discarded == 0 && self.dropped_agents.is_empty()
    }
}
