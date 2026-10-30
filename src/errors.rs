//! Typed mailbox errors and their HTTP / MCP wire mappings.
//!
//! Every variant serialises to the contract error body:
//! `{"error": code, ...context}`. `known` (the sorted roster) appears on
//! [`MailboxError::Unregistered`] only.

use std::fmt;

use serde_json::{json, Value};

/// Why a name could not be registered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TakenReason {
    /// Another agent has it on the roster now.
    Registered,
    /// It left the roster recently and is held for its previous holder until
    /// the given UTC RFC 3339 time.
    Held {
        /// When anyone may take it.
        until: String,
    },
}

/// Domain failures of the mailbox. Never logged with backtraces; mapped once
/// in REST (`rest.rs`) and once in MCP (`tools.rs`).
#[derive(Debug, Clone, PartialEq)]
pub enum MailboxError {
    /// Name fails the roster regex.
    InvalidName { agent: String },
    /// Missing / non-string / whitespace-only / oversized body, a body that
    /// is not JSON, a name field that is not a string, or a `reclaim` that is
    /// not a boolean.
    InvalidBody,
    /// `wait_ms` present but not an integer in `0..=30000`.
    InvalidWait,
    /// The name is registered or held; `reclaim` takes it anyway. Carries
    /// free names to pick instead.
    NameTaken {
        agent: String,
        reason: TakenReason,
        suggestions: Vec<String>,
    },
    /// `before` is not a positive integer.
    InvalidCursor,
    /// `description` is not a string, is over
    /// [`crate::bus::MAX_DESCRIPTION_BYTES`], or holds control characters.
    InvalidDescription,
    /// The named agent is not on the roster. Carries the current roster so
    /// callers can self-correct.
    Unregistered { agent: String, known: Vec<String> },
    /// That recipient already holds `MAX_PENDING_PER_AGENT` unread messages.
    QueueFull { to: String, pending: i64 },
    /// Global row cap hit.
    StorageFull { pending: i64 },
    /// A storage failure unrelated to capacity (not a full queue). The
    /// in-memory engine cannot raise it; the code stays in the wire contract.
    StorageError,
    /// Roster already holds [`crate::bus::MAX_ROSTER`] agents.
    RosterFull { agents: i64 },
    /// `MAX_WAITERS_PER_AGENT` pulls are already parked for that name.
    TooManyWaiters { agent: String, waiting: usize },
    /// Room name fails the same grammar as an agent name.
    InvalidRoom { room: String },
    /// No room has that name.
    RoomNotFound { room: String },
    /// A room with that name exists already.
    RoomExists { room: String },
    /// The agent is not a member of the room it tried to use.
    NotAMember { agent: String, room: String },
    /// The room is private and the agent was not invited.
    PrivateRoom { room: String },
    /// The daemon already holds [`crate::bus::MAX_ROOMS`] rooms.
    RoomsFull { rooms: i64 },
    /// REST path that is not a known route.
    NotFound,
    /// Known REST path, HTTP method it does not serve.
    MethodNotAllowed,
    /// Waiter aborted because the process is closing.
    ShuttingDown,
}

impl MailboxError {
    /// Stable machine-readable error code used as `"error"` on the wire.
    pub fn code(&self) -> &'static str {
        match self {
            Self::InvalidName { .. } => "invalid_name",
            Self::InvalidBody => "invalid_body",
            Self::InvalidWait => "invalid_wait",
            Self::InvalidDescription => "invalid_description",
            Self::InvalidCursor => "invalid_cursor",
            Self::NameTaken { .. } => "name_taken",
            Self::Unregistered { .. } => "unregistered",
            Self::QueueFull { .. } => "queue_full",
            Self::StorageFull { .. } => "storage_full",
            Self::StorageError => "storage_error",
            Self::RosterFull { .. } => "roster_full",
            Self::TooManyWaiters { .. } => "too_many_waiters",
            Self::InvalidRoom { .. } => "invalid_room",
            Self::RoomNotFound { .. } => "room_not_found",
            Self::RoomExists { .. } => "room_exists",
            Self::NotAMember { .. } => "not_a_member",
            Self::PrivateRoom { .. } => "private_room",
            Self::RoomsFull { .. } => "rooms_full",
            Self::NotFound => "not_found",
            Self::MethodNotAllowed => "method_not_allowed",
            Self::ShuttingDown => "shutting_down",
        }
    }

    /// HTTP status for the REST surface.
    pub fn http_status(&self) -> axum::http::StatusCode {
        use axum::http::StatusCode;
        match self {
            Self::InvalidName { .. }
            | Self::InvalidRoom { .. }
            | Self::InvalidBody
            | Self::InvalidWait
            | Self::InvalidDescription
            | Self::InvalidCursor => StatusCode::BAD_REQUEST,
            Self::Unregistered { .. } | Self::RoomNotFound { .. } | Self::NotFound => {
                StatusCode::NOT_FOUND
            }
            Self::NotAMember { .. } | Self::PrivateRoom { .. } => StatusCode::FORBIDDEN,
            Self::MethodNotAllowed => StatusCode::METHOD_NOT_ALLOWED,
            Self::QueueFull { .. }
            | Self::TooManyWaiters { .. }
            | Self::RosterFull { .. }
            | Self::RoomExists { .. }
            | Self::RoomsFull { .. }
            | Self::NameTaken { .. } => StatusCode::CONFLICT,
            Self::StorageFull { .. } => StatusCode::INSUFFICIENT_STORAGE,
            Self::StorageError => StatusCode::INTERNAL_SERVER_ERROR,
            Self::ShuttingDown => StatusCode::SERVICE_UNAVAILABLE,
        }
    }

    /// Serialise to the contract error body object.
    pub fn to_json(&self) -> Value {
        match self {
            Self::InvalidName { agent } => json!({ "error": self.code(), "agent": agent }),
            Self::InvalidBody
            | Self::InvalidWait
            | Self::InvalidDescription
            | Self::InvalidCursor => json!({ "error": self.code() }),
            Self::NameTaken {
                agent,
                reason,
                suggestions,
            } => {
                let mut body = json!({
                    "error": self.code(),
                    "agent": agent,
                    "suggestions": suggestions,
                });
                match reason {
                    TakenReason::Registered => body["reason"] = json!("registered"),
                    TakenReason::Held { until } => {
                        body["reason"] = json!("held");
                        body["heldUntil"] = json!(until);
                    }
                }
                body
            }
            Self::Unregistered { agent, known } => {
                json!({ "error": self.code(), "agent": agent, "known": known })
            }
            Self::QueueFull { to, pending } => {
                json!({ "error": self.code(), "to": to, "pending": pending })
            }
            Self::StorageFull { pending } => json!({ "error": self.code(), "pending": pending }),
            Self::StorageError => json!({ "error": self.code() }),
            Self::RosterFull { agents } => json!({ "error": self.code(), "agents": agents }),
            Self::TooManyWaiters { agent, waiting } => {
                json!({ "error": self.code(), "agent": agent, "waiting": waiting })
            }
            Self::InvalidRoom { room }
            | Self::RoomNotFound { room }
            | Self::RoomExists { room }
            | Self::PrivateRoom { room } => json!({ "error": self.code(), "room": room }),
            Self::NotAMember { agent, room } => {
                json!({ "error": self.code(), "agent": agent, "room": room })
            }
            Self::RoomsFull { rooms } => json!({ "error": self.code(), "rooms": rooms }),
            Self::NotFound | Self::MethodNotAllowed => json!({ "error": self.code() }),
            Self::ShuttingDown => json!({ "error": self.code() }),
        }
    }

    /// Compact JSON string form, ready to embed in an MCP
    /// `isError: true` content block.
    pub fn to_json_string(&self) -> String {
        self.to_json().to_string()
    }
}

impl fmt::Display for MailboxError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.code())
    }
}

impl std::error::Error for MailboxError {}
