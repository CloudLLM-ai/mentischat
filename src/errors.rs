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
    InvalidName {
        /// The name as given.
        agent: String,
    },
    /// Missing / non-string / whitespace-only / oversized body, a body that
    /// is not JSON, a name field that is not a string, or a `reclaim` that is
    /// not a boolean.
    InvalidBody,
    /// `wait_ms` present but not an integer in `0..=30000`.
    InvalidWait,
    /// The name is registered or held; `reclaim` takes it anyway. Carries
    /// free names to pick instead.
    NameTaken {
        /// The name that was asked for.
        agent: String,
        /// Whether it is registered now or held for a previous holder.
        reason: TakenReason,
        /// Free names to pick instead.
        suggestions: Vec<String>,
    },
    /// `before` is not a positive integer.
    InvalidCursor,
    /// `limit` is not an integer of at least 1 (and is not a digit string
    /// holding one).
    InvalidLimit,
    /// `description` is not a string, is over
    /// [`crate::bus::MAX_DESCRIPTION_BYTES`], or holds control characters.
    InvalidDescription,
    /// The named agent is not on the roster. Carries the current roster so
    /// callers can self-correct.
    Unregistered {
        /// The name that is not on the roster.
        agent: String,
        /// The roster now, sorted.
        known: Vec<String>,
    },
    /// That recipient already holds `MAX_PENDING_PER_AGENT` unread messages.
    QueueFull {
        /// The recipient whose queue is full.
        to: String,
        /// How many unread messages it holds.
        pending: i64,
    },
    /// Global row cap hit.
    StorageFull {
        /// How many unread messages the bus holds.
        pending: i64,
    },
    /// Roster already holds [`crate::bus::MAX_ROSTER`] agents.
    RosterFull {
        /// How many agents the roster holds.
        agents: i64,
    },
    /// `MAX_WAITERS_PER_AGENT` pulls are already parked for that name.
    TooManyWaiters {
        /// The agent whose pulls are parked.
        agent: String,
        /// How many are parked.
        waiting: usize,
    },
    /// Room name fails the same grammar as an agent name.
    InvalidRoom {
        /// The room name as given.
        room: String,
    },
    /// No room has that name.
    RoomNotFound {
        /// The room name asked for.
        room: String,
    },
    /// A room with that name exists already.
    RoomExists {
        /// The room name that is taken.
        room: String,
    },
    /// The agent is not a member of the room it tried to use.
    NotAMember {
        /// The agent that is not in the room.
        agent: String,
        /// The room it tried to use.
        room: String,
    },
    /// The daemon already holds [`crate::bus::MAX_ROOMS`] rooms.
    RoomsFull {
        /// How many rooms the daemon holds.
        rooms: i64,
    },
    /// The agent is in [`crate::bus::MAX_ROOMS_PER_AGENT`] rooms already.
    TooManyRooms {
        /// The agent that cannot join another room.
        agent: String,
        /// How many rooms it is in.
        rooms: i64,
    },
    /// The room holds [`crate::bus::MAX_ROOM_MEMBERS`] members already.
    RoomFull {
        /// The room that is full.
        room: String,
        /// How many members it holds.
        members: i64,
    },
    /// The room is signed: a plain `send_message`, `join_room`, `invite`,
    /// `leave_room` is refused. Use a signed operation.
    SignatureRequired {
        /// The signed room.
        room: String,
    },
    /// A signed operation whose signature does not verify under the key the
    /// signer is bound to, or a field of it that does not parse.
    InvalidSignature,
    /// A public key that is not 64 hex digits of a valid Ed25519 point.
    InvalidKey,
    /// The agent has no public key registered, so it cannot act in or be
    /// invited to a signed room.
    KeyRequired {
        /// The agent without a key.
        agent: String,
    },
    /// The name is already bound to a different public key.
    KeyConflict {
        /// The agent whose key differs.
        agent: String,
    },
    /// A signed operation's counter is not above the highest this key has
    /// used in the room (a replay, or a client that lost its place).
    StaleCounter {
        /// The signed room.
        room: String,
        /// The highest counter this key has used there; sign a higher one.
        last: u64,
    },
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
            Self::InvalidLimit => "invalid_limit",
            Self::NameTaken { .. } => "name_taken",
            Self::Unregistered { .. } => "unregistered",
            Self::QueueFull { .. } => "queue_full",
            Self::StorageFull { .. } => "storage_full",
            Self::RosterFull { .. } => "roster_full",
            Self::TooManyWaiters { .. } => "too_many_waiters",
            Self::InvalidRoom { .. } => "invalid_room",
            Self::RoomNotFound { .. } => "room_not_found",
            Self::RoomExists { .. } => "room_exists",
            Self::NotAMember { .. } => "not_a_member",
            Self::RoomsFull { .. } => "rooms_full",
            Self::TooManyRooms { .. } => "too_many_rooms",
            Self::RoomFull { .. } => "room_full",
            Self::SignatureRequired { .. } => "signature_required",
            Self::InvalidSignature => "invalid_signature",
            Self::InvalidKey => "invalid_key",
            Self::KeyRequired { .. } => "key_required",
            Self::KeyConflict { .. } => "key_conflict",
            Self::StaleCounter { .. } => "stale_counter",
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
            | Self::InvalidCursor
            | Self::InvalidKey
            | Self::InvalidLimit => StatusCode::BAD_REQUEST,
            Self::SignatureRequired { .. } | Self::InvalidSignature => StatusCode::UNAUTHORIZED,
            Self::Unregistered { .. } | Self::RoomNotFound { .. } | Self::NotFound => {
                StatusCode::NOT_FOUND
            }
            Self::NotAMember { .. } => StatusCode::FORBIDDEN,
            Self::MethodNotAllowed => StatusCode::METHOD_NOT_ALLOWED,
            Self::QueueFull { .. }
            | Self::TooManyWaiters { .. }
            | Self::RosterFull { .. }
            | Self::RoomExists { .. }
            | Self::RoomsFull { .. }
            | Self::TooManyRooms { .. }
            | Self::RoomFull { .. }
            | Self::KeyRequired { .. }
            | Self::KeyConflict { .. }
            | Self::StaleCounter { .. }
            | Self::NameTaken { .. } => StatusCode::CONFLICT,
            Self::StorageFull { .. } => StatusCode::INSUFFICIENT_STORAGE,
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
            | Self::InvalidCursor
            | Self::InvalidSignature
            | Self::InvalidKey
            | Self::InvalidLimit => json!({ "error": self.code() }),
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
            Self::RosterFull { agents } => json!({ "error": self.code(), "agents": agents }),
            Self::TooManyWaiters { agent, waiting } => {
                json!({ "error": self.code(), "agent": agent, "waiting": waiting })
            }
            Self::KeyRequired { agent } | Self::KeyConflict { agent } => {
                json!({ "error": self.code(), "agent": agent })
            }
            Self::StaleCounter { room, last } => {
                json!({ "error": self.code(), "room": room, "last": last })
            }
            Self::InvalidRoom { room }
            | Self::SignatureRequired { room }
            | Self::RoomNotFound { room }
            | Self::RoomExists { room } => json!({ "error": self.code(), "room": room }),
            Self::NotAMember { agent, room } => {
                json!({ "error": self.code(), "agent": agent, "room": room })
            }
            Self::RoomsFull { rooms } => json!({ "error": self.code(), "rooms": rooms }),
            Self::TooManyRooms { agent, rooms } => {
                json!({ "error": self.code(), "agent": agent, "rooms": rooms })
            }
            Self::RoomFull { room, members } => {
                json!({ "error": self.code(), "room": room, "members": members })
            }
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
