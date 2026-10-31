//! The client component of signed, end-to-end encrypted rooms: the process
//! that holds an agent's keys.
//!
//! The daemon is only the MLS Delivery Service and never holds a private key
//! or sees plaintext. This module is where keys live and where signing,
//! encrypting and decrypting happen, for one agent identity:
//!
//! - [`identity`]: the Ed25519 key file (`0600`) and the on-disk state.
//! - [`http`]: a minimal loopback HTTP client for the daemon's REST routes.
//! - [`session`]: [`Client`], the group operations (create, add, join, remove,
//!   send, sync) on top of OpenMLS.
//! - [`proxy`]: a stdio MCP proxy exposing the mailbox tools to a harness and
//!   handling secure rooms transparently, so the model never touches a key.
//!
//! See `docs/SIGNED_ROOMS.md` for the protocol and threat model.

use std::fmt;

pub mod cli;
pub mod http;
pub mod identity;
pub mod proxy;
pub mod session;

pub use http::Daemon;
pub use identity::{default_client_dir, Identity};
pub use session::{Client, Received, UpdatePolicy};

/// Why a client operation failed.
#[derive(Debug)]
pub enum ClientError {
    /// The daemon could not be reached or answered something that is not HTTP.
    Transport(String),
    /// The daemon answered with a contract error.
    Daemon {
        /// HTTP status.
        status: u16,
        /// The `error` code of the body, `"unknown"` when absent.
        code: String,
        /// The whole error body.
        body: serde_json::Value,
    },
    /// The client missed a Commit that fell out of the daemon's retention, so
    /// its group state cannot catch up: it must be added to the room again
    /// (see [`Client::join_room`]; an up-to-date member runs
    /// [`Client::readd_member`]).
    Behind {
        /// The room this client fell behind in.
        room: String,
    },
    /// The daemon's view of the room is older than what this client has already
    /// seen: its epoch is below the group's, or it has accepted fewer of this
    /// client's operations than the client got accepted. The daemon was restored
    /// from an older snapshot or its journal was truncated. The client stops
    /// using the room until [`Client::accept_rollback`].
    RolledBack {
        /// The room.
        room: String,
        /// What was wrong.
        detail: String,
    },
    /// An MLS operation failed (bad message, wrong epoch, invalid KeyPackage).
    Mls(String),
    /// Key or state files could not be read, written or parsed.
    Storage(String),
    /// A precondition of the operation does not hold (not a secure room, not
    /// the owner, a peer whose identity does not match).
    Usage(String),
}

impl ClientError {
    /// The daemon's error code, if this is a daemon error.
    pub fn daemon_code(&self) -> Option<&str> {
        match self {
            Self::Daemon { code, .. } => Some(code),
            _ => None,
        }
    }
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Transport(m) => write!(f, "daemon unreachable: {m}"),
            Self::Daemon { status, code, .. } => write!(f, "daemon error {status}: {code}"),
            Self::Behind { room } => write!(
                f,
                "this client missed commits in {room} and must be added to it again"
            ),
            Self::RolledBack { room, detail } => {
                write!(
                    f,
                    "secure room {room} was rolled back by the daemon: {detail}"
                )
            }
            Self::Mls(m) => write!(f, "mls: {m}"),
            Self::Storage(m) => write!(f, "storage: {m}"),
            Self::Usage(m) => f.write_str(m),
        }
    }
}

impl std::error::Error for ClientError {}
