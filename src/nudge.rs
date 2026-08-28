//! Shared nudge constants for wake transports.
//!
//! The in-daemon AppleScript doorbell (v1) was removed. `mentis-wake`
//! long-polls the daemon and injects this pointer into the harness PTY;
//! `nudgeTransport: terminal` survives only as a deprecated alias for
//! `none`.

use crate::errors::MailboxError;

/// Environment variable set by `mentis-wake` for auto-bind on register.
pub const WAKE_ENV: &str = "MENTISCHAT_WAKE_ID";

/// Injected prompt. The only string a doorbell may type or write.
pub const POINTER_TEXT: &str = "[mentischat] Mail waiting. Call check_inbox now.";

/// Accept a UUID v4 session id from `mentis-wake`.
pub fn validate_wake_id(raw: &str) -> Result<String, MailboxError> {
    let t = raw.trim();
    if uuid::Uuid::try_parse(t).is_ok() {
        Ok(t.to_string())
    } else {
        Err(MailboxError::InvalidBody)
    }
}
