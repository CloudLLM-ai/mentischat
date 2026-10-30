//! AgentName, RoomName and Provider against a hand-written reference
//! grammar: accepted iff the reference accepts, round-trip, folding is
//! idempotent, length is bounded, and rejections are the typed error with a
//! bounded echo.
#![no_main]

use libfuzzer_sys::fuzz_target;
use mentischat::errors::MailboxError;
use mentischat::names::{AgentName, Provider, RoomName};

/// The documented grammar: 1-63 chars, `[a-zA-Z0-9]` then `[a-zA-Z0-9._-]`.
fn reference_accepts(s: &str) -> bool {
    let b = s.as_bytes();
    (1..=63).contains(&b.len())
        && b[0].is_ascii_alphanumeric()
        && b.iter()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'.' | b'_' | b'-'))
}

/// A rejection echoes at most 64 characters plus an ellipsis.
fn echo_is_bounded(e: &MailboxError) -> bool {
    match e {
        MailboxError::InvalidName { agent: s } | MailboxError::InvalidRoom { room: s } => {
            s.chars().count() <= 65
        }
        _ => false,
    }
}

fuzz_target!(|data: &[u8]| {
    let Ok(s) = std::str::from_utf8(data) else {
        return;
    };
    let want = reference_accepts(s);

    match AgentName::parse(s) {
        Ok(n) => {
            assert!(want, "accepted {s:?}");
            assert_eq!(n.as_str(), s);
            assert_eq!(AgentName::parse(n.as_str()).unwrap(), n);
            assert_eq!(n.folded(), s.to_ascii_lowercase());
            assert_eq!(n.folded().to_ascii_lowercase(), n.folded());
            assert!(AgentName::parse(&n.folded()).is_ok());
            assert!(n.as_str().len() <= 63);
        }
        Err(e) => {
            assert!(!want, "rejected {s:?}");
            assert!(matches!(e, MailboxError::InvalidName { .. }));
            assert!(echo_is_bounded(&e));
        }
    }

    match RoomName::parse(s) {
        Ok(n) => {
            assert!(want, "accepted room {s:?}");
            assert_eq!(n.to_string(), s);
            assert_eq!(n.folded(), s.to_ascii_lowercase());
            assert_eq!(RoomName::parse(&n.folded()).unwrap().folded(), n.folded());
        }
        Err(e) => {
            assert!(!want, "rejected room {s:?}");
            assert!(matches!(e, MailboxError::InvalidRoom { .. }));
            assert!(echo_is_bounded(&e));
        }
    }

    // A provider is lowercased first, so the grammar applies to the lowercase.
    match Provider::parse(s) {
        Ok(p) => {
            assert!(want);
            assert_eq!(p.as_str(), s.to_ascii_lowercase());
            assert_eq!(Provider::parse(p.as_str()).unwrap(), p);
        }
        Err(e) => {
            assert!(!want);
            assert!(matches!(e, MailboxError::InvalidName { .. }));
            assert!(echo_is_bounded(&e));
        }
    }
});
