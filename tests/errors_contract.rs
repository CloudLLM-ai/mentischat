//! The error contract: every [`MailboxError`] has one stable code, one HTTP
//! status and one JSON body shape. Agents branch on these, so a change here is
//! a protocol change; the table below is the specification.

use axum::http::StatusCode;
use mentischat::errors::{MailboxError, TakenReason};
use serde_json::{json, Value};

fn s(text: &str) -> String {
    text.to_string()
}

/// Every variant with the code, status and context keys the contract gives it.
fn table() -> Vec<(MailboxError, &'static str, StatusCode, Vec<&'static str>)> {
    use MailboxError as E;
    use StatusCode as S;
    vec![
        (
            E::InvalidName { agent: s("x") },
            "invalid_name",
            S::BAD_REQUEST,
            vec!["agent"],
        ),
        (E::InvalidBody, "invalid_body", S::BAD_REQUEST, vec![]),
        (E::InvalidWait, "invalid_wait", S::BAD_REQUEST, vec![]),
        (
            E::InvalidDescription,
            "invalid_description",
            S::BAD_REQUEST,
            vec![],
        ),
        (E::InvalidCursor, "invalid_cursor", S::BAD_REQUEST, vec![]),
        (E::InvalidLimit, "invalid_limit", S::BAD_REQUEST, vec![]),
        (
            E::NameTaken {
                agent: s("Vega"),
                reason: TakenReason::Registered,
                suggestions: vec![s("Vega2")],
            },
            "name_taken",
            S::CONFLICT,
            vec!["agent", "suggestions", "reason"],
        ),
        (
            E::NumberedName {
                agent: s("Vega2"),
                suggestions: vec![s("Vega")],
            },
            "numbered_name",
            S::BAD_REQUEST,
            vec!["agent", "suggestions"],
        ),
        (
            E::Unregistered {
                agent: s("Vega"),
                known: vec![s("Orion")],
            },
            "unregistered",
            S::NOT_FOUND,
            vec!["agent", "known"],
        ),
        (
            E::QueueFull {
                to: s("Vega"),
                pending: 3,
            },
            "queue_full",
            S::CONFLICT,
            vec!["to", "pending"],
        ),
        (
            E::StorageFull { pending: 3 },
            "storage_full",
            S::INSUFFICIENT_STORAGE,
            vec!["pending"],
        ),
        (
            E::RosterFull { agents: 3 },
            "roster_full",
            S::CONFLICT,
            vec!["agents"],
        ),
        (
            E::TooManyWaiters {
                agent: s("Vega"),
                waiting: 2,
            },
            "too_many_waiters",
            S::CONFLICT,
            vec!["agent", "waiting"],
        ),
        (
            E::InvalidRoom { room: s("r") },
            "invalid_room",
            S::BAD_REQUEST,
            vec!["room"],
        ),
        (
            E::RoomNotFound { room: s("r") },
            "room_not_found",
            S::NOT_FOUND,
            vec!["room"],
        ),
        (
            E::RoomExists { room: s("r") },
            "room_exists",
            S::CONFLICT,
            vec!["room"],
        ),
        (
            E::NotAMember {
                agent: s("Vega"),
                room: s("r"),
            },
            "not_a_member",
            S::FORBIDDEN,
            vec!["agent", "room"],
        ),
        (
            E::RoomsFull { rooms: 9 },
            "rooms_full",
            S::CONFLICT,
            vec!["rooms"],
        ),
        (
            E::TooManyRooms {
                agent: s("Vega"),
                rooms: 9,
            },
            "too_many_rooms",
            S::CONFLICT,
            vec!["agent", "rooms"],
        ),
        (
            E::RoomFull {
                room: s("r"),
                members: 9,
            },
            "room_full",
            S::CONFLICT,
            vec!["room", "members"],
        ),
        (
            E::SignatureRequired { room: s("r") },
            "signature_required",
            S::UNAUTHORIZED,
            vec!["room"],
        ),
        (
            E::ProofRequired { agent: s("Vega") },
            "proof_required",
            S::UNAUTHORIZED,
            vec!["agent"],
        ),
        (
            E::InvalidSignature,
            "invalid_signature",
            S::UNAUTHORIZED,
            vec![],
        ),
        (E::InvalidKey, "invalid_key", S::BAD_REQUEST, vec![]),
        (
            E::KeyRequired { agent: s("Vega") },
            "key_required",
            S::CONFLICT,
            vec!["agent"],
        ),
        (
            E::KeyConflict { agent: s("Vega") },
            "key_conflict",
            S::CONFLICT,
            vec!["agent"],
        ),
        (
            E::StaleCounter {
                room: s("r"),
                last: 4,
            },
            "stale_counter",
            S::CONFLICT,
            vec!["room", "last"],
        ),
        (
            E::StaleEpoch {
                room: s("r"),
                epoch: 4,
            },
            "stale_epoch",
            S::CONFLICT,
            vec!["room", "epoch"],
        ),
        (
            E::NoKeyPackage { agent: s("Vega") },
            "no_key_package",
            S::NOT_FOUND,
            vec!["agent"],
        ),
        (
            E::RateLimited {
                retry_after_ms: 250,
            },
            "rate_limited",
            S::TOO_MANY_REQUESTS,
            vec!["retryAfterMs"],
        ),
        (E::NotFound, "not_found", S::NOT_FOUND, vec![]),
        (
            E::MethodNotAllowed,
            "method_not_allowed",
            S::METHOD_NOT_ALLOWED,
            vec![],
        ),
        (
            E::ShuttingDown,
            "shutting_down",
            S::SERVICE_UNAVAILABLE,
            vec![],
        ),
    ]
}

/// Each variant maps to its documented code and status, its JSON body carries
/// exactly the documented context keys beside `error`, and `Display` is the
/// code (what MCP clients and logs show).
#[test]
fn test_every_error_has_its_documented_code_status_and_body() {
    for (error, code, status, keys) in table() {
        assert_eq!(error.code(), code);
        assert_eq!(error.http_status(), status, "{code}");
        assert_eq!(error.to_string(), code);
        let body = error.to_json();
        let object = body.as_object().expect("an error body is an object");
        assert_eq!(body["error"], code);
        let mut got: Vec<&str> = object
            .keys()
            .map(String::as_str)
            .filter(|k| *k != "error")
            .collect();
        let mut want = keys.clone();
        got.sort_unstable();
        want.sort_unstable();
        assert_eq!(got, want, "context keys of {code}");
        assert_eq!(error.to_json_string(), body.to_string());
    }
}

/// A held name reports until when it is held, beside the `held` reason.
#[test]
fn test_name_taken_held_reports_the_hold_expiry() {
    let body = MailboxError::NameTaken {
        agent: s("Vega"),
        reason: TakenReason::Held {
            until: s("2026-11-06T10:00:00Z"),
        },
        suggestions: vec![],
    }
    .to_json();
    assert_eq!(body["reason"], "held");
    assert_eq!(body["heldUntil"], "2026-11-06T10:00:00Z");
    assert_eq!(body["suggestions"], json!([]));
}

/// A registered-name conflict names the reason and has no expiry.
#[test]
fn test_name_taken_registered_has_no_hold_expiry() {
    let body: Value = MailboxError::NameTaken {
        agent: s("Vega"),
        reason: TakenReason::Registered,
        suggestions: vec![s("Vega2")],
    }
    .to_json();
    assert_eq!(body["reason"], "registered");
    assert!(body.get("heldUntil").is_none());
}
