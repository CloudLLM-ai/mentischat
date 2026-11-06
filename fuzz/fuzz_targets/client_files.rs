//! The client's files on disk: `identity.key` (the Ed25519 seed) and
//! `state.json` (MLS storage and room logs), read back by `Identity::open` and
//! `Client::open`. A damaged, truncated or hostile file must be a typed
//! `ClientError::Storage`, never a panic and never a silently replaced key.
//!
//! Input: the key file's bytes, a NUL, then the state file's bytes (no NUL
//! means no state file). Invariants: `Identity::open` succeeds exactly when the
//! trimmed key text is 64 hex digits, never rewrites or removes the key file,
//! and yields the same public key on every open; `Client::open` on an opened
//! identity either fails with `Storage` or yields a client whose accessors
//! answer without panicking, and opening it twice agrees.
#![no_main]

use std::os::unix::fs::PermissionsExt;

use libfuzzer_sys::fuzz_target;
use mentischat::client::{Client, ClientError, Daemon, Identity};

fn write_private(path: &std::path::Path, bytes: &[u8]) {
    std::fs::write(path, bytes).expect("write");
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).expect("chmod");
}

fn is_seed(text: &str) -> bool {
    text.len() == 64 && text.bytes().all(|b| b.is_ascii_hexdigit())
}

fuzz_target!(|data: &[u8]| {
    let (key_bytes, state) = match data.iter().position(|&b| b == 0) {
        Some(i) => (&data[..i], Some(&data[i + 1..])),
        None => (data, None),
    };
    let root = tempfile::tempdir().expect("tempdir");
    let dir = root.path().join("fuzz");
    std::fs::create_dir_all(&dir).expect("mkdir");
    let key_file = dir.join("identity.key");
    write_private(&key_file, key_bytes);
    let state_file = dir.join("state.json");
    if let Some(state) = state {
        write_private(&state_file, state);
    }

    let opened = Identity::open(root.path(), "fuzz");
    assert_eq!(std::fs::read(&key_file).expect("key file stays"), key_bytes, "the key file was rewritten");
    let valid = std::str::from_utf8(key_bytes).is_ok_and(|t| is_seed(t.trim()));
    let identity = match opened {
        Ok(identity) => {
            assert!(valid, "accepted a key file that is not 64 hex digits");
            identity
        }
        Err(e) => {
            assert!(!valid, "refused a well-formed key file: {e}");
            assert!(matches!(e, ClientError::Storage(_)), "key errors are Storage: {e}");
            return;
        }
    };
    let again = Identity::open(root.path(), "fuzz").expect("second open");
    assert_eq!(identity.public(), again.public());

    let daemon = Daemon::local();
    let first = Client::open(identity, daemon);
    let second = Client::open(again, daemon);
    assert_eq!(first.is_ok(), second.is_ok(), "opening is not deterministic");
    match first {
        Err(e) => assert!(matches!(e, ClientError::Storage(_)), "state errors are Storage: {e}"),
        Ok(client) => {
            let rooms = client.secure_rooms();
            let _ = client.unread_counts();
            for room in rooms {
                assert!(client.is_secure(&room));
                let _ = client.history(&room);
                let _ = client.group_members(&room);
            }
        }
    }
});
