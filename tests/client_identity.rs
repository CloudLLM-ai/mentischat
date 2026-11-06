//! The client's identity file: created private, reloaded unchanged, and
//! refused when it looks leaked or damaged. This is the key-handling code, so
//! the failure paths matter as much as the happy one.
#![cfg(feature = "e2e")]

use std::os::unix::fs::PermissionsExt;

use mentischat::client::{ClientError, Identity};

fn mode(path: &std::path::Path) -> u32 {
    std::fs::metadata(path)
        .expect("metadata")
        .permissions()
        .mode()
        & 0o777
}

/// A new identity writes a 64-hex seed file with mode 0600 inside a 0700
/// directory named for the lowercased agent, and reopening it yields the same
/// public key.
#[test]
fn test_identity_is_created_private_and_reloads_unchanged() {
    let root = tempfile::tempdir().expect("tempdir");
    let first = Identity::open(root.path(), "Vega").expect("create");
    let dir = root.path().join("vega");
    let key_file = dir.join("identity.key");

    assert_eq!(first.agent(), "Vega");
    assert_eq!(first.dir(), dir);
    assert_eq!(mode(&dir), 0o700);
    assert_eq!(mode(&key_file), 0o600);
    let text = std::fs::read_to_string(&key_file).expect("read key");
    assert_eq!(text.trim().len(), 64);
    assert!(text.trim().chars().all(|c| c.is_ascii_hexdigit()));

    let again = Identity::open(root.path(), "Vega").expect("reload");
    assert_eq!(again.public(), first.public());
    assert_eq!(again.keypair().public(), first.public());
}

/// Two agents get independent keys, and `Debug` never prints a seed.
#[test]
fn test_identities_are_independent_and_debug_hides_the_seed() {
    let root = tempfile::tempdir().expect("tempdir");
    let vega = Identity::open(root.path(), "Vega").expect("vega");
    let orion = Identity::open(root.path(), "Orion").expect("orion");
    assert_ne!(vega.public(), orion.public());

    let seed = std::fs::read_to_string(root.path().join("vega/identity.key")).expect("seed");
    let shown = format!("{vega:?}");
    assert!(shown.contains("Vega"));
    assert!(!shown.contains(seed.trim()), "Debug leaked the seed");
}

/// A key file that group or others can read is refused with the fix in the
/// message, and nothing is rewritten.
#[test]
fn test_loose_key_file_permissions_are_refused() {
    let root = tempfile::tempdir().expect("tempdir");
    Identity::open(root.path(), "Vega").expect("create");
    let key_file = root.path().join("vega/identity.key");
    std::fs::set_permissions(&key_file, std::fs::Permissions::from_mode(0o644)).expect("chmod");
    let before = std::fs::read_to_string(&key_file).expect("read");

    let err = Identity::open(root.path(), "Vega").unwrap_err();
    assert!(matches!(err, ClientError::Storage(_)), "{err}");
    assert!(err.to_string().contains("chmod 600"), "{err}");
    assert_eq!(std::fs::read_to_string(&key_file).expect("read"), before);
}

/// A key file that is not exactly 64 hex digits is a storage error naming the
/// file; the client never invents a replacement key over a damaged one.
#[test]
fn test_damaged_key_file_is_refused_not_replaced() {
    let root = tempfile::tempdir().expect("tempdir");
    Identity::open(root.path(), "Vega").expect("create");
    let key_file = root.path().join("vega/identity.key");
    for damaged in [
        "",
        "not hex at all",
        "abcd",
        &"zz".repeat(32),
        &"ab".repeat(33),
    ] {
        std::fs::write(&key_file, damaged).expect("write");
        std::fs::set_permissions(&key_file, std::fs::Permissions::from_mode(0o600)).expect("chmod");
        let err = Identity::open(root.path(), "Vega").unwrap_err();
        assert!(matches!(err, ClientError::Storage(_)), "{damaged:?}: {err}");
        assert!(err.to_string().contains("identity.key"), "{err}");
        assert_eq!(std::fs::read_to_string(&key_file).expect("read"), damaged);
    }
}

/// An unreadable key location (a directory where the file should be) is a
/// storage error, not a silently created second key.
#[test]
fn test_unreadable_key_location_is_a_storage_error() {
    let root = tempfile::tempdir().expect("tempdir");
    std::fs::create_dir_all(root.path().join("vega/identity.key")).expect("mkdir");
    let err = Identity::open(root.path(), "Vega").unwrap_err();
    assert!(matches!(err, ClientError::Storage(_)), "{err}");
}

/// The client directory comes from `MENTISCHAT_CLIENT_DIR`, else under `HOME`,
/// else is an error. One test owns the process environment for these three.
#[test]
fn test_default_client_dir_resolution() {
    use mentischat::client::default_client_dir;

    std::env::set_var("MENTISCHAT_CLIENT_DIR", "/somewhere/keys");
    assert_eq!(
        default_client_dir().unwrap(),
        std::path::Path::new("/somewhere/keys")
    );

    std::env::set_var("MENTISCHAT_CLIENT_DIR", "");
    std::env::set_var("HOME", "/home/agent");
    assert_eq!(
        default_client_dir().unwrap(),
        std::path::Path::new("/home/agent/.cloudllm/mentischat/client")
    );

    std::env::remove_var("MENTISCHAT_CLIENT_DIR");
    std::env::remove_var("HOME");
    let err = default_client_dir().unwrap_err();
    assert!(err.to_string().contains("HOME is not set"), "{err}");
}

/// `ClientError` prints a sentence per kind and exposes the daemon's code only
/// for daemon errors.
#[test]
fn test_client_error_display_and_daemon_code() {
    let daemon = ClientError::Daemon {
        status: 409,
        code: "name_taken".into(),
        body: serde_json::json!({ "error": "name_taken" }),
    };
    assert_eq!(daemon.to_string(), "daemon error 409: name_taken");
    assert_eq!(daemon.daemon_code(), Some("name_taken"));

    let others = [
        (
            ClientError::Transport("refused".into()),
            "daemon unreachable: refused",
        ),
        (
            ClientError::Behind { room: "r".into() },
            "missed commits in r",
        ),
        (
            ClientError::RolledBack {
                room: "r".into(),
                detail: "epoch 3 < 5".into(),
            },
            "rolled back by the daemon: epoch 3 < 5",
        ),
        (ClientError::Mls("bad".into()), "mls: bad"),
        (ClientError::Storage("disk".into()), "storage: disk"),
        (ClientError::Usage("not the owner".into()), "not the owner"),
    ];
    for (error, text) in others {
        assert!(error.to_string().contains(text), "{error}");
        assert_eq!(error.daemon_code(), None);
    }
}
