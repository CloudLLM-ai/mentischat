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

/// RFC 8032 section 7.1, test 1: the seed and the public key every
/// conforming Ed25519 implementation (MentisDB's included) derives from it.
const RFC8032_SEED: &str = "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60";
const RFC8032_PUBLIC: &str = "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a";

/// A seed file the way `mentisdb key generate` writes it: 64 lowercase hex
/// digits and a newline, mode 0600.
fn mentisdb_seed_file(dir: &std::path::Path, hex: &str) -> std::path::PathBuf {
    let path = dir.join("instinct.key");
    std::fs::write(&path, format!("{hex}\n")).expect("write seed");
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).expect("chmod");
    path
}

/// An existing MentisDB seed becomes the agent's identity with the public key
/// MentisDB derives from it, stored private, and reloads unchanged.
#[test]
fn test_import_adopts_a_mentisdb_seed_with_the_same_public_key() {
    let root = tempfile::tempdir().expect("tempdir");
    let src = tempfile::tempdir().expect("src");
    let seed = mentisdb_seed_file(src.path(), RFC8032_SEED);
    let want: mentischat::signed::PublicKey = RFC8032_PUBLIC.parse().unwrap();

    let imported = Identity::import(root.path(), "Instinct", &seed, Some(&want)).expect("import");
    assert_eq!(imported.public().to_hex(), RFC8032_PUBLIC);
    let key = root.path().join("instinct").join("identity.key");
    assert_eq!(mode(&key), 0o600);
    assert_eq!(mode(&root.path().join("instinct")), 0o700);
    assert_eq!(std::fs::read_to_string(&key).unwrap().trim(), RFC8032_SEED);
    let reopened = Identity::open(root.path(), "Instinct").expect("reopen");
    assert_eq!(reopened.public(), imported.public());
}

/// `--public-key` pins the key: a seed that is some other key is refused and
/// nothing is written.
#[test]
fn test_import_refuses_a_seed_that_is_not_the_expected_key() {
    let root = tempfile::tempdir().expect("tempdir");
    let src = tempfile::tempdir().expect("src");
    let seed = mentisdb_seed_file(src.path(), RFC8032_SEED);
    let other = Identity::open(src.path(), "Other")
        .expect("other key")
        .public();

    let err = Identity::import(root.path(), "Instinct", &seed, Some(&other)).unwrap_err();
    assert!(
        matches!(err, ClientError::Usage(ref m) if m.contains("nothing was imported")),
        "{err}"
    );
    assert!(!root.path().join("instinct").exists());
}

/// A seed file other users can read is a leaked key: refused, not adopted.
#[test]
fn test_import_refuses_a_seed_file_others_can_read() {
    let root = tempfile::tempdir().expect("tempdir");
    let src = tempfile::tempdir().expect("src");
    let seed = mentisdb_seed_file(src.path(), RFC8032_SEED);
    std::fs::set_permissions(&seed, std::fs::Permissions::from_mode(0o644)).unwrap();

    let err = Identity::import(root.path(), "Instinct", &seed, None).unwrap_err();
    assert!(
        matches!(err, ClientError::Storage(ref m) if m.contains("chmod 600")),
        "{err}"
    );
    assert!(!root.path().join("instinct").exists());
}

/// A file that is not 64 hex digits is refused.
#[test]
fn test_import_refuses_a_damaged_seed_file() {
    let root = tempfile::tempdir().expect("tempdir");
    let src = tempfile::tempdir().expect("src");
    for bad in [
        "",
        "abc",
        &RFC8032_SEED[..62],
        &format!("{RFC8032_SEED}00"),
        &"zz".repeat(32),
    ] {
        let seed = mentisdb_seed_file(src.path(), bad);
        let err = Identity::import(root.path(), "Instinct", &seed, None).unwrap_err();
        assert!(matches!(err, ClientError::Storage(_)), "{bad:?}: {err}");
    }
    assert!(!root.path().join("instinct").exists());
}

/// Importing the key the agent already has changes nothing; importing a
/// different one is refused and the existing identity is left intact, since
/// its rooms and name binding belong to the old key.
#[test]
fn test_import_never_replaces_a_different_existing_key() {
    let root = tempfile::tempdir().expect("tempdir");
    let src = tempfile::tempdir().expect("src");
    let seed = mentisdb_seed_file(src.path(), RFC8032_SEED);

    let first = Identity::import(root.path(), "Instinct", &seed, None).expect("import");
    let again = Identity::import(root.path(), "Instinct", &seed, None).expect("same key again");
    assert_eq!(again.public(), first.public());

    let existing = Identity::open(root.path(), "Vega").expect("vega").public();
    let err = Identity::import(root.path(), "Vega", &seed, None).unwrap_err();
    assert!(
        matches!(err, ClientError::Usage(ref m) if m.contains("nothing was changed")),
        "{err}"
    );
    assert_eq!(
        Identity::open(root.path(), "Vega").unwrap().public(),
        existing
    );
}

/// The command line: `key import` needs `--seed-file`, and `--public-key`
/// must be a valid key.
#[test]
fn test_key_import_command_line() {
    use mentischat::client::cli::{parse, Command};
    let args = |v: &[&str]| v.iter().map(|s| s.to_string()).collect::<Vec<_>>();

    assert_eq!(
        parse(&args(&[
            "key",
            "import",
            "--agent",
            "Instinct",
            "--seed-file",
            "/k",
            "--public-key",
            RFC8032_PUBLIC
        ])),
        Ok(Command::KeyImport {
            agent: "Instinct".into(),
            seed_file: "/k".into(),
            expected: Some(RFC8032_PUBLIC.parse().unwrap()),
        })
    );
    assert!(parse(&args(&["key", "import", "--agent", "Instinct"])).is_err());
    assert!(parse(&args(&[
        "key",
        "import",
        "--agent",
        "Instinct",
        "--seed-file",
        "/k",
        "--public-key",
        "beef"
    ]))
    .is_err());
}
