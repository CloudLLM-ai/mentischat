//! The key registry: a name bound to a public key stays reserved for that key
//! on disk, across restarts and idle expiry, while mail stays in memory. Only a
//! `reclaim` proven by the key takes the name back (bound again at once), and
//! only a deregister signed by the key releases it.

use std::os::unix::fs::PermissionsExt;
use std::time::{Duration, SystemTime};

use mentischat::bus::{Bus, KeyProof, Registration, Retention, KEY_REGISTRY_FILE};
use mentischat::errors::{MailboxError, TakenReason};
use mentischat::names::AgentName;
use mentischat::signed::{deregister_payload, reclaim_payload, register_key_payload, Keypair};

fn name(s: &str) -> AgentName {
    AgentName::parse(s).unwrap()
}

fn keys(byte: u8) -> Keypair {
    Keypair::from_seed([byte; 32])
}

/// A bus over `dir` with the registry the daemon uses attached.
fn bus(dir: &std::path::Path) -> Bus {
    Bus::new(dir)
        .unwrap()
        .with_key_registry(&dir.join(KEY_REGISTRY_FILE))
        .unwrap()
        .0
}

async fn bind(bus: &Bus, agent: &str, k: &Keypair) -> Result<(), MailboxError> {
    let sig = k.sign(&register_key_payload(agent, &k.public()));
    bus.register_key(&name(agent), k.public(), &sig)
        .await
        .map(|_| ())
}

async fn reclaim(bus: &Bus, agent: &str, k: &Keypair, counter: u64) -> Result<(), MailboxError> {
    let proof = KeyProof {
        counter,
        signature: k.sign(&reclaim_payload(agent, counter)),
    };
    let opts = Registration {
        reclaim: true,
        proof: Some(proof),
        ..Registration::default()
    };
    bus.register_with(&name(agent), opts).await.map(|_| ())
}

/// A restart (a new bus over the same directory) keeps the binding: the name
/// is refused with reason `bound` and the key, a reclaim signed by another
/// key fails, and one signed by the bound key succeeds and is bound at once.
#[tokio::test]
async fn binding_survives_a_restart_and_only_its_key_reclaims() {
    let dir = tempfile::tempdir().unwrap();
    let vega = keys(7);
    {
        let first = bus(dir.path());
        first.register(&name("Vega")).await.unwrap();
        bind(&first, "Vega", &vega).await.unwrap();
    } // dropped without close: as a crash would

    let file = dir.path().join(KEY_REGISTRY_FILE);
    assert_eq!(
        std::fs::metadata(&file).unwrap().permissions().mode() & 0o777,
        0o600
    );

    let second = bus(dir.path());
    match second.register(&name("vega")).await.unwrap_err() {
        MailboxError::NameTaken {
            reason: TakenReason::Bound { public_key },
            suggestions,
            ..
        } => {
            assert_eq!(public_key, vega.public().to_hex());
            assert!(!suggestions.iter().any(|s| s.eq_ignore_ascii_case("vega")));
        }
        other => panic!("expected name_taken bound, got {other:?}"),
    }
    assert!(matches!(
        reclaim(&second, "Vega", &keys(8), 1).await,
        Err(MailboxError::InvalidSignature)
    ));
    reclaim(&second, "Vega", &vega, 1).await.unwrap();
    let bound = second.agent_key(&name("Vega")).await.unwrap().unwrap();
    assert_eq!(bound.public_key, vega.public().to_hex());
    // The reclaimed registration is bound to its key: another one conflicts.
    assert!(matches!(
        bind(&second, "Vega", &keys(9)).await,
        Err(MailboxError::KeyConflict { .. })
    ));
}

/// Idle expiry and the end of the name hold free a keyless name, but not a
/// bound one.
#[tokio::test]
async fn idle_expiry_does_not_free_a_bound_name() {
    let dir = tempfile::tempdir().unwrap();
    let retention = Retention {
        read_ttl: Duration::from_secs(60),
        agent_ttl: Duration::from_secs(60),
        name_hold: Duration::from_secs(60),
    };
    let bus = Bus::with_retention(dir.path(), retention)
        .unwrap()
        .with_key_registry(&dir.path().join(KEY_REGISTRY_FILE))
        .unwrap()
        .0;
    bus.register(&name("Vega")).await.unwrap();
    bus.register(&name("Orion")).await.unwrap();
    bind(&bus, "Vega", &keys(7)).await.unwrap();

    let later = SystemTime::now() + Duration::from_secs(3600);
    bus.sweep_at(later).await.unwrap();
    let after_hold = later + Duration::from_secs(120);
    bus.sweep_at(after_hold).await.unwrap();

    // Orion had no key: free again once idle and past its hold.
    bus.register(&name("Orion")).await.unwrap();
    // Vega is still reserved for its key.
    assert!(matches!(
        bus.register(&name("Vega")).await,
        Err(MailboxError::NameTaken {
            reason: TakenReason::Bound { .. },
            ..
        })
    ));
}

/// A deregister signed by the key releases the binding for good: after a
/// restart the name is free for anyone.
#[tokio::test]
async fn a_signed_deregister_releases_the_name_durably() {
    let dir = tempfile::tempdir().unwrap();
    let vega = keys(7);
    {
        let bus = bus(dir.path());
        bus.register(&name("Vega")).await.unwrap();
        bind(&bus, "Vega", &vega).await.unwrap();
        let proof = KeyProof {
            counter: 5,
            signature: vega.sign(&deregister_payload("Vega", 5)),
        };
        bus.deregister_with(&name("Vega"), Some(proof))
            .await
            .unwrap();
    }
    let bus = bus(dir.path());
    bus.register(&name("Vega")).await.unwrap();
    bind(&bus, "Vega", &keys(9)).await.unwrap();
}

/// The counter a key signed for its name is recorded, so a proof at that
/// counter cannot be replayed after a restart.
#[tokio::test]
async fn a_used_counter_cannot_be_replayed_after_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let vega = keys(7);
    {
        let bus = bus(dir.path());
        bus.register(&name("Vega")).await.unwrap();
        bind(&bus, "Vega", &vega).await.unwrap();
        // Take the registered name back once, at counter 10.
        reclaim(&bus, "Vega", &vega, 10).await.unwrap();
    }
    let bus = bus(dir.path());
    assert!(matches!(
        reclaim(&bus, "Vega", &vega, 10).await,
        Err(MailboxError::StaleCounter { last: 10, .. })
    ));
    reclaim(&bus, "Vega", &vega, 11).await.unwrap();
}

/// A line whose proof does not verify is dropped on load, so editing the
/// file cannot bind a key nobody holds; the name stays free.
#[tokio::test]
async fn a_forged_binding_is_dropped_on_load() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join(KEY_REGISTRY_FILE);
    let k = keys(7);
    let forged = format!(
        "{{\"bind\":{{\"agent\":\"Vega\",\"key\":\"{}\",\"proof\":\"{}\"}}}}\nnot json\n",
        k.public().to_hex(),
        k.sign(b"some other payload").to_hex()
    );
    std::fs::write(&file, forged).unwrap();

    let (bus, load) = Bus::new(dir.path())
        .unwrap()
        .with_key_registry(&file)
        .unwrap();
    assert_eq!(load.bound, 0);
    assert_eq!(load.dropped, 2);
    bus.register(&name("Vega")).await.unwrap();
    // The file was compacted to what survived: nothing.
    assert_eq!(std::fs::read_to_string(&file).unwrap(), "");
}

/// Without a registry the library keeps everything in memory, as before: a
/// new bus over the same directory has forgotten the binding.
#[tokio::test]
async fn without_a_registry_bindings_stay_in_memory() {
    let dir = tempfile::tempdir().unwrap();
    {
        let bus = Bus::new(dir.path()).unwrap();
        bus.register(&name("Vega")).await.unwrap();
        bind(&bus, "Vega", &keys(7)).await.unwrap();
    }
    assert!(!dir.path().join(KEY_REGISTRY_FILE).exists());
    let bus = Bus::new(dir.path()).unwrap();
    bus.register(&name("Vega")).await.unwrap();
}

/// The reclaim proof a hand-written client signs to take back its bound name
/// after a restart, pinned for the seed `0x01..0x20`, name `alice` and counter
/// 7 (the same seed as `tests/interop_vectors.rs`), so client authors can check
/// their encoding: `mentischat/reclaim/v1\0`, an empty room (u32 0), the
/// lowercase name length-prefixed (u32 big-endian), the counter (u64
/// big-endian).
#[test]
fn reclaim_proof_vector_for_client_authors() {
    let mut seed = [0u8; 32];
    for (i, b) in seed.iter_mut().enumerate() {
        *b = i as u8 + 1;
    }
    let k = Keypair::from_seed(seed);
    let payload = reclaim_payload("alice", 7);
    assert_eq!(
        mentischat::signed::hex_encode(&payload),
        "6d656e746973636861742f7265636c61696d2f7631000000000000000005616c6963650000000000000007"
    );
    assert_eq!(
        k.sign(&payload).to_hex(),
        "c5c64aa7475ac23461747e0206eba212fb0f3d7ccff0133ec5cbd55e23e8dd56ba2e4cdd345856db29a04dd7f191d9434f7e6d98a40c96134fd7640e7dbb810a"
    );
}
