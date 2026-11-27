//! Key rotation and identity checks: an agent replaces its key with one signed
//! by the old key; the key registry, every signed room that holds the old key
//! (as a pin, an invitation or a ban) and a restart all follow; fingerprints and
//! `whois` let people compare keys out of band.

mod common;

use common::{fresh_bus, name, register, room};
use mentischat::bus::{
    Bus, KeyProof, Registration, SignedOutcome, SignedRequest, KEY_REGISTRY_FILE,
};
use mentischat::errors::MailboxError;
use mentischat::signed::{
    reclaim_payload, register_key_payload, rotate_key_payload, Keypair, SignedOp,
};

struct Actor {
    name: &'static str,
    keys: Keypair,
}

impl Actor {
    fn new(name: &'static str, seed: u8) -> Self {
        Self {
            name,
            keys: Keypair::from_seed([seed; 32]),
        }
    }

    async fn enroll(&self, bus: &Bus) {
        register(bus, &[self.name]).await;
        let key = self.keys.public();
        let sig = self.keys.sign(&register_key_payload(self.name, &key));
        bus.register_key(&name(self.name), key, &sig).await.unwrap();
    }

    async fn op(
        &self,
        bus: &Bus,
        r: &str,
        counter: u64,
        op: SignedOp,
    ) -> Result<SignedOutcome, MailboxError> {
        let signature = self.keys.sign(&op.payload(r, self.name, counter));
        bus.signed_op(SignedRequest {
            agent: name(self.name),
            room: room(r),
            counter,
            op,
            signature,
        })
        .await
    }

    async fn say(&self, bus: &Bus, r: &str, counter: u64, body: &str) -> Result<(), MailboxError> {
        self.op(bus, r, counter, SignedOp::Send { body: body.into() })
            .await
            .map(drop)
    }

    fn invite(&self, who: &Actor) -> SignedOp {
        SignedOp::Invite {
            invitee: who.name.into(),
            invitee_key: who.keys.public(),
        }
    }

    /// Rotate to `next` at `counter`; on success this actor signs with `next`.
    async fn rotate(&mut self, bus: &Bus, next: Keypair, counter: u64) -> Result<(), MailboxError> {
        let new = next.public();
        let signature = self
            .keys
            .sign(&rotate_key_payload(self.name, counter, &new));
        let proof = next.sign(&register_key_payload(self.name, &new));
        bus.rotate_key(&name(self.name), new, counter, &signature, &proof)
            .await?;
        self.keys = next;
        Ok(())
    }
}

fn create() -> SignedOp {
    SignedOp::Create {
        description: String::new(),
        public: false,
    }
}

fn code(e: MailboxError) -> &'static str {
    e.code()
}

/// Alice's private room `den` with bob in it.
async fn den(bus: &Bus) -> (Actor, Actor) {
    let (alice, bob) = (Actor::new("alice", 1), Actor::new("bob", 2));
    alice.enroll(bus).await;
    bob.enroll(bus).await;
    alice.op(bus, "den", 1, create()).await.unwrap();
    alice.op(bus, "den", 2, alice.invite(&bob)).await.unwrap();
    bob.op(bus, "den", 1, SignedOp::Join).await.unwrap();
    (alice, bob)
}

/// After a rotation the room pins the new key: the new key signs, the old one
/// no longer does, and the room, the key route and `whois` all show it.
#[tokio::test]
async fn a_rotated_key_takes_over_in_every_room() {
    let (_d, bus) = fresh_bus("rotate");
    let (alice, mut bob) = den(&bus).await;
    let old = Actor::new("bob", 2);
    bob.rotate(&bus, Keypair::from_seed([22; 32]), 5)
        .await
        .unwrap();

    bob.say(&bus, "den", 2, "new key").await.unwrap();
    assert_eq!(
        code(old.say(&bus, "den", 3, "old key").await.unwrap_err()),
        "invalid_signature"
    );
    let info = bus
        .signed_room_info(&name("alice"), &room("den"))
        .await
        .unwrap();
    let entry = info.members.iter().find(|m| m.agent == "bob").unwrap();
    assert_eq!(entry.public_key, bob.keys.public().to_hex());
    assert_eq!(entry.fingerprint, bob.keys.public().fingerprint());

    let bound = bus.agent_key(&name("bob")).await.unwrap().unwrap();
    assert_eq!(bound.public_key, bob.keys.public().to_hex());
    let who = bus.whois(&name("bob")).await.unwrap();
    assert_eq!(who.fingerprint, Some(bob.keys.public().fingerprint()));
    assert_ne!(who.fingerprint, Some(old.keys.public().fingerprint()));
    let _ = alice;
}

/// Each way a rotation can be wrong is refused, and nothing changes.
#[tokio::test]
async fn bad_rotations_are_refused() {
    let (_d, bus) = fresh_bus("rotate-bad");
    let (_alice, bob) = den(&bus).await;
    let next = Keypair::from_seed([22; 32]);
    let new = next.public();
    let good_proof = next.sign(&register_key_payload("bob", &new));
    let good_sig = bob.keys.sign(&rotate_key_payload("bob", 5, &new));
    let rotate = |signature, proof, counter, key| {
        let bus = bus.clone();
        async move {
            bus.rotate_key(&name("bob"), key, counter, &signature, &proof)
                .await
                .unwrap_err()
        }
    };
    // Signed by the new key instead of the old.
    let forged = next.sign(&rotate_key_payload("bob", 5, &new));
    assert_eq!(
        code(rotate(forged, good_proof, 5, new).await),
        "invalid_signature"
    );
    // A proof of possession made by another key.
    let other = Keypair::from_seed([33; 32]);
    let bad_proof = other.sign(&register_key_payload("bob", &new));
    assert_eq!(
        code(rotate(good_sig, bad_proof, 5, new).await),
        "invalid_signature"
    );
    // The signature covers the counter.
    assert_eq!(
        code(rotate(good_sig, good_proof, 6, new).await),
        "invalid_signature"
    );
    // Rotating to the key already bound.
    let same = bob
        .keys
        .sign(&rotate_key_payload("bob", 5, &bob.keys.public()));
    let same_proof = bob
        .keys
        .sign(&register_key_payload("bob", &bob.keys.public()));
    assert_eq!(
        code(rotate(same, same_proof, 5, bob.keys.public()).await),
        "invalid_key"
    );
    // A keyless name has nothing to rotate.
    register(&bus, &["keyless"]).await;
    let err = bus
        .rotate_key(&name("keyless"), new, 5, &good_sig, &good_proof)
        .await
        .unwrap_err();
    assert_eq!(code(err), "key_required");

    // A used counter cannot be used again.
    let mut bob = bob;
    bob.rotate(&bus, Keypair::from_seed([22; 32]), 5)
        .await
        .unwrap();
    let again = Keypair::from_seed([44; 32]);
    let err = bob.rotate(&bus, again, 5).await.unwrap_err();
    assert_eq!(code(err), "stale_counter");
}

/// An invitation and a ban made under the old key follow the rotation.
#[tokio::test]
async fn invitations_and_bans_follow_the_new_key() {
    let (_d, bus) = fresh_bus("rotate-invite");
    let alice = Actor::new("alice", 1);
    let mut carol = Actor::new("carol", 3);
    let mut dave = Actor::new("dave", 4);
    for a in [&alice, &carol, &dave] {
        a.enroll(&bus).await;
    }
    alice.op(&bus, "den", 1, create()).await.unwrap();
    alice
        .op(&bus, "den", 2, alice.invite(&carol))
        .await
        .unwrap();
    let ban = SignedOp::Ban {
        target: "dave".into(),
        target_key: Some(dave.keys.public()),
    };
    alice.op(&bus, "den", 3, ban).await.unwrap();

    carol
        .rotate(&bus, Keypair::from_seed([33; 32]), 9)
        .await
        .unwrap();
    carol.op(&bus, "den", 1, SignedOp::Join).await.unwrap();

    dave.rotate(&bus, Keypair::from_seed([44; 32]), 9)
        .await
        .unwrap();
    let info = bus
        .signed_room_info(&name("alice"), &room("den"))
        .await
        .unwrap();
    assert_eq!(info.banned[0].public_key, Some(dave.keys.public().to_hex()));
}

/// The key of an end-to-end encrypted room's member is in the MLS group, so it
/// cannot be swapped under the group: leave first.
#[tokio::test]
async fn a_key_in_a_secure_room_cannot_rotate() {
    let (_d, bus) = fresh_bus("rotate-secure");
    let (alice, mut bob) = den(&bus).await;
    alice
        .say(&bus, "den", 3, "mls/1 commit 0 abcd")
        .await
        .unwrap();
    let err = bob
        .rotate(&bus, Keypair::from_seed([22; 32]), 5)
        .await
        .unwrap_err();
    assert!(matches!(err, MailboxError::KeyInUse { ref room, .. } if room == "den"));
    bob.op(&bus, "den", 2, SignedOp::Leave).await.unwrap();
    bob.rotate(&bus, Keypair::from_seed([22; 32]), 5)
        .await
        .unwrap();
}

/// A restart replays the rotation from the room's journal: the new key still
/// signs and messages it signed before the restart still verify.
#[tokio::test]
async fn a_rotation_survives_a_restart() {
    let (_d, bus) = fresh_bus("rotate-restart");
    let (_alice, mut bob) = den(&bus).await;
    bob.say(&bus, "den", 2, "before").await.unwrap();
    bob.rotate(&bus, Keypair::from_seed([22; 32]), 5)
        .await
        .unwrap();
    bob.say(&bus, "den", 3, "after").await.unwrap();

    let back = Bus::from_snapshot_bytes(&bus.snapshot_bytes()).expect("restores");
    let info = back
        .signed_room_info(&name("alice"), &room("den"))
        .await
        .expect("the room replays");
    let entry = info.members.iter().find(|m| m.agent == "bob").unwrap();
    assert_eq!(entry.public_key, bob.keys.public().to_hex());
    bob.say(&back, "den", 4, "after the restart").await.unwrap();
}

/// The key registry records the rotation: after a restart over the same
/// directory only the new key reclaims the name.
#[tokio::test]
async fn the_key_registry_keeps_the_new_key() {
    let dir = tempfile::tempdir().unwrap();
    let registry = |dir: &std::path::Path| {
        Bus::new(dir)
            .unwrap()
            .with_key_registry(&dir.join(KEY_REGISTRY_FILE))
            .unwrap()
            .0
    };
    let mut vega = Actor::new("vega", 7);
    {
        let bus = registry(dir.path());
        vega.enroll(&bus).await;
        vega.rotate(&bus, Keypair::from_seed([77; 32]), 3)
            .await
            .unwrap();
    }
    let bus = registry(dir.path());
    let reclaim = |k: &Keypair, counter| Registration {
        reclaim: true,
        proof: Some(KeyProof {
            counter,
            signature: k.sign(&reclaim_payload("vega", counter)),
        }),
        ..Registration::default()
    };
    let old = Keypair::from_seed([7; 32]);
    assert!(bus
        .register_with(&name("vega"), reclaim(&old, 10))
        .await
        .is_err());
    // The rotation's counter is spent too.
    assert_eq!(
        code(
            bus.register_with(&name("vega"), reclaim(&vega.keys, 3))
                .await
                .unwrap_err()
        ),
        "stale_counter"
    );
    bus.register_with(&name("vega"), reclaim(&vega.keys, 10))
        .await
        .unwrap();
    let bound = bus.agent_key(&name("vega")).await.unwrap().unwrap();
    assert_eq!(bound.public_key, vega.keys.public().to_hex());
}

/// A forged or out-of-order `rotate` line in the key registry is dropped on
/// load, and the binding stays with the key it had.
#[tokio::test]
async fn a_forged_rotation_in_the_registry_is_dropped() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join(KEY_REGISTRY_FILE);
    let open = |dir: &std::path::Path| {
        Bus::new(dir)
            .unwrap()
            .with_key_registry(&dir.join(KEY_REGISTRY_FILE))
            .unwrap()
    };
    let vega = Actor::new("vega", 7);
    {
        let (bus, _) = open(dir.path());
        vega.enroll(&bus).await;
    }
    let mallory = Keypair::from_seed([66; 32]);
    let new = mallory.public();
    let line = |signer: &Keypair, old: &Keypair, counter: u64| {
        serde_json::json!({ "rotate": {
            "agent": "vega",
            "old_key": old.public().to_hex(),
            "new_key": new.to_hex(),
            "counter": counter,
            "signature": signer.sign(&rotate_key_payload("vega", counter, &new)).to_hex(),
            "proof": mallory.sign(&register_key_payload("vega", &new)).to_hex(),
        }})
        .to_string()
    };
    let mut text = std::fs::read_to_string(&file).unwrap();
    // Signed by the wrong key; then naming an old key that is not bound;
    // then garbage.
    text.push_str(&format!("{}\n", line(&mallory, &vega.keys, 5)));
    text.push_str(&format!("{}\n", line(&mallory, &mallory, 5)));
    text.push_str("{\"rotate\":{\"agent\":\"vega\"}}\n");
    std::fs::write(&file, text).unwrap();

    let (bus, load) = open(dir.path());
    assert_eq!(load.dropped, 3);
    assert_eq!(load.bound, 1);
    let reclaim = Registration {
        reclaim: true,
        proof: Some(KeyProof {
            counter: 9,
            signature: vega.keys.sign(&reclaim_payload("vega", 9)),
        }),
        ..Registration::default()
    };
    bus.register_with(&name("vega"), reclaim).await.unwrap();
    let bound = bus.agent_key(&name("vega")).await.unwrap().unwrap();
    assert_eq!(bound.public_key, vega.keys.public().to_hex());
}

/// A genuine rotation line replays on load and compacts to a binding of the
/// new key.
#[tokio::test]
async fn a_rotation_line_replays_and_compacts() {
    let dir = tempfile::tempdir().unwrap();
    let open = |dir: &std::path::Path| {
        Bus::new(dir)
            .unwrap()
            .with_key_registry(&dir.join(KEY_REGISTRY_FILE))
            .unwrap()
    };
    let mut vega = Actor::new("vega", 7);
    {
        let (bus, _) = open(dir.path());
        vega.enroll(&bus).await;
        vega.rotate(&bus, Keypair::from_seed([77; 32]), 3)
            .await
            .unwrap();
    }
    let raw = std::fs::read_to_string(dir.path().join(KEY_REGISTRY_FILE)).unwrap();
    assert!(raw.contains("\"rotate\""), "{raw}");
    {
        let (_bus, load) = open(dir.path());
        assert_eq!((load.bound, load.dropped), (1, 0));
    }
    let raw = std::fs::read_to_string(dir.path().join(KEY_REGISTRY_FILE)).unwrap();
    assert!(!raw.contains("\"rotate\""), "compacted: {raw}");
    assert!(raw.contains(&vega.keys.public().to_hex()));
}
