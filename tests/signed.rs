//! Signed rooms: key binding, pinned membership, replay protection, and the
//! signed REST routes. Crypto and envelope tests are pure; the rest drive a
//! real `Bus` with real Ed25519 signatures.

mod common;

use common::{fresh_bus, name, register, room};
use mentischat::bus::{Bus, SignedOutcome, SignedRequest};
use mentischat::errors::MailboxError;
use mentischat::signed::{
    hex_decode, hex_encode, register_key_payload, Envelope, EnvelopeError, Keypair, OpKind,
    PublicKey, Signature, SignedOp,
};

/// An agent with a signing identity that counts its own operations.
struct Actor {
    name: &'static str,
    keys: Keypair,
}

impl Actor {
    /// A deterministic identity: `seed` fills the 32-byte secret.
    fn new(name: &'static str, seed: u8) -> Self {
        Self {
            name,
            keys: Keypair::from_seed([seed; 32]),
        }
    }

    /// Register on the bus and bind this identity's key.
    async fn join_roster(&self, bus: &Bus) {
        register(bus, &[self.name]).await;
        self.bind(bus).await.expect("bind key");
    }

    async fn bind(&self, bus: &Bus) -> Result<(), MailboxError> {
        let key = self.keys.public();
        let sig = self.keys.sign(&register_key_payload(self.name, &key));
        bus.register_key(&name(self.name), key, &sig)
            .await
            .map(drop)
    }

    /// A request for `op` in `room` at `counter`, signed by this identity.
    fn request(&self, room_name: &str, counter: u64, op: SignedOp) -> SignedRequest {
        let payload = op.payload(room_name, self.name, counter);
        SignedRequest {
            agent: name(self.name),
            room: room(room_name),
            counter,
            op,
            signature: self.keys.sign(&payload),
        }
    }

    async fn do_op(
        &self,
        bus: &Bus,
        room_name: &str,
        counter: u64,
        op: SignedOp,
    ) -> Result<SignedOutcome, MailboxError> {
        bus.signed_op(self.request(room_name, counter, op)).await
    }

    async fn say(
        &self,
        bus: &Bus,
        room_name: &str,
        counter: u64,
        body: &str,
    ) -> Result<(), MailboxError> {
        let op = SignedOp::Send { body: body.into() };
        self.do_op(bus, room_name, counter, op).await.map(drop)
    }

    fn invite_op(&self, invitee: &Actor) -> SignedOp {
        SignedOp::Invite {
            invitee: invitee.name.into(),
            invitee_key: invitee.keys.public(),
        }
    }
}

fn create_op() -> SignedOp {
    SignedOp::Create {
        description: String::new(),
    }
}

/// `alice` owns the signed room `ops` with `bob` joined.
async fn alice_and_bob(bus: &Bus) -> (Actor, Actor) {
    let (alice, bob) = (Actor::new("alice", 1), Actor::new("bob", 2));
    alice.join_roster(bus).await;
    bob.join_roster(bus).await;
    alice.do_op(bus, "ops", 1, create_op()).await.unwrap();
    alice
        .do_op(bus, "ops", 2, alice.invite_op(&bob))
        .await
        .unwrap();
    bob.do_op(bus, "ops", 1, SignedOp::Join).await.unwrap();
    (alice, bob)
}

async fn pull_bodies(bus: &Bus, who: &str) -> Vec<String> {
    let page = bus.check_inbox(&name(who), 0).await.unwrap();
    page.messages.into_iter().map(|m| m.body).collect()
}

fn code(e: MailboxError) -> &'static str {
    e.code()
}

// ---- pure: hex, keys, payloads, envelopes ----

/// Hex round-trips both cases and rejects bad length and digits.
#[test]
fn hex_roundtrips_and_rejects_malformed_input() {
    let bytes: [u8; 3] = hex_decode("00fFa5").unwrap();
    assert_eq!(hex_encode(&bytes), "00ffa5");
    assert!(hex_decode::<3>("00ffa").is_err());
    assert!(hex_decode::<3>("00ffzz").is_err());
    assert!(hex_decode::<3>("").is_err());
}

/// A key parses from its own hex, and 32 bytes that are not a curve point are
/// refused (not every 32 bytes is a key).
#[test]
fn public_keys_roundtrip_and_non_points_are_rejected() {
    let key = Keypair::from_seed([9; 32]).public();
    assert_eq!(key.to_hex().len(), 64);
    assert_eq!(key.to_hex().parse::<PublicKey>(), Ok(key));
    // y = 2 has no square root for x on this curve.
    let mut not_point = [0u8; 32];
    not_point[0] = 2;
    assert!(PublicKey::from_bytes(not_point).is_err());
    assert!("abcd".parse::<PublicKey>().is_err());
}

/// A signature binds the exact payload and key.
#[test]
fn signatures_verify_only_for_their_payload_and_key() {
    let (a, b) = (Keypair::from_seed([1; 32]), Keypair::from_seed([2; 32]));
    let sig = a.sign(b"payload");
    assert!(a.public().verify(b"payload", &sig).is_ok());
    assert!(a.public().verify(b"payloaD", &sig).is_err());
    assert!(b.public().verify(b"payload", &sig).is_err());
    assert_eq!(sig.to_hex().parse::<Signature>(), Ok(sig));
}

/// Every field of every op is covered: changing any one changes the bytes.
#[test]
fn payloads_differ_in_every_signed_field() {
    let send = |body: &str| SignedOp::Send { body: body.into() };
    let base = send("hi").payload("room", "alice", 5);
    assert_ne!(base, send("hI").payload("room", "alice", 5));
    assert_ne!(base, send("hi").payload("room2", "alice", 5));
    assert_ne!(base, send("hi").payload("room", "bob", 5));
    assert_ne!(base, send("hi").payload("room", "alice", 6));
    assert_ne!(base, SignedOp::Join.payload("room", "alice", 5));
    assert_ne!(
        SignedOp::Join.payload("room", "alice", 5),
        SignedOp::Leave.payload("room", "alice", 5)
    );
    let key = Keypair::from_seed([4; 32]).public();
    let other = Keypair::from_seed([5; 32]).public();
    let invite = |who: &str, k: PublicKey| SignedOp::Invite {
        invitee: who.into(),
        invitee_key: k,
    };
    assert_ne!(
        invite("bob", key).payload("r", "a", 1),
        invite("bob", other).payload("r", "a", 1),
        "an invitation pins one specific key"
    );
    assert_ne!(
        invite("bob", key).payload("r", "a", 1),
        invite("carol", key).payload("r", "a", 1)
    );
}

/// Names compare without case, so payloads fold them; bodies do not fold.
#[test]
fn payloads_fold_names_but_not_bodies() {
    let op = SignedOp::Send { body: "Hi".into() };
    assert_eq!(op.payload("OPS", "ALICE", 1), op.payload("ops", "alice", 1));
    assert_ne!(
        op.payload("ops", "alice", 1),
        SignedOp::Send { body: "hi".into() }.payload("ops", "alice", 1)
    );
}

/// Length prefixes stop two different field splits from sharing bytes.
#[test]
fn payload_fields_are_length_prefixed_so_they_cannot_be_shifted() {
    let a = SignedOp::Send { body: "bc".into() }.payload("r", "a", 1);
    let b = SignedOp::Send { body: "c".into() }.payload("r", "ab", 1);
    assert_ne!(a, b);
}

/// Op names round-trip through strings and unknown ones are refused.
#[test]
fn op_kinds_roundtrip_through_strings() {
    for kind in [
        OpKind::RegisterKey,
        OpKind::Create,
        OpKind::Invite,
        OpKind::Join,
        OpKind::Leave,
        OpKind::Remove,
        OpKind::Send,
    ] {
        assert_eq!(kind.to_string().parse::<OpKind>(), Ok(kind));
    }
    assert!("delete".parse::<OpKind>().is_err());
    assert!("".parse::<OpKind>().is_err());
}

/// An envelope round-trips, keeps a multi-line body byte for byte, and fails
/// verification for another room, sender or body.
#[test]
fn envelopes_roundtrip_and_bind_room_sender_and_body() {
    let kp = Keypair::from_seed([3; 32]);
    let body = "line one\nline two\n";
    let sig = kp.sign(&SignedOp::Send { body: body.into() }.payload("r", "a", 9));
    let text = Envelope::encode(9, &kp.public(), &sig, body);
    let env = Envelope::parse(&text).unwrap();
    assert_eq!((env.counter, env.body, env.key), (9, body, kp.public()));
    assert!(env.verify("R", "A").is_ok());
    assert!(env.verify("r2", "a").is_err());
    assert!(env.verify("r", "b").is_err());
    let tampered = text.replace("line two", "line 2");
    assert!(Envelope::parse(&tampered)
        .unwrap()
        .verify("r", "a")
        .is_err());
}

/// Anything that is not exactly the header line plus a newline is malformed.
#[test]
fn malformed_envelopes_are_rejected() {
    let kp = Keypair::from_seed([3; 32]);
    let sig = kp.sign(b"x").to_hex();
    let key = kp.public().to_hex();
    for bad in [
        String::new(),
        "no newline".to_string(),
        format!("wrong-tag/1 1 {key} {sig}\nbody"),
        format!("mentischat-signed/1 one {key} {sig}\nbody"),
        format!("mentischat-signed/1 -1 {key} {sig}\nbody"),
        format!("mentischat-signed/1 1 {key} {sig} extra\nbody"),
        format!("mentischat-signed/1 1 {key}\nbody"),
        format!("mentischat-signed/1  1 {key} {sig}\nbody"),
        format!("mentischat-signed/1 99999999999999999999999 {key} {sig}\nbody"),
    ] {
        assert_eq!(
            Envelope::parse(&bad),
            Err(EnvelopeError::Malformed),
            "{bad:?}"
        );
    }
    let short = format!("mentischat-signed/1 1 {} {sig}\nbody", &key[..62]);
    assert!(matches!(
        Envelope::parse(&short),
        Err(EnvelopeError::Field(_))
    ));
}

// ---- key binding ----

/// A key binds once; the same key again is fine; another key is a conflict.
#[tokio::test]
async fn a_name_binds_one_key_and_a_second_key_conflicts() {
    let (_d, bus) = fresh_bus("keys");
    let alice = Actor::new("alice", 1);
    alice.join_roster(&bus).await;
    alice
        .bind(&bus)
        .await
        .expect("same key again is idempotent");
    let rival = Actor::new("alice", 99);
    assert_eq!(code(rival.bind(&bus).await.unwrap_err()), "key_conflict");
    let bound = bus.agent_key(&name("alice")).await.unwrap().unwrap();
    assert_eq!(bound.public_key, alice.keys.public().to_hex());
}

/// Binding needs proof of possession: a signature by some other key fails.
#[tokio::test]
async fn binding_a_key_requires_its_signature() {
    let (_d, bus) = fresh_bus("pop");
    register(&bus, &["alice"]).await;
    let mine = Keypair::from_seed([1; 32]);
    let theirs = Keypair::from_seed([2; 32]);
    let sig = theirs.sign(&register_key_payload("alice", &mine.public()));
    let err = bus
        .register_key(&name("alice"), mine.public(), &sig)
        .await
        .unwrap_err();
    assert_eq!(code(err), "invalid_signature");
    // And a signature for another name does not carry over.
    let other_name = mine.sign(&register_key_payload("bob", &mine.public()));
    let err = bus
        .register_key(&name("alice"), mine.public(), &other_name)
        .await
        .unwrap_err();
    assert_eq!(code(err), "invalid_signature");
}

/// The binding belongs to the roster entry: after deregister and a fresh
/// registration the name has no key.
#[tokio::test]
async fn a_binding_dies_with_the_registration() {
    let (_d, bus) = fresh_bus("keydeath");
    let alice = Actor::new("alice", 1);
    alice.join_roster(&bus).await;
    bus.deregister(&name("alice")).await.unwrap();
    bus.register_with(&name("alice"), common::reclaim())
        .await
        .unwrap();
    assert!(bus.agent_key(&name("alice")).await.unwrap().is_none());
    Actor::new("alice", 50)
        .bind(&bus)
        .await
        .expect("fresh name binds anew");
}

/// An unregistered name cannot bind or look up a key.
#[tokio::test]
async fn key_calls_for_an_unregistered_name_fail() {
    let (_d, bus) = fresh_bus("nokey");
    let ghost = Actor::new("ghost", 1);
    assert_eq!(code(ghost.bind(&bus).await.unwrap_err()), "unregistered");
    assert_eq!(
        code(bus.agent_key(&name("ghost")).await.unwrap_err()),
        "unregistered"
    );
}

// ---- signed rooms ----

/// The whole lifecycle: create, invite, join, send; the other member reads an
/// envelope that verifies against the key the daemon lists for the sender.
#[tokio::test]
async fn signed_messages_arrive_as_verifiable_envelopes() {
    let (_d, bus) = fresh_bus("flow");
    let (alice, _bob) = alice_and_bob(&bus).await;
    alice
        .say(&bus, "ops", 3, "No reply needed: deploy done")
        .await
        .unwrap();

    let bodies = pull_bodies(&bus, "bob").await;
    assert_eq!(bodies.len(), 1);
    let env = Envelope::parse(&bodies[0]).unwrap();
    assert_eq!(env.body, "No reply needed: deploy done");
    assert!(env.verify("ops", "alice").is_ok());

    let info = bus
        .signed_room_info(&name("bob"), &room("ops"))
        .await
        .unwrap();
    let alice_entry = info.members.iter().find(|m| m.agent == "alice").unwrap();
    assert_eq!(alice_entry.public_key, env.key.to_hex());
    assert_eq!(info.owner, Some(alice.keys.public().to_hex()));
    assert_eq!(
        info.members
            .iter()
            .map(|m| m.agent.as_str())
            .collect::<Vec<_>>(),
        ["alice", "bob"]
    );
}

/// Signed rooms are private: invisible to others and invite-only.
#[tokio::test]
async fn a_signed_room_is_private_and_unlisted() {
    let (_d, bus) = fresh_bus("private");
    let (_alice, _bob) = alice_and_bob(&bus).await;
    register(&bus, &["eve"]).await;
    assert!(bus.list_rooms(&name("eve")).await.unwrap().rooms.is_empty());
    let mine = bus.list_rooms(&name("bob")).await.unwrap().rooms;
    assert!(mine[0].private && mine[0].joined);
    assert_eq!(
        code(bus.join_room(&name("eve"), &room("ops")).await.unwrap_err()),
        "room_not_found"
    );
}

/// Plain tools cannot act in a signed room: members and invitees are told to
/// sign, everyone else learns nothing.
#[tokio::test]
async fn plain_operations_on_a_signed_room_need_a_signature() {
    let (_d, bus) = fresh_bus("plain");
    let (alice, _bob) = alice_and_bob(&bus).await;
    let carol = Actor::new("carol", 3);
    carol.join_roster(&bus).await;
    alice
        .do_op(&bus, "ops", 3, alice.invite_op(&carol))
        .await
        .unwrap();
    register(&bus, &["eve"]).await;

    let b = name("bob");
    let ops = room("ops");
    let err = bus.send_room(&b, &ops, "plain".into()).await.unwrap_err();
    assert_eq!(code(err), "signature_required");
    let err = bus.invite(&b, &ops, &name("eve")).await.unwrap_err();
    assert_eq!(code(err), "signature_required");
    let err = bus.leave_room(&b, &ops).await.unwrap_err();
    assert_eq!(code(err), "signature_required");
    // An invitee may not join the plain way either.
    let err = bus.join_room(&name("carol"), &ops).await.unwrap_err();
    assert_eq!(code(err), "signature_required");
    // A stranger gets the answer it would get for a room that is not there.
    let err = bus
        .send_room(&name("eve"), &ops, "x".into())
        .await
        .unwrap_err();
    assert_eq!(code(err), "room_not_found");
    assert!(
        pull_bodies(&bus, "bob").await.is_empty(),
        "nothing was delivered"
    );
    // The room is intact.
    let info = bus.signed_room_info(&b, &ops).await.unwrap();
    assert_eq!(info.members.len(), 2);
}

/// A plain room and a signed room use different doors: signed operations on a
/// plain room are `room_not_found`, and a plain room is unaffected.
#[tokio::test]
async fn signed_operations_do_not_reach_plain_rooms() {
    let (_d, bus) = fresh_bus("mix");
    let alice = Actor::new("alice", 1);
    alice.join_roster(&bus).await;
    bus.create_room(&name("alice"), &room("lobby"), false, None)
        .await
        .unwrap();
    let err = alice.say(&bus, "lobby", 1, "signed hi").await.unwrap_err();
    assert_eq!(code(err), "room_not_found");
    let err = alice
        .do_op(&bus, "lobby", 2, SignedOp::Leave)
        .await
        .unwrap_err();
    assert_eq!(code(err), "room_not_found");
    bus.send_room(&name("alice"), &room("lobby"), "plain hi".into())
        .await
        .unwrap();
}

/// Creating needs a bound key and an unused name.
#[tokio::test]
async fn creating_a_signed_room_needs_a_key_and_a_free_name() {
    let (_d, bus) = fresh_bus("create");
    register(&bus, &["keyless"]).await;
    let keyless = Actor::new("keyless", 7);
    let err = keyless
        .do_op(&bus, "den", 1, create_op())
        .await
        .unwrap_err();
    assert_eq!(code(err), "key_required");

    let alice = Actor::new("alice", 1);
    alice.join_roster(&bus).await;
    alice.do_op(&bus, "den", 1, create_op()).await.unwrap();
    let err = alice.do_op(&bus, "den", 2, create_op()).await.unwrap_err();
    assert_eq!(code(err), "room_exists");
    bus.create_room(&name("alice"), &room("plain"), true, None)
        .await
        .unwrap();
    let err = alice
        .do_op(&bus, "plain", 3, create_op())
        .await
        .unwrap_err();
    assert_eq!(code(err), "room_exists");
}

/// A signature by another key does not act as a member, whether it claims the
/// member's name or its own.
#[tokio::test]
async fn another_key_cannot_act_as_a_member() {
    let (_d, bus) = fresh_bus("forge");
    let (_alice, _bob) = alice_and_bob(&bus).await;
    let mallory = Actor::new("mallory", 66);
    mallory.join_roster(&bus).await;

    // Signing as alice with mallory's key: alice is bound to a different key.
    let mut forged = mallory.request("ops", 50, SignedOp::Send { body: "pwn".into() });
    forged.agent = name("alice");
    let err = bus.signed_op(forged).await.unwrap_err();
    assert_eq!(code(err), "invalid_signature");

    // Mallory as itself is a stranger to the room.
    let err = mallory.say(&bus, "ops", 51, "hi").await.unwrap_err();
    assert_eq!(code(err), "room_not_found");
    assert!(pull_bodies(&bus, "bob").await.is_empty());
}

/// Taking over a member's name after it left the roster inherits nothing: the
/// pin left with it.
#[tokio::test]
async fn a_name_taken_over_after_deregistering_is_not_a_member() {
    let (_d, bus) = fresh_bus("takeover");
    let (_alice, bob) = alice_and_bob(&bus).await;
    bus.deregister(&name("bob")).await.unwrap();
    // Eve registers as bob with her own key.
    bus.register_with(&name("bob"), common::reclaim())
        .await
        .unwrap();
    let eve = Actor::new("bob", 77);
    eve.bind(&bus).await.unwrap();
    let err = eve.say(&bus, "ops", 99, "i am bob").await.unwrap_err();
    assert_eq!(code(err), "room_not_found");
    // The real bob's old signature does not verify under eve's key either.
    let replay = bob.request("ops", 98, SignedOp::Send { body: "x".into() });
    assert_eq!(
        code(bus.signed_op(replay).await.unwrap_err()),
        "invalid_signature"
    );
}

/// A captured request cannot be submitted twice, and counters only go up.
#[tokio::test]
async fn replayed_and_lower_counters_are_refused() {
    let (_d, bus) = fresh_bus("replay");
    let (alice, _bob) = alice_and_bob(&bus).await;
    let captured = alice.request(
        "ops",
        10,
        SignedOp::Send {
            body: "pay 5".into(),
        },
    );
    bus.signed_op(captured.clone()).await.unwrap();
    let err = bus.signed_op(captured).await.unwrap_err();
    assert!(
        matches!(err, MailboxError::StaleCounter { last: 10, .. }),
        "{err:?}"
    );
    assert_eq!(
        code(alice.say(&bus, "ops", 9, "late").await.unwrap_err()),
        "stale_counter"
    );
    alice.say(&bus, "ops", 11, "next").await.unwrap();
    let delivered = pull_bodies(&bus, "bob").await;
    assert_eq!(delivered.len(), 2, "the replay was not delivered");
}

/// Counters are shared by every op of a key in the room, so a captured
/// membership op cannot be replayed to evict a member who has since rejoined.
#[tokio::test]
async fn counters_cover_membership_ops_too() {
    let (_d, bus) = fresh_bus("opcounters");
    let (alice, bob) = alice_and_bob(&bus).await;
    let leave = bob.request("ops", 5, SignedOp::Leave);
    bus.signed_op(leave.clone()).await.unwrap();
    alice
        .do_op(&bus, "ops", 3, alice.invite_op(&bob))
        .await
        .unwrap();
    bob.do_op(&bus, "ops", 7, SignedOp::Join).await.unwrap();
    assert_eq!(
        code(bus.signed_op(leave).await.unwrap_err()),
        "stale_counter",
        "the captured leave must not evict bob again"
    );
    bob.say(&bus, "ops", 8, "still in").await.unwrap();
}

/// Leaving and rejoining does not reset a key's counter, so old signed
/// messages cannot be replayed into the room afterwards.
#[tokio::test]
async fn rejoining_does_not_reopen_old_counters() {
    let (_d, bus) = fresh_bus("rejoin");
    let (alice, bob) = alice_and_bob(&bus).await;
    let old = bob.request("ops", 5, SignedOp::Send { body: "old".into() });
    bus.signed_op(old.clone()).await.unwrap();
    bob.do_op(&bus, "ops", 6, SignedOp::Leave).await.unwrap();
    alice
        .do_op(&bus, "ops", 3, alice.invite_op(&bob))
        .await
        .unwrap();
    // The join needs a counter above the old watermark of 6.
    assert_eq!(
        code(bob.do_op(&bus, "ops", 2, SignedOp::Join).await.unwrap_err()),
        "stale_counter"
    );
    bob.do_op(&bus, "ops", 7, SignedOp::Join).await.unwrap();
    assert_eq!(
        code(bus.signed_op(old).await.unwrap_err()),
        "stale_counter",
        "the captured send stays dead"
    );
}

/// An invitation names a key: the invitee must have one bound, and it must be
/// the key the inviter signed for.
#[tokio::test]
async fn invitations_pin_the_invitees_key() {
    let (_d, bus) = fresh_bus("pin");
    let alice = Actor::new("alice", 1);
    alice.join_roster(&bus).await;
    alice.do_op(&bus, "ops", 1, create_op()).await.unwrap();

    register(&bus, &["nokey"]).await;
    let nokey = Actor::new("nokey", 5);
    let err = alice
        .do_op(&bus, "ops", 2, alice.invite_op(&nokey))
        .await
        .unwrap_err();
    assert_eq!(code(err), "key_required");

    let bob = Actor::new("bob", 2);
    bob.join_roster(&bus).await;
    let wrong = SignedOp::Invite {
        invitee: "bob".into(),
        invitee_key: Keypair::from_seed([42; 32]).public(),
    };
    let err = alice.do_op(&bus, "ops", 3, wrong).await.unwrap_err();
    assert_eq!(code(err), "key_conflict");

    // A joiner with the right name but another key cannot use the invitation.
    alice
        .do_op(&bus, "ops", 4, alice.invite_op(&bob))
        .await
        .unwrap();
    bus.deregister(&name("bob")).await.unwrap();
    bus.register_with(&name("bob"), common::reclaim())
        .await
        .unwrap();
    let imposter = Actor::new("bob", 88);
    imposter.bind(&bus).await.unwrap();
    let err = imposter
        .do_op(&bus, "ops", 1, SignedOp::Join)
        .await
        .unwrap_err();
    assert!(
        matches!(code(err), "invalid_signature" | "room_not_found"),
        "an invitation is for a key"
    );
}

/// Only an invited key joins; a member who was never invited cannot.
#[tokio::test]
async fn joining_needs_an_invitation() {
    let (_d, bus) = fresh_bus("uninvited");
    let alice = Actor::new("alice", 1);
    alice.join_roster(&bus).await;
    alice.do_op(&bus, "ops", 1, create_op()).await.unwrap();
    let eve = Actor::new("eve", 9);
    eve.join_roster(&bus).await;
    let err = eve.do_op(&bus, "ops", 1, SignedOp::Join).await.unwrap_err();
    assert_eq!(code(err), "room_not_found");
}

/// Joining twice is harmless and does not consume a counter.
#[tokio::test]
async fn joining_again_changes_nothing() {
    let (_d, bus) = fresh_bus("joinagain");
    let (_alice, bob) = alice_and_bob(&bus).await;
    bob.do_op(&bus, "ops", 1, SignedOp::Join).await.unwrap();
    bob.say(&bus, "ops", 2, "still fine").await.unwrap();
}

/// A non-member's signed send is refused even with a valid signature.
#[tokio::test]
async fn a_valid_signature_does_not_make_a_stranger_a_member() {
    let (_d, bus) = fresh_bus("stranger");
    let (_alice, _bob) = alice_and_bob(&bus).await;
    let carol = Actor::new("carol", 3);
    carol.join_roster(&bus).await;
    let err = carol.say(&bus, "ops", 1, "let me in").await.unwrap_err();
    assert_eq!(code(err), "room_not_found");
}

/// Only the owner removes; the removed member stops receiving and cannot
/// send; the owner cannot remove itself.
#[tokio::test]
async fn only_the_owner_removes_and_the_removed_are_out() {
    let (_d, bus) = fresh_bus("remove");
    let (alice, bob) = alice_and_bob(&bus).await;
    let carol = Actor::new("carol", 3);
    carol.join_roster(&bus).await;
    alice
        .do_op(&bus, "ops", 3, alice.invite_op(&carol))
        .await
        .unwrap();
    carol.do_op(&bus, "ops", 1, SignedOp::Join).await.unwrap();

    let remove = |who: &str| SignedOp::Remove { target: who.into() };
    let err = bob
        .do_op(&bus, "ops", 2, remove("carol"))
        .await
        .unwrap_err();
    assert_eq!(code(err), "not_a_member", "a plain member cannot remove");
    let err = alice
        .do_op(&bus, "ops", 4, remove("alice"))
        .await
        .unwrap_err();
    assert_eq!(
        code(err),
        "not_a_member",
        "the owner leaves rather than removes itself"
    );

    alice.do_op(&bus, "ops", 5, remove("bob")).await.unwrap();
    assert_eq!(
        code(bob.say(&bus, "ops", 3, "still here?").await.unwrap_err()),
        "room_not_found"
    );
    alice.say(&bus, "ops", 6, "after removal").await.unwrap();
    assert!(
        pull_bodies(&bus, "bob").await.is_empty(),
        "bob no longer receives"
    );
    assert_eq!(pull_bodies(&bus, "carol").await.len(), 1);
    let info = bus
        .signed_room_info(&name("alice"), &room("ops"))
        .await
        .unwrap();
    assert_eq!(info.members.len(), 2);
    // A removed member cannot slip back in without a fresh invitation.
    assert_eq!(
        code(bob.do_op(&bus, "ops", 9, SignedOp::Join).await.unwrap_err()),
        "room_not_found"
    );
}

/// The owner can revoke an invitation that has not been used.
#[tokio::test]
async fn the_owner_can_revoke_an_unused_invitation() {
    let (_d, bus) = fresh_bus("revoke");
    let alice = Actor::new("alice", 1);
    let dave = Actor::new("dave", 4);
    alice.join_roster(&bus).await;
    dave.join_roster(&bus).await;
    alice.do_op(&bus, "ops", 1, create_op()).await.unwrap();
    alice
        .do_op(&bus, "ops", 2, alice.invite_op(&dave))
        .await
        .unwrap();
    alice
        .do_op(
            &bus,
            "ops",
            3,
            SignedOp::Remove {
                target: "dave".into(),
            },
        )
        .await
        .unwrap();
    let err = dave
        .do_op(&bus, "ops", 1, SignedOp::Join)
        .await
        .unwrap_err();
    assert_eq!(code(err), "room_not_found");
}

/// A member leaves with a signature; the last one out deletes the room.
#[tokio::test]
async fn leaving_is_signed_and_the_last_member_deletes_the_room() {
    let (_d, bus) = fresh_bus("leave");
    let (alice, bob) = alice_and_bob(&bus).await;
    let out = bob.do_op(&bus, "ops", 2, SignedOp::Leave).await.unwrap();
    assert!(matches!(out, SignedOutcome::Left(ref l) if !l.deleted));
    let out = alice.do_op(&bus, "ops", 3, SignedOp::Leave).await.unwrap();
    assert!(matches!(out, SignedOutcome::Left(ref l) if l.deleted));
    // The name is free again, and may be signed or plain.
    bus.create_room(&name("alice"), &room("ops"), false, None)
        .await
        .unwrap();
}

/// When the owner leaves nobody holds removal rights; the room carries on.
#[tokio::test]
async fn an_ownerless_room_keeps_working_but_cannot_remove() {
    let (_d, bus) = fresh_bus("ownerless");
    let (alice, bob) = alice_and_bob(&bus).await;
    alice.do_op(&bus, "ops", 3, SignedOp::Leave).await.unwrap();
    let info = bus
        .signed_room_info(&name("bob"), &room("ops"))
        .await
        .unwrap();
    assert_eq!(info.owner, None);
    bob.say(&bus, "ops", 2, "carrying on").await.unwrap();
    let err = bob
        .do_op(
            &bus,
            "ops",
            3,
            SignedOp::Remove {
                target: "alice".into(),
            },
        )
        .await
        .unwrap_err();
    assert_eq!(code(err), "not_a_member");
}

/// An invalid body is rejected before anything is stored, without consuming
/// the counter.
#[tokio::test]
async fn an_invalid_signed_body_is_rejected_without_burning_the_counter() {
    let (_d, bus) = fresh_bus("body");
    let (alice, _bob) = alice_and_bob(&bus).await;
    assert_eq!(
        code(alice.say(&bus, "ops", 5, "   ").await.unwrap_err()),
        "invalid_body"
    );
    let huge = "x".repeat(mentischat::bus::MAX_BODY_BYTES + 1);
    assert_eq!(
        code(alice.say(&bus, "ops", 5, &huge).await.unwrap_err()),
        "invalid_body"
    );
    alice.say(&bus, "ops", 5, "ok").await.unwrap();
}

/// A tampered signature or payload is `invalid_signature`.
#[tokio::test]
async fn a_tampered_request_fails_verification() {
    let (_d, bus) = fresh_bus("tamper");
    let (alice, _bob) = alice_and_bob(&bus).await;
    let mut req = alice.request("ops", 5, SignedOp::Send { body: "a".into() });
    req.op = SignedOp::Send { body: "b".into() };
    assert_eq!(
        code(bus.signed_op(req).await.unwrap_err()),
        "invalid_signature"
    );
    let mut req = alice.request("ops", 5, SignedOp::Send { body: "a".into() });
    req.counter = 6;
    assert_eq!(
        code(bus.signed_op(req).await.unwrap_err()),
        "invalid_signature"
    );
    let mut req = alice.request("ops", 5, SignedOp::Send { body: "a".into() });
    req.room = room("other");
    assert_eq!(
        code(bus.signed_op(req).await.unwrap_err()),
        "invalid_signature"
    );
}

/// A signed room is not snapshotted: it ends with the process rather than
/// coming back unsigned; plain rooms survive as before.
#[tokio::test]
async fn signed_rooms_are_not_persisted_but_plain_ones_are() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("persist");
    let bus = Bus::new(&data).unwrap();
    let (_alice, _bob) = alice_and_bob(&bus).await;
    bus.create_room(&name("alice"), &room("plain"), true, None)
        .await
        .unwrap();
    bus.close().await;

    let bus = Bus::new(&data).unwrap();
    let rooms = bus.list_rooms(&name("alice")).await.unwrap().rooms;
    let names: Vec<&str> = rooms.iter().map(|r| r.room.as_str()).collect();
    assert_eq!(names, ["plain"]);
    let err = bus
        .send_room(&name("alice"), &room("ops"), "x".into())
        .await
        .unwrap_err();
    assert_eq!(code(err), "room_not_found");
}

/// Many signed senders at once: every message is delivered exactly once and
/// every counter is consumed once (a racing duplicate loses).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn racing_duplicates_of_one_signed_send_land_once() {
    let (_d, bus) = fresh_bus("race");
    let (alice, _bob) = alice_and_bob(&bus).await;
    let req = alice.request(
        "ops",
        10,
        SignedOp::Send {
            body: "once".into(),
        },
    );
    let tasks: Vec<_> = (0..16)
        .map(|_| {
            let (bus, req) = (bus.clone(), req.clone());
            tokio::spawn(async move { bus.signed_op(req).await.is_ok() })
        })
        .collect();
    let mut ok = 0;
    for t in tasks {
        ok += usize::from(t.await.unwrap());
    }
    assert_eq!(ok, 1);
    assert_eq!(pull_bodies(&bus, "bob").await.len(), 1);
}
