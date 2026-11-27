//! Signed direct messages and a bus that requires signatures: a direct
//! message signed by the sender's key arrives as an envelope the recipient can
//! check; replays and forgeries are refused; a bus that requires signatures
//! refuses plain sends and plain room calls, and still lets agents read and
//! leave.

mod common;

use common::{fresh_bus, name, register, room};
use mentischat::bus::{Bus, SignedOutcome, SignedRequest};
use mentischat::errors::MailboxError;
use mentischat::signed::{direct_payload, register_key_payload, Envelope, Keypair, SignedOp};

fn keys(seed: u8) -> Keypair {
    Keypair::from_seed([seed; 32])
}

async fn enroll(bus: &Bus, agent: &str, k: &Keypair) {
    register(bus, &[agent]).await;
    let sig = k.sign(&register_key_payload(agent, &k.public()));
    bus.register_key(&name(agent), k.public(), &sig)
        .await
        .unwrap();
}

async fn send(
    bus: &Bus,
    k: &Keypair,
    (from, to): (&str, &str),
    counter: u64,
    body: &str,
) -> Result<(), MailboxError> {
    let sig = k.sign(&direct_payload(from, to, counter, body));
    bus.send_signed(&name(from), &name(to), body.into(), counter, &sig)
        .await
        .map(drop)
}

fn code(e: MailboxError) -> &'static str {
    e.code()
}

fn strict(tag: &str) -> (tempfile::TempDir, Bus) {
    let dir = tempfile::tempdir().unwrap();
    let bus = Bus::new(&dir.path().join(tag))
        .unwrap()
        .with_signatures_required();
    (dir, bus)
}

/// The recipient gets an envelope that verifies for this sender and recipient
/// under the sender's bound key, and for nobody else.
#[tokio::test]
async fn a_signed_direct_message_arrives_as_a_checkable_envelope() {
    let (_d, bus) = fresh_bus("direct");
    let (vega, orion) = (keys(1), keys(2));
    enroll(&bus, "vega", &vega).await;
    enroll(&bus, "orion", &orion).await;
    send(&bus, &vega, ("vega", "orion"), 1, "hello\nthere")
        .await
        .unwrap();

    let page = bus.check_inbox(&name("orion"), 0).await.unwrap();
    let body = &page.messages[0].body;
    let env = Envelope::parse(body).unwrap();
    assert_eq!(env.body, "hello\nthere");
    assert_eq!(env.key, vega.public());
    assert!(env.verify_direct("vega", "orion").is_ok());
    assert!(env.verify_direct("vega", "lyra").is_err());
    assert!(env.verify_direct("orion", "orion").is_err());
}

/// A forged signature, a replayed counter and a keyless sender are refused;
/// a failed delivery does not spend the counter.
#[tokio::test]
async fn forged_replayed_and_keyless_direct_sends_are_refused() {
    let (_d, bus) = fresh_bus("direct-bad");
    let (vega, orion) = (keys(1), keys(2));
    enroll(&bus, "vega", &vega).await;
    enroll(&bus, "orion", &orion).await;
    assert_eq!(
        code(
            send(&bus, &orion, ("vega", "orion"), 1, "as vega")
                .await
                .unwrap_err()
        ),
        "invalid_signature"
    );
    send(&bus, &vega, ("vega", "orion"), 5, "first")
        .await
        .unwrap();
    let err = send(&bus, &vega, ("vega", "orion"), 5, "again")
        .await
        .unwrap_err();
    assert!(matches!(err, MailboxError::StaleCounter { last: 5, .. }));
    // To nobody: refused, and counter 6 is still free.
    assert_eq!(
        code(
            send(&bus, &vega, ("vega", "nobody"), 6, "x")
                .await
                .unwrap_err()
        ),
        "unregistered"
    );
    send(&bus, &vega, ("vega", "orion"), 6, "second")
        .await
        .unwrap();

    register(&bus, &["keyless"]).await;
    let sig = vega.sign(&direct_payload("keyless", "orion", 1, "hi"));
    let err = bus
        .send_signed(&name("keyless"), &name("orion"), "hi".into(), 1, &sig)
        .await
        .unwrap_err();
    assert_eq!(code(err), "key_required");
}

/// A bus that requires signatures refuses every plain send and plain room
/// call, takes signed ones, and still lets agents read and leave.
#[tokio::test]
async fn a_bus_that_requires_signatures_takes_only_signed_writes() {
    let (_d, bus) = strict("strict");
    assert!(bus.signatures_required());
    let (vega, orion) = (keys(1), keys(2));
    enroll(&bus, "vega", &vega).await;
    enroll(&bus, "orion", &orion).await;

    let err = bus
        .send(&name("vega"), &name("orion"), "plain".into())
        .await
        .unwrap_err();
    assert_eq!(err_to(&err), Some("orion".to_string()));
    assert_eq!(code(err), "signature_required");
    let err = bus
        .create_room(&name("vega"), &room("lobby"), false, None)
        .await
        .unwrap_err();
    assert_eq!(code(err), "signature_required");

    send(&bus, &vega, ("vega", "orion"), 1, "signed")
        .await
        .unwrap();
    let page = bus.check_inbox(&name("orion"), 0).await.unwrap();
    assert_eq!(page.messages.len(), 1);

    let create = SignedOp::Create {
        description: String::new(),
        public: true,
    };
    let signature = vega.sign(&create.payload("lobby", "vega", 1));
    let made = bus
        .signed_op(SignedRequest {
            agent: name("vega"),
            room: room("lobby"),
            counter: 1,
            op: create,
            signature,
        })
        .await
        .unwrap();
    assert!(matches!(made, SignedOutcome::Room(_)));
    for plain in [
        bus.join_room(&name("orion"), &room("lobby")).await,
        bus.send_room(&name("vega"), &room("lobby"), "plain".into())
            .await
            .map(|_| unreachable_info()),
    ] {
        assert_eq!(code(plain.unwrap_err()), "signature_required");
    }
}

fn err_to(e: &MailboxError) -> Option<String> {
    e.to_json()["to"].as_str().map(str::to_string)
}

fn unreachable_info() -> mentischat::bus::RoomInfo {
    unreachable!("a plain room send must be refused")
}

/// The direct counter survives a restart, so a captured message cannot be
/// sent again afterwards.
#[tokio::test]
async fn the_direct_counter_survives_a_restart() {
    let (_d, bus) = fresh_bus("direct-restart");
    let (vega, orion) = (keys(1), keys(2));
    enroll(&bus, "vega", &vega).await;
    enroll(&bus, "orion", &orion).await;
    send(&bus, &vega, ("vega", "orion"), 9, "before")
        .await
        .unwrap();
    let back = Bus::from_snapshot_bytes(&bus.snapshot_bytes()).unwrap();
    let err = send(&back, &vega, ("vega", "orion"), 9, "before")
        .await
        .unwrap_err();
    assert_eq!(code(err), "stale_counter");
    send(&back, &vega, ("vega", "orion"), 10, "after")
        .await
        .unwrap();
}

/// Over REST and the tool layer, `counter` and `signature` make a send signed;
/// one without the other is `invalid_body`.
#[tokio::test]
async fn the_send_contract_takes_a_signature() {
    use serde_json::json;
    let (_d, bus) = strict("strict-tools");
    let (vega, orion) = (keys(1), keys(2));
    enroll(&bus, "vega", &vega).await;
    enroll(&bus, "orion", &orion).await;
    let sig = vega.sign(&direct_payload("vega", "orion", 1, "hi"));
    let reply = mentischat::tools::send_message(
        &bus,
        &json!({ "from": "vega", "to": "orion", "body": "hi", "counter": 1, "signature": sig.to_hex() }),
    )
    .await
    .unwrap();
    assert_eq!(reply["to"], "orion");
    let half = mentischat::tools::send_message(
        &bus,
        &json!({ "from": "vega", "to": "orion", "body": "hi", "counter": 2 }),
    )
    .await
    .unwrap_err();
    assert_eq!(code(half), "invalid_body");
}

/// A room send with `counter` and `signature` through the send contract (REST
/// and WebSocket) is the signed room `send`.
#[tokio::test]
async fn a_signed_room_send_goes_through_the_send_contract() {
    use serde_json::json;
    let (_d, bus) = strict("strict-room-tools");
    let (vega, orion) = (keys(1), keys(2));
    enroll(&bus, "vega", &vega).await;
    enroll(&bus, "orion", &orion).await;
    let create = SignedOp::Create {
        description: String::new(),
        public: true,
    };
    bus.signed_op(SignedRequest {
        agent: name("vega"),
        room: room("lobby"),
        counter: 1,
        signature: vega.sign(&create.payload("lobby", "vega", 1)),
        op: create,
    })
    .await
    .unwrap();
    let send = SignedOp::Send {
        body: "hi all".into(),
    };
    let sig = vega.sign(&send.payload("lobby", "vega", 2));
    let reply = mentischat::tools::send_message(
        &bus,
        &json!({ "from": "vega", "room": "lobby", "body": "hi all", "counter": 2, "signature": sig.to_hex() }),
    )
    .await
    .unwrap();
    assert_eq!(reply["room"], "lobby");
    let bad = mentischat::tools::send_message(
        &bus,
        &json!({ "from": "vega", "room": "lobby", "body": "x", "counter": 3, "signature": "zz" }),
    )
    .await
    .unwrap_err();
    assert_eq!(code(bad), "invalid_signature");
}

/// A room keeps at most `MAX_BANS` bans; one more is `room_full`.
#[tokio::test]
async fn a_room_keeps_a_bounded_number_of_bans() {
    use mentischat::bus::MAX_BANS;
    let (_d, bus) = fresh_bus("bans-cap");
    let vega = keys(1);
    enroll(&bus, "vega", &vega).await;
    let op = |counter: u64, op: SignedOp| SignedRequest {
        agent: name("vega"),
        room: room("lobby"),
        counter,
        signature: vega.sign(&op.payload("lobby", "vega", counter)),
        op,
    };
    let create = SignedOp::Create {
        description: String::new(),
        public: true,
    };
    bus.signed_op(op(1, create)).await.unwrap();
    let ban = |i: usize| SignedOp::Ban {
        target: format!("ghost{i}"),
        target_key: None,
    };
    for i in 0..MAX_BANS {
        bus.signed_op(op(2 + i as u64, ban(i))).await.unwrap();
    }
    let err = bus
        .signed_op(op(2 + MAX_BANS as u64, ban(MAX_BANS)))
        .await
        .unwrap_err();
    assert_eq!(code(err), "room_full");
}
