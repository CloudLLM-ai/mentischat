//! Signed rooms, key bindings and the delivery service's queues across a
//! restart (`MENTISCHAT_PERSIST=1`): saved as signed records, replayed and
//! re-verified on restore, and rejected per room when a snapshot was tampered
//! with. Rooms never come back unsigned.

mod common;

use common::{name, register, room};
use mentischat::bus::{Bus, SignedOutcome, SignedRequest};
use mentischat::errors::MailboxError;
use mentischat::signed::{key_packages_payload, register_key_payload, Envelope, Keypair, SignedOp};
use serde_json::{json, Value};

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
}

async fn take_alice(bus: &Bus) -> mentischat::bus::KeyPackageOffer {
    bus.take_key_package(&name("alice"), &name("req"))
        .await
        .unwrap()
}

fn code(e: MailboxError) -> &'static str {
    e.code()
}

/// A busy signed room `den` (alice owns; bob member; carol invited with a
/// Welcome waiting; dave joined and left) plus a plain room `lobby`, and keys
/// and KeyPackages published, in a bus the caller snapshots.
async fn busy(bus: &Bus) -> (Actor, Actor, Actor, Actor) {
    let (alice, bob, carol, dave) = (
        Actor::new("alice", 1),
        Actor::new("bob", 2),
        Actor::new("carol", 3),
        Actor::new("dave", 4),
    );
    for a in [&alice, &bob, &carol, &dave] {
        a.enroll(bus).await;
    }
    let create = SignedOp::Create {
        description: "the den".into(),
    };
    alice.op(bus, "den", 1, create).await.unwrap();
    alice.op(bus, "den", 2, alice.invite(&bob)).await.unwrap();
    bob.op(bus, "den", 1, SignedOp::Join).await.unwrap();
    alice.say(bus, "den", 3, "mls/1 commit 0 aa").await.unwrap();
    bob.say(bus, "den", 2, "No reply needed: plain words")
        .await
        .unwrap();
    alice.op(bus, "den", 4, alice.invite(&dave)).await.unwrap();
    dave.op(bus, "den", 1, SignedOp::Join).await.unwrap();
    bob.say(bus, "den", 3, "mls/1 commit 1 bb").await.unwrap();
    dave.op(bus, "den", 2, SignedOp::Leave).await.unwrap();
    alice.op(bus, "den", 5, alice.invite(&carol)).await.unwrap();
    let welcome = SignedOp::Welcome {
        target: "carol".into(),
        welcome: vec![1, 2, 3],
    };
    alice.op(bus, "den", 6, welcome).await.unwrap();
    // A superseded Welcome must not linger.
    let newer = SignedOp::Welcome {
        target: "carol".into(),
        welcome: vec![9, 9],
    };
    alice.op(bus, "den", 7, newer).await.unwrap();
    bus.create_room(&name("alice"), &room("lobby"), false, None)
        .await
        .unwrap();
    bus.send_room(&name("alice"), &room("lobby"), "plain survives".into())
        .await
        .unwrap();
    let packages = vec![vec![5u8, 5], vec![6u8]];
    let sig = alice
        .keys
        .sign(&key_packages_payload("alice", 50, &packages, Some(&[7, 7])));
    bus.publish_key_packages(&name("alice"), 50, packages, Some(vec![7, 7]), &sig)
        .await
        .unwrap();
    (alice, bob, carol, dave)
}

async fn restart(bus: &Bus) -> Bus {
    Bus::from_snapshot_bytes(&bus.snapshot_bytes()).expect("snapshot restores")
}

fn snapshot_value(bus: &Bus) -> Value {
    serde_json::from_slice(&bus.snapshot_bytes()).unwrap()
}

fn bus_from(value: &Value) -> Bus {
    Bus::from_snapshot_bytes(&serde_json::to_vec(value).unwrap()).expect("still parses")
}

fn den_record(snapshot: &mut Value) -> &mut Value {
    snapshot["rooms"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|r| r["name"] == "den")
        .expect("the den is in the snapshot")
}

async fn den_gone(bus: &Bus) -> bool {
    matches!(
        bus.send_room(&name("alice"), &room("den"), "x".into())
            .await,
        Err(MailboxError::RoomNotFound { .. })
    )
}

/// The room, its members, owner, epoch, invitation and Welcome all come back,
/// and it is still signed.
#[tokio::test]
async fn a_signed_room_survives_a_restart_with_everything_it_had() {
    let (_d, bus) = common::fresh_bus("persist");
    let (alice, bob, carol, _dave) = busy(&bus).await;
    let before = bus
        .signed_room_info(&name("alice"), &room("den"))
        .await
        .unwrap();

    let back = restart(&bus).await;
    let after = back
        .signed_room_info(&name("alice"), &room("den"))
        .await
        .unwrap();
    assert_eq!(after, before);
    assert_eq!(after.epoch, 2);
    assert_eq!(after.invited, ["carol"]);
    assert_eq!(
        back.signed_welcome(&name("carol"), &room("den"))
            .await
            .unwrap(),
        Some(vec![9, 9]),
        "the latest Welcome, not the superseded one"
    );

    // Still signed: plain operations are refused, signed ones work.
    let err = back
        .send_room(&name("bob"), &room("den"), "x".into())
        .await
        .unwrap_err();
    assert_eq!(code(err), "signature_required");
    // Counters did not reset: an old one is stale, the next one lands.
    assert_eq!(
        code(bob.say(&back, "den", 3, "again").await.unwrap_err()),
        "stale_counter"
    );
    bob.say(&back, "den", 10, "after restart").await.unwrap();
    carol.op(&back, "den", 1, SignedOp::Join).await.unwrap();
    // The epoch chain continues where it left off.
    let wrong = alice
        .say(&back, "den", 20, "mls/1 commit 1 cc")
        .await
        .unwrap_err();
    assert!(
        matches!(wrong, MailboxError::StaleEpoch { epoch: 2, .. }),
        "{wrong:?}"
    );
    alice
        .say(&back, "den", 21, "mls/1 commit 2 cc")
        .await
        .unwrap();

    // Retained messages came back and verify.
    let page = back
        .room_history(&name("alice"), &room("den"), None)
        .await
        .unwrap();
    assert!(page.messages.iter().all(|m| Envelope::parse(&m.body)
        .unwrap()
        .verify("den", &m.from)
        .is_ok()));
    // The plain room is untouched.
    assert!(back
        .list_rooms(&name("alice"))
        .await
        .unwrap()
        .rooms
        .iter()
        .any(|r| r.room == "lobby"));
}

/// Key bindings and the KeyPackage directory, including the last-resort
/// package and the publish counter, come back.
#[tokio::test]
async fn key_bindings_and_key_packages_survive_a_restart() {
    let (_d, bus) = common::fresh_bus("persistkeys");
    let (alice, _bob, _carol, _dave) = busy(&bus).await;
    let back = restart(&bus).await;
    let bound = back.agent_key(&name("alice")).await.unwrap().unwrap();
    assert_eq!(bound.public_key, alice.keys.public().to_hex());
    register(&back, &["req"]).await;
    assert_eq!(take_alice(&back).await.package, vec![5, 5]);
    assert_eq!(take_alice(&back).await.package, vec![6]);
    let last = take_alice(&back).await;
    assert_eq!((last.package, last.last_resort), (vec![7, 7], true));
    // The publish counter is remembered: an old one is stale.
    let sig = alice
        .keys
        .sign(&key_packages_payload("alice", 50, &[vec![1]], None));
    let err = back
        .publish_key_packages(&name("alice"), 50, vec![vec![1]], None, &sig)
        .await
        .unwrap_err();
    assert_eq!(code(err), "stale_counter");
    // And a name still cannot be rebound to another key.
    let rival = Keypair::from_seed([99; 32]);
    let sig = rival.sign(&register_key_payload("alice", &rival.public()));
    let err = back
        .register_key(&name("alice"), rival.public(), &sig)
        .await
        .unwrap_err();
    assert_eq!(code(err), "key_conflict");
}

/// A restore is a fixed point: snapshotting the restored bus and restoring
/// again gives the same room.
#[tokio::test]
async fn restoring_twice_gives_the_same_room() {
    let (_d, bus) = common::fresh_bus("twice");
    busy(&bus).await;
    let once = restart(&bus).await;
    let twice = restart(&once).await;
    let (a, b) = (
        once.signed_room_info(&name("alice"), &room("den"))
            .await
            .unwrap(),
        twice
            .signed_room_info(&name("alice"), &room("den"))
            .await
            .unwrap(),
    );
    assert_eq!(a, b);
    let ours = snapshot_value(&once);
    let theirs = snapshot_value(&twice);
    assert_eq!(
        ours["rooms"]
            .as_array()
            .unwrap()
            .iter()
            .find(|r| r["name"] == "den")
            .unwrap()["signed"],
        theirs["rooms"]
            .as_array()
            .unwrap()
            .iter()
            .find(|r| r["name"] == "den")
            .unwrap()["signed"],
    );
}

/// A real stop and start through the data directory, as the daemon does it.
#[tokio::test]
async fn a_clean_stop_and_start_keeps_the_signed_room() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    let bus = Bus::new(&data).unwrap();
    let (alice, ..) = busy(&bus).await;
    bus.close().await;
    let bus = Bus::new(&data).unwrap();
    alice
        .say(&bus, "den", 30, "mls/1 commit 2 dd")
        .await
        .unwrap();
    let err = bus
        .send_room(&name("alice"), &room("den"), "x".into())
        .await
        .unwrap_err();
    assert_eq!(code(err), "signature_required");
}

// ---- tampering ----

/// Flipping a bit of any journal signature rejects the room, and only it.
#[tokio::test]
async fn a_bad_signature_in_the_journal_rejects_the_room() {
    let (_d, bus) = common::fresh_bus("badsig");
    busy(&bus).await;
    let mut snap = snapshot_value(&bus);
    let n = den_record(&mut snap)["signed"]["journal"]
        .as_array()
        .unwrap()
        .len();
    assert!(n >= 8);
    for i in 0..n {
        let mut s = snap.clone();
        let sig = den_record(&mut s)["signed"]["journal"][i]["signature"]
            .as_str()
            .unwrap()
            .to_string();
        let flipped = format!(
            "{}{}",
            if sig.starts_with('0') { "1" } else { "0" },
            &sig[1..]
        );
        den_record(&mut s)["signed"]["journal"][i]["signature"] = json!(flipped);
        let back = bus_from(&s);
        assert!(
            den_gone(&back).await,
            "record {i} tampered but the room came back"
        );
        assert!(back
            .list_rooms(&name("alice"))
            .await
            .unwrap()
            .rooms
            .iter()
            .any(|r| r.room == "lobby"));
    }
}

/// Editing any signed field of a record breaks its signature.
#[tokio::test]
async fn editing_a_journal_record_rejects_the_room() {
    let (_d, bus) = common::fresh_bus("editrec");
    busy(&bus).await;
    let snap = snapshot_value(&bus);
    let journal = snap["rooms"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["name"] == "den")
        .unwrap()["signed"]["journal"]
        .as_array()
        .unwrap()
        .clone();
    let mut tried = 0;
    for (i, rec) in journal.iter().enumerate() {
        for field in [
            "agent",
            "counter",
            "invitee",
            "target",
            "body",
            "welcome",
            "description",
        ] {
            let Some(old) = rec.get(field).filter(|v| !v.is_null()) else {
                continue;
            };
            let new = match old {
                Value::Number(n) => json!(n.as_u64().unwrap() + 1000),
                Value::String(t) if field == "agent" => {
                    json!(if t == "alice" { "bob" } else { "alice" })
                }
                Value::String(t) if field == "welcome" => json!(format!("{t}00")),
                Value::String(t) => json!(format!("{t}x")),
                _ => continue,
            };
            let mut s = snap.clone();
            den_record(&mut s)["signed"]["journal"][i][field] = new;
            tried += 1;
            assert!(
                den_gone(&bus_from(&s)).await,
                "record {i} field {field} edited, room survived"
            );
        }
    }
    assert!(tried >= 20, "only {tried} edits tried");
}

/// Repeating a record is refused (its counter is spent), and reordering two
/// records either breaks the chain or, when they commute, yields exactly the
/// same room: a reordering cannot produce a state the signers did not.
///
/// Omitting a record can: a snapshot writer can roll a room back, for example
/// by dropping a Leave or Remove, and each remaining record still verifies,
/// because nothing authenticates an omission. That is the same power as
/// swapping in an older snapshot, and is documented as out of scope.
#[tokio::test]
async fn repeating_or_reordering_records_cannot_forge_a_state() {
    let (_d, bus) = common::fresh_bus("chain");
    busy(&bus).await;
    let original = bus
        .signed_room_info(&name("alice"), &room("den"))
        .await
        .unwrap();
    let snap = snapshot_value(&bus);
    let len = snap["rooms"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["name"] == "den")
        .unwrap()["signed"]["journal"]
        .as_array()
        .unwrap()
        .len();
    let (mut broken, mut commuted) = (0, 0);
    for i in 0..len {
        let mut s = snap.clone();
        let rec = den_record(&mut s)["signed"]["journal"][i].clone();
        den_record(&mut s)["signed"]["journal"]
            .as_array_mut()
            .unwrap()
            .insert(i, rec);
        assert!(
            den_gone(&bus_from(&s)).await,
            "duplicated record {i} accepted"
        );
        if i + 1 < len {
            let mut s = snap.clone();
            den_record(&mut s)["signed"]["journal"]
                .as_array_mut()
                .unwrap()
                .swap(i, i + 1);
            let back = bus_from(&s);
            if den_gone(&back).await {
                broken += 1;
            } else {
                let info = back
                    .signed_room_info(&name("alice"), &room("den"))
                    .await
                    .unwrap();
                assert_eq!(
                    info,
                    original,
                    "swapping {i}/{} produced a different room",
                    i + 1
                );
                commuted += 1;
            }
        }
    }
    assert!(broken >= 3, "{broken} swaps broke the chain");
    assert!(commuted >= 1, "expected at least one commuting pair");
}

/// A snapshot that omits the create, or starts with something else, is refused.
#[tokio::test]
async fn a_journal_must_start_with_the_create() {
    let (_d, bus) = common::fresh_bus("nocreate");
    busy(&bus).await;
    let mut s = snapshot_value(&bus);
    den_record(&mut s)["signed"]["journal"]
        .as_array_mut()
        .unwrap()
        .remove(0);
    assert!(den_gone(&bus_from(&s)).await);
    let mut s = snapshot_value(&bus);
    den_record(&mut s)["signed"]["journal"] = json!([]);
    assert!(den_gone(&bus_from(&s)).await);
}

/// Dropping the `signed` section does not turn the room into an unsigned one:
/// there are no unsigned rooms called den with a signed history, and a plain
/// room record that claims a signed room's name is just a plain room that
/// cannot be used to read or write the signed one (the signed one is gone).
#[tokio::test]
async fn stripping_the_signed_section_cannot_downgrade_a_room_to_a_plain_one_with_its_members() {
    let (_d, bus) = common::fresh_bus("strip");
    busy(&bus).await;
    let mut s = snapshot_value(&bus);
    den_record(&mut s)["signed"] = Value::Null;
    let back = bus_from(&s);
    // The retained messages are signed envelopes; as a plain room it would
    // hold them, so the downgrade is visible and the room is plain. The
    // protection against it is that nothing authoritative lives outside the
    // `signed` records for a room that has them: without them it is no signed
    // room, and a daemon operator who strips them has chosen that.
    let err = Actor::new("alice", 1)
        .say(&back, "den", 99, "x")
        .await
        .unwrap_err();
    assert_eq!(
        code(err),
        "room_not_found",
        "signed operations never reach a plain room"
    );
}

/// Adding a member in the cursor list, or raising the epoch, changes nothing:
/// membership and epoch come from the verified records only.
#[tokio::test]
async fn membership_and_epoch_come_only_from_verified_records() {
    let (_d, bus) = common::fresh_bus("authority");
    let (alice, ..) = busy(&bus).await;
    register(&bus, &["mallory"]).await;
    let mut s = snapshot_value(&bus);
    den_record(&mut s)["members"]
        .as_array_mut()
        .unwrap()
        .push(json!({
            "name": "mallory", "cursor": 0, "skipped": 0
        }));
    den_record(&mut s)["invited"]
        .as_array_mut()
        .unwrap()
        .push(json!("mallory"));
    let back = bus_from(&s);
    let info = back
        .signed_room_info(&name("alice"), &room("den"))
        .await
        .unwrap();
    let names: Vec<&str> = info.members.iter().map(|m| m.agent.as_str()).collect();
    assert_eq!(names, ["alice", "bob"]);
    assert_eq!(info.invited, ["carol"]);
    assert_eq!(info.epoch, 2);
    let err = back
        .join_room(&name("mallory"), &room("den"))
        .await
        .unwrap_err();
    assert_eq!(code(err), "room_not_found");
    alice
        .say(&back, "den", 40, "mls/1 commit 2 ee")
        .await
        .unwrap();
}

/// A key binding whose proof does not verify is dropped; the agent stays.
#[tokio::test]
async fn a_key_binding_with_a_bad_proof_is_dropped() {
    let (_d, bus) = common::fresh_bus("badproof");
    busy(&bus).await;
    let mut s = snapshot_value(&bus);
    let agents = s["agents"].as_array_mut().unwrap();
    let alice = agents.iter_mut().find(|a| a["name"] == "alice").unwrap();
    let bob_key = Keypair::from_seed([2; 32]).public().to_hex();
    alice["key"]["public_key"] = json!(bob_key);
    let back = bus_from(&s);
    assert!(back.agent_key(&name("alice")).await.unwrap().is_none());
    assert!(back.agent_key(&name("bob")).await.unwrap().is_some());
}

/// Retained message tampering rejects the room: a forged body, an unknown key,
/// a re-attributed sender.
#[tokio::test]
async fn tampered_retained_messages_reject_the_room() {
    let (_d, bus) = common::fresh_bus("retained");
    busy(&bus).await;
    let snap = snapshot_value(&bus);
    let n = snap["rooms"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["name"] == "den")
        .unwrap()["messages"]
        .as_array()
        .unwrap()
        .len();
    assert!(n >= 3);
    for i in 0..n {
        let mut s = snap.clone();
        let body = den_record(&mut s)["messages"][i]["body"]
            .as_str()
            .unwrap()
            .to_string();
        den_record(&mut s)["messages"][i]["body"] = json!(format!("{body}!"));
        assert!(den_gone(&bus_from(&s)).await, "forged body {i} accepted");
        let mut s = snap.clone();
        let from = den_record(&mut s)["messages"][i]["from"]
            .as_str()
            .unwrap()
            .to_string();
        den_record(&mut s)["messages"][i]["from"] =
            json!(if from == "alice" { "bob" } else { "alice" });
        assert!(
            den_gone(&bus_from(&s)).await,
            "re-attributed message {i} accepted"
        );
    }
    // A message signed by a key the journal never saw.
    let outsider = Keypair::from_seed([77; 32]);
    let op = SignedOp::Send {
        body: "forged".into(),
    };
    let sig = outsider.sign(&op.payload("den", "alice", 999));
    let envelope = Envelope::encode(999, &outsider.public(), &sig, "forged");
    let mut s = snap.clone();
    let mut forged = den_record(&mut s)["messages"][0].clone();
    forged["body"] = json!(envelope);
    forged["from"] = json!("alice");
    forged["id"] = json!(9_999_999);
    den_record(&mut s)["messages"]
        .as_array_mut()
        .unwrap()
        .push(forged);
    assert!(
        den_gone(&bus_from(&s)).await,
        "a message by an unknown key was accepted"
    );
}

/// Lowering or removing a saved watermark cannot reopen counters the verified
/// records already closed.
#[tokio::test]
async fn lowering_a_watermark_cannot_reopen_proven_counters() {
    let (_d, bus) = common::fresh_bus("watermark");
    let (_alice, bob, ..) = busy(&bus).await;
    let mut s = snapshot_value(&bus);
    den_record(&mut s)["signed"]["watermarks"] = json!([]);
    let back = bus_from(&s);
    // Bob's journaled and retained records prove counters up to 3.
    assert_eq!(
        code(bob.say(&back, "den", 3, "replay").await.unwrap_err()),
        "stale_counter"
    );
    assert_eq!(
        code(bob.say(&back, "den", 1, "replay").await.unwrap_err()),
        "stale_counter"
    );
    bob.say(&back, "den", 4, "fresh").await.unwrap();
}

/// A journal that grows past its cap is not saved (the room ends with the
/// process) rather than saved in part.
#[tokio::test]
async fn an_overlong_journal_is_not_saved() {
    let (_d, bus) = common::fresh_bus("overflow");
    let alice = Actor::new("alice", 1);
    alice.enroll(&bus).await;
    let create = SignedOp::Create {
        description: String::new(),
    };
    alice.op(&bus, "den", 1, create).await.unwrap();
    for i in 0..8200u64 {
        // Commits journal; each needs the next epoch.
        alice
            .say(&bus, "den", 2 + i, &format!("mls/1 commit {i} aa"))
            .await
            .unwrap();
    }
    let back = restart(&bus).await;
    assert!(den_gone(&back).await);
}

/// Writes the seeds of the `signed_journal` fuzz target (run by hand:
/// `SEED_DIR=fuzz/corpus/signed_journal cargo test --test signed_persist
/// write_journal_seeds -- --ignored`): a valid record with its retained
/// messages, the same as the tamper tests produce, and each tampered variant.
#[tokio::test]
#[ignore = "writes fuzz seeds; run by hand"]
async fn write_journal_seeds() {
    let dir = std::path::PathBuf::from(std::env::var("SEED_DIR").expect("SEED_DIR"));
    std::fs::create_dir_all(&dir).unwrap();
    let (_d, bus) = common::fresh_bus("seeds");
    // The fuzz target's room is `r`; sign for it.
    let (alice, bob) = (Actor::new("alice", 1), Actor::new("bob", 2));
    for a in [&alice, &bob] {
        a.enroll(&bus).await;
    }
    let create = SignedOp::Create {
        description: "d".into(),
    };
    alice.op(&bus, "r", 1, create).await.unwrap();
    alice.op(&bus, "r", 2, alice.invite(&bob)).await.unwrap();
    bob.op(&bus, "r", 1, SignedOp::Join).await.unwrap();
    alice.say(&bus, "r", 3, "mls/1 commit 0 aa").await.unwrap();
    bob.say(&bus, "r", 2, "plain words").await.unwrap();
    let welcome = SignedOp::Welcome {
        target: "bob".into(),
        welcome: vec![1, 2, 3],
    };
    alice.op(&bus, "r", 4, welcome).await.unwrap();
    bob.say(&bus, "r", 3, "mls/1 commit 1 bb").await.unwrap();
    let snap = snapshot_value(&bus);
    let rec = snap["rooms"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["name"] == "r")
        .unwrap();
    let retained: Vec<Value> = rec["messages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| json!([m["from"], m["body"]]))
        .collect();
    let valid = json!({
        "journal": rec["signed"]["journal"],
        "watermarks": rec["signed"]["watermarks"],
        "retained": retained,
    });
    let write = |name: &str, v: &Value| std::fs::write(dir.join(name), v.to_string()).unwrap();
    write("valid", &valid);
    let mut no_retained = valid.clone();
    no_retained["retained"] = json!([]);
    write("valid_no_retained", &no_retained);
    let mut no_marks = valid.clone();
    no_marks["watermarks"] = json!([]);
    write("valid_no_watermarks", &no_marks);
    let n = valid["journal"].as_array().unwrap().len();
    for i in 0..n {
        let mut bad = valid.clone();
        let sig = bad["journal"][i]["signature"].as_str().unwrap().to_string();
        bad["journal"][i]["signature"] = json!(format!("0{}", &sig[1..]));
        write(&format!("bad_signature_{i}"), &bad);
        let mut dup = valid.clone();
        let r = dup["journal"][i].clone();
        dup["journal"].as_array_mut().unwrap().insert(i, r);
        write(&format!("repeated_{i}"), &dup);
        if i + 1 < n {
            let mut swapped = valid.clone();
            swapped["journal"].as_array_mut().unwrap().swap(i, i + 1);
            write(&format!("swapped_{i}"), &swapped);
        }
        let mut dropped = valid.clone();
        dropped["journal"].as_array_mut().unwrap().remove(i);
        write(&format!("dropped_{i}"), &dropped);
    }
    let mut forged = valid.clone();
    forged["retained"][0][1] = json!("mentischat-signed/1 1 00 00\nx");
    write("retained_malformed", &forged);
}
