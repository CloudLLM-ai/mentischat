//! The replay rules a signed room's journal must satisfy to be restored,
//! driven through `verify_signed_record` with hand-signed journals: one
//! accepted shape, then one rejection per rule, each with its reason. A
//! tampered snapshot must lose the room for exactly the reason the rule names.

use mentischat::bus::verify_signed_record;
use mentischat::signed::{hex_encode, Keypair, SignedOp};
use serde_json::{json, Value};

const ROOM: &str = "den";

struct Actor {
    name: &'static str,
    keys: Keypair,
}

fn actor(name: &'static str, seed: u8) -> Actor {
    Actor {
        name,
        keys: Keypair::from_seed([seed; 32]),
    }
}

impl Actor {
    /// `op` signed by this actor at `counter`, as a journal record.
    fn rec(&self, counter: u64, op: &SignedOp) -> Value {
        let signature = self.keys.sign(&op.payload(ROOM, self.name, counter));
        let mut record = json!({
            "agent": self.name,
            "key": self.keys.public().to_hex(),
            "counter": counter,
            "signature": signature.to_hex(),
            "kind": op.kind().as_str(),
        });
        match op {
            SignedOp::Create { description } => record["description"] = json!(description),
            SignedOp::Invite {
                invitee,
                invitee_key,
            } => {
                record["invitee"] = json!(invitee);
                record["invitee_key"] = json!(invitee_key.to_hex());
            }
            SignedOp::Join | SignedOp::Leave => {}
            SignedOp::Remove { target } => record["target"] = json!(target),
            SignedOp::Welcome { target, welcome } => {
                record["target"] = json!(target);
                record["welcome"] = json!(hex_encode(welcome));
            }
            SignedOp::Send { body } => record["body"] = json!(body),
        }
        record
    }

    fn invite(&self, who: &Actor) -> SignedOp {
        SignedOp::Invite {
            invitee: who.name.into(),
            invitee_key: who.keys.public(),
        }
    }
}

fn create(description: &str) -> SignedOp {
    SignedOp::Create {
        description: description.into(),
    }
}

fn verify(journal: Vec<Value>) -> Result<mentischat::bus::RecordSummary, &'static str> {
    let body = json!({ "journal": journal });
    verify_signed_record(ROOM, body.to_string().as_bytes())
}

fn rejected(journal: Vec<Value>) -> &'static str {
    verify(journal).expect_err("the journal must be rejected")
}

/// A create, an invitation, a join and a Commit replay to the room they
/// describe: members, owner, epoch and the records counted.
#[test]
fn test_a_valid_journal_replays_to_its_room() {
    let (alice, bob) = (actor("alice", 1), actor("bob", 2));
    let commit = SignedOp::Send {
        body: "mls/1 commit 0 abcd".into(),
    };
    let summary = verify(vec![
        alice.rec(1, &create("the den")),
        alice.rec(2, &alice.invite(&bob)),
        bob.rec(1, &SignedOp::Join),
        alice.rec(3, &commit),
    ])
    .expect("replays");
    assert_eq!(summary.members, ["alice", "bob"]);
    assert_eq!(summary.owner, Some(alice.keys.public().to_hex()));
    assert_eq!(summary.epoch, 1);
    assert!(summary.invited.is_empty());
    assert_eq!(summary.records, 4);

    let pending = verify(vec![
        alice.rec(1, &create("")),
        alice.rec(2, &alice.invite(&bob)),
    ])
    .expect("replays");
    assert_eq!(pending.invited, ["bob"]);
}

/// An owner who leaves takes the ownership with them; the room lives on with
/// the others and no owner.
#[test]
fn test_owner_leaving_clears_the_owner() {
    let (alice, bob) = (actor("alice", 1), actor("bob", 2));
    let summary = verify(vec![
        alice.rec(1, &create("")),
        alice.rec(2, &alice.invite(&bob)),
        bob.rec(1, &SignedOp::Join),
        alice.rec(3, &SignedOp::Leave),
    ])
    .expect("replays");
    assert_eq!(summary.members, ["bob"]);
    assert_eq!(summary.owner, None);
}

/// The owner can remove a member and a member's Welcome is kept; both replay.
#[test]
fn test_remove_and_welcome_replay() {
    let (alice, bob) = (actor("alice", 1), actor("bob", 2));
    let welcome = SignedOp::Welcome {
        target: "bob".into(),
        welcome: vec![1, 2, 3],
    };
    let summary = verify(vec![
        alice.rec(1, &create("")),
        alice.rec(2, &alice.invite(&bob)),
        alice.rec(3, &welcome),
        bob.rec(1, &SignedOp::Join),
        alice.rec(
            4,
            &SignedOp::Remove {
                target: "bob".into(),
            },
        ),
    ])
    .expect("replays");
    assert_eq!(summary.members, ["alice"]);
}

/// Each way a journal can contradict the rules of the live daemon has its own
/// rejection reason.
#[test]
fn test_every_replay_rule_rejects_with_its_reason() {
    let (alice, bob, carol) = (actor("alice", 1), actor("bob", 2), actor("carol", 3));
    let first = || alice.rec(1, &create(""));
    let with_bob = || {
        vec![
            first(),
            alice.rec(2, &alice.invite(&bob)),
            bob.rec(1, &SignedOp::Join),
        ]
    };
    let extend = |mut base: Vec<Value>, more: Value| {
        base.push(more);
        base
    };

    assert_eq!(rejected(vec![]), "room has no members");
    assert_eq!(
        rejected(vec![alice.rec(1, &SignedOp::Join)]),
        "first record is not create"
    );
    assert_eq!(
        rejected(vec![first(), alice.rec(2, &create(""))]),
        "create is not first"
    );
    assert_eq!(
        rejected(vec![first(), alice.rec(1, &alice.invite(&bob))]),
        "counter does not increase"
    );
    assert_eq!(
        rejected(vec![first(), carol.rec(1, &carol.invite(&bob))]),
        "invite by a non-member or of a member"
    );
    assert_eq!(
        rejected(extend(with_bob(), alice.rec(3, &alice.invite(&bob)))),
        "invite by a non-member or of a member"
    );
    assert_eq!(
        rejected(vec![first(), carol.rec(1, &SignedOp::Join)]),
        "join without a matching invitation"
    );
    assert_eq!(
        rejected(vec![first(), bob.rec(1, &SignedOp::Leave)]),
        "leave by a non-member"
    );
    let remove_alice = SignedOp::Remove {
        target: "alice".into(),
    };
    assert_eq!(
        rejected(extend(with_bob(), bob.rec(2, &remove_alice))),
        "remove not allowed"
    );
    let remove_self = SignedOp::Remove {
        target: "alice".into(),
    };
    assert_eq!(
        rejected(vec![first(), alice.rec(2, &remove_self)]),
        "remove not allowed"
    );
    let welcome_unknown = SignedOp::Welcome {
        target: "carol".into(),
        welcome: vec![9],
    };
    assert_eq!(
        rejected(vec![first(), alice.rec(2, &welcome_unknown)]),
        "welcome not allowed"
    );
    let welcome_empty = SignedOp::Welcome {
        target: "alice".into(),
        welcome: vec![],
    };
    assert_eq!(
        rejected(vec![first(), alice.rec(2, &welcome_empty)]),
        "welcome not allowed"
    );
    let wrong_epoch = SignedOp::Send {
        body: "mls/1 commit 5 ab".into(),
    };
    assert_eq!(
        rejected(vec![first(), alice.rec(2, &wrong_epoch)]),
        "commit out of chain"
    );
    let not_a_commit = SignedOp::Send {
        body: "hello".into(),
    };
    assert_eq!(
        rejected(vec![first(), alice.rec(2, &not_a_commit)]),
        "commit out of chain"
    );
    assert_eq!(
        rejected(vec![first(), alice.rec(2, &SignedOp::Leave)]),
        "room has no members"
    );
}

/// A record that is damaged rather than rule-breaking is refused before any
/// rule runs: an unparseable key or signature, an unknown kind, a missing
/// field, a name that is not a valid agent name, and a signature that does
/// not belong to the fields.
#[test]
fn test_damaged_records_are_refused() {
    let alice = actor("alice", 1);
    let good = || alice.rec(1, &create("the den"));

    let mut bad_key = good();
    bad_key["key"] = json!("zz");
    assert_eq!(rejected(vec![bad_key]), "malformed record");

    let mut bad_signature = good();
    bad_signature["signature"] = json!("00");
    assert_eq!(rejected(vec![bad_signature]), "malformed record");

    let mut bad_kind = good();
    bad_kind["kind"] = json!("explode");
    assert_eq!(rejected(vec![bad_kind]), "malformed record");

    for unjournaled in [
        "register_key",
        "reclaim",
        "deregister",
        "publish_key_packages",
    ] {
        let mut record = good();
        record["kind"] = json!(unjournaled);
        assert_eq!(rejected(vec![record]), "malformed record", "{unjournaled}");
    }

    let mut missing = good();
    missing.as_object_mut().unwrap().remove("description");
    assert_eq!(rejected(vec![missing]), "malformed record");

    let mut bad_name = good();
    bad_name["agent"] = json!("not a name!");
    assert_eq!(rejected(vec![bad_name]), "bad agent name");

    let mut forged = good();
    forged["description"] = json!("another den");
    assert_eq!(rejected(vec![forged]), "signature does not verify");

    let mut huge = good();
    huge["description"] = json!("x".repeat(100_000));
    assert!(verify(vec![huge]).is_err());
}

/// Retained messages must be signed envelopes by a key in the journal, and
/// the input itself must be the documented JSON.
#[test]
fn test_retained_messages_and_input_shape_are_checked() {
    let alice = actor("alice", 1);
    let journal = vec![alice.rec(1, &create(""))];
    let with = |retained: Value| {
        let body = json!({ "journal": journal, "retained": retained });
        verify_signed_record(ROOM, body.to_string().as_bytes())
    };

    assert!(with(json!([])).is_ok());
    assert_eq!(
        with(json!([["alice", "not an envelope"]])).unwrap_err(),
        "retained message is not an envelope"
    );
    assert_eq!(
        verify_signed_record(ROOM, b"[]").unwrap_err(),
        "not a signed record"
    );
    assert_eq!(
        verify_signed_record(ROOM, b"").unwrap_err(),
        "not a signed record"
    );

    let marks = json!({ "journal": journal, "watermarks": [["not-a-key", 3]] });
    assert_eq!(
        verify_signed_record(ROOM, marks.to_string().as_bytes()).unwrap_err(),
        "bad watermark key"
    );
}

/// A saved watermark above what the journal proves is kept (it only ever
/// raises), so the lowest acceptable counter cannot be rewound by the file.
#[test]
fn test_saved_watermarks_are_accepted_when_well_formed() {
    let alice = actor("alice", 1);
    let body = json!({
        "journal": [alice.rec(1, &create(""))],
        "watermarks": [[alice.keys.public().to_hex(), 40]],
    });
    assert!(verify_signed_record(ROOM, body.to_string().as_bytes()).is_ok());
}

/// A record carrying a field its kind does not sign (here a `body` on a
/// welcome, a `description` on a join) is refused, not silently ignored: a
/// journal that verifies has exactly the bytes that were signed.
#[test]
fn test_stray_fields_on_a_record_are_refused() {
    let (alice, bob) = (actor("alice", 1), actor("bob", 2));
    let base = || {
        vec![
            alice.rec(1, &create("")),
            alice.rec(2, &alice.invite(&bob)),
            bob.rec(1, &SignedOp::Join),
        ]
    };
    assert!(verify(base()).is_ok());
    for (index, field) in [
        (0, "body"),
        (0, "target"),
        (1, "description"),
        (2, "welcome"),
        (2, "invitee_key"),
    ] {
        let mut journal = base();
        journal[index][field] = json!("x");
        assert_eq!(
            rejected(journal),
            "malformed record",
            "{field} on record {index}"
        );
    }
}
