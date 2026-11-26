//! Roles, bans and public signed rooms: admins and participants, who may
//! invite, remove, promote, demote, ban and unban, how a room hands itself to
//! the member seated longest when its last admin goes, and how all of it
//! survives a restart and a replay of the journal.

mod common;

use common::{fresh_bus, idle_after, name, register, room};
use mentischat::bus::{verify_signed_record, Bus, Role, SignedOutcome, SignedRequest};
use mentischat::errors::MailboxError;
use mentischat::signed::{register_key_payload, Keypair, SignedOp};
use std::time::{Duration, UNIX_EPOCH};

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

    fn ban(&self, who: &Actor) -> SignedOp {
        SignedOp::Ban {
            target: who.name.into(),
            target_key: Some(who.keys.public()),
        }
    }

    /// `op` signed at `counter` as a journal record, for the replay checks.
    fn rec(&self, r: &str, counter: u64, op: &SignedOp) -> Value {
        let signature = self.keys.sign(&op.payload(r, self.name, counter));
        let mut record = json!({
            "agent": self.name,
            "key": self.keys.public().to_hex(),
            "counter": counter,
            "signature": signature.to_hex(),
            "kind": op.kind().as_str(),
        });
        match op {
            SignedOp::Create {
                description,
                public,
            } => {
                record["description"] = json!(description);
                if *public {
                    record["public"] = json!(true);
                }
            }
            SignedOp::Invite {
                invitee,
                invitee_key,
            } => {
                record["invitee"] = json!(invitee);
                record["invitee_key"] = json!(invitee_key.to_hex());
            }
            SignedOp::Ban { target, target_key } => {
                record["target"] = json!(target);
                if let Some(key) = target_key {
                    record["target_key"] = json!(key.to_hex());
                }
            }
            op => {
                if let Some(target) = op.target() {
                    record["target"] = json!(target);
                }
            }
        }
        record
    }
}

fn create(public: bool) -> SignedOp {
    SignedOp::Create {
        description: String::new(),
        public,
    }
}

fn target(op: fn(String) -> SignedOp, who: &Actor) -> SignedOp {
    op(who.name.into())
}

fn promote(who: String) -> SignedOp {
    SignedOp::Promote { target: who }
}

fn demote(who: String) -> SignedOp {
    SignedOp::Demote { target: who }
}

fn remove(who: String) -> SignedOp {
    SignedOp::Remove { target: who }
}

fn unban(who: String) -> SignedOp {
    SignedOp::Unban { target: who }
}

fn code(e: MailboxError) -> &'static str {
    e.code()
}

/// Four enrolled agents; alice founds the private room `den` and bob and carol
/// join it, in that order. Counters used: alice 1..=3, bob 1, carol 1.
async fn den(bus: &Bus) -> (Actor, Actor, Actor, Actor) {
    let (alice, bob, carol, dave) = (
        Actor::new("alice", 1),
        Actor::new("bob", 2),
        Actor::new("carol", 3),
        Actor::new("dave", 4),
    );
    for a in [&alice, &bob, &carol, &dave] {
        a.enroll(bus).await;
    }
    alice.op(bus, "den", 1, create(false)).await.unwrap();
    alice.op(bus, "den", 2, alice.invite(&bob)).await.unwrap();
    bob.op(bus, "den", 1, SignedOp::Join).await.unwrap();
    alice.op(bus, "den", 3, alice.invite(&carol)).await.unwrap();
    carol.op(bus, "den", 1, SignedOp::Join).await.unwrap();
    (alice, bob, carol, dave)
}

async fn roles(bus: &Bus, asker: &str, r: &str) -> Vec<(String, Role)> {
    let info = bus.signed_room_info(&name(asker), &room(r)).await.unwrap();
    info.members
        .into_iter()
        .map(|m| (m.agent, m.role))
        .collect()
}

fn pairs(list: &[(&str, Role)]) -> Vec<(String, Role)> {
    list.iter().map(|(n, r)| (n.to_string(), *r)).collect()
}

/// The founder is the only admin; everyone who joins is a participant.
#[tokio::test]
async fn the_founder_is_admin_and_joiners_are_participants() {
    let (_d, bus) = fresh_bus("roles-founder");
    den(&bus).await;
    assert_eq!(
        roles(&bus, "bob", "den").await,
        pairs(&[
            ("alice", Role::Admin),
            ("bob", Role::Participant),
            ("carol", Role::Participant),
        ])
    );
    let info = bus
        .signed_room_info(&name("carol"), &room("den"))
        .await
        .unwrap();
    assert_eq!(info.admins, ["alice"]);
    assert!(!info.public);
}

/// A participant cannot invite, remove, promote, demote, ban or unban; once
/// promoted it can, and demoted it cannot again.
#[tokio::test]
async fn only_admins_manage_the_room() {
    let (_d, bus) = fresh_bus("roles-manage");
    let (alice, bob, carol, dave) = den(&bus).await;
    let refusals = [
        bob.invite(&dave),
        target(remove, &carol),
        target(promote, &carol),
        target(demote, &alice),
        bob.ban(&carol),
        target(unban, &dave),
    ];
    for (i, op) in refusals.into_iter().enumerate() {
        let err = bob.op(&bus, "den", 2 + i as u64, op).await.unwrap_err();
        assert_eq!(code(err), "not_an_admin");
    }
    // A refused operation spends no counter.
    alice
        .op(&bus, "den", 4, target(promote, &bob))
        .await
        .unwrap();
    bob.op(&bus, "den", 2, bob.invite(&dave)).await.unwrap();
    dave.op(&bus, "den", 1, SignedOp::Join).await.unwrap();
    assert_eq!(
        bus.signed_room_info(&name("dave"), &room("den"))
            .await
            .unwrap()
            .admins,
        ["alice", "bob"]
    );

    alice
        .op(&bus, "den", 5, target(demote, &bob))
        .await
        .unwrap();
    let err = bob
        .op(&bus, "den", 3, target(remove, &dave))
        .await
        .unwrap_err();
    assert_eq!(code(err), "not_an_admin");
}

/// Promote takes members only; demote takes admins only.
#[tokio::test]
async fn promote_and_demote_need_the_right_target() {
    let (_d, bus) = fresh_bus("roles-targets");
    let (alice, bob, _carol, dave) = den(&bus).await;
    let err = alice
        .op(&bus, "den", 4, target(promote, &dave))
        .await
        .unwrap_err();
    assert_eq!(code(err), "not_a_member", "dave is not in the room");
    let err = alice
        .op(&bus, "den", 4, target(demote, &bob))
        .await
        .unwrap_err();
    assert_eq!(code(err), "not_a_member", "bob is not an admin");
}

/// When no member is an admin, the member seated longest is: alice leaves, so
/// bob (who joined before carol) runs the room, and can manage it.
#[tokio::test]
async fn the_member_seated_longest_inherits_the_room() {
    let (_d, bus) = fresh_bus("roles-inherit");
    let (alice, bob, carol, dave) = den(&bus).await;
    alice.op(&bus, "den", 4, SignedOp::Leave).await.unwrap();
    assert_eq!(
        roles(&bus, "carol", "den").await,
        pairs(&[("bob", Role::Admin), ("carol", Role::Participant)])
    );
    bob.op(&bus, "den", 2, bob.invite(&dave)).await.unwrap();

    // An admin that demotes itself while it is the only explicit admin hands
    // the room back to the rule, which picks the same longest-seated member.
    bob.op(&bus, "den", 3, target(promote, &carol))
        .await
        .unwrap();
    assert_eq!(
        roles(&bus, "carol", "den").await,
        pairs(&[("bob", Role::Participant), ("carol", Role::Admin)]),
        "an explicit admin outranks the seating rule"
    );
    carol
        .op(&bus, "den", 2, target(demote, &carol))
        .await
        .unwrap();
    assert_eq!(
        roles(&bus, "carol", "den").await,
        pairs(&[("bob", Role::Admin), ("carol", Role::Participant)])
    );
}

/// A ban removes a member, keeps it from rejoining (by name and by key), and
/// an unban lifts it.
#[tokio::test]
async fn a_ban_removes_and_keeps_out_until_lifted() {
    let (_d, bus) = fresh_bus("roles-ban");
    let (alice, bob, carol, _dave) = den(&bus).await;
    alice.op(&bus, "den", 4, alice.ban(&bob)).await.unwrap();
    assert_eq!(
        code(bob.say(&bus, "den", 2, "still here?").await.unwrap_err()),
        "room_not_found"
    );
    let info = bus
        .signed_room_info(&name("carol"), &room("den"))
        .await
        .unwrap();
    assert_eq!(info.banned.len(), 1);
    assert_eq!(info.banned[0].agent, "bob");
    assert_eq!(info.banned[0].public_key, Some(bob.keys.public().to_hex()));
    let err = alice
        .op(&bus, "den", 5, alice.invite(&bob))
        .await
        .unwrap_err();
    assert_eq!(code(err), "banned");

    // An admin cannot ban itself, and a ban must name the target's real key.
    let err = alice
        .op(&bus, "den", 5, alice.ban(&alice))
        .await
        .unwrap_err();
    assert_eq!(code(err), "not_a_member");
    let wrong = SignedOp::Ban {
        target: "carol".into(),
        target_key: Some(bob.keys.public()),
    };
    assert_eq!(
        code(alice.op(&bus, "den", 5, wrong).await.unwrap_err()),
        "key_conflict"
    );

    alice.op(&bus, "den", 5, target(unban, &bob)).await.unwrap();
    alice.op(&bus, "den", 6, alice.invite(&bob)).await.unwrap();
    bob.op(&bus, "den", 2, SignedOp::Join).await.unwrap();
    carol.say(&bus, "den", 2, "welcome back").await.unwrap();
    let err = alice
        .op(&bus, "den", 7, target(unban, &bob))
        .await
        .unwrap_err();
    assert_eq!(code(err), "not_a_member", "bob is no longer banned");
}

/// A name can be banned before it ever comes near the room.
#[tokio::test]
async fn a_name_can_be_banned_ahead_of_time() {
    let (_d, bus) = fresh_bus("roles-preban");
    let alice = Actor::new("alice", 1);
    alice.enroll(&bus).await;
    alice.op(&bus, "lobby", 1, create(true)).await.unwrap();
    let ghost = SignedOp::Ban {
        target: "ghost".into(),
        target_key: None,
    };
    alice.op(&bus, "lobby", 2, ghost).await.unwrap();
    let ghost = Actor::new("ghost", 9);
    ghost.enroll(&bus).await;
    assert_eq!(
        code(
            ghost
                .op(&bus, "lobby", 1, SignedOp::Join)
                .await
                .unwrap_err()
        ),
        "banned"
    );
}

/// A public room is listed for everyone, any agent with a key joins without an
/// invitation, and the plain tools are told to sign.
#[tokio::test]
async fn a_public_room_is_listed_and_open_to_keyed_agents() {
    let (_d, bus) = fresh_bus("roles-public");
    let (alice, bob) = (Actor::new("alice", 1), Actor::new("bob", 2));
    alice.enroll(&bus).await;
    bob.enroll(&bus).await;
    register(&bus, &["keyless"]).await;
    alice.op(&bus, "lobby", 1, create(true)).await.unwrap();

    let listing = bus.list_rooms(&name("bob")).await.unwrap();
    assert_eq!(listing.rooms.len(), 1);
    assert!(!listing.rooms[0].private);

    let err = bus
        .join_room(&name("keyless"), &room("lobby"))
        .await
        .unwrap_err();
    assert_eq!(code(err), "signature_required");

    bob.op(&bus, "lobby", 1, SignedOp::Join).await.unwrap();
    bob.say(&bus, "lobby", 2, "hi all").await.unwrap();
    let info = bus
        .signed_room_info(&name("alice"), &room("lobby"))
        .await
        .unwrap();
    assert!(info.public);
    assert_eq!(
        roles(&bus, "alice", "lobby").await,
        pairs(&[("alice", Role::Admin), ("bob", Role::Participant)])
    );

    // Banned, bob is out and cannot come back by himself.
    alice.op(&bus, "lobby", 2, alice.ban(&bob)).await.unwrap();
    assert_eq!(
        code(bob.op(&bus, "lobby", 3, SignedOp::Join).await.unwrap_err()),
        "banned"
    );
}

/// A private room still hides itself from a banned stranger.
#[tokio::test]
async fn a_private_room_does_not_tell_a_banned_stranger_it_exists() {
    let (_d, bus) = fresh_bus("roles-hidden");
    let (alice, _bob, _carol, dave) = den(&bus).await;
    alice.op(&bus, "den", 4, alice.ban(&dave)).await.unwrap();
    assert_eq!(
        code(dave.op(&bus, "den", 1, SignedOp::Join).await.unwrap_err()),
        "room_not_found"
    );
}

/// Roles, bans and the public flag come back from a snapshot, rebuilt from
/// the verified journal.
#[tokio::test]
async fn roles_bans_and_visibility_survive_a_restart() {
    let (_d, bus) = fresh_bus("roles-restart");
    let (alice, _bob, carol, dave) = den(&bus).await;
    alice
        .op(&bus, "den", 4, target(promote, &carol))
        .await
        .unwrap();
    alice.op(&bus, "den", 5, alice.ban(&dave)).await.unwrap();
    alice.op(&bus, "lobby", 6, create(true)).await.unwrap();

    let back = Bus::from_snapshot_bytes(&bus.snapshot_bytes()).expect("restores");
    assert_eq!(
        roles(&back, "bob", "den").await,
        pairs(&[
            ("alice", Role::Admin),
            ("bob", Role::Participant),
            ("carol", Role::Admin),
        ])
    );
    let info = back
        .signed_room_info(&name("bob"), &room("den"))
        .await
        .unwrap();
    assert_eq!(info.banned.len(), 1);
    assert!(!info.public);
    let lobby = back
        .signed_room_info(&name("alice"), &room("lobby"))
        .await
        .unwrap();
    assert!(lobby.public);
    let listing = back.list_rooms(&name("dave")).await.unwrap();
    assert!(listing
        .rooms
        .iter()
        .any(|r| r.room == "lobby" && !r.private));
}

/// A member dropped for being idle is journaled as gone, so it can join a
/// public room again and the room still replays with it in it.
#[tokio::test]
async fn an_idle_member_can_rejoin_and_the_room_still_restores() {
    let (_d, bus, clock) = common::bus_with_clock("roles-idle", idle_after(60));
    let (alice, bob) = (Actor::new("alice", 1), Actor::new("bob", 2));
    alice.enroll(&bus).await;
    bob.enroll(&bus).await;
    alice.op(&bus, "lobby", 1, create(true)).await.unwrap();
    bob.op(&bus, "lobby", 1, SignedOp::Join).await.unwrap();
    // Only alice stays active; bob goes past the idle limit.
    clock.advance(Duration::from_secs(30));
    alice.say(&bus, "lobby", 2, "still here").await.unwrap();
    clock.advance(Duration::from_secs(40));
    bus.sweep_at(UNIX_EPOCH + Duration::from_millis(clock.now_ms()))
        .await
        .unwrap();
    assert_eq!(
        roles(&bus, "alice", "lobby").await.len(),
        1,
        "bob was dropped"
    );

    bob.enroll_again(&bus).await;
    bob.op(&bus, "lobby", 2, SignedOp::Join).await.unwrap();
    let back = Bus::from_snapshot_bytes(&bus.snapshot_bytes()).expect("restores");
    assert_eq!(
        roles(&back, "alice", "lobby").await,
        pairs(&[("alice", Role::Admin), ("bob", Role::Participant)]),
        "the rejoined member replays"
    );
}

/// The journal replays roles, bans and public joins; a record that breaks a
/// rule is refused.
#[test]
fn the_journal_replays_roles_and_bans() {
    let (alice, bob, carol) = (
        Actor::new("alice", 1),
        Actor::new("bob", 2),
        Actor::new("carol", 3),
    );
    let verify = |records: Vec<Value>| {
        verify_signed_record(
            "lobby",
            &serde_json::to_vec(&json!({ "journal": records })).unwrap(),
        )
    };
    let summary = verify(vec![
        alice.rec("lobby", 1, &create(true)),
        bob.rec("lobby", 1, &SignedOp::Join),
        carol.rec("lobby", 1, &SignedOp::Join),
        alice.rec("lobby", 2, &target(promote, &bob)),
        alice.rec("lobby", 3, &alice.ban(&carol)),
        alice.rec("lobby", 4, &SignedOp::Leave),
    ])
    .expect("replays");
    assert!(summary.public);
    assert_eq!(summary.members, ["bob"]);
    assert_eq!(summary.admins, ["bob"]);
    assert_eq!(summary.banned, ["carol"]);

    let reason = |records| verify(records).unwrap_err();
    assert_eq!(
        reason(vec![
            alice.rec("lobby", 1, &create(false)),
            bob.rec("lobby", 1, &SignedOp::Join),
        ]),
        "join without a matching invitation",
        "a private room needs an invitation"
    );
    assert_eq!(
        reason(vec![
            alice.rec("lobby", 1, &create(true)),
            alice.rec("lobby", 2, &alice.ban(&bob)),
            bob.rec("lobby", 1, &SignedOp::Join),
        ]),
        "join without a matching invitation",
        "a banned agent cannot join"
    );
    assert_eq!(
        reason(vec![
            alice.rec("lobby", 1, &create(true)),
            bob.rec("lobby", 1, &SignedOp::Join),
            bob.rec("lobby", 2, &target(promote, &bob)),
        ]),
        "admin operation not allowed",
        "a participant cannot promote itself"
    );

    // A record claiming the room is not public is not one this daemon writes.
    let mut tampered = alice.rec("lobby", 1, &create(false));
    tampered["public"] = json!(false);
    assert_eq!(reason(vec![tampered]), "malformed record");
}

/// Turning a private room public in the snapshot breaks the `create`
/// signature, so the room is dropped rather than opened.
#[tokio::test]
async fn a_private_room_cannot_be_made_public_by_editing_the_snapshot() {
    let (_d, bus) = fresh_bus("roles-tamper");
    den(&bus).await;
    let mut snapshot: Value = serde_json::from_slice(&bus.snapshot_bytes()).unwrap();
    let den = snapshot["rooms"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|r| r["name"] == "den")
        .unwrap();
    den["private"] = json!(false);
    den["signed"]["journal"][0]["public"] = json!(true);
    let back = Bus::from_snapshot_bytes(&serde_json::to_vec(&snapshot).unwrap()).unwrap();
    assert_eq!(
        code(
            back.signed_room_info(&name("alice"), &room("den"))
                .await
                .unwrap_err()
        ),
        "room_not_found"
    );
}

impl Actor {
    /// Register again after being dropped: a key-bound name comes back only
    /// with a reclaim proven by the key.
    async fn enroll_again(&self, bus: &Bus) {
        bus.register_with(
            &name(self.name),
            common::proven_reclaim(&self.keys, self.name, 10),
        )
        .await
        .unwrap();
        // Binding the same key again is idempotent, and needed when the bus
        // keeps no key registry across the drop.
        let key = self.keys.public();
        let sig = self.keys.sign(&register_key_payload(self.name, &key));
        bus.register_key(&name(self.name), key, &sig).await.unwrap();
    }
}

/// A participant in a public room sends at most a minute's allowance; admins
/// and private rooms are not limited, and the window slides.
#[tokio::test]
async fn public_rooms_limit_how_fast_a_participant_sends() {
    use mentischat::bus::PUBLIC_SENDS_PER_MINUTE;
    let (_d, bus, clock) = common::bus_with_clock("roles-rate", Default::default());
    let (alice, bob) = (Actor::new("alice", 1), Actor::new("bob", 2));
    alice.enroll(&bus).await;
    bob.enroll(&bus).await;
    alice.op(&bus, "lobby", 1, create(true)).await.unwrap();
    bob.op(&bus, "lobby", 1, SignedOp::Join).await.unwrap();
    let mut counter = 1;
    for _ in 0..PUBLIC_SENDS_PER_MINUTE {
        counter += 1;
        bob.say(&bus, "lobby", counter, "chatter").await.unwrap();
    }
    counter += 1;
    let err = bob
        .say(&bus, "lobby", counter, "one too many")
        .await
        .unwrap_err();
    assert!(matches!(err, MailboxError::RateLimited { retry_after_ms } if retry_after_ms > 0));
    // The refused send spent no counter, and the admin is not limited.
    for i in 0..=PUBLIC_SENDS_PER_MINUTE as u64 {
        alice.say(&bus, "lobby", 2 + i, "admin").await.unwrap();
    }
    clock.advance(Duration::from_secs(61));
    bob.say(&bus, "lobby", counter, "later").await.unwrap();

    // A private room has no limit.
    alice.op(&bus, "den", 1, create(false)).await.unwrap();
    alice.op(&bus, "den", 2, alice.invite(&bob)).await.unwrap();
    bob.op(&bus, "den", 1, SignedOp::Join).await.unwrap();
    for c in 2..(PUBLIC_SENDS_PER_MINUTE as u64 + 5) {
        bob.say(&bus, "den", c, "private").await.unwrap();
    }
}

/// One key cannot churn in and out of a public room.
#[tokio::test]
async fn public_rooms_limit_how_often_a_key_joins() {
    use mentischat::bus::PUBLIC_JOINS_PER_KEY;
    let (_d, bus, clock) = common::bus_with_clock("roles-churn", Default::default());
    let (alice, bob) = (Actor::new("alice", 1), Actor::new("bob", 2));
    alice.enroll(&bus).await;
    bob.enroll(&bus).await;
    alice.op(&bus, "lobby", 1, create(true)).await.unwrap();
    let mut counter = 0;
    for _ in 0..PUBLIC_JOINS_PER_KEY {
        bob.op(&bus, "lobby", counter + 1, SignedOp::Join)
            .await
            .unwrap();
        bob.op(&bus, "lobby", counter + 2, SignedOp::Leave)
            .await
            .unwrap();
        counter += 2;
    }
    let err = bob
        .op(&bus, "lobby", counter + 1, SignedOp::Join)
        .await
        .unwrap_err();
    assert_eq!(code(err), "rate_limited");
    clock.advance(Duration::from_secs(61));
    bob.op(&bus, "lobby", counter + 1, SignedOp::Join)
        .await
        .unwrap();
}
