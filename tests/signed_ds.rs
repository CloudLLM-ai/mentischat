//! The daemon as an MLS Delivery Service for signed rooms: the KeyPackage
//! directory, Welcome hand-off to invitees, and one Commit per epoch. The
//! daemon treats every MLS byte as opaque, so these tests use plain bytes.

mod common;

use common::{fresh_bus, name, register, room};
use mentischat::bus::{Bus, SignedOutcome, SignedRequest, MAX_KEY_PACKAGES, MAX_KEY_PACKAGE_BYTES};
use mentischat::errors::MailboxError;
use mentischat::signed::{
    key_packages_payload, mls_commit_epoch, register_key_payload, Envelope, Keypair, SignedOp,
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
        room_name: &str,
        counter: u64,
        op: SignedOp,
    ) -> Result<SignedOutcome, MailboxError> {
        let signature = self.keys.sign(&op.payload(room_name, self.name, counter));
        bus.signed_op(SignedRequest {
            agent: name(self.name),
            room: room(room_name),
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

    async fn publish(
        &self,
        bus: &Bus,
        counter: u64,
        packages: Vec<Vec<u8>>,
    ) -> Result<usize, MailboxError> {
        let sig = self
            .keys
            .sign(&key_packages_payload(self.name, counter, &packages, None));
        bus.publish_key_packages(&name(self.name), counter, packages, None, &sig)
            .await
            .map(|s| s.available)
    }
}

/// `alice` owns `den`; `bob` is invited (key pinned) but has not joined.
async fn den_with_invited_bob(bus: &Bus) -> (Actor, Actor) {
    let (alice, bob) = (Actor::new("alice", 1), Actor::new("bob", 2));
    alice.enroll(bus).await;
    bob.enroll(bus).await;
    let create = SignedOp::Create {
        description: String::new(),
    };
    alice.op(bus, "den", 1, create).await.unwrap();
    let invite = SignedOp::Invite {
        invitee: "bob".into(),
        invitee_key: bob.keys.public(),
    };
    alice.op(bus, "den", 2, invite).await.unwrap();
    (alice, bob)
}

/// The room's epoch as its owner sees it.
async fn epoch(bus: &Bus) -> u64 {
    let info = bus.signed_room_info(&name("alice"), &room("den")).await;
    info.unwrap().epoch
}

/// A KeyPackage request by the registered agent `req` (registered on first use).
async fn take(
    bus: &Bus,
    agent: &mentischat::names::AgentName,
) -> Result<mentischat::bus::KeyPackageOffer, MailboxError> {
    let req = name("req");
    let _ = bus.register(&req).await;
    bus.take_key_package(agent, &req).await
}

fn code(e: MailboxError) -> &'static str {
    e.code()
}

/// The Commit header parses only in its exact shape.
#[test]
fn commit_epochs_parse_only_from_the_exact_header() {
    assert_eq!(mls_commit_epoch("mls/1 commit 0 ab"), Some(0));
    assert_eq!(
        mls_commit_epoch("mls/1 commit 18446744073709551615 ab"),
        Some(u64::MAX)
    );
    for not_a_commit in [
        "",
        "mls/1 commit",
        "mls/1 commit ",
        "mls/1 commit  ab",
        "mls/1 commit 7",
        "mls/1 commit -1 ab",
        "mls/1 commit 1x ab",
        "mls/1 commit 01 ab",
        "mls/1 commit 00 ab",
        "mls/1 commit 18446744073709551616 ab",
        "mls/1 app 7 ab",
        " mls/1 commit 7 ab",
        "MLS/1 commit 7 ab",
    ] {
        assert_eq!(mls_commit_epoch(not_a_commit), None, "{not_a_commit:?}");
    }
}

/// KeyPackages come out oldest first, once each, and an empty directory says so.
#[tokio::test]
async fn key_packages_are_handed_out_once_oldest_first() {
    let (_d, bus) = fresh_bus("kp");
    let alice = Actor::new("alice", 1);
    alice.enroll(&bus).await;
    assert_eq!(
        alice
            .publish(&bus, 1, vec![vec![1], vec![2]])
            .await
            .unwrap(),
        2
    );
    assert_eq!(alice.publish(&bus, 2, vec![vec![3]]).await.unwrap(), 3);
    let a = name("alice");
    for want in [vec![1], vec![2], vec![3]] {
        assert_eq!(take(&bus, &a).await.unwrap().package, want);
    }
    assert_eq!(code(take(&bus, &a).await.unwrap_err()), "no_key_package");
    assert_eq!(
        code(take(&bus, &name("ghost")).await.unwrap_err()),
        "unregistered"
    );
}

/// Publishing is signed, replay-protected and bounded.
#[tokio::test]
async fn publishing_key_packages_needs_a_key_a_signature_and_a_fresh_counter() {
    let (_d, bus) = fresh_bus("kpauth");
    register(&bus, &["keyless"]).await;
    let keyless = Actor::new("keyless", 9);
    assert_eq!(
        code(keyless.publish(&bus, 1, vec![vec![1]]).await.unwrap_err()),
        "key_required"
    );

    let alice = Actor::new("alice", 1);
    alice.enroll(&bus).await;
    let forged = Actor::new("alice", 77);
    assert_eq!(
        code(forged.publish(&bus, 1, vec![vec![1]]).await.unwrap_err()),
        "invalid_signature"
    );
    alice.publish(&bus, 5, vec![vec![1]]).await.unwrap();
    let err = alice.publish(&bus, 5, vec![vec![2]]).await.unwrap_err();
    assert!(
        matches!(err, MailboxError::StaleCounter { last: 5, .. }),
        "{err:?}"
    );
    assert_eq!(
        code(alice.publish(&bus, 6, vec![]).await.unwrap_err()),
        "invalid_body"
    );
    let big = vec![0u8; MAX_KEY_PACKAGE_BYTES + 1];
    assert_eq!(
        code(alice.publish(&bus, 6, vec![big]).await.unwrap_err()),
        "invalid_body"
    );
    let many = vec![vec![1u8]; MAX_KEY_PACKAGES + 1];
    assert_eq!(
        code(alice.publish(&bus, 6, many).await.unwrap_err()),
        "invalid_body"
    );
    assert_eq!(
        code(alice.publish(&bus, 6, vec![vec![]]).await.unwrap_err()),
        "invalid_body"
    );
}

/// A full directory drops its oldest package for the newest.
#[tokio::test]
async fn the_key_package_directory_keeps_the_newest() {
    let (_d, bus) = fresh_bus("kpcap");
    let alice = Actor::new("alice", 1);
    alice.enroll(&bus).await;
    let first: Vec<Vec<u8>> = (0..MAX_KEY_PACKAGES as u8).map(|i| vec![i]).collect();
    alice.publish(&bus, 1, first).await.unwrap();
    let available = alice.publish(&bus, 2, vec![vec![200]]).await.unwrap();
    assert_eq!(available, MAX_KEY_PACKAGES);
    assert_eq!(
        take(&bus, &name("alice")).await.unwrap().package,
        vec![1],
        "0 was dropped"
    );
}

/// The inviter leaves a Welcome that only the invitee can read, until it joins.
#[tokio::test]
async fn a_welcome_reaches_only_the_invitee_and_goes_away_on_join() {
    let (_d, bus) = fresh_bus("welcome");
    let (alice, bob) = den_with_invited_bob(&bus).await;
    let store = |bytes: Vec<u8>| SignedOp::Welcome {
        target: "bob".into(),
        welcome: bytes,
    };
    assert_eq!(
        bus.signed_welcome(&name("bob"), &room("den"))
            .await
            .unwrap(),
        None,
        "nothing waiting yet"
    );
    alice
        .op(&bus, "den", 3, store(vec![9, 9, 9]))
        .await
        .unwrap();
    assert_eq!(
        bus.signed_welcome(&name("bob"), &room("den"))
            .await
            .unwrap(),
        Some(vec![9, 9, 9])
    );
    // Alice cannot read it back (she is a member, not an invitee) and neither can a stranger.
    register(&bus, &["eve"]).await;
    for who in ["alice", "eve"] {
        let err = bus
            .signed_welcome(&name(who), &room("den"))
            .await
            .unwrap_err();
        assert_eq!(code(err), "room_not_found", "{who}");
    }
    // A newer Welcome replaces the older.
    alice.op(&bus, "den", 4, store(vec![7])).await.unwrap();
    assert_eq!(
        bus.signed_welcome(&name("bob"), &room("den"))
            .await
            .unwrap(),
        Some(vec![7])
    );
    bob.op(&bus, "den", 1, SignedOp::Join).await.unwrap();
    let err = bus
        .signed_welcome(&name("bob"), &room("den"))
        .await
        .unwrap_err();
    assert_eq!(code(err), "room_not_found", "a member needs no Welcome");
}

/// Only a member may leave a Welcome, only for an invitee, and within bounds.
#[tokio::test]
async fn welcomes_are_for_invitees_from_members_and_bounded() {
    let (_d, bus) = fresh_bus("welcomeauth");
    let (alice, bob) = den_with_invited_bob(&bus).await;
    let eve = Actor::new("eve", 5);
    eve.enroll(&bus).await;
    let welcome = |who: &str, bytes: Vec<u8>| SignedOp::Welcome {
        target: who.into(),
        welcome: bytes,
    };
    let err = eve
        .op(&bus, "den", 1, welcome("bob", vec![1]))
        .await
        .unwrap_err();
    assert_eq!(code(err), "room_not_found", "a stranger cannot");
    let err = bob
        .op(&bus, "den", 1, welcome("bob", vec![1]))
        .await
        .unwrap_err();
    assert_eq!(code(err), "not_a_member", "an invitee cannot write its own");
    let err = alice
        .op(&bus, "den", 3, welcome("eve", vec![1]))
        .await
        .unwrap_err();
    assert_eq!(code(err), "not_a_member", "eve was never invited");
    let err = alice
        .op(&bus, "den", 3, welcome("bob", vec![]))
        .await
        .unwrap_err();
    assert_eq!(code(err), "invalid_body");
    let huge = vec![0u8; mentischat::bus::MAX_WELCOME_BYTES + 1];
    let err = alice
        .op(&bus, "den", 3, welcome("bob", huge))
        .await
        .unwrap_err();
    assert_eq!(code(err), "invalid_body");
    alice
        .op(&bus, "den", 3, welcome("bob", vec![1]))
        .await
        .unwrap();
}

/// Revoking an invitation removes its Welcome too.
#[tokio::test]
async fn revoking_an_invitation_drops_its_welcome() {
    let (_d, bus) = fresh_bus("welcomerevoke");
    let (alice, _bob) = den_with_invited_bob(&bus).await;
    let welcome = SignedOp::Welcome {
        target: "bob".into(),
        welcome: vec![1],
    };
    alice.op(&bus, "den", 3, welcome).await.unwrap();
    alice
        .op(
            &bus,
            "den",
            4,
            SignedOp::Remove {
                target: "bob".into(),
            },
        )
        .await
        .unwrap();
    let err = bus
        .signed_welcome(&name("bob"), &room("den"))
        .await
        .unwrap_err();
    assert_eq!(code(err), "room_not_found");
}

/// One Commit per epoch: the first names epoch 0, a racing second one is
/// stale, and the next must name epoch 1. Other messages are unaffected.
#[tokio::test]
async fn the_room_accepts_one_commit_per_epoch() {
    let (_d, bus) = fresh_bus("epoch");
    let (alice, bob) = den_with_invited_bob(&bus).await;
    bob.op(&bus, "den", 1, SignedOp::Join).await.unwrap();
    assert_eq!(epoch(&bus).await, 0);

    alice
        .say(&bus, "den", 3, "mls/1 commit 0 aa")
        .await
        .unwrap();
    assert_eq!(epoch(&bus).await, 1);
    // Bob raced alice from epoch 0 and lost.
    let err = bob
        .say(&bus, "den", 2, "mls/1 commit 0 bb")
        .await
        .unwrap_err();
    assert!(
        matches!(err, MailboxError::StaleEpoch { epoch: 1, .. }),
        "{err:?}"
    );
    // The loss changed nothing: the epoch stands and bob's counter was not burned.
    assert_eq!(epoch(&bus).await, 1);
    bob.say(&bus, "den", 2, "mls/1 commit 1 cc").await.unwrap();
    assert_eq!(epoch(&bus).await, 2);
    // Application messages and malformed Commit headers never move the epoch.
    alice.say(&bus, "den", 4, "mls/1 app 2 dd").await.unwrap();
    alice
        .say(&bus, "den", 5, "mls/1 commit x ee")
        .await
        .unwrap();
    alice.say(&bus, "den", 6, "plain text").await.unwrap();
    assert_eq!(epoch(&bus).await, 2);
}

/// A Commit that cannot be stored does not advance the epoch.
#[tokio::test]
async fn a_rejected_commit_does_not_advance_the_epoch() {
    let (_d, bus) = fresh_bus("epochreject");
    let (alice, _bob) = den_with_invited_bob(&bus).await;
    // Blank body: rejected as invalid_body, before the epoch is looked at.
    assert_eq!(
        code(alice.say(&bus, "den", 3, "   ").await.unwrap_err()),
        "invalid_body"
    );
    // A stale counter on a Commit for the right epoch is refused and leaves the epoch alone.
    let err = alice
        .say(&bus, "den", 2, "mls/1 commit 0 aa")
        .await
        .unwrap_err();
    assert_eq!(code(err), "stale_counter");
    let info = bus
        .signed_room_info(&name("alice"), &room("den"))
        .await
        .unwrap();
    assert_eq!(info.epoch, 0);
}

/// Racing Commits for one epoch: exactly one lands, whatever the schedule.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn racing_commits_for_one_epoch_land_once() {
    let (_d, bus) = fresh_bus("epochrace");
    let (alice, bob) = den_with_invited_bob(&bus).await;
    bob.op(&bus, "den", 1, SignedOp::Join).await.unwrap();
    let mut tasks = Vec::new();
    for i in 0..8u64 {
        let bus = bus.clone();
        let (who, counter) = if i % 2 == 0 {
            (&alice, 10 + i)
        } else {
            (&bob, 10 + i)
        };
        let op = SignedOp::Send {
            body: format!("mls/1 commit 0 {i:02x}"),
        };
        let signature = who.keys.sign(&op.payload("den", who.name, counter));
        let req = SignedRequest {
            agent: name(who.name),
            room: room("den"),
            counter,
            op,
            signature,
        };
        tasks.push(tokio::spawn(
            async move { bus.signed_op(req).await.is_ok() },
        ));
    }
    let mut ok = 0;
    for t in tasks {
        ok += usize::from(t.await.unwrap());
    }
    assert_eq!(ok, 1);
    let info = bus
        .signed_room_info(&name("alice"), &room("den"))
        .await
        .unwrap();
    assert_eq!(info.epoch, 1);
    let page = bus
        .room_history(&name("alice"), &room("den"), None)
        .await
        .unwrap();
    let commits = page
        .messages
        .iter()
        .filter(|m| Envelope::parse(&m.body).is_ok_and(|e| mls_commit_epoch(e.body).is_some()))
        .count();
    assert_eq!(commits, 1);
}

/// Publish `packages` and a last-resort package for `alice`.
async fn publish_with_last_resort(bus: &Bus, alice: &Actor, counter: u64, last: &[u8]) {
    let packages = vec![vec![1u8], vec![2u8]];
    let sig = alice.keys.sign(&key_packages_payload(
        alice.name,
        counter,
        &packages,
        Some(last),
    ));
    let stock = bus
        .publish_key_packages(
            &name(alice.name),
            counter,
            packages,
            Some(last.to_vec()),
            &sig,
        )
        .await
        .unwrap();
    assert_eq!((stock.available, stock.last_resort), (2, true));
}

/// Once the ordinary packages are gone the last-resort one is handed out, and
/// it is never used up.
#[tokio::test]
async fn the_last_resort_package_outlasts_the_ordinary_ones() {
    let (_d, bus) = fresh_bus("lastresort");
    let alice = Actor::new("alice", 1);
    alice.enroll(&bus).await;
    publish_with_last_resort(&bus, &alice, 1, &[9, 9]).await;
    let a = name("alice");
    for want in [vec![1], vec![2]] {
        let offer = take(&bus, &a).await.unwrap();
        assert_eq!((offer.package, offer.last_resort), (want, false));
    }
    for _ in 0..3 {
        let offer = take(&bus, &a).await.unwrap();
        assert_eq!((offer.package, offer.last_resort), (vec![9, 9], true));
    }
}

/// The last-resort package is signed for: swapping or adding one invalidates
/// the signature, and a bad one is rejected whole.
#[tokio::test]
async fn the_last_resort_package_is_covered_by_the_signature() {
    let (_d, bus) = fresh_bus("lastresortsig");
    let alice = Actor::new("alice", 1);
    alice.enroll(&bus).await;
    let packages = vec![vec![1u8]];
    let sig = alice
        .keys
        .sign(&key_packages_payload("alice", 1, &packages, Some(&[1])));
    let err = bus
        .publish_key_packages(&name("alice"), 1, packages.clone(), Some(vec![2]), &sig)
        .await
        .unwrap_err();
    assert_eq!(code(err), "invalid_signature");
    let err = bus
        .publish_key_packages(&name("alice"), 1, packages.clone(), None, &sig)
        .await
        .unwrap_err();
    assert_eq!(code(err), "invalid_signature");
    let big = vec![0u8; MAX_KEY_PACKAGE_BYTES + 1];
    let sig = alice
        .keys
        .sign(&key_packages_payload("alice", 2, &[], Some(&big)));
    let err = bus
        .publish_key_packages(&name("alice"), 2, vec![], Some(big), &sig)
        .await
        .unwrap_err();
    assert_eq!(code(err), "invalid_body");
    // A publish of only a last-resort package is fine.
    let sig = alice
        .keys
        .sign(&key_packages_payload("alice", 3, &[], Some(&[5])));
    bus.publish_key_packages(&name("alice"), 3, vec![], Some(vec![5]), &sig)
        .await
        .unwrap();
}

/// One requester cannot take more than its share per minute, the target keeps
/// serving others, and the window slides on the bus clock.
#[tokio::test]
async fn one_requester_cannot_drain_an_agent() {
    use mentischat::bus::{TAKES_PER_MINUTE, TAKES_PER_REQUESTER};
    let (_d, bus, clock) =
        common::bus_with_clock("ratelimit", mentischat::bus::Retention::default());
    let alice = Actor::new("alice", 1);
    alice.enroll(&bus).await;
    publish_with_last_resort(&bus, &alice, 1, &[9]).await;
    register(&bus, &["mallory", "bob", "carol", "dave"]).await;
    let a = name("alice");
    for _ in 0..TAKES_PER_REQUESTER {
        bus.take_key_package(&a, &name("mallory")).await.unwrap();
    }
    let err = bus
        .take_key_package(&a, &name("mallory"))
        .await
        .unwrap_err();
    let MailboxError::RateLimited { retry_after_ms } = err else {
        panic!("expected rate_limited, got {err:?}");
    };
    assert!((1..=60_000).contains(&retry_after_ms));
    // Others are still served.
    bus.take_key_package(&a, &name("bob")).await.unwrap();
    // Together they cannot exceed the overall cap either.
    let mut served = TAKES_PER_REQUESTER + 1;
    for who in ["bob", "carol", "dave", "req1", "req2", "req3"] {
        let _ = bus.register(&name(who)).await;
        for _ in 0..TAKES_PER_REQUESTER {
            if bus.take_key_package(&a, &name(who)).await.is_ok() {
                served += 1;
            }
        }
    }
    assert_eq!(served, TAKES_PER_MINUTE);
    // A minute later the window is empty again.
    clock.advance(std::time::Duration::from_secs(61));
    bus.take_key_package(&a, &name("mallory")).await.unwrap();
}

/// A request needs a registered requester.
#[tokio::test]
async fn a_key_package_request_needs_a_registered_requester() {
    let (_d, bus) = fresh_bus("requester");
    let alice = Actor::new("alice", 1);
    alice.enroll(&bus).await;
    publish_with_last_resort(&bus, &alice, 1, &[9]).await;
    let err = bus
        .take_key_package(&name("alice"), &name("ghost"))
        .await
        .unwrap_err();
    assert_eq!(code(err), "unregistered");
}
