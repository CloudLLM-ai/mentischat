//! End-to-end encrypted rooms through the real client and a real daemon on a
//! loopback port chosen by the OS (never 9480): MLS groups across three and
//! more clients, adding and removing members, and what the daemon can see.
#![cfg(feature = "e2e")]

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use mentischat::bus::Bus;
use mentischat::client::{Client, Daemon, Identity};
use mentischat::names::{AgentName, RoomName};

/// A daemon on an ephemeral loopback port; the bus is kept for inspection.
struct Harness {
    _dir: tempfile::TempDir,
    clients: std::path::PathBuf,
    daemon: Daemon,
    bus: Arc<Bus>,
}

async fn harness() -> Harness {
    let dir = tempfile::tempdir().unwrap();
    let bus = Arc::new(Bus::new(&dir.path().join("daemon")).unwrap());
    let app = mentischat::build_router(bus.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .ok();
    });
    Harness {
        clients: dir.path().join("clients"),
        daemon: Daemon::new(addr),
        bus,
        _dir: dir,
    }
}

impl Harness {
    /// A registered, enrolled client for `name`.
    async fn client(&self, name: &str) -> Client {
        let mut c = self.open(name);
        c.register(false).await.unwrap();
        c.enroll().await.unwrap();
        c
    }

    fn open(&self, name: &str) -> Client {
        open_at(&self.clients, name, self.daemon)
    }

    /// Every message body the room still holds, as the daemon stores it,
    /// read straight from the bus (past any membership check a client faces).
    async fn stored_bodies(&self, room: &str, as_member: &str) -> Vec<(String, String)> {
        let (agent, room) = (
            AgentName::parse(as_member).unwrap(),
            RoomName::parse(room).unwrap(),
        );
        let mut all = Vec::new();
        let mut before = None;
        loop {
            let page = self.bus.room_history(&agent, &room, before).await.unwrap();
            let mut older: Vec<_> = page
                .messages
                .into_iter()
                .map(|m| (m.from, m.body))
                .collect();
            older.append(&mut all);
            all = older;
            match page.next {
                Some(next) => before = Some(next),
                None => return all,
            }
        }
    }
}

fn open_at(dir: &Path, name: &str, daemon: Daemon) -> Client {
    Client::open(Identity::open(dir, name).unwrap(), daemon).unwrap()
}

/// What `who` has decrypted and not yet seen in `room`, as `from: text`.
async fn inbox(who: &mut Client, room: &str) -> Vec<String> {
    who.sync(room).await.unwrap();
    who.take_unread(room)
        .unwrap()
        .into_iter()
        .map(|m| format!("{}: {}", m.from, m.body))
        .collect()
}

/// Alice's room with Bob and Carol added and joined.
async fn trio(h: &Harness) -> (Client, Client, Client) {
    let mut alice = h.client("alice").await;
    let mut bob = h.client("bob").await;
    let mut carol = h.client("carol").await;
    alice.create_room("den", "secret den").await.unwrap();
    alice.add_member("den", "bob").await.unwrap();
    bob.join_room("den").await.unwrap();
    bob.add_member("den", "carol").await.unwrap();
    carol.join_room("den").await.unwrap();
    alice.sync("den").await.unwrap();
    (alice, bob, carol)
}

/// The whole flow: three clients converge on one epoch and member list and
/// read each other's messages; the daemon holds only ciphertext.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn three_clients_share_a_group_and_the_daemon_sees_only_ciphertext() {
    let h = harness().await;
    let (mut alice, mut bob, mut carol) = trio(&h).await;

    alice.send("den", "the launch code is 7-4-1").await.unwrap();
    bob.send("den", "copy that, rotating the staging key")
        .await
        .unwrap();
    assert_eq!(
        inbox(&mut bob, "den").await,
        ["alice: the launch code is 7-4-1"]
    );
    assert_eq!(
        inbox(&mut carol, "den").await,
        [
            "alice: the launch code is 7-4-1",
            "bob: copy that, rotating the staging key"
        ]
    );
    assert_eq!(
        inbox(&mut alice, "den").await,
        ["bob: copy that, rotating the staging key"]
    );

    let (epoch_a, mut members_a) = alice.group_members("den").unwrap();
    let (epoch_b, mut members_b) = bob.group_members("den").unwrap();
    let (epoch_c, mut members_c) = carol.group_members("den").unwrap();
    assert_eq!((epoch_a, epoch_a), (epoch_b, epoch_c));
    for m in [&mut members_a, &mut members_b, &mut members_c] {
        m.sort();
    }
    assert_eq!(members_a, ["alice", "bob", "carol"]);
    assert_eq!((&members_a, &members_a), (&members_b, &members_c));

    // The daemon's whole view of the room: opaque, signed, no plaintext.
    let stored = h.stored_bodies("den", "alice").await;
    assert!(stored.len() >= 4, "two commits and two messages at least");
    for (from, body) in &stored {
        assert!(body.starts_with("mentischat-signed/1 "), "{from}: {body}");
        for secret in ["launch code", "7-4-1", "staging key", "rotating"] {
            assert!(
                !body.contains(secret),
                "plaintext leaked to the daemon: {body}"
            );
        }
        assert!(body.contains("mls/1 "));
    }
    // And nothing of it reaches a snapshot: a signed room is never saved.
    h.bus.close().await;
    let snapshot = std::fs::read_dir(h._dir.path().join("daemon"))
        .into_iter()
        .flatten()
        .flatten()
        .map(|e| std::fs::read(e.path()).unwrap_or_default())
        .any(|bytes| String::from_utf8_lossy(&bytes).contains("launch code"));
    assert!(!snapshot);
}

/// A member removed by the owner cannot read what follows, even holding the
/// ciphertext, whether or not it ever saw the removal Commit.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_removed_member_cannot_decrypt_later_messages() {
    let h = harness().await;
    let (mut alice, mut bob, mut carol) = trio(&h).await;
    alice.send("den", "before removal").await.unwrap();
    assert_eq!(inbox(&mut carol, "den").await, ["alice: before removal"]);

    // Control: carol can still read a message from before the removal that she
    // has not synced, so the failures below are about the removal itself.
    alice
        .send("den", "last words before removal")
        .await
        .unwrap();
    let control = h.stored_bodies("den", "alice").await;
    let (from, body) = control.last().unwrap();
    assert_eq!(
        carol.try_decrypt("den", from, body).unwrap().as_deref(),
        Some("last words before removal")
    );

    alice.remove_member("den", "carol").await.unwrap();
    alice
        .send("den", "after removal: the new plan")
        .await
        .unwrap();

    // Bob follows the removal and reads on.
    assert_eq!(
        inbox(&mut bob, "den").await,
        [
            "alice: before removal",
            "alice: last words before removal",
            "alice: after removal: the new plan"
        ]
    );
    // The daemon no longer lets carol read the room at all.
    let err = carol.sync("den").await.unwrap_err();
    assert_eq!(err.daemon_code(), Some("room_not_found"), "{err}");
    let err = carol.send("den", "let me back in").await.unwrap_err();
    assert_eq!(err.daemon_code(), Some("room_not_found"), "{err}");

    // Past the ACL: hand carol every stored ciphertext. She stayed at the
    // epoch before the removal, so none of the later ones decrypts.
    let stored = h.stored_bodies("den", "alice").await;
    let later: Vec<_> = stored
        .iter()
        .filter(|(_, b)| b.contains("mls/1 app"))
        .collect();
    assert_eq!(later.len(), 3, "before, last words, after");
    let mut readable = Vec::new();
    for (from, body) in later {
        if let Some(text) = carol.try_decrypt("den", from, body).unwrap() {
            readable.push(text);
        }
    }
    assert!(
        !readable.iter().any(|t| t.contains("new plan")),
        "carol read a message from after her removal: {readable:?}"
    );
    // The members' view of the group no longer lists her.
    let (_, members) = alice.group_members("den").unwrap();
    assert!(!members.contains(&"carol".to_string()), "{members:?}");
    let (_, members) = bob.group_members("den").unwrap();
    assert!(!members.contains(&"carol".to_string()), "{members:?}");
}

/// Processing the removal Commit ends the removed client's group, even if the
/// daemon had not yet taken it out of the room.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_client_that_sees_its_removal_closes_its_group() {
    let h = harness().await;
    let (mut alice, _bob, mut carol) = trio(&h).await;
    alice.commit_removal("den", "carol").await.unwrap();
    carol.sync("den").await.unwrap();
    let err = carol.send("den", "hello?").await.unwrap_err();
    assert!(
        err.to_string().contains("no longer in secure room"),
        "{err}"
    );
    // Finishing the removal at the daemon changes nothing for the group.
    alice.remove_member("den", "carol").await.unwrap();
}

/// A member who leaves is pruned from the group by the owner, and then cannot
/// read what follows.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_owner_prunes_members_who_left() {
    let h = harness().await;
    let (mut alice, mut bob, mut carol) = trio(&h).await;
    carol.leave_room("den").await.unwrap();
    let pruned = alice.prune("den").await.unwrap();
    assert_eq!(pruned, ["carol"]);
    alice.send("den", "carol is gone").await.unwrap();
    assert_eq!(inbox(&mut bob, "den").await, ["alice: carol is gone"]);
    let stored = h.stored_bodies("den", "alice").await;
    for (from, body) in stored.iter().filter(|(_, b)| b.contains("mls/1 app")) {
        let read = carol.try_decrypt("den", from, body);
        assert!(
            !matches!(read, Ok(Some(ref t)) if t.contains("gone")),
            "{read:?}"
        );
    }
    // Pruning again finds nothing to do.
    assert!(alice.prune("den").await.unwrap().is_empty());
}

/// Two members commit at once: the daemon accepts one per epoch, the loser
/// retries on top of the winner, and everyone ends in the same group.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn racing_commits_converge_on_one_group() {
    let h = harness().await;
    let (mut alice, mut bob, mut carol) = trio(&h).await;
    let mut dave = h.client("dave").await;
    let mut erin = h.client("erin").await;
    let (a, b) = tokio::join!(
        alice.add_member("den", "dave"),
        bob.add_member("den", "erin")
    );
    a.unwrap();
    b.unwrap();
    dave.join_room("den").await.unwrap();
    erin.join_room("den").await.unwrap();

    let mut views = Vec::new();
    for c in [&mut alice, &mut bob, &mut carol, &mut dave, &mut erin] {
        c.sync("den").await.unwrap();
        let (epoch, mut names) = c.group_members("den").unwrap();
        names.sort();
        views.push((epoch, names));
    }
    assert!(views.windows(2).all(|w| w[0] == w[1]), "{views:?}");
    assert_eq!(views[0].1, ["alice", "bob", "carol", "dave", "erin"]);

    dave.send("den", "everyone here?").await.unwrap();
    for c in [&mut alice, &mut bob, &mut carol, &mut erin] {
        assert_eq!(inbox(c, "den").await, ["dave: everyone here?"]);
    }
}

/// A client restarted from its files still has its groups and keys.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restarted_client_keeps_its_groups() {
    let h = harness().await;
    let (alice, mut bob, _carol) = trio(&h).await;
    drop(alice);
    let mut alice = h.open("alice");
    alice.send("den", "back after a restart").await.unwrap();
    assert_eq!(
        inbox(&mut bob, "den").await,
        ["alice: back after a restart"]
    );
    bob.send("den", "welcome back").await.unwrap();
    assert_eq!(inbox(&mut alice, "den").await, ["bob: welcome back"]);
    assert_eq!(alice.secure_rooms(), ["den"]);
}

/// A member added later reads what is sent from its epoch on, and the stored
/// history it receives is its own plaintext log.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_late_member_reads_from_its_epoch_on() {
    let h = harness().await;
    let (mut alice, mut bob, _carol) = trio(&h).await;
    alice.send("den", "old news").await.unwrap();
    let mut dave = h.client("dave").await;
    alice.add_member("den", "dave").await.unwrap();
    dave.join_room("den").await.unwrap();
    bob.sync("den").await.unwrap();
    alice.send("den", "fresh news").await.unwrap();
    assert_eq!(inbox(&mut dave, "den").await, ["alice: fresh news"]);
    assert_eq!(dave.history("den").len(), 1);
}

/// An agent whose ordinary KeyPackages were all taken can still be added, by
/// its last-resort package, into more than one room; and one process cannot
/// drain it faster than the rate limit lets it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_drained_agent_is_still_added_by_its_last_resort_package() {
    let h = harness().await;
    let mut alice = h.client("alice").await;
    let mut bob = h.client("bob").await;
    let mut carol = h.client("carol").await;
    let _mallory = h.client("mallory").await;
    let take = |who: &'static str| {
        let daemon = h.daemon;
        async move {
            daemon
                .call(
                    "POST",
                    "/agents/bob/keypackages/take",
                    Some(&serde_json::json!({ "requester": who })),
                )
                .await
        }
    };
    let mut drained = 0;
    let limited = loop {
        match take("mallory").await {
            Ok(v) if v["lastResort"] == false => drained += 1,
            Ok(_) => panic!("took the last resort before the rate limit"),
            Err(e) => break e,
        }
    };
    assert_eq!(limited.daemon_code(), Some("rate_limited"));
    assert_eq!(drained, mentischat::bus::TAKES_PER_REQUESTER);
    // Another requester drains the rest of the ordinary packages.
    for _ in 0..(mentischat::client::session::KEY_PACKAGE_STOCK - drained) {
        take("alice").await.unwrap();
    }
    assert_eq!(take("alice").await.unwrap()["lastResort"], true);

    alice.create_room("one", "").await.unwrap();
    alice.add_member("one", "bob").await.unwrap();
    bob.join_room("one").await.unwrap();
    carol.create_room("two", "").await.unwrap();
    carol.add_member("two", "bob").await.unwrap();
    bob.join_room("two").await.unwrap();

    alice.send("one", "first room").await.unwrap();
    carol.send("two", "second room").await.unwrap();
    assert_eq!(inbox(&mut bob, "one").await, ["alice: first room"]);
    assert_eq!(inbox(&mut bob, "two").await, ["carol: second room"]);
}

/// Adding an agent that never published anything fails clearly.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn adding_an_agent_with_no_key_packages_at_all_fails() {
    let h = harness().await;
    let mut alice = h.client("alice").await;
    alice.create_room("den", "").await.unwrap();
    let bare = h.open("bare");
    bare.register(false).await.unwrap();
    let err = alice.add_member("den", "bare").await.unwrap_err();
    assert!(
        matches!(err.daemon_code(), Some("key_required" | "no_key_package"))
            || err.to_string().contains("no key"),
        "{err}"
    );
}

/// Key files are private and a loosened one is refused.
#[tokio::test]
async fn key_files_are_private_and_loose_ones_are_refused() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let id = Identity::open(dir.path(), "alice").unwrap();
    let key_path = id.dir().join("identity.key");
    let mode = |p: &Path| std::fs::metadata(p).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode(&key_path), 0o600);
    assert_eq!(mode(id.dir()), 0o700);
    let again = Identity::open(dir.path(), "alice").unwrap();
    assert_eq!(
        again.public(),
        id.public(),
        "the same file gives the same key"
    );
    assert!(!format!("{id:?}").contains(&std::fs::read_to_string(&key_path).unwrap()));

    std::fs::set_permissions(&key_path, std::fs::Permissions::from_mode(0o644)).unwrap();
    assert!(Identity::open(dir.path(), "alice").is_err());
    std::fs::write(&key_path, "not hex").unwrap();
    std::fs::set_permissions(&key_path, std::fs::Permissions::from_mode(0o600)).unwrap();
    assert!(Identity::open(dir.path(), "alice").is_err());
}

/// The state file holds group secrets and is private too.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_state_file_is_private() {
    use std::os::unix::fs::PermissionsExt;
    let h = harness().await;
    let mut alice = h.client("alice").await;
    alice.create_room("den", "").await.unwrap();
    let state = h.clients.join("alice").join("state.json");
    assert_eq!(
        std::fs::metadata(state).unwrap().permissions().mode() & 0o777,
        0o600
    );
}

/// A secure room refuses a client that never joined it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn operations_on_an_unknown_secure_room_are_usage_errors() {
    let h = harness().await;
    let mut alice = h.client("alice").await;
    assert!(alice.send("nowhere", "x").await.is_err());
    assert!(alice.sync("nowhere").await.is_err());
    assert!(alice.history("nowhere").is_empty());
    assert!(alice.take_unread("nowhere").unwrap().is_empty());
    assert!(!alice.is_secure("nowhere"));
}
