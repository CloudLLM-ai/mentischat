//! Notify sockets at the bus level: they never take, lease or mark mail,
//! summarize it without bodies, ring on every send, hold their agent off the
//! idle sweep, are capped per agent, and make `notified` in a send receipt
//! true exactly while something attends to the recipient.

mod common;

use std::time::Duration;

use common::{bus_with_clock, fresh_bus, idle_after, name, register, room};
use mentischat::bus::{MAX_DIGEST_NAMES, MAX_NOTIFY_PER_AGENT};
use mentischat::errors::MailboxError;

/// A send to an agent nobody attends to is ordinary mail: `notified` false.
#[tokio::test]
async fn test_receipt_says_not_notified_when_nobody_listens() {
    let (_d, bus) = fresh_bus("n1");
    register(&bus, &["a", "b"]).await;
    let receipt = bus.send(&name("a"), &name("b"), "hi".into()).await.unwrap();
    assert_eq!(receipt.notified, Some(false));
    assert_eq!(receipt.notified_members, None);
}

/// An open notify socket makes the receipt say `notified`, and closing it
/// makes the next one say not notified again.
#[tokio::test]
async fn test_receipt_notified_follows_the_socket_lifetime() {
    let (_d, bus) = fresh_bus("n2");
    register(&bus, &["a", "b"]).await;
    let socket = bus.open_notify(&name("b")).unwrap();
    let open = bus.send(&name("a"), &name("b"), "1".into()).await.unwrap();
    assert_eq!(open.notified, Some(true));
    drop(socket);
    let closed = bus.send(&name("a"), &name("b"), "2".into()).await.unwrap();
    assert_eq!(closed.notified, Some(false));
}

/// A stream counts as attending to its agent.
#[tokio::test]
async fn test_receipt_notified_when_a_stream_is_open() {
    let (_d, bus) = fresh_bus("n3");
    register(&bus, &["a", "b"]).await;
    let _stream = bus.open_stream(&name("b")).unwrap();
    let r = bus.send(&name("a"), &name("b"), "x".into()).await.unwrap();
    assert_eq!(r.notified, Some(true));
}

/// A pull parked in `check_inbox` counts as attending to its agent.
#[tokio::test(start_paused = true)]
async fn test_receipt_notified_when_a_pull_is_parked() {
    let (_d, bus) = fresh_bus("n4");
    register(&bus, &["a", "b"]).await;
    let b = bus.clone();
    let pull = tokio::spawn(async move { b.check_inbox(&name("b"), 30_000).await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    let r = bus.send(&name("a"), &name("b"), "x".into()).await.unwrap();
    assert_eq!(r.notified, Some(true));
    assert_eq!(pull.await.unwrap().unwrap().messages.len(), 1);
    let after = bus.send(&name("a"), &name("b"), "y".into()).await.unwrap();
    assert_eq!(after.notified, Some(false), "the pull has ended");
}

/// The JSON of a receipt has `notified` for direct mail and
/// `notifiedMembers` for room mail, never both, and every older field.
#[tokio::test]
async fn test_receipt_json_shape_is_additive() {
    let (_d, bus) = fresh_bus("n5");
    register(&bus, &["a", "b"]).await;
    bus.create_room(&name("a"), &room("ops"), false, None)
        .await
        .unwrap();
    bus.join_room(&name("b"), &room("ops")).await.unwrap();
    let direct =
        serde_json::to_value(bus.send(&name("a"), &name("b"), "x".into()).await.unwrap()).unwrap();
    for key in ["id", "from", "to", "createdAt", "notified"] {
        assert!(direct.get(key).is_some(), "{key} in {direct}");
    }
    assert!(direct.get("notifiedMembers").is_none());
    let in_room = serde_json::to_value(
        bus.send_room(&name("a"), &room("ops"), "x".into())
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(in_room["notifiedMembers"], 0);
    assert!(in_room.get("notified").is_none());
    assert_eq!(in_room["room"], "ops");
}

/// Room receipts count the members attended to, never the sender, however
/// the member is attended to.
#[tokio::test]
async fn test_room_receipt_counts_listening_members_not_the_sender() {
    let (_d, bus) = fresh_bus("n6");
    register(&bus, &["a", "b", "c", "d"]).await;
    bus.create_room(&name("a"), &room("ops"), false, None)
        .await
        .unwrap();
    for who in ["b", "c", "d"] {
        bus.join_room(&name(who), &room("ops")).await.unwrap();
    }
    let _sender_socket = bus.open_notify(&name("a")).unwrap();
    let _b = bus.open_notify(&name("b")).unwrap();
    let _c = bus.open_stream(&name("c")).unwrap();
    let r = bus
        .send_room(&name("a"), &room("ops"), "hi".into())
        .await
        .unwrap();
    assert_eq!(r.notified_members, Some(2), "b and c, not a, not d");
}

/// Opening a notify socket takes nothing: `check_inbox` still returns the
/// mail, once, and the socket's digest never marks it read.
#[tokio::test]
async fn test_notify_never_leases_or_reads_mail() {
    let (_d, bus) = fresh_bus("n7");
    register(&bus, &["a", "b"]).await;
    let socket = bus.open_notify(&name("b")).unwrap();
    bus.send(&name("a"), &name("b"), "secret body".into())
        .await
        .unwrap();
    for _ in 0..3 {
        assert_eq!(bus.notify_digest(&socket).unwrap().unread, 1);
    }
    let page = bus.check_inbox(&name("b"), 0).await.unwrap();
    assert_eq!(page.messages.len(), 1);
    assert_eq!(bus.notify_digest(&socket).unwrap().unread, 0);
    assert!(bus
        .check_inbox(&name("b"), 0)
        .await
        .unwrap()
        .messages
        .is_empty());
}

/// The digest names senders and rooms, counts exactly, reports the newest
/// id, and carries no body (it has no field that could).
#[tokio::test]
async fn test_digest_summarizes_direct_and_room_mail() {
    let (_d, bus) = fresh_bus("n8");
    register(&bus, &["a", "b", "c"]).await;
    bus.create_room(&name("c"), &room("ops"), false, None)
        .await
        .unwrap();
    bus.join_room(&name("b"), &room("ops")).await.unwrap();
    let socket = bus.open_notify(&name("b")).unwrap();
    assert_eq!(bus.notify_digest(&socket).unwrap().unread, 0);
    bus.send(&name("a"), &name("b"), "one".into())
        .await
        .unwrap();
    bus.send_room(&name("c"), &room("ops"), "two".into())
        .await
        .unwrap();
    let last = bus
        .send(&name("a"), &name("b"), "three".into())
        .await
        .unwrap();
    let digest = bus.notify_digest(&socket).unwrap();
    assert_eq!(digest.unread, 3);
    assert_eq!(digest.from, ["a", "c"]);
    assert_eq!(digest.rooms, ["ops"]);
    assert_eq!(digest.newest.to_string(), last.id);
}

/// A member's own room messages are not unread for it.
#[tokio::test]
async fn test_digest_ignores_own_room_messages() {
    let (_d, bus) = fresh_bus("n9");
    register(&bus, &["a", "b"]).await;
    bus.create_room(&name("a"), &room("ops"), false, None)
        .await
        .unwrap();
    bus.join_room(&name("b"), &room("ops")).await.unwrap();
    let socket = bus.open_notify(&name("a")).unwrap();
    bus.send_room(&name("a"), &room("ops"), "mine".into())
        .await
        .unwrap();
    assert_eq!(bus.notify_digest(&socket).unwrap().unread, 0);
    bus.send_room(&name("b"), &room("ops"), "theirs".into())
        .await
        .unwrap();
    assert_eq!(bus.notify_digest(&socket).unwrap().unread, 1);
}

/// Sender and room lists are bounded; `unread` stays exact.
#[tokio::test]
async fn test_digest_lists_are_bounded_but_unread_is_exact() {
    let (_d, bus) = fresh_bus("n10");
    register(&bus, &["me"]).await;
    let senders = MAX_DIGEST_NAMES + 3;
    for i in 0..senders {
        let who = format!("s{i}");
        register(&bus, &[who.as_str()]).await;
        bus.send(&name(&who), &name("me"), "x".into())
            .await
            .unwrap();
    }
    let socket = bus.open_notify(&name("me")).unwrap();
    let digest = bus.notify_digest(&socket).unwrap();
    assert_eq!(digest.unread, senders);
    assert_eq!(digest.from.len(), MAX_DIGEST_NAMES);
}

/// Sockets coexist up to the cap; one more is refused; a closed socket
/// frees its slot; a refused socket never counts as attending.
#[tokio::test]
async fn test_notify_sockets_are_capped_per_agent() {
    let (_d, bus) = fresh_bus("n11");
    register(&bus, &["a", "b", "c"]).await;
    let open: Vec<_> = (0..MAX_NOTIFY_PER_AGENT)
        .map(|_| bus.open_notify(&name("b")).unwrap())
        .collect();
    let err = bus.open_notify(&name("b")).unwrap_err();
    assert!(
        matches!(err, MailboxError::TooManyWaiters { waiting, .. } if waiting == MAX_NOTIFY_PER_AGENT),
        "{err:?}"
    );
    // Another agent has its own budget.
    assert!(bus.open_notify(&name("c")).is_ok());
    drop(open);
    assert!(bus.open_notify(&name("b")).is_ok());
    // Refusals do not leak a count: with every socket closed, not notified.
    let _full: Vec<_> = (0..MAX_NOTIFY_PER_AGENT)
        .map(|_| bus.open_notify(&name("a")).unwrap())
        .collect();
    for _ in 0..5 {
        assert!(bus.open_notify(&name("a")).is_err());
    }
}

/// Every open socket hears every send, a parked pull still gets its wake,
/// and the socket does not take over a stream.
#[tokio::test(start_paused = true)]
async fn test_every_socket_hears_each_send_and_pulls_still_wake() {
    let (_d, bus) = fresh_bus("n12");
    register(&bus, &["a", "b"]).await;
    let s1 = bus.open_notify(&name("b")).unwrap();
    let s2 = bus.open_notify(&name("b")).unwrap();
    let (mut e1, mut e2) = (s1.events(), s2.events());
    let b = bus.clone();
    let pull = tokio::spawn(async move { b.check_inbox(&name("b"), 30_000).await });
    tokio::time::sleep(Duration::from_millis(10)).await;
    bus.send(&name("a"), &name("b"), "x".into()).await.unwrap();
    e1.changed().await.unwrap();
    e2.changed().await.unwrap();
    let page = pull.await.unwrap().unwrap();
    assert_eq!(page.messages.len(), 1, "the parked pull was woken too");
    let stream = bus.open_stream(&name("b")).unwrap();
    assert!(!stream.taken_over());
    drop(s1);
    assert!(
        !stream.taken_over(),
        "a notify socket never disturbs a stream"
    );
}

/// An open socket keeps its idle agent on the roster; once it closes the
/// agent's last-seen is recent, so the sweep still leaves it for a while.
#[tokio::test]
async fn test_open_notify_socket_keeps_the_agent_through_the_idle_sweep() {
    let (_d, bus, clock) = bus_with_clock("n13", idle_after(60));
    register(&bus, &["a", "b"]).await;
    let socket = bus.open_notify(&name("b")).unwrap();
    clock.advance(Duration::from_secs(3_600));
    let out = bus.sweep().await.unwrap();
    assert_eq!(out.dropped_agents, ["a"], "only the agent with no socket");
    bus.notify_touch(&socket);
    drop(socket);
    assert!(bus.sweep().await.unwrap().dropped_agents.is_empty());
    clock.advance(Duration::from_secs(3_600));
    assert_eq!(bus.sweep().await.unwrap().dropped_agents, ["b"]);
}

/// A socket learns when its agent leaves: the event fires and the digest
/// fails `unregistered`.
#[tokio::test]
async fn test_socket_notices_its_agent_leaving() {
    let (_d, bus) = fresh_bus("n14");
    register(&bus, &["b"]).await;
    let socket = bus.open_notify(&name("b")).unwrap();
    let mut events = socket.events();
    bus.deregister(&name("b")).await.unwrap();
    events.changed().await.unwrap();
    assert!(matches!(
        bus.notify_digest(&socket),
        Err(MailboxError::Unregistered { .. })
    ));
}

/// Opening for an unregistered name fails `unregistered`.
#[tokio::test]
async fn test_open_notify_requires_a_registered_agent() {
    let (_d, bus) = fresh_bus("n15");
    assert!(matches!(
        bus.open_notify(&name("ghost")),
        Err(MailboxError::Unregistered { .. })
    ));
}
