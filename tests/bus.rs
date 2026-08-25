//! Bus behavior tests: roster semantics, caps, batch+ack delivery, waiter
//! races, crash recovery, and mutex stress — all in-process against tempdirs.

use std::sync::Arc;

use mentischat::bus::{Bus, NudgeTransport};
use mentischat::errors::MailboxError;
use mentischat::names::AgentName;

fn name(s: &str) -> AgentName {
    AgentName::parse(s).expect("valid test name")
}

fn fresh_bus(tag: &str) -> (tempfile::TempDir, Arc<Bus>) {
    let dir = tempfile::tempdir().expect("tempdir");
    let data = dir.path().join(tag);
    let b = Bus::new(&data).expect("bus opens");
    (dir, Arc::new(b))
}

#[tokio::test]
async fn register_is_idempotent_and_keeps_unacked_rows() {
    let (_d, bus) = fresh_bus("idem");
    bus.register(&name("grok")).await.unwrap();
    bus.register(&name("opencode")).await.unwrap();
    bus.send(&name("opencode"), &name("grok"), "hi".into())
        .await
        .unwrap();

    let again = bus.register(&name("grok")).await.unwrap();
    assert!(!again.created);

    let page = bus.check_inbox(&name("grok"), 0, vec![]).await.unwrap();
    assert_eq!(page.messages.len(), 1, "queue survived re-register");
}

#[tokio::test]
async fn send_between_two_names() {
    let (_d, bus) = fresh_bus("pair");
    bus.register(&name("a")).await.unwrap();
    bus.register(&name("b")).await.unwrap();

    let receipt = bus
        .send(&name("a"), &name("b"), "hello".into())
        .await
        .unwrap();
    assert_eq!(receipt.from, "a");
    assert_eq!(receipt.to, "b");

    let page = bus.check_inbox(&name("b"), 0, vec![]).await.unwrap();
    assert_eq!(page.messages.len(), 1);
    assert_eq!(page.messages[0].body, "hello");
    assert_eq!(page.messages[0].from, "a");
    assert_eq!(page.remaining, 0);
}

#[tokio::test]
async fn unacked_pull_redelivers_same_ids() {
    let (_d, bus) = fresh_bus("redeliver");
    bus.register(&name("a")).await.unwrap();
    bus.register(&name("b")).await.unwrap();
    bus.send(&name("a"), &name("b"), "m1".into()).await.unwrap();

    let p1 = bus.check_inbox(&name("b"), 0, vec![]).await.unwrap();
    let p2 = bus.check_inbox(&name("b"), 0, vec![]).await.unwrap();
    assert_eq!(
        p1.messages[0].id, p2.messages[0].id,
        "in_flight redelivered"
    );
}

#[tokio::test]
async fn ack_deletes_then_promotes_next_batch() {
    let (_d, bus) = fresh_bus("ack");
    bus.register(&name("a")).await.unwrap();
    bus.register(&name("b")).await.unwrap();
    for i in 0..3 {
        bus.send(&name("a"), &name("b"), format!("m{i}"))
            .await
            .unwrap();
    }

    let p1 = bus.check_inbox(&name("b"), 0, vec![]).await.unwrap();
    assert_eq!(p1.messages.len(), 3);

    let ack_ids = vec![p1.messages[0].id.clone()];
    let p2 = bus.check_inbox(&name("b"), 0, ack_ids).await.unwrap();
    assert_eq!(p2.messages.len(), 2, "acked id deleted, rest redelivered");
    assert!(p2.messages.iter().all(|m| m.id != p1.messages[0].id));

    // Ack everything; inbox empties.
    let all: Vec<String> = p2.messages.iter().map(|m| m.id.clone()).collect();
    let p3 = bus.check_inbox(&name("b"), 0, all).await.unwrap();
    assert!(p3.messages.is_empty());
}

#[tokio::test]
async fn batch_cap_reports_remaining() {
    let (_d, bus) = fresh_bus("batch");
    bus.register(&name("a")).await.unwrap();
    bus.register(&name("b")).await.unwrap();
    for i in 0..10 {
        bus.send(&name("a"), &name("b"), format!("m{i}"))
            .await
            .unwrap();
    }

    let p1 = bus.check_inbox(&name("b"), 0, vec![]).await.unwrap();
    assert_eq!(p1.messages.len(), 8, "MAX_BATCH");
    assert_eq!(p1.remaining, 2);

    let acks: Vec<String> = p1.messages.iter().map(|m| m.id.clone()).collect();
    let p2 = bus.check_inbox(&name("b"), 0, acks).await.unwrap();
    assert_eq!(p2.messages.len(), 2);
    assert_eq!(p2.remaining, 0);
}

#[tokio::test]
async fn oversized_first_message_still_delivers() {
    let (_d, bus) = fresh_bus("bigfirst");
    bus.register(&name("a")).await.unwrap();
    bus.register(&name("b")).await.unwrap();
    let big = "x".repeat(20 * 1024); // > MAX_RETURN_BYTES
    bus.send(&name("a"), &name("b"), big).await.unwrap();
    bus.send(&name("a"), &name("b"), "small".into())
        .await
        .unwrap();

    let page = bus.check_inbox(&name("b"), 0, vec![]).await.unwrap();
    assert_eq!(page.messages.len(), 1, "oversized first rides alone");
    assert_eq!(page.remaining, 1);
}

#[tokio::test]
async fn unregistered_operations_fail_with_known_roster() {
    let (_d, bus) = fresh_bus("unreg");
    bus.register(&name("known")).await.unwrap();

    let err = bus
        .send(&name("ghost"), &name("known"), "x".into())
        .await
        .unwrap_err();
    match &err {
        MailboxError::Unregistered { agent, known } => {
            assert_eq!(agent, "ghost");
            assert_eq!(known, &vec!["known".to_string()]);
        }
        other => panic!("expected unregistered, got {other:?}"),
    }

    let err = bus
        .check_inbox(&name("nobody"), 0, vec![])
        .await
        .unwrap_err();
    assert!(matches!(err, MailboxError::Unregistered { .. }));

    let err = bus.deregister(&name("nobody")).await.unwrap_err();
    assert!(matches!(err, MailboxError::Unregistered { .. }));
}

#[tokio::test]
async fn invalid_and_bad_bodies_fail() {
    let (_d, bus) = fresh_bus("invalid");
    assert!(AgentName::parse("-lead").is_err());
    assert!(AgentName::parse("").is_err());
    assert!(AgentName::parse(&"x".repeat(64)).is_err());

    bus.register(&name("a")).await.unwrap();
    bus.register(&name("b")).await.unwrap();

    let err = bus
        .send(&name("a"), &name("b"), "   ".into())
        .await
        .unwrap_err();
    assert!(matches!(err, MailboxError::InvalidBody));

    let err = bus
        .send(&name("a"), &name("b"), "y".repeat(64 * 1024 + 1))
        .await
        .unwrap_err();
    assert!(matches!(err, MailboxError::InvalidBody));
}

#[tokio::test]
async fn per_recipient_queue_cap_hits_queue_full() {
    let (_d, bus) = fresh_bus("qcap");
    for n in ["filler", "victim"] {
        bus.register(&name(n)).await.unwrap();
    }
    for _ in 0..1024 {
        bus.send(&name("filler"), &name("victim"), "x".into())
            .await
            .unwrap();
    }
    let err = bus
        .send(&name("filler"), &name("victim"), "one too many".into())
        .await
        .unwrap_err();
    match err {
        MailboxError::QueueFull { to, pending } => {
            assert_eq!(to, "victim");
            assert_eq!(pending, 1024);
        }
        other => panic!("expected queue_full, got {other:?}"),
    }
}

#[tokio::test]
async fn global_row_cap_hits_storage_full() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("gcap");
    // Shrink nothing: fill 8 names to their per-recipient cap (8 x 1024 =
    // 8192 = MAX_TOTAL_MESSAGES), then the next insert must fail globally.
    let bus = Arc::new(Bus::new(&data).unwrap());
    let filler = name("filler");
    bus.register(&filler).await.unwrap();
    for i in 0..8 {
        let victim = name(&format!("v{i}"));
        bus.register(&victim).await.unwrap();
        for _ in 0..1024 {
            bus.send(&filler, &victim, "x".into()).await.unwrap();
        }
    }
    let extra = name("extra");
    bus.register(&extra).await.unwrap();
    let err = bus.send(&filler, &extra, "boom".into()).await.unwrap_err();
    assert!(matches!(err, MailboxError::StorageFull { pending: 8192 }));
}

#[tokio::test]
async fn immediate_delivery_beats_wait() {
    let (_d, bus) = fresh_bus("instant");
    bus.register(&name("a")).await.unwrap();
    bus.register(&name("b")).await.unwrap();
    bus.send(&name("a"), &name("b"), "already here".into())
        .await
        .unwrap();
    let started = std::time::Instant::now();
    let page = bus.check_inbox(&name("b"), 30_000, vec![]).await.unwrap();
    assert_eq!(page.messages.len(), 1);
    assert!(started.elapsed() < std::time::Duration::from_secs(1));
}

#[tokio::test]
async fn parked_waiter_wakes_on_send() {
    let (_d, bus) = fresh_bus("wake");
    bus.register(&name("a")).await.unwrap();
    bus.register(&name("b")).await.unwrap();

    let puller = bus.clone();
    let task = tokio::spawn(async move {
        puller
            .check_inbox(&name("b"), 15_000, vec![])
            .await
            .unwrap()
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    bus.send(&name("a"), &name("b"), "wake up".into())
        .await
        .unwrap();

    let page = task.await.unwrap();
    assert_eq!(page.messages.len(), 1);
    assert_eq!(page.messages[0].body, "wake up");
}

#[tokio::test]
async fn timeout_is_empty_success_and_mail_survives_it() {
    let (_d, bus) = fresh_bus("timeout");
    bus.register(&name("a")).await.unwrap();
    bus.register(&name("b")).await.unwrap();

    let page = bus.check_inbox(&name("b"), 150, vec![]).await.unwrap();
    assert!(page.messages.is_empty());
    assert_eq!(page.remaining, 0);

    // Mail arriving after the timed-out pull stays pending.
    bus.send(&name("a"), &name("b"), "late".into())
        .await
        .unwrap();
    let page = bus.check_inbox(&name("b"), 0, vec![]).await.unwrap();
    assert_eq!(page.messages.len(), 1);
}

#[tokio::test]
async fn only_oldest_waiter_gets_the_batch() {
    let (_d, bus) = fresh_bus("two");
    bus.register(&name("a")).await.unwrap();
    bus.register(&name("b")).await.unwrap();

    let b1 = bus.clone();
    let t1 = tokio::spawn(async move { b1.check_inbox(&name("b"), 5_000, vec![]).await });
    let b2 = bus.clone();
    let t2 = tokio::spawn(async move { b2.check_inbox(&name("b"), 5_000, vec![]).await });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    bus.send(&name("a"), &name("b"), "just one".into())
        .await
        .unwrap();

    let p1 = t1.await.unwrap().unwrap();
    let p2 = t2.await.unwrap().unwrap();
    assert_eq!(p1.messages.len(), 1);
    // Shared-name rule: unacked in_flight belongs to the name, not the
    // window — B's pull redelivers A's unacked batch until someone acks.
    assert_eq!(p2.messages.len(), 1);
    assert_eq!(p1.messages[0].id, p2.messages[0].id);
}

#[tokio::test]
async fn ninth_waiter_fails_too_many_waiters() {
    let (_d, bus) = fresh_bus("waitercap");
    bus.register(&name("a")).await.unwrap();
    bus.register(&name("b")).await.unwrap();

    let mut tasks = Vec::new();
    for _ in 0..8 {
        let b = bus.clone();
        tasks.push(tokio::spawn(async move {
            b.check_inbox(&name("b"), 2_000, vec![]).await
        }));
    }
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // The ninth pull attempts to park, which is what trips the cap.
    let ninth = bus.check_inbox(&name("b"), 2_000, vec![]).await;
    match ninth {
        Err(MailboxError::TooManyWaiters { agent, waiting }) => {
            assert_eq!(agent, "b");
            assert_eq!(waiting, 8);
        }
        other => panic!("expected too_many_waiters, got {other:?}"),
    }

    // Release the parkers via their timeouts.
    for t in tasks {
        let page = t.await.unwrap().unwrap();
        assert!(page.messages.is_empty());
    }
}

#[tokio::test]
async fn deregister_rejects_parked_waiters_as_unregistered() {
    let (_d, bus) = fresh_bus("deregwait");
    bus.register(&name("b")).await.unwrap();

    let b = bus.clone();
    let task = tokio::spawn(async move { b.check_inbox(&name("b"), 10_000, vec![]).await });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    bus.deregister(&name("b")).await.unwrap();

    let err = task.await.unwrap().unwrap_err();
    assert!(matches!(err, MailboxError::Unregistered { .. }));
}

#[tokio::test]
async fn deregister_discards_inbox_but_keeps_sent_rows() {
    let (_d, bus) = fresh_bus("discard");
    for n in ["leaver", "stayer"] {
        bus.register(&name(n)).await.unwrap();
    }
    bus.send(&name("leaver"), &name("stayer"), "for you".into())
        .await
        .unwrap();
    bus.send(&name("stayer"), &name("leaver"), "for leaver".into())
        .await
        .unwrap();

    let out = bus.deregister(&name("leaver")).await.unwrap();
    assert_eq!(out.discarded, 1, "only leaver's inbox dropped");

    let page = bus.check_inbox(&name("stayer"), 0, vec![]).await.unwrap();
    assert_eq!(page.messages.len(), 1, "mail leaver sent survives");
}

#[tokio::test]
async fn crash_reopen_preserves_roster_and_rows() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("crash");

    let bus = Bus::new(&data).unwrap();
    bus.register(&name("a")).await.unwrap();
    bus.register(&name("b")).await.unwrap();
    bus.send(&name("a"), &name("b"), "survives".into())
        .await
        .unwrap();
    drop(bus);

    let bus = Bus::new(&data).unwrap();
    let roster = bus.list_agents().await.unwrap();
    assert_eq!(roster.len(), 2, "roster survives a crash (no shutdown)");
    let page = bus.check_inbox(&name("b"), 0, vec![]).await.unwrap();
    assert_eq!(
        page.messages.len(),
        1,
        "unacked mail survives a crash (no shutdown)"
    );
}

#[tokio::test]
async fn graceful_shutdown_discards_roster_and_mail() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("clean");
    let bus = Bus::new(&data).unwrap();
    bus.register(&name("a")).await.unwrap();
    bus.register(&name("b")).await.unwrap();
    bus.send(&name("a"), &name("b"), "gone".into())
        .await
        .unwrap();

    bus.shutdown().await;

    let bus = Bus::new(&data).unwrap();
    assert!(
        bus.list_agents().await.unwrap().is_empty(),
        "graceful shutdown clears the roster"
    );
    let (agents, pending) = bus.stats().await.unwrap();
    assert_eq!(agents, 0);
    assert_eq!(pending, 0, "graceful shutdown discards undelivered mail");
}

#[tokio::test]
async fn concurrent_send_check_never_dups_or_drops() {
    let (_d, bus) = fresh_bus("stress");
    bus.register(&name("sender")).await.unwrap();
    bus.register(&name("receiver")).await.unwrap();

    const N: usize = 50;
    let mut seen = std::collections::HashSet::new();
    let mut tasks = Vec::new();
    for i in 0..N {
        let b = bus.clone();
        tasks.push(tokio::spawn(async move {
            b.send(&name("sender"), &name("receiver"), format!("m{i}"))
                .await
                .unwrap();
            b.check_inbox(&name("receiver"), 0, Vec::new())
                .await
                .unwrap()
        }));
    }
    for t in tasks {
        for msg in t.await.unwrap().messages {
            let id = msg.id;
            seen.insert(id.clone());
        }
    }
    // Drain everything left, acking the previous batch on every pull.
    // At-least-once allows the SAME id to arrive more than once while
    // unacked; the invariant is: distinct ids == N, and nothing outside N.
    let mut acks: Vec<String> = Vec::new();
    loop {
        let page = bus
            .check_inbox(&name("receiver"), 0, std::mem::take(&mut acks))
            .await
            .unwrap();
        acks = page.messages.iter().map(|m| m.id.clone()).collect();
        for id in &acks {
            seen.insert(id.clone());
        }
        if acks.is_empty() && page.remaining == 0 {
            break;
        }
    }
    assert_eq!(seen.len(), N, "all messages delivered");
}

#[tokio::test]
async fn shutdown_rejects_waiters() {
    let (_d, bus) = fresh_bus("shutdown");
    bus.register(&name("b")).await.unwrap();
    let b = bus.clone();
    let task = tokio::spawn(async move { b.check_inbox(&name("b"), 10_000, vec![]).await });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    bus.shutdown().await;
    let err = task.await.unwrap().unwrap_err();
    assert!(matches!(err, MailboxError::ShuttingDown));

    let err = bus.register(&name("b")).await.unwrap_err();
    assert!(matches!(err, MailboxError::ShuttingDown));
}

#[tokio::test]
async fn register_preserves_transport_on_reregister() {
    let dir = tempfile::tempdir().expect("tempdir");
    let data = dir.path().join("regtransport");
    let bus = Bus::new(&data).expect("open");
    bus.register_with_transport(&name("ox-alpha-one"), Some(NudgeTransport::None))
        .await
        .unwrap();
    let _token = bus.bind_wrapper(&name("ox-alpha-one"), None).await.unwrap();

    let listed = bus.list_agents().await.unwrap();
    assert_eq!(listed[0].nudge_transport, NudgeTransport::Wrapper);
    assert!(listed[0].nudge_window.is_none());

    drop(bus);
    let bus = Bus::new(&data).expect("reopen");
    bus.register(&name("ox-alpha-one")).await.unwrap();
    let listed = bus.list_agents().await.unwrap();
    assert_eq!(
        listed[0].nudge_transport,
        NudgeTransport::Wrapper,
        "re-register without transport keeps wrapper binding"
    );

    bus.deregister(&name("ox-alpha-one")).await.unwrap();
    assert!(bus.list_agents().await.unwrap().is_empty());
}

#[tokio::test]
async fn nudge_peek_reports_pending_without_acking_or_promoting() {
    let (_d, bus) = fresh_bus("peekpending");
    bus.register(&name("a")).await.unwrap();
    bus.register(&name("b")).await.unwrap();
    bus.send(&name("a"), &name("b"), "hi".into()).await.unwrap();

    let peek = bus.nudge_peek(&name("b"), 0).await.unwrap();
    assert!(peek.pending);
    assert_eq!(peek.ids.len(), 1);
    assert_eq!(
        peek.pointer,
        mentischat::nudge::POINTER_TEXT,
        "peek carries the shared pointer constant"
    );

    // Repeating the peek keeps returning the same unacked id (no ack).
    let again = bus.nudge_peek(&name("b"), 0).await.unwrap();
    assert_eq!(again.ids, peek.ids);

    // The message is still deliverable through the normal path, which also
    // proves peek never promoted it into the redeliver-on-in_flight shape.
    let page = bus.check_inbox(&name("b"), 0, vec![]).await.unwrap();
    assert_eq!(page.messages.len(), 1);
    assert_eq!(page.messages[0].body, "hi");
}

#[tokio::test]
async fn nudge_peek_timeout_is_pending_false() {
    let (_d, bus) = fresh_bus("peektimeout");
    bus.register(&name("b")).await.unwrap();
    let peek = bus.nudge_peek(&name("b"), 100).await.unwrap();
    assert!(!peek.pending);
    assert!(peek.ids.is_empty());
}

#[tokio::test]
async fn nudge_peek_wakes_on_send() {
    let (_d, bus) = fresh_bus("peekwake");
    bus.register(&name("a")).await.unwrap();
    bus.register(&name("b")).await.unwrap();

    let peek_task = bus.clone();
    let task = tokio::spawn(async move { peek_task.nudge_peek(&name("b"), 15_000).await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    bus.send(&name("a"), &name("b"), "wake".into())
        .await
        .unwrap();

    let peek = task.await.unwrap();
    assert!(peek.pending);
    assert_eq!(peek.ids.len(), 1);
}

#[tokio::test]
async fn nudge_peek_does_not_touch_last_seen() {
    let (_d, bus) = fresh_bus("peeklseen");
    bus.register(&name("b")).await.unwrap();
    let before = bus.list_agents().await.unwrap()[0].last_seen.clone();

    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    let _ = bus.nudge_peek(&name("b"), 50).await.unwrap();

    let after = bus.list_agents().await.unwrap()[0].last_seen.clone();
    assert_eq!(before, after, "peek must not refresh lastSeen");
}

#[tokio::test]
async fn ninth_nudge_peek_waiter_fails_too_many_waiters() {
    let (_d, bus) = fresh_bus("peekcap");
    bus.register(&name("b")).await.unwrap();

    let mut tasks = Vec::new();
    for _ in 0..2 {
        let b = bus.clone();
        tasks.push(tokio::spawn(async move {
            b.nudge_peek(&name("b"), 1_000).await
        }));
    }
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    match bus.nudge_peek(&name("b"), 1_000).await {
        Err(MailboxError::TooManyWaiters { agent, waiting }) => {
            assert_eq!(agent, "b");
            assert_eq!(waiting, 2);
        }
        other => panic!("expected too_many_waiters, got {other:?}"),
    }

    for t in tasks {
        assert!(!t.await.unwrap().unwrap().pending);
    }
}

#[tokio::test]
async fn wrapper_bind_toggles_transport() {
    let (_d, bus) = fresh_bus("wrappertransport");
    bus.register(&name("a")).await.unwrap();
    bus.register(&name("b")).await.unwrap();

    let t1 = bus.bind_wrapper(&name("a"), None).await.unwrap();
    let listed = bus.list_agents().await.unwrap();
    assert_eq!(listed[0].nudge_transport, NudgeTransport::Wrapper);
    assert_eq!(listed[0].nudge_window, None);

    bus.unbind_wrapper(&name("a"), &t1).await.unwrap();
    let listed = bus.list_agents().await.unwrap();
    assert_eq!(listed[0].nudge_transport, NudgeTransport::None);
}

#[tokio::test]
async fn wrapper_bind_requires_registered_agent() {
    let (_d, bus) = fresh_bus("wrapperunreg");
    let err = bus.bind_wrapper(&name("ghost"), None).await.unwrap_err();
    assert!(matches!(err, MailboxError::Unregistered { .. }));

    let err = bus
        .unbind_wrapper(&name("ghost"), &uuid::Uuid::new_v4().to_string())
        .await
        .unwrap_err();
    assert!(matches!(err, MailboxError::Unregistered { .. }));
}

#[tokio::test]
async fn reregister_none_after_wrapper_unbind_restores_transport() {
    let (_d, bus) = fresh_bus("reregnone");
    bus.register(&name("a")).await.unwrap();
    let t = bus.bind_wrapper(&name("a"), None).await.unwrap();
    bus.unbind_wrapper(&name("a"), &t).await.unwrap();

    let listed = bus.list_agents().await.unwrap();
    assert_eq!(listed[0].nudge_transport, NudgeTransport::None);

    bus.register_with_transport(&name("a"), Some(NudgeTransport::None))
        .await
        .unwrap();
    let listed = bus.list_agents().await.unwrap();
    assert_eq!(listed[0].nudge_transport, NudgeTransport::None);
    assert!(listed[0].nudge_window.is_none());
}

#[tokio::test]
async fn register_transport_none_on_reregister() {
    let (_d, bus) = fresh_bus("regnone");
    bus.register(&name("a")).await.unwrap();
    let _t = bus.bind_wrapper(&name("a"), None).await.unwrap();
    bus.register(&name("b")).await.unwrap();

    bus.register_with_transport(&name("a"), Some(NudgeTransport::None))
        .await
        .unwrap();
    let listed = bus.list_agents().await.unwrap();
    assert_eq!(listed[0].nudge_transport, NudgeTransport::None);
    assert!(listed[0].nudge_window.is_none());
}

#[tokio::test]
async fn wake_session_unbound_then_register_then_peek() {
    let (_d, bus) = fresh_bus("wakesession");
    let wake_id = uuid::Uuid::new_v4().to_string();

    let unbound = bus.nudge_peek_by_wake(&wake_id, 0).await.unwrap();
    assert!(!unbound.bound);
    assert!(!unbound.pending);
    assert!(unbound.agent.is_empty());

    bus.register_with_wake(&name("muse"), None, Some(&wake_id))
        .await
        .unwrap();

    let bound = bus.nudge_peek_by_wake(&wake_id, 0).await.unwrap();
    assert!(bound.bound);
    assert_eq!(bound.agent, "muse");
    assert!(!bound.pending);

    bus.register(&name("peer")).await.unwrap();
    bus.send(&name("peer"), &name("muse"), "hello".into())
        .await
        .unwrap();

    let pending = bus.nudge_peek_by_wake(&wake_id, 0).await.unwrap();
    assert!(pending.pending);
    assert_eq!(pending.agent, "muse");
    assert_eq!(pending.ids.len(), 1);
}

#[tokio::test]
async fn wake_session_unbind_clears_transport() {
    let (_d, bus) = fresh_bus("wakeunbind");
    let wake_id = uuid::Uuid::new_v4().to_string();
    bus.register_with_wake(&name("a"), None, Some(&wake_id))
        .await
        .unwrap();

    bus.unbind_wake(&wake_id).await.unwrap();
    let listed = bus.list_agents().await.unwrap();
    assert_eq!(listed[0].nudge_transport, NudgeTransport::None);
}

// ---------------------------------------------------------------------------
// Canceled waiters must not consume wake signals
// ---------------------------------------------------------------------------

#[tokio::test]
async fn aborted_inbox_waiter_does_not_starve_the_next_one() {
    let (_d, bus) = fresh_bus("abortinbox");
    bus.register(&name("a")).await.unwrap();
    bus.register(&name("b")).await.unwrap();

    // Park a pull and kill its task mid-park: the receiver is dropped while
    // the waiter still sits in the queue.
    let b = bus.clone();
    let first = tokio::spawn(async move { b.check_inbox(&name("b"), 30_000, vec![]).await });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    first.abort();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    bus.send(&name("a"), &name("b"), "wake".into())
        .await
        .unwrap();

    // The next parked pull must be woken promptly, not after its full wait.
    let started = std::time::Instant::now();
    let page = bus.check_inbox(&name("b"), 30_000, vec![]).await.unwrap();
    assert_eq!(page.messages.len(), 1);
    assert!(
        started.elapsed() < std::time::Duration::from_secs(5),
        "second waiter slept {:?}; the canceled one ate the signal",
        started.elapsed()
    );
}

#[tokio::test]
async fn aborted_nudge_waiter_does_not_starve_the_next_one() {
    let (_d, bus) = fresh_bus("abortnudge");
    bus.register(&name("a")).await.unwrap();
    bus.register(&name("b")).await.unwrap();

    let b = bus.clone();
    let first = tokio::spawn(async move { b.nudge_peek(&name("b"), 30_000).await });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    first.abort();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    bus.send(&name("a"), &name("b"), "wake".into())
        .await
        .unwrap();

    let started = std::time::Instant::now();
    let peek = bus.nudge_peek(&name("b"), 30_000).await.unwrap();
    assert!(peek.pending);
    assert!(
        started.elapsed() < std::time::Duration::from_secs(5),
        "second peeker slept {:?}; the canceled one ate the signal",
        started.elapsed()
    );
}

#[tokio::test]
async fn aborted_wake_waiter_does_not_starve_the_next_one() {
    let (_d, bus) = fresh_bus("abortwake");
    bus.register(&name("a")).await.unwrap();
    bus.register(&name("b")).await.unwrap();
    let wid = uuid::Uuid::new_v4().to_string();
    bus.register_with_wake(&name("b"), None, Some(&wid))
        .await
        .unwrap();

    let b = bus.clone();
    let w = wid.clone();
    let first = tokio::spawn(async move { b.nudge_peek_by_wake(&w, 30_000).await });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    first.abort();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    bus.send(&name("a"), &name("b"), "wake".into())
        .await
        .unwrap();

    let started = std::time::Instant::now();
    let peek = bus.nudge_peek_by_wake(&wid, 30_000).await.unwrap();
    assert!(peek.pending);
    assert!(
        started.elapsed() < std::time::Duration::from_secs(5),
        "second session slept {:?}; the canceled one ate the signal",
        started.elapsed()
    );
}

// ---------------------------------------------------------------------------
// Wake-session resource bounds
// ---------------------------------------------------------------------------

#[tokio::test]
async fn unbound_wake_sessions_honor_global_cap() {
    let (_d, bus) = fresh_bus("wakecap");
    let mut tasks = Vec::new();
    for _ in 0..mentischat::bus::MAX_WAKE_WAITERS_GLOBAL {
        let b = bus.clone();
        let id = uuid::Uuid::new_v4().to_string();
        tasks.push(tokio::spawn(async move {
            b.nudge_peek_by_wake(&id, /* wait */ 30_000).await
        }));
    }
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    // One more unbound long-poll exceeds the global budget.
    match bus
        .nudge_peek_by_wake(&uuid::Uuid::new_v4().to_string(), 30_000)
        .await
    {
        Err(MailboxError::TooManyWaiters { .. }) => {}
        other => panic!("expected global too_many_waiters, got {other:?}"),
    }

    for t in tasks {
        t.abort();
    }
}

#[tokio::test]
async fn expired_wake_sessions_release_budget_and_entries() {
    let (_d, bus) = fresh_bus("wakebudget");
    // Many distinct ids whose parks expire quickly; entries and budget must
    // drain so a fresh long park still succeeds.
    for _ in 0..(mentischat::bus::MAX_WAKE_WAITERS_GLOBAL * 3) {
        let id = uuid::Uuid::new_v4().to_string();
        let peek = bus.nudge_peek_by_wake(&id, 1).await.unwrap();
        assert!(!peek.bound);
    }
    let peek = bus
        .nudge_peek_by_wake(&uuid::Uuid::new_v4().to_string(), 1)
        .await;
    assert!(peek.is_ok(), "budget leaked after expiries: {peek:?}");
}

// ---------------------------------------------------------------------------
// Ownership tokens on wrapper bindings
// ---------------------------------------------------------------------------

#[tokio::test]
async fn wrapper_replacement_survives_stale_owner_unbind() {
    let (_d, bus) = fresh_bus("ownertoken");
    bus.register(&name("a")).await.unwrap();

    let old = bus.bind_wrapper(&name("a"), None).await.unwrap();
    let new = bus.bind_wrapper(&name("a"), None).await.unwrap();
    assert_ne!(old, new);

    // The older wrapper exits after the replacement: its unbind must not
    // clear the newer binding.
    let err = bus.unbind_wrapper(&name("a"), &old).await.unwrap_err();
    assert!(matches!(err, MailboxError::Forbidden { .. }));
    let listed = bus.list_agents().await.unwrap();
    assert_eq!(listed[0].nudge_transport, NudgeTransport::Wrapper);

    // The owner can still unbind.
    bus.unbind_wrapper(&name("a"), &new).await.unwrap();
    assert_eq!(
        bus.list_agents().await.unwrap()[0].nudge_transport,
        NudgeTransport::None
    );
}

#[tokio::test]
async fn explicit_transport_none_clears_session_token() {
    let (_d, bus) = fresh_bus("clearwake");
    bus.register(&name("a")).await.unwrap();
    let wid = uuid::Uuid::new_v4().to_string();
    bus.register_with_wake(&name("a"), None, Some(&wid))
        .await
        .unwrap();

    bus.register_with_transport(&name("a"), Some(NudgeTransport::None))
        .await
        .unwrap();

    // The old /wake/{id} poller no longer resolves to this agent.
    let peek = bus.nudge_peek_by_wake(&wid, 0).await.unwrap();
    assert!(!peek.bound, "stale session stayed attached after disable");
    assert_eq!(
        bus.list_agents().await.unwrap()[0].nudge_transport,
        NudgeTransport::None
    );

    // A plain re-register keeps a live binding.
    bus.register_with_wake(&name("a"), None, Some(&uuid::Uuid::new_v4().to_string()))
        .await
        .unwrap();
    bus.register(&name("a")).await.unwrap();
    let listed = bus.list_agents().await.unwrap();
    assert_eq!(listed[0].nudge_transport, NudgeTransport::Wrapper);
}
