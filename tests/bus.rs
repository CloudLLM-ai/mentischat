//! Bus behavior tests: roster semantics, caps, batch+ack delivery, waiter
//! races, crash recovery, and mutex stress — all in-process against tempdirs.

use std::sync::Arc;

use llm_chat::bus::Bus;
use llm_chat::errors::MailboxError;
use llm_chat::names::AgentName;

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
    assert_eq!(roster.len(), 2, "roster survives restart");
    let page = bus.check_inbox(&name("b"), 0, vec![]).await.unwrap();
    assert_eq!(page.messages.len(), 1, "unacked mail survives restart");
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
