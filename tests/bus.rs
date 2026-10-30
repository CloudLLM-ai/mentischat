//! Bus behavior tests: roster semantics, caps, read-marking delivery, waiter
//! races, restarts, and lock stress, all in-process.
//! Chosen names and holds are in `tests/names.rs`, history in
//! `tests/history.rs`, and lifetimes in `tests/retention.rs`.

mod common;

use std::sync::Arc;

use common::{fresh_bus, name, reclaim, stats};
use mentischat::bus::{Bus, Registration};
use mentischat::errors::MailboxError;
use mentischat::names::AgentName;

#[tokio::test]
async fn reclaiming_a_registered_name_keeps_its_unread_mail() {
    let (_d, bus) = fresh_bus("idem");
    bus.register(&name("grok")).await.unwrap();
    bus.register(&name("opencode")).await.unwrap();
    bus.send(&name("opencode"), &name("grok"), "hi".into())
        .await
        .unwrap();

    let again = bus.register_with(&name("grok"), reclaim()).await.unwrap();
    assert!(!again.created);

    let page = bus.check_inbox(&name("grok"), 0).await.unwrap();
    assert_eq!(page.messages.len(), 1, "queue survived the reclaim");
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

    let page = bus.check_inbox(&name("b"), 0).await.unwrap();
    assert_eq!(page.messages.len(), 1);
    assert_eq!(page.messages[0].body, "hello");
    assert_eq!(page.messages[0].from, "a");
    assert_eq!(page.remaining, 0);
}

#[tokio::test]
async fn a_read_message_is_pulled_once_and_kept_as_read() {
    let (_d, bus) = fresh_bus("readonce");
    bus.register(&name("a")).await.unwrap();
    bus.register(&name("b")).await.unwrap();
    let receipt = bus.send(&name("a"), &name("b"), "m1".into()).await.unwrap();
    assert_eq!(bus.stats().await.unwrap(), stats(2, 1, 0));

    let p1 = bus.check_inbox(&name("b"), 0).await.unwrap();
    assert_eq!(p1.messages.len(), 1);
    let read_at = p1.messages[0].read_at.clone().expect("marked read");
    assert!(read_at >= receipt.created_at);
    let p2 = bus.check_inbox(&name("b"), 0).await.unwrap();
    assert!(p2.messages.is_empty(), "check_inbox returns it once");
    assert_eq!(bus.stats().await.unwrap(), stats(2, 0, 1), "kept as read");
    assert_eq!(
        bus.list_agents().await.unwrap().agents[1].queued,
        0,
        "queued counts unread mail only"
    );
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

    let p1 = bus.check_inbox(&name("b"), 0).await.unwrap();
    assert_eq!(p1.messages.len(), 8, "MAX_BATCH");
    assert_eq!(p1.remaining, 2);

    let p2 = bus.check_inbox(&name("b"), 0).await.unwrap();
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

    let page = bus.check_inbox(&name("b"), 0).await.unwrap();
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

    let err = bus.check_inbox(&name("nobody"), 0).await.unwrap_err();
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
    let page = bus.check_inbox(&name("b"), 30_000).await.unwrap();
    assert_eq!(page.messages.len(), 1);
    assert!(started.elapsed() < std::time::Duration::from_secs(1));
}

#[tokio::test]
async fn parked_waiter_wakes_on_send() {
    let (_d, bus) = fresh_bus("wake");
    bus.register(&name("a")).await.unwrap();
    bus.register(&name("b")).await.unwrap();

    let puller = bus.clone();
    let task = tokio::spawn(async move { puller.check_inbox(&name("b"), 15_000).await.unwrap() });
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

    let page = bus.check_inbox(&name("b"), 150).await.unwrap();
    assert!(page.messages.is_empty());
    assert_eq!(page.remaining, 0);

    // Mail arriving after the timed-out pull stays pending.
    bus.send(&name("a"), &name("b"), "late".into())
        .await
        .unwrap();
    let page = bus.check_inbox(&name("b"), 0).await.unwrap();
    assert_eq!(page.messages.len(), 1);
}

#[tokio::test]
async fn only_oldest_waiter_gets_the_batch() {
    let (_d, bus) = fresh_bus("two");
    bus.register(&name("a")).await.unwrap();
    bus.register(&name("b")).await.unwrap();

    let b1 = bus.clone();
    let t1 = tokio::spawn(async move { b1.check_inbox(&name("b"), 5_000).await });
    let b2 = bus.clone();
    let t2 = tokio::spawn(async move { b2.check_inbox(&name("b"), 500).await });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    bus.send(&name("a"), &name("b"), "just one".into())
        .await
        .unwrap();

    let p1 = t1.await.unwrap().unwrap();
    let p2 = t2.await.unwrap().unwrap();
    assert_eq!(p1.messages.len(), 1);
    // check_inbox hands a message out once: the other pull under the same
    // name times out empty.
    assert!(p2.messages.is_empty());
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
            b.check_inbox(&name("b"), 2_000).await
        }));
    }
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // The ninth pull attempts to park, which is what trips the cap.
    let ninth = bus.check_inbox(&name("b"), 2_000).await;
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
    let task = tokio::spawn(async move { b.check_inbox(&name("b"), 10_000).await });
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

    let page = bus.check_inbox(&name("stayer"), 0).await.unwrap();
    assert_eq!(page.messages.len(), 1, "mail leaver sent survives");
}

#[tokio::test]
async fn a_crash_leaves_nothing_behind() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("crash");

    let bus = Bus::new(&data).unwrap();
    bus.register(&name("a")).await.unwrap();
    bus.register(&name("b")).await.unwrap();
    bus.send(&name("a"), &name("b"), "gone with the process".into())
        .await
        .unwrap();
    drop(bus);

    assert!(!data.exists(), "the mailbox only lives in memory");
    let bus = Bus::new(&data).unwrap();
    assert!(bus.list_agents().await.unwrap().agents.is_empty());
    assert_eq!(bus.stats().await.unwrap(), stats(0, 0, 0));
}

#[tokio::test]
async fn graceful_shutdown_discards_roster_and_mail() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("clean");
    let bus = Bus::new(&data).unwrap();
    bus.register(&name("a")).await.unwrap();
    bus.register(&name("b")).await.unwrap();
    bus.send(&name("a"), &name("b"), "read".into())
        .await
        .unwrap();
    bus.check_inbox(&name("b"), 0).await.unwrap();
    bus.send(&name("a"), &name("b"), "gone".into())
        .await
        .unwrap();
    bus.deregister(&name("a")).await.unwrap();

    bus.shutdown().await.unwrap();

    let bus = Bus::new(&data).unwrap();
    assert!(
        bus.list_agents().await.unwrap().agents.is_empty(),
        "graceful shutdown clears the roster"
    );
    assert_eq!(
        bus.stats().await.unwrap(),
        stats(0, 0, 0),
        "graceful shutdown discards read and unread mail"
    );
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
            b.check_inbox(&name("receiver"), 0).await.unwrap()
        }));
    }
    // check_inbox is exactly-once: no id may ever arrive twice.
    for t in tasks {
        for msg in t.await.unwrap().messages {
            assert!(seen.insert(msg.id), "duplicate delivery");
        }
    }
    loop {
        let page = bus.check_inbox(&name("receiver"), 0).await.unwrap();
        for msg in &page.messages {
            assert!(seen.insert(msg.id.clone()), "duplicate delivery");
        }
        if page.messages.is_empty() && page.remaining == 0 {
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
    let task = tokio::spawn(async move { b.check_inbox(&name("b"), 10_000).await });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    bus.shutdown().await.unwrap();
    let err = task.await.unwrap().unwrap_err();
    assert!(matches!(err, MailboxError::ShuttingDown));

    let err = bus.register(&name("b")).await.unwrap_err();
    assert!(matches!(err, MailboxError::ShuttingDown));
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
    let first = tokio::spawn(async move { b.check_inbox(&name("b"), 30_000).await });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    first.abort();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    bus.send(&name("a"), &name("b"), "wake".into())
        .await
        .unwrap();

    // The next parked pull must be woken promptly, not after its full wait.
    let started = std::time::Instant::now();
    let page = bus.check_inbox(&name("b"), 30_000).await.unwrap();
    assert_eq!(page.messages.len(), 1);
    assert!(
        started.elapsed() < std::time::Duration::from_secs(5),
        "second waiter slept {:?}; the canceled one ate the signal",
        started.elapsed()
    );
}

// ---------------------------------------------------------------------------
// Roster and delivery bounds
// ---------------------------------------------------------------------------

#[tokio::test]
async fn roster_honors_max_agents() {
    let (_d, bus) = fresh_bus("rosterfull");
    for i in 0..mentischat::bus::MAX_ROSTER {
        bus.register(&name(&format!("agent_{i}"))).await.unwrap();
    }
    let err = bus.register(&name("one_too_many")).await.unwrap_err();
    assert!(
        matches!(err, MailboxError::RosterFull { agents } if agents == mentischat::bus::MAX_ROSTER),
        "got {err:?}"
    );
}

#[tokio::test]
async fn remaining_counts_what_the_next_pull_will_get() {
    let (_d, bus) = fresh_bus("remaining");
    bus.register(&name("a")).await.unwrap();
    bus.register(&name("b")).await.unwrap();
    // Three mid-size bodies: first pull takes two (12 KiB), leaves one pending.
    let body = "x".repeat(6 * 1024);
    for _ in 0..3 {
        bus.send(&name("a"), &name("b"), body.clone())
            .await
            .unwrap();
    }
    let p1 = bus.check_inbox(&name("b"), 0).await.unwrap();
    assert_eq!(p1.messages.len(), 2);
    assert_eq!(p1.remaining, 1);

    let p2 = bus.check_inbox(&name("b"), 0).await.unwrap();
    assert_eq!(p2.messages.len(), 1);
    assert_eq!(p2.remaining, 0);
    assert_ne!(p1.messages[0].id, p2.messages[0].id);
}

#[tokio::test]
async fn overview_reports_queue_depth_and_age_sorted_by_name() {
    let (_d, bus) = fresh_bus("overview");
    bus.register(&name("opencode")).await.unwrap();
    bus.register(&name("grok")).await.unwrap();
    bus.send(&name("grok"), &name("opencode"), "one".into())
        .await
        .unwrap();
    bus.send(&name("grok"), &name("opencode"), "two".into())
        .await
        .unwrap();

    let overview = bus.overview().await.unwrap();
    let names: Vec<&str> = overview.agents.iter().map(|r| r.name.as_str()).collect();
    assert_eq!(names, ["grok", "opencode"], "sorted by name");
    assert_eq!(overview.agents[0].queued, 0);
    assert_eq!(overview.agents[1].queued, 2);
    assert!(overview
        .agents
        .iter()
        .all(|r| (0..5).contains(&r.seen_secs_ago)));
}

#[tokio::test]
async fn overview_counts_messages_sent_even_after_they_are_read_or_discarded() {
    let (_d, bus) = fresh_bus("overview_sent");
    bus.register(&name("a")).await.unwrap();
    bus.register(&name("b")).await.unwrap();
    assert_eq!(bus.overview().await.unwrap().messages_sent, 0);

    bus.send(&name("a"), &name("b"), "one".into())
        .await
        .unwrap();
    bus.send(&name("a"), &name("b"), "two".into())
        .await
        .unwrap();
    bus.check_inbox(&name("b"), 0).await.unwrap();
    assert_eq!(bus.overview().await.unwrap().agents[1].queued, 0, "read");

    bus.send(&name("a"), &name("b"), "three".into())
        .await
        .unwrap();
    bus.deregister(&name("b")).await.unwrap();
    assert_eq!(
        bus.overview().await.unwrap().messages_sent,
        3,
        "read and discarded mail still counts as sent"
    );
}

#[tokio::test]
async fn overview_does_not_count_rejected_sends_or_reregistration() {
    let (_d, bus) = fresh_bus("overview_rejected");
    bus.register(&name("a")).await.unwrap();
    assert!(bus.register(&name("a")).await.is_err(), "name_taken");
    assert!(bus
        .send(&name("a"), &name("ghost"), "x".into())
        .await
        .is_err());
    assert!(bus.send(&name("a"), &name("a"), "  ".into()).await.is_err());
    assert_eq!(bus.overview().await.unwrap().messages_sent, 0);
}

#[tokio::test]
async fn overview_counts_unread_mail_and_never_consumes_it() {
    let (_d, bus) = fresh_bus("overview_unread");
    bus.register(&name("a")).await.unwrap();
    bus.register(&name("b")).await.unwrap();
    bus.send(&name("a"), &name("b"), "hi".into()).await.unwrap();

    let before = bus.overview().await.unwrap();
    let again = bus.overview().await.unwrap();
    assert_eq!(before, again, "overview is read-only");
    assert_eq!(before.agents[1].queued, 1);

    let page = bus.check_inbox(&name("b"), 0).await.unwrap();
    assert_eq!(page.messages.len(), 1, "still there for the recipient");
}

#[tokio::test]
async fn overview_fails_once_the_bus_is_closed() {
    let (_d, bus) = fresh_bus("overview_closed");
    bus.register(&name("a")).await.unwrap();
    bus.shutdown().await.unwrap();
    assert!(matches!(
        bus.overview().await,
        Err(MailboxError::ShuttingDown)
    ));
}

#[tokio::test]
async fn description_and_provider_are_set_kept_and_cleared_by_register() {
    let (_d, bus) = fresh_bus("describe");
    let set = |d: &str| Registration {
        description: Some(d.into()),
        reclaim: true,
        ..Registration::default()
    };

    let out = bus
        .register_with(
            &name("grok"),
            Registration {
                description: Some("reviews PRs".into()),
                provider: Some(mentischat::names::Provider::parse("Grok_Build").unwrap()),
                reclaim: false,
            },
        )
        .await
        .unwrap();
    assert!(out.created);
    assert_eq!(out.description.as_deref(), Some("reviews PRs"));
    assert_eq!(out.provider.as_deref(), Some("grok_build"), "lowercased");

    let kept = bus.register_with(&name("grok"), reclaim()).await.unwrap();
    assert_eq!(
        kept.description.as_deref(),
        Some("reviews PRs"),
        "a register without a description keeps the stored one"
    );
    assert_eq!(kept.provider.as_deref(), Some("grok_build"));

    bus.register_with(&name("grok"), set("writes docs"))
        .await
        .unwrap();
    let roster = bus.list_agents().await.unwrap().agents;
    assert_eq!(roster[0].description.as_deref(), Some("writes docs"));
    assert_eq!(roster[0].provider.as_deref(), Some("grok_build"));

    let cleared = bus.register_with(&name("grok"), set("")).await.unwrap();
    assert_eq!(cleared.description, None, "an empty description clears it");
    assert_eq!(bus.list_agents().await.unwrap().agents[0].description, None);
}

#[tokio::test]
async fn a_bad_description_registers_nothing() {
    let (_d, bus) = fresh_bus("bad_description");
    let bad = Registration {
        description: Some("x".repeat(257)),
        ..Registration::default()
    };
    assert!(matches!(
        bus.register_with(&name("cursor"), bad).await,
        Err(MailboxError::InvalidDescription)
    ));
    assert!(bus.list_agents().await.unwrap().agents.is_empty());
}

#[tokio::test]
async fn a_send_that_fails_stores_nothing() {
    let (_d, bus) = fresh_bus("atomic_send");
    bus.register(&name("a")).await.unwrap();
    bus.register(&name("b")).await.unwrap();
    let before = bus.list_agents().await.unwrap().agents[0].last_seen.clone();
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;

    assert!(matches!(
        bus.send(&name("a"), &name("nobody"), "lost?".into()).await,
        Err(MailboxError::Unregistered { .. })
    ));
    assert_eq!(
        bus.stats().await.unwrap(),
        stats(2, 0, 0),
        "the failed send left no message behind"
    );
    assert_eq!(
        bus.list_agents().await.unwrap().agents[0].last_seen,
        before,
        "nor did it count as activity"
    );
    assert_eq!(bus.overview().await.unwrap().messages_sent, 0);
}

#[tokio::test]
async fn list_agents_reports_queue_depth_without_delivering() {
    let (_d, bus) = fresh_bus("queued");
    bus.register(&name("a")).await.unwrap();
    bus.register(&name("b")).await.unwrap();
    for i in 0..3 {
        bus.send(&name("a"), &name("b"), format!("m{i}"))
            .await
            .unwrap();
    }
    let roster = bus.list_agents().await.unwrap().agents;
    let queued: Vec<(String, i64)> = roster.iter().map(|e| (e.name.clone(), e.queued)).collect();
    assert_eq!(queued, [("a".to_string(), 0), ("b".to_string(), 3)]);

    let page = bus.check_inbox(&name("b"), 0).await.unwrap();
    assert_eq!(page.messages.len(), 3, "listing did not consume anything");
}

#[tokio::test]
async fn abandoned_blocking_pulls_do_not_fill_the_waiter_cap() {
    let (_d, bus) = fresh_bus("abandoned");
    bus.register(&name("a")).await.unwrap();
    bus.register(&name("b")).await.unwrap();

    // Eight pulls park, then their clients go away (the futures are dropped
    // mid-await, as when an HTTP connection closes), so none of them unparks.
    let mut tasks = Vec::new();
    for _ in 0..8 {
        let b = bus.clone();
        tasks.push(tokio::spawn(async move {
            b.check_inbox(&name("b"), 30_000).await
        }));
    }
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    for t in &tasks {
        t.abort();
    }
    for t in tasks {
        assert!(t.await.unwrap_err().is_cancelled());
    }

    // A fresh blocking pull must still park, and be woken by a send.
    let b = bus.clone();
    let pull = tokio::spawn(async move { b.check_inbox(&name("b"), 5_000).await });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    bus.send(&name("a"), &name("b"), "still reachable".into())
        .await
        .unwrap();
    let page = pull.await.unwrap().expect("not too_many_waiters");
    assert_eq!(page.messages.len(), 1);
}

// current_thread runtime: after `send` returns, the woken pull has not run
// yet, so aborting it reproduces "woken, then the client went away".
#[tokio::test]
async fn a_cancelled_pull_hands_its_wake_to_the_next_waiter() {
    let (_d, bus) = fresh_bus("handoff");
    bus.register(&name("a")).await.unwrap();
    bus.register(&name("b")).await.unwrap();

    let b = bus.clone();
    let first = tokio::spawn(async move { b.check_inbox(&name("b"), 10_000).await });
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    let b = bus.clone();
    let second = tokio::spawn(async move { b.check_inbox(&name("b"), 10_000).await });
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    bus.send(&name("a"), &name("b"), "for whoever is left".into())
        .await
        .unwrap(); // wakes `first`, the oldest waiter
    first.abort();

    let started = std::time::Instant::now();
    let page = tokio::time::timeout(std::time::Duration::from_secs(2), second)
        .await
        .expect("second waiter was woken, not left to its 10 s timeout")
        .unwrap()
        .unwrap();
    assert_eq!(page.messages.len(), 1);
    assert!(started.elapsed() < std::time::Duration::from_secs(2));
}

#[tokio::test]
async fn a_wake_with_nothing_left_keeps_waiting_until_the_deadline() {
    let (_d, bus) = fresh_bus("respark");
    bus.register(&name("a")).await.unwrap();
    bus.register(&name("b")).await.unwrap();

    let b = bus.clone();
    let started = std::time::Instant::now();
    let parked = tokio::spawn(async move { b.check_inbox(&name("b"), 400).await });
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    // The send wakes the parked pull, but another pull under the same name
    // takes the message before the woken one gets the lock.
    bus.send(&name("a"), &name("b"), "taken".into())
        .await
        .unwrap();
    let page = bus.check_inbox(&name("b"), 0).await.unwrap();
    assert_eq!(page.messages.len(), 1);

    let page = parked.await.unwrap().unwrap();
    assert!(page.messages.is_empty());
    assert!(
        started.elapsed() >= std::time::Duration::from_millis(350),
        "returned empty after {:?}, before its wait_ms",
        started.elapsed()
    );
}

#[tokio::test]
async fn batch_size_counts_escaped_json_not_raw_body_bytes() {
    let (_d, bus) = fresh_bus("escaped");
    bus.register(&name("a")).await.unwrap();
    bus.register(&name("b")).await.unwrap();
    // 6000 raw bytes each, but every control character serializes as \u0001
    // (6 bytes): two raw bodies fit in 16 KiB, two escaped ones do not.
    let body = format!("x{}", "\u{1}".repeat(5999));
    for _ in 0..2 {
        bus.send(&name("a"), &name("b"), body.clone())
            .await
            .unwrap();
    }
    let page = bus.check_inbox(&name("b"), 0).await.unwrap();
    assert_eq!(page.messages.len(), 1);
    assert_eq!(page.remaining, 1);
}

#[tokio::test]
async fn delivery_order_survives_a_persisted_restart() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("seq");
    let bus = Bus::new(&data).unwrap();
    bus.register(&name("a")).await.unwrap();
    bus.register(&name("b")).await.unwrap();
    bus.send(&name("a"), &name("b"), "first".into())
        .await
        .unwrap();
    bus.close().await; // MENTISCHAT_PERSIST=1: the snapshot keeps the mailbox

    let bus = Bus::new(&data).unwrap();
    bus.send(&name("a"), &name("b"), "second".into())
        .await
        .unwrap();
    let page = bus.check_inbox(&name("b"), 0).await.unwrap();
    let bodies: Vec<&str> = page.messages.iter().map(|m| m.body.as_str()).collect();
    assert_eq!(bodies, ["first", "second"]);
}

/// Case 4 of the agent instructions: each side sends, then waits with
/// check_inbox (calling again after an empty result) until the other
/// answers. No one has to prompt either agent between rounds.
#[tokio::test]
async fn two_agents_hold_a_conversation_by_waiting_on_each_other() {
    async fn next_message(bus: &Bus, me: &str) -> String {
        for _ in 0..10 {
            let page = bus.check_inbox(&name(me), 300).await.unwrap();
            if let Some(msg) = page.messages.into_iter().next() {
                return msg.body;
            }
        }
        panic!("{me} heard nothing after 10 waits");
    }

    let (_d, bus) = fresh_bus("conversation");
    bus.register(&name("Vega")).await.unwrap();
    bus.register(&name("Orion")).await.unwrap();

    let orion = {
        let bus = bus.clone();
        tokio::spawn(async move {
            for round in 0..3 {
                let got = next_message(&bus, "Orion").await;
                assert_eq!(got, format!("Waiting for your reply. ping {round}"));
                bus.send(
                    &name("Orion"),
                    &name("Vega"),
                    format!("Waiting for your reply. pong {round}"),
                )
                .await
                .unwrap();
            }
            let last = next_message(&bus, "Orion").await;
            assert!(last.starts_with("No reply needed"), "{last}");
        })
    };
    for round in 0..3 {
        bus.send(
            &name("Vega"),
            &name("Orion"),
            format!("Waiting for your reply. ping {round}"),
        )
        .await
        .unwrap();
        assert_eq!(
            next_message(&bus, "Vega").await,
            format!("Waiting for your reply. pong {round}")
        );
    }
    bus.send(
        &name("Vega"),
        &name("Orion"),
        "No reply needed. Done.".into(),
    )
    .await
    .unwrap();
    orion.await.unwrap();

    let page = bus
        .history(&name("Vega"), Some(&name("Orion")), None)
        .await
        .unwrap();
    let bodies: Vec<&str> = page.messages.iter().map(|m| m.body.as_str()).collect();
    assert_eq!(
        bodies,
        [
            "Waiting for your reply. ping 0",
            "Waiting for your reply. pong 0",
            "Waiting for your reply. ping 1",
            "Waiting for your reply. pong 1",
            "Waiting for your reply. ping 2",
            "Waiting for your reply. pong 2",
            "No reply needed. Done.",
        ]
    );
}
