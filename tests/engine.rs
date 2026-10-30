//! Engine internals seen from the public API: message sizing for pages,
//! ids and cursors, and logs that outlive many pruned messages.

mod common;

use common::{fresh_bus, name};
use mentischat::bus::{Message, MAX_RETURN_BYTES};

/// The serialized size of the message `body` makes, with `readAt` as given,
/// for ids of one digit and the names `a` and `b`.
async fn wire_size(body: &str, read: bool) -> usize {
    let (_d, bus) = fresh_bus("probe");
    bus.register(&name("a")).await.unwrap();
    bus.register(&name("b")).await.unwrap();
    bus.send(&name("a"), &name("b"), body.into()).await.unwrap();
    let mut msg: Message = bus
        .check_inbox(&name("b"), 0)
        .await
        .unwrap()
        .messages
        .remove(0);
    if !read {
        msg.read_at = None;
    }
    serde_json::to_vec(&msg).unwrap().len()
}

/// Send two messages sized so their JSON adds up to `budget` bytes (plus
/// `extra`), and report how many the first page of `pull` returns.
async fn page_len_for(budget: usize, extra: usize, read: bool, history: bool) -> usize {
    let overhead = wire_size("x", read).await - 1;
    let total_body = budget + extra - 2 * overhead;
    let (first, second) = (total_body / 2, total_body - total_body / 2);
    let (_d, bus) = fresh_bus("sized");
    bus.register(&name("a")).await.unwrap();
    bus.register(&name("b")).await.unwrap();
    for len in [first, second] {
        bus.send(&name("a"), &name("b"), "x".repeat(len))
            .await
            .unwrap();
    }
    if history {
        bus.check_inbox(&name("b"), 0).await.unwrap();
        return bus
            .history(&name("b"), None, None)
            .await
            .unwrap()
            .messages
            .len();
    }
    bus.check_inbox(&name("b"), 0).await.unwrap().messages.len()
}

#[tokio::test]
async fn an_inbox_page_is_sized_by_the_exact_serialized_bytes() {
    // A pull sizes messages as unread, `"readAt":null`.
    assert_eq!(page_len_for(MAX_RETURN_BYTES, 0, false, false).await, 2);
    assert_eq!(page_len_for(MAX_RETURN_BYTES, 1, false, false).await, 1);
}

#[tokio::test]
async fn a_history_page_is_sized_by_the_exact_serialized_bytes_with_read_at() {
    assert_eq!(page_len_for(MAX_RETURN_BYTES, 0, true, true).await, 2);
    assert_eq!(page_len_for(MAX_RETURN_BYTES, 1, true, true).await, 1);
}

#[tokio::test]
async fn ids_increase_and_are_the_history_cursor() {
    let (_d, bus) = fresh_bus("ids");
    bus.register(&name("a")).await.unwrap();
    bus.register(&name("b")).await.unwrap();
    let mut ids = Vec::new();
    for i in 0..20 {
        let receipt = bus
            .send(&name("a"), &name("b"), format!("m{i}"))
            .await
            .unwrap();
        ids.push(receipt.id.parse::<i64>().expect("ids are decimal numbers"));
    }
    assert!(ids.windows(2).all(|w| w[0] < w[1]));

    let page = bus.history(&name("a"), None, None).await.unwrap();
    assert_eq!(
        page.next,
        Some(ids[12]),
        "next is the oldest id on the page"
    );
    let older = bus.history(&name("a"), None, page.next).await.unwrap();
    assert_eq!(older.messages[7].id, ids[11].to_string());
}

/// A conversation whose oldest message stays unread while thousands of later
/// ones are read and pruned: its log is compacted over and over and history
/// must still page through it without gaps or repeats.
#[tokio::test]
async fn a_log_survives_being_pruned_and_compacted_around_an_unread_message() {
    let (_d, bus) = fresh_bus("compact");
    bus.register(&name("a")).await.unwrap();
    bus.register(&name("b")).await.unwrap();
    bus.send(&name("a"), &name("b"), "first, never read".into())
        .await
        .unwrap();
    for i in 0..9_000 {
        bus.send(&name("b"), &name("a"), format!("m{i}"))
            .await
            .unwrap();
        bus.check_inbox(&name("a"), 0).await.unwrap();
    }
    let mut ids = Vec::new();
    let mut before = None;
    loop {
        let page = bus.history(&name("a"), None, before).await.unwrap();
        ids.extend(
            page.messages
                .iter()
                .rev()
                .map(|m| m.id.parse::<u64>().unwrap()),
        );
        before = page.next;
        if before.is_none() {
            break;
        }
    }
    assert!(
        ids.windows(2).all(|w| w[0] > w[1]),
        "newest first, no repeats"
    );
    assert_eq!(ids.last(), Some(&1), "the oldest message is still there");
    assert!(ids.len() < 9_001, "pruned mail is gone from history");
}

/// Senders race a recipient that keeps leaving and coming back, and pullers
/// race the senders. Whatever interleaving happens, the global counts must
/// agree with the per-agent queues at the end.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn counts_stay_consistent_while_agents_come_and_go() {
    let (_d, bus) = fresh_bus("churn");
    for who in ["flaky", "s0", "s1", "s2", "s3"] {
        bus.register(&name(who)).await.unwrap();
    }
    let senders: Vec<_> = (0..4)
        .map(|i| {
            let bus = bus.clone();
            tokio::spawn(async move {
                let from = name(&format!("s{i}"));
                for _ in 0..2_000 {
                    // Fails `unregistered` while flaky is away; that is fine.
                    let _ = bus.send(&from, &name("flaky"), "x".into()).await;
                    let _ = bus.send(&from, &name("s0"), "y".into()).await;
                }
            })
        })
        .collect();
    let churn = {
        let bus = bus.clone();
        tokio::spawn(async move {
            for _ in 0..300 {
                let _ = bus.deregister(&name("flaky")).await;
                let _ = bus.register_with(&name("flaky"), common::reclaim()).await;
                let _ = bus.check_inbox(&name("flaky"), 0).await;
            }
        })
    };
    for task in senders {
        task.await.unwrap();
    }
    churn.await.unwrap();
    bus.register_with(&name("flaky"), common::reclaim())
        .await
        .unwrap();

    let stats = bus.stats().await.unwrap();
    let queued: i64 = bus
        .list_agents()
        .await
        .unwrap()
        .agents
        .iter()
        .map(|a| a.queued)
        .sum();
    assert_eq!(stats.unread, queued, "unread counter matches the queues");
    assert!(stats.unread + stats.read <= 8192, "the cap held: {stats:?}");
}

/// A pull that parks while a send lands must be woken, not left to sleep to
/// its deadline. The window is a few instructions wide, so this races a send
/// against a fresh blocking pull thousands of times and gives each one a
/// generous but finite time to finish (a lost wake costs the full 30 s).
/// Probabilistic: it fails reliably only when the bug is present and the
/// threads interleave, which on a multi-core machine they do within the loop.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_send_racing_a_parking_pull_is_never_slept_through() {
    let (_d, bus) = fresh_bus("lost_wake");
    bus.register(&name("a")).await.unwrap();
    bus.register(&name("b")).await.unwrap();
    for round in 0..3_000 {
        let puller = bus.clone();
        let pull = tokio::spawn(async move { puller.check_inbox(&name("b"), 30_000).await });
        let sender = bus.clone();
        let send = tokio::spawn(async move {
            sender
                .send(&name("a"), &name("b"), "x".into())
                .await
                .unwrap();
        });
        send.await.unwrap();
        let page = tokio::time::timeout(std::time::Duration::from_secs(2), pull)
            .await
            .unwrap_or_else(|_| panic!("round {round}: the pull slept through its wake"))
            .unwrap()
            .unwrap();
        assert_eq!(page.messages.len(), 1, "round {round}");
    }
}

/// Sends racing a `clear` must not leave a live-message count behind: when
/// every agent is gone the mailbox is empty, whatever was in flight.
/// Probabilistic, like the test above.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_clear_racing_sends_leaves_no_phantom_count() {
    let (_d, bus) = fresh_bus("phantom");
    for round in 0..500 {
        for who in ["a", "b"] {
            bus.register(&name(who)).await.unwrap();
        }
        let senders: Vec<_> = (0..4)
            .map(|_| {
                let bus = bus.clone();
                tokio::spawn(async move {
                    for _ in 0..50 {
                        let _ = bus.send(&name("a"), &name("b"), "x".into()).await;
                    }
                })
            })
            .collect();
        tokio::task::yield_now().await;
        bus.clear().await.unwrap();
        for task in senders {
            task.await.unwrap();
        }
        assert_eq!(
            bus.stats().await.unwrap(),
            common::stats(0, 0, 0),
            "round {round}"
        );
    }
}

/// Agents leaving while they send must not make the sent counter drift from
/// the number of sends that succeeded.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_sent_counter_matches_the_sends_that_succeeded_through_churn() {
    let (_d, bus) = fresh_bus("sent_count");
    for who in ["flaky", "s0", "s1"] {
        bus.register(&name(who)).await.unwrap();
    }
    let ok = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let senders: Vec<_> = ["flaky", "s0", "s1"]
        .into_iter()
        .map(|who| {
            let (bus, ok) = (bus.clone(), ok.clone());
            tokio::spawn(async move {
                for _ in 0..2_000 {
                    if bus.send(&name(who), &name("s0"), "x".into()).await.is_ok() {
                        ok.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                }
            })
        })
        .collect();
    let churn = {
        let bus = bus.clone();
        tokio::spawn(async move {
            for _ in 0..200 {
                let _ = bus.deregister(&name("flaky")).await;
                let _ = bus.register_with(&name("flaky"), common::reclaim()).await;
                let _ = bus.overview().await;
            }
        })
    };
    for task in senders {
        task.await.unwrap();
    }
    churn.await.unwrap();
    assert_eq!(
        bus.overview().await.unwrap().messages_sent,
        ok.load(std::sync::atomic::Ordering::Relaxed)
    );
}
