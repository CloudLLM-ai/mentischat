//! History: what an agent sent and has read, oldest first, filtered by peer,
//! paged by size, and scoped to the current holder of a name.

mod common;

use std::time::Duration;

use common::{bus_with_clock, fresh_bus, name, reclaim, stats};
use mentischat::bus::{Bus, HistoryPage, Retention};
use mentischat::errors::MailboxError;

async fn register(bus: &Bus, names: &[&str]) {
    for n in names {
        bus.register(&name(n)).await.unwrap();
    }
}

async fn send(bus: &Bus, from: &str, to: &str, body: &str) {
    bus.send(&name(from), &name(to), body.into()).await.unwrap();
}

async fn history(bus: &Bus, agent: &str) -> HistoryPage {
    bus.history(&name(agent), None, None).await.unwrap()
}

fn bodies(page: &HistoryPage) -> Vec<&str> {
    page.messages.iter().map(|m| m.body.as_str()).collect()
}

/// Every page of `agent`'s history, newest page first.
async fn all_pages(bus: &Bus, agent: &str) -> Vec<HistoryPage> {
    let mut pages = Vec::new();
    let mut before = None;
    loop {
        let page = bus.history(&name(agent), None, before).await.unwrap();
        before = page.next;
        pages.push(page);
        if before.is_none() {
            return pages;
        }
    }
}

#[tokio::test]
async fn history_shows_sent_and_read_mail_but_not_unread_mail() {
    let (_d, bus) = fresh_bus("basics");
    register(&bus, &["a", "b"]).await;
    send(&bus, "a", "b", "one").await;
    send(&bus, "b", "a", "two").await;
    send(&bus, "a", "b", "three").await;

    let page = history(&bus, "a").await;
    assert_eq!(
        bodies(&page),
        ["one", "three"],
        "unread 'two' is left to check_inbox"
    );
    assert!(page.messages.iter().all(|m| m.read_at.is_none()));
    assert_eq!(page.next, None);

    let inbox = bus.check_inbox(&name("a"), 0).await.unwrap();
    assert_eq!(inbox.messages[0].body, "two");
    let page = history(&bus, "a").await;
    assert_eq!(bodies(&page), ["one", "two", "three"], "oldest first");
    assert_eq!(page.messages[1].read_at, inbox.messages[0].read_at);

    bus.check_inbox(&name("b"), 0).await.unwrap();
    let page = history(&bus, "a").await;
    assert!(
        page.messages.iter().all(|m| m.read_at.is_some()),
        "the sender sees when its mail was read"
    );
    assert_eq!(bodies(&history(&bus, "b").await), ["one", "two", "three"]);
    assert_eq!(
        bus.check_inbox(&name("a"), 0).await.unwrap().messages.len(),
        0,
        "history does not re-queue anything"
    );
}

#[tokio::test]
async fn with_keeps_one_conversation_whatever_its_case() {
    let (_d, bus) = fresh_bus("with");
    register(&bus, &["a", "b", "Cygnus"]).await;
    send(&bus, "a", "b", "ab").await;
    send(&bus, "a", "Cygnus", "ac").await;
    send(&bus, "Cygnus", "a", "ca").await;
    send(&bus, "b", "a", "ba").await;
    bus.check_inbox(&name("a"), 0).await.unwrap();

    for spelling in ["Cygnus", "cygnus"] {
        let page = bus
            .history(&name("a"), Some(&name(spelling)), None)
            .await
            .unwrap();
        assert_eq!(bodies(&page), ["ac", "ca"], "{spelling}");
    }
    let page = bus
        .history(&name("a"), Some(&name("b")), None)
        .await
        .unwrap();
    assert_eq!(bodies(&page), ["ab", "ba"]);
    let page = bus
        .history(&name("a"), Some(&name("nobody")), None)
        .await
        .unwrap();
    assert!(page.messages.is_empty(), "an unknown peer is just empty");
}

#[tokio::test]
async fn pages_walk_back_without_gaps_or_repeats() {
    let (_d, bus) = fresh_bus("paging");
    register(&bus, &["a", "b"]).await;
    for i in 0..20 {
        send(&bus, "a", "b", &format!("m{i:02}")).await;
    }
    let pages = all_pages(&bus, "a").await;
    let sizes: Vec<usize> = pages.iter().map(|p| p.messages.len()).collect();
    assert_eq!(sizes, [8, 8, 4]);
    assert_eq!(bodies(&pages[0]).first(), Some(&"m12"), "the newest page");
    let walked: Vec<String> = pages
        .iter()
        .rev()
        .flat_map(|p| p.messages.iter().map(|m| m.body.clone()))
        .collect();
    let expected: Vec<String> = (0..20).map(|i| format!("m{i:02}")).collect();
    assert_eq!(walked, expected);
}

#[tokio::test]
async fn pages_respect_the_byte_budget_and_an_oversized_message_rides_alone() {
    let (_d, bus) = fresh_bus("budget");
    register(&bus, &["a", "b"]).await;
    send(&bus, "a", "b", &"x".repeat(20 * 1024)).await;
    for _ in 0..5 {
        send(&bus, "a", "b", &"y".repeat(6 * 1024)).await;
    }
    let sizes: Vec<usize> = all_pages(&bus, "a")
        .await
        .iter()
        .map(|p| p.messages.len())
        .collect();
    assert_eq!(sizes, [2, 2, 1, 1], "two 6 KiB bodies per 16 KiB page");
}

#[tokio::test]
async fn a_cursor_before_everything_is_an_empty_last_page() {
    let (_d, bus) = fresh_bus("cursor_end");
    register(&bus, &["a", "b"]).await;
    send(&bus, "a", "b", "only").await;
    let page = bus.history(&name("a"), None, Some(1)).await.unwrap();
    assert!(page.messages.is_empty());
    assert_eq!(page.next, None);
    let page = bus.history(&name("a"), None, Some(i64::MAX)).await.unwrap();
    assert_eq!(bodies(&page), ["only"]);
}

#[tokio::test]
async fn history_needs_a_registered_name() {
    let (_d, bus) = fresh_bus("unreg");
    register(&bus, &["a"]).await;
    match bus.history(&name("ghost"), None, None).await {
        Err(MailboxError::Unregistered { agent, known }) => {
            assert_eq!(agent, "ghost");
            assert_eq!(known, ["a"]);
        }
        other => panic!("expected unregistered, got {other:?}"),
    }
}

#[tokio::test]
async fn history_counts_as_activity_and_changes_nothing_else() {
    let (_d, bus, clock) = bus_with_clock("activity", Retention::default());
    register(&bus, &["a", "b"]).await;
    send(&bus, "b", "a", "unread").await;
    let before = bus.list_agents().await.unwrap().agents[0].last_seen.clone();
    clock.advance(Duration::from_millis(5));

    history(&bus, "a").await;
    let after = bus.list_agents().await.unwrap().agents[0].last_seen.clone();
    assert!(after > before, "history refreshed lastSeen");
    assert_eq!(bus.stats().await.unwrap(), stats(2, 1, 0));
}

#[tokio::test]
async fn a_reclaimed_name_keeps_its_sent_history_and_a_new_holder_sees_none() {
    let retention = Retention {
        name_hold: Duration::from_millis(150),
        ..Retention::default()
    };
    let (_dir, bus, clock) = bus_with_clock("scoped", retention);
    register(&bus, &["Vega", "Orion"]).await;
    send(&bus, "Vega", "Orion", "from the first Vega").await;
    bus.check_inbox(&name("Orion"), 0).await.unwrap();

    bus.deregister(&name("Vega")).await.unwrap();
    bus.register_with(&name("Vega"), reclaim()).await.unwrap();
    assert_eq!(
        bodies(&history(&bus, "Vega").await),
        ["from the first Vega"],
        "reclaim resumes the registration"
    );

    bus.deregister(&name("Vega")).await.unwrap();
    clock.advance(Duration::from_millis(250));
    bus.register(&name("Vega")).await.unwrap();
    assert!(
        history(&bus, "Vega").await.messages.is_empty(),
        "a new holder does not see the old one's mail"
    );
    assert_eq!(
        bodies(&history(&bus, "Orion").await),
        ["from the first Vega"],
        "the recipient keeps what it read"
    );
}

#[tokio::test]
async fn a_message_to_yourself_appears_once() {
    let (_d, bus) = fresh_bus("self");
    register(&bus, &["a"]).await;
    send(&bus, "a", "a", "note to self").await;
    assert_eq!(bodies(&history(&bus, "a").await), ["note to self"]);
    bus.check_inbox(&name("a"), 0).await.unwrap();
    assert_eq!(bodies(&history(&bus, "a").await), ["note to self"]);
}

#[tokio::test]
async fn with_and_before_page_back_through_one_conversation() {
    let (_d, bus) = fresh_bus("with_before");
    register(&bus, &["a", "b", "c"]).await;
    for i in 0..12 {
        send(&bus, "a", "b", &format!("ab{i:02}")).await;
        send(&bus, "a", "c", &format!("ac{i:02}")).await;
    }
    let mut walked = Vec::new();
    let mut before = None;
    loop {
        let page = bus
            .history(&name("a"), Some(&name("b")), before)
            .await
            .unwrap();
        assert!(page.messages.iter().all(|m| m.to == "b"), "only a ↔ b");
        walked.splice(0..0, page.messages.iter().map(|m| m.body.clone()));
        before = page.next;
        if before.is_none() {
            break;
        }
    }
    let expected: Vec<String> = (0..12).map(|i| format!("ab{i:02}")).collect();
    assert_eq!(walked, expected);
}
