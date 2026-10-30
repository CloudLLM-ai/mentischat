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
