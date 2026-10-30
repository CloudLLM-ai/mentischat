//! `check_inbox` with a `limit`: bigger pages to drain a backlog, with the
//! byte cap raised to 1 MiB; without one, the usual 8 messages / 16 KiB.

mod common;

use common::{fresh_bus, name};
use mentischat::bus::{MAX_LIMITED_PAGE_BYTES, MAX_PAGE};
use mentischat::names::RoomName;

#[test]
fn the_limited_page_caps_at_one_mib() {
    assert_eq!(MAX_LIMITED_PAGE_BYTES, 1024 * 1024);
}

#[tokio::test]
async fn without_a_limit_the_page_is_unchanged() {
    let (_d, bus) = fresh_bus("nolimit");
    bus.register(&name("a")).await.unwrap();
    bus.register(&name("b")).await.unwrap();
    for i in 0..20 {
        bus.send(&name("a"), &name("b"), format!("m{i}"))
            .await
            .unwrap();
    }
    let page = bus.check_inbox(&name("b"), 0).await.unwrap();
    assert_eq!((page.messages.len(), page.remaining), (8, 12));
    let page = bus.check_inbox_limit(&name("b"), 0, None).await.unwrap();
    assert_eq!((page.messages.len(), page.remaining), (8, 4));
}

#[tokio::test]
async fn a_limit_drains_a_backlog_in_one_page() {
    let (_d, bus) = fresh_bus("limit");
    bus.register(&name("a")).await.unwrap();
    bus.register(&name("b")).await.unwrap();
    for i in 0..300 {
        bus.send(&name("a"), &name("b"), format!("m{i}"))
            .await
            .unwrap();
    }
    let page = bus.check_inbox_limit(&name("b"), 0, Some(3)).await.unwrap();
    assert_eq!((page.messages.len(), page.remaining), (3, 297));
    assert_eq!(page.messages[0].body, "m0");
    let page = bus
        .check_inbox_limit(&name("b"), 0, Some(256))
        .await
        .unwrap();
    assert_eq!((page.messages.len(), page.remaining), (256, 41));
    let page = bus
        .check_inbox_limit(&name("b"), 0, Some(10_000))
        .await
        .unwrap();
    assert_eq!(
        page.messages.len(),
        41,
        "clamped to {MAX_PAGE}, and only 41 were left"
    );
    assert_eq!(page.remaining, 0);
    let page = bus.check_inbox_limit(&name("b"), 0, Some(0)).await.unwrap();
    assert!(page.messages.is_empty());
}

#[tokio::test]
async fn a_limit_raises_the_byte_cap_and_the_first_message_always_fits() {
    let (_d, bus) = fresh_bus("bytes");
    bus.register(&name("a")).await.unwrap();
    bus.register(&name("b")).await.unwrap();
    let body = "x".repeat(6 * 1024);
    for _ in 0..40 {
        bus.send(&name("a"), &name("b"), body.clone())
            .await
            .unwrap();
    }
    let default = bus.check_inbox(&name("b"), 0).await.unwrap();
    assert_eq!(default.messages.len(), 2, "16 KiB holds two of 6 KiB");
    let big = bus
        .check_inbox_limit(&name("b"), 0, Some(200))
        .await
        .unwrap();
    assert_eq!(
        big.messages.len(),
        38,
        "all that were left fit in 1 MiB: {}",
        big.remaining
    );

    // 20 messages of 60 KiB: 17 fit in 1 MiB, the 18th does not.
    let huge = "y".repeat(60 * 1024);
    for _ in 0..20 {
        bus.send(&name("a"), &name("b"), huge.clone())
            .await
            .unwrap();
    }
    let page = bus
        .check_inbox_limit(&name("b"), 0, Some(256))
        .await
        .unwrap();
    assert!(
        page.messages.len() >= 16 && page.messages.len() < 20,
        "{}",
        page.messages.len()
    );
    assert!(page.remaining > 0);
}

#[tokio::test]
async fn a_limit_applies_to_the_merge_of_direct_and_room_mail() {
    let (_d, bus) = fresh_bus("merge");
    bus.register(&name("a")).await.unwrap();
    bus.register(&name("b")).await.unwrap();
    let room = RoomName::parse("lobby").unwrap();
    bus.create_room(&name("a"), &room, false, None)
        .await
        .unwrap();
    bus.join_room(&name("b"), &room).await.unwrap();
    for i in 0..30 {
        bus.send(&name("a"), &name("b"), format!("dm{i}"))
            .await
            .unwrap();
        bus.send_room(&name("a"), &room, format!("room{i}"))
            .await
            .unwrap();
    }
    let page = bus
        .check_inbox_limit(&name("b"), 0, Some(40))
        .await
        .unwrap();
    assert_eq!(page.messages.len(), 40);
    assert_eq!(page.remaining, 20);
    let bodies: Vec<&str> = page.messages.iter().map(|m| m.body.as_str()).collect();
    assert_eq!(
        &bodies[..4],
        ["dm0", "room0", "dm1", "room1"],
        "oldest first across both"
    );
    let rest = bus
        .check_inbox_limit(&name("b"), 0, Some(40))
        .await
        .unwrap();
    assert_eq!((rest.messages.len(), rest.remaining), (20, 0));
}
