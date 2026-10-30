//! The byte budget for message bodies: direct and room mail share it, direct
//! sends delete the oldest read mail to fit, room sends push out their own
//! room's oldest, and unread direct mail is never evicted.

mod common;

use common::name;
use mentischat::bus::{Bus, MAX_RETAINED_BYTES};
use mentischat::errors::MailboxError;
use mentischat::names::RoomName;

fn bus(budget: usize) -> Bus {
    let dir = tempfile::tempdir().unwrap();
    let bus = Bus::new(&dir.path().join("budget"))
        .unwrap()
        .with_byte_budget(budget);
    std::mem::forget(dir);
    bus
}

fn body(n: usize) -> String {
    "x".repeat(n)
}

#[test]
fn the_default_budget_is_256_mib() {
    assert_eq!(MAX_RETAINED_BYTES, 256 * 1024 * 1024);
}

#[tokio::test]
async fn unread_direct_mail_is_never_evicted_so_a_full_budget_fails_storage_full() {
    let bus = bus(1000);
    bus.register(&name("a")).await.unwrap();
    bus.register(&name("b")).await.unwrap();
    for _ in 0..3 {
        bus.send(&name("a"), &name("b"), body(300)).await.unwrap();
    }
    let err = bus
        .send(&name("a"), &name("b"), body(300))
        .await
        .unwrap_err();
    assert!(
        matches!(err, MailboxError::StorageFull { pending: 3 }),
        "{err:?}"
    );
    assert_eq!(err.http_status().as_u16(), 507);
    bus.send(&name("a"), &name("b"), body(100))
        .await
        .expect("a small one still fits");
}

#[tokio::test]
async fn a_full_budget_makes_room_by_deleting_the_oldest_read_mail() {
    let bus = bus(1000);
    bus.register(&name("a")).await.unwrap();
    bus.register(&name("b")).await.unwrap();
    for i in 0..3 {
        bus.send(&name("a"), &name("b"), format!("{i}{}", body(299)))
            .await
            .unwrap();
    }
    bus.check_inbox(&name("b"), 0).await.unwrap();
    bus.send(&name("a"), &name("b"), body(300)).await.unwrap();
    let stats = bus.stats().await.unwrap();
    assert_eq!(
        (stats.unread, stats.read),
        (1, 2),
        "one read message made room"
    );
    let history = bus.history(&name("b"), None, None).await.unwrap();
    assert!(
        history.messages[0].body.starts_with('1'),
        "the oldest read one went"
    );
}

#[tokio::test]
async fn leaving_and_clearing_give_the_bytes_back() {
    let bus = bus(700);
    bus.register(&name("a")).await.unwrap();
    bus.register(&name("b")).await.unwrap();
    bus.send(&name("a"), &name("b"), body(600)).await.unwrap();
    assert!(bus.send(&name("a"), &name("b"), body(600)).await.is_err());
    bus.deregister(&name("b")).await.unwrap();
    bus.register_with(&name("b"), common::reclaim())
        .await
        .unwrap();
    bus.send(&name("a"), &name("b"), body(600))
        .await
        .expect("the discarded mail freed its bytes");
    bus.clear().await.unwrap();
    bus.register(&name("a")).await.unwrap();
    bus.register(&name("b")).await.unwrap();
    bus.send(&name("a"), &name("b"), body(600)).await.unwrap();
}

#[tokio::test]
async fn room_mail_shares_the_budget_and_a_room_pushes_out_its_own_oldest() {
    let bus = bus(1000);
    bus.register(&name("a")).await.unwrap();
    bus.register(&name("b")).await.unwrap();
    let room = RoomName::parse("lobby").unwrap();
    bus.create_room(&name("a"), &room, false, None)
        .await
        .unwrap();
    bus.join_room(&name("b"), &room).await.unwrap();
    for i in 0..5 {
        bus.send_room(&name("a"), &room, format!("{i}{}", body(299)))
            .await
            .unwrap();
    }
    let page = bus.check_inbox(&name("b"), 0).await.unwrap();
    let firsts: Vec<char> = page
        .messages
        .iter()
        .map(|m| m.body.chars().next().unwrap())
        .collect();
    assert_eq!(
        firsts,
        ['2', '3', '4'],
        "only 1000 bytes of the room were kept"
    );
    assert_eq!(page.skipped, 2);

    // Direct mail is not pushed out by a room, and a room that cannot fit
    // fails without losing anything it holds.
    bus.send(&name("a"), &name("b"), body(100)).await.unwrap();
    bus.send_room(&name("a"), &room, body(300)).await.unwrap();
    let err = bus
        .send_room(&name("a"), &room, body(950))
        .await
        .unwrap_err();
    assert!(matches!(err, MailboxError::StorageFull { .. }), "{err:?}");
    let kept = bus.room_history(&name("a"), &room, None).await.unwrap();
    assert_eq!(kept.messages.len(), 3, "the failed send evicted nothing");
    bus.leave_room(&name("a"), &room).await.unwrap();
    bus.leave_room(&name("b"), &room).await.unwrap();
    bus.send(&name("a"), &name("b"), body(300))
        .await
        .expect("the deleted room freed its bytes");
}
