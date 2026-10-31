//! Pages serialize with their JSON keys sorted, at every level. REST writes
//! pull and history pages straight from these types (no `Value` detour), so
//! the declared field order is the wire's key order.

mod common;

use common::{fresh_bus, name, room};
use serde::Serialize;

/// `to_value` re-sorts object keys, so equal strings mean the direct
/// serialization was already sorted.
fn assert_sorted(page: &impl Serialize) {
    let direct = serde_json::to_string(page).unwrap();
    let sorted = serde_json::to_string(&serde_json::to_value(page).unwrap()).unwrap();
    assert_eq!(direct, sorted);
}

/// Inbox and history pages, with direct and room mail, read and unread,
/// serialize with sorted keys.
#[tokio::test]
async fn inbox_and_history_pages_serialize_with_sorted_keys() {
    let (_d, bus) = fresh_bus("wire_order");
    for n in ["a", "b"] {
        bus.register(&name(n)).await.unwrap();
    }
    bus.create_room(&name("a"), &room("r"), false, None)
        .await
        .unwrap();
    bus.join_room(&name("b"), &room("r")).await.unwrap();
    bus.send(&name("a"), &name("b"), "dm".into()).await.unwrap();
    bus.send_room(&name("a"), &room("r"), "hi".into())
        .await
        .unwrap();

    let inbox = bus.check_inbox(&name("b"), 0).await.unwrap();
    assert_eq!(inbox.messages.len(), 2);
    assert_sorted(&inbox);
    assert_sorted(&bus.history(&name("a"), None, None).await.unwrap());
    assert_sorted(&bus.history(&name("b"), None, None).await.unwrap());
}
