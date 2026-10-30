//! A pull looks only at the rooms that may hold mail for the agent. These
//! check that no room's mail is lost or repeated by that: leftovers past a
//! page, mail in one of many rooms, a room that overflowed, and a pull that
//! finds nothing in between.

use std::collections::HashSet;

use mentischat::bus::{Bus, MAX_BATCH};
use mentischat::names::{AgentName, RoomName};

fn name(s: &str) -> AgentName {
    AgentName::parse(s).expect("valid test name")
}

fn room(s: &str) -> RoomName {
    RoomName::parse(s).expect("valid test room")
}

/// Fifty quiet rooms and one busy one; the busy one holds more than a page.
/// Every message comes out exactly once across pulls, an empty pull between
/// them changes nothing, and the quiet rooms never produce mail.
#[tokio::test]
async fn test_leftover_room_mail_is_pulled_once_among_many_rooms() {
    let dir = tempfile::tempdir().expect("tempdir");
    let bus = Bus::new(&dir.path().join("bus")).expect("bus");
    let (me, other) = (name("me"), name("other"));
    bus.register(&me).await.unwrap();
    bus.register(&other).await.unwrap();
    for i in 0..50 {
        let r = room(&format!("quiet{i}"));
        bus.create_room(&me, &r, false, None).await.unwrap();
    }
    let busy = room("busy");
    bus.create_room(&other, &busy, false, None).await.unwrap();
    bus.join_room(&me, &busy).await.unwrap();
    let total = MAX_BATCH as usize * 2 + 5;
    for i in 0..total {
        bus.send_room(&other, &busy, format!("m{i}")).await.unwrap();
    }

    let mut seen = HashSet::new();
    let mut pulls = 0;
    loop {
        let page = bus.check_inbox(&me, 0).await.unwrap();
        if page.messages.is_empty() {
            break;
        }
        pulls += 1;
        for m in &page.messages {
            assert!(seen.insert(m.body.clone()), "{} delivered twice", m.body);
        }
        if page.remaining == 0 {
            break;
        }
    }
    assert_eq!(seen.len(), total);
    assert!(pulls >= 3);
    assert!(bus.check_inbox(&me, 0).await.unwrap().messages.is_empty());

    bus.send_room(&other, &busy, "later".into()).await.unwrap();
    let page = bus.check_inbox(&me, 0).await.unwrap();
    assert_eq!(page.messages.len(), 1);
    assert_eq!(page.messages[0].body, "later");
}

/// A member that falls behind a small room is told how much it missed even
/// though its other rooms are quiet.
#[tokio::test]
async fn test_skipped_is_reported_once_for_a_room_that_overflowed() {
    let dir = tempfile::tempdir().expect("tempdir");
    let bus = Bus::new(&dir.path().join("bus"))
        .expect("bus")
        .with_room_capacity(3);
    let (me, other) = (name("me"), name("other"));
    bus.register(&me).await.unwrap();
    bus.register(&other).await.unwrap();
    bus.create_room(&me, &room("quiet"), false, None)
        .await
        .unwrap();
    let busy = room("busy");
    bus.create_room(&other, &busy, false, None).await.unwrap();
    bus.join_room(&me, &busy).await.unwrap();
    for i in 0..5 {
        bus.send_room(&other, &busy, format!("m{i}")).await.unwrap();
    }
    let page = bus.check_inbox(&me, 0).await.unwrap();
    assert_eq!(page.messages.len(), 3);
    assert_eq!(page.skipped, 2);
    let again = bus.check_inbox(&me, 0).await.unwrap();
    assert!(again.messages.is_empty());
    assert_eq!(again.skipped, 0);
}
