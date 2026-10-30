//! Streams at the bus level: leases, acknowledgements, return on drop,
//! the in-flight window, takeover, and the interplay with pulls, history, the
//! sweep and a persisted shutdown.

mod common;

use std::time::Duration;

use common::{bus_with, fresh_bus, idle_after, later, name};
use mentischat::bus::{Bus, StreamSession, MAX_STREAM_IN_FLIGHT};
use mentischat::names::RoomName;

fn bodies(messages: &[mentischat::bus::Message]) -> Vec<&str> {
    messages.iter().map(|m| m.body.as_str()).collect()
}

async fn pair(bus: &Bus) {
    bus.register(&name("a")).await.unwrap();
    bus.register(&name("b")).await.unwrap();
}

fn open(bus: &Bus, who: &str) -> StreamSession {
    bus.open_stream(&name(who)).unwrap()
}

#[tokio::test]
async fn a_pushed_message_is_leased_not_read_until_it_is_acknowledged() {
    let (_d, bus) = fresh_bus("lease");
    pair(&bus).await;
    let stream = open(&bus, "b");
    bus.send(&name("a"), &name("b"), "one".into())
        .await
        .unwrap();
    bus.send(&name("a"), &name("b"), "two".into())
        .await
        .unwrap();

    let push = bus.stream_push(&stream).unwrap().expect("mail to push");
    assert_eq!(bodies(&push.messages), ["one", "two"]);
    assert!(push.messages.iter().all(|m| m.read_at.is_none()));
    assert!(bus.stream_push(&stream).unwrap().is_none(), "nothing more");

    // Leased mail is not for pulls, and is still unread for everyone else.
    assert!(bus
        .check_inbox(&name("b"), 0)
        .await
        .unwrap()
        .messages
        .is_empty());
    let sent = bus.history(&name("a"), None, None).await.unwrap();
    assert!(sent.messages.iter().all(|m| m.read_at.is_none()));
    assert!(bus
        .history(&name("b"), None, None)
        .await
        .unwrap()
        .messages
        .is_empty());
    assert_eq!(bus.list_agents().await.unwrap().agents[1].queued, 2);
    assert_eq!(bus.stats().await.unwrap().unread, 2);

    let last: u64 = push.messages[1].id.parse().unwrap();
    assert_eq!(bus.stream_ack(&stream, last).unwrap(), 2);
    let sent = bus.history(&name("a"), None, None).await.unwrap();
    assert!(
        sent.messages.iter().all(|m| m.read_at.is_some()),
        "acked is read"
    );
    assert_eq!(
        bus.history(&name("b"), None, None)
            .await
            .unwrap()
            .messages
            .len(),
        2
    );
    assert_eq!(bus.stats().await.unwrap().unread, 0);
    drop(stream);
    assert!(
        bus.check_inbox(&name("b"), 0)
            .await
            .unwrap()
            .messages
            .is_empty(),
        "not redelivered"
    );
}

#[tokio::test]
async fn unacknowledged_mail_returns_in_order_when_the_stream_ends() {
    let (_d, bus) = fresh_bus("return");
    pair(&bus).await;
    let stream = open(&bus, "b");
    for body in ["m1", "m2", "m3"] {
        bus.send(&name("a"), &name("b"), body.into()).await.unwrap();
    }
    let push = bus.stream_push(&stream).unwrap().unwrap();
    // m1 is acknowledged; m2 and m3 are not.
    bus.stream_ack(&stream, push.messages[0].id.parse().unwrap())
        .unwrap();
    bus.send(&name("a"), &name("b"), "m4".into()).await.unwrap();
    drop(stream);

    let page = bus.check_inbox(&name("b"), 0).await.unwrap();
    assert_eq!(
        bodies(&page.messages),
        ["m2", "m3", "m4"],
        "order preserved"
    );
    assert_eq!(bus.stats().await.unwrap().unread, 0);
}

#[tokio::test]
async fn a_new_stream_gets_what_the_last_one_never_acknowledged() {
    let (_d, bus) = fresh_bus("takeover");
    pair(&bus).await;
    let first = open(&bus, "b");
    bus.send(&name("a"), &name("b"), "x".into()).await.unwrap();
    bus.send(&name("a"), &name("b"), "y".into()).await.unwrap();
    let pushed = bus.stream_push(&first).unwrap().unwrap();
    assert_eq!(pushed.messages.len(), 2);

    let second = open(&bus, "b");
    assert!(first.taken_over());
    assert!(!second.taken_over());
    // The old stream can no longer lease or acknowledge.
    assert!(bus.stream_push(&first).unwrap().is_none());
    assert_eq!(bus.stream_ack(&first, u64::MAX).unwrap(), 0);
    let again = bus.stream_push(&second).unwrap().unwrap();
    assert_eq!(
        bodies(&again.messages),
        ["x", "y"],
        "returned first, in order"
    );
    // Dropping the old session does not take the new one's leases.
    drop(first);
    assert_eq!(bus.stream_ack(&second, u64::MAX).unwrap(), 2);
}

#[tokio::test]
async fn at_most_the_window_is_in_flight_until_acknowledged() {
    let (_d, bus) = fresh_bus("window");
    pair(&bus).await;
    let stream = open(&bus, "b");
    for i in 0..300 {
        bus.send(&name("a"), &name("b"), format!("m{i}"))
            .await
            .unwrap();
    }
    let first = bus.stream_push(&stream).unwrap().unwrap();
    assert_eq!(first.messages.len(), MAX_STREAM_IN_FLIGHT);
    assert!(
        bus.stream_push(&stream).unwrap().is_none(),
        "the window is full"
    );

    let tenth: u64 = first.messages[9].id.parse().unwrap();
    assert_eq!(bus.stream_ack(&stream, tenth).unwrap(), 10);
    let next = bus.stream_push(&stream).unwrap().unwrap();
    assert_eq!(
        next.messages.len(),
        10,
        "acks free exactly that much window"
    );
    assert_eq!(next.messages[0].body, "m256");
    assert!(bus.stream_push(&stream).unwrap().is_none());
    bus.stream_ack(&stream, u64::MAX).unwrap();
    let rest = bus.stream_push(&stream).unwrap().unwrap();
    assert_eq!(rest.messages.len(), 300 - 266);
}

#[tokio::test]
async fn the_window_is_also_a_byte_budget() {
    let (_d, bus) = fresh_bus("bytes");
    pair(&bus).await;
    let stream = open(&bus, "b");
    let big = "x".repeat(60 * 1024);
    for _ in 0..30 {
        bus.send(&name("a"), &name("b"), big.clone()).await.unwrap();
    }
    let push = bus.stream_push(&stream).unwrap().unwrap();
    assert!(
        push.messages.len() >= 16 && push.messages.len() <= 18,
        "{}",
        push.messages.len()
    );
    // The first message of a push always fits, so the window can pass 1 MiB
    // by at most one message; then it is full.
    let more = bus
        .stream_push(&stream)
        .unwrap()
        .map_or(0, |p| p.messages.len());
    assert!(more <= 1, "{more}");
    assert!(bus.stream_push(&stream).unwrap().is_none());
}

#[tokio::test]
async fn room_mail_is_leased_and_wound_back_on_drop() {
    let (_d, bus) = fresh_bus("rooms");
    pair(&bus).await;
    let lobby = RoomName::parse("lobby").unwrap();
    bus.create_room(&name("a"), &lobby, false, None)
        .await
        .unwrap();
    bus.join_room(&name("b"), &lobby).await.unwrap();
    let stream = open(&bus, "b");
    bus.send(&name("a"), &name("b"), "dm".into()).await.unwrap();
    bus.send_room(&name("a"), &lobby, "r1".into())
        .await
        .unwrap();
    bus.send_room(&name("a"), &lobby, "r2".into())
        .await
        .unwrap();

    let push = bus.stream_push(&stream).unwrap().unwrap();
    assert_eq!(bodies(&push.messages), ["dm", "r1", "r2"]);
    assert_eq!(push.messages[1].room.as_deref(), Some("lobby"));
    assert!(bus
        .check_inbox(&name("b"), 0)
        .await
        .unwrap()
        .messages
        .is_empty());

    bus.stream_ack(&stream, push.messages[1].id.parse().unwrap())
        .unwrap();
    drop(stream);
    let page = bus.check_inbox(&name("b"), 0).await.unwrap();
    assert_eq!(
        bodies(&page.messages),
        ["r2"],
        "only the unacknowledged room message returns"
    );
    assert_eq!(page.skipped, 0);
}

#[tokio::test]
async fn room_mail_pushed_out_while_leased_is_reported_skipped_on_return() {
    let dir = tempfile::tempdir().unwrap();
    let bus = Bus::new(&dir.path().join("evict"))
        .unwrap()
        .with_room_capacity(3);
    pair(&bus).await;
    let lobby = RoomName::parse("lobby").unwrap();
    bus.create_room(&name("a"), &lobby, false, None)
        .await
        .unwrap();
    bus.join_room(&name("b"), &lobby).await.unwrap();
    let stream = open(&bus, "b");
    for i in 0..3 {
        bus.send_room(&name("a"), &lobby, format!("r{i}"))
            .await
            .unwrap();
    }
    assert_eq!(bus.stream_push(&stream).unwrap().unwrap().messages.len(), 3);
    for i in 3..5 {
        bus.send_room(&name("a"), &lobby, format!("r{i}"))
            .await
            .unwrap();
    }
    drop(stream); // r0 and r1 were pushed out of the room while leased.
    let page = bus.check_inbox(&name("b"), 0).await.unwrap();
    assert_eq!(bodies(&page.messages), ["r2", "r3", "r4"]);
    assert_eq!(page.skipped, 2);
}

#[tokio::test]
async fn a_pull_beside_a_stream_gets_only_what_is_not_leased() {
    let (_d, bus) = fresh_bus("beside");
    pair(&bus).await;
    let stream = open(&bus, "b");
    bus.send(&name("a"), &name("b"), "leased".into())
        .await
        .unwrap();
    bus.stream_push(&stream).unwrap().unwrap();
    bus.send(&name("a"), &name("b"), "fresh".into())
        .await
        .unwrap();
    let page = bus.check_inbox(&name("b"), 0).await.unwrap();
    assert_eq!(bodies(&page.messages), ["fresh"]);
}

#[tokio::test]
async fn a_send_wakes_a_stream_that_is_waiting() {
    let (_d, bus) = fresh_bus("wake");
    pair(&bus).await;
    let stream = open(&bus, "b");
    let wake = stream.wake();
    tokio::pin!(wake);
    wake.as_mut().enable();
    assert!(bus.stream_push(&stream).unwrap().is_none());
    bus.send(&name("a"), &name("b"), "now".into())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), wake)
        .await
        .expect("woken by the send");
    assert_eq!(
        bodies(&bus.stream_push(&stream).unwrap().unwrap().messages),
        ["now"]
    );
}

#[tokio::test]
async fn an_open_stream_keeps_its_agent_off_the_idle_sweep() {
    let (_d, bus) = bus_with("sweep", idle_after(3600));
    pair(&bus).await;
    let stream = open(&bus, "b");
    let out = bus.sweep_at(later(2 * 3600)).await.unwrap();
    assert_eq!(
        out.dropped_agents,
        ["a"],
        "b has a stream, like a parked pull"
    );
    drop(stream);
    let out = bus.sweep_at(later(2 * 3600)).await.unwrap();
    assert_eq!(out.dropped_agents, ["b"]);
}

#[tokio::test]
async fn deregistering_discards_leased_mail_and_closes_the_books() {
    let (_d, bus) = fresh_bus("dereg");
    pair(&bus).await;
    let stream = open(&bus, "b");
    bus.send(&name("a"), &name("b"), "x".into()).await.unwrap();
    bus.stream_push(&stream).unwrap().unwrap();
    let out = bus.deregister(&name("b")).await.unwrap();
    assert_eq!(out.discarded, 1);
    assert_eq!(bus.stats().await.unwrap().unread, 0);
    assert!(bus.stream_push(&stream).is_err());
}

#[tokio::test]
async fn leases_return_before_a_persisted_shutdown_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("persist");
    let bus = Bus::new(&data).unwrap();
    pair(&bus).await;
    let lobby = RoomName::parse("lobby").unwrap();
    bus.create_room(&name("a"), &lobby, false, None)
        .await
        .unwrap();
    bus.join_room(&name("b"), &lobby).await.unwrap();
    let stream = open(&bus, "b");
    bus.send(&name("a"), &name("b"), "dm".into()).await.unwrap();
    bus.send_room(&name("a"), &lobby, "room".into())
        .await
        .unwrap();
    assert_eq!(bus.stream_push(&stream).unwrap().unwrap().messages.len(), 2);
    bus.close().await;
    assert!(bus.stream_push(&stream).is_err(), "closed");

    let bus = Bus::new(&data).unwrap();
    let page = bus.check_inbox(&name("b"), 0).await.unwrap();
    assert_eq!(
        bodies(&page.messages),
        ["dm", "room"],
        "in flight at shutdown, unread after"
    );
}

#[tokio::test]
async fn opening_a_stream_needs_a_registered_agent() {
    let (_d, bus) = fresh_bus("who");
    assert!(bus.open_stream(&name("ghost")).is_err());
    assert!(bus.check_registered(&name("ghost")).is_err());
    pair(&bus).await;
    assert!(bus.check_registered(&name("a")).is_ok());
}
