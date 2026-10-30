//! Rooms at the bus level: membership, private rooms, fan-out, the merge of
//! room and direct mail in `check_inbox`, capacity and `skipped`, history,
//! and what leaving does.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{bus_with, fresh_bus, idle_after, later, name};
use mentischat::bus::{Bus, InboxPage};
use mentischat::errors::MailboxError;
use mentischat::names::RoomName;

fn room(s: &str) -> RoomName {
    RoomName::parse(s).expect("valid test room")
}

async fn register(bus: &Bus, names: &[&str]) {
    for n in names {
        bus.register(&name(n)).await.unwrap();
    }
}

async fn say(bus: &Bus, from: &str, in_room: &str, body: &str) {
    bus.send_room(&name(from), &room(in_room), body.into())
        .await
        .unwrap();
}

async fn pull(bus: &Bus, who: &str) -> InboxPage {
    bus.check_inbox(&name(who), 0).await.unwrap()
}

fn bodies(page: &InboxPage) -> Vec<&str> {
    page.messages.iter().map(|m| m.body.as_str()).collect()
}

/// A room with capacity `n` and `members` already in it (the first creates it).
async fn room_of(n: usize, members: &[&str]) -> Arc<Bus> {
    let dir = tempfile::tempdir().unwrap();
    let bus = Arc::new(
        Bus::new(&dir.path().join("rooms"))
            .unwrap()
            .with_room_capacity(n),
    );
    std::mem::forget(dir);
    register(&bus, members).await;
    bus.create_room(&name(members[0]), &room("lobby"), false, None)
        .await
        .unwrap();
    for m in &members[1..] {
        bus.join_room(&name(m), &room("lobby")).await.unwrap();
    }
    bus
}

#[test]
fn room_names_follow_the_agent_name_grammar() {
    assert!(RoomName::parse("release-war-room.v2").is_ok());
    for bad in ["", "-x", "has space", &"x".repeat(64)] {
        assert!(
            matches!(RoomName::parse(bad), Err(MailboxError::InvalidRoom { .. })),
            "{bad:?}"
        );
    }
    assert_eq!(RoomName::parse("Lobby").unwrap().folded(), "lobby");
}

#[tokio::test]
async fn a_room_has_a_name_a_creator_and_members_who_joined() {
    let (_d, bus) = fresh_bus("create");
    register(&bus, &["a", "b"]).await;
    let info = bus
        .create_room(&name("a"), &room("Lobby"), false, Some("  chat  ".into()))
        .await
        .unwrap();
    assert_eq!(info.room, "Lobby");
    assert_eq!(info.description.as_deref(), Some("chat"));
    assert_eq!(info.members, ["a"]);

    assert!(matches!(
        bus.create_room(&name("b"), &room("lobby"), false, None)
            .await,
        Err(MailboxError::RoomExists { .. })
    ));
    let joined = bus.join_room(&name("b"), &room("LOBBY")).await.unwrap();
    assert_eq!(joined.members, ["a", "b"]);
    assert_eq!(
        bus.join_room(&name("b"), &room("lobby")).await.unwrap(),
        joined,
        "joining twice changes nothing"
    );
    assert!(matches!(
        bus.join_room(&name("b"), &room("nowhere")).await,
        Err(MailboxError::RoomNotFound { .. })
    ));
    assert!(matches!(
        bus.create_room(&name("ghost"), &room("x"), false, None)
            .await,
        Err(MailboxError::Unregistered { .. })
    ));
}

#[tokio::test]
async fn a_private_room_is_invite_only_and_unlisted() {
    let (_d, bus) = fresh_bus("private");
    register(&bus, &["a", "b", "c"]).await;
    bus.create_room(&name("a"), &room("den"), true, None)
        .await
        .unwrap();
    bus.create_room(&name("a"), &room("plaza"), false, None)
        .await
        .unwrap();

    let seen = |who: &'static str| {
        let bus = bus.clone();
        async move {
            let listing = bus.list_rooms(&name(who)).await.unwrap();
            listing
                .rooms
                .into_iter()
                .map(|r| r.room)
                .collect::<Vec<_>>()
        }
    };
    assert_eq!(seen("a").await, ["den", "plaza"]);
    assert_eq!(seen("b").await, ["plaza"], "den is unlisted");
    assert!(matches!(
        bus.join_room(&name("b"), &room("den")).await,
        Err(MailboxError::PrivateRoom { .. })
    ));
    assert!(matches!(
        bus.invite(&name("b"), &room("den"), &name("c")).await,
        Err(MailboxError::NotAMember { .. })
    ));

    bus.invite(&name("a"), &room("den"), &name("b"))
        .await
        .unwrap();
    assert_eq!(seen("b").await, ["den", "plaza"], "invitees see it");
    assert_eq!(seen("c").await, ["plaza"]);
    let joined = bus.join_room(&name("b"), &room("den")).await.unwrap();
    assert_eq!(joined.members, ["a", "b"]);
    assert!(matches!(
        bus.invite(&name("a"), &room("den"), &name("ghost")).await,
        Err(MailboxError::Unregistered { .. })
    ));
}

#[tokio::test]
async fn a_room_message_reaches_every_member_but_its_sender() {
    let bus = room_of(1024, &["a", "b", "c"]).await;
    bus.register(&name("outsider")).await.unwrap();
    say(&bus, "a", "lobby", "hello all").await;

    assert_eq!(bodies(&pull(&bus, "b").await), ["hello all"]);
    assert_eq!(bodies(&pull(&bus, "c").await), ["hello all"]);
    assert!(pull(&bus, "a").await.messages.is_empty(), "not its own");
    assert!(pull(&bus, "outsider").await.messages.is_empty());
    assert!(matches!(
        bus.send_room(&name("outsider"), &room("lobby"), "hi".into())
            .await,
        Err(MailboxError::NotAMember { .. })
    ));
    assert!(matches!(
        bus.send_room(&name("a"), &room("nowhere"), "hi".into())
            .await,
        Err(MailboxError::RoomNotFound { .. })
    ));
    assert!(matches!(
        bus.send_room(&name("a"), &room("lobby"), "  ".into()).await,
        Err(MailboxError::InvalidBody)
    ));
}

#[tokio::test]
async fn room_messages_carry_room_and_direct_mail_keeps_to() {
    let bus = room_of(1024, &["a", "b"]).await;
    say(&bus, "a", "lobby", "to the room").await;
    bus.send(&name("a"), &name("b"), "to b".into())
        .await
        .unwrap();

    let page = serde_json::to_value(pull(&bus, "b").await).unwrap();
    let msgs = page["messages"].as_array().unwrap();
    assert_eq!(msgs[0]["room"], "lobby");
    assert!(msgs[0].get("to").is_none(), "{msgs:?}");
    assert_eq!(msgs[1]["to"], "b");
    assert!(msgs[1].get("room").is_none());
    assert!(page.get("skipped").is_none(), "skipped only when > 0");
}

#[tokio::test]
async fn check_inbox_merges_direct_and_room_mail_oldest_first_with_exact_remaining() {
    let bus = room_of(1024, &["a", "b"]).await;
    bus.register(&name("c")).await.unwrap();
    bus.create_room(&name("c"), &room("side"), false, None)
        .await
        .unwrap();
    bus.join_room(&name("b"), &room("side")).await.unwrap();
    // Twelve messages to b through three paths, interleaved.
    for i in 0..4 {
        bus.send(&name("a"), &name("b"), format!("dm{i}"))
            .await
            .unwrap();
        say(&bus, "a", "lobby", &format!("lobby{i}")).await;
        say(&bus, "c", "side", &format!("side{i}")).await;
    }
    let first = pull(&bus, "b").await;
    assert_eq!(
        bodies(&first),
        ["dm0", "lobby0", "side0", "dm1", "lobby1", "side1", "dm2", "lobby2"]
    );
    assert_eq!(first.remaining, 4);
    let second = pull(&bus, "b").await;
    assert_eq!(bodies(&second), ["side2", "dm3", "lobby3", "side3"]);
    assert_eq!(second.remaining, 0);
    assert!(pull(&bus, "b").await.messages.is_empty());
}

#[tokio::test]
async fn new_members_start_at_the_head_and_history_shows_the_past() {
    let bus = room_of(1024, &["a", "b"]).await;
    say(&bus, "a", "lobby", "before late joined").await;
    pull(&bus, "b").await;
    bus.register(&name("late")).await.unwrap();
    bus.join_room(&name("late"), &room("lobby")).await.unwrap();
    assert!(pull(&bus, "late").await.messages.is_empty());
    say(&bus, "b", "lobby", "after").await;
    assert_eq!(bodies(&pull(&bus, "late").await), ["after"]);

    let page = bus
        .room_history(&name("late"), &room("lobby"), None)
        .await
        .unwrap();
    let seen: Vec<&str> = page.messages.iter().map(|m| m.body.as_str()).collect();
    assert_eq!(
        seen,
        ["before late joined", "after"],
        "retained log, oldest first"
    );
    assert!(page
        .messages
        .iter()
        .all(|m| m.room.as_deref() == Some("lobby")));
    assert!(matches!(
        bus.room_history(&name("a"), &room("nowhere"), None).await,
        Err(MailboxError::RoomNotFound { .. })
    ));
}

#[tokio::test]
async fn room_history_pages_back_by_cursor() {
    let bus = room_of(1024, &["a", "b"]).await;
    for i in 0..20 {
        say(&bus, "a", "lobby", &format!("m{i}")).await;
    }
    let mut walked = Vec::new();
    let mut before = None;
    loop {
        let page = bus
            .room_history(&name("b"), &room("lobby"), before)
            .await
            .unwrap();
        walked.splice(0..0, page.messages.iter().map(|m| m.body.clone()));
        before = page.next;
        if before.is_none() {
            break;
        }
    }
    let expected: Vec<String> = (0..20).map(|i| format!("m{i}")).collect();
    assert_eq!(walked, expected);
}

#[tokio::test]
async fn a_full_room_pushes_out_the_oldest_and_tells_the_laggard_what_it_missed() {
    let bus = room_of(3, &["a", "slow", "quick"]).await;
    for i in 0..5 {
        say(&bus, "a", "lobby", &format!("m{i}")).await;
    }
    // quick keeps up: it reads two, then the room moves on.
    let page = pull(&bus, "quick").await;
    assert_eq!(bodies(&page), ["m2", "m3", "m4"], "only 3 were ever kept");
    assert_eq!(page.skipped, 2);
    say(&bus, "a", "lobby", "m5").await;

    let page = serde_json::to_value(pull(&bus, "quick").await).unwrap();
    assert!(page.get("skipped").is_none(), "reported once");
    let slow = pull(&bus, "slow").await;
    assert_eq!(bodies(&slow), ["m3", "m4", "m5"]);
    assert_eq!(slow.skipped, 3, "m0, m1 and m2 were pushed out unread");
    assert_eq!(slow.remaining, 0);
}

#[tokio::test]
async fn a_pull_that_lost_everything_still_reports_it_and_does_not_park() {
    // Capacity 1: b's own message pushes out the one a sent before b read it.
    let bus = room_of(1, &["a", "b"]).await;
    say(&bus, "a", "lobby", "x").await;
    say(&bus, "b", "lobby", "y").await;
    let started = std::time::Instant::now();
    let page = bus.check_inbox(&name("b"), 5_000).await.unwrap();
    assert!(page.messages.is_empty());
    assert_eq!(page.skipped, 1);
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "nothing to wait for"
    );
}

#[tokio::test]
async fn a_parked_pull_wakes_on_a_room_message() {
    let bus = room_of(1024, &["a", "b"]).await;
    let puller = bus.clone();
    let parked = tokio::spawn(async move { puller.check_inbox(&name("b"), 10_000).await });
    tokio::time::sleep(Duration::from_millis(80)).await;
    say(&bus, "a", "lobby", "wake up").await;
    let page = tokio::time::timeout(Duration::from_secs(2), parked)
        .await
        .expect("woken by the room message")
        .unwrap()
        .unwrap();
    assert_eq!(bodies(&page), ["wake up"]);
}

#[tokio::test]
async fn the_last_member_leaving_deletes_the_room() {
    let (_d, bus) = fresh_bus("leave");
    register(&bus, &["a", "b"]).await;
    bus.create_room(&name("a"), &room("tmp"), false, None)
        .await
        .unwrap();
    bus.join_room(&name("b"), &room("tmp")).await.unwrap();

    assert!(
        !bus.leave_room(&name("a"), &room("tmp"))
            .await
            .unwrap()
            .deleted
    );
    assert!(matches!(
        bus.leave_room(&name("a"), &room("tmp")).await,
        Err(MailboxError::NotAMember { .. })
    ));
    let out = bus.leave_room(&name("b"), &room("tmp")).await.unwrap();
    assert!(out.deleted);
    assert!(bus.list_rooms(&name("a")).await.unwrap().rooms.is_empty());
    bus.create_room(&name("a"), &room("tmp"), false, None)
        .await
        .expect("the name is free again");
}

#[tokio::test]
async fn leaving_stops_room_mail_and_deregistering_leaves_every_room() {
    let bus = room_of(1024, &["a", "b", "c"]).await;
    say(&bus, "a", "lobby", "one").await;
    bus.leave_room(&name("b"), &room("lobby")).await.unwrap();
    assert!(
        pull(&bus, "b").await.messages.is_empty(),
        "left before reading"
    );
    say(&bus, "a", "lobby", "two").await;
    assert!(pull(&bus, "b").await.messages.is_empty());

    bus.deregister(&name("c")).await.unwrap();
    let rooms = bus.list_rooms(&name("a")).await.unwrap().rooms;
    assert_eq!(rooms[0].members, 1, "c left with its registration");
    bus.deregister(&name("a")).await.unwrap();
    assert!(bus.list_rooms(&name("b")).await.unwrap().rooms.is_empty());
}

#[tokio::test]
async fn an_idle_agent_dropped_by_the_sweep_leaves_its_rooms() {
    let (_d, bus) = bus_with("idle_rooms", idle_after(3600));
    register(&bus, &["a", "b"]).await;
    bus.create_room(&name("a"), &room("lobby"), false, None)
        .await
        .unwrap();
    bus.join_room(&name("b"), &room("lobby")).await.unwrap();
    let out = bus.sweep_at(later(2 * 3600)).await.unwrap();
    assert_eq!(out.dropped_agents, ["a", "b"]);
    bus.register(&name("a")).await.expect_err("held");
    bus.register_with(&name("a"), common::reclaim())
        .await
        .unwrap();
    assert!(bus.list_rooms(&name("a")).await.unwrap().rooms.is_empty());
}

#[tokio::test]
async fn list_rooms_reports_membership_and_unread_counts() {
    let bus = room_of(1024, &["a", "b"]).await;
    say(&bus, "a", "lobby", "1").await;
    say(&bus, "a", "lobby", "2").await;
    let for_b = bus.list_rooms(&name("b")).await.unwrap().rooms;
    assert_eq!(
        (for_b[0].joined, for_b[0].unread, for_b[0].members),
        (true, 2, 2)
    );
    let for_a = bus.list_rooms(&name("a")).await.unwrap().rooms;
    assert_eq!(for_a[0].unread, 0, "a's own messages are not unread");
    assert_eq!(
        bus.list_agents().await.unwrap().agents[1].queued,
        2,
        "queued counts room mail too"
    );
}

#[tokio::test]
async fn rooms_survive_a_persisted_restart_with_cursors_and_members() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("persist");
    let bus = Bus::new(&data).unwrap();
    register(&bus, &["a", "b"]).await;
    bus.create_room(&name("a"), &room("den"), true, Some("secret".into()))
        .await
        .unwrap();
    bus.invite(&name("a"), &room("den"), &name("b"))
        .await
        .unwrap();
    bus.join_room(&name("b"), &room("den")).await.unwrap();
    say(&bus, "a", "den", "read").await;
    pull(&bus, "b").await;
    say(&bus, "a", "den", "unread").await;
    bus.close().await;

    let bus = Bus::new(&data).unwrap();
    let rooms = bus.list_rooms(&name("b")).await.unwrap().rooms;
    assert_eq!(
        (rooms[0].room.as_str(), rooms[0].private, rooms[0].unread),
        ("den", true, 1)
    );
    assert_eq!(bodies(&pull(&bus, "b").await), ["unread"]);
    let page = bus
        .room_history(&name("a"), &room("den"), None)
        .await
        .unwrap();
    assert_eq!(page.messages.len(), 2);
}

#[tokio::test]
async fn room_mail_does_not_count_against_the_direct_mail_caps() {
    let bus = room_of(1024, &["a", "b"]).await;
    for i in 0..1100 {
        say(&bus, "a", "lobby", &format!("m{i}")).await;
    }
    let stats = bus.stats().await.unwrap();
    assert_eq!((stats.unread, stats.read), (0, 0));
    bus.send(&name("a"), &name("b"), "dm still fits".into())
        .await
        .unwrap();
}
