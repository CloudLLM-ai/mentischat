//! Scale: the roster and room caps, paged listings that cost a page however
//! large the roster, and the per-agent and per-room caps that keep a send and
//! a pull bounded.

mod common;

use common::{fresh_bus, name};
use mentischat::bus::{
    Bus, MAX_PAGE, MAX_ROOMS, MAX_ROOMS_PER_AGENT, MAX_ROOM_MEMBERS, MAX_ROSTER,
};
use mentischat::errors::MailboxError;
use mentischat::names::RoomName;

fn room(s: &str) -> RoomName {
    RoomName::parse(s).unwrap()
}

async fn register_many(bus: &Bus, n: usize) {
    for i in 0..n {
        bus.register(&name(&format!("agent{i:05}"))).await.unwrap();
    }
}

#[test]
fn the_caps_are_sized_for_many_agents_and_rooms() {
    assert_eq!(MAX_ROSTER, 65_536);
    assert_eq!(MAX_ROOMS, 65_536);
    assert_eq!(MAX_PAGE, 256);
    assert_eq!(MAX_ROOMS_PER_AGENT, 256);
    assert_eq!(MAX_ROOM_MEMBERS, 1024);
}

#[tokio::test]
async fn list_agents_pages_through_a_large_roster() {
    let (_d, bus) = fresh_bus("pages");
    register_many(&bus, 600).await;

    let first = bus.list_agents().await.unwrap();
    assert_eq!(first.agents.len(), MAX_PAGE);
    assert_eq!(first.next.as_deref(), Some("agent00255"));
    assert!(
        !first.suggestions.is_empty(),
        "suggestions come with every page"
    );

    let mut seen = Vec::new();
    let mut after = None;
    loop {
        let page = bus.list_agents_page(after.as_ref(), 100).await.unwrap();
        assert!(page.agents.len() <= 100);
        seen.extend(page.agents.iter().map(|a| a.name.clone()));
        match page.next {
            Some(next) => after = Some(name(&next)),
            None => break,
        }
    }
    let expected: Vec<String> = (0..600).map(|i| format!("agent{i:05}")).collect();
    assert_eq!(seen, expected, "every agent once, in order");
}

#[tokio::test]
async fn a_short_roster_has_no_next() {
    let (_d, bus) = fresh_bus("short");
    register_many(&bus, 3).await;
    let json = serde_json::to_value(bus.list_agents().await.unwrap()).unwrap();
    assert!(
        json.get("next").is_none(),
        "absent when nothing follows: {json}"
    );
    assert_eq!(json["agents"].as_array().unwrap().len(), 3);
}

#[tokio::test]
async fn unregistered_lists_a_bounded_roster_and_name_taken_still_suggests() {
    let (_d, bus) = fresh_bus("known");
    register_many(&bus, 600).await;
    match bus.check_inbox(&name("ghost"), 0).await {
        Err(MailboxError::Unregistered { known, .. }) => {
            assert_eq!(known.len(), 256, "the first 256 names");
            assert_eq!(known[0], "agent00000");
        }
        other => panic!("{other:?}"),
    }
    match bus.register(&name("agent00007")).await {
        Err(MailboxError::NameTaken { suggestions, .. }) => assert!(!suggestions.is_empty()),
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
async fn list_rooms_pages_and_skips_private_rooms_it_cannot_show() {
    let (_d, bus) = fresh_bus("room_pages");
    register_many(&bus, 8).await;
    let viewer = name("agent00001");
    for i in 0..300 {
        // Six creators keep each under the per-agent cap; every third room
        // is private.
        let creator = name(&format!("agent{:05}", 2 + i % 6));
        bus.create_room(&creator, &room(&format!("room{i:04}")), i % 3 == 0, None)
            .await
            .unwrap();
    }
    let mut seen = Vec::new();
    let mut after = None;
    loop {
        let page = bus
            .list_rooms_page(&viewer, after.as_ref(), 50)
            .await
            .unwrap();
        assert!(page.rooms.len() <= 50);
        seen.extend(page.rooms.iter().map(|r| r.room.clone()));
        match page.next {
            Some(next) => after = Some(room(&next)),
            None => break,
        }
    }
    let expected: Vec<String> = (0..300)
        .filter(|i| i % 3 != 0)
        .map(|i| format!("room{i:04}"))
        .collect();
    assert_eq!(
        seen, expected,
        "the viewer sees only the public rooms, all of them"
    );

    // A creator also sees its own private rooms.
    let own = bus.list_rooms(&name("agent00002")).await.unwrap();
    assert_eq!(
        own.rooms.len(),
        // Its rooms are i = 0, 6, 12, ...: all private, 50 of them.
        200 + 50,
        "public rooms plus its own private ones"
    );
}

#[tokio::test]
async fn an_agent_is_in_a_bounded_number_of_rooms() {
    let (_d, bus) = fresh_bus("per_agent");
    register_many(&bus, 2).await;
    let (a, b) = (name("agent00000"), name("agent00001"));
    for i in 0..MAX_ROOMS_PER_AGENT {
        bus.create_room(&a, &room(&format!("r{i}")), false, None)
            .await
            .unwrap();
    }
    assert!(matches!(
        bus.create_room(&a, &room("one-too-many"), false, None)
            .await,
        Err(MailboxError::TooManyRooms { .. })
    ));
    bus.create_room(&b, &room("elsewhere"), false, None)
        .await
        .unwrap();
    assert!(matches!(
        bus.join_room(&a, &room("elsewhere")).await,
        Err(MailboxError::TooManyRooms { .. })
    ));
    bus.leave_room(&a, &room("r0")).await.unwrap();
    bus.join_room(&a, &room("elsewhere")).await.unwrap();
}

#[tokio::test]
async fn a_room_has_a_bounded_number_of_members() {
    let (_d, bus) = fresh_bus("members");
    register_many(&bus, MAX_ROOM_MEMBERS + 1).await;
    let lobby = room("lobby");
    bus.create_room(&name("agent00000"), &lobby, false, None)
        .await
        .unwrap();
    for i in 1..MAX_ROOM_MEMBERS {
        bus.join_room(&name(&format!("agent{i:05}")), &lobby)
            .await
            .unwrap();
    }
    let err = bus
        .join_room(&name(&format!("agent{MAX_ROOM_MEMBERS:05}")), &lobby)
        .await
        .unwrap_err();
    assert!(
        matches!(err, MailboxError::RoomFull { members, .. } if members == 1024),
        "{err:?}"
    );
    assert_eq!(err.http_status().as_u16(), 409);
    bus.send_room(&name("agent00000"), &lobby, "hi all".into())
        .await
        .unwrap();
    assert_eq!(
        bus.list_agents_page(None, 1).await.unwrap().agents[0].queued,
        0
    );
}

#[tokio::test]
async fn deleting_rooms_and_agents_in_bulk_stays_correct() {
    let (_d, bus) = fresh_bus("bulk");
    register_many(&bus, 50).await;
    for i in 0..50 {
        let who = name(&format!("agent{i:05}"));
        for j in 0..20 {
            // Rooms shared by neighbors, created once.
            let r = room(&format!("shared{j}-{}", i / 2));
            if i % 2 == 0 {
                bus.create_room(&who, &r, false, None).await.unwrap();
            } else {
                bus.join_room(&who, &r).await.unwrap();
            }
        }
    }
    for i in 0..50 {
        bus.deregister(&name(&format!("agent{i:05}")))
            .await
            .unwrap();
    }
    bus.register(&name("agent00000")).await.expect_err("held");
    bus.register_with(&name("agent00000"), common::reclaim())
        .await
        .unwrap();
    assert!(
        bus.list_rooms(&name("agent00000"))
            .await
            .unwrap()
            .rooms
            .is_empty(),
        "every room emptied and was deleted"
    );
    assert_eq!(bus.stats().await.unwrap().unread, 0);
}

#[tokio::test]
async fn the_overview_reads_a_bounded_number_of_rows_but_counts_the_whole_roster() {
    let (_d, bus) = fresh_bus("overview");
    register_many(&bus, 1100).await;
    let ov = bus.overview().await.unwrap();
    assert_eq!(ov.agents.len(), mentischat::bus::MAX_OVERVIEW_AGENTS);
    assert_eq!(ov.agents_total, 1100);
}

/// Private rooms the viewer cannot see are never visited: a page costs the
/// public rooms and the viewer's own, however many private rooms exist.
#[tokio::test]
async fn list_rooms_does_not_visit_private_rooms_it_cannot_show() {
    let (_d, bus) = fresh_bus("private_walk");
    register_many(&bus, 41).await;
    let viewer = name("agent00040");
    for i in 0..40 {
        let creator = name(&format!("agent{i:05}"));
        for j in 0..200 {
            bus.create_room(&creator, &room(&format!("secret{i:02}-{j:03}")), true, None)
                .await
                .unwrap();
        }
    }
    bus.create_room(&name("agent00000"), &room("open"), false, None)
        .await
        .unwrap();
    bus.invite(&name("agent00001"), &room("secret01-000"), &viewer)
        .await
        .unwrap();

    let started = std::time::Instant::now();
    let listing = bus.list_rooms(&viewer).await.unwrap();
    let elapsed = started.elapsed();
    let names: Vec<&str> = listing.rooms.iter().map(|r| r.room.as_str()).collect();
    assert_eq!(
        names,
        ["open", "secret01-000"],
        "public plus the one invited to"
    );
    assert!(
        elapsed < std::time::Duration::from_millis(50),
        "8000 private rooms must not be walked: {elapsed:?}"
    );
}
