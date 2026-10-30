//! The optional snapshot (`MENTISCHAT_PERSIST=1`): what `Bus::close` writes,
//! what the next `Bus::new` brings back, and what it refuses.

mod common;

use std::os::unix::fs::PermissionsExt;

use common::{name, reclaim, stats};
use mentischat::bus::{remove_snapshot, Bus, SnapshotError};
use mentischat::errors::MailboxError;

#[tokio::test]
async fn nothing_is_written_until_the_bus_is_closed() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("lazy");
    let bus = Bus::new(&data).unwrap();
    bus.register(&name("a")).await.unwrap();
    bus.register(&name("b")).await.unwrap();
    bus.send(&name("a"), &name("b"), "hello".into())
        .await
        .unwrap();
    assert!(!data.exists(), "a running mailbox never touches the disk");
    bus.shutdown().await.unwrap();
    assert!(!data.exists(), "nor does a wiping shutdown");
}

#[tokio::test]
async fn close_writes_a_private_snapshot_and_the_next_start_consumes_it() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("private");
    let bus = Bus::new(&data).unwrap();
    bus.register(&name("a")).await.unwrap();
    bus.close().await;

    let file = data.join("snapshot.json");
    let mode = |p: &std::path::Path| std::fs::metadata(p).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode(&data), 0o700);
    assert_eq!(mode(&file), 0o600);

    let bus = Bus::new(&data).unwrap();
    assert!(!file.exists(), "a snapshot is read once, then deleted");
    assert_eq!(bus.stats().await.unwrap(), stats(1, 0, 0));
}

#[tokio::test]
async fn a_restart_restores_roster_mail_read_state_history_and_holds() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("restore");
    let bus = Bus::new(&data).unwrap();
    for n in ["a", "b", "gone"] {
        bus.register(&name(n)).await.unwrap();
    }
    bus.send(&name("a"), &name("b"), "read before".into())
        .await
        .unwrap();
    bus.check_inbox(&name("b"), 0).await.unwrap();
    bus.send(&name("a"), &name("b"), "unread".into())
        .await
        .unwrap();
    bus.deregister(&name("gone")).await.unwrap();
    let sent = bus.overview().await.unwrap().messages_sent;
    bus.close().await;

    let bus = Bus::new(&data).unwrap();
    assert_eq!(bus.stats().await.unwrap(), stats(2, 1, 1));
    assert_eq!(bus.overview().await.unwrap().messages_sent, sent);
    let history = bus.history(&name("a"), None, None).await.unwrap();
    let seen: Vec<_> = history.messages.iter().map(|m| m.body.as_str()).collect();
    assert_eq!(seen, ["read before", "unread"]);
    assert!(history.messages[0].read_at.is_some());
    assert!(history.messages[1].read_at.is_none());
    let page = bus.check_inbox(&name("b"), 0).await.unwrap();
    assert_eq!(page.messages[0].body, "unread");
    assert!(matches!(
        bus.register(&name("gone")).await,
        Err(MailboxError::NameTaken { .. })
    ));
    bus.register_with(&name("gone"), reclaim()).await.unwrap();
}

#[tokio::test]
async fn a_snapshot_that_cannot_be_read_is_refused_and_kept() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("corrupt");
    std::fs::create_dir_all(&data).unwrap();
    let file = data.join("snapshot.json");

    std::fs::write(&file, "not json").unwrap();
    assert!(matches!(Bus::new(&data), Err(SnapshotError::Corrupt(_))));
    assert!(file.exists(), "left in place for the operator");

    std::fs::write(
        &file,
        r#"{"version":99,"next_id":1,"messages_sent":0,"agents":[],"holds":[],"messages":[]}"#,
    )
    .unwrap();
    assert!(matches!(Bus::new(&data), Err(SnapshotError::Version(99))));

    remove_snapshot(&data).unwrap();
    remove_snapshot(&data).expect("removing a missing snapshot is fine");
    assert!(Bus::new(&data).is_ok());
}

#[tokio::test]
async fn close_fails_parked_pulls_with_shutting_down() {
    let dir = tempfile::tempdir().unwrap();
    let bus = std::sync::Arc::new(Bus::new(&dir.path().join("closing")).unwrap());
    bus.register(&name("a")).await.unwrap();
    let puller = bus.clone();
    let parked = tokio::spawn(async move { puller.check_inbox(&name("a"), 10_000).await });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    bus.close().await;
    assert!(matches!(
        parked.await.unwrap(),
        Err(MailboxError::ShuttingDown)
    ));
}

/// Write `snapshot` as the snapshot of a data directory and open a bus on it.
fn bus_from(snapshot: serde_json::Value) -> (tempfile::TempDir, Bus) {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("crafted");
    std::fs::create_dir_all(&data).unwrap();
    std::fs::write(data.join("snapshot.json"), snapshot.to_string()).unwrap();
    let bus = Bus::new(&data).expect("a sane-enough snapshot loads");
    (dir, bus)
}

fn agent_rec(name: &str) -> serde_json::Value {
    serde_json::json!({"name": name, "provider": null, "description": null,
        "registered_ms": 1, "last_seen_ms": u64::MAX / 4})
}

fn msg_rec(id: u64, from: &str, to: &str, read_ms: u64) -> serde_json::Value {
    serde_json::json!({"id": id, "from": from, "to": to, "body": format!("m{id}"),
        "created_ms": 2, "read_ms": read_ms})
}

fn snapshot(
    agents: &[&str],
    messages: Vec<serde_json::Value>,
    rooms: Vec<serde_json::Value>,
) -> serde_json::Value {
    serde_json::json!({
        "version": 1, "next_id": 1, "messages_sent": 0,
        "agents": agents.iter().map(|a| agent_rec(a)).collect::<Vec<_>>(),
        "holds": [], "messages": messages, "rooms": rooms,
    })
}

#[tokio::test]
async fn a_snapshot_with_huge_or_repeated_ids_loads_without_overflow() {
    let (_d, bus) = bus_from(snapshot(
        &["a", "b"],
        vec![
            msg_rec(u64::MAX, "a", "b", 0),
            msg_rec(u64::MAX - 1, "a", "b", 0),
            msg_rec(7, "a", "b", 0),
            msg_rec(5, "a", "b", 0),
            msg_rec(7, "a", "b", 0),
        ],
        vec![],
    ));
    let page = bus.check_inbox(&name("b"), 0).await.unwrap();
    let bodies: Vec<&str> = page.messages.iter().map(|m| m.body.as_str()).collect();
    assert_eq!(
        bodies,
        ["m5", "m7"],
        "sorted, de-duplicated, impossible ids dropped"
    );
    // Numbering carries on without wrapping.
    let receipt = bus
        .send(&name("a"), &name("b"), "next".into())
        .await
        .unwrap();
    assert!(receipt.id.parse::<u64>().unwrap() > 7);
}

#[tokio::test]
async fn a_snapshot_is_held_to_the_caps_of_a_running_daemon() {
    use mentischat::bus::{MAX_PENDING_PER_AGENT, MAX_ROOM_MEMBERS};
    let too_many = MAX_PENDING_PER_AGENT as u64 + 100;
    let room_msgs: Vec<_> = (1..=1100u64)
        .rev()
        .map(|id| {
            serde_json::json!({"id": id, "from": "a", "to": "", "body": format!("r{id}"),
            "created_ms": 2, "read_ms": 0})
        })
        .collect();
    let members: Vec<_> = (0..MAX_ROOM_MEMBERS + 10)
        .map(|i| serde_json::json!({"name": format!("m{i}"), "cursor": 0, "skipped": 0}))
        .collect();
    let names: Vec<String> = std::iter::once("a".to_string())
        .chain((0..MAX_ROOM_MEMBERS + 10).map(|i| format!("m{i}")))
        .chain(["b".to_string()])
        .collect();
    let names: Vec<&str> = names.iter().map(String::as_str).collect();
    let (_d, bus) = bus_from(snapshot(
        &names,
        (1..=too_many).map(|i| msg_rec(i, "a", "b", 0)).collect(),
        vec![
            serde_json::json!({"name": "big", "private": false, "description": null,
            "members": members, "invited": [], "messages": room_msgs}),
        ],
    ));

    let stats = bus.stats().await.unwrap();
    assert_eq!(
        stats.unread, MAX_PENDING_PER_AGENT,
        "per-recipient cap re-applied"
    );

    let room = mentischat::names::RoomName::parse("big").unwrap();
    let listed = bus.list_rooms(&name("m0")).await.unwrap();
    assert_eq!(listed.rooms[0].members, MAX_ROOM_MEMBERS, "members capped");
    let page = bus.room_history(&name("m0"), &room, None).await.unwrap();
    let newest = &page.messages.last().unwrap().body;
    assert_eq!(
        newest, "r1100",
        "the log is sorted and keeps the newest 1024"
    );
    let mut count = page.messages.len();
    let mut before = page.next;
    while let Some(b) = before {
        let page = bus.room_history(&name("m0"), &room, Some(b)).await.unwrap();
        count += page.messages.len();
        before = page.next;
    }
    assert_eq!(count, 1024);
}

#[tokio::test]
async fn a_snapshot_with_invalid_names_or_repeated_agents_skips_them() {
    let (_d, bus) = bus_from(snapshot(
        &["ok", "OK", "-bad", "has space", &"x".repeat(80), "fine"],
        vec![msg_rec(1, "ok", "fine", 0), msg_rec(2, "-bad", "fine", 0)],
        vec![
            serde_json::json!({"name": "-bad-room", "private": false, "description": null,
            "members": [{"name": "ok", "cursor": 0, "skipped": 0}], "invited": [], "messages": []}),
        ],
    ));
    let roster = bus.list_agents().await.unwrap();
    let names: Vec<&str> = roster.agents.iter().map(|a| a.name.as_str()).collect();
    assert_eq!(names, ["fine", "ok"]);
    let page = bus.check_inbox(&name("fine"), 0).await.unwrap();
    assert_eq!(
        page.messages.len(),
        1,
        "mail from an invalid name is dropped"
    );
    assert!(bus.list_rooms(&name("ok")).await.unwrap().rooms.is_empty());
}
