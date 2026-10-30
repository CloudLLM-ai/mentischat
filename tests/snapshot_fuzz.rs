//! Regression tests for what fuzzing `Bus::from_snapshot_bytes` found, and
//! for restore applying the same rules as the live paths. Each input is a
//! snapshot file as a tampered or corrupt one could look.

mod common;

use common::name;
use mentischat::bus::Bus;
use serde_json::{json, Value};

/// A snapshot with two agents, `a` and `b`, plus the given extras.
fn snapshot(next_id: u64, messages: Value, rooms: Value) -> Vec<u8> {
    serde_json::to_vec(&json!({
        "version": 1, "next_id": next_id, "messages_sent": 0,
        "agents": [
            {"name": "a", "provider": null, "description": null, "registered_ms": 1, "last_seen_ms": 2},
            {"name": "b", "provider": null, "description": null, "registered_ms": 1, "last_seen_ms": 2},
        ],
        "holds": [], "messages": messages, "rooms": rooms,
    }))
    .unwrap()
}

fn msg(id: u64, body: &str) -> Value {
    json!({"id": id, "from": "a", "to": "b", "body": body, "created_ms": 1, "read_ms": 0})
}

fn room(name: &str, messages: Vec<Value>) -> Value {
    json!({
        "name": name, "private": false, "description": null,
        "members": [{"name": "a", "cursor": 0, "skipped": 0}],
        "invited": [], "messages": messages,
    })
}

fn restored(bytes: &[u8]) -> (Bus, Value) {
    let bus = Bus::from_snapshot_bytes(bytes).expect("restores");
    let snap = serde_json::from_slice(&bus.snapshot_bytes()).unwrap();
    (bus, snap)
}

fn ids(v: &Value) -> Vec<u64> {
    v.as_array()
        .unwrap()
        .iter()
        .map(|m| m["id"].as_u64().unwrap())
        .collect()
}

/// A room message numbered past the recorded counter must push the counter
/// up, or the next send reuses an id the room already holds.
#[tokio::test]
async fn a_room_message_id_above_next_id_does_not_get_reused() {
    let bytes = snapshot(5, json!([]), json!([room("r", vec![msg(13, "room")])]));
    let (bus, snap) = restored(&bytes);
    assert!(snap["next_id"].as_u64().unwrap() > 13);
    let receipt = bus
        .send(&name("a"), &name("b"), "fresh".into())
        .await
        .unwrap();
    assert!(receipt.id.parse::<u64>().unwrap() > 13);
}

/// One id counter numbers direct and room messages, so an id found in both
/// lists is kept once.
#[test]
fn an_id_in_both_a_direct_and_a_room_log_is_kept_once() {
    let bytes = snapshot(
        9,
        json!([msg(3, "direct")]),
        json!([room("r", vec![msg(3, "room")])]),
    );
    let (_, snap) = restored(&bytes);
    let mut all = ids(&snap["messages"]);
    all.extend(ids(&snap["rooms"][0]["messages"]));
    assert_eq!(all, [3]);
}

/// Room logs stay sorted by id however the file listed them.
#[test]
fn a_room_log_restores_sorted_by_id() {
    let bytes = snapshot(
        9,
        json!([]),
        json!([room("r", vec![msg(7, "b"), msg(2, "a")])]),
    );
    let (_, snap) = restored(&bytes);
    assert_eq!(ids(&snap["rooms"][0]["messages"]), [2, 7]);
}

/// An id at the limit would leave the counter above it: dropped, and the
/// result is a fixed point.
#[test]
fn a_message_id_at_the_id_limit_is_dropped() {
    let limit = u64::MAX / 2;
    let bytes = snapshot(1, json!([msg(limit, "x"), msg(4, "y")]), json!([]));
    let (_, snap) = restored(&bytes);
    assert_eq!(ids(&snap["messages"]), [4]);
    assert!(snap["next_id"].as_u64().unwrap() <= limit);
    let (_, again) = restored(&serde_json::to_vec(&snap).unwrap());
    assert_eq!(snap, again);
}

/// Restore refuses the bodies a send refuses: blank, and over the cap.
#[test]
fn restore_drops_bodies_a_send_would_refuse() {
    let big = "x".repeat(mentischat::bus::MAX_BODY_BYTES + 1);
    let bytes = snapshot(
        9,
        json!([msg(1, "  "), msg(2, &big), msg(3, "ok")]),
        json!([]),
    );
    let (_, snap) = restored(&bytes);
    assert_eq!(ids(&snap["messages"]), [3]);
}

/// Provider and description pass the registration validators: an invalid
/// one is dropped, a valid one normalized as registration would.
#[test]
fn restore_validates_provider_and_description_like_registration() {
    let bytes = serde_json::to_vec(&json!({
        "version": 1, "next_id": 1, "messages_sent": 0, "holds": [], "messages": [],
        "agents": [
            {"name": "a", "provider": "Claude_Code", "description": "  fine  ", "registered_ms": 1, "last_seen_ms": 1},
            {"name": "b", "provider": "-bad", "description": "ctl\u{7}", "registered_ms": 1, "last_seen_ms": 1},
        ],
    }))
    .unwrap();
    let (_, snap) = restored(&bytes);
    let agents = snap["agents"].as_array().unwrap();
    assert_eq!(agents[0]["provider"], "claude_code");
    assert_eq!(agents[0]["description"], "fine");
    assert_eq!(agents[1]["name"], "b");
    assert!(agents[1]["provider"].is_null());
    assert!(agents[1]["description"].is_null());
}

/// Invitations are stored folded, and members are not also invited.
#[test]
fn restored_invitations_are_folded_and_exclude_members() {
    let mut r = room("r", vec![]);
    r["invited"] = json!(["B", "A", "-bad"]);
    let (_, snap) = restored(&snapshot(1, json!([]), json!([r])));
    assert_eq!(snap["rooms"][0]["invited"], json!(["b"]));
}

/// A member listed twice joins once.
#[test]
fn a_member_listed_twice_joins_once() {
    let mut r = room("r", vec![]);
    r["members"] = json!([
        {"name": "a", "cursor": 0, "skipped": 0},
        {"name": "A", "cursor": 0, "skipped": 0},
    ]);
    let (_, snap) = restored(&snapshot(1, json!([]), json!([r])));
    assert_eq!(snap["rooms"][0]["members"].as_array().unwrap().len(), 1);
}

/// Bytes that are not a snapshot are a typed error, not a panic.
#[test]
fn garbage_and_other_versions_are_rejected() {
    assert!(Bus::from_snapshot_bytes(b"\x00\xff").is_err());
    assert!(Bus::from_snapshot_bytes(b"{\"version\":2,\"next_id\":1}").is_err());
}
