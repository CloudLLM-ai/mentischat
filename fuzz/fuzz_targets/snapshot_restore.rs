//! Snapshot restore: arbitrary bytes as `snapshot.json`, or an arbitrary
//! structured snapshot serialised to one, into `Bus::from_snapshot_bytes`.
//!
//! Input: when the first byte is `{` the whole input is the file, which
//! lets real snapshots seed the corpus and the fuzzer mutate their text.
//! Otherwise the input drives `arbitrary` to build a snapshot from a small
//! pool of names and ids, so collisions (the interesting case) are common.
//!
//! Invariants on a restore that succeeds: every cap holds; message ids are
//! unique across direct mail and rooms; room logs are sorted by id; the
//! counters agree with the contents; a fresh send, direct or to a room, gets
//! an id above everything restored; and restoring a snapshot of the result
//! gives the same snapshot (a fixed point).
#![no_main]

use std::collections::{BTreeMap, HashSet};
use std::sync::OnceLock;

use arbitrary::{Arbitrary, Unstructured};
use libfuzzer_sys::fuzz_target;
use mentischat::bus::{
    Bus, DEFAULT_ROOM_CAPACITY, MAX_PENDING_PER_AGENT, MAX_ROOMS, MAX_ROOMS_PER_AGENT,
    MAX_ROOM_MEMBERS, MAX_ROSTER, MAX_TOTAL_MESSAGES,
};
use mentischat::names::{AgentName, RoomName};
use serde_json::{json, Value};

/// A name from a small pool, with the occasional invalid one.
#[derive(Arbitrary, Debug, Clone, Copy)]
struct Nm(u8);

impl Nm {
    fn get(self) -> &'static str {
        const POOL: [&str; 12] = [
            "a", "A", "b", "B", "c", "d", "e", "f", "-bad", "", "bad name", "Gamma.1",
        ];
        POOL[usize::from(self.0) % POOL.len()]
    }
}

/// A message id: small (so lists collide), or anywhere in `u64`.
#[derive(Arbitrary, Debug)]
enum Id {
    Small(u8),
    Any(u64),
}

impl Id {
    fn get(&self) -> u64 {
        match self {
            Self::Small(n) => u64::from(*n % 24),
            Self::Any(n) => *n,
        }
    }
}

#[derive(Arbitrary, Debug)]
struct Msg {
    id: Id,
    from: Nm,
    to: Nm,
    body: Body,
    created_ms: u16,
    read_ms: u8,
}

#[derive(Arbitrary, Debug)]
enum Body {
    Text,
    Blank,
    Other(String),
}

impl Msg {
    fn json(&self) -> Value {
        let body = match &self.body {
            Body::Text => "hello".to_string(),
            Body::Blank => "  ".to_string(),
            Body::Other(s) => s.clone(),
        };
        json!({
            "id": self.id.get(), "from": self.from.get(), "to": self.to.get(),
            "body": body, "created_ms": self.created_ms, "read_ms": self.read_ms,
        })
    }
}

#[derive(Arbitrary, Debug)]
struct Agent {
    name: Nm,
    provider: Option<Nm>,
    description: Option<String>,
}

#[derive(Arbitrary, Debug)]
struct Room {
    name: Nm,
    private: bool,
    description: Option<String>,
    members: Vec<(Nm, u8, u8)>,
    invited: Vec<Nm>,
    messages: Vec<Msg>,
}

#[derive(Arbitrary, Debug)]
struct Snap {
    next_id: Id,
    sent: u16,
    agents: Vec<Agent>,
    messages: Vec<Msg>,
    rooms: Vec<Room>,
}

impl Snap {
    fn json(&self) -> Vec<u8> {
        let agents: Vec<Value> = self
            .agents
            .iter()
            .map(|a| {
                json!({
                    "name": a.name.get(), "provider": a.provider.map(Nm::get),
                    "description": a.description, "registered_ms": 1, "last_seen_ms": 2,
                })
            })
            .collect();
        let rooms: Vec<Value> = self
            .rooms
            .iter()
            .map(|r| {
                let members: Vec<Value> = r
                    .members
                    .iter()
                    .map(|(n, c, s)| json!({"name": n.get(), "cursor": c % 24, "skipped": s}))
                    .collect();
                json!({
                    "name": r.name.get(), "private": r.private, "description": r.description,
                    "members": members,
                    "invited": r.invited.iter().map(|n| n.get()).collect::<Vec<_>>(),
                    "messages": r.messages.iter().map(Msg::json).collect::<Vec<_>>(),
                })
            })
            .collect();
        serde_json::to_vec(&json!({
            "version": 1, "next_id": self.next_id.get(), "messages_sent": self.sent,
            "agents": agents, "holds": [],
            "messages": self.messages.iter().map(Msg::json).collect::<Vec<_>>(),
            "rooms": rooms,
        }))
        .expect("serialises")
    }
}

fn runtime() -> &'static tokio::runtime::Runtime {
    static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
            .expect("runtime")
    })
}

/// Arrays sorted by their text, so two snapshots that differ only in the
/// order of unordered collections (members, holds) compare equal.
fn canonical(v: Value) -> Value {
    match v {
        Value::Array(items) => {
            let mut items: Vec<Value> = items.into_iter().map(canonical).collect();
            items.sort_by_key(Value::to_string);
            Value::Array(items)
        }
        Value::Object(map) => {
            Value::Object(map.into_iter().map(|(k, v)| (k, canonical(v))).collect())
        }
        other => other,
    }
}

fn ids(messages: &Value) -> Vec<u64> {
    messages
        .as_array()
        .expect("messages is an array")
        .iter()
        .map(|m| m["id"].as_u64().expect("id"))
        .collect()
}

/// Everything that must hold of a snapshot of a restored bus.
fn check_snapshot(snap: &Value) {
    let agents = snap["agents"].as_array().unwrap();
    assert!(agents.len() <= MAX_ROSTER as usize);
    let direct = ids(&snap["messages"]);
    assert!(direct.len() <= MAX_TOTAL_MESSAGES as usize);
    let mut unread: BTreeMap<String, usize> = BTreeMap::new();
    for m in snap["messages"].as_array().unwrap() {
        if m["read_ms"] == 0 {
            *unread
                .entry(m["to"].as_str().unwrap().to_ascii_lowercase())
                .or_default() += 1;
        }
    }
    assert!(unread
        .values()
        .all(|n| *n <= MAX_PENDING_PER_AGENT as usize));

    let mut seen: HashSet<u64> = direct.iter().copied().collect();
    assert_eq!(seen.len(), direct.len(), "direct ids unique");
    let mut top = direct.iter().copied().max().unwrap_or(0);
    let rooms = snap["rooms"].as_array().unwrap();
    assert!(rooms.len() <= MAX_ROOMS);
    let mut per_agent: BTreeMap<String, usize> = BTreeMap::new();
    for room in rooms {
        let log = ids(&room["messages"]);
        assert!(log.len() <= DEFAULT_ROOM_CAPACITY);
        assert!(
            log.windows(2).all(|w| w[0] < w[1]),
            "room log sorted by id: {log:?}"
        );
        for id in log {
            assert!(seen.insert(id), "id {id} is in two places");
            top = top.max(id);
        }
        let members = room["members"].as_array().unwrap();
        assert!(!members.is_empty() && members.len() <= MAX_ROOM_MEMBERS);
        for m in members {
            *per_agent
                .entry(m["name"].as_str().unwrap().to_ascii_lowercase())
                .or_default() += 1;
        }
        let invited = room["invited"].as_array().unwrap();
        assert!(invited.len() + members.len() <= MAX_ROOM_MEMBERS);
        for who in invited {
            let who = who.as_str().unwrap();
            assert_eq!(who, who.to_ascii_lowercase(), "invitations are folded");
        }
    }
    assert!(per_agent.values().all(|n| *n <= MAX_ROOMS_PER_AGENT));
    let next = snap["next_id"].as_u64().unwrap();
    assert!(
        next > top || (next == 0 && seen.is_empty()),
        "next_id {next} <= top {top}"
    );
}

/// A fresh send, direct and to a room, is numbered above everything restored.
fn check_next_ids(bus: &Bus, snap: &Value) {
    let top = ids(&snap["messages"])
        .into_iter()
        .chain(
            snap["rooms"]
                .as_array()
                .unwrap()
                .iter()
                .flat_map(|r| ids(&r["messages"])),
        )
        .max();
    runtime().block_on(async {
        let (x, y) = (
            AgentName::parse("zx1").unwrap(),
            AgentName::parse("zy1").unwrap(),
        );
        // A full roster or an existing name just skips the probe.
        if bus.register(&x).await.is_err() || bus.register(&y).await.is_err() {
            return;
        }
        let direct = bus.send(&x, &y, "probe".into()).await.expect("send");
        let id: u64 = direct.id.parse().expect("numeric id");
        assert!(
            top.is_none_or(|t| id > t),
            "fresh id {id} <= restored {top:?}"
        );
        let room = RoomName::parse("zroom1").unwrap();
        if bus.create_room(&x, &room, false, None).await.is_ok() {
            let r = bus
                .send_room(&x, &room, "probe".into())
                .await
                .expect("send_room");
            assert!(r.id.parse::<u64>().unwrap() > id);
        }
    });
}

fuzz_target!(|data: &[u8]| {
    let structured = data.first() != Some(&b'{');
    let bytes = if structured {
        let Ok(snap) = Snap::arbitrary_take_rest(Unstructured::new(data)) else {
            return;
        };
        snap.json()
    } else {
        data.to_vec()
    };
    let Ok(bus) = Bus::from_snapshot_bytes(&bytes) else {
        assert!(!structured, "a structured snapshot is always readable");
        return;
    };
    let first = bus.snapshot_bytes();
    let snap: Value = serde_json::from_slice(&first).expect("capture is JSON");
    check_snapshot(&snap);

    // Fixed point: what a restored bus writes restores to the same thing.
    let again = Bus::from_snapshot_bytes(&first).expect("our own snapshot restores");
    let second: Value = serde_json::from_slice(&again.snapshot_bytes()).unwrap();
    assert_eq!(
        canonical(snap.clone()),
        canonical(second),
        "restore is a fixed point"
    );

    check_next_ids(&bus, &snap);
});
