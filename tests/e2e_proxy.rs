//! The stdio MCP proxy end to end: two harnesses, each behind its own proxy,
//! talking through a real daemon. The "harness" side here only ever sends and
//! receives MCP JSON text, as a model would.
#![cfg(feature = "e2e")]

use std::net::SocketAddr;
use std::sync::Arc;

use mentischat::bus::Bus;
use mentischat::client::cli::{describe_key, parse, Command};
use mentischat::client::{proxy, Client, Daemon, Identity};
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

/// One MCP stdio session against a proxy running on a duplex pipe.
struct Harness {
    writer: tokio::io::DuplexStream,
    reader: BufReader<tokio::io::DuplexStream>,
    next_id: u64,
}

impl Harness {
    fn spawn_with(
        client: Client,
        daemon: Daemon,
        policy: mentischat::client::UpdatePolicy,
    ) -> Self {
        let (to_proxy, proxy_in) = tokio::io::duplex(1 << 20);
        let (proxy_out, from_proxy) = tokio::io::duplex(1 << 20);
        tokio::spawn(proxy::serve(
            client,
            daemon,
            policy,
            BufReader::new(proxy_in),
            proxy_out,
        ));
        Self {
            writer: to_proxy,
            reader: BufReader::new(from_proxy),
            next_id: 1,
        }
    }

    async fn rpc(&mut self, method: &str, params: Value) -> Value {
        self.next_id += 1;
        let line =
            json!({ "jsonrpc": "2.0", "id": self.next_id, "method": method, "params": params });
        self.writer
            .write_all(format!("{line}\n").as_bytes())
            .await
            .unwrap();
        let mut reply = String::new();
        self.reader.read_line(&mut reply).await.unwrap();
        let reply: Value = serde_json::from_str(&reply).unwrap();
        assert_eq!(reply["id"], self.next_id);
        reply
    }

    /// A tool call: `(is_error, parsed JSON text)`.
    async fn tool(&mut self, name: &str, args: Value) -> (bool, Value) {
        let reply = self
            .rpc("tools/call", json!({ "name": name, "arguments": args }))
            .await;
        let result = &reply["result"];
        let text = result["content"][0]["text"].as_str().expect("text content");
        (
            result["isError"] == true,
            serde_json::from_str(text).unwrap_or(Value::Null),
        )
    }
}

struct World {
    bus: Arc<Bus>,
    _dir: tempfile::TempDir,
    daemon: Daemon,
    clients: std::path::PathBuf,
}

async fn world() -> World {
    world_with_capacity(mentischat::bus::DEFAULT_ROOM_CAPACITY).await
}

async fn world_with_capacity(capacity: usize) -> World {
    let dir = tempfile::tempdir().unwrap();
    let bus = Arc::new(
        // As the daemon runs: every write must be signed, so everything the
        // harness does goes through the proxy's signatures.
        Bus::new(&dir.path().join("daemon"))
            .unwrap()
            .with_room_capacity(capacity)
            .with_signatures_required(),
    );
    let app = mentischat::build_router(bus.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .ok();
    });
    World {
        bus,
        clients: dir.path().join("clients"),
        daemon: Daemon::new(addr),
        _dir: dir,
    }
}

impl World {
    fn harness(&self, agent: &str) -> Harness {
        self.harness_with(agent, mentischat::client::UpdatePolicy::default())
    }

    fn harness_with(&self, agent: &str, policy: mentischat::client::UpdatePolicy) -> Harness {
        self.harness_on(agent, self.daemon, policy)
    }

    /// A proxy for `agent` talking to `daemon` (not necessarily this world's).
    fn harness_on(
        &self,
        agent: &str,
        daemon: Daemon,
        policy: mentischat::client::UpdatePolicy,
    ) -> Harness {
        let id = Identity::open(&self.clients, agent).unwrap();
        Harness::spawn_with(Client::open(id, daemon).unwrap(), daemon, policy)
    }
}

fn names(v: &Value) -> Vec<&str> {
    v.as_array()
        .unwrap()
        .iter()
        .map(|m| m.as_str().unwrap())
        .collect()
}

/// The proxy is an MCP server: it initializes, lists the daemon's eleven tools
/// unchanged except that `create_room` gains `secure`, and answers ping.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_proxy_lists_the_daemons_tools_plus_secure() {
    let w = world().await;
    let mut h = w.harness("alice");
    let init = h
        .rpc(
            "initialize",
            json!({ "protocolVersion": "2025-06-18", "capabilities": {} }),
        )
        .await;
    assert_eq!(init["result"]["serverInfo"]["name"], "mentischat-client");
    assert_eq!(init["result"]["protocolVersion"], "2025-06-18");
    let list = h.rpc("tools/list", json!({})).await;
    let tools = list["result"]["tools"].as_array().unwrap();
    let tool_names: Vec<&str> = tools.iter().map(|t| t["name"].as_str().unwrap()).collect();
    assert_eq!(tool_names.len(), 11);
    assert!(tool_names.contains(&"send_message") && tool_names.contains(&"create_room"));
    let create = tools.iter().find(|t| t["name"] == "create_room").unwrap();
    assert_eq!(
        create["inputSchema"]["properties"]["secure"]["type"],
        "boolean"
    );
    let register = tools.iter().find(|t| t["name"] == "register").unwrap();
    assert!(register["inputSchema"]["properties"]
        .get("secure")
        .is_none());
    assert_eq!(h.rpc("ping", json!({})).await["result"], json!({}));
    assert_eq!(h.rpc("nonsense", json!({})).await["error"]["code"], -32601);
}

/// Two harnesses hold a secure conversation through the proxies using only the
/// ordinary tool names, and see plain text throughout.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_harnesses_converse_in_a_secure_room_with_the_usual_tools() {
    let w = world().await;
    let (mut alice, mut bob) = (w.harness("alice"), w.harness("bob"));
    for (h, name) in [(&mut alice, "alice"), (&mut bob, "bob")] {
        let (err, body) = h.tool("register", json!({ "agent": name })).await;
        assert!(!err, "{body}");
        assert_eq!(body["secure"], true, "{body}");
    }
    let (err, room) = alice
        .tool(
            "create_room",
            json!({ "agent": "alice", "room": "den", "secure": true }),
        )
        .await;
    assert!(!err, "{room}");
    assert_eq!(
        (room["secure"].as_bool(), room["private"].as_bool()),
        (Some(true), Some(true))
    );

    let (err, room) = alice
        .tool(
            "invite",
            json!({ "agent": "alice", "room": "den", "invitee": "bob" }),
        )
        .await;
    assert!(!err, "{room}");
    let (err, room) = bob
        .tool("join_room", json!({ "agent": "bob", "room": "den" }))
        .await;
    assert!(!err, "{room}");
    assert_eq!(names(&room["members"]).len(), 2);

    let (err, receipt) = alice
        .tool(
            "send_message",
            json!({ "from": "alice", "room": "den", "body": "No reply needed: the vault code is 1234" }),
        )
        .await;
    assert!(!err, "{receipt}");
    assert_eq!(receipt["room"], "den");

    let (err, page) = bob
        .tool("check_inbox", json!({ "agent": "bob", "wait_ms": 0 }))
        .await;
    assert!(!err, "{page}");
    let messages = page["messages"].as_array().unwrap();
    assert_eq!(messages.len(), 1, "{page}");
    assert_eq!(
        messages[0]["body"],
        "No reply needed: the vault code is 1234"
    );
    assert_eq!(
        (messages[0]["from"].as_str(), messages[0]["room"].as_str()),
        (Some("alice"), Some("den"))
    );

    // Seen once; history still shows it; the other direction works too.
    let (_, again) = bob.tool("check_inbox", json!({ "agent": "bob" })).await;
    assert!(again["messages"].as_array().unwrap().is_empty(), "{again}");
    let (_, hist) = bob
        .tool("history", json!({ "agent": "bob", "room": "den" }))
        .await;
    assert_eq!(
        hist["messages"][0]["body"],
        "No reply needed: the vault code is 1234"
    );
    bob.tool(
        "send_message",
        json!({ "from": "bob", "room": "den", "body": "Reply when you can: noted" }),
    )
    .await;
    let (_, page) = alice.tool("check_inbox", json!({ "agent": "alice" })).await;
    assert_eq!(page["messages"][0]["body"], "Reply when you can: noted");
}

/// Rooms that are not secure and direct mail are signed by the proxy: the
/// harness uses the usual tools with no keys, the daemon (which takes nothing
/// unsigned) accepts them, and the reader sees plain text marked `verified`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rooms_and_direct_mail_are_signed_for_the_harness() {
    let w = world().await;
    let (mut alice, mut bob, mut carol) =
        (w.harness("alice"), w.harness("bob"), w.harness("carol"));
    for (h, n) in [
        (&mut alice, "alice"),
        (&mut bob, "bob"),
        (&mut carol, "carol"),
    ] {
        let (err, v) = h.tool("register", json!({ "agent": n })).await;
        assert!(!err, "{v}");
    }
    let (err, room) = alice
        .tool("create_room", json!({ "agent": "alice", "room": "lobby" }))
        .await;
    assert!(!err, "{room}");
    assert!(
        room.get("secure").is_none(),
        "a room that is not secure is not marked: {room}"
    );
    assert_eq!(room["private"], false);
    let (err, v) = bob
        .tool("join_room", json!({ "agent": "bob", "room": "lobby" }))
        .await;
    assert!(!err, "{v}");
    let (err, v) = alice
        .tool(
            "send_message",
            json!({ "from": "alice", "room": "lobby", "body": "hello lobby" }),
        )
        .await;
    assert!(!err, "{v}");
    let (err, v) = alice
        .tool(
            "send_message",
            json!({ "from": "alice", "to": "bob", "body": "hello bob" }),
        )
        .await;
    assert!(!err, "{v}");
    let (_, page) = bob.tool("check_inbox", json!({ "agent": "bob" })).await;
    let seen: Vec<(&str, bool)> = page["messages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| (m["body"].as_str().unwrap(), m["verified"] == true))
        .collect();
    assert_eq!(seen, [("hello lobby", true), ("hello bob", true)]);

    // History shows the same text, checked, for both kinds.
    let (_, page) = bob
        .tool("history", json!({ "agent": "bob", "room": "lobby" }))
        .await;
    assert_eq!(page["messages"][0]["body"], "hello lobby");
    assert_eq!(page["messages"][0]["verified"], true);

    // A private room: the admin invites through the proxy, the invitee joins.
    let (err, v) = alice
        .tool(
            "create_room",
            json!({ "agent": "alice", "room": "den", "private": true }),
        )
        .await;
    assert!(!err, "{v}");
    let (err, v) = carol
        .tool("join_room", json!({ "agent": "carol", "room": "den" }))
        .await;
    assert!(err, "not invited yet: {v}");
    let (err, v) = alice
        .tool(
            "invite",
            json!({ "agent": "alice", "room": "den", "invitee": "carol" }),
        )
        .await;
    assert!(!err, "{v}");
    let (err, v) = carol
        .tool("join_room", json!({ "agent": "carol", "room": "den" }))
        .await;
    assert!(!err, "{v}");
    // A participant cannot invite.
    let (err, v) = carol
        .tool(
            "invite",
            json!({ "agent": "carol", "room": "den", "invitee": "bob" }),
        )
        .await;
    assert!(err);
    assert_eq!(v["error"], "not_an_admin");

    // The proxy signs only as its own agent.
    let (err, v) = alice
        .tool(
            "send_message",
            json!({ "from": "bob", "to": "carol", "body": "as bob" }),
        )
        .await;
    assert!(err, "{v}");

    // A secure room next to them leaves them alone.
    alice
        .tool(
            "create_room",
            json!({ "agent": "alice", "room": "vault", "secure": true }),
        )
        .await;
    alice
        .tool(
            "send_message",
            json!({ "from": "alice", "to": "bob", "body": "still direct" }),
        )
        .await;
    let (_, page) = bob.tool("check_inbox", json!({ "agent": "bob" })).await;
    assert_eq!(page["messages"][0]["body"], "still direct");
    assert_eq!(page["messages"][0]["verified"], true);
}

/// Without the proxy, a harness talking to the daemon directly cannot write:
/// plain sends and rooms are refused with `signature_required`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn without_the_proxy_nothing_unsigned_is_accepted() {
    let w = world().await;
    let mut alice = w.harness("alice");
    alice.tool("register", json!({ "agent": "alice" })).await;
    w.bus
        .register(&mentischat::names::AgentName::parse("mallory").unwrap())
        .await
        .unwrap();
    let err = w
        .bus
        .send(
            &mentischat::names::AgentName::parse("mallory").unwrap(),
            &mentischat::names::AgentName::parse("alice").unwrap(),
            "pretend".into(),
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), "signature_required");
}

/// The harness never receives anything that looks like key material or
/// ciphertext, even when it asks for everything the proxy will give.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nothing_the_harness_sees_contains_keys_or_ciphertext() {
    let w = world().await;
    let (mut alice, mut bob) = (w.harness("alice"), w.harness("bob"));
    alice.tool("register", json!({ "agent": "alice" })).await;
    bob.tool("register", json!({ "agent": "bob" })).await;
    alice
        .tool(
            "create_room",
            json!({ "agent": "alice", "room": "den", "secure": true }),
        )
        .await;
    alice
        .tool(
            "invite",
            json!({ "agent": "alice", "room": "den", "invitee": "bob" }),
        )
        .await;
    bob.tool("join_room", json!({ "agent": "bob", "room": "den" }))
        .await;
    alice
        .tool(
            "send_message",
            json!({ "from": "alice", "room": "den", "body": "plain words only" }),
        )
        .await;

    let seed = std::fs::read_to_string(w.clients.join("alice/identity.key")).unwrap();
    let mut seen = Vec::new();
    for (h, name) in [(&mut alice, "alice"), (&mut bob, "bob")] {
        for (tool, args) in [
            ("check_inbox", json!({ "agent": name })),
            ("history", json!({ "agent": name, "room": "den" })),
            ("list_rooms", json!({ "agent": name })),
            ("list_agents", json!({})),
        ] {
            seen.push(h.tool(tool, args).await.1.to_string());
        }
    }
    let all = seen.join("\n");
    assert!(all.contains("plain words only"));
    assert!(
        !all.contains(seed.trim()),
        "the private seed reached the harness"
    );
    assert!(!all.contains("mls/1"), "ciphertext reached the harness");
    assert!(
        !all.contains("mentischat-signed"),
        "an envelope reached the harness"
    );
}

/// A failing secure call comes back as an `isError` result with the daemon's
/// code, so the model can react.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn secure_failures_are_error_results_with_codes() {
    let w = world().await;
    let (mut alice, mut bob) = (w.harness("alice"), w.harness("bob"));
    alice.tool("register", json!({ "agent": "alice" })).await;
    bob.tool("register", json!({ "agent": "bob" })).await;
    alice
        .tool(
            "create_room",
            json!({ "agent": "alice", "room": "den", "secure": true }),
        )
        .await;
    let (err, body) = alice
        .tool(
            "create_room",
            json!({ "agent": "alice", "room": "den", "secure": true }),
        )
        .await;
    assert!(err);
    assert_eq!(body["error"], "room_exists");
    // Bob was never invited: a plain join answers like any private room.
    let (err, body) = bob
        .tool("join_room", json!({ "agent": "bob", "room": "den" }))
        .await;
    assert!(err);
    assert_eq!(body["error"], "room_not_found");
    // Inviting someone who never registered fails with the roster error.
    let (err, body) = alice
        .tool(
            "invite",
            json!({ "agent": "alice", "room": "den", "invitee": "ghost" }),
        )
        .await;
    assert!(err);
    assert_eq!(body["error"], "unregistered");
}

/// The command line: subcommands, flags, loopback-only addresses, and the key
/// description in MentisDB's shape.
#[test]
fn the_command_line_parses_and_describes_keys() {
    let args = |a: &[&str]| a.iter().map(|s| s.to_string()).collect::<Vec<_>>();
    assert_eq!(
        parse(&args(&[
            "run",
            "--agent",
            "Vega",
            "--addr",
            "127.0.0.1:1234"
        ])),
        Ok(Command::Run {
            agent: "Vega".into(),
            addr: "127.0.0.1:1234".parse().unwrap(),
            update: mentischat::client::UpdatePolicy::default(),
        })
    );
    let tuned = parse(&args(&[
        "run",
        "--agent",
        "a",
        "--update-messages",
        "7",
        "--update-minutes",
        "2",
    ]));
    let Ok(Command::Run { update, .. }) = tuned else {
        panic!("{tuned:?}");
    };
    assert_eq!(update.messages, 7);
    assert_eq!(update.interval, std::time::Duration::from_secs(120));
    for bad in [["--update-messages", "x"], ["--update-minutes", "-1"]] {
        let mut a = args(&["run", "--agent", "a"]);
        a.extend(bad.map(String::from));
        assert!(parse(&a).is_err(), "{a:?}");
    }
    assert_eq!(
        parse(&args(&["key", "init", "--agent", "Vega"])),
        Ok(Command::KeyInit {
            agent: "Vega".into()
        })
    );
    for bad in [
        args(&[]),
        args(&["run"]),
        args(&["run", "--agent"]),
        args(&["run", "--agent", "bad name"]),
        args(&["run", "--agent", "a", "--addr", "8.8.8.8:80"]),
        args(&["run", "--agent", "a", "--addr", "nope"]),
        args(&["run", "--agent", "a", "--wat", "x"]),
        args(&["key", "--agent", "a"]),
        args(&["key", "zap", "--agent", "a"]),
        args(&["fly", "--agent", "a"]),
    ] {
        assert!(parse(&bad).is_err(), "{bad:?}");
    }

    let dir = tempfile::tempdir().unwrap();
    let id = Identity::open(dir.path(), "Vega").unwrap();
    let id_dir = id.dir().to_path_buf();
    let text = describe_key(&id);
    let hex = id.public().to_hex();
    assert!(text.contains(&hex));
    let suggested = text
        .lines()
        .find(|l| l.starts_with("mentisdb key_id: "))
        .unwrap();
    let key_id = suggested.trim_start_matches("mentisdb key_id: ");
    let (agent, month) = key_id.rsplit_once('-').unwrap();
    assert_eq!(agent, "Vega");
    assert!(
        month.len() == 6 && month.bytes().all(|b| b.is_ascii_digit()) && month.starts_with("20")
    );
    let file = std::fs::read_to_string(id_file_path(&id_dir)).unwrap();
    assert!(
        file.ends_with('\n') && file.trim().len() == 64,
        "seed file is 64 hex plus newline"
    );
    let array: Vec<String> = id.public().as_bytes().iter().map(u8::to_string).collect();
    assert!(text.contains(&format!("[{}]", array.join(","))));
    // Key registration is operator-only in MentisDB, so the output carries the
    // exact operator command with this key's id and hex.
    let register = text
        .lines()
        .find(|l| l.starts_with("register (MentisDB operator): "))
        .unwrap();
    assert!(register.contains(&format!(
        "mentisdb agent key add Vega --key-id {key_id} --public-key-hex {hex} --chain <chain>"
    )));
    assert!(!text.contains(
        std::fs::read_to_string(id.dir().join("identity.key"))
            .unwrap()
            .trim()
    ));
}

fn id_file_path(dir: &std::path::Path) -> std::path::PathBuf {
    dir.join("identity.key")
}

/// A client that falls behind is brought back by its proxy alone: it asks the
/// owner by direct message, the owner's proxy re-adds it without the harness
/// seeing the request, and its next check_inbox rejoins and reads on.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_proxy_that_fell_behind_asks_the_owner_and_rejoins_by_itself() {
    use mentischat::client::UpdatePolicy;
    let w = world_with_capacity(6).await;
    // The owner updates its key after every message, which floods the room
    // with Commits past its small retention.
    let eager = UpdatePolicy {
        messages: 1,
        interval: std::time::Duration::ZERO,
    };
    let (mut alice, mut bob) = (w.harness_with("alice", eager), w.harness("bob"));
    alice.tool("register", json!({ "agent": "alice" })).await;
    bob.tool("register", json!({ "agent": "bob" })).await;
    alice
        .tool(
            "create_room",
            json!({ "agent": "alice", "room": "den", "secure": true }),
        )
        .await;
    alice
        .tool(
            "invite",
            json!({ "agent": "alice", "room": "den", "invitee": "bob" }),
        )
        .await;
    bob.tool("join_room", json!({ "agent": "bob", "room": "den" }))
        .await;
    for i in 0..9 {
        let (err, body) = alice
            .tool(
                "send_message",
                json!({ "from": "alice", "room": "den", "body": format!("note {i}") }),
            )
            .await;
        assert!(!err, "{body}");
    }
    // Bob never looked while the room's retention rolled past its Commits. His
    // first look cannot read them; it asks alice for a re-add.
    let (_, page) = bob.tool("check_inbox", json!({ "agent": "bob" })).await;
    assert!(page["messages"].as_array().unwrap().is_empty(), "{page}");
    // Alice's next look consumes the request: the harness sees no such message.
    let (_, page) = alice.tool("check_inbox", json!({ "agent": "alice" })).await;
    assert!(page["messages"].as_array().unwrap().is_empty(), "{page}");
    // Bob's next look joins from the Welcome; later messages are readable.
    bob.tool("check_inbox", json!({ "agent": "bob" })).await;
    alice
        .tool(
            "send_message",
            json!({ "from": "alice", "room": "den", "body": "after the re-add" }),
        )
        .await;
    let (_, page) = bob.tool("check_inbox", json!({ "agent": "bob" })).await;
    let bodies: Vec<&str> = page["messages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["body"].as_str().unwrap())
        .collect();
    assert_eq!(bodies, ["after the re-add"], "{page}");
}

/// The proxy updates its key on its own policy: after enough sends the epoch
/// has moved, and the other side still reads everything.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_proxy_refreshes_its_key_by_policy_and_peers_keep_reading() {
    use mentischat::client::UpdatePolicy;
    let w = world().await;
    let every_two = UpdatePolicy {
        messages: 2,
        interval: std::time::Duration::ZERO,
    };
    let (mut alice, mut bob) = (w.harness_with("alice", every_two), w.harness("bob"));
    alice.tool("register", json!({ "agent": "alice" })).await;
    bob.tool("register", json!({ "agent": "bob" })).await;
    alice
        .tool(
            "create_room",
            json!({ "agent": "alice", "room": "den", "secure": true }),
        )
        .await;
    alice
        .tool(
            "invite",
            json!({ "agent": "alice", "room": "den", "invitee": "bob" }),
        )
        .await;
    bob.tool("join_room", json!({ "agent": "bob", "room": "den" }))
        .await;
    for i in 0..5 {
        alice
            .tool(
                "send_message",
                json!({ "from": "alice", "room": "den", "body": format!("m{i}") }),
            )
            .await;
    }
    let (_, page) = bob.tool("check_inbox", json!({ "agent": "bob" })).await;
    let bodies: Vec<&str> = page["messages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["body"].as_str().unwrap())
        .collect();
    assert_eq!(bodies, ["m0", "m1", "m2", "m3", "m4"], "{page}");
    // Joining added 1 Commit and bob's first look 1 of his own; alice's policy
    // fired twice (after m1 and m3): four Commits, so the daemon's epoch is 4.
    let (_, room) = bob
        .tool("history", json!({ "agent": "bob", "room": "den" }))
        .await;
    assert!(room["messages"].as_array().unwrap().len() >= 5);
    let info = w
        .daemon
        .call("GET", "/signed/rooms/den?agent=bob", None)
        .await
        .unwrap();
    assert_eq!(info["epoch"], 4, "{info}");
}

/// A daemon restored from an older snapshot makes the proxy put a warning in
/// check_inbox and stop serving the room, instead of quietly carrying on.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_rolled_back_daemon_shows_as_a_warning_in_check_inbox() {
    use mentischat::client::UpdatePolicy;
    let w = world().await;
    let (mut alice, mut bob) = (w.harness("alice"), w.harness("bob"));
    alice.tool("register", json!({ "agent": "alice" })).await;
    bob.tool("register", json!({ "agent": "bob" })).await;
    alice
        .tool(
            "create_room",
            json!({ "agent": "alice", "room": "den", "secure": true }),
        )
        .await;
    alice
        .tool(
            "invite",
            json!({ "agent": "alice", "room": "den", "invitee": "bob" }),
        )
        .await;
    bob.tool("join_room", json!({ "agent": "bob", "room": "den" }))
        .await;
    // An honest look: no warning.
    let (_, page) = bob.tool("check_inbox", json!({ "agent": "bob" })).await;
    assert!(page.get("warnings").is_none(), "{page}");
    let old = w.bus.snapshot_bytes();
    bob.tool(
        "send_message",
        json!({ "from": "bob", "room": "den", "body": "will be rolled away" }),
    )
    .await;
    let older = Arc::new(Bus::from_snapshot_bytes(&old).unwrap());
    let app = mentischat::build_router(older);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .ok();
    });
    let mut bob = w.harness_on("bob", Daemon::new(addr), UpdatePolicy::default());
    let (err, page) = bob.tool("check_inbox", json!({ "agent": "bob" })).await;
    assert!(!err, "{page}");
    let warnings = page["warnings"].as_array().expect("a warning is shown");
    assert!(
        warnings[0].as_str().unwrap().contains("rolled back"),
        "{page}"
    );
    assert!(page["messages"].as_array().unwrap().is_empty());
    // The room stays blocked: sending a secure message fails with the reason.
    let (err, body) = bob
        .tool(
            "send_message",
            json!({ "from": "bob", "room": "den", "body": "x" }),
        )
        .await;
    assert!(err);
    assert!(
        body["message"].as_str().unwrap().contains("rolled back"),
        "{body}"
    );
}

/// A restarted proxy takes back its own name, still registered or held,
/// with a reclaim it signs, and a proxy with another key for the same name
/// cannot. The harness only ever passes `reclaim: true`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restarted_proxy_reclaims_its_own_name_and_an_impostor_cannot() {
    let w = world().await;
    let mut first = w.harness("vega");
    let (err, out) = first.tool("register", json!({ "agent": "vega" })).await;
    assert!(!err, "{out}");
    assert_eq!(out["secure"], true, "{out}");
    drop(first);

    // The same identity after a restart: the name is still registered.
    let mut again = w.harness("vega");
    let list = again.rpc("tools/list", json!({})).await;
    let register = list["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .find(|t| t["name"] == "register")
        .unwrap()
        .clone();
    let reclaim_doc = register["inputSchema"]["properties"]["reclaim"]["description"]
        .as_str()
        .unwrap();
    assert!(reclaim_doc.contains("proves"), "{reclaim_doc}");
    let (err, out) = again.tool("register", json!({ "agent": "vega" })).await;
    assert!(err && out["error"] == "name_taken", "{out}");
    let (err, out) = again
        .tool(
            "register",
            json!({ "agent": "vega", "reclaim": true, "description": "back" }),
        )
        .await;
    assert!(!err, "{out}");
    assert_eq!(
        (out["created"].as_bool(), out["secure"].as_bool()),
        (Some(false), Some(true)),
        "{out}"
    );
    assert_eq!(out["description"], "back");

    // Another key for the name: its signed reclaim does not verify.
    let elsewhere = tempfile::tempdir().unwrap();
    let id = Identity::open(elsewhere.path(), "vega").unwrap();
    let mut impostor = Harness::spawn_with(
        Client::open(id, w.daemon).unwrap(),
        w.daemon,
        Default::default(),
    );
    let (err, out) = impostor
        .tool("register", json!({ "agent": "vega", "reclaim": true }))
        .await;
    assert!(err && out["error"] == "invalid_signature", "{out}");

    // After a deregister the hold keeps the key: the impostor still fails,
    // the owner gets the name back.
    let (err, out) = again.tool("deregister", json!({ "agent": "vega" })).await;
    assert!(!err, "{out}");
    let (err, out) = impostor
        .tool("register", json!({ "agent": "vega", "reclaim": "true" }))
        .await;
    assert!(err && out["error"] == "invalid_signature", "{out}");
    let (err, out) = again
        .tool("register", json!({ "agent": "vega", "reclaim": true }))
        .await;
    assert!(!err, "{out}");
    assert_eq!(
        (out["created"].as_bool(), out["secure"].as_bool()),
        (Some(true), Some(true)),
        "{out}"
    );
    assert!(
        w.bus
            .agent_key(&mentischat::names::AgentName::parse("vega").unwrap())
            .await
            .unwrap()
            .is_some(),
        "the proxy bound its key again"
    );
}

/// A proxy deregisters its own key-bound name by signing for it; a proxy with
/// another key for the name, and a plain caller, cannot. A deregister naming
/// another agent goes to the daemon unsigned and fails the same way.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn only_the_key_holder_deregisters_a_key_bound_name() {
    let w = world().await;
    let mut vega = w.harness("vega");
    let (err, out) = vega.tool("register", json!({ "agent": "vega" })).await;
    assert!(!err && out["secure"] == true, "{out}");
    let mut lyra = w.harness("lyra");
    let (err, out) = lyra.tool("register", json!({ "agent": "lyra" })).await;
    assert!(!err && out["secure"] == true, "{out}");

    let vega_name = mentischat::names::AgentName::parse("vega").unwrap();
    let plain = w.bus.deregister(&vega_name).await.unwrap_err();
    assert_eq!(plain.code(), "proof_required");
    let (err, out) = lyra.tool("deregister", json!({ "agent": "vega" })).await;
    assert!(err && out["error"] == "proof_required", "{out}");

    let elsewhere = tempfile::tempdir().unwrap();
    let id = Identity::open(elsewhere.path(), "vega").unwrap();
    let mut impostor = Harness::spawn_with(
        Client::open(id, w.daemon).unwrap(),
        w.daemon,
        Default::default(),
    );
    let (err, out) = impostor
        .tool("deregister", json!({ "agent": "Vega" }))
        .await;
    assert!(err && out["error"] == "invalid_signature", "{out}");

    let (err, out) = vega.tool("deregister", json!({ "agent": "Vega" })).await;
    assert!(!err, "{out}");
    assert_eq!(out["agent"], "vega");
    let roster = w.bus.list_agents().await.unwrap();
    let names: Vec<&str> = roster.agents.iter().map(|a| a.name.as_str()).collect();
    assert_eq!(names, ["lyra"]);
}

/// A daemon over `data` with the key registry attached, as `main.rs` serves it.
async fn daemon_with_registry(data: &std::path::Path) -> Daemon {
    let (bus, _) = Bus::new(data)
        .unwrap()
        .with_key_registry(&data.join(mentischat::bus::KEY_REGISTRY_FILE))
        .unwrap();
    let app = mentischat::build_router(Arc::new(bus));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(
            mentischat::low_latency_listener(listener),
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .ok();
    });
    Daemon::new(addr)
}

/// After a daemon restart the name is reserved for its key in the registry.
/// A proxy holding that key gets it back with a plain `register` (it signs the
/// reclaim itself, no retry by the model), already bound; a proxy with another
/// key for the name is refused with reason `bound`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn after_a_restart_the_proxy_takes_its_bound_name_back_by_itself() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("daemon");
    let clients = dir.path().join("clients");
    let proxy_on = |daemon: Daemon, clients: &std::path::Path| {
        let id = Identity::open(clients, "vega").unwrap();
        Harness::spawn_with(
            Client::open(id, daemon).unwrap(),
            daemon,
            mentischat::client::UpdatePolicy::default(),
        )
    };

    let before = daemon_with_registry(&data).await;
    let mut h = proxy_on(before, &clients);
    let (err, body) = h.tool("register", json!({ "agent": "vega" })).await;
    assert!(!err, "{body}");
    assert_eq!(body["secure"], true, "{body}");

    // The restarted daemon: a new mailbox over the same registry.
    let after = daemon_with_registry(&data).await;
    let mut h = proxy_on(after, &clients);
    let (err, body) = h.tool("register", json!({ "agent": "vega" })).await;
    assert!(!err, "the proxy should reclaim its own bound name: {body}");
    assert_eq!(body["secure"], true, "{body}");

    let impostor_clients = dir.path().join("impostor");
    let mut impostor = proxy_on(after, &impostor_clients);
    let (err, body) = impostor.tool("register", json!({ "agent": "vega" })).await;
    assert!(err, "{body}");
    assert_eq!(body["error"], "name_taken", "{body}");
    assert_eq!(body["reason"], "registered", "{body}");

    // On the next start, before vega is back, the impostor is told the name
    // is reserved and for which key.
    let third = daemon_with_registry(&data).await;
    let mut impostor = proxy_on(third, &impostor_clients);
    let (err, body) = impostor.tool("register", json!({ "agent": "vega" })).await;
    assert!(err, "{body}");
    assert_eq!(body["reason"], "bound", "{body}");
    let vega_key = Identity::open(&clients, "vega").unwrap().public().to_hex();
    assert_eq!(body["publicKey"], vega_key, "{body}");
}

/// Room commands typed as messages run through the proxy, signed: admins
/// promote, ban and kick, everyone sees roles and fingerprints, a bad command
/// says what the commands are, and `//` sends a leading slash as text.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn room_commands_typed_in_the_chat_run_signed() {
    let w = world().await;
    let (mut alice, mut bob, mut carol) =
        (w.harness("alice"), w.harness("bob"), w.harness("carol"));
    for (h, n) in [
        (&mut alice, "alice"),
        (&mut bob, "bob"),
        (&mut carol, "carol"),
    ] {
        h.tool("register", json!({ "agent": n })).await;
    }
    alice
        .tool("create_room", json!({ "agent": "alice", "room": "lobby" }))
        .await;
    for (h, n) in [(&mut bob, "bob"), (&mut carol, "carol")] {
        let (err, v) = h
            .tool("join_room", json!({ "agent": n, "room": "lobby" }))
            .await;
        assert!(!err, "{v}");
    }
    let say = |body: &str| json!({ "from": "x", "room": "lobby", "body": body });
    let as_ = |who: &str, body: &str| {
        let mut v = say(body);
        v["from"] = json!(who);
        v
    };

    let (err, v) = bob.tool("send_message", as_("bob", "/ban carol")).await;
    assert!(err);
    assert_eq!(v["error"], "not_an_admin");
    let (err, v) = alice
        .tool("send_message", as_("alice", "/promote bob"))
        .await;
    assert!(!err, "{v}");
    assert_eq!(v["command"], "promote");
    let (err, v) = bob.tool("send_message", as_("bob", "/ban carol")).await;
    assert!(!err, "{v}");

    let (err, v) = carol
        .tool("send_message", as_("carol", "still here?"))
        .await;
    assert!(err, "carol is banned: {v}");
    let (err, v) = carol
        .tool("join_room", json!({ "agent": "carol", "room": "lobby" }))
        .await;
    assert!(err);
    assert_eq!(v["error"], "banned");

    let (_, v) = alice.tool("send_message", as_("alice", "/members")).await;
    let members = v["result"]["members"].as_array().unwrap();
    assert_eq!(members.len(), 2);
    assert!(members.iter().all(|m| m["role"] == "admin"));
    assert!(members.iter().all(|m| m["fingerprint"].as_str().is_some()));
    assert_eq!(v["result"]["banned"][0]["agent"], "carol");

    let (_, v) = alice.tool("send_message", as_("alice", "/whois bob")).await;
    assert_eq!(v["result"]["agent"], "bob");
    let (_, mine) = bob.tool("send_message", as_("bob", "/key")).await;
    assert_eq!(v["result"]["fingerprint"], mine["result"]["fingerprint"]);

    let (err, v) = alice
        .tool("send_message", as_("alice", "/frobnicate"))
        .await;
    assert!(err);
    assert_eq!(v["error"], "invalid_command");
    assert!(v["commands"].as_array().unwrap().len() > 5);

    let (err, v) = alice
        .tool("send_message", as_("alice", "//etc/hosts"))
        .await;
    assert!(!err, "{v}");
    let (_, page) = bob.tool("check_inbox", json!({ "agent": "bob" })).await;
    let bodies: Vec<&str> = page["messages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["body"].as_str().unwrap())
        .collect();
    assert_eq!(bodies, ["/etc/hosts"], "commands never reach the room");

    // Kicking takes a member out; they can come back to a public room.
    let (err, v) = alice.tool("send_message", as_("alice", "/kick bob")).await;
    assert!(!err, "{v}");
    let (err, v) = bob
        .tool("join_room", json!({ "agent": "bob", "room": "lobby" }))
        .await;
    assert!(!err, "{v}");
}

/// In a secure room `/kick` also commits the member out of the MLS group.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kicking_from_a_secure_room_through_the_chat_removes_the_leaf() {
    let w = world().await;
    let (mut alice, mut bob) = (w.harness("alice"), w.harness("bob"));
    for (h, n) in [(&mut alice, "alice"), (&mut bob, "bob")] {
        h.tool("register", json!({ "agent": n })).await;
    }
    alice
        .tool(
            "create_room",
            json!({ "agent": "alice", "room": "vault", "secure": true }),
        )
        .await;
    let (err, v) = alice
        .tool(
            "send_message",
            json!({ "from": "alice", "room": "vault", "body": "/invite bob" }),
        )
        .await;
    assert!(!err, "{v}");
    let (err, v) = bob
        .tool("join_room", json!({ "agent": "bob", "room": "vault" }))
        .await;
    assert!(!err, "{v}");
    let (err, v) = alice
        .tool(
            "send_message",
            json!({ "from": "alice", "room": "vault", "body": "/kick bob" }),
        )
        .await;
    assert!(!err, "{v}");
    assert_eq!(v["result"]["members"].as_array().unwrap().len(), 1);
    let (err, _) = bob
        .tool(
            "send_message",
            json!({ "from": "bob", "room": "vault", "body": "hello?" }),
        )
        .await;
    assert!(err, "bob is out");
}

/// The command line takes room commands, `whois` and `key rotate`.
#[test]
fn the_command_line_parses_room_commands() {
    use mentischat::client::commands::RoomCommand;
    let args = |s: &str| s.split(' ').map(String::from).collect::<Vec<_>>();
    match parse(&args("room ban lobby Mallory --agent Vega")).unwrap() {
        Command::Room { room, command, .. } => {
            assert_eq!(room, "lobby");
            assert_eq!(command, RoomCommand::Ban("Mallory".into()));
        }
        other => panic!("{other:?}"),
    }
    assert!(matches!(
        parse(&args("room members lobby --agent Vega")).unwrap(),
        Command::Room {
            command: RoomCommand::Members,
            ..
        }
    ));
    assert!(matches!(
        parse(&args("whois Orion --agent Vega")).unwrap(),
        Command::Whois { ref target, .. } if target == "Orion"
    ));
    assert!(matches!(
        parse(&args("key rotate --agent Vega")).unwrap(),
        Command::KeyRotate { .. }
    ));
    for bad in [
        "room ban lobby --agent Vega",
        "room members lobby extra --agent Vega",
        "room frob lobby --agent Vega",
        "room ban lobby bad!name --agent Vega",
        "whois --agent Vega",
    ] {
        assert!(parse(&args(bad)).is_err(), "{bad}");
    }
}

/// The remaining chat commands: `/help`, `/invite` and `/leave` in a private
/// room, `/unban` and `/demote`, and `/leave` from a secure room.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_other_chat_commands_run_too() {
    let w = world().await;
    let (mut alice, mut bob) = (w.harness("alice"), w.harness("bob"));
    for (h, n) in [(&mut alice, "alice"), (&mut bob, "bob")] {
        h.tool("register", json!({ "agent": n })).await;
    }
    let say =
        |who: &str, room: &str, body: &str| json!({ "from": who, "room": room, "body": body });
    alice
        .tool(
            "create_room",
            json!({ "agent": "alice", "room": "den", "private": true }),
        )
        .await;
    let (err, v) = alice
        .tool("send_message", say("alice", "den", "/help"))
        .await;
    assert!(!err, "{v}");
    assert!(v["result"].as_array().unwrap().len() > 5);
    let (err, v) = alice
        .tool("send_message", say("alice", "den", "/invite bob"))
        .await;
    assert!(!err, "{v}");
    bob.tool("join_room", json!({ "agent": "bob", "room": "den" }))
        .await;
    let (err, v) = alice
        .tool("send_message", say("alice", "den", "/promote bob"))
        .await;
    assert!(!err, "{v}");
    let (err, v) = alice
        .tool("send_message", say("alice", "den", "/demote bob"))
        .await;
    assert!(!err, "{v}");
    let (err, v) = alice
        .tool("send_message", say("alice", "den", "/ban bob"))
        .await;
    assert!(!err, "{v}");
    let (err, v) = alice
        .tool("send_message", say("alice", "den", "/unban bob"))
        .await;
    assert!(!err, "{v}");
    assert_eq!(v["command"], "unban");
    let (err, v) = alice
        .tool("send_message", say("alice", "den", "/leave"))
        .await;
    assert!(!err, "{v}");
    assert_eq!(v["result"]["deleted"], true);

    alice
        .tool(
            "create_room",
            json!({ "agent": "alice", "room": "vault", "secure": true }),
        )
        .await;
    let (err, v) = alice
        .tool("send_message", say("alice", "vault", "/leave"))
        .await;
    assert!(!err, "{v}");
    assert_eq!(v["command"], "leave");
    // `//` in a secure room is sent as text too.
    let (err, _) = alice
        .tool("send_message", say("alice", "vault", "//x"))
        .await;
    assert!(err, "alice left the vault");
}

/// Every write in another agent's name is refused before it is signed, and an
/// invitation to a keyless name says the name has no key.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_proxy_never_signs_as_someone_else() {
    let w = world().await;
    let mut alice = w.harness("alice");
    alice.tool("register", json!({ "agent": "alice" })).await;
    for (tool, args) in [
        ("create_room", json!({ "agent": "bob", "room": "r" })),
        ("join_room", json!({ "agent": "bob", "room": "r" })),
        ("leave_room", json!({ "agent": "bob", "room": "r" })),
        (
            "invite",
            json!({ "agent": "bob", "room": "r", "invitee": "carol" }),
        ),
        (
            "send_message",
            json!({ "from": "bob", "room": "r", "body": "x" }),
        ),
    ] {
        let (err, v) = alice.tool(tool, args).await;
        assert!(err, "{tool}: {v}");
        assert_eq!(v["error"], "invalid_body", "{tool}");
    }
    alice
        .tool(
            "create_room",
            json!({ "agent": "alice", "room": "den", "private": "true" }),
        )
        .await;
    w.bus
        .register(&mentischat::names::AgentName::parse("keyless").unwrap())
        .await
        .unwrap();
    let (err, v) = alice
        .tool(
            "invite",
            json!({ "agent": "alice", "room": "den", "invitee": "keyless" }),
        )
        .await;
    assert!(err, "{v}");
    let (err, v) = alice
        .tool("send_message", json!({ "from": "alice", "room": "den" }))
        .await;
    assert!(err, "no body: {v}");
    let (err, v) = alice
        .tool("send_message", json!({ "from": "alice", "body": "x" }))
        .await;
    assert!(err, "no recipient: {v}");
}
