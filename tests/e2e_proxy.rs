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
        Bus::new(&dir.path().join("daemon"))
            .unwrap()
            .with_room_capacity(capacity),
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

/// Ordinary rooms and direct mail pass through the proxy exactly as they do
/// without it, alongside a secure room.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plain_rooms_and_direct_mail_pass_straight_through() {
    let w = world().await;
    let (mut alice, mut bob) = (w.harness("alice"), w.harness("bob"));
    alice.tool("register", json!({ "agent": "alice" })).await;
    bob.tool("register", json!({ "agent": "bob" })).await;
    let (err, room) = alice
        .tool("create_room", json!({ "agent": "alice", "room": "lobby" }))
        .await;
    assert!(!err, "{room}");
    assert!(
        room.get("secure").is_none(),
        "a plain room is not marked: {room}"
    );
    bob.tool("join_room", json!({ "agent": "bob", "room": "lobby" }))
        .await;
    alice
        .tool(
            "send_message",
            json!({ "from": "alice", "room": "lobby", "body": "hello lobby" }),
        )
        .await;
    alice
        .tool(
            "send_message",
            json!({ "from": "alice", "to": "bob", "body": "hello bob" }),
        )
        .await;
    let (_, page) = bob.tool("check_inbox", json!({ "agent": "bob" })).await;
    let bodies: Vec<&str> = page["messages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["body"].as_str().unwrap())
        .collect();
    assert_eq!(bodies, ["hello lobby", "hello bob"]);

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
