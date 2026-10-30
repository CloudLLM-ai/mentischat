//! MCP wire tests: initialize handshake, tools/list, tools/call through the
//! composed router, plus transport rules (GET/DELETE 405, Origin 403).

use std::sync::Arc;

use axum::Router;
use http_body_util::BodyExt;
use serde_json::{json, Value};
use tower::ServiceExt;

use mentischat::bus::Bus;

fn app() -> (tempfile::TempDir, Router) {
    let dir = tempfile::tempdir().unwrap();
    let bus = Arc::new(Bus::new(&dir.path().join("mcp")).unwrap());
    // Production injects ConnectInfo via into_make_service_with_connect_info;
    // under oneshot we stub it with axum's test layer so IP filtering runs.
    let router = mentischat::build_router(bus).layer(axum::extract::connect_info::MockConnectInfo(
        std::net::SocketAddr::from(([127, 0, 0, 1], 51_234)),
    ));
    (dir, router)
}

async fn rpc(app: &Router, method: &str, params: Value) -> (u16, Value) {
    let body = json!({ "jsonrpc": "2.0", "id": 1, "method": method, "params": params });
    let req = http::Request::builder()
        .method("POST")
        .uri("/mcp")
        .header("content-type", "application/json")
        .body(axum::body::Body::from(body.to_string()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status().as_u16();
    let bytes = BodyExt::collect(resp.into_body()).await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

async fn rest_post(app: &mut Router, uri: &str, body: &str) {
    let req = http::Request::builder()
        .method("POST")
        .uri(uri)
        .header("content-type", "application/json")
        .body(axum::body::Body::from(body.as_bytes().to_vec()))
        .unwrap();
    let _ = app.clone().oneshot(req).await;
}

#[tokio::test]
async fn initialize_returns_protocol_version_and_server_name() {
    let (_d, app) = app();
    let (status, resp) = rpc(
        &app,
        "initialize",
        json!({
            "protocolVersion": "2025-06-18",
            "capabilities": {},
            "clientInfo": { "name": "test-harness", "version": "0" }
        }),
    )
    .await;
    assert_eq!(status, 200);
    assert!(resp["result"]["protocolVersion"].as_str().is_some());
    assert_eq!(resp["result"]["serverInfo"]["name"], "mentischat");
    assert_eq!(resp["result"]["serverInfo"]["version"], "0.1.0");
    // Instructions ritual rides the initialize result.
    assert!(resp["result"]["instructions"]
        .as_str()
        .unwrap()
        .contains("register"));
}

#[tokio::test]
async fn notification_gets_202() {
    let (_d, app) = app();
    let body = json!({ "jsonrpc": "2.0", "method": "notifications/initialized" });
    let req = http::Request::builder()
        .method("POST")
        .uri("/mcp")
        .header("content-type", "application/json")
        .body(axum::body::Body::from(body.to_string()))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status().as_u16(), 202);
}

#[tokio::test]
async fn tools_list_shows_the_mailbox_and_room_tools_pattern_free() {
    let (_d, app) = app();
    let (_, resp) = rpc(&app, "tools/list", json!({})).await;
    let tools = resp["result"]["tools"].as_array().unwrap();
    let names: Vec<&str> = tools.iter().filter_map(|t| t["name"].as_str()).collect();
    assert_eq!(
        names,
        vec![
            "register",
            "deregister",
            "send_message",
            "check_inbox",
            "history",
            "list_agents",
            "create_room",
            "join_room",
            "invite",
            "leave_room",
            "list_rooms"
        ]
    );
    let raw = serde_json::to_string(tools).unwrap();
    assert!(
        !raw.contains("\"pattern\""),
        "no regex patterns may leak into schemas"
    );
    assert!(raw.contains("check_inbox"));
}

#[tokio::test]
async fn tool_call_after_rest_register_proves_shared_bus() {
    let (_d, mut app) = app();
    rest_post(&mut app, "/agents", r#"{"agent":"opencode"}"#).await;

    let (status, resp) = rpc(
        &app,
        "tools/call",
        json!({ "name": "list_agents", "arguments": {} }),
    )
    .await;
    assert_eq!(status, 200);
    let text = resp["result"]["content"][0]["text"].as_str().unwrap();
    let parsed: Value = serde_json::from_str(text).unwrap();
    assert_eq!(parsed["agents"][0]["name"], "opencode");
    assert_eq!(resp["result"]["isError"], false);
}

#[tokio::test]
async fn domain_error_is_iserror_true_content_block() {
    let (_d, app) = app();
    let (_, resp) = rpc(
        &app,
        "tools/call",
        json!({ "name": "register", "arguments": { "agent": "-bad name" } }),
    )
    .await;
    assert_eq!(resp["result"]["isError"], true);
    let text = resp["result"]["content"][0]["text"].as_str().unwrap();
    let err: Value = serde_json::from_str(text).unwrap();
    assert_eq!(err["error"], "invalid_name");
}

#[tokio::test]
async fn register_tool_requires_a_name_and_takes_provider_description_and_reclaim() {
    let (_d, app) = app();
    let (_, resp) = rpc(&app, "tools/list", json!({})).await;
    let tools = resp["result"]["tools"].as_array().unwrap();
    let register = tools.iter().find(|t| t["name"] == "register").unwrap();
    let props = register["inputSchema"]["properties"].as_object().unwrap();
    let mut names: Vec<&str> = props.keys().map(String::as_str).collect();
    names.sort_unstable();
    assert_eq!(names, ["agent", "description", "provider", "reclaim"]);
    assert_eq!(props["reclaim"]["type"], "boolean");
    assert_eq!(register["inputSchema"]["required"], json!(["agent"]));

    let history = tools.iter().find(|t| t["name"] == "history").unwrap();
    let props = history["inputSchema"]["properties"].as_object().unwrap();
    let mut names: Vec<&str> = props.keys().map(String::as_str).collect();
    names.sort_unstable();
    assert_eq!(names, ["agent", "before", "room", "with"]);
    assert_eq!(history["inputSchema"]["required"], json!(["agent"]));
}

#[tokio::test]
async fn instructions_say_nothing_wakes_an_idle_agent() {
    let (_d, app) = app();
    let (_, init) = rpc(
        &app,
        "initialize",
        json!({"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "t", "version": "0"}}),
    )
    .await;
    let instructions = init["result"]["instructions"].as_str().unwrap();
    assert!(
        instructions.contains("Nothing wakes an idle agent"),
        "{instructions}"
    );
    assert!(instructions.contains("check_inbox"));
}

#[tokio::test]
async fn send_and_pull_over_mcp_roundtrip() {
    let (_d, app) = app();
    for who in ["grok", "codex"] {
        rpc(
            &app,
            "tools/call",
            json!({ "name": "register", "arguments": { "agent": who } }),
        )
        .await;
    }
    let (s, _) = rpc(
        &app,
        "tools/call",
        json!({ "name": "send_message", "arguments": { "from": "grok", "to": "codex", "body": "ping" } }),
    )
    .await;
    assert_eq!(s, 200);

    let (_, resp) = rpc(
        &app,
        "tools/call",
        json!({ "name": "check_inbox", "arguments": { "agent": "codex", "wait_ms": 0 } }),
    )
    .await;
    let text = resp["result"]["content"][0]["text"].as_str().unwrap();
    let page: Value = serde_json::from_str(text).unwrap();
    assert_eq!(page["messages"][0]["body"], "ping");

    // check_inbox returns a message once; history shows it again.
    let (_, resp) = rpc(
        &app,
        "tools/call",
        json!({ "name": "check_inbox", "arguments": { "agent": "codex", "wait_ms": 0 } }),
    )
    .await;
    let text = resp["result"]["content"][0]["text"].as_str().unwrap();
    let page: Value = serde_json::from_str(text).unwrap();
    assert_eq!(page["messages"].as_array().unwrap().len(), 0);

    let (_, resp) = rpc(
        &app,
        "tools/call",
        json!({ "name": "history", "arguments": { "agent": "grok", "with": "codex" } }),
    )
    .await;
    let text = resp["result"]["content"][0]["text"].as_str().unwrap();
    let page: Value = serde_json::from_str(text).unwrap();
    assert_eq!(page["messages"][0]["body"], "ping", "{page}");
    assert!(page["messages"][0]["readAt"].as_str().is_some(), "{page}");
    assert!(page["next"].is_null());

    for before in [json!(0), json!(-3), json!("x"), json!(1.5), json!(true)] {
        let (_, resp) = rpc(
            &app,
            "tools/call",
            json!({ "name": "history", "arguments": { "agent": "grok", "before": before } }),
        )
        .await;
        let err: Value =
            serde_json::from_str(resp["result"]["content"][0]["text"].as_str().unwrap()).unwrap();
        assert_eq!(err["error"], "invalid_cursor", "{before}");
    }
}

#[tokio::test]
async fn get_and_delete_mcp_are_405() {
    let (_d, app) = app();
    for method in ["GET", "DELETE"] {
        let req = http::Request::builder()
            .method(method)
            .uri("/mcp")
            .body(axum::body::Body::empty())
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status().as_u16(), 405, "{method} /mcp must be 405");
    }
}

#[tokio::test]
async fn foreign_origin_gets_403() {
    let (_d, app) = app();
    let body = json!({ "jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {} });
    let req = http::Request::builder()
        .method("POST")
        .uri("/mcp")
        .header("content-type", "application/json")
        .header("origin", "https://evil.example")
        .body(axum::body::Body::from(body.to_string()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status().as_u16(), 403);

    // Localhost origin passes.
    let req = http::Request::builder()
        .method("POST")
        .uri("/mcp")
        .header("content-type", "application/json")
        .header("origin", "http://127.0.0.1:9480")
        .body(axum::body::Body::from(body.to_string()))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status().as_u16(), 200);
}

#[tokio::test]
async fn rest_foreign_origin_gets_403() {
    let (_d, app) = app();
    let req = http::Request::builder()
        .method("GET")
        .uri("/health")
        .header("origin", "https://evil.example")
        .body(axum::body::Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status().as_u16(), 403);

    let req = http::Request::builder()
        .method("GET")
        .uri("/health")
        .header("origin", "http://localhost:3000")
        .body(axum::body::Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status().as_u16(), 200);

    // curl / scripts with no Origin still work.
    let req = http::Request::builder()
        .method("GET")
        .uri("/health")
        .body(axum::body::Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status().as_u16(), 200);
}

#[tokio::test]
async fn merged_unknown_rest_path_is_not_found_not_mcp_405() {
    let (_d, app) = app();
    let req = http::Request::builder()
        .method("GET")
        .uri("/definitely-not-a-route")
        .body(axum::body::Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status().as_u16(), 404);
    let bytes = BodyExt::collect(resp.into_body()).await.unwrap().to_bytes();
    let v: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(v["error"], "not_found");
}

#[tokio::test]
async fn merged_health_is_served_beside_mcp() {
    let (_d, app) = app();
    let req = http::Request::builder()
        .method("GET")
        .uri("/health")
        .body(axum::body::Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status().as_u16(), 200);
    let bytes = BodyExt::collect(resp.into_body()).await.unwrap().to_bytes();
    let v: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(v["ok"], true);
}

#[tokio::test]
async fn non_loopback_peer_is_403() {
    let dir = tempfile::tempdir().unwrap();
    let bus = Arc::new(Bus::new(&dir.path().join("offbox")).unwrap());
    let app = mentischat::build_router(bus).layer(axum::extract::connect_info::MockConnectInfo(
        std::net::SocketAddr::from(([8, 8, 8, 8], 9)),
    ));
    let req = http::Request::builder()
        .method("GET")
        .uri("/health")
        .body(axum::body::Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status().as_u16(), 403);
}

#[tokio::test]
async fn register_over_mcp_takes_a_chosen_name_and_suggests_free_ones() {
    let (_d, app) = app();
    let call = |args: Value| json!({ "name": "register", "arguments": args });
    let out = |resp: &Value| -> Value {
        serde_json::from_str(resp["result"]["content"][0]["text"].as_str().unwrap()).unwrap()
    };

    let (_, one) = rpc(
        &app,
        "tools/call",
        call(json!({"agent": "Orion", "provider": "claude_code", "description": "reviews PRs"})),
    )
    .await;
    assert_eq!(one["result"]["isError"], false, "{one}");
    assert_eq!(out(&one)["agent"], "Orion");
    assert_eq!(out(&one)["created"], true);
    assert_eq!(out(&one)["provider"], "claude_code");
    assert_eq!(out(&one)["description"], "reviews PRs");

    let (_, two) = rpc(&app, "tools/call", call(json!({"agent": "orion"}))).await;
    assert_eq!(two["result"]["isError"], true, "{two}");
    assert_eq!(out(&two)["error"], "name_taken");
    assert_eq!(out(&two)["agent"], "Orion");
    assert_eq!(out(&two)["suggestions"].as_array().unwrap().len(), 5);

    let (_, again) = rpc(
        &app,
        "tools/call",
        call(json!({"agent": "orion", "reclaim": "true"})),
    )
    .await;
    assert_eq!(out(&again)["agent"], "Orion");
    assert_eq!(out(&again)["created"], false);
    assert_eq!(
        out(&again)["description"],
        "reviews PRs",
        "a reclaim without a description keeps the stored one"
    );

    for bad in [json!({}), json!({"provider": "claude_code"})] {
        let (_, resp) = rpc(&app, "tools/call", call(bad)).await;
        assert_eq!(resp["result"]["isError"], true, "{resp}");
        assert_eq!(out(&resp)["error"], "invalid_name");
    }

    let (_, list) = rpc(
        &app,
        "tools/call",
        json!({ "name": "list_agents", "arguments": {} }),
    )
    .await;
    let roster: Value =
        serde_json::from_str(list["result"]["content"][0]["text"].as_str().unwrap()).unwrap();
    assert_eq!(roster["agents"][0]["description"], "reviews PRs");
    assert_eq!(roster["agents"][0]["provider"], "claude_code");
    assert_eq!(roster["agents"][0]["queued"], 0);
    assert_eq!(roster["suggestions"].as_array().unwrap().len(), 5);
}

#[tokio::test]
async fn dns_rebinding_host_gets_403() {
    let (_d, app) = app();
    let get = |host: &str| {
        http::Request::builder()
            .method("GET")
            .uri("/agents")
            .header("host", host)
            .body(axum::body::Body::empty())
            .unwrap()
    };
    // A rebound page sends its own hostname and, on a same-origin GET, no Origin.
    for foreign in [
        "evil.example:9480",
        "localhost.evil.example:9480",
        "127.0.0.2:9480",
    ] {
        let resp = app.clone().oneshot(get(foreign)).await.unwrap();
        assert_eq!(resp.status().as_u16(), 403, "Host {foreign}");
    }
    for loopback in [
        "127.0.0.1:9480",
        "localhost:9480",
        "[::1]:9480",
        "localhost",
    ] {
        let resp = app.clone().oneshot(get(loopback)).await.unwrap();
        assert_eq!(resp.status().as_u16(), 200, "Host {loopback}");
    }
}

#[tokio::test]
async fn agents_are_told_how_names_reads_and_retention_work() {
    let (_d, app) = app();
    let (_, init) = rpc(
        &app,
        "initialize",
        json!({"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "t", "version": "0"}}),
    )
    .await;
    let instructions = init["result"]["instructions"].as_str().unwrap();
    for phrase in [
        "a name of your own",
        "name_taken",
        "reclaim: true",
        "marks them read",
        "history shows",
        "Unread messages wait until they are read",
        "kept for 1 day after they are read",
        "for 1 day is dropped",
        "held for 10 minutes",
    ] {
        assert!(instructions.contains(phrase), "{phrase:?}: {instructions}");
    }
}

#[tokio::test]
async fn agents_are_told_when_to_wait_and_how_to_mark_what_they_need() {
    let (_d, app) = app();
    let (_, init) = rpc(
        &app,
        "initialize",
        json!({"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "t", "version": "0"}}),
    )
    .await;
    let instructions = init["result"]["instructions"].as_str().unwrap();
    for phrase in [
        // The four cases, each with an example request.
        "1. Tell.",
        "tell Orion the tests pass",
        "2. Ask, answer can wait.",
        "ask Lyra to look at the flaky test when she can",
        "3. Ask, answer needed now.",
        "ask Vega which port the API uses",
        "4. Conversation.",
        "debug this with Vega",
        // How to wait, and when to stop.
        "check_inbox with wait_ms 30000",
        "After 10 empty calls",
        "Finish with a message marked",
        // The markers, for senders and for recipients.
        "\"No reply needed\"",
        "\"Reply when you can\"",
        "\"Waiting for your reply\"",
        "the sender is blocked on you",
        // Recovering mail a failed pull already marked read.
        "If check_inbox fails or times out",
    ] {
        assert!(instructions.contains(phrase), "{phrase:?}: {instructions}");
    }
    assert!(
        !instructions.contains("expire"),
        "unread mail has no expiry to mention: {instructions}"
    );

    let (_, list) = rpc(&app, "tools/list", json!({})).await;
    let tools = list["result"]["tools"].as_array().unwrap();
    let described = |tool: &str| {
        tools.iter().find(|t| t["name"] == tool).unwrap()["description"]
            .as_str()
            .unwrap()
            .to_string()
    };
    let send = described("send_message");
    assert!(send.contains("Waiting for your reply"), "{send}");
    assert!(send.contains("wait_ms 30000"), "{send}");
    let inbox = described("check_inbox");
    assert!(inbox.contains("wait_ms 30000"), "{inbox}");
    assert!(inbox.contains("up to 10 times"), "{inbox}");
}

#[tokio::test]
async fn a_cross_site_page_cannot_drain_an_inbox() {
    let (_d, mut app) = app();
    rest_post(&mut app, "/agents", r#"{"agent":"a"}"#).await;
    rest_post(&mut app, "/agents", r#"{"agent":"b"}"#).await;
    rest_post(
        &mut app,
        "/messages",
        r#"{"from":"a","to":"b","body":"keep me"}"#,
    )
    .await;

    // What an <img src> on another site sends: loopback Host, no Origin.
    for site in ["cross-site", "same-site"] {
        let req = http::Request::builder()
            .method("GET")
            .uri("/inboxes/b")
            .header("host", "127.0.0.1:9480")
            .header("sec-fetch-site", site)
            .body(axum::body::Body::empty())
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status().as_u16(), 403, "{site}");
    }

    // Typed into the address bar, and curl (no header), still work.
    let req = http::Request::builder()
        .method("GET")
        .uri("/health")
        .header("sec-fetch-site", "none")
        .body(axum::body::Body::empty())
        .unwrap();
    assert_eq!(
        app.clone().oneshot(req).await.unwrap().status().as_u16(),
        200
    );
    let req = http::Request::builder()
        .uri("/inboxes/b")
        .body(axum::body::Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    let bytes = BodyExt::collect(resp.into_body()).await.unwrap().to_bytes();
    let page: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(page["messages"][0]["body"], "keep me", "the mail survived");
}

#[tokio::test]
async fn an_unknown_tool_is_a_parameter_error_with_a_short_echo() {
    let (_d, app) = app();
    let long = "x".repeat(10_000);
    let (status, resp) = rpc(&app, "tools/call", json!({"name": long, "arguments": {}})).await;
    assert_eq!(status, 400, "{resp}");
    assert_eq!(resp["error"]["code"], -32602, "{resp}");
    let message = resp["error"]["message"].as_str().unwrap();
    assert!(
        message.len() < 200,
        "the name is not echoed in full: {} bytes",
        message.len()
    );
}

#[tokio::test]
async fn a_non_string_name_is_invalid_body() {
    let (_d, app) = app();
    let call = |name: &str, args: Value| json!({ "name": name, "arguments": args });
    for (tool, args) in [
        ("register", json!({"agent": "a", "provider": 5})),
        ("register", json!({"agent": ["a"]})),
        ("history", json!({"agent": "a", "with": 3})),
        ("send_message", json!({"from": 1, "to": "b", "body": "x"})),
        ("check_inbox", json!({"agent": true})),
    ] {
        let (_, resp) = rpc(&app, "tools/call", call(tool, args.clone())).await;
        let err: Value =
            serde_json::from_str(resp["result"]["content"][0]["text"].as_str().unwrap()).unwrap();
        assert_eq!(err["error"], "invalid_body", "{tool} {args}");
    }
}

/// Call a tool and return `(is_error, parsed JSON text)`.
async fn call(app: &Router, tool: &str, args: Value) -> (bool, Value) {
    let (_, resp) = rpc(
        app,
        "tools/call",
        json!({ "name": tool, "arguments": args }),
    )
    .await;
    let text = resp["result"]["content"][0]["text"].as_str().unwrap();
    (
        resp["result"]["isError"].as_bool().unwrap_or(false),
        serde_json::from_str(text).unwrap(),
    )
}

#[tokio::test]
async fn room_tools_describe_themselves_with_primitive_schemas() {
    let (_d, app) = app();
    let (_, resp) = rpc(&app, "tools/list", json!({})).await;
    let tools = resp["result"]["tools"].as_array().unwrap();
    let tool = |name: &str| tools.iter().find(|t| t["name"] == name).unwrap();
    let props = |name: &str| {
        let mut keys: Vec<String> = tool(name)["inputSchema"]["properties"]
            .as_object()
            .unwrap()
            .keys()
            .cloned()
            .collect();
        keys.sort();
        keys
    };
    assert_eq!(
        props("create_room"),
        ["agent", "description", "private", "room"]
    );
    assert_eq!(
        tool("create_room")["inputSchema"]["required"],
        json!(["agent", "room"])
    );
    assert_eq!(
        tool("create_room")["inputSchema"]["properties"]["private"]["type"],
        "boolean"
    );
    assert_eq!(props("join_room"), ["agent", "room"]);
    assert_eq!(props("invite"), ["agent", "invitee", "room"]);
    assert_eq!(props("leave_room"), ["agent", "room"]);
    assert_eq!(props("list_rooms"), ["after", "agent", "limit"]);
    assert_eq!(props("list_agents"), ["after", "limit"]);
    assert_eq!(props("send_message"), ["body", "from", "room", "to"]);
    assert_eq!(
        tool("send_message")["inputSchema"]["required"],
        json!(["from", "body"])
    );
    for t in tools {
        assert!(
            !t["description"].as_str().unwrap().is_empty(),
            "{}",
            t["name"]
        );
    }
}

#[tokio::test]
async fn two_agents_talk_in_a_room_through_the_tools() {
    let (_d, app) = app();
    for agent in ["a", "b"] {
        call(&app, "register", json!({ "agent": agent })).await;
    }
    let (err, room) = call(
        &app,
        "create_room",
        json!({"agent": "a", "room": "Lobby", "description": "chat"}),
    )
    .await;
    assert!(!err, "{room}");
    assert_eq!(room["room"], "Lobby");
    assert_eq!(room["private"], false);
    assert_eq!(room["members"], json!(["a"]));
    let (_, joined) = call(&app, "join_room", json!({"agent": "b", "room": "lobby"})).await;
    assert_eq!(joined["members"], json!(["a", "b"]));

    let (err, receipt) = call(
        &app,
        "send_message",
        json!({"from": "a", "room": "lobby", "body": "Reply when you can: hi room"}),
    )
    .await;
    assert!(!err, "{receipt}");
    assert_eq!(receipt["room"], "Lobby");
    assert!(receipt.get("to").is_none(), "{receipt}");

    let (_, page) = call(&app, "check_inbox", json!({"agent": "b"})).await;
    assert_eq!(page["messages"][0]["room"], "Lobby", "{page}");
    assert_eq!(page["messages"][0]["from"], "a");
    assert!(page["messages"][0].get("to").is_none());
    assert_eq!(page["remaining"], 0);

    let (_, history) = call(&app, "history", json!({"agent": "b", "room": "lobby"})).await;
    assert_eq!(
        history["messages"][0]["body"],
        "Reply when you can: hi room"
    );

    let (_, listing) = call(&app, "list_rooms", json!({"agent": "b"})).await;
    assert_eq!(listing["rooms"][0]["joined"], true);
    assert_eq!(listing["rooms"][0]["members"], 2);

    let (_, left) = call(&app, "leave_room", json!({"agent": "b", "room": "lobby"})).await;
    assert_eq!(left, json!({"room": "lobby", "deleted": false}));
}

#[tokio::test]
async fn room_tool_failures_are_contract_errors() {
    let (_d, app) = app();
    for agent in ["a", "b"] {
        call(&app, "register", json!({ "agent": agent })).await;
    }
    call(
        &app,
        "create_room",
        json!({"agent": "a", "room": "den", "private": true}),
    )
    .await;
    let cases = [
        (
            "join_room",
            json!({"agent": "b", "room": "den"}),
            "private_room",
        ),
        (
            "join_room",
            json!({"agent": "b", "room": "nowhere"}),
            "room_not_found",
        ),
        (
            "join_room",
            json!({"agent": "b", "room": "-bad"}),
            "invalid_room",
        ),
        ("join_room", json!({"agent": "b"}), "invalid_room"),
        (
            "join_room",
            json!({"agent": "b", "room": 7}),
            "invalid_body",
        ),
        (
            "create_room",
            json!({"agent": "b", "room": "DEN"}),
            "room_exists",
        ),
        (
            "create_room",
            json!({"agent": "b", "room": "x", "private": "maybe"}),
            "invalid_body",
        ),
        (
            "invite",
            json!({"agent": "b", "room": "den", "invitee": "a"}),
            "not_a_member",
        ),
        (
            "invite",
            json!({"agent": "a", "room": "den", "invitee": "ghost"}),
            "unregistered",
        ),
        (
            "leave_room",
            json!({"agent": "b", "room": "den"}),
            "not_a_member",
        ),
        (
            "history",
            json!({"agent": "b", "room": "den"}),
            "not_a_member",
        ),
        (
            "history",
            json!({"agent": "a", "room": "den", "with": "b"}),
            "invalid_body",
        ),
        (
            "send_message",
            json!({"from": "b", "room": "den", "body": "x"}),
            "not_a_member",
        ),
        (
            "send_message",
            json!({"from": "a", "room": "den", "to": "b", "body": "x"}),
            "invalid_body",
        ),
        (
            "send_message",
            json!({"from": "a", "body": "x"}),
            "invalid_name",
        ),
        ("list_rooms", json!({"agent": "ghost"}), "unregistered"),
    ];
    for (tool, args, code) in cases {
        let (is_error, body) = call(&app, tool, args.clone()).await;
        assert!(is_error, "{tool} {args}");
        assert_eq!(body["error"], code, "{tool} {args}: {body}");
    }
}

#[tokio::test]
async fn agents_are_told_about_rooms_without_the_rest_changing() {
    let (_d, app) = app();
    let (_, init) = rpc(
        &app,
        "initialize",
        json!({"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "t", "version": "0"}}),
    )
    .await;
    let instructions = init["result"]["instructions"].as_str().unwrap();
    for phrase in [
        "ROOMS",
        "create_room",
        "private: true",
        "not security",
        "join_room",
        "send_message with room instead of to",
        "each marked with its room",
        "skipped",
        "history with room",
        "leave_room",
        // The sections that were already there.
        "JOINING",
        "1. Tell.",
        "Call deregister only when the user says they are done.",
    ] {
        assert!(instructions.contains(phrase), "{phrase:?}: {instructions}");
    }
}
