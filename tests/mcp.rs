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
async fn tools_list_shows_exactly_five_pattern_free_tools() {
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
            "list_agents"
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
async fn register_tool_takes_a_name_description_and_exclusive_flag() {
    let (_d, app) = app();
    let (_, resp) = rpc(&app, "tools/list", json!({})).await;
    let tools = resp["result"]["tools"].as_array().unwrap();
    let register = tools.iter().find(|t| t["name"] == "register").unwrap();
    let props = register["inputSchema"]["properties"].as_object().unwrap();
    let mut names: Vec<&str> = props.keys().map(String::as_str).collect();
    names.sort_unstable();
    assert_eq!(
        names,
        ["agent", "description", "exclusive"],
        "no wakeId or nudgeTransport parameter"
    );
    let required = register["inputSchema"]["required"].as_array().unwrap();
    assert_eq!(required, &[json!("agent")], "only the name is required");

    let (_, reg) = rpc(
        &app,
        "tools/call",
        json!({"name": "register", "arguments": {"agent": "grok", "wakeId": "ignored"}}),
    )
    .await;
    assert_eq!(reg["result"]["isError"], false, "{reg}");
    let text = reg["result"]["content"][0]["text"].as_str().unwrap();
    assert!(!text.contains("nudge"), "{text}");
}

#[tokio::test]
async fn nothing_agents_read_promises_a_wake() {
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

    let (_, list) = rpc(&app, "tools/list", json!({})).await;
    let everything = format!("{instructions}\n{list}").to_lowercase();
    for stale in [
        "mentis-wake",
        "mentischat_wake_id",
        "doorbell",
        "nudge",
        "wakeid",
    ] {
        assert!(!everything.contains(stale), "{stale:?} still advertised");
    }
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

    // Ack via the next pull.
    let id = page["messages"][0]["id"].as_str().unwrap().to_string();
    let (_, resp) = rpc(
        &app,
        "tools/call",
        json!({ "name": "check_inbox", "arguments": { "agent": "codex", "wait_ms": 0, "ack_ids": [id] } }),
    )
    .await;
    let text = resp["result"]["content"][0]["text"].as_str().unwrap();
    let page: Value = serde_json::from_str(text).unwrap();
    assert_eq!(page["messages"].as_array().unwrap().len(), 0);
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
async fn register_over_mcp_stores_a_description_and_honors_exclusive() {
    let (_d, app) = app();
    let call = |args: Value| json!({ "name": "register", "arguments": args });

    let (_, reg) = rpc(
        &app,
        "tools/call",
        call(json!({"agent": "grok", "description": "reviews PRs", "exclusive": true})),
    )
    .await;
    assert_eq!(reg["result"]["isError"], false, "{reg}");
    let out: Value =
        serde_json::from_str(reg["result"]["content"][0]["text"].as_str().unwrap()).unwrap();
    assert_eq!(out["created"], true);
    assert_eq!(out["description"], "reviews PRs");

    let (_, taken) = rpc(
        &app,
        "tools/call",
        call(json!({"agent": "grok", "exclusive": true})),
    )
    .await;
    assert_eq!(taken["result"]["isError"], true, "{taken}");
    let err: Value =
        serde_json::from_str(taken["result"]["content"][0]["text"].as_str().unwrap()).unwrap();
    assert_eq!(err["error"], "name_taken");

    let (_, list) = rpc(
        &app,
        "tools/call",
        json!({ "name": "list_agents", "arguments": {} }),
    )
    .await;
    let roster: Value =
        serde_json::from_str(list["result"]["content"][0]["text"].as_str().unwrap()).unwrap();
    assert_eq!(roster["agents"][0]["description"], "reviews PRs");
    assert_eq!(roster["agents"][0]["queued"], 0);
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
