//! MCP wire tests: initialize handshake, tools/list, tools/call through the
//! composed router, plus transport rules (GET/DELETE 405, Origin 403).

use std::sync::Arc;

use axum::Router;
use http_body_util::BodyExt;
use serde_json::{json, Value};
use tower::ServiceExt;

use llm_chat::bus::Bus;

fn app() -> (tempfile::TempDir, Router) {
    let dir = tempfile::tempdir().unwrap();
    let bus = Arc::new(Bus::new(&dir.path().join("mcp")).unwrap());
    // Production injects ConnectInfo via into_make_service_with_connect_info;
    // under oneshot we stub it with axum's test layer so IP filtering runs.
    let router = llm_chat::build_router(bus).layer(axum::extract::connect_info::MockConnectInfo(
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
    assert_eq!(resp["result"]["serverInfo"]["name"], "llm_chat");
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
