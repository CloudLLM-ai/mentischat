//! REST surface integration tests driven through `tower::ServiceExt::oneshot`
//! against the real router — no sockets, no ports, no `~/.llm_chat`.

use std::sync::Arc;

use axum::Router;
use http_body_util::BodyExt;
use serde_json::Value;
use tower::ServiceExt;

use llm_chat::bus::Bus;
use llm_chat::rest;

fn app() -> (tempfile::TempDir, Router) {
    let dir = tempfile::tempdir().unwrap();
    let bus = Arc::new(Bus::new(&dir.path().join("http")).unwrap());
    (dir, rest::router(bus))
}

async fn send(app: &mut Router, req: http::Request<axum::body::Body>) -> (u16, Value) {
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status().as_u16();
    let bytes = BodyExt::collect(resp.into_body()).await.unwrap().to_bytes();
    let value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(Value::Null)
    };
    (status, value)
}

fn post(uri: &str, body: &str) -> http::Request<axum::body::Body> {
    http::Request::builder()
        .method("POST")
        .uri(uri)
        .header("content-type", "application/json")
        .body(axum::body::Body::from(body.as_bytes().to_vec()))
        .unwrap()
}

fn req(method: &str, uri: &str) -> http::Request<axum::body::Body> {
    http::Request::builder()
        .method(method)
        .uri(uri)
        .body(axum::body::Body::empty())
        .unwrap()
}

async fn register_two(app: &mut Router) {
    send(app, post("/agents", r#"{"agent":"a"}"#)).await;
    send(app, post("/agents", r#"{"agent":"b"}"#)).await;
}

#[tokio::test]
async fn agent_routes_happy_path() {
    let (_d, mut app) = app();
    let (s, v) = send(&mut app, post("/agents", r#"{"agent":"grok"}"#)).await;
    assert_eq!(s, 200);
    assert_eq!(v["created"], true);

    let (_, v) = send(&mut app, req("GET", "/agents")).await;
    assert_eq!(v["agents"][0]["name"], "grok");
    assert!(v["agents"][0]["lastSeen"].as_str().is_some());

    send(&mut app, post("/agents", r#"{"agent":"codex"}"#)).await;
    let (s, v) = send(&mut app, req("DELETE", "/agents/codex")).await;
    assert_eq!(s, 200);
    assert_eq!(v["discarded"], 0);
    let (_, v) = send(&mut app, req("GET", "/agents")).await;
    assert_eq!(v["agents"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn inbox_get_redelivers_until_rest_ack() {
    let (_d, mut app) = app();
    register_two(&mut app).await;
    send(
        &mut app,
        post("/messages", r#"{"from":"a","to":"b","body":"m1"}"#),
    )
    .await;

    let (s, p1) = send(&mut app, req("GET", "/inboxes/b")).await;
    assert_eq!(s, 200);
    let id = p1["messages"][0]["id"].as_str().unwrap().to_string();

    let (_, p2) = send(&mut app, req("GET", "/inboxes/b")).await;
    assert_eq!(
        p2["messages"][0]["id"],
        id.as_str(),
        "redelivered until acked"
    );

    let ack_body = format!(r#"{{"ids":["{id}"]}}"#);
    let (_, acked) = send(&mut app, post("/inboxes/b/ack", &ack_body)).await;
    assert_eq!(acked["acked"], 1);

    let (_, p3) = send(&mut app, req("GET", "/inboxes/b")).await;
    assert_eq!(p3["messages"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn inbox_get_sets_no_store() {
    let (_d, mut app) = app();
    register_two(&mut app).await;
    let resp = app.clone().oneshot(req("GET", "/inboxes/b")).await.unwrap();
    assert_eq!(
        resp.headers()
            .get("cache-control")
            .map(|v| v.to_str().unwrap()),
        Some("no-store")
    );
}

#[tokio::test]
async fn wait_ms_returns_when_mail_lands_mid_request() {
    let (_d, mut app) = app();
    register_two(&mut app).await;

    let waiter = app.clone();
    let bg = tokio::spawn(async move {
        waiter
            .oneshot(req("GET", "/inboxes/b?wait_ms=2000"))
            .await
            .unwrap()
    });
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    let (_, receipt) = send(
        &mut app,
        post("/messages", r#"{"from":"a","to":"b","body":"knock"}"#),
    )
    .await;

    let bytes = BodyExt::collect(bg.await.unwrap().into_body())
        .await
        .unwrap()
        .to_bytes();
    let page: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(page["messages"][0]["id"], receipt["id"]);
}

#[tokio::test]
async fn overlapping_waits_one_post_one_delivery() {
    let (_d, mut app) = app();
    register_two(&mut app).await;

    let w1 = app.clone();
    let t1 = tokio::spawn(async move {
        w1.oneshot(req("GET", "/inboxes/b?wait_ms=2000"))
            .await
            .unwrap()
    });
    let w2 = app.clone();
    let t2 = tokio::spawn(async move {
        w2.oneshot(req("GET", "/inboxes/b?wait_ms=2000"))
            .await
            .unwrap()
    });
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    send(
        &mut app,
        post("/messages", r#"{"from":"a","to":"b","body":"solo"}"#),
    )
    .await;

    async fn first_msg_id(resp: axum::response::Response) -> String {
        let bytes = BodyExt::collect(resp.into_body()).await.unwrap().to_bytes();
        let page: Value = serde_json::from_slice(&bytes).unwrap();
        page["messages"][0]["id"].as_str().unwrap_or("").to_string()
    }
    // Oldest waiter got the row; the second redelivers it (shared-name rule).
    let id1 = first_msg_id(t1.await.unwrap()).await;
    let id2 = first_msg_id(t2.await.unwrap()).await;
    assert_eq!(id1, id2);
    assert!(!id1.is_empty());
}

#[tokio::test]
async fn error_status_codes_map() {
    let (_d, mut app) = app();

    // 400: invalid JSON body.
    let (s, v) = send(&mut app, post("/agents", "{not json")).await;
    assert_eq!(s, 400);
    assert_eq!(v["error"], "invalid_body");

    // 400: invalid name.
    let (s, v) = send(&mut app, post("/agents", r#"{"agent":"-bad"}"#)).await;
    assert_eq!(s, 400);
    assert_eq!(v["error"], "invalid_name");

    // 404: unknown REST route.
    let (s, v) = send(&mut app, req("GET", "/definitely-not-a-route")).await;
    assert_eq!(s, 404);
    assert_eq!(v["error"], "not_found");

    // 404 unregistered with known roster.
    send(&mut app, post("/agents", r#"{"agent":"a"}"#)).await;
    let (s, v) = send(&mut app, req("GET", "/inboxes/ghost")).await;
    assert_eq!(s, 404);
    assert_eq!(v["error"], "unregistered");
    assert_eq!(v["known"], serde_json::json!(["a"]));

    // 400 invalid_wait via query string.
    send(&mut app, post("/agents", r#"{"agent":"b"}"#)).await;
    let (s, _) = send(&mut app, req("GET", "/inboxes/b?wait_ms=-5")).await;
    assert_eq!(s, 400);

    // Missing body field -> 400 invalid_body.
    let (s, v) = send(&mut app, post("/messages", r#"{"from":"a","to":"b"}"#)).await;
    assert_eq!(s, 400);
    assert_eq!(v["error"], "invalid_body");
}

#[tokio::test]
async fn health_counts_rows_and_roster() {
    let (_d, mut app) = app();
    register_two(&mut app).await;
    send(
        &mut app,
        post("/messages", r#"{"from":"a","to":"b","body":"x"}"#),
    )
    .await;

    let (_, h) = send(&mut app, req("GET", "/health")).await;
    assert_eq!(h["ok"], true);
    assert_eq!(h["agents"], 2);
    assert_eq!(h["pending"], 1, "unacked row counts; acked rows would not");
}
