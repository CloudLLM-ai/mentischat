//! REST surface integration tests driven through `tower::ServiceExt::oneshot`
//! against the real router — no sockets, no ports, no `~/.cloudllm/mentischat`.

use std::sync::Arc;

use axum::Router;
use http_body_util::BodyExt;
use serde_json::Value;
use tower::ServiceExt;

use mentischat::bus::Bus;
use mentischat::rest;

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
async fn register_response_has_no_wake_fields_and_ignores_legacy_input() {
    let (_d, mut app) = app();
    let (s, v) = send(
        &mut app,
        post(
            "/agents",
            r#"{"agent":"grok","wakeId":"11111111-1111-4111-8111-111111111111","nudgeTransport":"wrapper"}"#,
        ),
    )
    .await;
    assert_eq!(s, 200, "an older client's extra fields are not an error");
    assert_eq!(v["agent"], "grok");
    assert_eq!(v["created"], true);
    assert!(
        v.get("nudgeTransport").is_none() && v.get("nudgeWindow").is_none(),
        "{v}"
    );

    let (_, v) = send(&mut app, req("GET", "/agents")).await;
    assert!(v["agents"][0].get("nudgeTransport").is_none(), "{v}");
}

#[tokio::test]
async fn wake_and_wrapper_routes_are_gone() {
    let (_d, mut app) = app();
    send(&mut app, post("/agents", r#"{"agent":"grok"}"#)).await;
    for (method, uri) in [
        ("GET", "/nudge/grok"),
        ("GET", "/wake/11111111-1111-4111-8111-111111111111"),
        ("DELETE", "/wake/11111111-1111-4111-8111-111111111111"),
        ("POST", "/agents/grok/wrapper"),
        ("DELETE", "/agents/grok/wrapper"),
    ] {
        let (s, v) = send(&mut app, req(method, uri)).await;
        assert!(
            s == 404 || s == 405,
            "{method} {uri} should not be served, got {s} {v}"
        );
    }
}

#[tokio::test]
async fn inbox_get_deletes_what_it_returns() {
    let (_d, mut app) = app();
    register_two(&mut app).await;
    send(
        &mut app,
        post("/messages", r#"{"from":"a","to":"b","body":"m1"}"#),
    )
    .await;

    let (s, p1) = send(&mut app, req("GET", "/inboxes/b")).await;
    assert_eq!(s, 200);
    assert_eq!(p1["messages"][0]["body"], "m1");

    let (_, p2) = send(&mut app, req("GET", "/inboxes/b")).await;
    assert_eq!(p2["messages"].as_array().unwrap().len(), 0, "read once");

    // There is nothing to acknowledge any more.
    let (s, v) = send(&mut app, post("/inboxes/b/ack", r#"{"ids":[]}"#)).await;
    assert_eq!((s, v["error"].as_str()), (404, Some("not_found")));
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
        w2.oneshot(req("GET", "/inboxes/b?wait_ms=500"))
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
    // The oldest waiter got the row and deleted it; the other times out empty.
    let id1 = first_msg_id(t1.await.unwrap()).await;
    let id2 = first_msg_id(t2.await.unwrap()).await;
    assert!(!id1.is_empty());
    assert!(id2.is_empty(), "delivered once");
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
    assert_eq!(h["pending"], 1, "unread mail counts");
    send(&mut app, req("GET", "/inboxes/b")).await;
    let (_, h) = send(&mut app, req("GET", "/health")).await;
    assert_eq!(h["pending"], 0, "read mail is gone");
}

#[tokio::test]
async fn register_accepts_a_description_and_lists_it() {
    let (_d, mut app) = app();
    let (s, v) = send(
        &mut app,
        post(
            "/agents",
            r#"{"agent":"grok","description":"  reviews PRs  "}"#,
        ),
    )
    .await;
    assert_eq!(s, 200, "{v}");
    assert_eq!(v["description"], "reviews PRs", "stored trimmed");

    let (_, v) = send(&mut app, req("GET", "/agents")).await;
    assert_eq!(v["agents"][0]["description"], "reviews PRs");
    assert_eq!(v["agents"][0]["queued"], 0);

    let too_long = format!(r#"{{"agent":"grok","description":"{}"}}"#, "x".repeat(257));
    let (s, v) = send(&mut app, post("/agents", &too_long)).await;
    assert_eq!((s, v["error"].as_str()), (400, Some("invalid_description")));

    let (s, v) = send(
        &mut app,
        post("/agents", r#"{"agent":"grok","description":7}"#),
    )
    .await;
    assert_eq!((s, v["error"].as_str()), (400, Some("invalid_description")));

    let (s, v) = send(
        &mut app,
        post("/agents", r#"{"agent":"grok","description":"line\nbreak"}"#),
    )
    .await;
    assert_eq!((s, v["error"].as_str()), (400, Some("invalid_description")));
}

#[tokio::test]
async fn exclusive_register_of_a_taken_name_is_a_conflict() {
    let (_d, mut app) = app();
    let (s, _) = send(
        &mut app,
        post("/agents", r#"{"agent":"grok","exclusive":true}"#),
    )
    .await;
    assert_eq!(s, 200);
    let (s, v) = send(
        &mut app,
        post("/agents", r#"{"agent":"grok","exclusive":true}"#),
    )
    .await;
    assert_eq!(s, 409, "{v}");
    assert_eq!(v["error"], "name_taken");
    assert_eq!(v["known"], serde_json::json!(["grok"]));

    let (s, v) = send(
        &mut app,
        post("/agents", r#"{"agent":"grok","exclusive":"yes"}"#),
    )
    .await;
    assert_eq!((s, v["error"].as_str()), (400, Some("invalid_body")));
}

#[tokio::test]
async fn undecodable_paths_and_queries_get_contract_errors() {
    let (_d, mut app) = app();
    register_two(&mut app).await;
    let (s, v) = send(&mut app, req("GET", "/inboxes/%FF")).await;
    assert_eq!((s, v["error"].as_str()), (400, Some("invalid_name")), "{v}");
    let (s, v) = send(&mut app, req("DELETE", "/agents/%FF%FE")).await;
    assert_eq!((s, v["error"].as_str()), (400, Some("invalid_name")), "{v}");
    let (s, v) = send(&mut app, req("GET", "/inboxes/b?wait_ms=%FF")).await;
    assert_eq!((s, v["error"].as_str()), (400, Some("invalid_wait")), "{v}");
}

#[tokio::test]
async fn invalid_name_echo_is_capped() {
    let (_d, mut app) = app();
    let huge = format!(r#"{{"agent":"-{}"}}"#, "x".repeat(100_000));
    let (s, v) = send(&mut app, post("/agents", &huge)).await;
    assert_eq!(s, 400);
    assert_eq!(v["error"], "invalid_name");
    assert!(
        v["agent"].as_str().unwrap().chars().count() <= 65,
        "{}",
        v["agent"]
    );
}
