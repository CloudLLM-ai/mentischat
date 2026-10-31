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
async fn inbox_get_marks_what_it_returns_read() {
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
    assert!(p1["messages"][0]["readAt"].as_str().is_some(), "{p1}");

    let (_, p2) = send(&mut app, req("GET", "/inboxes/b")).await;
    assert_eq!(p2["messages"].as_array().unwrap().len(), 0, "pulled once");
    let (_, h) = send(&mut app, req("GET", "/history/b")).await;
    assert_eq!(h["messages"][0]["body"], "m1", "kept as read: {h}");
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

#[tokio::test(start_paused = true)]
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

#[tokio::test(start_paused = true)]
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
    // The oldest waiter got the row and marked it read; the other times out
    // empty.
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
    assert_eq!(h["read"], 0);
    send(&mut app, req("GET", "/inboxes/b")).await;
    let (_, h) = send(&mut app, req("GET", "/health")).await;
    assert_eq!(h["pending"], 0, "read mail is not pending");
    assert_eq!(h["read"], 1, "read mail is kept");
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
async fn register_takes_a_chosen_name_and_refuses_a_taken_one() {
    let (_d, mut app) = app();
    let (s, v) = send(
        &mut app,
        post(
            "/agents",
            r#"{"agent":"Vega","provider":"Claude_Code","description":"cli"}"#,
        ),
    )
    .await;
    assert_eq!(s, 200, "{v}");
    assert_eq!(v["agent"], "Vega");
    assert_eq!(v["provider"], "claude_code");
    assert_eq!(v["created"], true);

    let (s, v) = send(&mut app, post("/agents", r#"{"agent":"vega"}"#)).await;
    assert_eq!((s, v["error"].as_str()), (409, Some("name_taken")), "{v}");
    assert_eq!(v["agent"], "Vega");
    assert_eq!(v["reason"], "registered");
    assert_eq!(v["suggestions"].as_array().unwrap().len(), 5);

    let (s, v) = send(
        &mut app,
        post("/agents", r#"{"agent":"vega","reclaim":true}"#),
    )
    .await;
    assert_eq!((s, v["created"].as_bool()), (200, Some(false)), "{v}");
    assert_eq!(v["provider"], "claude_code", "kept");

    send(&mut app, req("DELETE", "/agents/Vega")).await;
    let (s, v) = send(&mut app, post("/agents", r#"{"agent":"Vega"}"#)).await;
    assert_eq!((s, v["reason"].as_str()), (409, Some("held")), "{v}");
    assert!(v["heldUntil"].as_str().is_some(), "{v}");

    let (_, v) = send(&mut app, req("GET", "/agents")).await;
    assert_eq!(v["suggestions"].as_array().unwrap().len(), 5, "{v}");

    for (body, error) in [
        (r#"{}"#, "invalid_name"),
        (r#"{"provider":"cursor"}"#, "invalid_name"),
        (r#"{"agent":"Lyra","provider":"-cursor"}"#, "invalid_name"),
        (r#"{"agent":"Lyra","provider":7}"#, "invalid_body"),
        (r#"{"agent":"Lyra","reclaim":"yes"}"#, "invalid_body"),
        (
            r#"{"agent":"Lyra","description":"a\nb"}"#,
            "invalid_description",
        ),
    ] {
        let (s, v) = send(&mut app, post("/agents", body)).await;
        assert_eq!((s, v["error"].as_str()), (400, Some(error)), "{body}");
    }
}

#[tokio::test]
async fn history_route_pages_and_rejects_bad_cursors() {
    let (_d, mut app) = app();
    register_two(&mut app).await;
    for i in 0..10 {
        let body = format!(r#"{{"from":"a","to":"b","body":"m{i}"}}"#);
        send(&mut app, post("/messages", &body)).await;
    }
    let resp = app
        .clone()
        .oneshot(req("GET", "/history/a?with=B"))
        .await
        .unwrap();
    assert_eq!(
        resp.headers()
            .get("cache-control")
            .map(|v| v.to_str().unwrap()),
        Some("no-store")
    );
    let bytes = BodyExt::collect(resp.into_body()).await.unwrap().to_bytes();
    let page: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(page["agent"], "a");
    assert_eq!(page["messages"].as_array().unwrap().len(), 8);
    assert_eq!(page["messages"][0]["body"], "m2");
    assert!(page["messages"][0]["readAt"].is_null(), "unread so far");
    let next = page["next"].as_i64().expect("an older page");

    let (s, older) = send(&mut app, req("GET", &format!("/history/a?before={next}"))).await;
    assert_eq!(s, 200);
    assert_eq!(older["messages"][1]["body"], "m1");
    assert!(older["next"].is_null(), "{older}");

    for bad in ["0", "-1", "abc", "1.5", "%FF"] {
        let (s, v) = send(&mut app, req("GET", &format!("/history/a?before={bad}"))).await;
        assert_eq!(
            (s, v["error"].as_str()),
            (400, Some("invalid_cursor")),
            "{bad}"
        );
    }
    let (s, v) = send(&mut app, req("GET", "/history/a?with=-x")).await;
    assert_eq!((s, v["error"].as_str()), (400, Some("invalid_name")), "{v}");
    let (s, v) = send(&mut app, req("GET", "/history/ghost")).await;
    assert_eq!((s, v["error"].as_str()), (404, Some("unregistered")), "{v}");
    let (s, v) = send(&mut app, req("POST", "/history/a")).await;
    assert_eq!((s, v["error"].as_str()), (405, Some("method_not_allowed")));
}

#[tokio::test]
async fn wrong_methods_get_a_json_405_and_head_does_not_drain() {
    let (_d, mut app) = app();
    register_two(&mut app).await;
    let (s, _) = send(
        &mut app,
        post("/messages", r#"{"from":"a","to":"b","body":"still here"}"#),
    )
    .await;
    assert_eq!(s, 200);

    let (s, v) = send(&mut app, req("GET", "/messages")).await;
    assert_eq!((s, v["error"].as_str()), (405, Some("method_not_allowed")));
    let (s, v) = send(&mut app, req("PUT", "/agents")).await;
    assert_eq!((s, v["error"].as_str()), (405, Some("method_not_allowed")));

    let (s, _) = send(&mut app, req("HEAD", "/inboxes/b")).await;
    assert_eq!(s, 405);
    let (_, page) = send(&mut app, req("GET", "/inboxes/b")).await;
    assert_eq!(
        page["messages"][0]["body"], "still here",
        "HEAD deleted nothing"
    );
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

#[tokio::test]
async fn room_routes_happy_path() {
    let (_d, mut app) = app();
    register_two(&mut app).await;
    let (s, v) = send(
        &mut app,
        post(
            "/rooms",
            r#"{"agent":"a","room":"Lobby","description":"chat"}"#,
        ),
    )
    .await;
    assert_eq!(
        (s, &v["room"], &v["members"]),
        (200, &Value::from("Lobby"), &Value::from(vec!["a"]))
    );

    let (s, v) = send(&mut app, post("/rooms/lobby/members", r#"{"agent":"b"}"#)).await;
    assert_eq!(s, 200);
    assert_eq!(v["members"], serde_json::json!(["a", "b"]));

    let (s, v) = send(
        &mut app,
        post(
            "/messages",
            r#"{"from":"a","room":"lobby","body":"hi room"}"#,
        ),
    )
    .await;
    assert_eq!(s, 200);
    assert_eq!(v["room"], "Lobby");

    let (s, v) = send(&mut app, req("GET", "/inboxes/b")).await;
    assert_eq!(s, 200);
    assert_eq!(v["messages"][0]["room"], "Lobby");
    assert_eq!(v["messages"][0]["body"], "hi room");

    let (s, v) = send(&mut app, req("GET", "/history/b?room=lobby")).await;
    assert_eq!(
        (s, &v["messages"][0]["body"]),
        (200, &Value::from("hi room"))
    );

    let (s, v) = send(&mut app, req("GET", "/rooms?agent=b")).await;
    assert_eq!(s, 200);
    assert_eq!(v["rooms"][0]["joined"], true);

    let (s, v) = send(&mut app, req("DELETE", "/rooms/lobby/members/b")).await;
    assert_eq!((s, &v["deleted"]), (200, &Value::from(false)));
}

#[tokio::test]
async fn private_rooms_are_joined_by_invitation_over_rest() {
    let (_d, mut app) = app();
    register_two(&mut app).await;
    send(
        &mut app,
        post("/rooms", r#"{"agent":"a","room":"den","private":true}"#),
    )
    .await;
    let (s, v) = send(&mut app, post("/rooms/den/members", r#"{"agent":"b"}"#)).await;
    assert_eq!((s, &v["error"]), (404, &Value::from("room_not_found")));
    let (_, listed) = send(&mut app, req("GET", "/rooms?agent=b")).await;
    assert_eq!(listed["rooms"], serde_json::json!([]));

    let (s, _) = send(
        &mut app,
        post("/rooms/den/invitations", r#"{"agent":"a","invitee":"b"}"#),
    )
    .await;
    assert_eq!(s, 200);
    let (s, v) = send(&mut app, post("/rooms/den/members", r#"{"agent":"b"}"#)).await;
    assert_eq!((s, &v["private"]), (200, &Value::from(true)));
}

#[tokio::test]
async fn room_errors_map_to_contract_statuses() {
    let (_d, mut app) = app();
    register_two(&mut app).await;
    send(&mut app, post("/rooms", r#"{"agent":"a","room":"den"}"#)).await;
    let cases = [
        (
            post("/rooms", r#"{"agent":"b","room":"DEN"}"#),
            409,
            "room_exists",
        ),
        (
            post("/rooms", r#"{"agent":"b","room":"-x"}"#),
            400,
            "invalid_room",
        ),
        (post("/rooms", r#"{"agent":"b"}"#), 400, "invalid_room"),
        (post("/rooms", "not json"), 400, "invalid_body"),
        (
            post("/rooms/nowhere/members", r#"{"agent":"b"}"#),
            404,
            "room_not_found",
        ),
        (
            post("/rooms/-x/members", r#"{"agent":"b"}"#),
            400,
            "invalid_room",
        ),
        (post("/rooms/den/members", "[]"), 400, "invalid_body"),
        (
            post("/rooms/den/members", r#"{"agent":"ghost"}"#),
            404,
            "unregistered",
        ),
        (
            post("/rooms/den/invitations", r#"{"agent":"b","invitee":"a"}"#),
            403,
            "not_a_member",
        ),
        (req("DELETE", "/rooms/den/members/b"), 403, "not_a_member"),
        (req("GET", "/rooms"), 400, "invalid_name"),
        (req("GET", "/rooms?agent=ghost"), 404, "unregistered"),
        (req("GET", "/history/b?room=den"), 403, "not_a_member"),
        (
            req("GET", "/history/a?room=den&with=b"),
            400,
            "invalid_body",
        ),
        (req("GET", "/history/a?room=-x"), 400, "invalid_room"),
        (
            post(
                "/messages",
                r#"{"from":"a","room":"den","to":"b","body":"x"}"#,
            ),
            400,
            "invalid_body",
        ),
        (
            post("/messages", r#"{"from":"b","room":"den","body":"x"}"#),
            403,
            "not_a_member",
        ),
        (req("GET", "/rooms/den/members"), 405, "method_not_allowed"),
    ];
    for (request, status, code) in cases {
        let desc = format!("{} {}", request.method(), request.uri());
        let (s, v) = send(&mut app, request).await;
        assert_eq!(
            (s, v["error"].as_str()),
            (status, Some(code)),
            "{desc}: {v}"
        );
    }
}

#[tokio::test]
async fn agents_and_rooms_list_in_pages_over_rest() {
    let (_d, mut app) = app();
    for i in 0..5 {
        send(&mut app, post("/agents", &format!(r#"{{"agent":"n{i}"}}"#))).await;
    }
    let (s, page) = send(&mut app, req("GET", "/agents?limit=2")).await;
    assert_eq!(s, 200);
    assert_eq!(page["agents"].as_array().unwrap().len(), 2);
    assert_eq!(page["next"], "n1");
    let (_, page) = send(&mut app, req("GET", "/agents?limit=2&after=n1")).await;
    assert_eq!(page["agents"][0]["name"], "n2");
    let (_, last) = send(&mut app, req("GET", "/agents?after=n3")).await;
    assert_eq!(last["agents"].as_array().unwrap().len(), 1);
    assert!(last.get("next").is_none());
    for bad in ["limit=0", "limit=x", "limit=-1", "after=-x"] {
        let (s, v) = send(&mut app, req("GET", &format!("/agents?{bad}"))).await;
        assert_eq!(s, 400, "{bad}: {v}");
    }

    for i in 0..3 {
        send(
            &mut app,
            post("/rooms", &format!(r#"{{"agent":"n0","room":"r{i}"}}"#)),
        )
        .await;
    }
    let (_, page) = send(&mut app, req("GET", "/rooms?agent=n1&limit=2")).await;
    assert_eq!(page["rooms"].as_array().unwrap().len(), 2);
    assert_eq!(page["next"], "r1");
    let (_, page) = send(&mut app, req("GET", "/rooms?agent=n1&limit=2&after=r1")).await;
    assert_eq!(page["rooms"][0]["room"], "r2");
    assert!(page.get("next").is_none());
}

#[tokio::test]
async fn room_size_limits_are_conflicts_over_rest() {
    let (_d, mut app) = app();
    send(&mut app, post("/agents", r#"{"agent":"a"}"#)).await;
    for i in 0..256 {
        let (s, _) = send(
            &mut app,
            post("/rooms", &format!(r#"{{"agent":"a","room":"r{i}"}}"#)),
        )
        .await;
        assert_eq!(s, 200);
    }
    let (s, v) = send(&mut app, post("/rooms", r#"{"agent":"a","room":"extra"}"#)).await;
    assert_eq!(
        (s, v["error"].as_str()),
        (409, Some("too_many_rooms")),
        "{v}"
    );
    assert_eq!(v["rooms"], 256);
}

#[tokio::test]
async fn batch_send_applies_each_item_like_post_messages_and_answers_in_order() {
    let (_d, mut app) = app();
    register_two(&mut app).await;
    send(&mut app, post("/rooms", r#"{"agent":"a","room":"lobby"}"#)).await;
    send(&mut app, post("/rooms/lobby/members", r#"{"agent":"b"}"#)).await;
    let batch = r#"[
        {"from":"a","to":"b","body":"one"},
        {"from":"a","to":"ghost","body":"lost"},
        {"from":"a","room":"lobby","body":"to the room"},
        {"from":"a","to":"b","body":"  "},
        {"from":"b","room":"lobby","to":"a","body":"both"},
        7,
        {"from":"a","to":"b","body":"two"}
    ]"#;
    let (s, v) = send(&mut app, post("/messages/batch", batch)).await;
    assert_eq!(s, 200);
    let results = v.as_array().unwrap();
    assert_eq!(results.len(), 7);
    assert_eq!(results[0]["to"], "b");
    assert!(results[0]["id"].as_str().is_some());
    assert_eq!(results[1]["error"], "unregistered");
    assert_eq!(results[2]["room"], "lobby");
    assert_eq!(results[3]["error"], "invalid_body");
    assert_eq!(results[4]["error"], "invalid_body");
    assert_eq!(results[5]["error"], "invalid_body");
    assert_eq!(results[6]["to"], "b");

    let (_, page) = send(&mut app, req("GET", "/inboxes/b")).await;
    let bodies: Vec<&str> = page["messages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["body"].as_str().unwrap())
        .collect();
    assert_eq!(
        bodies,
        ["one", "to the room", "two"],
        "failures did not stop or undo the rest"
    );
}

#[tokio::test]
async fn batch_send_rejects_a_bad_envelope() {
    let (_d, mut app) = app();
    register_two(&mut app).await;
    let one = r#"{"from":"a","to":"b","body":"x"}"#;
    let too_many = format!("[{}]", vec![one; 257].join(","));
    let exactly = format!("[{}]", vec![one; 256].join(","));
    for (body, status) in [
        ("[]".to_string(), 400),
        (one.to_string(), 400),
        ("not json".to_string(), 400),
        (too_many, 400),
        (exactly, 200),
    ] {
        let (s, v) = send(&mut app, post("/messages/batch", &body)).await;
        assert_eq!(s, status, "{}: {v}", &body[..body.len().min(40)]);
    }
    let (_, v) = send(&mut app, post("/messages/batch", "[]")).await;
    assert_eq!(v["error"], "invalid_body");
}

#[tokio::test]
async fn batch_send_accepts_bodies_past_the_one_mib_limit_of_other_routes() {
    let (_d, mut app) = app();
    register_two(&mut app).await;
    // Twenty 60 KiB messages are about 1.2 MiB of JSON.
    let big = "x".repeat(60 * 1024);
    let items: Vec<String> = (0..20)
        .map(|_| format!(r#"{{"from":"a","to":"b","body":"{big}"}}"#))
        .collect();
    let batch = format!("[{}]", items.join(","));
    assert!(batch.len() > 1024 * 1024);
    let (s, v) = send(&mut app, post("/messages/batch", &batch)).await;
    assert_eq!(s, 200);
    assert!(
        v.as_array().unwrap().iter().all(|r| r["id"].is_string()),
        "{v}"
    );

    // POST /messages keeps the 1 MiB limit.
    let oversized = format!(
        r#"{{"from":"a","to":"b","body":"{}"}}"#,
        "x".repeat(1024 * 1024)
    );
    let (s, _) = send(&mut app, post("/messages", &oversized)).await;
    assert_ne!(s, 200);
}

/// The composed daemon router caps bodies at 1 MiB, except the batch route.
#[tokio::test]
async fn in_the_composed_router_only_the_batch_route_may_exceed_one_mib() {
    let dir = tempfile::tempdir().unwrap();
    let bus = Arc::new(Bus::new(&dir.path().join("limits")).unwrap());
    let mut app =
        mentischat::build_router(bus).layer(axum::extract::connect_info::MockConnectInfo(
            std::net::SocketAddr::from(([127, 0, 0, 1], 51_234)),
        ));
    register_two(&mut app).await;
    let big = "x".repeat(60 * 1024);
    let items: Vec<String> = (0..20)
        .map(|_| format!(r#"{{"from":"a","to":"b","body":"{big}"}}"#))
        .collect();
    let batch = format!("[{}]", items.join(","));
    let (s, v) = send(&mut app, post("/messages/batch", &batch)).await;
    assert_eq!(s, 200, "{v}");

    // The same bytes to any other route are over the 1 MiB limit.
    let (s, _) = send(&mut app, post("/messages", &batch)).await;
    assert_eq!(s, 400);
    let (s, v) = send(
        &mut app,
        post("/messages/batch", &"[".repeat(17 * 1024 * 1024)),
    )
    .await;
    assert_eq!(s, 400, "{v}");
}

#[tokio::test]
async fn pulls_take_a_limit_and_bad_limits_are_invalid_limit() {
    let (_d, mut app) = app();
    register_two(&mut app).await;
    let item = r#"{"from":"a","to":"b","body":"x"}"#;
    send(
        &mut app,
        post(
            "/messages/batch",
            &format!("[{}]", vec![item; 100].join(",")),
        ),
    )
    .await;

    let (s, v) = send(&mut app, req("GET", "/inboxes/b?limit=30")).await;
    assert_eq!(
        (s, v["messages"].as_array().unwrap().len(), &v["remaining"]),
        (200, 30, &Value::from(70))
    );
    let (_, v) = send(&mut app, req("GET", "/inboxes/b")).await;
    assert_eq!(
        v["messages"].as_array().unwrap().len(),
        8,
        "no limit: the usual page"
    );
    let (_, v) = send(&mut app, req("GET", "/inboxes/b?limit=")).await;
    assert_eq!(
        v["messages"].as_array().unwrap().len(),
        8,
        "empty means none"
    );
    let (_, v) = send(&mut app, req("GET", "/inboxes/b?limit=9999")).await;
    assert_eq!(
        v["messages"].as_array().unwrap().len(),
        54,
        "capped at 256, 54 were left"
    );

    for bad in ["0", "-1", "x", "1.5", "99999999999999999999999"] {
        let (s, v) = send(&mut app, req("GET", &format!("/inboxes/b?limit={bad}"))).await;
        assert_eq!(
            (s, v["error"].as_str()),
            (400, Some("invalid_limit")),
            "{bad}: {v}"
        );
    }
    let (s, v) = send(&mut app, req("GET", "/agents?limit=0")).await;
    assert_eq!((s, v["error"].as_str()), (400, Some("invalid_limit")));
}
