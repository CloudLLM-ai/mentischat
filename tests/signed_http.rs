//! The signed-room REST routes end to end through the real router: key
//! binding, signed operations, the plain routes' refusal, and the status codes
//! of the new errors. No sockets.

use std::sync::Arc;

use axum::Router;
use http_body_util::BodyExt;
use serde_json::{json, Value};
use tower::ServiceExt;

use mentischat::bus::Bus;
use mentischat::rest;
use mentischat::signed::{register_key_payload, Envelope, Keypair, SignedOp};

fn app() -> (tempfile::TempDir, Router) {
    let dir = tempfile::tempdir().unwrap();
    let bus = Arc::new(Bus::new(&dir.path().join("signed-http")).unwrap());
    (dir, rest::router(bus))
}

async fn call(app: &Router, method: &str, uri: &str, body: Option<Value>) -> (u16, Value) {
    let builder = http::Request::builder().method(method).uri(uri);
    let req = match body {
        Some(v) => builder
            .header("content-type", "application/json")
            .body(axum::body::Body::from(v.to_string()))
            .unwrap(),
        None => builder.body(axum::body::Body::empty()).unwrap(),
    };
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status().as_u16();
    let bytes = BodyExt::collect(resp.into_body()).await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

/// Register `name` and bind `keys` to it over REST.
async fn enroll(app: &Router, name: &str, keys: &Keypair) {
    call(app, "POST", "/agents", Some(json!({ "agent": name }))).await;
    let key = keys.public();
    let sig = keys.sign(&register_key_payload(name, &key));
    let (status, v) = call(
        app,
        "PUT",
        &format!("/agents/{name}/key"),
        Some(json!({ "public_key": key.to_hex(), "signature": sig.to_hex() })),
    )
    .await;
    assert_eq!(
        (status, v["publicKey"].as_str()),
        (200, Some(key.to_hex().as_str()))
    );
}

/// A signed op as the JSON body of `POST /signed/ops`.
fn op_body(keys: &Keypair, agent: &str, room: &str, counter: u64, op: &SignedOp) -> Value {
    let sig = keys.sign(&op.payload(room, agent, counter));
    let mut v = json!({
        "op": op.kind().as_str(), "agent": agent, "room": room,
        "counter": counter, "signature": sig.to_hex(),
    });
    match op {
        SignedOp::Send { body } => v["body"] = json!(body),
        SignedOp::Invite {
            invitee,
            invitee_key,
        } => {
            v["invitee"] = json!(invitee);
            v["invitee_key"] = json!(invitee_key.to_hex());
        }
        SignedOp::Remove { target } => v["target"] = json!(target),
        SignedOp::Create { description } => v["description"] = json!(description),
        SignedOp::Join | SignedOp::Leave => {}
    }
    v
}

/// The key routes: a missing key reads as null, binding shows up, a second key
/// conflicts with 409, a bad key is 400, a bad signature 401.
#[tokio::test]
async fn key_routes_bind_read_and_conflict() {
    let (_d, app) = app();
    let keys = Keypair::from_seed([1; 32]);
    call(&app, "POST", "/agents", Some(json!({ "agent": "alice" }))).await;
    let (s, v) = call(&app, "GET", "/agents/alice/key", None).await;
    assert_eq!((s, v["publicKey"].is_null()), (200, true));

    enroll(&app, "alice", &keys).await;
    let (_, v) = call(&app, "GET", "/agents/alice/key", None).await;
    assert_eq!(v["publicKey"], keys.public().to_hex());

    let rival = Keypair::from_seed([2; 32]);
    let sig = rival.sign(&register_key_payload("alice", &rival.public()));
    let body = json!({ "public_key": rival.public().to_hex(), "signature": sig.to_hex() });
    let (s, v) = call(&app, "PUT", "/agents/alice/key", Some(body)).await;
    assert_eq!((s, v["error"].as_str()), (409, Some("key_conflict")));

    let (s, v) = call(
        &app,
        "PUT",
        "/agents/alice/key",
        Some(json!({"public_key": "zz", "signature": "00"})),
    )
    .await;
    assert_eq!((s, v["error"].as_str()), (400, Some("invalid_key")));
    let body = json!({ "public_key": keys.public().to_hex(), "signature": "00" });
    let (s, v) = call(&app, "PUT", "/agents/alice/key", Some(body)).await;
    assert_eq!((s, v["error"].as_str()), (401, Some("invalid_signature")));
    let (s, v) = call(&app, "GET", "/agents/ghost/key", None).await;
    assert_eq!((s, v["error"].as_str()), (404, Some("unregistered")));
}

/// Create, invite, join, send over REST; the plain inbox shows an envelope
/// that verifies; the plain send route answers 401 `signature_required`.
#[tokio::test]
async fn a_signed_conversation_over_rest() {
    let (_d, app) = app();
    let (ak, bk) = (Keypair::from_seed([1; 32]), Keypair::from_seed([2; 32]));
    enroll(&app, "alice", &ak).await;
    enroll(&app, "bob", &bk).await;

    let create = SignedOp::Create {
        description: "ops room".into(),
    };
    let (s, v) = call(
        &app,
        "POST",
        "/signed/ops",
        Some(op_body(&ak, "alice", "ops", 1, &create)),
    )
    .await;
    assert_eq!((s, v["private"].as_bool()), (200, Some(true)), "{v}");
    let invite = SignedOp::Invite {
        invitee: "bob".into(),
        invitee_key: bk.public(),
    };
    let (s, _) = call(
        &app,
        "POST",
        "/signed/ops",
        Some(op_body(&ak, "alice", "ops", 2, &invite)),
    )
    .await;
    assert_eq!(s, 200);
    let (s, v) = call(
        &app,
        "POST",
        "/signed/ops",
        Some(op_body(&bk, "bob", "ops", 1, &SignedOp::Join)),
    )
    .await;
    assert_eq!((s, v["members"].as_array().map(Vec::len)), (200, Some(2)));

    let hi = SignedOp::Send {
        body: "No reply needed: hello".into(),
    };
    let (s, v) = call(
        &app,
        "POST",
        "/signed/ops",
        Some(op_body(&ak, "alice", "ops", 3, &hi)),
    )
    .await;
    assert_eq!((s, v["room"].as_str()), (200, Some("ops")), "{v}");

    let (_, inbox) = call(&app, "GET", "/inboxes/bob", None).await;
    let text = inbox["messages"][0]["body"].as_str().unwrap();
    let env = Envelope::parse(text).unwrap();
    assert_eq!(env.body, "No reply needed: hello");
    assert!(env.verify("ops", "alice").is_ok());

    let (s, v) = call(&app, "GET", "/signed/rooms/ops?agent=bob", None).await;
    assert_eq!(s, 200);
    assert_eq!(v["members"][0]["publicKey"], ak.public().to_hex());

    let plain = json!({ "from": "alice", "room": "ops", "body": "unsigned" });
    let (s, v) = call(&app, "POST", "/messages", Some(plain)).await;
    assert_eq!((s, v["error"].as_str()), (401, Some("signature_required")));
    let (s, v) = call(
        &app,
        "POST",
        "/signed/ops",
        Some(op_body(&ak, "alice", "ops", 3, &hi)),
    )
    .await;
    assert_eq!(
        (s, v["error"].as_str()),
        (409, Some("stale_counter")),
        "replay: {v}"
    );
    assert_eq!(v["last"], 3);
}

/// Bad request shapes: unknown op, `register_key` as a room op, a missing
/// counter, a non-hex signature, and a stranger asking for the member list.
#[tokio::test]
async fn malformed_signed_requests_are_client_errors() {
    let (_d, app) = app();
    let ak = Keypair::from_seed([1; 32]);
    enroll(&app, "alice", &ak).await;
    let good = op_body(
        &ak,
        "alice",
        "ops",
        1,
        &SignedOp::Create {
            description: String::new(),
        },
    );

    let mut bad = good.clone();
    bad["op"] = json!("explode");
    let (s, v) = call(&app, "POST", "/signed/ops", Some(bad)).await;
    assert_eq!((s, v["error"].as_str()), (400, Some("invalid_body")));
    let mut bad = good.clone();
    bad["op"] = json!("register_key");
    assert_eq!(call(&app, "POST", "/signed/ops", Some(bad)).await.0, 400);
    let mut bad = good.clone();
    bad.as_object_mut().unwrap().remove("counter");
    assert_eq!(call(&app, "POST", "/signed/ops", Some(bad)).await.0, 400);
    let mut bad = good.clone();
    bad["signature"] = json!("not hex");
    let (s, v) = call(&app, "POST", "/signed/ops", Some(bad)).await;
    assert_eq!((s, v["error"].as_str()), (401, Some("invalid_signature")));
    let mut bad = good.clone();
    bad["counter"] = json!("12abc");
    assert_eq!(call(&app, "POST", "/signed/ops", Some(bad)).await.0, 400);
    assert_eq!(
        call(&app, "POST", "/signed/ops", Some(json!([1]))).await.0,
        400
    );

    call(&app, "POST", "/signed/ops", Some(good)).await;
    call(&app, "POST", "/agents", Some(json!({ "agent": "eve" }))).await;
    let (s, v) = call(&app, "GET", "/signed/rooms/ops?agent=eve", None).await;
    assert_eq!((s, v["error"].as_str()), (404, Some("room_not_found")));
    let (s, _) = call(&app, "GET", "/signed/rooms/ops", None).await;
    assert_eq!(s, 400);
}
