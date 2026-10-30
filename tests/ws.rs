//! The WebSocket stream over a real socket: push, acks, return on drop, the
//! window, takeover, room fan-out, send and batch frames, the Origin check and
//! shutdown.

mod common;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use common::name;
use futures_util::{SinkExt, StreamExt};
use mentischat::bus::Bus;
use mentischat::names::RoomName;
use serde_json::{json, Value};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

type Client = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

struct Server {
    bus: Arc<Bus>,
    addr: SocketAddr,
    _dir: tempfile::TempDir,
}

async fn server_with(capacity: Option<usize>) -> Server {
    let dir = tempfile::tempdir().unwrap();
    let mut bus = Bus::new(&dir.path().join("ws")).unwrap();
    if let Some(n) = capacity {
        bus = bus.with_room_capacity(n);
    }
    let bus = Arc::new(bus);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = mentischat::build_router(bus.clone());
    tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });
    for who in ["a", "b", "c"] {
        bus.register(&name(who)).await.unwrap();
    }
    Server {
        bus,
        addr,
        _dir: dir,
    }
}

async fn server() -> Server {
    server_with(None).await
}

async fn connect(s: &Server, agent: &str) -> Client {
    let url = format!("ws://{}/ws?agent={agent}", s.addr);
    connect_async(url).await.expect("upgrade").0
}

/// The next JSON text frame, skipping pings, or `None` on close.
async fn next(ws: &mut Client) -> Option<Value> {
    loop {
        let frame = tokio::time::timeout(Duration::from_secs(3), ws.next())
            .await
            .expect("a frame within 3 s")?
            .ok()?;
        match frame {
            Message::Text(t) => return Some(serde_json::from_str(t.as_str()).unwrap()),
            Message::Close(_) => return None,
            _ => {}
        }
    }
}

async fn say(ws: &mut Client, frame: Value) {
    ws.send(Message::Text(frame.to_string().into()))
        .await
        .unwrap();
}

fn bodies(frame: &Value) -> Vec<&str> {
    frame["messages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["body"].as_str().unwrap())
        .collect()
}

#[tokio::test]
async fn mail_is_pushed_the_moment_it_lands() {
    let s = server().await;
    let mut ws = connect(&s, "b").await;
    s.bus
        .send(&name("a"), &name("b"), "hello".into())
        .await
        .unwrap();
    let frame = next(&mut ws).await.expect("a push without polling");
    assert_eq!(frame["type"], "messages");
    assert_eq!(bodies(&frame), ["hello"]);
    assert!(frame["messages"][0]["readAt"].is_null());
    assert_eq!(frame["messages"][0]["to"], "b");
}

#[tokio::test]
async fn an_ack_marks_it_read_and_it_is_not_delivered_again() {
    let s = server().await;
    let mut ws = connect(&s, "b").await;
    s.bus
        .send(&name("a"), &name("b"), "one".into())
        .await
        .unwrap();
    let frame = next(&mut ws).await.unwrap();
    let id: u64 = frame["messages"][0]["id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    assert!(
        s.bus
            .check_inbox(&name("b"), 0)
            .await
            .unwrap()
            .messages
            .is_empty(),
        "leased"
    );

    say(&mut ws, json!({"type": "ack", "upTo": id})).await;
    // The ack is processed in order with the next frame; a send is a barrier.
    say(
        &mut ws,
        json!({"type": "send", "ref": 1, "to": "a", "body": "barrier"}),
    )
    .await;
    assert_eq!(next(&mut ws).await.unwrap()["type"], "receipt");
    let sent = s.bus.history(&name("a"), None, None).await.unwrap();
    assert!(sent.messages[0].read_at.is_some(), "readAt is the ack time");
    ws.close(None).await.unwrap();
    // The server drops the session (returning leases) before its close
    // reply, so the stream ending is the signal that nothing more returns.
    assert!(next(&mut ws).await.is_none(), "the stream ends");
    let page = s.bus.check_inbox(&name("b"), 0).await.unwrap();
    assert!(page.messages.is_empty(), "never redelivered");
}

#[tokio::test]
async fn unacknowledged_mail_comes_back_by_pull_and_by_a_new_stream() {
    let s = server().await;
    let mut ws = connect(&s, "b").await;
    for body in ["m1", "m2", "m3"] {
        s.bus
            .send(&name("a"), &name("b"), body.into())
            .await
            .unwrap();
    }
    let mut got = Vec::new();
    while got.len() < 3 {
        let frame = next(&mut ws).await.unwrap();
        got.extend(bodies(&frame).into_iter().map(String::from));
    }
    drop(ws); // no ack, no close handshake

    // The new stream is pushed the mail as soon as the dead one's leases
    // return; no wait for the server to notice the drop is needed.
    let mut again = connect(&s, "b").await;
    let frame = next(&mut again).await.unwrap();
    assert_eq!(bodies(&frame), ["m1", "m2", "m3"], "order preserved");
    drop(again);
    // A pull parks until the leases return, so it needs no sleep either.
    let page = s.bus.check_inbox(&name("b"), 3_000).await.unwrap();
    let pulled: Vec<&str> = page.messages.iter().map(|m| m.body.as_str()).collect();
    assert_eq!(pulled, ["m1", "m2", "m3"]);
}

#[tokio::test]
async fn pushing_pauses_at_the_window_until_acks_free_it() {
    let s = server().await;
    for i in 0..300 {
        s.bus
            .send(&name("a"), &name("b"), format!("m{i}"))
            .await
            .unwrap();
    }
    let mut ws = connect(&s, "b").await;
    let first = next(&mut ws).await.unwrap();
    assert_eq!(first["messages"].as_array().unwrap().len(), 256);
    assert!(
        tokio::time::timeout(Duration::from_millis(300), ws.next())
            .await
            .is_err(),
        "nothing more while 256 are unacknowledged"
    );
    let last = first["messages"][255]["id"]
        .as_str()
        .unwrap()
        .parse::<u64>()
        .unwrap();
    say(&mut ws, json!({"type": "ack", "upTo": last})).await;
    let rest = next(&mut ws).await.unwrap();
    assert_eq!(rest["messages"].as_array().unwrap().len(), 44);
}

#[tokio::test]
async fn a_newer_stream_takes_over_and_the_old_one_is_closed_with_a_reason() {
    let s = server().await;
    let mut old = connect(&s, "b").await;
    s.bus
        .send(&name("a"), &name("b"), "x".into())
        .await
        .unwrap();
    assert_eq!(bodies(&next(&mut old).await.unwrap()), ["x"]);

    let mut new = connect(&s, "b").await;
    let closed = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            match old.next().await {
                Some(Ok(Message::Close(frame))) => return frame,
                Some(Ok(_)) => {}
                _ => return None,
            }
        }
    })
    .await
    .expect("the old stream is closed");
    let reason = closed.expect("a close frame").reason.to_string();
    assert!(reason.contains("newer stream"), "{reason}");
    assert_eq!(
        bodies(&next(&mut new).await.unwrap()),
        ["x"],
        "its leases moved over"
    );
}

#[tokio::test]
async fn a_foreign_origin_is_refused_before_any_upgrade() {
    let s = server().await;
    let mut request = format!("ws://{}/ws?agent=b", s.addr)
        .into_client_request()
        .unwrap();
    request
        .headers_mut()
        .insert("origin", "https://evil.example".parse().unwrap());
    match connect_async(request).await {
        Err(tokio_tungstenite::tungstenite::Error::Http(resp)) => assert_eq!(resp.status(), 403),
        other => panic!("expected a 403, got {:?}", other.map(|_| ())),
    }
    // The same request from a loopback origin is fine.
    let mut ok = format!("ws://{}/ws?agent=b", s.addr)
        .into_client_request()
        .unwrap();
    ok.headers_mut()
        .insert("origin", format!("http://{}", s.addr).parse().unwrap());
    assert!(connect_async(ok).await.is_ok());
}

#[tokio::test]
async fn bad_agents_get_the_contract_error_before_the_upgrade() {
    let s = server().await;
    for (agent, status, code) in [
        ("ghost", 404, "unregistered"),
        ("-bad", 400, "invalid_name"),
        ("", 400, "invalid_name"),
    ] {
        let url = format!("ws://{}/ws?agent={agent}", s.addr);
        match connect_async(url).await {
            Err(tokio_tungstenite::tungstenite::Error::Http(resp)) => {
                assert_eq!(resp.status(), status, "{agent}");
                let body: Value = serde_json::from_slice(resp.body().as_deref().unwrap()).unwrap();
                assert_eq!(body["error"], code, "{agent}");
            }
            other => panic!("{agent}: {:?}", other.map(|_| ())),
        }
    }
}

#[tokio::test]
async fn room_mail_fans_out_over_streams_and_a_full_room_reports_skipped() {
    let s = server_with(Some(2)).await;
    let lobby = RoomName::parse("lobby").unwrap();
    s.bus
        .create_room(&name("a"), &lobby, false, None)
        .await
        .unwrap();
    s.bus.join_room(&name("b"), &lobby).await.unwrap();
    s.bus.join_room(&name("c"), &lobby).await.unwrap();
    let mut b = connect(&s, "b").await;
    let mut c = connect(&s, "c").await;
    s.bus
        .send_room(&name("a"), &lobby, "hi".into())
        .await
        .unwrap();
    for ws in [&mut b, &mut c] {
        let frame = next(ws).await.unwrap();
        assert_eq!(bodies(&frame), ["hi"]);
        assert_eq!(frame["messages"][0]["room"], "lobby");
        assert!(frame["messages"][0].get("to").is_none());
    }
    // A close handshake ends the stream (and returns its leases) before the
    // server replies, so both are settled once these return.
    for ws in [&mut b, &mut c] {
        ws.close(None).await.unwrap();
        assert!(next(ws).await.is_none(), "the stream ends");
    }
    // Four messages into a room of two: a member that connects late lost two.
    for i in 0..4 {
        s.bus
            .send_room(&name("a"), &lobby, format!("r{i}"))
            .await
            .unwrap();
    }
    let mut late = connect(&s, "b").await;
    let skipped = next(&mut late).await.unwrap();
    assert_eq!(
        skipped,
        json!({"type": "skipped", "count": 3}),
        "hi, r0 and r1 were pushed out"
    );
    assert_eq!(bodies(&next(&mut late).await.unwrap()), ["r2", "r3"]);
}

#[tokio::test]
async fn send_and_batch_frames_reply_with_their_ref() {
    let s = server().await;
    let mut ws = connect(&s, "a").await;
    say(
        &mut ws,
        json!({"type": "send", "ref": "r-1", "to": "b", "body": "hello"}),
    )
    .await;
    let receipt = next(&mut ws).await.unwrap();
    assert_eq!(
        (receipt["type"].as_str(), &receipt["ref"]),
        (Some("receipt"), &json!("r-1"))
    );
    assert_eq!(receipt["from"], "a");
    assert_eq!(receipt["to"], "b");
    assert!(receipt["id"].is_string());

    // The stream's identity is fixed: a `from` in the frame is ignored.
    say(
        &mut ws,
        json!({"type": "send", "ref": 2, "from": "c", "to": "b", "body": "x"}),
    )
    .await;
    assert_eq!(next(&mut ws).await.unwrap()["from"], "a");

    say(
        &mut ws,
        json!({"type": "send", "ref": 3, "to": "ghost", "body": "x"}),
    )
    .await;
    let error = next(&mut ws).await.unwrap();
    assert_eq!(
        (error["type"].as_str(), error["error"].as_str()),
        (Some("error"), Some("unregistered"))
    );
    assert_eq!(error["ref"], 3);

    say(
        &mut ws,
        json!({"type": "batch", "ref": 4, "items": [
            {"to": "b", "body": "one"}, {"to": "ghost", "body": "two"}, {"to": "b", "body": " "}, 5
        ]}),
    )
    .await;
    let batch = next(&mut ws).await.unwrap();
    assert_eq!(
        (batch["type"].as_str(), &batch["ref"]),
        (Some("batch"), &json!(4))
    );
    let results = batch["results"].as_array().unwrap();
    assert!(results[0]["id"].is_string());
    assert_eq!(results[1]["error"], "unregistered");
    assert_eq!(results[2]["error"], "invalid_body");
    assert_eq!(results[3]["error"], "invalid_body");

    let big: Vec<Value> = (0..257).map(|_| json!({"to": "b", "body": "x"})).collect();
    say(&mut ws, json!({"type": "batch", "ref": 5, "items": big})).await;
    let too_many = next(&mut ws).await.unwrap();
    assert_eq!(
        (too_many["error"].as_str(), &too_many["ref"]),
        (Some("invalid_body"), &json!(5))
    );
}

#[tokio::test]
async fn malformed_and_unknown_frames_get_an_error_and_the_stream_stays_open() {
    let s = server().await;
    let mut ws = connect(&s, "a").await;
    ws.send(Message::Text("not json".into())).await.unwrap();
    assert_eq!(next(&mut ws).await.unwrap()["error"], "invalid_body");
    say(&mut ws, json!({"type": "dance", "ref": 9})).await;
    let e = next(&mut ws).await.unwrap();
    assert_eq!(
        (e["error"].as_str(), &e["ref"]),
        (Some("invalid_body"), &json!(9))
    );
    say(&mut ws, json!({"type": "ack", "upTo": "x", "ref": 10})).await;
    let e = next(&mut ws).await.unwrap();
    assert_eq!(
        (e["error"].as_str(), &e["ref"]),
        (Some("invalid_cursor"), &json!(10))
    );
    ws.send(Message::Binary(vec![1, 2, 3].into()))
        .await
        .unwrap();
    assert_eq!(next(&mut ws).await.unwrap()["type"], "error");
    // Still open and working.
    say(
        &mut ws,
        json!({"type": "send", "ref": 11, "to": "b", "body": "still here"}),
    )
    .await;
    assert_eq!(next(&mut ws).await.unwrap()["type"], "receipt");
}

#[tokio::test]
async fn shutdown_closes_every_stream_and_a_stream_keeps_its_agent_alive() {
    let s = server().await;
    let mut ws = connect(&s, "b").await;
    s.bus
        .send(&name("a"), &name("b"), "x".into())
        .await
        .unwrap();
    next(&mut ws).await.unwrap();
    s.bus.shutdown().await.unwrap();
    let mut closed = false;
    let ended = tokio::time::timeout(Duration::from_secs(3), async {
        while let Some(frame) = ws.next().await {
            if let Ok(Message::Close(Some(f))) = frame {
                closed = f.reason.contains("shutting down");
            }
        }
    })
    .await;
    assert!(ended.is_ok(), "the connection ends");
    assert!(closed, "with a close frame that says why");
}

/// `parse_frame` turns each client frame shape into its variant, keeping the
/// `ref`, and a numeric or digit-string `upTo` both read as an id.
#[test]
fn parse_frame_reads_each_client_frame() {
    use mentischat::ws::{parse_frame, ClientFrame};

    assert_eq!(
        parse_frame(r#"{"type":"ack","upTo":"7","ref":1}"#),
        Ok(ClientFrame::Ack {
            up_to: 7,
            reference: Some(json!(1))
        })
    );
    assert!(matches!(
        parse_frame(r#"{"type":"send","to":"b","body":"x"}"#),
        Ok(ClientFrame::Send {
            reference: None,
            ..
        })
    ));
    assert!(matches!(
        parse_frame(r#"{"type":"batch","items":[{}, 3]}"#),
        Ok(ClientFrame::Batch { items, .. }) if items.len() == 2
    ));
}

/// Refusals are typed and carry the `ref` when the frame was an object.
#[test]
fn parse_frame_refuses_with_a_typed_error_and_the_ref() {
    use mentischat::errors::MailboxError;
    use mentischat::ws::parse_frame;

    let err = |text| parse_frame(text).unwrap_err();
    assert_eq!(err("not json").error, MailboxError::InvalidBody);
    assert_eq!(err("[1]").reference, None);
    assert_eq!(
        err(r#"{"type":"nope","ref":"r"}"#).reference,
        Some(json!("r"))
    );
    assert_eq!(
        err(r#"{"type":"ack","upTo":-1,"ref":2}"#).error,
        MailboxError::InvalidCursor
    );
    assert_eq!(
        err(r#"{"type":"batch","items":"x"}"#).error,
        MailboxError::InvalidBody
    );
}
