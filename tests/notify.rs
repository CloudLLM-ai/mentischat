//! `GET /notify` over a real socket: it wakes without taking anything, says
//! the same thing once for a burst, never carries a body, tells a re-armed
//! watch about mail it missed, and leaves `check_inbox`, `/ws` streams and
//! the idle sweep to work as before.

mod common;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use common::{bus_with_clock, idle_after, name};
use futures_util::{SinkExt, StreamExt};
use http_body_util::BodyExt;
use mentischat::bus::{Bus, MAX_NOTIFY_PER_AGENT};
use mentischat::names::RoomName;
use mentischat::notify::{COOLDOWN, SETTLE};
use serde_json::{json, Value};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};
use tower::ServiceExt;

type Client = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

struct Server {
    bus: Arc<Bus>,
    addr: SocketAddr,
    _dir: tempfile::TempDir,
}

async fn serve(bus: Arc<Bus>, dir: tempfile::TempDir) -> Server {
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
    let dir = tempfile::tempdir().unwrap();
    let bus = Arc::new(Bus::new(&dir.path().join("notify")).unwrap());
    serve(bus, dir).await
}

async fn watch(s: &Server, agent: &str) -> Client {
    let url = format!("ws://{}/notify?agent={agent}", s.addr);
    connect_async(url).await.expect("upgrade").0
}

async fn stream(s: &Server, agent: &str) -> Client {
    let url = format!("ws://{}/ws?agent={agent}", s.addr);
    connect_async(url).await.expect("upgrade").0
}

/// The next text frame as JSON within `within`, skipping pings; `None` when
/// nothing arrived (or the socket closed).
async fn frame_within(ws: &mut Client, within: Duration) -> Option<Value> {
    let deadline = tokio::time::Instant::now() + within;
    loop {
        let left = deadline.saturating_duration_since(tokio::time::Instant::now());
        let item = tokio::time::timeout(left, ws.next()).await.ok()??;
        match item.ok()? {
            Message::Text(t) => return Some(serde_json::from_str(t.as_str()).unwrap()),
            Message::Close(_) => return None,
            _ => {}
        }
    }
}

/// A frame that must arrive soon (settle window plus slack).
async fn next(ws: &mut Client) -> Value {
    frame_within(ws, Duration::from_secs(3))
        .await
        .expect("a notify frame")
}

/// Assert nothing but control frames arrives for `quiet`.
async fn silent_for(ws: &mut Client, quiet: Duration) {
    if let Some(frame) = frame_within(ws, quiet).await {
        panic!("unexpected frame {frame}");
    }
}

async fn tell(s: &Server, from: &str, to: &str, body: &str) {
    s.bus
        .send(&name(from), &name(to), body.into())
        .await
        .unwrap();
}

/// The route's one frame shape, for mail from `a` to `b`.
#[tokio::test]
async fn test_a_send_wakes_the_socket_with_a_metadata_only_frame() {
    let s = server().await;
    let mut ws = watch(&s, "b").await;
    tell(&s, "a", "b", "the launch codes are 1234").await;
    let frame = next(&mut ws).await;
    assert_eq!(frame["type"], "mail");
    assert_eq!(frame["unread"], 1);
    assert_eq!(frame["from"], json!(["a"]));
    assert_eq!(frame["rooms"], json!([]));
    assert!(frame["newest"].as_str().unwrap().parse::<u64>().is_ok());
    let text = frame.to_string();
    assert!(!text.contains("launch codes"), "no body on this channel");
    assert_eq!(frame.as_object().unwrap().len(), 5, "no other fields");
}

/// The reason the route exists: while a socket is open `check_inbox` still
/// gets the mail (a `/ws` stream would have leased it), exactly once.
#[tokio::test]
async fn test_notify_never_leases_check_inbox_still_gets_the_mail() {
    let s = server().await;
    let mut ws = watch(&s, "b").await;
    tell(&s, "a", "b", "hello").await;
    next(&mut ws).await;
    let page = s.bus.check_inbox(&name("b"), 0).await.unwrap();
    assert_eq!(page.messages.len(), 1);
    assert_eq!(page.messages[0].body, "hello");
    assert!(s
        .bus
        .check_inbox(&name("b"), 0)
        .await
        .unwrap()
        .messages
        .is_empty());
}

/// A burst is one frame (coalescing), counting every message, and a second
/// frame needs new mail.
#[tokio::test]
async fn test_a_burst_is_coalesced_into_one_frame() {
    let s = server().await;
    let mut ws = watch(&s, "b").await;
    for i in 0..5 {
        tell(&s, "a", "b", &format!("m{i}")).await;
    }
    let frame = next(&mut ws).await;
    assert_eq!(frame["unread"], 5);
    silent_for(&mut ws, SETTLE * 3).await;
}

/// Mail that lands within the cooldown of the previous frame waits for it,
/// then arrives as one frame covering all of it; nothing is lost.
#[tokio::test]
async fn test_cooldown_spaces_frames_and_the_next_one_covers_everything() {
    let s = server().await;
    let mut ws = watch(&s, "b").await;
    tell(&s, "a", "b", "one").await;
    let first = next(&mut ws).await;
    let started = tokio::time::Instant::now();
    tell(&s, "a", "b", "two").await;
    tell(&s, "c", "b", "three").await;
    let second = frame_within(&mut ws, COOLDOWN + Duration::from_secs(3))
        .await
        .expect("the second frame");
    assert!(started.elapsed() + SETTLE >= COOLDOWN - Duration::from_millis(500));
    assert_eq!(first["unread"], 1);
    assert_eq!(second["unread"], 3);
    assert_eq!(second["from"], json!(["a", "c"]));
}

/// A watch that was re-armed after mail landed hears about it at once.
#[tokio::test]
async fn test_pending_mail_is_announced_on_connect() {
    let s = server().await;
    tell(&s, "a", "b", "while you were away").await;
    tell(&s, "c", "b", "me too").await;
    let mut ws = watch(&s, "b").await;
    let frame = frame_within(&mut ws, Duration::from_millis(150))
        .await
        .expect("immediately, not after the settle window");
    assert_eq!(frame["unread"], 2);
    assert_eq!(frame["from"], json!(["a", "c"]));
}

/// With nothing waiting, connecting says nothing: no text heartbeat.
#[tokio::test]
async fn test_an_empty_inbox_stays_silent() {
    let s = server().await;
    let mut ws = watch(&s, "b").await;
    silent_for(&mut ws, Duration::from_millis(600)).await;
}

/// Mail read before the settle window ends is not announced: the frame would
/// wake the agent for nothing.
#[tokio::test]
async fn test_mail_read_in_the_meantime_gets_no_frame() {
    let s = server().await;
    let mut ws = watch(&s, "b").await;
    tell(&s, "a", "b", "quick").await;
    assert_eq!(
        s.bus
            .check_inbox(&name("b"), 0)
            .await
            .unwrap()
            .messages
            .len(),
        1
    );
    silent_for(&mut ws, SETTLE * 4).await;
    // And the socket still works for the next message.
    tell(&s, "a", "b", "later").await;
    assert_eq!(next(&mut ws).await["unread"], 1);
}

/// Every socket of the agent hears the mail.
#[tokio::test]
async fn test_several_sockets_for_one_agent_all_hear_it() {
    let s = server().await;
    let mut one = watch(&s, "b").await;
    let mut two = watch(&s, "b").await;
    tell(&s, "a", "b", "x").await;
    assert_eq!(next(&mut one).await["unread"], 1);
    assert_eq!(next(&mut two).await["unread"], 1);
}

/// The fifth socket for a name is refused before the upgrade, with the
/// contract error; closing one frees a slot.
#[tokio::test]
async fn test_the_socket_cap_is_a_contract_error_before_the_upgrade() {
    let s = server().await;
    let mut open = Vec::new();
    for _ in 0..MAX_NOTIFY_PER_AGENT {
        open.push(watch(&s, "b").await);
    }
    let url = format!("ws://{}/notify?agent=b", s.addr);
    match connect_async(url.clone()).await {
        Err(tokio_tungstenite::tungstenite::Error::Http(resp)) => {
            assert_eq!(resp.status(), 409);
            let body: Value = serde_json::from_slice(resp.body().as_deref().unwrap()).unwrap();
            assert_eq!(body["error"], "too_many_waiters");
        }
        other => panic!("expected a refusal, got {:?}", other.map(|_| ())),
    }
    drop(open.pop());
    let mut ok = None;
    for _ in 0..50 {
        if let Ok((ws, _)) = connect_async(url.clone()).await {
            ok = Some(ws);
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(ok.is_some(), "a closed socket frees its slot");
}

/// Room mail notifies the members that watch, with the room named; the
/// sender's own socket stays silent.
#[tokio::test]
async fn test_room_mail_notifies_members_but_not_the_sender() {
    let s = server().await;
    let ops = RoomName::parse("ops").unwrap();
    s.bus
        .create_room(&name("a"), &ops, false, None)
        .await
        .unwrap();
    s.bus.join_room(&name("b"), &ops).await.unwrap();
    s.bus.join_room(&name("c"), &ops).await.unwrap();
    let (mut a, mut b) = (watch(&s, "a").await, watch(&s, "b").await);
    s.bus
        .send_room(&name("a"), &ops, "team".into())
        .await
        .unwrap();
    let frame = next(&mut b).await;
    assert_eq!(frame["unread"], 1);
    assert_eq!(frame["rooms"], json!(["ops"]));
    assert_eq!(frame["from"], json!(["a"]));
    silent_for(&mut a, SETTLE * 4).await;
}

/// The same mail is never announced twice: reading none of it and getting
/// unrelated activity (a ping interval is too long to wait) changes nothing,
/// but new mail is announced with the whole backlog counted.
#[tokio::test]
async fn test_unread_backlog_is_counted_in_the_next_frame() {
    let s = server().await;
    let mut ws = watch(&s, "b").await;
    tell(&s, "a", "b", "1").await;
    assert_eq!(next(&mut ws).await["unread"], 1);
    tell(&s, "a", "b", "2").await;
    let second = frame_within(&mut ws, COOLDOWN + Duration::from_secs(3))
        .await
        .unwrap();
    assert_eq!(second["unread"], 2);
}

/// What a client says is ignored: no reply, no error, the socket lives.
#[tokio::test]
async fn test_client_frames_are_ignored() {
    let s = server().await;
    let mut ws = watch(&s, "b").await;
    ws.send(Message::Text(r#"{"type":"ack","upTo":9}"#.into()))
        .await
        .unwrap();
    ws.send(Message::Text("garbage".into())).await.unwrap();
    ws.send(Message::Binary(vec![1, 2, 3].into()))
        .await
        .unwrap();
    silent_for(&mut ws, Duration::from_millis(400)).await;
    tell(&s, "a", "b", "still here").await;
    assert_eq!(next(&mut ws).await["unread"], 1);
}

/// A frame bigger than the bound ends the socket instead of being buffered.
#[tokio::test]
async fn test_an_oversized_client_frame_ends_the_socket() {
    let s = server().await;
    let mut ws = watch(&s, "b").await;
    let big = "x".repeat(mentischat::notify::MAX_CLIENT_FRAME_BYTES + 1);
    let _ = ws.send(Message::Text(big.into())).await;
    let ended = tokio::time::timeout(Duration::from_secs(3), async {
        while let Some(item) = ws.next().await {
            if item.is_err() || matches!(item, Ok(Message::Close(_))) {
                break;
            }
        }
    })
    .await;
    assert!(ended.is_ok());
}

/// Invalid, empty and unregistered names get the contract error before the
/// upgrade, as on `/ws`.
#[tokio::test]
async fn test_bad_agents_get_the_contract_error_before_the_upgrade() {
    let s = server().await;
    for (agent, status, code) in [
        ("ghost", 404, "unregistered"),
        ("-bad", 400, "invalid_name"),
        ("", 400, "invalid_name"),
    ] {
        let url = format!("ws://{}/notify?agent={agent}", s.addr);
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

/// Same Origin and Host guards as every route; a refused handshake never
/// counts as an open socket.
#[tokio::test]
async fn test_foreign_origin_and_host_are_refused() {
    let s = server().await;
    for (header, value) in [
        ("origin", "https://evil.example"),
        ("host", "evil.example"),
        ("sec-fetch-site", "cross-site"),
    ] {
        let mut request = format!("ws://{}/notify?agent=b", s.addr)
            .into_client_request()
            .unwrap();
        request.headers_mut().insert(header, value.parse().unwrap());
        match connect_async(request).await {
            Err(tokio_tungstenite::tungstenite::Error::Http(resp)) => {
                assert_eq!(resp.status(), 403, "{header}")
            }
            other => panic!("{header}: expected a 403, got {:?}", other.map(|_| ())),
        }
    }
    let r = s
        .bus
        .send(&name("a"), &name("b"), "x".into())
        .await
        .unwrap();
    assert_eq!(
        r.notified,
        Some(false),
        "refused handshakes attend to nothing"
    );
}

/// A notify socket and a `/ws` stream for one agent coexist: the stream
/// keeps its leases and is not taken over, the notify socket is not closed,
/// and the receipt says notified.
#[tokio::test]
async fn test_a_notify_socket_and_a_stream_coexist() {
    let s = server().await;
    let mut notify = watch(&s, "b").await;
    let mut ws = stream(&s, "b").await;
    let r = s
        .bus
        .send(&name("a"), &name("b"), "both".into())
        .await
        .unwrap();
    assert_eq!(r.notified, Some(true));
    let pushed = frame_within(&mut ws, Duration::from_secs(3)).await.unwrap();
    assert_eq!(pushed["type"], "messages");
    assert_eq!(pushed["messages"][0]["body"], "both");
    // A second stream takes over the first, as before; the notify socket is
    // neither the taker nor the taken.
    let _second = stream(&s, "b").await;
    let closed = frame_within(&mut ws, Duration::from_secs(3)).await;
    assert!(closed.is_none(), "the first stream was replaced");
    // The leases go back when the streams end, with the notify socket
    // still open and counted.
    drop(_second);
    let mut returned = Vec::new();
    for _ in 0..50 {
        returned = s.bus.check_inbox(&name("b"), 0).await.unwrap().messages;
        if !returned.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(returned.len(), 1);
    assert_eq!(returned[0].body, "both");
    let r = s
        .bus
        .send(&name("a"), &name("b"), "x".into())
        .await
        .unwrap();
    assert_eq!(r.notified, Some(true), "the notify socket is still open");
    assert!(frame_within(&mut notify, Duration::from_secs(3))
        .await
        .is_some());
}

/// When the agent leaves the roster the socket is closed with a reason.
#[tokio::test]
async fn test_the_socket_closes_when_its_agent_leaves() {
    let s = server().await;
    let mut ws = watch(&s, "b").await;
    s.bus.deregister(&name("b")).await.unwrap();
    let mut reason = None;
    let ended = tokio::time::timeout(Duration::from_secs(3), async {
        while let Some(item) = ws.next().await {
            if let Ok(Message::Close(Some(f))) = item {
                reason = Some((u16::from(f.code), f.reason.to_string()));
            }
        }
    })
    .await;
    assert!(ended.is_ok());
    assert_eq!(reason, Some((4001, "agent left the roster".into())));
}

/// Shutdown closes every notify socket with a reason.
#[tokio::test]
async fn test_shutdown_closes_notify_sockets() {
    let s = server().await;
    let mut ws = watch(&s, "b").await;
    s.bus.shutdown().await.unwrap();
    let mut said = false;
    let ended = tokio::time::timeout(Duration::from_secs(3), async {
        while let Some(item) = ws.next().await {
            if let Ok(Message::Close(Some(f))) = item {
                said = f.reason.contains("shutting down");
            }
        }
    })
    .await;
    assert!(ended.is_ok());
    assert!(said);
}

/// The receipt of every way to send says whether the recipient was
/// notified: REST, the `/ws` send frame, and room mail by count.
#[tokio::test]
async fn test_receipts_over_rest_and_ws_report_notified() {
    let s = server().await;
    let app = mentischat::rest::router(s.bus.clone());
    let post = |body: &str| {
        http::Request::builder()
            .method("POST")
            .uri("/messages")
            .header("content-type", "application/json")
            .body(axum::body::Body::from(body.to_string()))
            .unwrap()
    };
    let call = |req: http::Request<axum::body::Body>| {
        let app = app.clone();
        async move {
            let resp = app.oneshot(req).await.unwrap();
            let bytes = BodyExt::collect(resp.into_body()).await.unwrap().to_bytes();
            serde_json::from_slice::<Value>(&bytes).unwrap()
        }
    };
    let quiet = call(post(r#"{"from":"a","to":"b","body":"x"}"#)).await;
    assert_eq!(quiet["notified"], false);
    let _watching = watch(&s, "b").await;
    let loud = call(post(r#"{"from":"a","to":"b","body":"y"}"#)).await;
    assert_eq!(loud["notified"], true);
    assert_eq!(loud["from"], "a");

    // The /ws send receipt carries it too.
    let mut sender = stream(&s, "a").await;
    sender
        .send(Message::Text(
            json!({"type":"send","ref":1,"to":"b","body":"z"})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    let receipt = loop {
        let frame = frame_within(&mut sender, Duration::from_secs(3))
            .await
            .unwrap();
        if frame["type"] == "receipt" {
            break frame;
        }
    };
    assert_eq!(receipt["notified"], true);
}

/// An open socket keeps its agent on the roster through the idle sweep.
#[tokio::test]
async fn test_an_open_notify_socket_keeps_its_agent_through_the_idle_sweep() {
    let (dir, bus, clock) = bus_with_clock("notify-sweep", idle_after(60));
    let s = serve(bus, dir).await;
    let _ws = watch(&s, "b").await;
    clock.advance(Duration::from_secs(7_200));
    let dropped = s.bus.sweep().await.unwrap().dropped_agents;
    assert!(dropped.contains(&"a".to_string()));
    assert!(!dropped.contains(&"b".to_string()), "{dropped:?}");
}
