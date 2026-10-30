//! `GET /ws?agent=NAME`: a WebSocket stream for one agent. Mail is pushed as
//! it lands, and the agent sends over the same socket. It is for scripts,
//! bridges and custom runtimes; harness agents keep pulling with
//! `check_inbox`.
//!
//! The route sits behind the same loopback and `Origin` checks as every
//! other route, so a web page on another site cannot open a stream (browsers
//! always send `Origin` on a WebSocket handshake). An unregistered agent gets
//! the contract error (status and JSON) before the upgrade.
//!
//! # Frames
//!
//! Every frame is a JSON text frame with a `type`.
//!
//! Server to client:
//! - `{"type":"messages","messages":[...]}`: pushed mail, direct and room,
//!   oldest first by id, in the shape `check_inbox` returns. Pushed mail is
//!   leased, not read, until acknowledged (see [`crate::bus`] streams).
//! - `{"type":"skipped","count":N}`: room messages pushed out of a full room
//!   before they could be delivered.
//! - `{"type":"receipt","ref":..,...}` and `{"type":"error","ref":..,"error":..}`:
//!   the reply to a `send`, with the `send_message` receipt or the contract
//!   error. `{"type":"batch","ref":..,"results":[...]}`: the replies to a
//!   `batch`, one per item in order.
//!
//! Client to server:
//! - `{"type":"ack","upTo":ID}`: everything pushed with an id up to `ID` is
//!   now read. Cumulative. At most 256 messages / 1 MiB are in flight; pushing
//!   pauses until acks free the window.
//! - `{"type":"send","ref":..,"to":NAME|"room":NAME,"body":TEXT}`: like
//!   `POST /messages`, from this stream's agent (a `from` is ignored).
//! - `{"type":"batch","ref":..,"items":[{send fields}, ...]}`: up to 256
//!   sends, applied like `POST /messages/batch`.
//!
//! A malformed or unknown frame gets an `error` frame (with its `ref` if it
//! had one) and the connection stays open.
//!
//! # Lifetime
//!
//! One stream per agent: a new connection for the same name takes over and the
//! old one is closed with a reason. The server pings every 20 s and closes a
//! connection it has heard nothing from for 60 s. However a stream ends, what
//! it had not acknowledged returns to the inbox in order.

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::ws::{CloseFrame, Message as Frame, WebSocket, WebSocketUpgrade};
use axum::extract::{Query, State};
use axum::response::{IntoResponse, Response};
use serde_json::{json, Map, Value};
use tokio::time::Instant;

use crate::bus::{Bus, Push, StreamSession};
use crate::errors::MailboxError;
use crate::names::AgentName;
use crate::rest::MAX_BATCH_BODY_BYTES;

/// How often the server pings.
const PING_EVERY: Duration = Duration::from_secs(20);
/// How long a connection may be silent before it is closed.
const SILENCE_LIMIT: Duration = Duration::from_secs(60);
/// How long one frame may take to send before the connection is given up on.
const SEND_LIMIT: Duration = Duration::from_secs(30);

/// Upgrade to a stream for `?agent=`.
pub async fn upgrade(
    State(bus): State<Arc<Bus>>,
    query: Result<Query<HashMap<String, String>>, axum::extract::rejection::QueryRejection>,
    ws: WebSocketUpgrade,
) -> Response {
    let Ok(Query(q)) = query else {
        return MailboxError::InvalidBody.into_response();
    };
    let agent = match AgentName::parse(q.get("agent").map_or("", String::as_str)) {
        Ok(a) => a,
        Err(e) => return e.into_response(),
    };
    if let Err(e) = bus.check_registered(&agent) {
        return e.into_response();
    }
    ws.max_message_size(MAX_BATCH_BODY_BYTES)
        .on_upgrade(move |socket| run(bus, agent, socket))
        .into_response()
}

/// Why a stream's loop ended.
enum Ending {
    /// The peer is gone or stopped answering: nothing to say.
    Dropped,
    /// Close with this code and reason.
    Close(u16, &'static str),
}

async fn run(bus: Arc<Bus>, agent: AgentName, mut socket: WebSocket) {
    let session = match bus.open_stream(&agent) {
        Ok(session) => session,
        Err(e) => {
            let _ = send_json(&mut socket, error_frame(&e, None)).await;
            return close(socket, Ending::Close(1011, "stream unavailable")).await;
        }
    };
    let ending = serve(&bus, &session, &mut socket).await;
    // Leases return when the session drops, before the close handshake.
    drop(session);
    close(socket, ending).await;
}

async fn close(mut socket: WebSocket, ending: Ending) {
    if let Ending::Close(code, reason) = ending {
        let frame = CloseFrame {
            code,
            reason: reason.into(),
        };
        let _ = tokio::time::timeout(
            Duration::from_secs(5),
            socket.send(Frame::Close(Some(frame))),
        )
        .await;
    }
}

/// Push mail and handle frames until something ends the stream.
async fn serve(bus: &Bus, session: &StreamSession, socket: &mut WebSocket) -> Ending {
    let mut ping = tokio::time::interval(PING_EVERY);
    ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ping.tick().await;
    let mut last_heard = Instant::now();
    loop {
        // Both are enabled before the inbox is looked at: a send, or a newer
        // stream, that lands in between is not missed.
        let wake = session.wake();
        tokio::pin!(wake);
        wake.as_mut().enable();
        let gone = session.takeover();
        tokio::pin!(gone);
        gone.as_mut().enable();
        if let Some(ending) = stop_reason(bus, session) {
            return ending;
        }
        match push_mail(bus, session, socket).await {
            Ok(true) => continue,
            Ok(false) => {}
            Err(ending) => return ending,
        }
        let ended = tokio::select! {
            () = &mut wake => None,
            () = &mut gone => None,
            _ = ping.tick() => on_ping(socket, last_heard).await,
            frame = socket.recv() => match frame {
                None | Some(Err(_)) => Some(Ending::Dropped),
                Some(Ok(frame)) => {
                    last_heard = Instant::now();
                    on_frame(bus, session, socket, frame).await
                }
            },
        };
        if let Some(ending) = ended {
            return ending;
        }
    }
}

/// Why the stream must end now, if it must: the server is shutting down or a
/// newer stream took this agent over.
fn stop_reason(bus: &Bus, session: &StreamSession) -> Option<Ending> {
    if bus.is_closed() {
        return Some(Ending::Close(1001, "server shutting down"));
    }
    session.taken_over().then_some(Ending::Close(
        4000,
        "replaced by a newer stream for this agent",
    ))
}

/// Send the next push, if one is ready: `Ok(true)` when it was sent (look
/// again), `Ok(false)` when there was nothing to push.
async fn push_mail(
    bus: &Bus,
    session: &StreamSession,
    socket: &mut WebSocket,
) -> Result<bool, Ending> {
    match bus.stream_push(session) {
        Ok(Some(push)) => send_push(socket, push)
            .await
            .map(|()| true)
            .map_err(|()| Ending::Dropped),
        Ok(None) => Ok(false),
        Err(MailboxError::ShuttingDown) => Err(Ending::Close(1001, "server shutting down")),
        Err(e) => {
            let _ = send_json(socket, error_frame(&e, None)).await;
            Err(Ending::Close(1011, "stream ended"))
        }
    }
}

/// A ping tick: close a peer silent past [`SILENCE_LIMIT`], else ping it.
async fn on_ping(socket: &mut WebSocket, last_heard: Instant) -> Option<Ending> {
    if last_heard.elapsed() > SILENCE_LIMIT {
        return Some(Ending::Close(1008, "no answer to pings"));
    }
    send_frame(socket, Frame::Ping(Vec::new().into()))
        .await
        .err()
        .map(|()| Ending::Dropped)
}

/// One received WebSocket message; `Some` when it ends the stream.
async fn on_frame(
    bus: &Bus,
    session: &StreamSession,
    socket: &mut WebSocket,
    frame: Frame,
) -> Option<Ending> {
    let reply = match frame {
        Frame::Text(text) => reply_to(bus, session, text.as_str()).await,
        Frame::Binary(_) => Some(error_frame(&MailboxError::InvalidBody, None)),
        Frame::Close(_) => return Some(Ending::Dropped),
        Frame::Ping(_) | Frame::Pong(_) => None,
    };
    match reply {
        Some(reply) => send_json(socket, reply)
            .await
            .err()
            .map(|()| Ending::Dropped),
        None => None,
    }
}

/// Send one frame, giving up on a peer that will not take it.
async fn send_frame(socket: &mut WebSocket, frame: Frame) -> Result<(), ()> {
    match tokio::time::timeout(SEND_LIMIT, socket.send(frame)).await {
        Ok(Ok(())) => Ok(()),
        _ => Err(()),
    }
}

async fn send_json(socket: &mut WebSocket, value: Value) -> Result<(), ()> {
    send_frame(socket, Frame::Text(value.to_string().into())).await
}

/// The frames for one push: `skipped` first, then the messages.
async fn send_push(socket: &mut WebSocket, push: Push) -> Result<(), ()> {
    if push.skipped > 0 {
        send_json(socket, json!({"type": "skipped", "count": push.skipped})).await?;
    }
    if !push.messages.is_empty() {
        let messages = serde_json::to_value(&push.messages).map_err(|_| ())?;
        send_json(socket, json!({"type": "messages", "messages": messages})).await?;
    }
    Ok(())
}

/// An `error` frame for `e`, carrying the request's `ref` if it had one.
fn error_frame(e: &MailboxError, reference: Option<&Value>) -> Value {
    typed("error", e.to_json(), reference)
}

/// `body` (a JSON object) with `type` and, when there is one, `ref` added.
fn typed(kind: &str, body: Value, reference: Option<&Value>) -> Value {
    let mut object = match body {
        Value::Object(map) => map,
        other => Map::from_iter([("result".to_string(), other)]),
    };
    object.insert("type".into(), json!(kind));
    if let Some(reference) = reference {
        object.insert("ref".into(), reference.clone());
    }
    Value::Object(object)
}

/// A frame a client may send, validated by [`parse_frame`].
#[derive(Debug, Clone, PartialEq)]
pub enum ClientFrame {
    /// `{"type":"ack","upTo":ID}`: everything pushed up to `up_to` is read.
    Ack {
        /// The highest message id acknowledged.
        up_to: u64,
        /// The frame's `ref`, echoed on an error.
        reference: Option<Value>,
    },
    /// `{"type":"send","to"|"room":..,"body":..}`: one send from this
    /// stream's agent.
    Send {
        /// The frame's `ref`, echoed on the reply.
        reference: Option<Value>,
        /// The whole frame object, whose `to`, `room` and `body` are read.
        item: Value,
    },
    /// `{"type":"batch","items":[..]}`: sends applied in order.
    Batch {
        /// The frame's `ref`, echoed on the reply.
        reference: Option<Value>,
        /// The send objects; their count is checked when applied.
        items: Vec<Value>,
    },
}

/// Why a client frame was refused, with its `ref` if it had one.
#[derive(Debug, Clone, PartialEq)]
pub struct FrameError {
    /// The contract error to answer with.
    pub error: MailboxError,
    /// The frame's `ref`, if it was an object that had one.
    pub reference: Option<Value>,
}

/// Parse one client text frame.
///
/// # Errors
///
/// `invalid_body` for text that is not a JSON object, an unknown or missing
/// `type`, or a `batch` without an `items` array; `invalid_cursor` for an
/// `ack` whose `upTo` is not an id (a number, or a string of digits).
pub fn parse_frame(text: &str) -> Result<ClientFrame, FrameError> {
    let Ok(Value::Object(mut frame)) = serde_json::from_str::<Value>(text) else {
        return Err(FrameError {
            error: MailboxError::InvalidBody,
            reference: None,
        });
    };
    let reference = frame.get("ref").cloned();
    let refuse = |error| FrameError {
        error,
        reference: reference.clone(),
    };
    match frame.get("type").and_then(Value::as_str) {
        Some("ack") => frame
            .get("upTo")
            .and_then(crate::tools::uint)
            .map(|up_to| ClientFrame::Ack {
                up_to,
                reference: reference.clone(),
            })
            .ok_or_else(|| refuse(MailboxError::InvalidCursor)),
        Some("send") => Ok(ClientFrame::Send {
            reference: reference.clone(),
            item: Value::Object(frame),
        }),
        Some("batch") => match frame.remove("items") {
            Some(Value::Array(items)) => Ok(ClientFrame::Batch { reference, items }),
            _ => Err(refuse(MailboxError::InvalidBody)),
        },
        _ => Err(refuse(MailboxError::InvalidBody)),
    }
}

/// Handle one client text frame; the reply to send, if any.
async fn reply_to(bus: &Bus, session: &StreamSession, text: &str) -> Option<Value> {
    match parse_frame(text) {
        Err(e) => Some(error_frame(&e.error, e.reference.as_ref())),
        Ok(ClientFrame::Ack { up_to, reference }) => bus
            .stream_ack(session, up_to)
            .err()
            .map(|e| error_frame(&e, reference.as_ref())),
        Ok(ClientFrame::Send { reference, item }) => {
            Some(send_one(bus, session, &item, reference.as_ref()).await)
        }
        Ok(ClientFrame::Batch { reference, items }) => {
            Some(send_batch(bus, session, &items, reference.as_ref()).await)
        }
    }
}

/// The `send_message` parameters for one send from this stream's agent.
fn send_params(session: &StreamSession, item: &Value) -> Value {
    json!({
        "from": session.name(),
        "to": item.get("to"),
        "room": item.get("room"),
        "body": item.get("body"),
    })
}

async fn send_one(
    bus: &Bus,
    session: &StreamSession,
    item: &Value,
    reference: Option<&Value>,
) -> Value {
    match crate::tools::send_message(bus, &send_params(session, item)).await {
        Ok(receipt) => typed("receipt", receipt, reference),
        Err(e) => error_frame(&e, reference),
    }
}

async fn send_batch(
    bus: &Bus,
    session: &StreamSession,
    items: &[Value],
    reference: Option<&Value>,
) -> Value {
    match crate::tools::send_batch(bus, items, |item| Cow::Owned(send_params(session, item))).await
    {
        Ok(results) => typed("batch", json!({ "results": results }), reference),
        Err(e) => error_frame(&e, reference),
    }
}
