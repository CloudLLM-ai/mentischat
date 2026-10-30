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
use crate::rest::{err_response, MAX_BATCH_BODY_BYTES, MAX_BATCH_SEND};

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
        return err_response(MailboxError::InvalidBody);
    };
    let agent = match AgentName::parse(q.get("agent").map_or("", String::as_str)) {
        Ok(a) => a,
        Err(e) => return err_response(e),
    };
    if let Err(e) = bus.check_registered(&agent) {
        return err_response(e);
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
        if bus.is_closed() {
            return Ending::Close(1001, "server shutting down");
        }
        if session.taken_over() {
            return Ending::Close(4000, "replaced by a newer stream for this agent");
        }
        match bus.stream_push(session) {
            Ok(Some(push)) => {
                if send_push(socket, push).await.is_err() {
                    return Ending::Dropped;
                }
                continue;
            }
            Ok(None) => {}
            Err(MailboxError::ShuttingDown) => return Ending::Close(1001, "server shutting down"),
            Err(e) => {
                let _ = send_json(socket, error_frame(&e, None)).await;
                return Ending::Close(1011, "stream ended");
            }
        }
        tokio::select! {
            () = &mut wake => {}
            () = &mut gone => {}
            _ = ping.tick() => {
                if last_heard.elapsed() > SILENCE_LIMIT {
                    return Ending::Close(1008, "no answer to pings");
                }
                if send_frame(socket, Frame::Ping(Vec::new().into())).await.is_err() {
                    return Ending::Dropped;
                }
            }
            frame = socket.recv() => match frame {
                None | Some(Err(_)) => return Ending::Dropped,
                Some(Ok(frame)) => {
                    last_heard = Instant::now();
                    match frame {
                        Frame::Text(text) => {
                            let reply = handle_text(bus, session, text.as_str()).await;
                            if let Some(reply) = reply {
                                if send_json(socket, reply).await.is_err() {
                                    return Ending::Dropped;
                                }
                            }
                        }
                        Frame::Binary(_) => {
                            let reply = error_frame(&MailboxError::InvalidBody, None);
                            if send_json(socket, reply).await.is_err() {
                                return Ending::Dropped;
                            }
                        }
                        Frame::Close(_) => return Ending::Dropped,
                        Frame::Ping(_) | Frame::Pong(_) => {}
                    }
                }
            },
        }
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

/// Handle one client frame; the reply to send, if any.
async fn handle_text(bus: &Bus, session: &StreamSession, text: &str) -> Option<Value> {
    let Ok(Value::Object(frame)) = serde_json::from_str::<Value>(text) else {
        return Some(error_frame(&MailboxError::InvalidBody, None));
    };
    let reference = frame.get("ref");
    match frame.get("type").and_then(Value::as_str) {
        Some("ack") => ack(bus, session, &frame)
            .err()
            .map(|e| error_frame(&e, reference)),
        Some("send") => Some(send_one(bus, session, &frame, reference).await),
        Some("batch") => Some(send_batch(bus, session, &frame, reference).await),
        _ => Some(error_frame(&MailboxError::InvalidBody, reference)),
    }
}

/// Apply an `ack` frame: `upTo` is an id (a number, or a string of digits).
fn ack(bus: &Bus, session: &StreamSession, frame: &Map<String, Value>) -> Result<(), MailboxError> {
    let up_to = match frame.get("upTo") {
        Some(Value::Number(n)) => n.as_u64(),
        Some(Value::String(s)) if s.bytes().all(|b| b.is_ascii_digit()) => s.parse().ok(),
        _ => None,
    };
    let up_to = up_to.ok_or(MailboxError::InvalidCursor)?;
    bus.stream_ack(session, up_to).map(|_| ())
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
    frame: &Map<String, Value>,
    reference: Option<&Value>,
) -> Value {
    let item = Value::Object(frame.clone());
    match crate::tools::send_message(bus, &send_params(session, &item)).await {
        Ok(receipt) => typed("receipt", receipt, reference),
        Err(e) => error_frame(&e, reference),
    }
}

async fn send_batch(
    bus: &Bus,
    session: &StreamSession,
    frame: &Map<String, Value>,
    reference: Option<&Value>,
) -> Value {
    let items = match frame.get("items") {
        Some(Value::Array(items)) if (1..=MAX_BATCH_SEND).contains(&items.len()) => items,
        _ => return error_frame(&MailboxError::InvalidBody, reference),
    };
    let mut results = Vec::with_capacity(items.len());
    for item in items {
        let outcome = if item.is_object() {
            crate::tools::send_message(bus, &send_params(session, item)).await
        } else {
            Err(MailboxError::InvalidBody)
        };
        results.push(outcome.unwrap_or_else(|e| e.to_json()));
    }
    typed("batch", json!({ "results": results }), reference)
}
