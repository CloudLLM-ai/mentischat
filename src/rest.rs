//! REST surface: same behaviors as the MCP tools, on the shared bus.
//!
//! Success is always 200 + JSON (no 201/204). Errors map once via
//! [`MailboxError::http_status`] + [`MailboxError::to_json`]. Inbox pulls
//! mutate state and history pages hold message text, so both answer with
//! `Cache-Control: no-store`.

use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::rejection::{JsonRejection, PathRejection, QueryRejection};
use axum::extract::{DefaultBodyLimit, Path, Query, State};
use axum::http::header;
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use serde_json::{json, Value};

use crate::bus::Bus;
use crate::errors::MailboxError;

type Ctx = Arc<Bus>;

/// Most messages in one `POST /messages/batch`.
pub const MAX_BATCH_SEND: usize = 256;
/// Largest `POST /messages/batch` request body: 16 MiB, room for a full batch
/// of bodies of a few tens of KiB (the 1 MiB limit of every other route would
/// cap a batch of large messages at 16 of them).
pub const MAX_BATCH_BODY_BYTES: usize = 16 * 1024 * 1024;

/// Build the REST router (merged into the app next to `/mcp`).
pub fn router(bus: Ctx) -> Router {
    Router::new()
        .route("/agents", post(register).get(list))
        .route("/agents/{agent}", delete(deregister_one))
        .route("/messages", post(send_msg))
        // A batch may carry many messages, so it alone gets a larger body
        // limit than the 1 MiB the rest of the daemon allows.
        .route(
            "/messages/batch",
            post(send_batch).layer(DefaultBodyLimit::max(MAX_BATCH_BODY_BYTES)),
        )
        // A pull marks mail read, so HEAD (which axum would otherwise send to
        // the GET handler) is refused rather than consuming mail it does not
        // return.
        .route("/inboxes/{agent}", get(pull).head(method_not_allowed))
        .route("/history/{agent}", get(history))
        .route("/rooms", post(create_room).get(list_rooms))
        .route("/rooms/{room}/members", post(join_room))
        .route("/rooms/{room}/members/{agent}", delete(leave_room))
        .route("/rooms/{room}/invitations", post(invite))
        .route("/ws", get(crate::ws::upgrade))
        .route("/health", get(health))
        .fallback(not_found)
        .method_not_allowed_fallback(method_not_allowed)
        .with_state(bus)
}

/// The contract error response: [`MailboxError::http_status`] and the
/// [`MailboxError::to_json`] object. Every REST and WebSocket-upgrade failure
/// goes through here, so handlers can propagate with `?`.
impl IntoResponse for MailboxError {
    fn into_response(self) -> Response {
        (self.http_status(), Json(self.to_json())).into_response()
    }
}

/// What a handler answers: 200 + JSON, or a contract error.
type Reply = Result<Json<Value>, MailboxError>;

/// Serialise a pull result with `Cache-Control: no-store` — it mutates live
/// state and must never be cached.
fn no_store_response(value: impl serde::Serialize) -> Response {
    let mut resp = Json(crate::tools::to_wire(value)).into_response();
    resp.headers_mut().insert(
        header::CACHE_CONTROL,
        header::HeaderValue::from_static("no-store"),
    );
    resp
}

/// Parse a JSON request body; any extractor rejection is a client error.
fn json_body(body: Result<Json<Value>, JsonRejection>) -> Result<Value, MailboxError> {
    body.map(|Json(v)| v).map_err(|_| MailboxError::InvalidBody)
}

/// The `{agent}` path segment, raw: a segment axum cannot decode (invalid
/// percent-encoding or UTF-8) is an `invalid_name` like any other bad name,
/// not axum's plain-text 400. The tools validate the name itself.
fn path_agent(path: Result<Path<String>, PathRejection>) -> Result<String, MailboxError> {
    path.map(|Path(raw)| raw)
        .map_err(|_| MailboxError::InvalidName {
            agent: String::new(),
        })
}

/// The query parameters named `keys` as the JSON object the tools take
/// (absent ones are `null`, which the tools treat as omitted).
fn query_params(q: &HashMap<String, String>, keys: &[&str]) -> Value {
    keys.iter()
        .map(|key| ((*key).to_string(), json!(q.get(*key))))
        .collect::<serde_json::Map<_, _>>()
        .into()
}

/// `POST /agents` `{ "agent", "provider"?, "description"?, "reclaim"? }` —
/// same as register.
async fn register(State(bus): State<Ctx>, body: Result<Json<Value>, JsonRejection>) -> Reply {
    Ok(Json(
        crate::tools::register_agent(&bus, &json_body(body)?).await?,
    ))
}

/// `GET /agents?after=&limit=` — roster sorted by name ascending, one page,
/// plus free names.
async fn list(
    State(bus): State<Ctx>,
    query: Result<Query<HashMap<String, String>>, QueryRejection>,
) -> Reply {
    let Query(q) = query.map_err(|_| MailboxError::InvalidBody)?;
    Ok(Json(
        crate::tools::list_agents(&bus, &query_params(&q, &["after", "limit"])).await?,
    ))
}

/// `DELETE /agents/{agent}` — same as the deregister tool.
async fn deregister_one(
    State(bus): State<Ctx>,
    path: Result<Path<String>, PathRejection>,
) -> Reply {
    let params = json!({ "agent": path_agent(path)? });
    Ok(Json(crate::tools::deregister(&bus, &params).await?))
}

/// `POST /messages` `{ "from", "to", "body" }` — same as send_message.
async fn send_msg(State(bus): State<Ctx>, body: Result<Json<Value>, JsonRejection>) -> Reply {
    Ok(Json(
        crate::tools::send_message(&bus, &json_body(body)?).await?,
    ))
}

/// `POST /messages/batch` `[{ "from", "to" | "room", "body" }, ...]` — up to
/// [`MAX_BATCH_SEND`] sends in one request. Each item is validated and
/// applied exactly like `POST /messages`, in order, and answers with its
/// receipt or its contract error object at the same position of the response
/// array: one failing item does not stop or undo the others. A body that is
/// not an array of 1 to [`MAX_BATCH_SEND`] items is `invalid_body`.
async fn send_batch(State(bus): State<Ctx>, body: Result<Json<Value>, JsonRejection>) -> Reply {
    let items = match json_body(body)? {
        Value::Array(items) if (1..=MAX_BATCH_SEND).contains(&items.len()) => items,
        _ => return Err(MailboxError::InvalidBody),
    };
    let mut results = Vec::with_capacity(items.len());
    for item in &items {
        let outcome = if item.is_object() {
            crate::tools::send_message(&bus, item).await
        } else {
            Err(MailboxError::InvalidBody)
        };
        results.push(outcome.unwrap_or_else(|e| e.to_json()));
    }
    Ok(Json(Value::Array(results)))
}

/// `GET /inboxes/{agent}?wait_ms=&limit=` — same as check_inbox: returned messages
/// are marked read.
async fn pull(
    State(bus): State<Ctx>,
    path: Result<Path<String>, PathRejection>,
    query: Result<Query<HashMap<String, String>>, QueryRejection>,
) -> Result<Response, MailboxError> {
    let agent = path_agent(path)?;
    let Query(q) = query.map_err(|_| MailboxError::InvalidWait)?;
    let mut params = query_params(&q, &["wait_ms", "limit"]);
    params["agent"] = json!(agent);
    Ok(no_store_response(
        crate::tools::check_inbox(&bus, &params).await?,
    ))
}

/// `GET /history/{agent}?with=&room=&before=` — same as the history tool.
async fn history(
    State(bus): State<Ctx>,
    path: Result<Path<String>, PathRejection>,
    query: Result<Query<HashMap<String, String>>, QueryRejection>,
) -> Result<Response, MailboxError> {
    let agent = path_agent(path)?;
    let Query(q) = query.map_err(|_| MailboxError::InvalidCursor)?;
    let mut params = query_params(&q, &["with", "room", "before"]);
    params["agent"] = json!(agent);
    Ok(no_store_response(
        crate::tools::history(&bus, &params).await?,
    ))
}

/// `POST /rooms` `{ "agent", "room", "private"?, "description"? }` — same as
/// create_room.
async fn create_room(State(bus): State<Ctx>, body: Result<Json<Value>, JsonRejection>) -> Reply {
    Ok(Json(
        crate::tools::create_room(&bus, &json_body(body)?).await?,
    ))
}

/// `GET /rooms?agent=&after=&limit=` — same as list_rooms.
async fn list_rooms(
    State(bus): State<Ctx>,
    query: Result<Query<HashMap<String, String>>, QueryRejection>,
) -> Reply {
    let Query(q) = query.map_err(|_| MailboxError::InvalidBody)?;
    let params = query_params(&q, &["agent", "after", "limit"]);
    Ok(Json(crate::tools::list_rooms(&bus, &params).await?))
}

/// The `{room}` path segment, as `path_name` does for agents.
fn path_room(path: Result<Path<String>, PathRejection>) -> Result<String, MailboxError> {
    path.map(|Path(raw)| raw)
        .map_err(|_| MailboxError::InvalidRoom {
            room: String::new(),
        })
}

/// A request body that must be a JSON object, with the path's `room` set in
/// it (the path wins over a `room` in the body).
fn body_with_room(
    body: Result<Json<Value>, JsonRejection>,
    room: String,
) -> Result<Value, MailboxError> {
    let mut v = json_body(body)?;
    let Some(object) = v.as_object_mut() else {
        return Err(MailboxError::InvalidBody);
    };
    object.insert("room".to_string(), Value::String(room));
    Ok(v)
}

/// The JSON body of a `/rooms/{room}/...` request with the path's room set
/// in it.
fn room_request(
    path: Result<Path<String>, PathRejection>,
    body: Result<Json<Value>, JsonRejection>,
) -> Result<Value, MailboxError> {
    let room = path_room(path)?;
    body_with_room(body, room)
}

/// `POST /rooms/{room}/members` `{ "agent" }` — same as join_room.
async fn join_room(
    State(bus): State<Ctx>,
    path: Result<Path<String>, PathRejection>,
    body: Result<Json<Value>, JsonRejection>,
) -> Reply {
    Ok(Json(
        crate::tools::join_room(&bus, &room_request(path, body)?).await?,
    ))
}

/// `POST /rooms/{room}/invitations` `{ "agent", "invitee" }` — same as invite.
async fn invite(
    State(bus): State<Ctx>,
    path: Result<Path<String>, PathRejection>,
    body: Result<Json<Value>, JsonRejection>,
) -> Reply {
    Ok(Json(
        crate::tools::invite(&bus, &room_request(path, body)?).await?,
    ))
}

/// `DELETE /rooms/{room}/members/{agent}` — same as leave_room.
async fn leave_room(
    State(bus): State<Ctx>,
    path: Result<Path<(String, String)>, PathRejection>,
) -> Reply {
    let Path((room, agent)) = path.map_err(|_| MailboxError::InvalidRoom {
        room: String::new(),
    })?;
    Ok(Json(
        crate::tools::leave_room(&bus, &json!({ "agent": agent, "room": room })).await?,
    ))
}

/// `GET /health` — `{ ok, agents, pending, read }`; counts only.
async fn health(State(bus): State<Ctx>) -> Reply {
    let stats = bus.stats().await?;
    Ok(Json(json!({
        "ok": true,
        "agents": stats.agents,
        "pending": stats.unread,
        "read": stats.read,
    })))
}

/// Unknown REST path.
async fn not_found() -> MailboxError {
    MailboxError::NotFound
}

/// Known REST path, method it does not serve.
async fn method_not_allowed() -> MailboxError {
    MailboxError::MethodNotAllowed
}
