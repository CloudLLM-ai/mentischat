//! REST surface: same behaviors as the MCP tools, on the shared bus.
//!
//! Success is always 200 + JSON (no 201/204). Errors map once via
//! [`MailboxError::http_status`] + [`MailboxError::to_json`]. Inbox pulls
//! mutate state and history pages hold message text, so both answer with
//! `Cache-Control: no-store`.

use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::rejection::{JsonRejection, PathRejection, QueryRejection};
use axum::extract::{Path, Query, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use serde_json::{json, Value};

use crate::bus::Bus;
use crate::errors::MailboxError;
use crate::names::AgentName;

type Ctx = Arc<Bus>;

/// Build the REST router (merged into the app next to `/mcp`).
pub fn router(bus: Ctx) -> Router {
    Router::new()
        .route("/agents", post(register).get(list))
        .route("/agents/{agent}", delete(deregister_one))
        .route("/messages", post(send_msg))
        // A pull marks mail read, so HEAD (which axum would otherwise send to
        // the GET handler) is refused rather than consuming mail it does not
        // return.
        .route("/inboxes/{agent}", get(pull).head(method_not_allowed))
        .route("/history/{agent}", get(history))
        .route("/rooms", post(create_room).get(list_rooms))
        .route("/rooms/{room}/members", post(join_room))
        .route("/rooms/{room}/members/{agent}", delete(leave_room))
        .route("/rooms/{room}/invitations", post(invite))
        .route("/health", get(health))
        .fallback(not_found)
        .method_not_allowed_fallback(method_not_allowed)
        .with_state(bus)
}

fn err_response(e: MailboxError) -> Response {
    (e.http_status(), Json(e.to_json())).into_response()
}

fn ok_response(value: Value) -> Response {
    (StatusCode::OK, Json(value)).into_response()
}

fn respond(result: Result<Value, MailboxError>) -> Response {
    result.map_or_else(err_response, ok_response)
}

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
#[allow(clippy::result_large_err)]
fn json_body(body: Result<Json<Value>, JsonRejection>) -> Result<Value, Response> {
    match body {
        Ok(Json(v)) => Ok(v),
        Err(_) => Err(err_response(MailboxError::InvalidBody)),
    }
}

/// The `{agent}` path segment. A segment axum cannot decode (invalid
/// percent-encoding or UTF-8) is an `invalid_name` like any other bad name,
/// not axum's plain-text 400.
#[allow(clippy::result_large_err)]
fn path_name(path: Result<Path<String>, PathRejection>) -> Result<AgentName, Response> {
    let Path(raw) = path.map_err(|_| {
        err_response(MailboxError::InvalidName {
            agent: String::new(),
        })
    })?;
    AgentName::parse(&raw).map_err(err_response)
}

/// `POST /agents` `{ "agent", "provider"?, "description"?, "reclaim"? }` —
/// same as register.
async fn register(State(bus): State<Ctx>, body: Result<Json<Value>, JsonRejection>) -> Response {
    let v = match json_body(body) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    respond(crate::tools::register_agent(&bus, &v).await)
}

/// `GET /agents` — roster sorted by name ascending, plus free names.
async fn list(State(bus): State<Ctx>) -> Response {
    respond(bus.list_agents().await.map(crate::tools::to_wire))
}

/// `DELETE /agents/{agent}` — same as the deregister tool.
async fn deregister_one(
    State(bus): State<Ctx>,
    path: Result<Path<String>, PathRejection>,
) -> Response {
    let agent = match path_name(path) {
        Ok(a) => a,
        Err(resp) => return resp,
    };
    respond(bus.deregister(&agent).await.map(crate::tools::to_wire))
}

/// `POST /messages` `{ "from", "to", "body" }` — same as send_message.
async fn send_msg(State(bus): State<Ctx>, body: Result<Json<Value>, JsonRejection>) -> Response {
    let v = match json_body(body) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    respond(crate::tools::send_message(&bus, &v).await)
}

/// `GET /inboxes/{agent}?wait_ms=` — same as check_inbox: returned messages
/// are marked read.
async fn pull(
    State(bus): State<Ctx>,
    path: Result<Path<String>, PathRejection>,
    query: Result<Query<HashMap<String, String>>, QueryRejection>,
) -> Response {
    let agent = match path_name(path) {
        Ok(a) => a,
        Err(resp) => return resp,
    };
    let Ok(Query(q)) = query else {
        return err_response(MailboxError::InvalidWait);
    };
    let wait_ms = match parse_wait_query(q.get("wait_ms").map(String::as_str)) {
        Ok(w) => w,
        Err(e) => return err_response(e),
    };
    match bus.check_inbox(&agent, wait_ms).await {
        Ok(page) => no_store_response(page),
        Err(e) => err_response(e),
    }
}

/// `GET /history/{agent}?with=&room=&before=` — same as the history tool.
async fn history(
    State(bus): State<Ctx>,
    path: Result<Path<String>, PathRejection>,
    query: Result<Query<HashMap<String, String>>, QueryRejection>,
) -> Response {
    let agent = match path_name(path) {
        Ok(a) => a,
        Err(resp) => return resp,
    };
    let Ok(Query(q)) = query else {
        return err_response(MailboxError::InvalidCursor);
    };
    let field = |key: &str| q.get(key).map(|v| Value::String(v.clone()));
    let with = match crate::tools::optional_name(field("with").as_ref()) {
        Ok(w) => w,
        Err(e) => return err_response(e),
    };
    let before = match crate::tools::parse_cursor(field("before").as_ref()) {
        Ok(b) => b,
        Err(e) => return err_response(e),
    };
    let room = match crate::tools::optional_room(field("room").as_ref()) {
        Ok(r) => r,
        Err(e) => return err_response(e),
    };
    let page = match room {
        Some(_) if with.is_some() => Err(MailboxError::InvalidBody),
        Some(room) => bus.room_history(&agent, &room, before).await,
        None => bus.history(&agent, with.as_ref(), before).await,
    };
    match page {
        Ok(page) => no_store_response(page),
        Err(e) => err_response(e),
    }
}

/// `POST /rooms` `{ "agent", "room", "private"?, "description"? }` — same as
/// create_room.
async fn create_room(State(bus): State<Ctx>, body: Result<Json<Value>, JsonRejection>) -> Response {
    let v = match json_body(body) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    respond(crate::tools::create_room(&bus, &v).await)
}

/// `GET /rooms?agent=` — same as list_rooms.
async fn list_rooms(
    State(bus): State<Ctx>,
    query: Result<Query<HashMap<String, String>>, QueryRejection>,
) -> Response {
    let Ok(Query(q)) = query else {
        return err_response(MailboxError::InvalidBody);
    };
    let agent = q.get("agent").map_or(Value::Null, |a| json!(a));
    respond(crate::tools::list_rooms(&bus, &json!({ "agent": agent })).await)
}

/// The `{room}` path segment, as `path_name` does for agents.
#[allow(clippy::result_large_err)]
fn path_room(path: Result<Path<String>, PathRejection>) -> Result<String, Response> {
    path.map(|Path(raw)| raw).map_err(|_| {
        err_response(MailboxError::InvalidRoom {
            room: String::new(),
        })
    })
}

/// A request body that must be a JSON object, with the path's `room` set in
/// it (the path wins over a `room` in the body).
#[allow(clippy::result_large_err)]
fn body_with_room(
    body: Result<Json<Value>, JsonRejection>,
    room: String,
) -> Result<Value, Response> {
    let mut v = json_body(body)?;
    let Some(object) = v.as_object_mut() else {
        return Err(err_response(MailboxError::InvalidBody));
    };
    object.insert("room".to_string(), Value::String(room));
    Ok(v)
}

/// `POST /rooms/{room}/members` `{ "agent" }` — same as join_room.
async fn join_room(
    State(bus): State<Ctx>,
    path: Result<Path<String>, PathRejection>,
    body: Result<Json<Value>, JsonRejection>,
) -> Response {
    let v = match path_room(path).and_then(|room| body_with_room(body, room)) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    respond(crate::tools::join_room(&bus, &v).await)
}

/// `POST /rooms/{room}/invitations` `{ "agent", "invitee" }` — same as invite.
async fn invite(
    State(bus): State<Ctx>,
    path: Result<Path<String>, PathRejection>,
    body: Result<Json<Value>, JsonRejection>,
) -> Response {
    let v = match path_room(path).and_then(|room| body_with_room(body, room)) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    respond(crate::tools::invite(&bus, &v).await)
}

/// `DELETE /rooms/{room}/members/{agent}` — same as leave_room.
async fn leave_room(
    State(bus): State<Ctx>,
    path: Result<Path<(String, String)>, PathRejection>,
) -> Response {
    let Ok(Path((room, agent))) = path else {
        return err_response(MailboxError::InvalidRoom {
            room: String::new(),
        });
    };
    respond(crate::tools::leave_room(&bus, &json!({ "agent": agent, "room": room })).await)
}

/// `GET /health` — `{ ok, agents, pending, read }`; counts only.
async fn health(State(bus): State<Ctx>) -> Response {
    match bus.stats().await {
        Ok(stats) => ok_response(json!({
            "ok": true,
            "agents": stats.agents,
            "pending": stats.unread,
            "read": stats.read,
        })),
        Err(e) => err_response(e),
    }
}

/// Unknown REST path.
async fn not_found() -> Response {
    err_response(MailboxError::NotFound)
}

/// Known REST path, method it does not serve.
async fn method_not_allowed() -> Response {
    err_response(MailboxError::MethodNotAllowed)
}

/// Contract wait_ms query semantics: absent/empty -> 0; digit string within
/// range -> value; anything else -> `invalid_wait`.
fn parse_wait_query(raw: Option<&str>) -> Result<u32, MailboxError> {
    match raw {
        None | Some("") => Ok(0),
        Some(s) => crate::tools::parse_wait_ms(Some(&Value::String(s.to_string()))),
    }
}
