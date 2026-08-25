//! REST surface: same behaviors as the MCP tools, on the shared bus.
//!
//! Success is always 200 + JSON (no 201/204). Errors map once via
//! [`MailboxError::http_status`] + [`MailboxError::to_json`]. Inbox pulls
//! mutate state, so they answer with `Cache-Control: no-store`.

use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::rejection::JsonRejection;
use axum::extract::{DefaultBodyLimit, Path, Query, State};
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
        .route(
            "/agents/{agent}/wrapper",
            post(bind_wrapper).delete(unbind_wrapper),
        )
        .route("/messages", post(send_msg))
        .route("/inboxes/{agent}", get(pull))
        .route("/inboxes/{agent}/ack", post(ack))
        .route("/nudge/{agent}", get(nudge_peek))
        .route("/wake/{wake_id}", get(wake_peek).delete(unbind_wake))
        .route("/health", get(health))
        .fallback(not_found)
        .layer(DefaultBodyLimit::max(1024 * 1024))
        .with_state(bus)
}

fn err_response(e: MailboxError) -> Response {
    (e.http_status(), Json(e.to_json())).into_response()
}

fn ok_response(value: Value) -> Response {
    (StatusCode::OK, Json(value)).into_response()
}

/// Parse a JSON request body; any extractor rejection is a client error.
async fn json_body(body: Result<Json<Value>, JsonRejection>) -> Result<Value, Response> {
    match body {
        Ok(Json(v)) => Ok(v),
        Err(_) => Err(err_response(MailboxError::InvalidBody)),
    }
}

/// Parse a JSON object body where an empty body means `{}` (optional-field
/// endpoints). Malformed JSON is still a client error.
fn json_body_lenient(raw: String) -> Result<Value, MailboxError> {
    if raw.trim().is_empty() {
        return Ok(json!({}));
    }
    serde_json::from_str::<Value>(&raw).map_err(|_| MailboxError::InvalidBody)
}

/// Extract an optional `wakeId` string field.
fn optional_wake_id(v: &Value) -> Result<Option<String>, MailboxError> {
    match v.get("wakeId") {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(s)) => crate::nudge::validate_wake_id(s).map(Some),
        Some(_) => Err(MailboxError::InvalidBody),
    }
}

/// `POST /agents` `{ "agent", "wakeId"?, "nudgeTransport"? }` — same as register.
async fn register(State(bus): State<Ctx>, body: Result<Json<Value>, JsonRejection>) -> Response {
    let v = match json_body(body).await {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let agent = match AgentName::parse(v.get("agent").and_then(Value::as_str).unwrap_or("")) {
        Ok(a) => a,
        Err(e) => return err_response(e),
    };
    let transport = match crate::tools::parse_transport(v.get("nudgeTransport")) {
        Ok(t) => t,
        Err(e) => return err_response(e),
    };
    let wake_id = match crate::tools::parse_wake_id(v.get("wakeId")) {
        Ok(w) => w,
        Err(e) => return err_response(e),
    };
    match bus
        .register_with_wake(&agent, transport, wake_id.as_deref())
        .await
    {
        Ok(out) => ok_response(json!({
            "agent": out.agent,
            "created": out.created,
            "lastSeen": out.last_seen,
            "nudgeWindow": out.nudge_window,
            "nudgeTransport": out.nudge_transport
        })),
        Err(e) => err_response(e),
    }
}

/// `GET /agents` — roster sorted by name ascending.
async fn list(State(bus): State<Ctx>) -> Response {
    match bus.list_agents().await {
        Ok(entries) => ok_response(json!({ "agents": entries })),
        Err(e) => err_response(e),
    }
}

/// `DELETE /agents/{agent}` — same as the deregister tool.
async fn deregister_one(State(bus): State<Ctx>, Path(agent): Path<String>) -> Response {
    let agent = match AgentName::parse(&agent) {
        Ok(a) => a,
        Err(e) => return err_response(e),
    };
    match bus.deregister(&agent).await {
        Ok(out) => ok_response(json!({ "agent": out.agent, "discarded": out.discarded })),
        Err(e) => err_response(e),
    }
}

/// `POST /agents/{agent}/wrapper` — bind `mentis-wake` as this agent's
/// doorbell. Body is optional (`{}`); `{ "pid": 12345 }` is accepted and
/// ignored; `{ "wakeId": "<uuid>" }` pins or refreshes the ownership token.
/// When already bound, the request must present the current token
/// (otherwise `403 forbidden`). Returns the effective token — only it may
/// unbind or re-bind later.
async fn bind_wrapper(State(bus): State<Ctx>, Path(agent): Path<String>, body: String) -> Response {
    let agent = match AgentName::parse(&agent) {
        Ok(a) => a,
        Err(e) => return err_response(e),
    };
    let v = match json_body_lenient(body) {
        Ok(v) => v,
        Err(e) => return err_response(e),
    };
    if !v.is_object() {
        return err_response(MailboxError::InvalidBody);
    }
    let wake_id = match optional_wake_id(&v) {
        Ok(w) => w,
        Err(e) => return err_response(e),
    };
    match bus.bind_wrapper(&agent, wake_id.as_deref()).await {
        Ok(token) => ok_response(json!({
            "agent": agent.to_string(),
            "nudgeTransport": "wrapper",
            "wakeId": token
        })),
        Err(e) => err_response(e),
    }
}

/// `DELETE /agents/{agent}/wrapper` — clear the wrapper binding; transport
/// becomes `none` so the agent must explicitly re-bind. Requires the
/// `wakeId` token returned by bind; a stale wrapper cannot unbind its
/// replacement.
async fn unbind_wrapper(
    State(bus): State<Ctx>,
    Path(agent): Path<String>,
    body: String,
) -> Response {
    let agent = match AgentName::parse(&agent) {
        Ok(a) => a,
        Err(e) => return err_response(e),
    };
    let v = match json_body_lenient(body) {
        Ok(v) => v,
        Err(e) => return err_response(e),
    };
    let wake_id = match optional_wake_id(&v) {
        Ok(Some(w)) => w,
        Ok(None) => return err_response(MailboxError::InvalidBody),
        Err(e) => return err_response(e),
    };
    match bus.unbind_wrapper(&agent, &wake_id).await {
        Ok(()) => ok_response(json!({
            "agent": agent.to_string(),
            "nudgeTransport": "none"
        })),
        Err(e) => err_response(e),
    }
}

/// `GET /nudge/{agent}?wait_ms=` — read-only nudge long-poll (never
/// promotes, acks, or touches `lastSeen`).
async fn nudge_peek(
    State(bus): State<Ctx>,
    Path(agent): Path<String>,
    Query(q): Query<HashMap<String, String>>,
) -> Response {
    let agent = match AgentName::parse(&agent) {
        Ok(a) => a,
        Err(e) => return err_response(e),
    };
    let wait_ms = match parse_wait_query(q.get("wait_ms").map(String::as_str)) {
        Ok(w) => w,
        Err(e) => return err_response(e),
    };
    match bus.nudge_peek(&agent, wait_ms).await {
        Ok(peek) => {
            let mut resp = Json(serde_json::to_value(peek).unwrap_or(Value::Null)).into_response();
            resp.headers_mut().insert(
                header::CACHE_CONTROL,
                header::HeaderValue::from_static("no-store"),
            );
            resp
        }
        Err(e) => err_response(e),
    }
}

/// `GET /wake/{wake_id}?wait_ms=` — long-poll before or after register binds
/// this session (read-only; same semantics as `/nudge/{agent}`).
async fn wake_peek(
    State(bus): State<Ctx>,
    Path(wake_id): Path<String>,
    Query(q): Query<HashMap<String, String>>,
) -> Response {
    let wake_id = match crate::nudge::validate_wake_id(&wake_id) {
        Ok(w) => w,
        Err(e) => return err_response(e),
    };
    let wait_ms = match parse_wait_query(q.get("wait_ms").map(String::as_str)) {
        Ok(w) => w,
        Err(e) => return err_response(e),
    };
    match bus.nudge_peek_by_wake(&wake_id, wait_ms).await {
        Ok(peek) => {
            let mut resp = Json(serde_json::to_value(peek).unwrap_or(Value::Null)).into_response();
            resp.headers_mut().insert(
                header::CACHE_CONTROL,
                header::HeaderValue::from_static("no-store"),
            );
            resp
        }
        Err(e) => err_response(e),
    }
}

/// `DELETE /wake/{wake_id}` — clear wrapper binding on `mentis-wake` exit.
async fn unbind_wake(State(bus): State<Ctx>, Path(wake_id): Path<String>) -> Response {
    let wake_id = match crate::nudge::validate_wake_id(&wake_id) {
        Ok(w) => w,
        Err(e) => return err_response(e),
    };
    match bus.unbind_wake(&wake_id).await {
        Ok(()) => ok_response(json!({ "wakeId": wake_id, "nudgeTransport": "none" })),
        Err(e) => err_response(e),
    }
}

/// `POST /messages` `{ "from", "to", "body" }` — same as send_message.
async fn send_msg(State(bus): State<Ctx>, body: Result<Json<Value>, JsonRejection>) -> Response {
    let v = match json_body(body).await {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    // Validate from before to: when both are bad the error names from.
    let from = match AgentName::parse(v.get("from").and_then(Value::as_str).unwrap_or("")) {
        Ok(a) => a,
        Err(e) => return err_response(e),
    };
    let to = match AgentName::parse(v.get("to").and_then(Value::as_str).unwrap_or("")) {
        Ok(a) => a,
        Err(e) => return err_response(e),
    };
    let text = match v.get("body").and_then(Value::as_str) {
        Some(t) => t.to_string(),
        None => return err_response(MailboxError::InvalidBody),
    };
    match bus.send(&from, &to, text).await {
        Ok(out) => ok_response(
            json!({ "id": out.id, "from": out.from, "to": out.to, "createdAt": out.created_at }),
        ),
        Err(e) => err_response(e),
    }
}

/// `GET /inboxes/{agent}?wait_ms=` — check_inbox without ack.
async fn pull(
    State(bus): State<Ctx>,
    Path(agent): Path<String>,
    Query(q): Query<HashMap<String, String>>,
) -> Response {
    let agent = match AgentName::parse(&agent) {
        Ok(a) => a,
        Err(e) => return err_response(e),
    };
    let wait_ms = match parse_wait_query(q.get("wait_ms").map(String::as_str)) {
        Ok(w) => w,
        Err(e) => return err_response(e),
    };
    match bus.check_inbox(&agent, wait_ms, Vec::new()).await {
        Ok(page) => {
            let mut resp = Json(serde_json::to_value(page).unwrap_or(Value::Null)).into_response();
            resp.headers_mut().insert(
                header::CACHE_CONTROL,
                header::HeaderValue::from_static("no-store"),
            );
            resp
        }
        Err(e) => err_response(e),
    }
}

/// `POST /inboxes/{agent}/ack` `{ "ids": [...] }` — ack only.
async fn ack(
    State(bus): State<Ctx>,
    Path(agent): Path<String>,
    body: Result<Json<Value>, JsonRejection>,
) -> Response {
    let agent = match AgentName::parse(&agent) {
        Ok(a) => a,
        Err(e) => return err_response(e),
    };
    let v = match json_body(body).await {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let ids = match crate::tools::parse_ack_ids(v.get("ids")) {
        Ok(ids) => ids,
        Err(e) => return err_response(e),
    };
    match bus.ack(&agent, ids).await {
        Ok(acked) => ok_response(json!({ "agent": agent.to_string(), "acked": acked })),
        Err(e) => err_response(e),
    }
}

/// `GET /health` — `{ ok, agents, pending }`; counts only.
async fn health(State(bus): State<Ctx>) -> Response {
    match bus.stats().await {
        Ok((agents, pending)) => {
            ok_response(json!({ "ok": true, "agents": agents, "pending": pending }))
        }
        Err(e) => err_response(e),
    }
}

/// Unknown REST path.
async fn not_found() -> Response {
    err_response(MailboxError::NotFound)
}

/// Contract wait_ms query semantics: absent/empty -> 0; digit string within
/// range -> value; anything else -> `invalid_wait`.
fn parse_wait_query(raw: Option<&str>) -> Result<u32, MailboxError> {
    match raw {
        None | Some("") => Ok(0),
        Some(s) => crate::tools::parse_wait_ms(Some(&Value::String(s.to_string()))),
    }
}
