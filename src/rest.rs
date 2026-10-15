//! REST surface: same behaviors as the MCP tools, on the shared bus.
//!
//! Success is always 200 + JSON (no 201/204). Errors map once via
//! [`MailboxError::http_status`] + [`MailboxError::to_json`]. Inbox pulls
//! mutate state, so they answer with `Cache-Control: no-store`.

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
        .route("/inboxes/{agent}", get(pull))
        .route("/health", get(health))
        .fallback(not_found)
        .with_state(bus)
}

fn err_response(e: MailboxError) -> Response {
    (e.http_status(), Json(e.to_json())).into_response()
}

fn ok_response(value: Value) -> Response {
    (StatusCode::OK, Json(value)).into_response()
}

/// Serialise a pull result with `Cache-Control: no-store` — it mutates live
/// state and must never be cached.
fn no_store_response<T: serde::Serialize>(value: &T) -> Response {
    let mut resp = Json(serde_json::to_value(value).unwrap_or(Value::Null)).into_response();
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

/// `POST /agents` `{ "agent", "description"?, "exclusive"? }` — same as register.
async fn register(State(bus): State<Ctx>, body: Result<Json<Value>, JsonRejection>) -> Response {
    let v = match json_body(body) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let agent = match AgentName::parse(v.get("agent").and_then(Value::as_str).unwrap_or("")) {
        Ok(a) => a,
        Err(e) => return err_response(e),
    };
    let opts = match crate::tools::parse_registration(&v) {
        Ok(opts) => opts,
        Err(e) => return err_response(e),
    };
    match bus.register_with(&agent, opts).await {
        Ok(out) => ok_response(serde_json::to_value(out).unwrap_or(Value::Null)),
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
async fn deregister_one(
    State(bus): State<Ctx>,
    path: Result<Path<String>, PathRejection>,
) -> Response {
    let agent = match path_name(path) {
        Ok(a) => a,
        Err(resp) => return resp,
    };
    match bus.deregister(&agent).await {
        Ok(out) => ok_response(json!({ "agent": out.agent, "discarded": out.discarded })),
        Err(e) => err_response(e),
    }
}

/// `POST /messages` `{ "from", "to", "body" }` — same as send_message.
async fn send_msg(State(bus): State<Ctx>, body: Result<Json<Value>, JsonRejection>) -> Response {
    let v = match json_body(body) {
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

/// `GET /inboxes/{agent}?wait_ms=` — same as check_inbox: returned messages
/// are deleted.
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
        Ok(page) => no_store_response(&page),
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
