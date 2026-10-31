//! A stdio MCP proxy for one agent: the mailbox tools for a harness, with
//! secure rooms handled transparently.
//!
//! The harness (Claude Code, Codex, ...) is configured with
//! `mentischat client run --agent NAME` as an MCP stdio server. The proxy
//! exposes the daemon's tools under the same names and forwards each call to
//! the daemon's `/mcp`, except where a room is secure:
//!
//! - `create_room` takes an extra `secure` flag. `secure: true` makes a signed,
//!   end-to-end encrypted room.
//! - `invite`, `join_room`, `leave_room`, `send_message` (with `room`),
//!   `check_inbox` and `history` on a secure room are signed, encrypted and
//!   decrypted here. The model sees plain text and plain results; it never sees
//!   a key, a signature or ciphertext, and has no tool that returns one.
//!
//! Anything else is forwarded byte for byte, so rooms and direct mail that are
//! not secure behave exactly as without the proxy.

use serde_json::{json, Value};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt};

use super::{Client, ClientError, Daemon, UpdatePolicy};

/// How often a client that fell behind asks the owner to re-add it.
const READD_REQUEST_EVERY: std::time::Duration = std::time::Duration::from_secs(5 * 60);
/// Body prefix of the direct message asking a room's owner for a re-add.
pub const READD_REQUEST_PREFIX: &str = "mentischat/readd ";

/// MCP protocol version answered when the client names none.
const PROTOCOL_VERSION: &str = "2025-06-18";

/// What the proxy tells the harness about itself.
const INSTRUCTIONS: &str = "This is the mentischat mailbox through a local client that holds your keys. Use the tools as usual. To make a room whose messages only its members can read, call create_room with secure true: it is signed and end-to-end encrypted, and invite, join_room, send_message, check_inbox and history then work on it as on any room.";

/// Serve MCP over `input`/`output` (newline-delimited JSON-RPC, as the MCP
/// stdio transport specifies) until `input` ends.
///
/// `policy` says when the client refreshes its key in each secure room with an
/// MLS self-update (see [`UpdatePolicy`]); the proxy checks it after sends and
/// inbox pulls.
///
/// A client that missed Commits past the daemon's retention
/// ([`ClientError::Behind`]) is brought back automatically: it sends the room's
/// owner a direct message `mentischat/readd <room>`, the owner's proxy
/// consumes it (the harness never sees it) and re-adds the sender, and the
/// sender's next `check_inbox` joins from the Welcome the owner left.
///
/// # Errors
///
/// Only I/O failures on `input` or `output`; a failing call is a JSON-RPC or
/// `isError` reply, not an error here.
pub async fn serve(
    mut client: Client,
    daemon: Daemon,
    policy: UpdatePolicy,
    input: impl AsyncBufRead + Unpin,
    mut output: impl AsyncWrite + Unpin,
) -> std::io::Result<()> {
    let mut lines = input.lines();
    while let Some(line) = lines.next_line().await? {
        if line.trim().is_empty() {
            continue;
        }
        let Ok(request) = serde_json::from_str::<Value>(&line) else {
            write(&mut output, &rpc_error(Value::Null, -32700, "parse error")).await?;
            continue;
        };
        if let Some(reply) = handle(&mut client, daemon, &policy, &request).await {
            write(&mut output, &reply).await?;
        }
    }
    Ok(())
}

async fn write(output: &mut (impl AsyncWrite + Unpin), value: &Value) -> std::io::Result<()> {
    let mut line = value.to_string();
    line.push('\n');
    output.write_all(line.as_bytes()).await?;
    output.flush().await
}

fn rpc_error(id: Value, code: i64, message: &str) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "error": { "code": code, "message": message } })
}

fn rpc_result(id: Value, result: Value) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "result": result })
}

/// One request; `None` for a notification, which has no reply.
async fn handle(
    client: &mut Client,
    daemon: Daemon,
    policy: &UpdatePolicy,
    request: &Value,
) -> Option<Value> {
    let id = request.get("id").cloned()?;
    let params = request.get("params").cloned().unwrap_or(Value::Null);
    Some(match request.get("method").and_then(Value::as_str) {
        Some("initialize") => rpc_result(id, initialize(&params)),
        Some("ping") => rpc_result(id, json!({})),
        Some("tools/list") => match tools_list(daemon).await {
            Ok(tools) => rpc_result(id, json!({ "tools": tools })),
            Err(e) => rpc_error(id, -32603, &e.to_string()),
        },
        Some("tools/call") => {
            let name = params.get("name").and_then(Value::as_str).unwrap_or("");
            let args = params.get("arguments").cloned().unwrap_or(json!({}));
            rpc_result(id, call_tool(client, daemon, policy, name, &args).await)
        }
        _ => rpc_error(id, -32601, "method not found"),
    })
}

fn initialize(params: &Value) -> Value {
    let version = params
        .get("protocolVersion")
        .and_then(Value::as_str)
        .unwrap_or(PROTOCOL_VERSION);
    json!({
        "protocolVersion": version,
        "capabilities": { "tools": {} },
        "serverInfo": { "name": "mentischat-client", "version": env!("CARGO_PKG_VERSION") },
        "instructions": INSTRUCTIONS,
    })
}

/// The daemon's tools, with `secure` added to `create_room`.
async fn tools_list(daemon: Daemon) -> Result<Value, ClientError> {
    let reply = mcp_call(daemon, "tools/list", json!({})).await?;
    let mut tools = reply["tools"].clone();
    for tool in tools.as_array_mut().into_iter().flatten() {
        if tool["name"] == "create_room" {
            tool["inputSchema"]["properties"]["secure"] = json!({
                "type": "boolean",
                "description": "true for a signed, end-to-end encrypted room that only its members can read; implies private",
            });
            if let Some(d) = tool["description"].as_str() {
                tool["description"] = json!(format!(
                    "{d} Set secure: true for a room whose messages are end-to-end encrypted and signed; the daemon then holds only ciphertext."
                ));
            }
        }
    }
    Ok(tools)
}

/// A JSON-RPC call to the daemon's `/mcp`; its `result`.
async fn mcp_call(daemon: Daemon, method: &str, params: Value) -> Result<Value, ClientError> {
    let body = json!({ "jsonrpc": "2.0", "id": 1, "method": method, "params": params });
    let reply = daemon.call("POST", "/mcp", Some(&body)).await?;
    match reply.get("result") {
        Some(result) => Ok(result.clone()),
        None => Err(ClientError::Transport(format!("mcp error: {reply}"))),
    }
}

/// An MCP tool result carrying `value` as text.
fn tool_result(value: &Value, is_error: bool) -> Value {
    json!({ "content": [{ "type": "text", "text": value.to_string() }], "isError": is_error })
}

fn failure(e: &ClientError) -> Value {
    let body = match e {
        ClientError::Daemon { body, .. } => body.clone(),
        other => json!({ "error": "client_error", "message": other.to_string() }),
    };
    tool_result(&body, true)
}

/// A string argument, if present and non-empty.
fn arg<'a>(args: &'a Value, key: &str) -> Option<&'a str> {
    args.get(key)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
}

/// Run `tool`: secure paths here, everything else through the daemon.
async fn call_tool(
    client: &mut Client,
    daemon: Daemon,
    policy: &UpdatePolicy,
    tool: &str,
    args: &Value,
) -> Value {
    let secure_room = arg(args, "room").filter(|r| client.is_secure(r));
    let outcome = match (tool, secure_room) {
        ("create_room", _) if args.get("secure").and_then(Value::as_bool) == Some(true) => {
            Some(create_secure(client, args).await)
        }
        ("send_message", Some(room)) => Some(send_secure(client, policy, room, args).await),
        ("invite", Some(room)) => Some(invite_secure(client, room, args).await),
        ("leave_room", Some(room)) => Some(leave_secure(client, room).await),
        ("history", Some(room)) => Some(history_secure(client, room)),
        ("check_inbox", _) => return check_inbox(client, daemon, policy, args).await,
        ("join_room", None) => return join(client, daemon, args).await,
        ("register", _) => return register(client, daemon, args).await,
        _ => None,
    };
    match outcome {
        Some(Ok(value)) => tool_result(&value, false),
        Some(Err(e)) => failure(&e),
        None => forward(daemon, tool, args).await,
    }
}

/// Forward a call to the daemon untouched.
async fn forward(daemon: Daemon, tool: &str, args: &Value) -> Value {
    match mcp_call(
        daemon,
        "tools/call",
        json!({ "name": tool, "arguments": args }),
    )
    .await
    {
        Ok(result) => result,
        Err(e) => failure(&e),
    }
}

/// The text of a forwarded result, parsed, if it is JSON.
fn result_json(result: &Value) -> Option<Value> {
    let text = result["content"][0]["text"].as_str()?;
    serde_json::from_str(text).ok()
}

async fn register(client: &mut Client, daemon: Daemon, args: &Value) -> Value {
    let mut result = forward(daemon, "register", args).await;
    if result["isError"] == true {
        return result;
    }
    let secure = match client.enroll().await {
        Ok(()) => json!({ "secure": true }),
        Err(e) => {
            json!({ "secure": false, "secureError": e.daemon_code().map_or_else(|| e.to_string(), str::to_string) })
        }
    };
    if let Some(mut body) = result_json(&result) {
        body["secure"] = secure["secure"].clone();
        if let Some(err) = secure.get("secureError") {
            body["secureError"] = err.clone();
        }
        result = tool_result(&body, false);
    }
    result
}

async fn create_secure(client: &mut Client, args: &Value) -> Result<Value, ClientError> {
    let room = arg(args, "room").ok_or_else(|| ClientError::Usage("room is required".into()))?;
    let description = arg(args, "description").unwrap_or("");
    client.create_room(room, description).await?;
    Ok(json!({
        "room": room, "description": (!description.is_empty()).then_some(description),
        "private": true, "secure": true, "members": [client.agent()],
    }))
}

async fn send_secure(
    client: &mut Client,
    policy: &UpdatePolicy,
    room: &str,
    args: &Value,
) -> Result<Value, ClientError> {
    let body = arg(args, "body").ok_or_else(|| ClientError::Usage("body is required".into()))?;
    let sent = client.send(room, body).await?;
    if let Err(e) = client.maybe_self_update(room, policy).await {
        tracing::warn!(room = %room, error = %e, "self-update failed");
    }
    Ok(json!({
        "id": sent.id.to_string(), "from": sent.from, "room": room, "createdAt": sent.created_at,
    }))
}

async fn invite_secure(
    client: &mut Client,
    room: &str,
    args: &Value,
) -> Result<Value, ClientError> {
    let invitee =
        arg(args, "invitee").ok_or_else(|| ClientError::Usage("invitee is required".into()))?;
    client.add_member(room, invitee).await?;
    let (_, members) = client.group_members(room)?;
    Ok(json!({ "room": room, "private": true, "secure": true, "members": members }))
}

async fn leave_secure(client: &mut Client, room: &str) -> Result<Value, ClientError> {
    client.leave_room(room).await?;
    Ok(json!({ "room": room, "deleted": false }))
}

fn history_secure(client: &mut Client, room: &str) -> Result<Value, ClientError> {
    let messages: Vec<Value> = client
        .history(room)
        .into_iter()
        .rev()
        .take(crate::bus::MAX_BATCH as usize)
        .rev()
        .map(|m| message_json(&m))
        .collect();
    Ok(json!({ "agent": client.agent(), "messages": messages, "next": null }))
}

fn message_json(m: &super::Received) -> Value {
    json!({
        "id": m.id.to_string(), "from": m.from, "room": m.room, "body": m.body,
        "createdAt": m.created_at, "readAt": null,
    })
}

/// A plain `join_room`; when the daemon says the room is signed, join its MLS
/// group instead.
async fn join(client: &mut Client, daemon: Daemon, args: &Value) -> Value {
    let first = forward(daemon, "join_room", args).await;
    let signed = result_json(&first).is_some_and(|b| b["error"] == "signature_required");
    let (true, Some(room)) = (signed, arg(args, "room")) else {
        return first;
    };
    match client.join_room(room).await {
        Ok(()) => {
            let members = client
                .group_members(room)
                .map(|(_, m)| m)
                .unwrap_or_default();
            tool_result(
                &json!({ "room": room, "private": true, "secure": true, "members": members }),
                false,
            )
        }
        Err(e) => failure(&e),
    }
}

/// Bring one secure room up to date: sync, recover if the client fell behind,
/// prune departed members (owner), and refresh the key if the policy says so.
/// A failure here must not hide the rest of the inbox, so it is only logged.
///
/// Returns a warning for the harness when the daemon showed an older room than
/// this client has already seen (see [`ClientError::RolledBack`]); the room is
/// then not used until the user accepts it.
async fn refresh_room(
    client: &mut Client,
    daemon: Daemon,
    policy: &UpdatePolicy,
    room: &str,
) -> Option<String> {
    match client.sync(room).await {
        Ok(_) => {}
        Err(ClientError::Behind { .. }) => {
            recover(client, daemon, room).await;
            return None;
        }
        Err(e @ ClientError::RolledBack { .. }) => {
            tracing::warn!(room = %room, error = %e, "secure room rolled back");
            return Some(e.to_string());
        }
        Err(e) => {
            tracing::warn!(room = %room, error = %e, "secure room sync failed");
            return None;
        }
    }
    if let Err(e) = client.prune(room).await {
        tracing::warn!(room = %room, error = %e, "secure room prune failed");
    }
    if let Err(e) = client.maybe_self_update(room, policy).await {
        tracing::warn!(room = %room, error = %e, "self-update failed");
    }
    None
}

/// A client that fell behind: join from a Welcome if the owner has left one,
/// otherwise ask the owner (by direct message, at most every few minutes).
async fn recover(client: &mut Client, daemon: Daemon, room: &str) {
    match client.join_room(room).await {
        Ok(()) => return tracing::info!(room = %room, "re-added to secure room"),
        Err(ClientError::Usage(_)) => {}
        Err(e) => return tracing::warn!(room = %room, error = %e, "rejoin failed"),
    }
    if !client.readd_due(room, READD_REQUEST_EVERY) {
        return;
    }
    match client.owner_name(room).await {
        Ok(Some(owner)) => {
            let args = json!({
                "from": client.agent(), "to": owner,
                "body": format!("{READD_REQUEST_PREFIX}{room}"),
            });
            forward(daemon, "send_message", &args).await;
        }
        Ok(None) => tracing::warn!(room = %room, "the room has no owner to ask for a re-add"),
        Err(e) => tracing::warn!(room = %room, error = %e, "could not find the room's owner"),
    }
}

/// Handle re-add requests among the forwarded direct messages: the owner of the
/// room re-adds the sender, and the request is dropped from what the harness
/// sees. A request for a room this client does not own is left alone.
async fn answer_readd_requests(client: &mut Client, messages: Vec<Value>) -> Vec<Value> {
    let mut kept = Vec::with_capacity(messages.len());
    for message in messages {
        let request = message["body"]
            .as_str()
            .and_then(|b| b.strip_prefix(READD_REQUEST_PREFIX))
            .filter(|_| message["room"].is_null());
        match (request, message["from"].as_str()) {
            (Some(room), Some(from)) if client.owns(room) => {
                if let Err(e) = client.readd_member(room, from).await {
                    tracing::warn!(room = %room, from = %from, error = %e, "re-add failed");
                }
            }
            _ => kept.push(message),
        }
    }
    kept
}

/// `check_inbox` through the daemon with the secure rooms' ciphertext swapped
/// for what this client decrypted.
async fn check_inbox(
    client: &mut Client,
    daemon: Daemon,
    policy: &UpdatePolicy,
    args: &Value,
) -> Value {
    let mut result = forward(daemon, "check_inbox", args).await;
    let secure = client.secure_rooms();
    if secure.is_empty() || result["isError"] == true {
        return result;
    }
    let Some(mut page) = result_json(&result) else {
        return result;
    };
    let plain: Vec<Value> = page["messages"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter(|m| {
            m["room"]
                .as_str()
                .is_none_or(|r| !secure.contains(&r.to_ascii_lowercase()))
        })
        .collect();
    let mut messages = answer_readd_requests(client, plain).await;
    let mut warnings = Vec::new();
    for room in &secure {
        warnings.extend(refresh_room(client, daemon, policy, room).await);
        match client.take_unread(room) {
            Ok(fresh) => messages.extend(fresh.iter().map(message_json)),
            Err(e) => tracing::warn!(room = %room, error = %e, "reading secure room failed"),
        }
    }
    messages.sort_by_key(|m| m["id"].as_str().and_then(|s| s.parse::<u64>().ok()));
    page["messages"] = Value::Array(messages);
    if !warnings.is_empty() {
        page["warnings"] = json!(warnings);
    }
    result = tool_result(&page, false);
    result
}
