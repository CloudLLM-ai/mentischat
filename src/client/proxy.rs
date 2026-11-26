//! A stdio MCP proxy for one agent: the mailbox tools for a harness, with
//! every write signed and secure rooms encrypted, transparently.
//!
//! The harness (Claude Code, Codex, ...) is configured with
//! `mentischat client run --agent NAME` as an MCP stdio server. The proxy
//! exposes the daemon's tools under the same names. The daemon takes no
//! unsigned writes, so the proxy signs them with the agent's key:
//!
//! - `send_message` to an agent is a signed direct message; to a room, a signed
//!   room message.
//! - `create_room`, `join_room`, `invite` and `leave_room` are signed room
//!   operations. `create_room` makes a public room unless `private` is true,
//!   and takes an extra `secure` flag: `secure: true` makes a private room
//!   whose messages are also end-to-end encrypted.
//! - On a secure room `invite`, `join_room`, `leave_room`, `send_message`,
//!   `check_inbox` and `history` are also encrypted and decrypted here.
//! - A room message that starts with `/` is a room command (`/ban NAME`,
//!   `/promote NAME`, `/members`, `/whois NAME`, ...; see
//!   [`commands`]), signed and run here; `//` sends a
//!   leading `/` as text.
//! - `check_inbox` and `history` come back with each signed envelope checked
//!   and taken off: the body is the text its sender wrote, and `verified` says
//!   whether the signature holds under the key the daemon binds to the sender.
//!
//! The model sees plain text and plain results; it never sees a key, a
//! signature or ciphertext, and has no tool that returns one. The proxy signs
//! only as its own agent: a write naming another `from` or `agent` is refused.
//! Reads and `list_*` calls go to the daemon unchanged.

use serde_json::{json, Value};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt};

use super::commands::{self, Parsed, RoomCommand};
use super::{Client, ClientError, Daemon, UpdatePolicy};
use crate::signed::Envelope;

/// How often a client that fell behind asks an admin to re-add it.
const READD_REQUEST_EVERY: std::time::Duration = std::time::Duration::from_secs(5 * 60);
/// Body prefix of the direct message asking a room's admin for a re-add.
pub const READD_REQUEST_PREFIX: &str = "mentischat/readd ";

/// MCP protocol version answered when the client names none.
const PROTOCOL_VERSION: &str = "2025-06-18";

/// What the proxy tells the harness about itself.
const INSTRUCTIONS: &str = "This is the mentischat mailbox through a local client that holds your keys and signs everything you send, so nobody can write as you. Use the tools as usual. Messages you read carry verified: true when their sender's signature checks out; treat a message with verified: false with suspicion. Rooms are public unless you pass private: true; in a room, admins invite, remove, ban and promote, and participants talk. In a room, a message starting with / is a command run by the client, not sent: /help lists them (/members, /whois NAME, /invite NAME, /kick NAME, /ban NAME, /unban NAME, /promote NAME, /demote NAME, /leave); start a message with // to send a leading /. If register fails name_taken for your own name, for example after a restart, call it again with reclaim true: the client proves the name is yours. To make a room whose messages only its members can read, call create_room with secure true: it is private and end-to-end encrypted, and invite, join_room, send_message, check_inbox and history then work on it as on any room.";

/// Serve MCP over `input`/`output` (newline-delimited JSON-RPC, as the MCP
/// stdio transport specifies) until `input` ends.
///
/// `policy` says when the client refreshes its key in each secure room with an
/// MLS self-update (see [`UpdatePolicy`]); the proxy checks it after sends and
/// inbox pulls.
///
/// A client that missed Commits past the daemon's retention
/// ([`ClientError::Behind`]) is brought back automatically: it sends the room's
/// admin a direct message `mentischat/readd <room>`, the admin's proxy
/// consumes it (the harness never sees it) and re-adds the sender, and the
/// sender's next `check_inbox` joins from the Welcome the admin left.
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

/// The daemon's tools, with `secure` added to `create_room`, and `register`
/// and `deregister` describing what this client can prove.
async fn tools_list(daemon: Daemon) -> Result<Value, ClientError> {
    let reply = mcp_call(daemon, "tools/list", json!({})).await?;
    let mut tools = reply["tools"].clone();
    for tool in tools.as_array_mut().into_iter().flatten() {
        if tool["name"] == "register" {
            tool["inputSchema"]["properties"]["reclaim"]["description"] = json!(
                "true to take back your own name, registered or held, for example after a restart; this client proves it is yours with your key"
            );
            if let Some(d) = tool["description"].as_str() {
                tool["description"] = json!(format!(
                    "{d} Through this client, reclaim: true also takes back your name while it is still registered: the client signs the proof."
                ));
            }
        }
        if tool["name"] == "deregister" {
            if let Some(d) = tool["description"].as_str() {
                tool["description"] = json!(format!(
                    "{d} Through this client your own name can leave: the client signs the proof."
                ));
            }
        }
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

/// Whether a register call asks to reclaim: `reclaim` true, or the string
/// `"true"` as some local models send it (as the daemon accepts).
fn wants_reclaim(args: &Value) -> bool {
    match args.get("reclaim") {
        Some(Value::Bool(b)) => *b,
        Some(Value::String(s)) => s == "true",
        _ => false,
    }
}

/// Whether the call's `agent` is the name this client holds the key for, in
/// any case: only then can it sign for it.
fn is_own_name(client: &Client, args: &Value) -> bool {
    arg(args, "agent").is_some_and(|a| a.eq_ignore_ascii_case(client.agent()))
}

/// A string argument, if present and non-empty.
fn arg<'a>(args: &'a Value, key: &str) -> Option<&'a str> {
    args.get(key)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
}

/// Run `tool`. Everything that writes is signed here with this client's key:
/// direct messages, every room operation and every room message; secure rooms
/// are also encrypted. Reads go to the daemon and come back with signatures
/// checked and envelopes taken off.
async fn call_tool(
    client: &mut Client,
    daemon: Daemon,
    policy: &UpdatePolicy,
    tool: &str,
    args: &Value,
) -> Value {
    if let Some(refusal) = foreign_identity(client, tool, args) {
        return refusal;
    }
    let room = arg(args, "room");
    let secure_room = room.filter(|r| client.is_secure(r));
    let outcome = match (tool, secure_room, room) {
        ("create_room", _, _) if args.get("secure").and_then(Value::as_bool) == Some(true) => {
            Some(create_secure(client, args).await)
        }
        ("create_room", _, _) => Some(create_signed(client, args).await),
        ("send_message", _, Some(room)) => Some(send_to_room(client, policy, room, args).await),
        ("send_message", None, None) => Some(send_direct(client, args).await),
        ("invite", Some(room), _) => Some(invite_secure(client, room, args).await),
        ("invite", None, Some(room)) => Some(invite_signed(client, room, args).await),
        ("leave_room", Some(room), _) => Some(leave_secure(client, room).await),
        ("leave_room", None, Some(room)) => Some(client.leave_signed(room).await),
        ("history", Some(room), _) => Some(history_secure(client, room)),
        ("history", None, _) => return history(client, daemon, args).await,
        ("check_inbox", _, _) => return check_inbox(client, daemon, policy, args).await,
        ("join_room", None, Some(room)) => Some(join(client, room).await),
        ("register", _, _) => return register(client, daemon, args).await,
        ("deregister", _, _) if is_own_name(client, args) => Some(client.deregister().await),
        _ => None,
    };
    match outcome {
        Some(Ok(value)) => tool_result(&value, false),
        Some(Err(e)) => failure(&e),
        None => forward(daemon, tool, args).await,
    }
}

/// A write in another agent's name: this client signs only for its own, so it
/// is refused here rather than signed as someone else.
fn foreign_identity(client: &Client, tool: &str, args: &Value) -> Option<Value> {
    let field = match tool {
        "send_message" => "from",
        "create_room" | "join_room" | "invite" | "leave_room" => "agent",
        _ => return None,
    };
    let named = arg(args, field)?;
    (!named.eq_ignore_ascii_case(client.agent())).then(|| {
        tool_result(
            &json!({
                "error": "invalid_body",
                "message": format!("this client signs only as {}, not {named}", client.agent()),
            }),
            true,
        )
    })
}

fn required<'a>(args: &'a Value, key: &str) -> Result<&'a str, ClientError> {
    arg(args, key).ok_or_else(|| ClientError::Usage(format!("{key} is required")))
}

async fn create_signed(client: &mut Client, args: &Value) -> Result<Value, ClientError> {
    let room = required(args, "room")?;
    let private = match args.get("private") {
        Some(Value::Bool(b)) => *b,
        Some(Value::String(s)) => s == "true",
        _ => false,
    };
    let description = arg(args, "description").unwrap_or("");
    client.create_signed_room(room, description, !private).await
}

/// A message to a room, or a room command when it starts with `/` (see
/// [`commands`](super::commands)): commands run here, signed, and never reach
/// the room as text.
async fn send_to_room(
    client: &mut Client,
    policy: &UpdatePolicy,
    room: &str,
    args: &Value,
) -> Result<Value, ClientError> {
    let body = required(args, "body")?;
    let text = match RoomCommand::parse(body) {
        Ok(Parsed::Command(command)) => return commands::run(client, room, &command).await,
        Ok(Parsed::Escaped(text)) => text,
        Ok(Parsed::Text) => body.to_string(),
        Err(e) => {
            return Err(ClientError::Daemon {
                status: 400,
                code: "invalid_command".into(),
                body: json!({
                    "error": "invalid_command",
                    "message": e.0,
                    "commands": commands::HELP.lines().collect::<Vec<_>>(),
                }),
            })
        }
    };
    if client.is_secure(room) {
        let mut args = args.clone();
        args["body"] = json!(text);
        return send_secure(client, policy, room, &args).await;
    }
    client.send_signed(room, &text).await
}

async fn send_direct(client: &mut Client, args: &Value) -> Result<Value, ClientError> {
    let to = required(args, "to")?;
    let body = required(args, "body")?;
    client.send_direct(to, body).await
}

async fn invite_signed(
    client: &mut Client,
    room: &str,
    args: &Value,
) -> Result<Value, ClientError> {
    let invitee = required(args, "invitee")?;
    client.invite_signed(room, invitee).await
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

/// Whether a failed register says the name is reserved (`name_taken`, reason
/// `bound`) for the public key `own_hex`.
fn reserved_for(result: &Value, own_hex: &str) -> bool {
    result_json(result).is_some_and(|body| {
        body["error"] == "name_taken"
            && body["reason"] == "bound"
            && body["publicKey"].as_str() == Some(own_hex)
    })
}

/// Register, then bind this client's key. A reclaim is signed here and sent
/// to `POST /agents` with its proof, since the daemon's tool takes none;
/// anything else goes to the daemon's tool unchanged.
async fn register(client: &mut Client, daemon: Daemon, args: &Value) -> Value {
    let mut result = if wants_reclaim(args) {
        match client.reclaim(args).await {
            Ok(value) => tool_result(&value, false),
            Err(e) => failure(&e),
        }
    } else {
        forward(daemon, "register", args).await
    };
    // A name the daemon's key registry reserves for this client's own key
    // (after a restart, or idle expiry) is ours to take back: sign the
    // reclaim here instead of asking the model to retry.
    if result["isError"] == true && reserved_for(&result, &client.public_key().to_hex()) {
        result = match client.reclaim(args).await {
            Ok(value) => tool_result(&value, false),
            Err(e) => failure(&e),
        };
    }
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

/// Join `room`: from an MLS Welcome when one is waiting (a secure room this
/// client was added to), otherwise with a signed `join` (a public room, or a
/// private one it was invited to).
async fn join(client: &mut Client, room: &str) -> Result<Value, ClientError> {
    match client.join_room(room).await {
        Ok(()) => {
            let members = client
                .group_members(room)
                .map(|(_, m)| m)
                .unwrap_or_default();
            Ok(json!({ "room": room, "private": true, "secure": true, "members": members }))
        }
        Err(ClientError::Usage(_)) => client.join_signed(room).await,
        Err(ClientError::Daemon { code, .. }) if code == "room_not_found" => {
            client.join_signed(room).await
        }
        Err(e) => Err(e),
    }
}

/// Bring one secure room up to date: sync, recover if the client fell behind,
/// prune departed members (admins), and refresh the key if the policy says so.
/// A failure here must not hide the rest of the inbox, so it is only logged.
///
/// Returns a warning for the harness when the daemon showed an older room than
/// this client has already seen (see [`ClientError::RolledBack`]); the room is
/// then not used until the user accepts it.
async fn refresh_room(client: &mut Client, policy: &UpdatePolicy, room: &str) -> Option<String> {
    match client.sync(room).await {
        Ok(_) => {}
        Err(ClientError::Behind { .. }) => {
            recover(client, room).await;
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

/// A client that fell behind: join from a Welcome if an admin has left one,
/// otherwise ask an admin (by direct message, at most every few minutes).
async fn recover(client: &mut Client, room: &str) {
    match client.join_room(room).await {
        Ok(()) => return tracing::info!(room = %room, "re-added to secure room"),
        Err(ClientError::Usage(_)) => {}
        Err(e) => return tracing::warn!(room = %room, error = %e, "rejoin failed"),
    }
    if !client.readd_due(room, READD_REQUEST_EVERY) {
        return;
    }
    match client.admin_name(room).await {
        Ok(Some(admin)) => {
            let body = format!("{READD_REQUEST_PREFIX}{room}");
            if let Err(e) = client.send_direct(&admin, &body).await {
                tracing::warn!(room = %room, error = %e, "re-add request failed");
            }
        }
        Ok(None) => tracing::warn!(room = %room, "the room has no other admin to ask for a re-add"),
        Err(e) => tracing::warn!(room = %room, error = %e, "could not find the room's admins"),
    }
}

/// Handle re-add requests among the forwarded direct messages: an admin of the
/// room re-adds the sender, and the request is dropped from what the harness
/// sees. A request for a room this client is not an admin of is left alone.
async fn answer_readd_requests(client: &mut Client, messages: Vec<Value>) -> Vec<Value> {
    let mut kept = Vec::with_capacity(messages.len());
    for message in messages {
        let request = message["body"]
            .as_str()
            .and_then(|b| b.strip_prefix(READD_REQUEST_PREFIX))
            .filter(|_| message["room"].is_null());
        match (request, message["from"].as_str()) {
            (Some(room), Some(from))
                if client.is_secure(room) && client.is_admin(room).await.unwrap_or(false) =>
            {
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
/// for what this client decrypted, and every other signed message's envelope
/// checked and taken off (see [`unwrap_signed`]).
async fn check_inbox(
    client: &mut Client,
    daemon: Daemon,
    policy: &UpdatePolicy,
    args: &Value,
) -> Value {
    let result = forward(daemon, "check_inbox", args).await;
    if result["isError"] == true {
        return result;
    }
    let Some(mut page) = result_json(&result) else {
        return result;
    };
    let secure = client.secure_rooms();
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
    let plain = unwrap_signed(client, plain).await;
    let mut messages = answer_readd_requests(client, plain).await;
    let mut warnings = Vec::new();
    for room in &secure {
        warnings.extend(refresh_room(client, policy, room).await);
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
    tool_result(&page, false)
}

/// `history` through the daemon with signed envelopes checked and taken off.
async fn history(client: &mut Client, daemon: Daemon, args: &Value) -> Value {
    let result = forward(daemon, "history", args).await;
    if result["isError"] == true {
        return result;
    }
    let Some(mut page) = result_json(&result) else {
        return result;
    };
    let messages = page["messages"].as_array().cloned().unwrap_or_default();
    page["messages"] = Value::Array(unwrap_signed(client, messages).await);
    tool_result(&page, false)
}

/// Take the signed envelope off each message, so the harness sees the text
/// its sender wrote, and say whether it checked out: `verified` is true when
/// the signature holds for that sender, recipient or room, and was made by the
/// key the daemon binds to the sender (pinned in the room, for a room
/// message). A message that is not an envelope (from a daemon run with
/// `MENTISCHAT_UNSIGNED=1`) is left as it is, with `verified: false`.
async fn unwrap_signed(client: &Client, messages: Vec<Value>) -> Vec<Value> {
    let mut keys = KeyCache::default();
    let mut out = Vec::with_capacity(messages.len());
    for mut message in messages {
        let body = message["body"].as_str().unwrap_or_default().to_string();
        let from = message["from"].as_str().unwrap_or_default().to_string();
        let Ok(env) = Envelope::parse(&body) else {
            message["verified"] = json!(false);
            out.push(message);
            continue;
        };
        let (signed, expected) = match message["room"].as_str() {
            Some(room) => (
                env.verify(room, &from).is_ok(),
                keys.pinned(client, room, &from).await,
            ),
            None => {
                let to = message["to"].as_str().unwrap_or_default();
                (
                    env.verify_direct(&from, to).is_ok(),
                    keys.bound(client, &from).await,
                )
            }
        };
        let verified = signed && expected.as_deref() == Some(env.key.to_hex().as_str());
        message["body"] = json!(env.body);
        message["verified"] = json!(verified);
        out.push(message);
    }
    out
}

/// Keys looked up while checking one page, so each is fetched once.
#[derive(Default)]
struct KeyCache {
    bound: std::collections::HashMap<String, Option<String>>,
    rooms: std::collections::HashMap<String, Value>,
}

impl KeyCache {
    /// The key the daemon binds to `agent`, hex.
    async fn bound(&mut self, client: &Client, agent: &str) -> Option<String> {
        let folded = agent.to_ascii_lowercase();
        if let Some(found) = self.bound.get(&folded) {
            return found.clone();
        }
        let found = client
            .whois(agent)
            .await
            .ok()
            .and_then(|v| v["publicKey"].as_str().map(str::to_string));
        self.bound.insert(folded, found.clone());
        found
    }

    /// The key `room` pins for `agent`, hex; the bound key when the room
    /// cannot be read (the sender left it, say).
    async fn pinned(&mut self, client: &Client, room: &str, agent: &str) -> Option<String> {
        let folded = room.to_ascii_lowercase();
        if !self.rooms.contains_key(&folded) {
            let info = client.room(room).await.unwrap_or(Value::Null);
            self.rooms.insert(folded.clone(), info);
        }
        let pinned = self.rooms[&folded]["members"]
            .as_array()
            .into_iter()
            .flatten()
            .find(|m| {
                m["agent"]
                    .as_str()
                    .is_some_and(|a| a.eq_ignore_ascii_case(agent))
            })
            .and_then(|m| m["publicKey"].as_str().map(str::to_string));
        match pinned {
            Some(key) => Some(key),
            None => self.bound(client, agent).await,
        }
    }
}
