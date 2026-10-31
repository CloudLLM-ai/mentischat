//! Parameter parsing for the signed-room REST routes: the same shape as the
//! tool functions in [`crate::tools`], JSON object in, bus call, wire value
//! out. Requests use snake_case keys like the tool parameters (`wait_ms`);
//! responses use camelCase like every other response.
//!
//! Signed operations have no MCP tool on purpose: `tools/list` is part of the
//! wire contract, and a model must not be asked to produce signatures.

use serde_json::Value;

use crate::bus::{Bus, SignedRequest};
use crate::errors::MailboxError;
use crate::names::{AgentName, RoomName};
use crate::signed::{OpKind, PublicKey, Signature, SignedOp};
use crate::tools::{to_wire, uint};

/// A required string field; a missing or non-string value is `invalid_body`.
fn text<'a>(params: &'a Value, key: &str) -> Result<&'a str, MailboxError> {
    params
        .get(key)
        .and_then(Value::as_str)
        .ok_or(MailboxError::InvalidBody)
}

fn signature(params: &Value) -> Result<Signature, MailboxError> {
    text(params, "signature")?
        .parse()
        .map_err(|_| MailboxError::InvalidSignature)
}

fn public_key(params: &Value, key: &str) -> Result<PublicKey, MailboxError> {
    text(params, key)?
        .parse()
        .map_err(|_| MailboxError::InvalidKey)
}

/// `PUT /agents/{agent}/key` `{ "public_key", "signature" }`: bind the key to
/// the roster name (trust on first use).
///
/// # Errors
///
/// See [`Bus::register_key`], plus `invalid_name`, `invalid_key` and
/// `invalid_body` for parameters that do not parse.
pub async fn register_key(bus: &Bus, agent: &str, params: &Value) -> Result<Value, MailboxError> {
    let agent = AgentName::parse(agent)?;
    let key = public_key(params, "public_key")?;
    let binding = bus.register_key(&agent, key, &signature(params)?).await?;
    Ok(to_wire(binding))
}

/// `GET /agents/{agent}/key`: the bound key, `{ "agent", "publicKey" }`, or
/// `{ "agent", "publicKey": null }` when none is bound.
///
/// # Errors
///
/// `invalid_name`, `unregistered`, `shutting_down`.
pub async fn get_key(bus: &Bus, agent: &str) -> Result<Value, MailboxError> {
    let agent = AgentName::parse(agent)?;
    Ok(match bus.agent_key(&agent).await? {
        Some(binding) => to_wire(binding),
        None => serde_json::json!({ "agent": agent.as_str(), "publicKey": null }),
    })
}

/// Parse the `op` field and the fields it needs into a [`SignedOp`].
fn parse_op(params: &Value) -> Result<SignedOp, MailboxError> {
    let kind: OpKind = text(params, "op")?
        .parse()
        .map_err(|_| MailboxError::InvalidBody)?;
    let own = |key: &str| text(params, key).map(str::to_string);
    Ok(match kind {
        OpKind::RegisterKey => return Err(MailboxError::InvalidBody),
        OpKind::Create => SignedOp::Create {
            description: match params.get("description") {
                None | Some(Value::Null) => String::new(),
                Some(Value::String(s)) => s.clone(),
                Some(_) => return Err(MailboxError::InvalidDescription),
            },
        },
        OpKind::Invite => SignedOp::Invite {
            invitee: own("invitee")?,
            invitee_key: public_key(params, "invitee_key")?,
        },
        OpKind::Join => SignedOp::Join,
        OpKind::Leave => SignedOp::Leave,
        OpKind::Remove => SignedOp::Remove {
            target: own("target")?,
        },
        OpKind::Send => SignedOp::Send { body: own("body")? },
    })
}

/// `POST /signed/ops`: one signed room operation. `agent`, `room`, `op`,
/// `counter` and `signature` are required, plus the fields the op signs:
/// `description` (create, optional), `invitee` and `invitee_key` (invite),
/// `target` (remove), `body` (send).
///
/// # Errors
///
/// See [`Bus::signed_op`], plus `invalid_name`, `invalid_room`,
/// `invalid_body`, `invalid_key` and `invalid_signature` for parameters that
/// do not parse.
pub async fn signed_op(bus: &Bus, params: &Value) -> Result<Value, MailboxError> {
    let agent = AgentName::parse(text(params, "agent")?)?;
    let room = RoomName::parse(text(params, "room")?)?;
    let counter = params
        .get("counter")
        .and_then(uint)
        .ok_or(MailboxError::InvalidBody)?;
    let request = SignedRequest {
        agent,
        room,
        counter,
        op: parse_op(params)?,
        signature: signature(params)?,
    };
    Ok(to_wire(bus.signed_op(request).await?))
}

/// `GET /signed/rooms/{room}?agent=`: the members of a signed room with their
/// pinned keys, for a member.
///
/// # Errors
///
/// `invalid_name`, `invalid_room`, and see [`Bus::signed_room_info`].
pub async fn signed_room(bus: &Bus, room: &str, agent: &str) -> Result<Value, MailboxError> {
    let agent = AgentName::parse(agent)?;
    let room = RoomName::parse(room)?;
    Ok(to_wire(bus.signed_room_info(&agent, &room).await?))
}
