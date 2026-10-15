//! The five mailbox tools exposed over MCP.
//!
//! One [`ToolProtocol`] implementation dispatches by tool name; schemas stay
//! primitive (no regex patterns) so local GGUF/OpenCode grammars keep
//! compiling. Domain failures return `Ok(ToolResult::failure(..))` so
//! harnesses render an `isError: true` content block carrying the contract
//! error JSON; Rust `Err` is reserved for malformed *requests* (upstream
//! turns those into JSON-RPC `-32602`).

use std::sync::Arc;

use mcp::protocol::{ToolMetadata, ToolParameter, ToolParameterType, ToolProtocol, ToolResult};
use serde_json::{json, Value};

use crate::bus::{Bus, Registration, Retention};
use crate::errors::MailboxError;
use crate::names::AgentName;

/// Ritual returned in the MCP `initialize` result so every harness sees the
/// same instructions; [`instructions`] appends the retention line.
pub const INSTRUCTIONS: &str = "\
mentischat is a local mailbox between agents on this machine.
Register using your model name in snake_case, e.g. deepseek_v4_flash, with a one-line description of who you are and what you are working on. Call list_agents first to see which names are taken. If that exact name is already on the roster (multiple instances of the same model can coexist), append a numeric suffix starting at 1 — deepseek_v4_flash_1, _2, ... — and pick the lowest free name.
The first time you register in a conversation, pass exclusive: true. If that fails with name_taken, another agent already holds the name: try the next free suffix. Never take over a name you did not register yourself: without exclusive, created: false means the name was already on the roster and you now share its inbox.
Before send_message, call list_agents. Only send to names that appear there; each entry shows the agent's description and how many messages are queued for it. from must be your registered name. When you need a reply, say so in the body so the recipient responds when done.
Nothing wakes an idle agent: a message is read only when its recipient calls check_inbox, usually because the user asked it to check its messages. After send_message, end your turn unless a peer is answering within the same turn, in which case call check_inbox with wait_ms > 0 to wait for the reply.
check_inbox deletes the messages it returns: each is delivered once and nothing is kept, so quote the bodies in your reply right away. If remaining > 0, check_inbox again (wait_ms 0) until remaining is 0.
Call deregister only when the user says they are done.";

/// [`INSTRUCTIONS`] plus how long unread mail and idle agents are kept, in
/// the units the operator configured.
pub fn instructions(retention: &Retention) -> String {
    format!(
        "{INSTRUCTIONS}\nUnread messages expire {} after they are sent. An agent that has not called register or check_inbox for {} is dropped from the roster with its unread mail; if a call fails unregistered, register again.",
        human(retention.message_ttl),
        human(retention.agent_ttl),
    )
}

/// `90s` -> "90 seconds", `3600s` -> "1 hour": the largest unit that divides
/// the duration exactly.
fn human(d: std::time::Duration) -> String {
    let secs = d.as_secs();
    let (n, unit) = [(86_400, "day"), (3_600, "hour"), (60, "minute")]
        .into_iter()
        .find(|(size, _)| secs >= *size && secs.is_multiple_of(*size))
        .map_or((secs, "second"), |(size, unit)| (secs / size, unit));
    format!("{n} {unit}{}", if n == 1 { "" } else { "s" })
}

/// MCP tool surface over the shared [`Bus`].
pub struct MailboxProtocol {
    bus: Arc<Bus>,
}

impl MailboxProtocol {
    /// Bind the protocol to a bus.
    pub fn new(bus: Arc<Bus>) -> Self {
        Self { bus }
    }
}

fn str_param(name: &str, description: &str) -> ToolParameter {
    ToolParameter::new(name, ToolParameterType::String)
        .with_description(description)
        .required()
}

fn metadata_for(tool_name: &str) -> Option<ToolMetadata> {
    match tool_name {
        "register" => Some(
            ToolMetadata::new(
                "register",
                "Claim this agent name on the local mentischat roster. Call list_agents first. Use your model name in snake_case (e.g. deepseek_v4_flash); if that name is taken, append a numeric suffix starting at 1 (..._1, ..._2) and pick the lowest free one. Pass exclusive: true the first time; name_taken means someone else holds the name, so try the next suffix. Without exclusive the call is idempotent and created: false means the name was already registered (its inbox is shared).",
            )
            .with_parameter(str_param("agent", "The exact roster name to claim"))
            .with_parameter(
                ToolParameter::new("description", ToolParameterType::String).with_description(
                    "One line on who you are and what you are working on (max 256 bytes); shown to peers in list_agents. Omit to keep the current one, empty string clears it",
                ),
            )
            .with_parameter(
                ToolParameter::new("exclusive", ToolParameterType::Boolean)
                    .with_description("Fail with name_taken instead of joining a name that is already registered; pass true the first time you register")
                    .with_default(json!(false)),
            ),
        ),
        "deregister" => Some(
            ToolMetadata::new(
                "deregister",
                "Leave the roster and discard this name's undelivered messages. Only call when the user says they are done.",
            )
            .with_parameter(str_param("agent", "The roster name to release")),
        ),
        "send_message" => Some(
            ToolMetadata::new(
                "send_message",
                "Queue a message for a registered agent. Fails if from or to is not registered. Put what you need in the body (say if you expect a reply). Nothing wakes the recipient: it reads the message when it next calls check_inbox.",
            )
            .with_parameter(str_param("from", "Your registered name"))
            .with_parameter(str_param("to", "Recipient's registered name (must be on the roster)"))
            .with_parameter(str_param("body", "Message text (max 64 KiB, non-empty)")),
        ),
        "check_inbox" => Some(
            ToolMetadata::new(
                "check_inbox",
                "Return and delete the next batch of messages for this registered name: each message is delivered once, so quote the bodies right away. wait_ms (0-30000) blocks for mail — use only when a peer is replying within the same turn. If remaining > 0, call again.",
            )
            .with_parameter(str_param("agent", "Your registered name"))
            .with_parameter(
                ToolParameter::new("wait_ms", ToolParameterType::Integer)
                    .with_description("Block up to this many ms waiting for mail (0-30000); use 0 unless a peer is replying within this turn")
                    .with_default(json!(0)),
            ),
        ),
        "list_agents" => Some(ToolMetadata::new(
            "list_agents",
            "Names on the roster, each with its description, when it was last seen, and how many messages are queued for it. Call before register and before send_message.",
        )),
        _ => None,
    }
}

const TOOL_NAMES: [&str; 5] = [
    "register",
    "deregister",
    "send_message",
    "check_inbox",
    "list_agents",
];

#[async_trait::async_trait]
impl ToolProtocol for MailboxProtocol {
    async fn execute(
        &self,
        tool_name: &str,
        parameters: Value,
    ) -> Result<ToolResult, Box<dyn std::error::Error + Send + Sync>> {
        tracing::info!(tool = tool_name, "mcp tool");
        let outcome: Result<Value, MailboxError> = match tool_name {
            "register" => self.register(&parameters).await,
            "deregister" => self.deregister(&parameters).await,
            "send_message" => self.send_message(&parameters).await,
            "check_inbox" => self.check_inbox(&parameters).await,
            "list_agents" => self.list_agents().await,
            other => return Err(format!("unknown tool: {other}").into()),
        };
        Ok(match outcome {
            Ok(value) => ToolResult::success(value),
            Err(e) => ToolResult::failure(e.to_json_string()),
        })
    }

    async fn list_tools(
        &self,
    ) -> Result<Vec<ToolMetadata>, Box<dyn std::error::Error + Send + Sync>> {
        // A name without metadata is a bug in this file, not a tool to hide.
        let mut tools = Vec::with_capacity(TOOL_NAMES.len());
        for name in TOOL_NAMES {
            match metadata_for(name) {
                Some(meta) => tools.push(meta),
                None => return Err(format!("missing metadata for tool: {name}").into()),
            }
        }
        Ok(tools)
    }

    async fn get_tool_metadata(
        &self,
        tool_name: &str,
    ) -> Result<ToolMetadata, Box<dyn std::error::Error + Send + Sync>> {
        metadata_for(tool_name).ok_or_else(|| format!("unknown tool: {tool_name}").into())
    }

    fn protocol_name(&self) -> &str {
        "mentischat.mailbox"
    }
}

impl MailboxProtocol {
    async fn register(&self, params: &Value) -> Result<Value, MailboxError> {
        let agent = required_name(params, "agent")?;
        let opts = parse_registration(params)?;
        let out = self.bus.register_with(&agent, opts).await?;
        Ok(serde_json::to_value(out).expect("RegisterOutcome is always serializable"))
    }

    async fn deregister(&self, params: &Value) -> Result<Value, MailboxError> {
        let agent = required_name(params, "agent")?;
        let out = self.bus.deregister(&agent).await?;
        Ok(json!({ "agent": out.agent, "discarded": out.discarded }))
    }

    async fn send_message(&self, params: &Value) -> Result<Value, MailboxError> {
        // Validate from before to: when both are bad the error names from.
        let from = required_name(params, "from")?;
        let to = required_name(params, "to")?;
        let body = params
            .get("body")
            .and_then(Value::as_str)
            .ok_or(MailboxError::InvalidBody)?
            .to_string();
        let out = self.bus.send(&from, &to, body).await?;
        Ok(json!({ "id": out.id, "from": out.from, "to": out.to, "createdAt": out.created_at }))
    }

    async fn check_inbox(&self, params: &Value) -> Result<Value, MailboxError> {
        let agent = required_name(params, "agent")?;
        let wait_ms = parse_wait_ms(params.get("wait_ms"))?;
        let page = self.bus.check_inbox(&agent, wait_ms).await?;
        Ok(serde_json::to_value(page).expect("InboxPage is always serializable"))
    }

    async fn list_agents(&self) -> Result<Value, MailboxError> {
        let agents = self.bus.list_agents().await?;
        Ok(json!({ "agents": agents }))
    }
}

/// Pull a required string field and validate it as an [`AgentName`].
fn required_name(params: &Value, key: &str) -> Result<AgentName, MailboxError> {
    let raw = params.get(key).and_then(Value::as_str).unwrap_or("");
    AgentName::parse(raw)
}

/// The optional `description` and `exclusive` fields of a register call,
/// shared by the tool and `POST /agents`.
pub fn parse_registration(params: &Value) -> Result<Registration, MailboxError> {
    Ok(Registration {
        description: parse_description(params.get("description"))?,
        exclusive: parse_flag(params.get("exclusive"))?,
    })
}

/// Contract description semantics: absent/null -> keep the stored one; a
/// string is passed to the bus, which trims it, treats empty as "clear", and
/// rejects anything over [`crate::bus::MAX_DESCRIPTION_BYTES`] or containing
/// control characters; any other JSON type -> `invalid_description`.
pub fn parse_description(v: Option<&Value>) -> Result<Option<String>, MailboxError> {
    match v {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(s)) => Ok(Some(s.clone())),
        Some(_) => Err(MailboxError::InvalidDescription),
    }
}

/// Optional boolean: absent/null -> false; `true`/`false` or the strings
/// `"true"`/`"false"` (some clients stringify booleans) -> value; anything
/// else -> `invalid_body`.
fn parse_flag(v: Option<&Value>) -> Result<bool, MailboxError> {
    match v {
        None | Some(Value::Null) => Ok(false),
        Some(Value::Bool(b)) => Ok(*b),
        Some(Value::String(s)) if s == "true" => Ok(true),
        Some(Value::String(s)) if s == "false" => Ok(false),
        Some(_) => Err(MailboxError::InvalidBody),
    }
}

/// Contract wait_ms semantics: absent/null -> 0; integer or digit string in
/// 0..=30000 -> value; anything else -> `invalid_wait`.
pub fn parse_wait_ms(v: Option<&Value>) -> Result<u32, MailboxError> {
    let raw = match v {
        None | Some(Value::Null) => return Ok(0),
        Some(Value::Number(n)) => {
            return n
                .as_u64()
                .filter(|w| *w <= u64::from(crate::bus::MAX_WAIT_MS))
                .map(|w| w as u32)
                .ok_or(MailboxError::InvalidWait);
        }
        Some(Value::String(s)) => s,
        Some(_) => return Err(MailboxError::InvalidWait),
    };
    if raw.is_empty() {
        return Ok(0);
    }
    if !raw.bytes().all(|b| b.is_ascii_digit()) {
        return Err(MailboxError::InvalidWait);
    }
    raw.parse::<u32>()
        .ok()
        .filter(|w| *w <= crate::bus::MAX_WAIT_MS)
        .ok_or(MailboxError::InvalidWait)
}
