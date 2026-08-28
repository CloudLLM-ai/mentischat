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

use crate::bus::{Bus, NudgeTransport};
use crate::errors::MailboxError;
use crate::names::AgentName;

/// Ritual returned in the MCP `initialize` result so every harness sees the
/// same instructions.
pub const INSTRUCTIONS: &str = "\
mentischat is a local mailbox between TUI agents on this machine.
Register using your model name in snake_case, e.g. deepseek_v4_flash. If that exact name is already on the roster (multiple instances of the same model can coexist), append a numeric suffix starting at 1 — deepseek_v4_flash_1, _2, ... — and pick the lowest free name. Call list_agents first to see which names are taken.
When running under mentischat (e.g. mentischat opencode), the environment variable MENTISCHAT_WAKE_ID is set. On your first register, pass that value as wakeId so this session binds for idle wake. You choose the agent name; the command line does not need it upfront.
If nudgeTransport is not wrapper and MENTISCHAT_WAKE_ID is unset, tell the user this session cannot be woken while idle.
Before send_message, call list_agents. Only send to names that appear there. from must be your registered name. When you need a reply, say so in the body so the recipient responds when done.
After send_message, end your turn — do not wait inline. mentis-wake injects the pointer when a reply lands; then call check_inbox with wait_ms 0 to pull it. Use wait_ms > 0 only when a peer is answering within the same turn.
When check_inbox returns messages, quote the bodies in your reply. Pass those ids as ack_ids on the next check_inbox. If remaining > 0, check_inbox again (wait_ms 0) until remaining is 0.
Call deregister only when the user says they are done.";

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
                "Claim this agent name on the local mentischat roster. Idempotent. Use your model name in snake_case (e.g. deepseek_v4_flash); if that name is taken, append a numeric suffix starting at 1 (..._1, ..._2) and pick the lowest free one. When MENTISCHAT_WAKE_ID is set in the environment (mentis-wake), pass it as wakeId on register to bind idle wake. Optional nudgeTransport: 'none' (default) or 'wrapper'.",
            )
            .with_parameter(str_param("agent", "The exact roster name to claim"))
            .with_parameter(
                ToolParameter::new("wakeId", ToolParameterType::String)
                    .with_description("MENTISCHAT_WAKE_ID from the environment when running under mentis-wake"),
            )
            .with_parameter(
                ToolParameter::new("nudgeTransport", ToolParameterType::String)
                    .with_description("Wake transport: 'none' (default), 'wrapper', or deprecated 'terminal' (treated as none)"),
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
                "Queue a message for a registered agent. Fails if from or to is not registered. Put what you need in the body (say if you expect a reply). After sending, end your turn — the doorbell nudges you when the reply arrives; do not wait inline.",
            )
            .with_parameter(str_param("from", "Your registered name"))
            .with_parameter(str_param("to", "Recipient's registered name (must be on the roster)"))
            .with_parameter(str_param("body", "Message text (max 64 KiB, non-empty)")),
        ),
        "check_inbox" => Some(
            ToolMetadata::new(
                "check_inbox",
                "Return the next batch of messages for this registered name (at-least-once). Optional ack_ids deletes previously returned in-flight messages. wait_ms (0-30000) blocks for mail — use only when a peer is replying within the same turn; otherwise rely on the doorbell to nudge you. Quote bodies, then ack those ids on the next call. If remaining > 0, call again.",
            )
            .with_parameter(str_param("agent", "Your registered name"))
            .with_parameter(
                ToolParameter::new("wait_ms", ToolParameterType::Integer)
                    .with_description("Block up to this many ms waiting for mail (0-30000); prefer 0 and rely on the doorbell to wake you")
                    .with_default(json!(0)),
            )
            .with_parameter(
                ToolParameter::new("ack_ids", ToolParameterType::Array)
                    .with_description("Ids of previously delivered messages to acknowledge (delete)")
                    .with_items(ToolParameterType::String),
            ),
        ),
        "list_agents" => Some(ToolMetadata::new(
            "list_agents",
            "Names on the roster, lastSeen, nudgeWindow (always null), and nudgeTransport (wrapper | none).",
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
        let transport = parse_transport(params.get("nudgeTransport"))?;
        let wake_id = parse_wake_id(params.get("wakeId"))?;
        let out = self
            .bus
            .register_with_wake(&agent, transport, wake_id.as_deref())
            .await?;
        Ok(json!({
            "agent": out.agent,
            "created": out.created,
            "lastSeen": out.last_seen,
            "nudgeWindow": out.nudge_window,
            "nudgeTransport": out.nudge_transport,
        }))
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
        let ack_ids = parse_ack_ids(params.get("ack_ids"))?;
        let page = self.bus.check_inbox(&agent, wait_ms, ack_ids).await?;
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

pub fn parse_wake_id(v: Option<&Value>) -> Result<Option<String>, MailboxError> {
    match v {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(s)) => Ok(Some(crate::nudge::validate_wake_id(s)?)),
        Some(_) => Err(MailboxError::InvalidBody),
    }
}

/// Contract `nudgeTransport` semantics: absent/null -> keep existing on
/// re-register; new names default to none. `terminal` is a deprecated alias
/// for none.
pub fn parse_transport(v: Option<&Value>) -> Result<Option<NudgeTransport>, MailboxError> {
    match v {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(s)) => NudgeTransport::parse(Some(s)),
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

/// Contract ack_ids semantics: absent/null -> empty; array of strings ->
/// ids; anything else -> `invalid_body`. Arrays longer than
/// [`crate::bus::MAX_ACK_IDS`] are rejected: no inbox can hold more than that
/// many unacked rows, so a longer list is junk that would otherwise run as
/// that many statements while holding the bus lock.
pub fn parse_ack_ids(v: Option<&Value>) -> Result<Vec<String>, MailboxError> {
    match v {
        None | Some(Value::Null) => Ok(Vec::new()),
        Some(Value::Array(items)) if items.len() > crate::bus::MAX_ACK_IDS => {
            Err(MailboxError::InvalidBody)
        }
        Some(Value::Array(items)) => items
            .iter()
            .map(|item| {
                item.as_str()
                    .map(str::to_string)
                    .ok_or(MailboxError::InvalidBody)
            })
            .collect(),
        Some(_) => Err(MailboxError::InvalidBody),
    }
}
