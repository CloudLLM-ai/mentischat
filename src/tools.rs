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

use crate::bus::Bus;
use crate::errors::MailboxError;
use crate::names::AgentName;

/// Ritual returned in the MCP `initialize` result so every harness sees the
/// same instructions.
pub const INSTRUCTIONS: &str = "\
llm_chat is a local mailbox between TUI agents on this machine.
You are not on the roster until you call register with the exact name the user gave you. Do not invent a name.
Before send_message, call list_agents. Only send to names that appear there. from must be your registered name.
After send_message, call check_inbox with wait_ms 15000 so a reply can arrive in this turn.
When check_inbox returns messages, quote the bodies in your reply. Pass those ids as ack_ids on the next check_inbox. If remaining > 0, check_inbox again until remaining is 0.
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
                "Claim this agent name on the local llm_chat roster. Idempotent. Pass the exact name the user told you to use.",
            )
            .with_parameter(str_param("agent", "The exact roster name to claim")),
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
                "Queue a message for a registered agent. Fails if from or to is not registered. After sending, call check_inbox with wait_ms so a reply can arrive.",
            )
            .with_parameter(str_param("from", "Your registered name"))
            .with_parameter(str_param("to", "Recipient's registered name (must be on the roster)"))
            .with_parameter(str_param("body", "Message text (max 64 KiB, non-empty)")),
        ),
        "check_inbox" => Some(
            ToolMetadata::new(
                "check_inbox",
                "Return the next batch of messages for this registered name (at-least-once). Optional wait_ms (0-30000). Optional ack_ids deletes previously returned in-flight messages. Quote bodies, then ack those ids on the next call. If remaining > 0, call again.",
            )
            .with_parameter(str_param("agent", "Your registered name"))
            .with_parameter(
                ToolParameter::new("wait_ms", ToolParameterType::Integer)
                    .with_description("Block up to this many milliseconds waiting for mail (0-30000)")
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
            "Names currently on the roster and when each was last seen (register or check_inbox).",
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
        Ok(TOOL_NAMES.iter().filter_map(|n| metadata_for(n)).collect())
    }

    async fn get_tool_metadata(
        &self,
        tool_name: &str,
    ) -> Result<ToolMetadata, Box<dyn std::error::Error + Send + Sync>> {
        metadata_for(tool_name).ok_or_else(|| format!("unknown tool: {tool_name}").into())
    }

    fn protocol_name(&self) -> &str {
        "llm_chat.mailbox"
    }
}

impl MailboxProtocol {
    async fn register(&self, params: &Value) -> Result<Value, MailboxError> {
        let agent = required_name(params, "agent")?;
        let out = self.bus.register(&agent).await?;
        Ok(json!({ "agent": out.agent, "created": out.created, "lastSeen": out.last_seen }))
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
        serde_json::to_value(page).map_err(|_| MailboxError::InvalidBody)
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
/// ids; anything else -> `invalid_body`.
pub fn parse_ack_ids(v: Option<&Value>) -> Result<Vec<String>, MailboxError> {
    match v {
        None | Some(Value::Null) => Ok(Vec::new()),
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
