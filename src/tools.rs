//! The five mailbox tools exposed over MCP.
//!
//! One [`ToolProtocol`] implementation dispatches by tool name; schemas stay
//! primitive (no regex patterns) so local GGUF/OpenCode grammars keep
//! compiling. Domain failures return `Ok(ToolResult::failure(..))` so
//! harnesses render an `isError: true` content block carrying the contract
//! error JSON; Rust `Err` is reserved for malformed *requests* (upstream
//! turns those into JSON-RPC `-32602`).

use std::sync::Arc;

use mcp::protocol::{
    ToolError, ToolMetadata, ToolParameter, ToolParameterType, ToolProtocol, ToolResult,
};
use serde::Serialize;
use serde_json::{json, Value};

use crate::bus::{Bus, Registration, Retention};
use crate::errors::MailboxError;
use crate::names::{AgentName, Provider};

/// Ritual returned in the MCP `initialize` result so every harness sees the
/// same instructions; [`instructions`] appends the retention line.
pub const INSTRUCTIONS: &str = "\
mentischat is a local mailbox between agents on this machine.
Register with provider set to where you are running, the harness or app (claude_code, claude_desktop, cursor, codex, grok_build, grok_desktop), and description set to your model, e.g. claude sonnet 5.5 or grok 4.6. The server assigns your name: the next {provider}_{n}, e.g. claude_code_1, then claude_code_2 for a second instance; numbers are not reused while the server runs. Use the agent field in the reply as your name for every later call. Pass agent (a name of your choosing) only when the user asks for a specific name.
Before send_message, call list_agents. Only send to names that appear there; each entry shows the agent's description and how many messages are queued for it. from must be your registered name. When you need a reply, say so in the body so the recipient responds when done.
Nothing wakes an idle agent: a message is read only when its recipient calls check_inbox, usually because the user asked it to check its messages. After send_message, end your turn unless a peer is answering within the same turn, in which case call check_inbox with wait_ms > 0 to wait for the reply.
check_inbox deletes the messages it returns: each is delivered once and nothing is kept, so quote the bodies in your reply right away. If remaining > 0, check_inbox again (wait_ms 0) until remaining is 0.
Call deregister only when the user says they are done.";

/// [`INSTRUCTIONS`] plus how long unread mail and idle agents are kept, in
/// the units the operator configured.
pub fn instructions(retention: &Retention) -> String {
    format!(
        "{INSTRUCTIONS}\nUnread messages expire {} after they are sent. An agent that has not called register, send_message or check_inbox for {} is dropped from the roster with its unread mail; if a call fails unregistered, register again and use the name it returns.",
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
                "Join the local mentischat roster. Pass provider, where you are running (claude_code, claude_desktop, cursor, codex, grok_build, grok_desktop), and description, your model (e.g. claude sonnet 5.5). The server assigns the next {provider}_{n}, e.g. claude_code_1, and returns it in agent. Use that name for every later call. Pass agent only when the user asks for a specific name: it claims that exact name, or refreshes it if it exists, and anyone else using it shares its inbox.",
            )
            .with_parameter(
                ToolParameter::new("provider", ToolParameterType::String).with_description(
                    "The harness or app you are running in, e.g. claude_code, grok_build, claude_desktop; the server appends the number. Required unless agent is given",
                ),
            )
            .with_parameter(
                ToolParameter::new("agent", ToolParameterType::String).with_description(
                    "A name the user asked you to use; omit it to get the default {provider}_{n}",
                ),
            )
            .with_parameter(
                ToolParameter::new("description", ToolParameterType::String).with_description(
                    "Your model, e.g. claude sonnet 5.5 or grok 4.6 (max 256 bytes); shown to peers in list_agents. Omit to keep the current one, empty string clears it",
                ),
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
        let outcome: Result<Value, MailboxError> = match tool_name {
            "register" => self.register(&parameters).await,
            "deregister" => self.deregister(&parameters).await,
            "send_message" => self.send_message(&parameters).await,
            "check_inbox" => self.check_inbox(&parameters).await,
            "list_agents" => self.list_agents().await,
            other => return Err(unknown_tool(other)),
        };
        // Logged after the match, so only the five known names reach the log.
        tracing::info!(tool = tool_name, "mcp tool");
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
        metadata_for(tool_name).ok_or_else(|| unknown_tool(tool_name))
    }

    fn protocol_name(&self) -> &str {
        "mentischat.mailbox"
    }
}

impl MailboxProtocol {
    async fn register(&self, params: &Value) -> Result<Value, MailboxError> {
        register_agent(&self.bus, params).await
    }

    async fn deregister(&self, params: &Value) -> Result<Value, MailboxError> {
        let agent = required_name(params, "agent")?;
        let out = self.bus.deregister(&agent).await?;
        Ok(to_wire(out))
    }

    async fn send_message(&self, params: &Value) -> Result<Value, MailboxError> {
        send_message(&self.bus, params).await
    }

    async fn check_inbox(&self, params: &Value) -> Result<Value, MailboxError> {
        let agent = required_name(params, "agent")?;
        let wait_ms = parse_wait_ms(params.get("wait_ms"))?;
        let page = self.bus.check_inbox(&agent, wait_ms).await?;
        Ok(to_wire(page))
    }

    async fn list_agents(&self) -> Result<Value, MailboxError> {
        let agents = self.bus.list_agents().await?;
        Ok(json!({ "agents": agents }))
    }
}

/// Pull a required name field: missing or empty is `invalid_name`, a
/// non-string value is `invalid_body`.
fn required_name(params: &Value, key: &str) -> Result<AgentName, MailboxError> {
    match params.get(key) {
        None | Some(Value::Null) => AgentName::parse(""),
        Some(Value::String(raw)) => AgentName::parse(raw),
        Some(_) => Err(MailboxError::InvalidBody),
    }
}

/// The error for a tool name this server does not have: upstream maps
/// [`ToolError::NotFound`] to JSON-RPC `-32602`. The name is shortened, so a
/// huge one is not echoed back in full.
fn unknown_tool(name: &str) -> Box<dyn std::error::Error + Send + Sync> {
    Box::new(ToolError::NotFound(crate::names::echo(name)))
}

/// Register per the tool and `POST /agents` contract: a non-null `agent`
/// claims or refreshes that exact name; otherwise `provider` is assigned the
/// next `{provider}_{n}`. Neither is `invalid_body`.
pub async fn register_agent(bus: &Bus, params: &Value) -> Result<Value, MailboxError> {
    let opts = Registration {
        description: parse_description(params.get("description"))?,
    };
    let present = |key: &str| params.get(key).is_some_and(|v| !v.is_null());
    let out = if present("agent") {
        bus.register_with(&required_name(params, "agent")?, opts)
            .await?
    } else if present("provider") {
        let raw = params
            .get("provider")
            .and_then(Value::as_str)
            .ok_or(MailboxError::InvalidBody)?;
        bus.register_provider(&Provider::parse(raw)?, opts).await?
    } else {
        return Err(MailboxError::InvalidBody);
    };
    Ok(to_wire(out))
}

/// Queue a message per the tool and `POST /messages` contract. `from` is
/// validated before `to`, so when both are bad the error names `from`.
pub async fn send_message(bus: &Bus, params: &Value) -> Result<Value, MailboxError> {
    let from = required_name(params, "from")?;
    let to = required_name(params, "to")?;
    let body = params
        .get("body")
        .and_then(Value::as_str)
        .ok_or(MailboxError::InvalidBody)?
        .to_string();
    Ok(to_wire(bus.send(&from, &to, body).await?))
}

/// A bus result as its JSON wire shape.
pub(crate) fn to_wire(value: impl Serialize) -> Value {
    serde_json::to_value(value).expect("wire types are always serializable")
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
