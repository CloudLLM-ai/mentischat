//! The six mailbox tools exposed over MCP.
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

use crate::bus::{Bus, Registration};
use crate::errors::MailboxError;
use crate::names::{AgentName, Provider};

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
                "Join the local mentischat roster under a name of your own (one word; case does not matter), e.g. a star, constellation or moon such as Vega or Orion; list_agents suggests free ones. Also pass provider, where you are running (claude_code, claude_desktop, cursor, codex, grok_build, grok_desktop), and description, your model (e.g. claude sonnet 5.5). A name in use or recently released fails name_taken with suggestions. Pass reclaim: true only to take back your own name.",
            )
            .with_parameter(str_param(
                "agent",
                "The name you want: 1-63 letters, digits, '.', '_' or '-', starting with a letter or digit",
            ))
            .with_parameter(
                ToolParameter::new("provider", ToolParameterType::String).with_description(
                    "The harness or app you are running in, e.g. claude_code, grok_build, claude_desktop. Omit to keep the current one",
                ),
            )
            .with_parameter(
                ToolParameter::new("description", ToolParameterType::String).with_description(
                    "Your model, e.g. claude sonnet 5.5 or grok 4.6 (max 256 bytes); shown to peers in list_agents. Omit to keep the current one, empty string clears it",
                ),
            )
            .with_parameter(
                ToolParameter::new("reclaim", ToolParameterType::Boolean)
                    .with_description(
                        "true to take back a name you registered earlier even though it is registered or held; it keeps that name's mail and history",
                    )
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
                "Queue a message for a registered agent. Fails if from or to is not registered. Start the body with a reply marker: \"No reply needed\", \"Reply when you can\" or \"Waiting for your reply\". Nothing wakes the recipient: it reads the message when it next calls check_inbox. When you need the answer now, follow with check_inbox using wait_ms 30000.",
            )
            .with_parameter(str_param("from", "Your registered name"))
            .with_parameter(str_param("to", "Recipient's registered name (must be on the roster)"))
            .with_parameter(str_param(
                "body",
                "Message text starting with a reply marker (max 64 KiB, non-empty)",
            )),
        ),
        "check_inbox" => Some(
            ToolMetadata::new(
                "check_inbox",
                "Return the next batch of unread messages for this registered name and mark them read: check_inbox returns each message once (history shows it again). wait_ms 0 just checks. While waiting for a reply, use wait_ms 30000 and call again after an empty result, up to 10 times in a row. If remaining > 0, call again with wait_ms 0.",
            )
            .with_parameter(str_param("agent", "Your registered name"))
            .with_parameter(
                ToolParameter::new("wait_ms", ToolParameterType::Integer)
                    .with_description("Block up to this many ms until mail arrives (0-30000): 30000 while waiting for a reply, 0 to just check")
                    .with_default(json!(0)),
            ),
        ),
        "history" => Some(
            ToolMetadata::new(
                "history",
                "Messages you sent (with readAt once read) and messages you have read, oldest first, up to 8 per page. Use it to look back at a conversation. Pass next from the reply as before to get older messages.",
            )
            .with_parameter(str_param("agent", "Your registered name"))
            .with_parameter(
                ToolParameter::new("with", ToolParameterType::String)
                    .with_description("Only messages exchanged with this agent; omit for all"),
            )
            .with_parameter(
                ToolParameter::new("before", ToolParameterType::Integer).with_description(
                    "The next value from a previous history reply, for older messages; omit for the newest",
                ),
            ),
        ),
        "list_agents" => Some(ToolMetadata::new(
            "list_agents",
            "Names on the roster, each with its provider, description, when it was last seen, and how many unread messages are queued for it, plus suggestions: free names you can register. Call before register and before send_message.",
        )),
        _ => None,
    }
}

const TOOL_NAMES: [&str; 6] = [
    "register",
    "deregister",
    "send_message",
    "check_inbox",
    "history",
    "list_agents",
];

#[async_trait::async_trait]
impl ToolProtocol for MailboxProtocol {
    async fn execute(
        &self,
        tool_name: &str,
        parameters: Value,
    ) -> Result<ToolResult, Box<dyn std::error::Error + Send + Sync>> {
        let bus = &self.bus;
        let outcome: Result<Value, MailboxError> = match tool_name {
            "register" => register_agent(bus, &parameters).await,
            "deregister" => deregister(bus, &parameters).await,
            "send_message" => send_message(bus, &parameters).await,
            "check_inbox" => check_inbox(bus, &parameters).await,
            "history" => history(bus, &parameters).await,
            "list_agents" => bus.list_agents().await.map(to_wire),
            other => return Err(unknown_tool(other)),
        };
        // Logged after the match, so only known tool names reach the log.
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

/// Leave the roster per the tool contract.
async fn deregister(bus: &Bus, params: &Value) -> Result<Value, MailboxError> {
    let agent = required_name(params, "agent")?;
    Ok(to_wire(bus.deregister(&agent).await?))
}

/// Pull the next batch per the tool contract.
async fn check_inbox(bus: &Bus, params: &Value) -> Result<Value, MailboxError> {
    let agent = required_name(params, "agent")?;
    let wait_ms = parse_wait_ms(params.get("wait_ms"))?;
    Ok(to_wire(bus.check_inbox(&agent, wait_ms).await?))
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

/// Register per the tool and `POST /agents` contract: `agent` is the name
/// to claim; `provider`, `description` and `reclaim` are optional.
pub async fn register_agent(bus: &Bus, params: &Value) -> Result<Value, MailboxError> {
    let name = required_name(params, "agent")?;
    let opts = Registration {
        description: parse_description(params.get("description"))?,
        provider: parse_provider(params.get("provider"))?,
        reclaim: parse_reclaim(params.get("reclaim"))?,
    };
    Ok(to_wire(bus.register_with(&name, opts).await?))
}

/// One history page per the tool contract: `agent` is required, `with` and
/// `before` optional.
pub async fn history(bus: &Bus, params: &Value) -> Result<Value, MailboxError> {
    let agent = required_name(params, "agent")?;
    let with = optional_name(params.get("with"))?;
    let before = parse_cursor(params.get("before"))?;
    Ok(to_wire(bus.history(&agent, with.as_ref(), before).await?))
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

/// Contract provider semantics: absent/null -> keep the stored one; a string
/// must be a valid [`Provider`] (`invalid_name` otherwise); any other JSON
/// type -> `invalid_body`.
pub fn parse_provider(v: Option<&Value>) -> Result<Option<Provider>, MailboxError> {
    match v {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(s)) => Provider::parse(s).map(Some),
        Some(_) => Err(MailboxError::InvalidBody),
    }
}

/// Contract reclaim semantics: absent/null -> false; a boolean, or the
/// string `"true"` / `"false"` (some local models quote booleans) -> its
/// value; anything else -> `invalid_body`.
pub fn parse_reclaim(v: Option<&Value>) -> Result<bool, MailboxError> {
    match v {
        None | Some(Value::Null) => Ok(false),
        Some(Value::Bool(b)) => Ok(*b),
        Some(Value::String(s)) if s == "true" => Ok(true),
        Some(Value::String(s)) if s == "false" => Ok(false),
        Some(_) => Err(MailboxError::InvalidBody),
    }
}

/// An optional name field such as `with`: absent, null or `""` -> `None`; a
/// string -> a validated name; any other JSON type -> `invalid_body`.
pub fn optional_name(v: Option<&Value>) -> Result<Option<AgentName>, MailboxError> {
    match v {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(s)) if s.is_empty() => Ok(None),
        Some(Value::String(s)) => AgentName::parse(s).map(Some),
        Some(_) => Err(MailboxError::InvalidBody),
    }
}

/// Contract `before` semantics: absent, null or `""` -> `None`; a positive
/// integer, or a string of digits holding one -> its value; anything else
/// -> `invalid_cursor`.
pub fn parse_cursor(v: Option<&Value>) -> Result<Option<i64>, MailboxError> {
    let parsed = match v {
        None | Some(Value::Null) => return Ok(None),
        Some(Value::String(s)) if s.is_empty() => return Ok(None),
        Some(Value::Number(n)) => n.as_i64(),
        Some(Value::String(s)) if s.bytes().all(|b| b.is_ascii_digit()) => s.parse().ok(),
        Some(_) => None,
    };
    parsed
        .filter(|c| *c > 0)
        .map(Some)
        .ok_or(MailboxError::InvalidCursor)
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
