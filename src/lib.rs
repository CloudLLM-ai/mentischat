//! # llm_chat
//!
//! Local mailbox daemon that lets TUI agents (opencode, Grok Build, Claude
//! Code, ChatGPT/Codex) message each other over MCP Streamable HTTP plus a
//! plain REST surface, served from a single loopback port.
//!
//! The daemon is a pipe, not a log: undelivered mail persists in sqlite
//! (`pending` / `in_flight`) until the recipient acks, then the row is
//! deleted. There is no history API.
//!
//! The MCP wire protocol is supplied by the [`mcp`] crate (`cloudllm_mcp`);
//! this crate owns mailbox semantics: roster, caps, ack/batch delivery, and
//! blocking pulls.

pub mod bus;
pub mod errors;
pub mod names;
pub mod rest;
pub mod store;
pub mod tools;

use std::net::SocketAddr;
use std::sync::Arc;

use mcp::http::HttpServerConfig;
use mcp::streamable_http::{streamable_http_router, StreamableHttpConfig};

/// Server identity advertised during MCP `initialize`.
pub const SERVER_NAME: &str = "llm_chat";
/// Server version advertised during MCP `initialize`.
pub const SERVER_VERSION: &str = env!("CARGO_PKG_VERSION");
/// Path of the MCP Streamable HTTP endpoint.
pub const MCP_ENDPOINT_PATH: &str = "/mcp";

/// Compose the full application [`axum::Router`]: the upstream MCP router
/// mounted at [`MCP_ENDPOINT_PATH`] merged with the REST surface.
///
/// The caller is responsible for binding the listener and serving the router
/// with `into_make_service_with_connect_info::<SocketAddr>()`, which the
/// upstream router requires for IP filtering.
pub fn build_router(bus: Arc<bus::Bus>) -> axum::Router {
    let transport = StreamableHttpConfig::new(SERVER_NAME, SERVER_VERSION)
        .with_endpoint_path(MCP_ENDPOINT_PATH)
        .with_instructions(tools::INSTRUCTIONS);

    let http_config = HttpServerConfig {
        addr: SocketAddr::from(([127, 0, 0, 1], 0)),
        bearer_token: None,
        bearer_authorizer: None,
        ip_filter: mcp::IpFilter::new(),
        event_handler: None,
    };

    let mailbox = Arc::new(tools::MailboxProtocol::new(bus.clone()));
    let mcp_router = streamable_http_router(&http_config, &transport, mailbox as _);

    mcp_router.merge(rest::router(bus))
}
