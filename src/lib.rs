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

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

use axum::extract::{ConnectInfo, DefaultBodyLimit, Request};
use axum::http::StatusCode;
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use mcp::http::HttpServerConfig;
use mcp::streamable_http::{streamable_http_router, StreamableHttpConfig};

/// Server identity advertised during MCP `initialize`.
pub const SERVER_NAME: &str = "llm_chat";
/// Server version advertised during MCP `initialize`.
pub const SERVER_VERSION: &str = env!("CARGO_PKG_VERSION");
/// Path of the MCP Streamable HTTP endpoint.
pub const MCP_ENDPOINT_PATH: &str = "/mcp";
/// Loopback listener the process binds. Also the single-instance lock.
pub const BIND_ADDR: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9480);

/// Compose the full application [`axum::Router`]: REST on the outer router
/// with the upstream MCP router nested at [`MCP_ENDPOINT_PATH`].
///
/// Nesting is required: `streamable_http_router` registers a root catch-all
/// `/{*rest}` that would otherwise answer unknown REST paths with 405
/// instead of the contract `not_found` body.
///
/// The caller is responsible for binding the listener and serving the router
/// with `into_make_service_with_connect_info::<SocketAddr>()`, which both
/// the loopback middleware and the upstream MCP handlers need for IP checks.
pub fn build_router(bus: Arc<bus::Bus>) -> axum::Router {
    let transport = StreamableHttpConfig::new(SERVER_NAME, SERVER_VERSION)
        .with_endpoint_path("/")
        .with_instructions(tools::INSTRUCTIONS);

    let mut ip_filter = mcp::IpFilter::new();
    let _ = ip_filter.allow("127.0.0.1");
    let _ = ip_filter.allow("::1");

    let http_config = HttpServerConfig {
        addr: BIND_ADDR,
        bearer_token: None,
        bearer_authorizer: None,
        ip_filter,
        event_handler: None,
    };

    let mailbox = Arc::new(tools::MailboxProtocol::new(bus.clone()));
    let mcp_router = streamable_http_router(&http_config, &transport, mailbox as _);

    rest::router(bus)
        .nest(MCP_ENDPOINT_PATH, mcp_router)
        .layer(DefaultBodyLimit::max(1024 * 1024))
        .layer(middleware::from_fn(loopback_only))
}

/// Reject anything whose peer address is not loopback. Bind is already
/// 127.0.0.1; this is the PLAN defense-in-depth so a mis-bind cannot serve
/// off-box on either MCP or REST.
async fn loopback_only(
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    request: Request,
    next: Next,
) -> Response {
    if addr.ip().is_loopback() {
        next.run(request).await
    } else {
        StatusCode::FORBIDDEN.into_response()
    }
}
