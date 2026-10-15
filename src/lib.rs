//! # mentischat
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
//! this crate owns mailbox semantics: roster, caps, ack/batch delivery,
//! blocking pulls, and the peek endpoint for harness wake.

// The daemon is loopback-only by design, but the harness half of this crate
// (PTY, termios, process groups, SIGWINCH) has no Windows equivalent, so say
// so at compile time instead of failing with a wall of unresolved symbols.
#[cfg(not(unix))]
compile_error!(
    "mentischat targets Unix (macOS and Linux): the PTY harness, raw-mode \
     terminal handling and signal plumbing have no Windows implementation."
);

pub mod bus;
pub mod errors;
pub mod names;
pub mod nudge;
pub mod rest;
pub mod store;
pub mod tools;
pub mod tui;
pub mod wake;

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

use axum::extract::{ConnectInfo, DefaultBodyLimit, Request};
use axum::http::{header, HeaderMap, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use mcp::http::HttpServerConfig;
use mcp::streamable_http::{streamable_http_router, StreamableHttpConfig};

/// Server identity advertised during MCP `initialize`.
pub const SERVER_NAME: &str = "mentischat";
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
/// This is also where the request body limit lives, so it covers REST and
/// `/mcp` alike; `rest::router` deliberately carries none of its own.
///
/// The caller is responsible for binding the listener and serving the router
/// with `into_make_service_with_connect_info::<SocketAddr>()`, which both
/// the loopback middleware and the upstream MCP handlers need for IP checks.
pub fn build_router(bus: Arc<bus::Bus>) -> axum::Router {
    let transport = StreamableHttpConfig::new(SERVER_NAME, SERVER_VERSION)
        .with_endpoint_path("/")
        .with_instructions(tools::INSTRUCTIONS);

    // These two entries are the whole allowlist. Swallowing a rejection here
    // would silently ship a filter that does not say what we think it says,
    // so treat it as the build-time bug it would be.
    let mut ip_filter = mcp::IpFilter::new();
    for loopback in ["127.0.0.1", "::1"] {
        ip_filter
            .allow(loopback)
            .unwrap_or_else(|e| panic!("mentischat: IP filter rejected {loopback}: {e}"));
    }

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
        .layer(middleware::from_fn(loopback_origin))
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

/// Browser CSRF guard for REST (and defense-in-depth beside MCP's Origin
/// filter). Missing `Origin` is allowed (curl, MCP clients). A present
/// `Origin` must be a loopback http(s) URL.
async fn loopback_origin(headers: HeaderMap, request: Request, next: Next) -> Response {
    match headers.get(header::ORIGIN).and_then(|v| v.to_str().ok()) {
        None => next.run(request).await,
        Some(origin) if origin_is_loopback(origin) => next.run(request).await,
        Some(_) => StatusCode::FORBIDDEN.into_response(),
    }
}

fn origin_is_loopback(origin: &str) -> bool {
    let without_scheme = match origin {
        o if o.starts_with("http://") => &o["http://".len()..],
        o if o.starts_with("https://") => &o["https://".len()..],
        _ => return false,
    };
    let hostport = without_scheme.split('/').next().unwrap_or("");
    let host = if let Some(rest) = hostport.strip_prefix('[') {
        rest.split(']').next().unwrap_or("")
    } else {
        hostport.split(':').next().unwrap_or("")
    };
    matches!(host, "127.0.0.1" | "localhost" | "::1")
}
