//! # mentischat
//!
//! Local mailbox daemon that lets agents (opencode, Grok Build, Claude Code,
//! ChatGPT/Codex) message each other over MCP Streamable HTTP plus a plain
//! REST surface, served from a single loopback port. Mail is read when its
//! recipient calls `check_inbox`; an agent whose harness can watch a WebSocket
//! is woken by `GET /notify` (see [`notify`]) and then calls it.
//!
//! Agents register under names they choose. `check_inbox` hands each message
//! out once and marks it read; `history` shows an agent what it sent and has
//! read. Unread mail waits until it is read, read mail is kept a day after it
//! was read, idle agents leave the roster after a day, and a name that leaves
//! is held for its previous holder for ten minutes. Everything is wiped at
//! startup and on a clean stop.
//!
//! The MCP wire protocol is supplied by the [`mcp`] crate (`cloudllm_mcp`);
//! this crate owns mailbox semantics: roster, held names, caps, read
//! marking, history, expiry, and blocking pulls.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

// The data directory permissions (0700/0600) and SIGTERM handling use
// Unix-only APIs, so say so at compile time instead of failing with a wall of
// unresolved symbols.
#[cfg(not(unix))]
compile_error!("mentischat targets Unix (macOS and Linux) only");

pub mod bus;
#[cfg(feature = "e2e")]
pub mod client;
pub mod errors;
pub mod instructions;
pub mod names;
pub mod notify;
pub mod rest;
pub mod signed;
pub mod signed_api;
pub mod tools;
pub mod tui;
pub mod ws;

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

/// Environment variable that moves the listener to another loopback port.
///
/// The documented contract is the fixed [`BIND_ADDR`], and agents and clients
/// look for the daemon there, so the variable is for process-level tests and
/// embedding only: it never changes the host, which stays `127.0.0.1`, and `0`
/// asks the OS for a free port (the daemon logs the address it got).
pub const LISTEN_PORT_VAR: &str = "MENTISCHAT_LISTEN_PORT";

/// Why [`listen_addr`] refused the value of [`LISTEN_PORT_VAR`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListenPortError(pub String);

impl std::fmt::Display for ListenPortError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{LISTEN_PORT_VAR} must be a port number from 0 to 65535, got '{}'",
            self.0
        )
    }
}

impl std::error::Error for ListenPortError {}

/// The address the daemon binds: [`BIND_ADDR`], or the same loopback host on
/// the port named by [`LISTEN_PORT_VAR`] when `var` returns it.
///
/// `var` is the environment lookup (`|name| std::env::var(name).ok()` in the
/// daemon), injected so the rule is testable without touching the process
/// environment.
///
/// # Errors
///
/// [`ListenPortError`] when the variable is set to anything but a `u16`; an
/// empty value counts as unset.
///
/// # Examples
///
/// ```
/// use mentischat::{listen_addr, BIND_ADDR};
///
/// assert_eq!(listen_addr(|_| None).unwrap(), BIND_ADDR);
/// let moved = listen_addr(|_| Some("0".to_string())).unwrap();
/// assert_eq!(moved.port(), 0);
/// assert!(moved.ip().is_loopback());
/// assert!(listen_addr(|_| Some("http".to_string())).is_err());
/// ```
pub fn listen_addr(var: impl Fn(&str) -> Option<String>) -> Result<SocketAddr, ListenPortError> {
    match var(LISTEN_PORT_VAR).filter(|v| !v.is_empty()) {
        None => Ok(BIND_ADDR),
        Some(raw) => raw
            .parse::<u16>()
            .map(|port| SocketAddr::new(BIND_ADDR.ip(), port))
            .map_err(|_| ListenPortError(raw)),
    }
}

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
        .with_instructions(instructions::instructions(&bus.retention()));

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
        .layer(middleware::from_fn(loopback_headers))
        .layer(middleware::from_fn(loopback_only))
}

/// Reject anything whose peer address is not loopback. Bind is already
/// 127.0.0.1; this is defense in depth so a mis-bind cannot serve off-box on
/// either MCP or REST.
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

/// Browser guards for REST (and defense-in-depth beside MCP's own Origin
/// filter), each header checked only when present so curl and MCP clients
/// pass:
///
/// - `Origin` must be a loopback http(s) URL (cross-site requests / CSRF).
/// - `Host` must name a loopback host. A DNS-rebinding page reaches
///   127.0.0.1 under its own hostname and its same-origin `GET`s carry no
///   `Origin`, so only the `Host` header gives it away.
/// - `Sec-Fetch-Site`, when present, must be `same-origin` or `none`. A page
///   on another site that loads `http://127.0.0.1:9480/inboxes/...` as an
///   image sends a loopback `Host` and no `Origin`, but a `cross-site` fetch
///   site; without this it would mark that inbox's mail read, so its agent
///   would never get it from `check_inbox`.
async fn loopback_headers(headers: HeaderMap, request: Request, next: Next) -> Response {
    let present_and_foreign = |name: header::HeaderName, is_loopback: fn(&str) -> bool| {
        headers
            .get(name)
            .is_some_and(|v| !v.to_str().is_ok_and(is_loopback))
    };
    if present_and_foreign(header::ORIGIN, origin_is_loopback)
        || present_and_foreign(header::HOST, host_is_loopback)
        || present_and_foreign(SEC_FETCH_SITE, fetch_site_is_local)
    {
        return StatusCode::FORBIDDEN.into_response();
    }
    next.run(request).await
}

/// Set by browsers on every request. A page on another site can make a GET
/// with no `Origin` (an `<img>`, a link), so this is what tells such a request
/// apart; curl and agents do not send it.
const SEC_FETCH_SITE: header::HeaderName = header::HeaderName::from_static("sec-fetch-site");

/// `same-origin` (a page served by this daemon) and `none` (typed into the
/// address bar) are local; `cross-site` and `same-site` come from another page.
fn fetch_site_is_local(value: &str) -> bool {
    matches!(value, "same-origin" | "none")
}

fn origin_is_loopback(origin: &str) -> bool {
    let without_scheme = match origin {
        o if o.starts_with("http://") => &o["http://".len()..],
        o if o.starts_with("https://") => &o["https://".len()..],
        _ => return false,
    };
    host_is_loopback(without_scheme.split('/').next().unwrap_or(""))
}

/// `hostport` is `host`, `host:port`, `[v6]` or `[v6]:port`.
fn host_is_loopback(hostport: &str) -> bool {
    let host = if let Some(rest) = hostport.strip_prefix('[') {
        rest.split(']').next().unwrap_or("")
    } else {
        hostport.split(':').next().unwrap_or("")
    };
    matches!(host, "127.0.0.1" | "localhost" | "::1")
}
