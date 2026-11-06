//! The whole HTTP surface: an arbitrary request (method, path and query,
//! headers that browsers send, body) through `build_router` on a primed bus.
//! This is the daemon's real front door, so the invariants are the contract:
//!
//! - no panic, no 5xx other than the documented 503 (shutting down) and 507
//!   (storage full);
//! - the loopback guards agree with a reference reading of the documented
//!   rule: a request is refused with 403 exactly when its peer is not
//!   loopback or its `Host`, `Origin` or `Sec-Fetch-Site` names somewhere
//!   foreign, and a refused request never reaches a handler (no side effect
//!   is visible afterwards);
//! - every other REST failure is a JSON object whose `error` is a documented
//!   code (axum's own plain-text rejections must not leak through), except
//!   `/mcp` and `/ws`, which have their own framing.
//!
//! Input: byte 0 picks the method, byte 1 the peer and header flavour, then
//! the request target up to the first NUL, then the body. The seeds are the
//! README's requests, so mutations start inside real routes.
#![no_main]

use std::net::SocketAddr;
use std::sync::{Arc, OnceLock};

use axum::body::Body;
use axum::extract::ConnectInfo;
use axum::http::{header, Method, Request, StatusCode};
use libfuzzer_sys::fuzz_target;
use mentischat::bus::Bus;
use tower::ServiceExt;

const METHODS: [Method; 7] = [
    Method::GET,
    Method::POST,
    Method::PUT,
    Method::DELETE,
    Method::HEAD,
    Method::PATCH,
    Method::OPTIONS,
];

/// Every `error` code in the wire contract.
const CODES: [&str; 33] = [
    "invalid_name",
    "invalid_body",
    "invalid_wait",
    "invalid_description",
    "invalid_cursor",
    "invalid_limit",
    "name_taken",
    "numbered_name",
    "unregistered",
    "queue_full",
    "storage_full",
    "roster_full",
    "too_many_waiters",
    "invalid_room",
    "room_not_found",
    "room_exists",
    "not_a_member",
    "rooms_full",
    "too_many_rooms",
    "room_full",
    "signature_required",
    "proof_required",
    "invalid_signature",
    "invalid_key",
    "key_required",
    "key_conflict",
    "stale_counter",
    "stale_epoch",
    "no_key_package",
    "rate_limited",
    "not_found",
    "method_not_allowed",
    "shutting_down",
];

fn runtime() -> &'static tokio::runtime::Runtime {
    static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
            .expect("runtime")
    })
}

/// Header and peer flavours, picked by one byte: whether the peer is
/// loopback and what, if anything, the three browser headers say.
struct Flavour {
    peer: SocketAddr,
    host: Option<&'static str>,
    origin: Option<&'static str>,
    fetch_site: Option<&'static str>,
}

impl Flavour {
    fn from_byte(b: u8) -> Self {
        let pick = |n: u8, options: &[Option<&'static str>]| options[usize::from(n) % options.len()];
        Self {
            peer: if b & 0x80 == 0 {
                "127.0.0.1:50000".parse().expect("addr")
            } else {
                "192.0.2.7:50000".parse().expect("addr")
            },
            host: pick(b & 3, &[None, Some("127.0.0.1:9480"), Some("localhost"), Some("evil.example")]),
            origin: pick(
                (b >> 2) & 3,
                &[None, Some("http://127.0.0.1:9480"), Some("https://[::1]:1"), Some("http://evil.example")],
            ),
            fetch_site: pick(
                (b >> 4) & 3,
                &[None, Some("same-origin"), Some("none"), Some("cross-site")],
            ),
        }
    }

    /// The documented rule, written independently of `lib.rs`.
    fn refused(&self) -> bool {
        let foreign = |h: Option<&str>| h.is_some_and(|v| v.contains("evil"));
        !self.peer.ip().is_loopback()
            || foreign(self.host)
            || foreign(self.origin)
            || self.fetch_site == Some("cross-site")
    }
}

fuzz_target!(|data: &[u8]| {
    let [method, flavour, rest @ ..] = data else {
        return;
    };
    let (target, body) = match rest.iter().position(|&b| b == 0) {
        Some(i) => (&rest[..i], &rest[i + 1..]),
        None => (rest, &[][..]),
    };
    let Ok(target) = std::str::from_utf8(target) else {
        return;
    };
    let flavour = Flavour::from_byte(*flavour);
    let mut builder = Request::builder()
        .method(METHODS[usize::from(*method) % METHODS.len()].clone())
        .uri(format!("/{}", target.trim_start_matches('/')));
    for (name, value) in [
        (header::HOST, flavour.host),
        (header::ORIGIN, flavour.origin),
        (header::HeaderName::from_static("sec-fetch-site"), flavour.fetch_site),
    ] {
        if let Some(value) = value {
            builder = builder.header(name, value);
        }
    }
    let Ok(mut request) = builder
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body.to_vec()))
    else {
        return; // a target that is not a URI never reaches the router
    };
    request.extensions_mut().insert(ConnectInfo(flavour.peer));
    let path = request.uri().path().to_string();

    runtime().block_on(async {
        // The data directory does not exist, so nothing is restored or written.
        let bus = Arc::new(Bus::new(std::path::Path::new("/nonexistent/mentischat-fuzz")).expect("bus"));
        let app = mentischat::build_router(bus.clone());
        for name in ["a", "b"] {
            bus.register(&mentischat::names::AgentName::parse(name).expect("name"))
                .await
                .expect("priming register");
        }
        let before = bus.stats().await.expect("stats");

        let response = app.oneshot(request).await.expect("the router is infallible");
        let status = response.status();
        let is_json = response
            .headers()
            .get(header::CONTENT_TYPE)
            .is_some_and(|v| v.as_bytes().starts_with(b"application/json"));
        let bytes = axum::body::to_bytes(Body::new(response.into_body()), 1 << 24)
            .await
            .unwrap_or_default();

        if flavour.refused() {
            assert_eq!(status, StatusCode::FORBIDDEN, "a foreign request was served: {path}");
            assert_eq!(bus.stats().await.expect("stats"), before, "a refused request had an effect");
            return;
        }
        assert!(
            !status.is_server_error() || matches!(status.as_u16(), 503 | 507),
            "{status} for {path}"
        );
        let framed_elsewhere = path == "/mcp" || path.starts_with("/mcp/") || path == "/ws";
        if status.as_u16() >= 400 && !framed_elsewhere && !bytes.is_empty() {
            assert!(is_json, "{status} for {path} is not JSON: {bytes:?}");
            let value: serde_json::Value = serde_json::from_slice(&bytes).expect("error body is JSON");
            let code = value["error"].as_str().expect("error is a string");
            assert!(CODES.contains(&code), "unknown code {code:?} for {path}");
        }
    });
});
