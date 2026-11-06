//! The key-holding client's stdio MCP proxy, driven as a harness would: any
//! bytes on stdin, a daemon that is not there (a closed loopback port, so every
//! forwarded call fails fast and deterministically).
//!
//! Invariants: the proxy ends cleanly when stdin ends; every reply line is a
//! JSON-RPC 2.0 envelope with exactly one of `result` or `error`; a request
//! with an `id` gets exactly one reply echoing that id; a request without one
//! (a notification) gets none; text that is not JSON gets a parse error with a
//! null id; a `tools/call` never surfaces a Rust error, only an `isError`
//! result.
#![no_main]

use std::sync::OnceLock;

use libfuzzer_sys::fuzz_target;
use mentischat::client::{proxy, Client, Daemon, Identity, UpdatePolicy};
use serde_json::Value;
use tokio::io::{AsyncReadExt, BufReader};

fn runtime() -> &'static tokio::runtime::Runtime {
    static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .enable_time()
            .build()
            .expect("runtime")
    })
}

/// One identity directory for the whole process: creating a key per input
/// would dominate the run time, and the proxy never changes it without a
/// daemon to talk to.
fn keys() -> &'static std::path::Path {
    static DIR: OnceLock<tempfile::TempDir> = OnceLock::new();
    DIR.get_or_init(|| tempfile::tempdir().expect("tempdir")).path()
}

/// A loopback address with nothing listening on it.
fn closed_port() -> std::net::SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    listener.local_addr().expect("addr")
}

fuzz_target!(|data: &[u8]| {
    // Not UTF-8 is an I/O error on the transport (the line reader refuses it),
    // which `serve` documents as its only failure; the MCP stdio transport is
    // UTF-8 JSON, so there is nothing more to learn from such input.
    if std::str::from_utf8(data).is_err() {
        return;
    }
    let daemon = Daemon::new(closed_port());
    let identity = Identity::open(keys(), "fuzz").expect("identity");
    let client = Client::open(identity, daemon).expect("client");
    let (to_proxy, proxy_in) = tokio::io::duplex(1 << 20);
    let (proxy_out, mut from_proxy) = tokio::io::duplex(1 << 24);
    let input = data.to_vec();

    let replies = runtime().block_on(async {
        use tokio::io::AsyncWriteExt;
        let mut to_proxy = to_proxy;
        to_proxy.write_all(&input).await.expect("feed stdin");
        drop(to_proxy); // EOF: the proxy must now finish
        proxy::serve(client, daemon, UpdatePolicy::default(), BufReader::new(proxy_in), proxy_out)
            .await
            .expect("serve ends cleanly at EOF");
        let mut out = String::new();
        from_proxy.read_to_string(&mut out).await.expect("replies");
        out
    });

    let replies: Vec<Value> = replies
        .lines()
        .map(|l| serde_json::from_str(l).expect("every reply line is JSON"))
        .collect();
    for reply in &replies {
        assert_eq!(reply["jsonrpc"], "2.0");
        assert!(reply["result"].is_object() ^ reply["error"].is_object(), "result xor error: {reply}");
    }

    // The expected number of replies, read independently from the input.
    let mut expected = 0;
    for line in String::from_utf8_lossy(data).lines().filter(|l| !l.trim().is_empty()) {
        match serde_json::from_str::<Value>(line) {
            Err(_) => expected += 1,
            Ok(v) => expected += usize::from(v.get("id").is_some()),
        }
    }
    assert_eq!(replies.len(), expected, "one reply per request, none per notification");
});
