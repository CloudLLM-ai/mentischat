//! The client's loopback HTTP: reply parsing (including hostile replies from
//! something that is not the daemon) and `Daemon::call` against a scripted
//! server on a port the OS chose.
#![cfg(feature = "e2e")]

use std::net::SocketAddr;

use mentischat::client::http::{parse_reply, query_escape};
use mentischat::client::{ClientError, Daemon};
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// A one-shot server on `127.0.0.1:0` that reads the request, answers with the
/// raw bytes `reply`, and returns the request it saw.
async fn serve_once(reply: &'static [u8]) -> (SocketAddr, tokio::task::JoinHandle<String>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let seen = tokio::spawn(async move {
        let (mut conn, _) = listener.accept().await.expect("accept");
        let mut buf = vec![0u8; 8192];
        let n = conn.read(&mut buf).await.expect("read");
        conn.write_all(reply).await.expect("write");
        conn.shutdown().await.expect("shutdown");
        String::from_utf8_lossy(&buf[..n]).into_owned()
    });
    (addr, seen)
}

/// A content-length reply, a chunked reply and a bodyless reply all parse; the
/// bodyless one is `null`.
#[test]
fn test_parse_reply_forms() {
    let plain = parse_reply(b"HTTP/1.1 200 OK\r\ncontent-length: 7\r\n\r\n{\"a\":1}").unwrap();
    assert_eq!(plain, (200, json!({ "a": 1 })));

    let chunked = parse_reply(
        b"HTTP/1.1 201 Created\r\nTransfer-Encoding: Chunked\r\n\r\n3\r\n{\"a\r\n4;ext=1\r\n\":2}\r\n0\r\n\r\n",
    )
    .unwrap();
    assert_eq!(chunked, (201, json!({ "a": 2 })));

    assert_eq!(
        parse_reply(b"HTTP/1.1 204 No Content\r\n\r\n").unwrap(),
        (204, json!(null))
    );
}

/// Malformed replies are typed transport errors, never panics: no header end,
/// non-UTF-8 header, bad status line, bad chunk framing, non-JSON body.
#[test]
fn test_parse_reply_rejects_malformed_input() {
    let cases: [&[u8]; 9] = [
        b"",
        b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n",
        b"HTTP/1.1 200 OK\xff\r\n\r\n{}",
        b"HTTP/1.1\r\n\r\n{}",
        b"HTTP/1.1 abc OK\r\n\r\n{}",
        b"HTTP/1.1 99999 OK\r\n\r\n{}",
        b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\nzz\r\n{}\r\n0\r\n\r\n",
        b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n10\r\nshort\r\n",
        b"HTTP/1.1 200 OK\r\n\r\nnot json",
    ];
    for raw in cases {
        let err = parse_reply(raw).unwrap_err();
        assert!(matches!(err, ClientError::Transport(_)), "{raw:?}: {err}");
        assert!(err.to_string().contains("malformed reply"), "{err}");
    }
}

/// Unreserved characters pass; everything else, including multi-byte UTF-8,
/// is percent-encoded byte by byte.
#[test]
fn test_query_escape() {
    assert_eq!(query_escape("AZaz09-_.~"), "AZaz09-_.~");
    assert_eq!(query_escape("a b&c=d/é"), "a%20b%26c%3Dd%2F%C3%A9");
    assert_eq!(query_escape(""), "");
}

/// `Daemon::local` is the documented address and `new` keeps what it is given.
#[test]
fn test_daemon_addresses() {
    assert_eq!(Daemon::local().addr(), mentischat::BIND_ADDR);
    let addr: SocketAddr = "127.0.0.1:1".parse().unwrap();
    assert_eq!(Daemon::new(addr).addr(), addr);
}

/// A 2xx reply is its JSON, and the request on the wire carries the method,
/// path, Host, and the JSON body with its exact length.
#[tokio::test]
async fn test_call_sends_the_request_and_returns_the_body() {
    let (addr, seen) =
        serve_once(b"HTTP/1.1 200 OK\r\ncontent-length: 11\r\n\r\n{\"ok\":true}").await;
    let body = json!({ "agent": "Vega" });
    let reply = Daemon::new(addr)
        .call("POST", "/agents", Some(&body))
        .await
        .unwrap();
    assert_eq!(reply, json!({ "ok": true }));

    let request = seen.await.unwrap();
    assert!(
        request.starts_with("POST /agents HTTP/1.1\r\n"),
        "{request}"
    );
    assert!(request.contains(&format!("Host: {addr}")), "{request}");
    assert!(request.contains("Content-Length: 16\r\n"), "{request}");
    assert!(request.ends_with(r#"{"agent":"Vega"}"#), "{request}");
}

/// A non-2xx reply becomes a daemon error with the contract code and body; one
/// without an `error` field reports `unknown`.
#[tokio::test]
async fn test_call_maps_error_statuses() {
    let (addr, _) = serve_once(
        b"HTTP/1.1 409 Conflict\r\ncontent-length: 26\r\n\r\n{\"error\":\"name_taken\",\"x\":1}",
    )
    .await;
    let err = Daemon::new(addr)
        .call("POST", "/agents", None)
        .await
        .unwrap_err();
    assert_eq!(err.daemon_code(), Some("name_taken"));
    match err {
        ClientError::Daemon { status, body, .. } => {
            assert_eq!(status, 409);
            assert_eq!(body["x"], 1);
        }
        other => panic!("{other}"),
    }

    let (addr, _) = serve_once(b"HTTP/1.1 500 Oops\r\ncontent-length: 2\r\n\r\n{}").await;
    let err = Daemon::new(addr)
        .call("GET", "/health", None)
        .await
        .unwrap_err();
    assert_eq!(err.daemon_code(), Some("unknown"));
}

/// Something that is not HTTP, and nothing listening at all, are transport
/// errors.
#[tokio::test]
async fn test_call_reports_transport_failures() {
    let (addr, _) = serve_once(b"SSH-2.0-not-http\r\n").await;
    let err = Daemon::new(addr)
        .call("GET", "/health", None)
        .await
        .unwrap_err();
    assert!(matches!(err, ClientError::Transport(_)), "{err}");

    let free = {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        l.local_addr().unwrap()
    };
    let err = Daemon::new(free)
        .call("GET", "/health", None)
        .await
        .unwrap_err();
    assert!(matches!(err, ClientError::Transport(_)), "{err}");
}
