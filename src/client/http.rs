//! A minimal HTTP/1.1 client for the daemon's loopback REST routes.
//!
//! One request per connection (`Connection: close`), JSON in and out. The
//! daemon only listens on loopback and checks `Host`, so this speaks plain
//! HTTP to `127.0.0.1:port` and needs no TLS or connection pool.

use std::net::SocketAddr;
use std::time::Duration;

use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use super::ClientError;

/// Longest the client waits for a reply: a blocking pull parks up to 30 s.
const REPLY_LIMIT: Duration = Duration::from_secs(45);
/// Largest reply accepted (a batch of retained messages is well under it).
const MAX_REPLY_BYTES: usize = 32 * 1024 * 1024;

/// A daemon on loopback.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Daemon {
    addr: SocketAddr,
}

impl Daemon {
    /// The daemon listening on `addr`.
    pub fn new(addr: SocketAddr) -> Self {
        Self { addr }
    }

    /// The default daemon, [`crate::BIND_ADDR`].
    pub fn local() -> Self {
        Self::new(crate::BIND_ADDR)
    }

    /// The address requests go to.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Send `method path` with an optional JSON `body`; the JSON reply on a
    /// 2xx status.
    ///
    /// # Errors
    ///
    /// [`ClientError::Daemon`] for a non-2xx status (carrying the contract
    /// error), [`ClientError::Transport`] for anything below HTTP.
    pub async fn call(
        &self,
        method: &str,
        path: &str,
        body: Option<&Value>,
    ) -> Result<Value, ClientError> {
        let (status, value) = tokio::time::timeout(REPLY_LIMIT, self.exchange(method, path, body))
            .await
            .map_err(|_| ClientError::Transport("timed out".into()))??;
        if (200..300).contains(&status) {
            return Ok(value);
        }
        let code = value
            .get("error")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_string();
        Err(ClientError::Daemon {
            status,
            code,
            body: value,
        })
    }

    async fn exchange(
        &self,
        method: &str,
        path: &str,
        body: Option<&Value>,
    ) -> Result<(u16, Value), ClientError> {
        let io = |e: std::io::Error| ClientError::Transport(e.to_string());
        let payload = body.map(Value::to_string).unwrap_or_default();
        let request = format!(
            "{method} {path} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{payload}",
            self.addr,
            payload.len()
        );
        let mut stream = TcpStream::connect(self.addr).await.map_err(io)?;
        stream.write_all(request.as_bytes()).await.map_err(io)?;
        let mut raw = Vec::new();
        stream
            .take(MAX_REPLY_BYTES as u64)
            .read_to_end(&mut raw)
            .await
            .map_err(io)?;
        parse_reply(&raw)
    }
}

/// Split a raw HTTP/1.1 reply into its status and JSON body (decoding a
/// chunked body, treating an empty one as `null`).
///
/// # Errors
///
/// [`ClientError::Transport`] when the bytes are not a well-formed reply with a
/// JSON body.
///
/// # Examples
///
/// ```
/// use mentischat::client::http::parse_reply;
///
/// let raw = b"HTTP/1.1 200 OK\r\ncontent-length: 7\r\n\r\n{\"a\":1}";
/// let (status, body) = parse_reply(raw).unwrap();
/// assert_eq!((status, body["a"].as_u64()), (200, Some(1)));
/// assert!(parse_reply(b"garbage").is_err());
/// ```
pub fn parse_reply(raw: &[u8]) -> Result<(u16, Value), ClientError> {
    let bad = |what: &str| ClientError::Transport(format!("malformed reply: {what}"));
    let split = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .ok_or_else(|| bad("no header end"))?;
    let head = std::str::from_utf8(&raw[..split]).map_err(|_| bad("header is not UTF-8"))?;
    let mut lines = head.split("\r\n");
    let status = lines
        .next()
        .and_then(|l| l.split(' ').nth(1))
        .and_then(|s| s.parse::<u16>().ok())
        .ok_or_else(|| bad("status line"))?;
    let chunked = lines.any(|l| {
        l.split_once(':').is_some_and(|(k, v)| {
            k.eq_ignore_ascii_case("transfer-encoding") && v.trim().eq_ignore_ascii_case("chunked")
        })
    });
    let body = &raw[split + 4..];
    let body = if chunked {
        dechunk(body).ok_or_else(|| bad("chunked body"))?
    } else {
        body.to_vec()
    };
    let value = if body.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&body).map_err(|_| bad("body is not JSON"))?
    };
    Ok((status, value))
}

/// Decode a chunked transfer-encoded body.
fn dechunk(mut body: &[u8]) -> Option<Vec<u8>> {
    let mut out = Vec::new();
    loop {
        let eol = body.windows(2).position(|w| w == b"\r\n")?;
        let size = usize::from_str_radix(
            std::str::from_utf8(&body[..eol])
                .ok()?
                .split(';')
                .next()?
                .trim(),
            16,
        )
        .ok()?;
        body = &body[eol + 2..];
        if size == 0 {
            return Some(out);
        }
        out.extend_from_slice(body.get(..size)?);
        body = body.get(size + 2..)?;
    }
}

/// Percent-encode a query value (unreserved characters pass through).
pub fn query_escape(raw: &str) -> String {
    raw.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (b as char).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
}
