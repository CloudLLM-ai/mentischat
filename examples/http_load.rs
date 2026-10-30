//! End-to-end HTTP load against a running daemon: many keep-alive clients on a
//! multi-threaded runtime, each with its own pair of agents in a room.
//!
//! ```text
//! MENTISCHAT_PLAIN=1 MENTISCHAT_DIR=$(mktemp -d) target/release/mentischat &
//! cargo run --release --example http_load -- --mode batch --clients 64 --seconds 5
//! ```
//!
//! Options: `--addr 127.0.0.1:9480`, `--clients 64`, `--seconds 5`, and
//! `--mode`:
//! - `send`: one `POST /messages` per request, to a room (a room's ring never
//!   fills up the way direct mail's 1024-per-name cap would);
//! - `batch`: `POST /messages/batch` of 64 messages per request;
//! - `pull`: per round the client sends a batch of 64, then drains it with
//!   `GET /inboxes/{agent}?limit=256` (one page); the pull is what is timed, so
//!   `requests/s` counts pulls and `messages/s` the messages they returned.
//!
//! It speaks HTTP/1.1 over plain sockets (no client library), so what it
//! measures is the daemon plus loopback. It registers agents named `lg…` and
//! rooms named `lg…`; run it against a throwaway daemon.

use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

const BATCH: usize = 64;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    Send,
    Batch,
    Pull,
}

struct Config {
    addr: String,
    clients: usize,
    seconds: u64,
    mode: Mode,
}

/// One keep-alive HTTP/1.1 connection.
struct Conn {
    stream: BufReader<TcpStream>,
    addr: String,
}

impl Conn {
    async fn open(addr: &str) -> std::io::Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        Ok(Self {
            stream: BufReader::new(stream),
            addr: addr.to_string(),
        })
    }

    /// Send a request and return the status and body.
    async fn request(
        &mut self,
        method: &str,
        path: &str,
        body: &str,
    ) -> std::io::Result<(u16, String)> {
        let head = format!(
            "{method} {path} HTTP/1.1\r\nHost: {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
            self.addr,
            body.len()
        );
        self.stream.write_all(head.as_bytes()).await?;
        self.stream.write_all(body.as_bytes()).await?;
        self.read_response().await
    }

    async fn read_response(&mut self) -> std::io::Result<(u16, String)> {
        let mut head = Vec::with_capacity(256);
        while !head.ends_with(b"\r\n\r\n") {
            head.push(self.stream.read_u8().await?);
        }
        let text = String::from_utf8_lossy(&head).to_ascii_lowercase();
        let status = text
            .split_whitespace()
            .nth(1)
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let len = text
            .lines()
            .find_map(|l| l.strip_prefix("content-length:"))
            .and_then(|v| v.trim().parse::<usize>().ok())
            .unwrap_or(0);
        let mut body = vec![0u8; len];
        self.stream.read_exact(&mut body).await?;
        Ok((status, String::from_utf8_lossy(&body).into_owned()))
    }
}

fn parse_args() -> Config {
    let mut cfg = Config {
        addr: "127.0.0.1:9480".into(),
        clients: 64,
        seconds: 5,
        mode: Mode::Batch,
    };
    let mut args = std::env::args().skip(1);
    while let Some(flag) = args.next() {
        let value = args.next().unwrap_or_default();
        match flag.as_str() {
            "--addr" => cfg.addr = value,
            "--clients" => cfg.clients = value.parse().expect("--clients N"),
            "--seconds" => cfg.seconds = value.parse().expect("--seconds N"),
            "--mode" => {
                cfg.mode = match value.as_str() {
                    "send" => Mode::Send,
                    "batch" => Mode::Batch,
                    "pull" => Mode::Pull,
                    other => panic!("--mode send|batch|pull, not {other}"),
                }
            }
            other => panic!("unknown option {other}"),
        }
    }
    cfg
}

/// Register the client's two agents and a room holding both.
async fn setup(conn: &mut Conn, id: usize) -> std::io::Result<()> {
    for who in ["s", "r"] {
        let body = format!(r#"{{"agent":"lg{who}{id}","reclaim":true}}"#);
        let (status, text) = conn.request("POST", "/agents", &body).await?;
        assert_eq!(status, 200, "register: {text}");
    }
    let body = format!(r#"{{"agent":"lgs{id}","room":"lg{id}"}}"#);
    let (status, text) = conn.request("POST", "/rooms", &body).await?;
    assert!(status == 200 || status == 409, "create_room: {text}");
    let body = format!(r#"{{"agent":"lgr{id}"}}"#);
    let (status, text) = conn
        .request("POST", &format!("/rooms/lg{id}/members"), &body)
        .await?;
    assert_eq!(status, 200, "join_room: {text}");
    Ok(())
}

fn batch_body(id: usize) -> String {
    let item = format!(
        r#"{{"from":"lgs{id}","room":"lg{id}","body":"Reply when you can: load test message"}}"#
    );
    format!("[{}]", vec![item; BATCH].join(","))
}

/// What one client did: requests, messages moved, and each request's latency.
#[derive(Default)]
struct Tally {
    requests: u64,
    messages: u64,
    latencies: Vec<Duration>,
}

async fn client(cfg: &Config, id: usize, deadline: Instant) -> std::io::Result<Tally> {
    let mut conn = Conn::open(&cfg.addr).await?;
    setup(&mut conn, id).await?;
    let single = format!(
        r#"{{"from":"lgs{id}","room":"lg{id}","body":"Reply when you can: load test message"}}"#
    );
    let batch = batch_body(id);
    let inbox = format!("/inboxes/lgr{id}?limit=256");
    let mut tally = Tally::default();
    while Instant::now() < deadline {
        match cfg.mode {
            Mode::Send => {
                timed(&mut tally, 1, conn.request("POST", "/messages", &single)).await?;
            }
            Mode::Batch => {
                timed(
                    &mut tally,
                    BATCH as u64,
                    conn.request("POST", "/messages/batch", &batch),
                )
                .await?;
            }
            Mode::Pull => {
                conn.request("POST", "/messages/batch", &batch).await?;
                loop {
                    let (_, text) = timed(&mut tally, 0, conn.request("GET", &inbox, "")).await?;
                    let got = text.matches(r#""body""#).count() as u64;
                    tally.messages += got;
                    if got == 0
                        || !text.contains(r#""remaining":"#)
                        || text.contains(r#""remaining":0"#)
                    {
                        break;
                    }
                }
            }
        }
    }
    Ok(tally)
}

/// Run one request, counting it and its latency (`messages` more messages
/// moved, for the modes where that is known up front).
async fn timed(
    tally: &mut Tally,
    messages: u64,
    request: impl std::future::Future<Output = std::io::Result<(u16, String)>>,
) -> std::io::Result<(u16, String)> {
    let started = Instant::now();
    let out = request.await?;
    tally.latencies.push(started.elapsed());
    tally.requests += 1;
    tally.messages += messages;
    Ok(out)
}

#[tokio::main]
async fn main() {
    let cfg = std::sync::Arc::new(parse_args());
    let deadline = Instant::now() + Duration::from_secs(cfg.seconds);
    let started = Instant::now();
    let tasks: Vec<_> = (0..cfg.clients)
        .map(|id| {
            let cfg = cfg.clone();
            tokio::spawn(async move { client(&cfg, id, deadline).await })
        })
        .collect();
    let mut all = Tally::default();
    for task in tasks {
        let t = task.await.expect("client task").expect("client io");
        all.requests += t.requests;
        all.messages += t.messages;
        all.latencies.extend(t.latencies);
    }
    let secs = started.elapsed().as_secs_f64();
    all.latencies.sort_unstable();
    let pct = |p: f64| {
        all.latencies
            .get(((all.latencies.len() as f64) * p) as usize)
            .copied()
            .unwrap_or_default()
    };
    println!(
        "{} clients, {:.1}s: {:.0} requests/s, {:.0} messages/s, latency p50 {:?} p99 {:?}",
        cfg.clients,
        secs,
        all.requests as f64 / secs,
        all.messages as f64 / secs,
        pct(0.5),
        pct(0.99),
    );
}
