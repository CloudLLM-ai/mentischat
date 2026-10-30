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
//! - `ws`: per client, a sender stream sends batches of 64 to a room over
//!   `GET /ws` and a receiver stream is pushed them and acknowledges; it
//!   reports end-to-end delivered messages/s and the push latency (send to
//!   arrival, taken on the first message of each pushed frame).
//!
//! The HTTP modes speak HTTP/1.1 over plain sockets (no client library), so
//! what they measure is the daemon plus loopback. It registers agents named `lg…` and
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
    Ws,
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
                    "ws" => Mode::Ws,
                    other => panic!("--mode send|batch|pull|ws, not {other}"),
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
            Mode::Ws => unreachable!("ws mode has its own runner"),
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
    if cfg.mode == Mode::Ws {
        return ws_load(cfg).await;
    }
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

/// Microseconds since `epoch`, the clock the messages are stamped with.
fn micros(epoch: Instant) -> u128 {
    epoch.elapsed().as_micros()
}

/// What a receiver saw: messages delivered and the push latency of the first
/// message of each frame, in microseconds.
#[derive(Default)]
struct Received {
    messages: u64,
    latencies: Vec<u64>,
}

/// The `ws` mode: sender and receiver streams per client, a room between them.
async fn ws_load(cfg: std::sync::Arc<Config>) {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::connect_async;
    use tokio_tungstenite::tungstenite::Message;

    let epoch = Instant::now();
    let mut setup_conn = Conn::open(&cfg.addr).await.expect("connect");
    for id in 0..cfg.clients {
        setup(&mut setup_conn, id).await.expect("setup");
    }
    let run_for = Duration::from_secs(cfg.seconds);
    let started = Instant::now();
    let deadline = started + run_for;
    let mut receivers = Vec::new();
    let mut senders = Vec::new();
    for id in 0..cfg.clients {
        let url = format!("ws://{}/ws?agent=lgr{id}", cfg.addr);
        let (mut rx, _) = connect_async(url).await.expect("receiver stream");
        receivers.push(tokio::spawn(async move {
            let mut seen = Received::default();
            // Drain for a second past the deadline, then stop.
            let stop = deadline + Duration::from_secs(1);
            while let Ok(Some(Ok(frame))) = tokio::time::timeout_at(stop.into(), rx.next()).await {
                let Message::Text(text) = frame else { continue };
                let text = text.as_str();
                // Keys come out sorted, so `type` is last.
                if !text.ends_with(r#""type":"messages"}"#) {
                    continue;
                }
                let arrived = micros(epoch) as u64;
                let count = text.matches(r#""body":""#).count() as u64;
                let stamp = text
                    .split_once(r#""body":""#)
                    .and_then(|(_, rest)| rest.split('"').next())
                    .and_then(|b| b.parse::<u64>().ok());
                if let Some(sent) = stamp {
                    seen.latencies.push(arrived.saturating_sub(sent));
                }
                seen.messages += count;
                let last = text.rfind(r#""id":""#).map(|i| &text[i + 6..]);
                if let Some(id) = last.and_then(|r| r.split('"').next()) {
                    let ack = format!(r#"{{"type":"ack","upTo":{id}}}"#);
                    if rx.send(Message::Text(ack.into())).await.is_err() {
                        break;
                    }
                }
            }
            seen
        }));
        let url = format!("ws://{}/ws?agent=lgs{id}", cfg.addr);
        let (mut tx, _) = connect_async(url).await.expect("sender stream");
        senders.push(tokio::spawn(async move {
            let mut sent = 0u64;
            while Instant::now() < deadline {
                let items: Vec<String> = (0..BATCH)
                    .map(|_| format!(r#"{{"room":"lg{id}","body":"{}"}}"#, micros(epoch)))
                    .collect();
                let frame = format!(
                    r#"{{"type":"batch","ref":1,"items":[{}]}}"#,
                    items.join(",")
                );
                if tx.send(Message::Text(frame.into())).await.is_err() {
                    break;
                }
                // One batch in flight per sender: wait for its replies.
                match tx.next().await {
                    Some(Ok(Message::Text(_))) => sent += BATCH as u64,
                    _ => break,
                }
            }
            sent
        }));
    }
    let mut sent = 0;
    for task in senders {
        sent += task.await.expect("sender");
    }
    let mut all = Received::default();
    for task in receivers {
        let r = task.await.expect("receiver");
        all.messages += r.messages;
        all.latencies.extend(r.latencies);
    }
    let secs = started
        .elapsed()
        .as_secs_f64()
        .min(cfg.seconds as f64 + 1.0);
    all.latencies.sort_unstable();
    let pct = |p: f64| {
        let i = (all.latencies.len() as f64 * p) as usize;
        Duration::from_micros(all.latencies.get(i).copied().unwrap_or_default())
    };
    println!(
        "{} clients over WebSocket, {:.1}s: sent {:.0} msg/s, delivered {:.0} msg/s, push latency p50 {:?} p99 {:?}",
        cfg.clients,
        secs,
        sent as f64 / cfg.seconds as f64,
        all.messages as f64 / secs,
        pct(0.5),
        pct(0.99),
    );
}
