//! One ping-pong player: a hand-written MentisChat client that counts with a
//! peer, `1`, `2`, `3`, ... up to a target, each number answering the last.
//!
//! It speaks HTTP/1.1 over plain sockets (no client library) and WebSocket
//! through `tokio-tungstenite`, every socket with `TCP_NODELAY`, so what it
//! measures is the daemon and loopback. Shared by `examples/ping_pong` (two
//! separate processes) and `tests/ping_pong.rs` (two tasks against an
//! in-process daemon), so the benchmark and the regression test cannot drift.
//!
//! How a player hears about the next number is the [`Mode`]:
//!
//! | Mode | Receives with | Sends with |
//! |---|---|---|
//! | [`Mode::Ws`] | mail pushed on its `/ws` stream, acknowledged | a `send` frame on the same stream |
//! | [`Mode::Notify`] | a `/notify` wake-up frame, then `GET /inboxes/{me}` | `POST /messages` |
//! | [`Mode::Poll`] | `GET /inboxes/{me}?wait_ms=30000`, a parked pull | `POST /messages` |
//!
//! `Notify` is how a harness agent (Claude Code's Monitor) is woken, and the
//! daemon coalesces its frames on purpose: one frame 200 ms after mail stops
//! arriving, never within 3 s of the previous one, so an LLM is not woken per
//! message. A ping-pong over it therefore runs at about two numbers per 3 s by
//! design; the other two modes show what the bus itself does.
//!
//! A message body is `"<n> <sent unix micros>"`: the receiver subtracts the
//! sender's wall-clock stamp from its own, which is meaningful because both
//! players run on one machine.

#![allow(dead_code)] // each includer uses a different subset

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;

/// How a player receives (and, for `Ws`, sends) its numbers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    /// Pushed over `/ws`, sent over the same stream.
    Ws,
    /// Woken by `/notify`, then reads with `GET /inboxes`.
    Notify,
    /// Long-polls `GET /inboxes?wait_ms=`.
    Poll,
}

impl Mode {
    /// Every mode, in the order the benchmark runs them.
    pub const ALL: [Mode; 3] = [Mode::Ws, Mode::Poll, Mode::Notify];

    /// The name used on the command line and in agent names.
    pub fn name(self) -> &'static str {
        match self {
            Mode::Ws => "ws",
            Mode::Notify => "notify",
            Mode::Poll => "poll",
        }
    }

    /// Parse a command-line mode name.
    pub fn parse(s: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|m| m.name() == s)
    }
}

/// One number a player received.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Hop {
    /// The number.
    pub n: u32,
    /// When the peer sent it, unix microseconds (the peer's clock).
    pub sent_us: u64,
    /// When this player had it in hand, unix microseconds.
    pub recv_us: u64,
}

impl Hop {
    /// Send-to-receive time.
    pub fn latency(&self) -> Duration {
        Duration::from_micros(self.recv_us.saturating_sub(self.sent_us))
    }
}

/// What can end a game early.
#[derive(Debug)]
pub enum PlayError {
    /// A socket or HTTP failure.
    Io(String),
    /// The daemon answered something unexpected.
    Daemon(String),
    /// A number arrived out of turn, from the wrong sender, or malformed.
    Sequence(String),
}

impl std::fmt::Display for PlayError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PlayError::Io(m) => write!(f, "io: {m}"),
            PlayError::Daemon(m) => write!(f, "daemon: {m}"),
            PlayError::Sequence(m) => write!(f, "sequence: {m}"),
        }
    }
}

impl std::error::Error for PlayError {}

fn io(e: impl std::fmt::Display) -> PlayError {
    PlayError::Io(e.to_string())
}

/// Unix microseconds now.
pub fn now_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_micros() as u64)
}

/// One keep-alive HTTP/1.1 connection with `TCP_NODELAY`.
struct Conn {
    stream: BufReader<TcpStream>,
    addr: String,
}

impl Conn {
    async fn open(addr: &str) -> Result<Self, PlayError> {
        let stream = TcpStream::connect(addr).await.map_err(io)?;
        stream.set_nodelay(true).map_err(io)?;
        Ok(Self {
            stream: BufReader::new(stream),
            addr: addr.to_string(),
        })
    }

    /// Send one request; the status and the JSON body (`Null` when empty).
    async fn request(
        &mut self,
        method: &str,
        path: &str,
        body: &str,
    ) -> Result<(u16, Value), PlayError> {
        let head = format!(
            "{method} {path} HTTP/1.1\r\nHost: {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
            self.addr,
            body.len()
        );
        self.stream.write_all(head.as_bytes()).await.map_err(io)?;
        self.stream.write_all(body.as_bytes()).await.map_err(io)?;
        let mut head = Vec::with_capacity(256);
        while !head.ends_with(b"\r\n\r\n") {
            head.push(self.stream.read_u8().await.map_err(io)?);
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
        self.stream.read_exact(&mut body).await.map_err(io)?;
        let value = serde_json::from_slice(&body).unwrap_or(Value::Null);
        Ok((status, value))
    }
}

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn open_ws(addr: &str, path: &str) -> Result<Ws, PlayError> {
    // `true` disables Nagle on the client side of the stream.
    tokio_tungstenite::connect_async_with_config(format!("ws://{addr}{path}"), None, true)
        .await
        .map(|(ws, _)| ws)
        .map_err(io)
}

/// The next JSON text frame, answering nothing (tungstenite answers pings).
async fn next_frame(ws: &mut Ws) -> Result<Value, PlayError> {
    loop {
        match ws.next().await {
            Some(Ok(Message::Text(t))) => {
                return serde_json::from_str(t.as_str())
                    .map_err(|e| PlayError::Daemon(e.to_string()))
            }
            Some(Ok(Message::Close(c))) => {
                return Err(PlayError::Daemon(format!("stream closed: {c:?}")))
            }
            Some(Ok(_)) => {}
            Some(Err(e)) => return Err(io(e)),
            None => return Err(PlayError::Daemon("stream ended".into())),
        }
    }
}

/// Register `name`; a name still on its 10-minute hold from an earlier run is
/// taken back with `reclaim` (these demo names carry no key).
async fn register(conn: &mut Conn, name: &str) -> Result<(), PlayError> {
    let body = json!({"agent": name, "provider": "ping-pong", "description": "mentischat ping-pong player"});
    let (status, reply) = conn.request("POST", "/agents", &body.to_string()).await?;
    match status {
        200 => Ok(()),
        409 if reply["reason"] == "held" || reply["reason"] == "registered" => {
            let body = json!({"agent": name, "provider": "ping-pong", "reclaim": true});
            let (status, reply) = conn.request("POST", "/agents", &body.to_string()).await?;
            if status == 200 {
                Ok(())
            } else {
                Err(PlayError::Daemon(format!(
                    "register {name}: {status} {reply}"
                )))
            }
        }
        _ => Err(PlayError::Daemon(format!(
            "register {name}: {status} {reply}"
        ))),
    }
}

/// The receiving side of a [`Mode`].
enum Inbox {
    Ws(Ws),
    Notify { socket: Ws, conn: Conn },
    Poll(Conn),
}

impl Inbox {
    /// The next batch of messages addressed to `me`, as `check_inbox` returns
    /// them (the pushed `/ws` frames are acknowledged here).
    async fn next(&mut self, me: &str) -> Result<Vec<Value>, PlayError> {
        match self {
            Inbox::Ws(ws) => loop {
                let frame = next_frame(ws).await?;
                match frame["type"].as_str() {
                    Some("messages") => {
                        let messages = frame["messages"].as_array().cloned().unwrap_or_default();
                        if let Some(last) = messages.last() {
                            let ack = json!({"type": "ack", "upTo": last["id"]});
                            ws.send(Message::Text(ack.to_string().into()))
                                .await
                                .map_err(io)?;
                        }
                        return Ok(messages);
                    }
                    Some("error") => return Err(PlayError::Daemon(frame.to_string())),
                    _ => {} // receipts for our own sends
                }
            },
            Inbox::Notify { socket, conn } => loop {
                let frame = next_frame(socket).await?;
                if frame["type"] != "mail" {
                    continue;
                }
                let (status, reply) = conn.request("GET", &format!("/inboxes/{me}"), "").await?;
                if status != 200 {
                    return Err(PlayError::Daemon(format!("inbox: {status} {reply}")));
                }
                let messages = reply["messages"].as_array().cloned().unwrap_or_default();
                if !messages.is_empty() {
                    return Ok(messages);
                }
            },
            Inbox::Poll(conn) => loop {
                let path = format!("/inboxes/{me}?wait_ms=30000");
                let (status, reply) = conn.request("GET", &path, "").await?;
                if status != 200 {
                    return Err(PlayError::Daemon(format!("inbox: {status} {reply}")));
                }
                let messages = reply["messages"].as_array().cloned().unwrap_or_default();
                if !messages.is_empty() {
                    return Ok(messages);
                }
            },
        }
    }
}

/// A registered player, listening, ready to play. Splitting [`Player::join`]
/// from [`Player::play`] lets a caller start the second player only once the
/// first is listening, so the opening number is never sent into the void.
pub struct Player {
    addr: String,
    me: String,
    peer: String,
    mode: Mode,
    inbox: Inbox,
    /// The connection sends go out on (`Notify` and `Poll`); `Ws` sends on
    /// its stream.
    out: Option<Conn>,
}

impl Player {
    /// Register `me` on the daemon at `addr` and open its receiving side.
    ///
    /// # Errors
    ///
    /// [`PlayError::Io`] if the daemon cannot be reached, [`PlayError::Daemon`]
    /// if registration or the upgrade is refused.
    pub async fn join(addr: &str, me: &str, peer: &str, mode: Mode) -> Result<Self, PlayError> {
        let mut conn = Conn::open(addr).await?;
        register(&mut conn, me).await?;
        let (inbox, out) = match mode {
            Mode::Ws => (
                Inbox::Ws(open_ws(addr, &format!("/ws?agent={me}")).await?),
                None,
            ),
            Mode::Notify => {
                let socket = open_ws(addr, &format!("/notify?agent={me}")).await?;
                (
                    Inbox::Notify {
                        socket,
                        conn: Conn::open(addr).await?,
                    },
                    Some(conn),
                )
            }
            Mode::Poll => (Inbox::Poll(Conn::open(addr).await?), Some(conn)),
        };
        Ok(Self {
            addr: addr.to_string(),
            me: me.to_string(),
            peer: peer.to_string(),
            mode,
            inbox,
            out,
        })
    }

    async fn send(&mut self, n: u32) -> Result<(), PlayError> {
        let body = format!("{n} {}", now_us());
        match (&mut self.inbox, &mut self.out) {
            (Inbox::Ws(ws), _) => {
                let frame = json!({"type": "send", "ref": n, "to": self.peer, "body": body});
                ws.send(Message::Text(frame.to_string().into()))
                    .await
                    .map_err(io)
            }
            (_, Some(conn)) => {
                let msg = json!({"from": self.me, "to": self.peer, "body": body});
                let (status, reply) = conn.request("POST", "/messages", &msg.to_string()).await?;
                if status == 200 {
                    Ok(())
                } else {
                    Err(PlayError::Daemon(format!("send {n}: {status} {reply}")))
                }
            }
            (_, None) => unreachable!("Notify and Poll players always have a send connection"),
        }
    }

    /// Count to `to` with the peer. The player that `opens` sends `1`; after
    /// that each player answers the number it receives with the next one, and
    /// the game ends at `to`. Returns every number this player received, in
    /// order, with its timing.
    ///
    /// # Errors
    ///
    /// [`PlayError::Sequence`] when a number arrives out of turn, from anyone
    /// but the peer, or malformed; I/O and daemon errors as for
    /// [`Player::join`].
    pub async fn play(mut self, opens: bool, to: u32) -> Result<Vec<Hop>, PlayError> {
        let mut hops = Vec::new();
        let mut expect = if opens {
            self.send(1).await?;
            2
        } else {
            1
        };
        while expect <= to {
            let me = self.me.clone();
            for m in self.inbox.next(&me).await? {
                let recv_us = now_us();
                let from = m["from"].as_str().unwrap_or_default();
                if !from.eq_ignore_ascii_case(&self.peer) {
                    return Err(PlayError::Sequence(format!(
                        "{me} got mail from {from}, expected {}",
                        self.peer
                    )));
                }
                let body = m["body"].as_str().unwrap_or_default();
                let mut parts = body.split(' ');
                let (Some(Ok(n)), Some(Ok(sent_us))) = (
                    parts.next().map(str::parse::<u32>),
                    parts.next().map(str::parse::<u64>),
                ) else {
                    return Err(PlayError::Sequence(format!(
                        "{me} got a malformed body {body:?}"
                    )));
                };
                if n != expect {
                    return Err(PlayError::Sequence(format!(
                        "{me} got {n}, expected {expect}"
                    )));
                }
                hops.push(Hop {
                    n,
                    sent_us,
                    recv_us,
                });
                if n < to {
                    self.send(n + 1).await?;
                }
                expect = n + 2;
            }
        }
        Ok(hops)
    }

    /// Leave the roster (best effort; a keyless name needs no proof).
    pub async fn leave(addr: &str, me: &str) {
        if let Ok(mut conn) = Conn::open(addr).await {
            let _ = conn.request("DELETE", &format!("/agents/{me}"), "").await;
        }
    }

    /// This player's name.
    pub fn name(&self) -> &str {
        &self.me
    }

    /// This player's mode.
    pub fn mode(&self) -> Mode {
        self.mode
    }

    /// The daemon address.
    pub fn addr(&self) -> &str {
        &self.addr
    }
}

/// Timing of a whole game, from both players' hops.
#[derive(Clone, Debug)]
pub struct Summary {
    /// Numbers delivered.
    pub hops: usize,
    /// From the opening send to the last number in hand.
    pub total: Duration,
    /// Median send-to-receive time.
    pub p50: Duration,
    /// 99th percentile.
    pub p99: Duration,
    /// Slowest hop.
    pub max: Duration,
}

impl Summary {
    /// Combine both players' hops. `None` when there are none.
    pub fn of(mut hops: Vec<Hop>) -> Option<Self> {
        if hops.is_empty() {
            return None;
        }
        hops.sort_by_key(|h| h.n);
        let start = hops[0].sent_us;
        let end = hops.iter().map(|h| h.recv_us).max().unwrap_or(start);
        let mut lat: Vec<Duration> = hops.iter().map(Hop::latency).collect();
        lat.sort();
        let at = |q: f64| lat[((lat.len() - 1) as f64 * q).round() as usize];
        Some(Self {
            hops: hops.len(),
            total: Duration::from_micros(end.saturating_sub(start)),
            p50: at(0.50),
            p99: at(0.99),
            max: lat[lat.len() - 1],
        })
    }

    /// Numbers per second over the whole game.
    pub fn per_second(&self) -> f64 {
        self.hops as f64 / self.total.as_secs_f64().max(1e-9)
    }
}
