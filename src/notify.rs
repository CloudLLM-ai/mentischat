//! `GET /notify?agent=NAME`: a read-only WebSocket that says "you have mail",
//! so an idle agent can be woken by its harness and then read the mail with
//! `check_inbox` like any other.
//!
//! It exists because `/ws` cannot do this job: a `/ws` stream *leases* the
//! mail it pushes, so while one is open `check_inbox` finds nothing. A notify
//! socket never takes anything. Mail stays unread, `check_inbox` still hands
//! each message out exactly once, and no body ever crosses this channel (so
//! secure-room ciphertext and the delivery contract are untouched). The
//! intended client is a harness that can watch a WebSocket and turn each text
//! frame into a notification for the agent, such as Claude Code's Monitor
//! tool; those watches are receive-only and expire, so everything here is
//! designed for a client that cannot answer and re-arms often.
//!
//! The route sits behind the same loopback, `Host` and `Origin` checks as
//! every other route. An invalid or unregistered `agent` gets the contract
//! error (status and JSON) before the upgrade, and so does a name that
//! already has [`MAX_NOTIFY_PER_AGENT`](crate::bus::MAX_NOTIFY_PER_AGENT)
//! notify sockets open (`too_many_waiters`).
//!
//! # Frames
//!
//! The server sends exactly one kind of text frame, and only about mail,
//! because every text frame wakes the agent:
//!
//! ```json
//! {"type":"mail","unread":3,"from":["Vega","Orion"],"rooms":["ops"],"newest":"1042"}
//! ```
//!
//! - `unread`: how many messages `check_inbox` would return now, direct and
//!   room together. Exact.
//! - `from`: up to [`MAX_DIGEST_NAMES`](crate::bus::MAX_DIGEST_NAMES)
//!   distinct senders (direct mail first, then room mail).
//! - `rooms`: up to that many rooms with unread mail, as the rooms were
//!   created. Empty when everything is direct mail.
//! - `newest`: the id of the newest unread message, a decimal string like
//!   every message id.
//!
//! Liveness uses WebSocket ping and pong control frames (the server pings
//! every 20 s), never text. A peer that has said nothing, not even a pong, for
//! 60 s is closed. Each ping also marks the agent as seen, so the idle sweep
//! does not drop it in the gap between one watch and the next.
//!
//! Anything the client sends is ignored (text and binary alike); only a close
//! frame ends the socket. Frames larger than [`MAX_CLIENT_FRAME_BYTES`] end
//! it, so a client cannot make the daemon buffer more than that.
//!
//! # When a frame is sent
//!
//! - **On connect**, at once, if mail is already waiting. A harness whose
//!   watch expired and was re-armed thereby learns about mail that landed in
//!   the gap.
//! - **After a send**, once [`SETTLE`] has passed with no further mail, so a
//!   burst is one frame, and never sooner than [`COOLDOWN`] after the
//!   previous frame, so a steady trickle costs the agent a frame every few
//!   seconds, not one per message.
//! - **Never for nothing**: not when the mail was read meanwhile (by a
//!   `check_inbox`, say), not for a send that is not newer than what the last
//!   frame already announced, and not for the agent's own sends.
//!
//! Mail sent to a room notifies its members that have an open socket, never
//! the sender. A socket coexists with others for the same name, with a `/ws`
//! stream and with parked pulls: it takes over from nothing and disturbs
//! nothing. While open it counts as attending to the agent, so a sender's
//! receipt says `notified` and the idle sweep leaves the agent alone.
//!
//! # Ending
//!
//! The server closes with 1001 when it shuts down and 4001 when the agent
//! leaves the roster (deregistered or swept); the client closing or going
//! silent ends the socket without a word. Whichever way it ends, nothing
//! needs returning: the socket held nothing.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::ws::{Message as Frame, WebSocket, WebSocketUpgrade};
use axum::extract::{Query, State};
use axum::response::{IntoResponse, Response};
use tokio::time::Instant;

use crate::bus::{Bus, MailDigest, NotifySession};
use crate::errors::MailboxError;
use crate::names::AgentName;
use crate::ws::{close, send_frame, Ending, PING_EVERY, SILENCE_LIMIT};

/// How long mail must stop arriving before a frame is sent for it: the
/// coalescing window that turns a burst into one frame.
pub const SETTLE: Duration = Duration::from_millis(200);
/// The least time between two frames on one socket, so a steady trickle of
/// mail costs the agent a frame every few seconds, not one per message.
pub const COOLDOWN: Duration = Duration::from_secs(3);
/// Largest WebSocket message or frame a client may send. The server ignores
/// what it sends; the bound only stops a client making the daemon buffer.
pub const MAX_CLIENT_FRAME_BYTES: usize = 1024;

/// Upgrade to a notify socket for `?agent=`.
pub async fn upgrade(
    State(bus): State<Arc<Bus>>,
    query: Result<Query<HashMap<String, String>>, axum::extract::rejection::QueryRejection>,
    ws: WebSocketUpgrade,
) -> Response {
    let Ok(Query(q)) = query else {
        return MailboxError::InvalidBody.into_response();
    };
    let agent = match AgentName::parse(q.get("agent").map_or("", String::as_str)) {
        Ok(a) => a,
        Err(e) => return e.into_response(),
    };
    // Opened before the upgrade so that an unregistered agent and a full
    // budget are contract errors, not a socket that opens and closes.
    let session = match bus.open_notify(&agent) {
        Ok(session) => session,
        Err(e) => return e.into_response(),
    };
    ws.max_message_size(MAX_CLIENT_FRAME_BYTES)
        .max_frame_size(MAX_CLIENT_FRAME_BYTES)
        .on_upgrade(move |socket| run(bus, session, socket))
        .into_response()
}

async fn run(bus: Arc<Bus>, session: NotifySession, mut socket: WebSocket) {
    let ending = serve(&bus, &session, &mut socket).await;
    // The slot is free before the close handshake, which can take seconds.
    drop(session);
    close(socket, ending).await;
}

/// The one frame a notify socket sends, in the field order clients see.
#[derive(serde::Serialize)]
struct MailFrame<'a> {
    r#type: &'static str,
    unread: usize,
    from: &'a [String],
    rooms: &'a [String],
    newest: String,
}

/// The text of the one frame a notify socket sends, for `digest`.
///
/// # Examples
///
/// ```
/// use mentischat::bus::MailDigest;
/// use mentischat::notify::mail_frame;
///
/// let digest = MailDigest {
///     unread: 2,
///     from: vec!["Vega".into()],
///     rooms: vec!["ops".into()],
///     newest: 1042,
/// };
/// assert_eq!(
///     mail_frame(&digest),
///     r#"{"type":"mail","unread":2,"from":["Vega"],"rooms":["ops"],"newest":"1042"}"#
/// );
/// ```
pub fn mail_frame(digest: &MailDigest) -> String {
    serde_json::to_string(&MailFrame {
        r#type: "mail",
        unread: digest.unread,
        from: &digest.from,
        rooms: &digest.rooms,
        newest: digest.newest.to_string(),
    })
    .expect("a frame of strings and numbers serializes")
}

/// What a socket remembers between frames.
struct Pacing {
    /// Newest id the last frame announced; 0 when nothing is announced (at
    /// the start, and whenever the inbox was seen empty).
    announced: u64,
    /// When the last frame went out.
    last_sent: Option<Instant>,
    /// When the next frame is due, once mail has landed since the last look.
    due: Option<Instant>,
}

impl Pacing {
    /// Mail landed: schedule a frame after the settle window, but not before
    /// the cooldown since the previous frame has passed.
    fn schedule(&mut self) {
        let settled = Instant::now() + SETTLE;
        self.due = Some(match self.last_sent {
            Some(sent) => settled.max(sent + COOLDOWN),
            None => settled,
        });
    }

    /// Whether `digest` is worth a frame, updating what is announced: mail
    /// that is unread and newer than what the last frame said.
    fn worth_a_frame(&mut self, digest: &MailDigest) -> bool {
        if digest.unread == 0 {
            self.announced = 0;
            return false;
        }
        let fresh = digest.newest > self.announced;
        if fresh {
            self.announced = digest.newest;
        }
        fresh
    }
}

/// Look at the agent's mail and, when it deserves one, send a frame. `Some`
/// ends the socket.
async fn announce(
    bus: &Bus,
    session: &NotifySession,
    socket: &mut WebSocket,
    pacing: &mut Pacing,
) -> Option<Ending> {
    pacing.due = None;
    let digest = match bus.notify_digest(session) {
        Ok(digest) => digest,
        Err(MailboxError::ShuttingDown) => {
            return Some(Ending::Close(1001, "server shutting down"))
        }
        Err(_) => return Some(Ending::Close(4001, "agent left the roster")),
    };
    if !pacing.worth_a_frame(&digest) {
        return None;
    }
    pacing.last_sent = Some(Instant::now());
    send_frame(socket, Frame::Text(mail_frame(&digest).into()))
        .await
        .err()
        .map(|()| Ending::Dropped)
}

/// Tell the peer about mail until something ends the socket.
async fn serve(bus: &Bus, session: &NotifySession, socket: &mut WebSocket) -> Ending {
    let mut ping = tokio::time::interval(PING_EVERY);
    ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ping.tick().await;
    let mut last_heard = Instant::now();
    let mut pacing = Pacing {
        announced: 0,
        last_sent: None,
        due: None,
    };
    // Subscribed before the first look, so mail landing in between rings.
    let mut events = session.events();
    if let Some(ending) = announce(bus, session, socket, &mut pacing).await {
        return ending;
    }
    loop {
        let due = pacing.due;
        let ended = tokio::select! {
            changed = events.changed(), if due.is_none() => match changed {
                Ok(()) => {
                    pacing.schedule();
                    None
                }
                Err(_) => Some(Ending::Dropped),
            },
            () = tokio::time::sleep_until(due.unwrap_or_else(Instant::now)), if due.is_some() => {
                // Marked seen before the look, so mail that lands during it
                // rings again.
                events.borrow_and_update();
                announce(bus, session, socket, &mut pacing).await
            }
            _ = ping.tick() => on_ping(bus, session, socket, last_heard).await,
            frame = socket.recv() => match frame {
                None | Some(Err(_)) | Some(Ok(Frame::Close(_))) => Some(Ending::Dropped),
                // Text, binary, pings and pongs: heard, and nothing more.
                Some(Ok(_)) => {
                    last_heard = Instant::now();
                    None
                }
            },
        };
        if let Some(ending) = ended {
            return ending;
        }
        if bus.is_closed() {
            return Ending::Close(1001, "server shutting down");
        }
    }
}

/// A ping tick: close a peer silent past [`SILENCE_LIMIT`], else mark the
/// agent seen and ping the peer.
async fn on_ping(
    bus: &Bus,
    session: &NotifySession,
    socket: &mut WebSocket,
    last_heard: Instant,
) -> Option<Ending> {
    if last_heard.elapsed() > SILENCE_LIMIT {
        return Some(Ending::Close(1008, "no answer to pings"));
    }
    bus.notify_touch(session);
    send_frame(socket, Frame::Ping(Vec::new().into()))
        .await
        .err()
        .map(|()| Ending::Dropped)
}
