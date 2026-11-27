//! Traffic lines for the operator's log: one `info` line when a message is
//! sent and one when it is read, so the person running the daemon can follow
//! conversations in the terminal view's log pane.
//!
//! The lines are written from messages the bus already returns, after every
//! lock is released. Writing one changes nothing: it never marks mail read,
//! moves a room cursor or refreshes `lastSeen`. With [`TrafficLog::Metadata`]
//! a line names the sender, the recipient or room, the message id and the
//! body's size. Only with [`TrafficLog::WithPreviews`] does a sent line also
//! carry the start of the body, and never for a signed room message.

use std::collections::VecDeque;
use std::fmt;
use std::sync::Arc;

use parking_lot::Mutex;
use serde::Serialize;

use super::clock::rfc3339_ms;
use super::log::Msg;

/// How many recent events the activity feed keeps.
pub const ACTIVITY_CAPACITY: usize = 1024;

/// Whether an [`Activity`] is a message sent or one read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ActivityKind {
    /// A message was accepted.
    Sent,
    /// A recipient read a message (a pull, or a stream acknowledgement).
    Read,
}

/// One event of the activity feed: what the operator's dashboard shows of the
/// traffic. Built from messages the bus already handed out; recording one
/// changes nothing in the mailbox.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Activity {
    /// Position in the feed, increasing for the life of the process.
    pub seq: u64,
    /// Sent or read.
    pub kind: ActivityKind,
    /// When, UTC RFC 3339.
    pub at: String,
    /// The sender.
    pub from: String,
    /// The recipient of a direct message.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub to: Option<String>,
    /// The room of a room message.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub room: Option<String>,
    /// Who read it, for a read.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reader: Option<String>,
    /// The message id.
    pub id: u64,
    /// The stored body's size in bytes.
    pub bytes: usize,
    /// The body is a signed envelope.
    pub signed: bool,
    /// The body is end-to-end encrypted (an MLS message).
    pub encrypted: bool,
    /// The start of the text, for a sent message that is not encrypted: the
    /// first line, cut to [`PREVIEW_CHARS`] characters.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub preview: Option<String>,
}

/// The recent [`Activity`], oldest first, bounded to [`ACTIVITY_CAPACITY`].
#[derive(Debug, Default)]
pub(super) struct Feed {
    events: Mutex<(u64, VecDeque<Activity>)>,
}

impl Feed {
    fn push(&self, make: impl FnOnce(u64) -> Activity) {
        let mut guard = self.events.lock();
        let (next, events) = &mut *guard;
        *next += 1;
        events.push_back(make(*next));
        while events.len() > ACTIVITY_CAPACITY {
            events.pop_front();
        }
    }

    /// Events after `after`, oldest first, at most `limit`; and the newest
    /// sequence number (0 when nothing happened yet).
    pub(super) fn since(&self, after: u64, limit: usize) -> (Vec<Activity>, u64) {
        let guard = self.events.lock();
        let (next, events) = &*guard;
        let start = events.partition_point(|e| e.seq <= after);
        (events.range(start..).take(limit).cloned().collect(), *next)
    }
}

/// The traffic log and the activity feed together: what the bus reports of
/// each message sent and read.
#[derive(Debug, Default)]
pub(super) struct Traffic {
    /// What the log shows.
    pub log: TrafficLog,
    /// What the dashboard shows.
    pub feed: Feed,
}

impl Traffic {
    /// Report that `msg` was sent.
    pub(super) fn sent(&self, msg: &Msg, body: Body) {
        self.log.sent(msg, body);
        let (signed, encrypted, text) = match body {
            Body::Plain => (false, false, Some(&*msg.body)),
            Body::Signed => {
                let text = crate::signed::Envelope::parse(&msg.body)
                    .ok()
                    .map(|e| e.body);
                let encrypted = text.is_none_or(|t| t.starts_with("mls/"));
                (true, encrypted, text.filter(|_| !encrypted))
            }
        };
        self.feed.push(|seq| Activity {
            seq,
            kind: ActivityKind::Sent,
            at: rfc3339_ms(msg.created_ms),
            from: msg.from.to_string(),
            to: msg.room.is_none().then(|| msg.to.to_string()),
            room: msg.room.as_deref().map(str::to_string),
            reader: None,
            id: msg.id,
            bytes: msg.body.len(),
            signed,
            encrypted,
            preview: text.map(preview),
        });
    }

    /// Report that `reader` read each of `messages` at `now_ms`.
    pub(super) fn read(&self, reader: &str, messages: &[Arc<Msg>], now_ms: u64) {
        self.log.read(reader, messages);
        for msg in messages {
            let signed = crate::signed::Envelope::parse(&msg.body).ok();
            self.feed.push(|seq| Activity {
                seq,
                kind: ActivityKind::Read,
                at: rfc3339_ms(now_ms),
                from: msg.from.to_string(),
                to: msg.room.is_none().then(|| msg.to.to_string()),
                room: msg.room.as_deref().map(str::to_string),
                reader: Some(reader.to_string()),
                id: msg.id,
                bytes: msg.body.len(),
                signed: signed.is_some(),
                encrypted: signed.is_some_and(|e| e.body.starts_with("mls/")),
                preview: None,
            });
        }
    }
}

/// How many characters of a body a preview keeps.
pub const PREVIEW_CHARS: usize = 100;

/// What a sent line shows of the message body.
///
/// # Examples
///
/// ```
/// use mentischat::bus::TrafficLog;
///
/// assert_eq!(TrafficLog::default(), TrafficLog::Off);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TrafficLog {
    /// No traffic lines.
    #[default]
    Off,
    /// Names, id and size only; no part of the body.
    Metadata,
    /// Also the first line of an unsigned body, cut to [`PREVIEW_CHARS`]
    /// characters. Signed room messages show `signed` in its place.
    WithPreviews,
}

/// Whether a sent body is plain text the sender wrote, or a signed envelope
/// (whose content may be ciphertext) that is never previewed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Body {
    Plain,
    Signed,
}

impl TrafficLog {
    /// Log that `msg` was sent.
    fn sent(self, msg: &Msg, body: Body) {
        if self != Self::Off {
            let line = Sent {
                msg,
                body,
                mode: self,
            };
            tracing::info!("{line}");
        }
    }

    /// Log that `reader` read each of `messages`.
    fn read(self, reader: &str, messages: &[Arc<Msg>]) {
        if self != Self::Off {
            for msg in messages {
                tracing::info!("{}", Read { reader, msg });
            }
        }
    }
}

/// `sent Alpha → Beta (id 42, 312 B) "Waiting for your reply: …"`
struct Sent<'a> {
    msg: &'a Msg,
    body: Body,
    mode: TrafficLog,
}

impl fmt::Display for Sent<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let msg = self.msg;
        write!(f, "sent {} → ", msg.from)?;
        match &msg.room {
            Some(room) => write!(f, "#{room}")?,
            None => write!(f, "{}", msg.to)?,
        }
        write!(f, " (id {}, {}", msg.id, Size(msg.body.len()))?;
        match (self.body, self.mode) {
            (Body::Signed, TrafficLog::WithPreviews) => match signed_text(&msg.body) {
                Some(text) => write!(f, ", signed) \"{}\"", preview(text)),
                None => write!(f, ", signed)"),
            },
            (Body::Signed, _) => write!(f, ", signed)"),
            (Body::Plain, TrafficLog::WithPreviews) => {
                write!(f, ") \"{}\"", preview(&msg.body))
            }
            (Body::Plain, TrafficLog::Off | TrafficLog::Metadata) => write!(f, ")"),
        }
    }
}

/// `read Beta ← Alpha (id 42)` or `read Beta ← Gamma in #release (id 43)`
struct Read<'a> {
    reader: &'a str,
    msg: &'a Msg,
}

impl fmt::Display for Read<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "read {} ← {}", self.reader, self.msg.from)?;
        if let Some(room) = &self.msg.room {
            write!(f, " in #{room}")?;
        }
        write!(f, " (id {})", self.msg.id)
    }
}

/// A body size: bytes under 1 KiB, else KiB to one decimal place.
struct Size(usize);

impl fmt::Display for Size {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0 < 1024 {
            write!(f, "{} B", self.0)
        } else {
            write!(f, "{:.1} KB", self.0 as f64 / 1024.0)
        }
    }
}

/// The text a signed body carries, unless it is end-to-end encrypted (an MLS
/// message, which is ciphertext and never previewed).
fn signed_text(body: &str) -> Option<&str> {
    let text = crate::signed::Envelope::parse(body).ok()?.body;
    (!text.starts_with("mls/")).then_some(text)
}

/// The first line of the trimmed `body`, control characters turned to
/// spaces, cut to [`PREVIEW_CHARS`] characters, with `…` when anything was
/// left out.
fn preview(body: &str) -> String {
    let text = body.trim();
    let first = text.lines().next().unwrap_or("").trim_end();
    let kept: String = first
        .chars()
        .take(PREVIEW_CHARS)
        .map(|c| if c.is_control() { ' ' } else { c })
        .collect();
    let cut = first.len() < text.len() || first.chars().count() > PREVIEW_CHARS;
    if cut {
        format!("{kept}…")
    } else {
        kept
    }
}
