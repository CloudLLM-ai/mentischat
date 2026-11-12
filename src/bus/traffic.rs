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

use std::fmt;
use std::sync::Arc;

use super::log::Msg;

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
    pub(super) fn sent(self, msg: &Msg, body: Body) {
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
    pub(super) fn read(self, reader: &str, messages: &[Arc<Msg>]) {
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
