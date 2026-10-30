//! Messages and the conversation logs that hold them.
//!
//! A message is written once, shared as an [`Arc`] between the log it lives
//! in, its recipient's unread queue and the read-order queue, and never
//! mutated except for its read time and its `dead` mark, both atomics. A
//! conversation log is an append-only [`VecDeque`] sorted by message id; the
//! mail between two agents is one log. Rooms (a later phase) are the same
//! shape: a log plus a cursor per reader.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::Arc;

use super::clock::rfc3339_ms;
use super::Message;

/// Bytes of a message's wire JSON that do not depend on its content: the
/// punctuation, the key names, a 24-character `createdAt` and `"readAt":null`.
const WIRE_FIXED_BYTES: usize =
    r#"{"id":"","from":"","to":"","body":,"createdAt":"2000-01-01T00:00:00.000Z","readAt":null}"#
        .len();

/// A mail message. Immutable once sent, apart from the two atomics.
#[derive(Debug)]
pub(super) struct Msg {
    /// Delivery order: strictly increasing for the life of the process (and
    /// across a persisted restart), assigned under the bus lock. Also the
    /// `history` cursor.
    pub id: u64,
    /// Sender, spelled as the roster stored it when the message was sent.
    pub from: Arc<str>,
    /// Recipient, spelled as the roster stored it when the message was sent.
    pub to: Arc<str>,
    /// Body exactly as sent (never trimmed).
    pub body: Box<str>,
    /// Milliseconds since the Unix epoch when the send was accepted.
    pub created_ms: u64,
    /// Milliseconds since the Unix epoch when the recipient read it; 0 while
    /// unread.
    pub read_ms: AtomicU64,
    /// Set when the message is gone (expired, pruned, or discarded with its
    /// recipient). Logs and queues drop dead entries lazily.
    pub dead: AtomicBool,
    /// Serialized size of the message as an unread [`Message`], computed once
    /// so sizing a page never serializes anything.
    pub wire_len: usize,
}

impl Msg {
    /// Build a message. `body_json_len` is the body's size once JSON-escaped
    /// and quoted, measured by the caller outside the bus lock.
    pub(super) fn new(
        id: u64,
        from: Arc<str>,
        to: Arc<str>,
        body: Box<str>,
        body_json_len: usize,
        created_ms: u64,
    ) -> Self {
        // Roster names are ASCII without characters JSON escapes, and an id
        // is decimal digits, so their wire size is their length.
        let wire_len = WIRE_FIXED_BYTES + digits(id) + from.len() + to.len() + body_json_len;
        Self {
            id,
            from,
            to,
            body,
            created_ms,
            read_ms: AtomicU64::new(0),
            dead: AtomicBool::new(false),
            wire_len,
        }
    }

    /// Milliseconds since the Unix epoch when it was read; `None` while unread.
    pub(super) fn read_at_ms(&self) -> Option<u64> {
        Some(self.read_ms.load(Relaxed)).filter(|&ms| ms != 0)
    }

    /// True once the message is gone.
    pub(super) fn is_dead(&self) -> bool {
        self.dead.load(Relaxed)
    }

    /// Mark the message gone. Returns true for the call that did it.
    pub(super) fn kill(&self) -> bool {
        !self.dead.swap(true, Relaxed)
    }

    /// Serialized size as it would go in a page: [`Msg::wire_len`] plus what
    /// a read time adds over `null`.
    pub(super) fn page_len(&self) -> usize {
        // `"2000-01-01T00:00:00.000Z"` is 26 bytes where `null` is 4.
        self.wire_len + self.read_at_ms().map_or(0, |_| 22)
    }

    /// The wire shape. Builds the id and timestamps here, so call it after
    /// releasing the bus lock.
    pub(super) fn to_wire(&self) -> Message {
        Message {
            id: self.id.to_string(),
            from: self.from.to_string(),
            to: self.to.to_string(),
            body: self.body.to_string(),
            created_at: rfc3339_ms(self.created_ms),
            read_at: self.read_at_ms().map(rfc3339_ms),
        }
    }
}

fn digits(n: u64) -> usize {
    n.checked_ilog10().map_or(1, |d| d as usize + 1)
}

/// The size of `body` once JSON-escaped and quoted, counted without building
/// the escaped string.
pub(super) fn json_len(body: &str) -> usize {
    struct Count(usize);
    impl std::io::Write for Count {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0 += buf.len();
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut count = Count(0);
    // Serializing a str cannot fail and Count never errors; the fallback
    // only keeps the signature honest.
    serde_json::to_writer(&mut count, body).map_or(body.len(), |()| count.0)
}

/// Dead entries a log tolerates before its first compaction.
const MIN_COMPACT_LEN: usize = 64;

/// Every message exchanged between two agents, in both directions, oldest
/// first. Ids are strictly increasing along the log, so a position is found
/// by binary search however many dead entries lie between.
#[derive(Debug)]
pub(super) struct Convo {
    log: VecDeque<Arc<Msg>>,
    /// Length at which the next append sweeps dead entries out of the middle.
    compact_at: usize,
}

impl Default for Convo {
    fn default() -> Self {
        Self {
            log: VecDeque::new(),
            compact_at: MIN_COMPACT_LEN,
        }
    }
}

impl Convo {
    /// Append `msg`, whose id exceeds every id in the log. Dead entries at
    /// the front are dropped now; the ones between live messages when the
    /// log doubles past its last compaction, so appends stay O(1) amortized
    /// and a log never holds more than twice its live messages.
    pub(super) fn push(&mut self, msg: Arc<Msg>) {
        while self.log.front().is_some_and(|m| m.is_dead()) {
            self.log.pop_front();
        }
        if self.log.len() >= self.compact_at {
            self.compact();
        }
        self.log.push_back(msg);
    }

    /// Drop every dead entry.
    pub(super) fn compact(&mut self) {
        self.log.retain(|m| !m.is_dead());
        self.compact_at = (self.log.len() * 2).max(MIN_COMPACT_LEN);
    }

    /// True when no message in the log is alive.
    pub(super) fn is_empty(&self) -> bool {
        self.log.iter().all(|m| m.is_dead())
    }

    /// Live messages with an id below `before` (all of them for `None`),
    /// newest first.
    pub(super) fn newest_first(&self, before: Option<u64>) -> impl Iterator<Item = &Arc<Msg>> {
        let end = before.map_or(self.log.len(), |b| self.log.partition_point(|m| m.id < b));
        self.log.range(..end).rev().filter(|m| !m.is_dead())
    }

    /// Every live message, oldest first.
    pub(super) fn live(&self) -> impl Iterator<Item = &Arc<Msg>> {
        self.log.iter().filter(|m| !m.is_dead())
    }
}
