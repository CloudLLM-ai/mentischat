//! Delete-on-read delivery: a pull takes the oldest unread rows that fit the
//! batch caps and deletes them in the same transaction that returns them.
//! Nothing is kept once it has been read.

use rusqlite::params;

use super::{
    now_rfc3339, sqlite_storage, InboxPage, Inner, MailErr, Message, MAX_BATCH, MAX_RETURN_BYTES,
};

pub(super) fn empty_page(name: &crate::names::AgentName) -> InboxPage {
    InboxPage {
        agent: name.to_string(),
        messages: Vec::new(),
        remaining: 0,
    }
}

pub(super) fn touch_last_seen(g: &mut Inner, name: &str) -> Result<(), MailErr> {
    g.conn
        .execute(
            "UPDATE roster SET last_seen = ?2 WHERE name = ?1",
            params![name, now_rfc3339()],
        )
        .map_err(sqlite_storage)
        .map(|_| ())
}

/// Take the next batch for `name` and delete it; `None` when the inbox is
/// empty.
///
/// Up to [`MAX_BATCH`] rows within [`MAX_RETURN_BYTES`] of serialized message
/// JSON (the first message always rides even when oversized — progress must
/// be possible). The rows are deleted before the page is returned, so a
/// message is handed out exactly once; the caller truncates the write-ahead
/// log after releasing the lock.
pub(super) fn take_locked(g: &mut Inner, name: &str) -> Result<Option<InboxPage>, MailErr> {
    let candidates = load_rows(g, name, MAX_BATCH)?;
    if candidates.is_empty() {
        return Ok(None);
    }
    let messages = take_batch(&candidates);
    let tx = g.conn.transaction().map_err(sqlite_storage)?;
    {
        let mut stmt = tx
            .prepare("DELETE FROM messages WHERE id = ?1")
            .map_err(sqlite_storage)?;
        for msg in &messages {
            stmt.execute(params![msg.id]).map_err(sqlite_storage)?;
        }
    }
    let remaining: i64 = tx
        .query_row(
            "SELECT COUNT(*) FROM messages WHERE recipient = ?1",
            params![name],
            |r| r.get(0),
        )
        .map_err(sqlite_storage)?;
    tx.commit().map_err(sqlite_storage)?;
    Ok(Some(InboxPage {
        agent: name.to_string(),
        messages,
        remaining,
    }))
}

/// Select a prefix of `candidates` (already seq-ordered) that fits
/// [`MAX_BATCH`] and [`MAX_RETURN_BYTES`]. The first message is always kept.
///
/// Size is each message's serialized JSON, not its raw body: escaping can
/// make a body several times longer on the wire (a control character becomes
/// six bytes), and the wire size is what has to stay under client truncation
/// limits.
fn take_batch(candidates: &[Message]) -> Vec<Message> {
    let mut take = 0usize;
    let mut bytes = 0usize;
    for msg in candidates.iter().take(MAX_BATCH as usize) {
        let size = wire_len(msg);
        if take > 0 && bytes + size > MAX_RETURN_BYTES {
            break;
        }
        bytes += size;
        take += 1;
    }
    candidates[..take].to_vec()
}

fn wire_len(msg: &Message) -> usize {
    // Serializing plain strings cannot fail; fall back to the body length
    // rather than panic if it ever did.
    serde_json::to_vec(msg).map_or(msg.body.len(), |v| v.len())
}

fn load_rows(g: &Inner, name: &str, limit: i64) -> Result<Vec<Message>, MailErr> {
    let map = |r: &rusqlite::Row| {
        Ok(Message {
            id: r.get(0)?,
            from: r.get(1)?,
            to: name.to_string(),
            body: r.get(2)?,
            created_at: r.get(3)?,
        })
    };
    let mut stmt = g
        .conn
        .prepare(
            "SELECT id, sender, body, created_at FROM messages
             WHERE recipient = ?1 ORDER BY seq ASC LIMIT ?2",
        )
        .map_err(sqlite_storage)?;
    let rows = stmt
        .query_map(params![name, limit], map)
        .map_err(sqlite_storage)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(sqlite_storage)?;
    Ok(rows)
}
