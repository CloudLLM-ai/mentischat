//! Batch-and-ack delivery: promoting `pending` rows to `in_flight`,
//! redelivering unacked rows within the same caps, and deleting acked rows.

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

/// Deletes the `in_flight` rows named in `ack_ids` that `name` owns, in one
/// transaction, and returns how many were removed. Unknown ids are ignored.
pub(super) fn delete_acked(g: &mut Inner, name: &str, ack_ids: &[String]) -> Result<i64, MailErr> {
    if ack_ids.is_empty() {
        return Ok(0);
    }
    let tx = g.conn.transaction().map_err(sqlite_storage)?;
    let mut acked = 0i64;
    {
        let mut stmt = tx
            .prepare(
                "DELETE FROM messages
                 WHERE id = ?1 AND recipient = ?2 AND state = 'in_flight'",
            )
            .map_err(sqlite_storage)?;
        for id in ack_ids {
            acked += stmt.execute(params![id, name]).map_err(sqlite_storage)? as i64;
        }
    }
    tx.commit().map_err(sqlite_storage)?;
    Ok(acked)
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

/// Deliver per batch+ack rules; `None` means both states are empty.
///
/// Redeliver unacked `in_flight` FIFO within the same batch caps as a fresh
/// promote; else promote up to `MAX_BATCH` pending rows within
/// `MAX_RETURN_BYTES` of serialized message JSON (the first message always rides even when
/// oversized — progress must be possible).
pub(super) fn deliver_locked(g: &mut Inner, name: &str) -> Result<Option<InboxPage>, MailErr> {
    let in_flight = load_rows(g, name, "in_flight", None)?;
    if !in_flight.is_empty() {
        let batch = take_batch(&in_flight);
        return page(g, name, batch).map(Some);
    }

    let candidates = load_rows(g, name, "pending", Some(MAX_BATCH))?;
    if candidates.is_empty() {
        return Ok(None);
    }
    let batch = take_batch(&candidates);
    let tx = g.conn.transaction().map_err(sqlite_storage)?;
    {
        let mut stmt = tx
            .prepare("UPDATE messages SET state = 'in_flight' WHERE id = ?1")
            .map_err(sqlite_storage)?;
        for msg in &batch {
            stmt.execute(params![msg.id]).map_err(sqlite_storage)?;
        }
    }
    tx.commit().map_err(sqlite_storage)?;
    page(g, name, batch).map(Some)
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

fn page(g: &Inner, name: &str, messages: Vec<Message>) -> Result<InboxPage, MailErr> {
    // Unacked rows not in this page (leftover in_flight + still-pending).
    let total: i64 = g
        .conn
        .query_row(
            "SELECT COUNT(*) FROM messages WHERE recipient = ?1",
            params![name],
            |r| r.get(0),
        )
        .map_err(sqlite_storage)?;
    let remaining = (total - messages.len() as i64).max(0);
    Ok(InboxPage {
        agent: name.to_string(),
        messages,
        remaining,
    })
}

fn load_rows(
    g: &Inner,
    name: &str,
    state: &str,
    limit: Option<i64>,
) -> Result<Vec<Message>, MailErr> {
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
             WHERE recipient = ?1 AND state = ?2 ORDER BY seq ASC LIMIT ?3",
        )
        .map_err(sqlite_storage)?;
    let rows = stmt
        .query_map(params![name, state, limit.unwrap_or(i64::MAX)], map)
        .map_err(sqlite_storage)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(sqlite_storage)?;
    Ok(rows)
}
