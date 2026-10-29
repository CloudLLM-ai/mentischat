//! Delivery and history: a pull takes the oldest unread rows that fit the
//! batch caps and marks them read in the same transaction that returns them;
//! a history page reads back what an agent sent and has read.

use rusqlite::params;

use super::{
    now_rfc3339, sqlite_storage, HistoryPage, InboxPage, Inner, MailErr, Message, MAX_BATCH,
    MAX_RETURN_BYTES,
};

pub(super) fn empty_page(name: &str) -> InboxPage {
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

/// Take the next batch of unread mail for `name` and mark it read; `None`
/// when nothing is unread.
///
/// Up to [`MAX_BATCH`] rows within [`MAX_RETURN_BYTES`] of serialized message
/// JSON (the first message always rides even when oversized — progress must
/// be possible). The rows are marked read before the page is returned, so a
/// pull hands each message out exactly once.
pub(super) fn take_locked(g: &mut Inner, name: &str) -> Result<Option<InboxPage>, MailErr> {
    let candidates = load_unread(g, name)?;
    if candidates.is_empty() {
        return Ok(None);
    }
    let take = fitting(candidates.iter());
    let read_at = now_rfc3339();
    let tx = g.conn.transaction().map_err(sqlite_storage)?;
    {
        let mut stmt = tx
            .prepare("UPDATE messages SET read_at = ?2 WHERE id = ?1")
            .map_err(sqlite_storage)?;
        for msg in &candidates[..take] {
            stmt.execute(params![msg.id, read_at])
                .map_err(sqlite_storage)?;
        }
    }
    let remaining: i64 = tx
        .query_row(
            "SELECT COUNT(*) FROM messages WHERE recipient = ?1 AND read_at IS NULL",
            params![name],
            |r| r.get(0),
        )
        .map_err(sqlite_storage)?;
    tx.commit().map_err(sqlite_storage)?;
    let messages = candidates
        .into_iter()
        .take(take)
        .map(|msg| Message {
            read_at: Some(read_at.clone()),
            ..msg
        })
        .collect();
    Ok(Some(InboxPage {
        agent: name.to_string(),
        messages,
        remaining,
    }))
}

/// One history page for registered `me` (see `Bus::history`): newest first
/// from the database, the prefix that fits the batch caps, returned oldest
/// first. Only mail created since `me`'s registration began counts, so an
/// agent that takes a name another agent used before sees none of its mail.
pub(super) fn history_locked(
    g: &Inner,
    me: &str,
    with: Option<&str>,
    before: Option<i64>,
) -> Result<HistoryPage, MailErr> {
    let registered_at: String = g
        .conn
        .query_row(
            "SELECT registered_at FROM roster WHERE name = ?1",
            params![me],
            |r| r.get(0),
        )
        .map_err(sqlite_storage)?;
    let mut stmt = g
        .conn
        .prepare(
            "SELECT id, sender, recipient, body, created_at, read_at, seq FROM messages
              WHERE created_at >= ?2
                AND ((sender = ?1 AND (?3 IS NULL OR recipient = ?3))
                  OR (recipient = ?1 AND read_at IS NOT NULL AND (?3 IS NULL OR sender = ?3)))
                AND (?4 IS NULL OR seq < ?4)
              ORDER BY seq DESC LIMIT ?5",
        )
        .map_err(sqlite_storage)?;
    // One row past a full page says whether older messages exist.
    let rows: Vec<(Message, i64)> = stmt
        .query_map(
            params![me, registered_at, with, before, MAX_BATCH + 1],
            |r| Ok((message_row(r)?, r.get(6)?)),
        )
        .map_err(sqlite_storage)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(sqlite_storage)?;
    let take = fitting(rows.iter().map(|(msg, _)| msg));
    let next = (rows.len() > take).then(|| rows[take - 1].1);
    let messages = rows
        .into_iter()
        .take(take)
        .map(|(msg, _)| msg)
        .rev()
        .collect();
    Ok(HistoryPage {
        agent: me.to_string(),
        messages,
        next,
    })
}

/// How many of `candidates`, in order, fit [`MAX_BATCH`] and
/// [`MAX_RETURN_BYTES`]. The first message always fits.
///
/// Size is each message's serialized JSON, not its raw body: escaping can
/// make a body several times longer on the wire (a control character becomes
/// six bytes), and the wire size is what has to stay under client truncation
/// limits.
fn fitting<'a>(candidates: impl Iterator<Item = &'a Message>) -> usize {
    let mut take = 0usize;
    let mut bytes = 0usize;
    for msg in candidates.take(MAX_BATCH as usize) {
        let size = wire_len(msg);
        if take > 0 && bytes + size > MAX_RETURN_BYTES {
            break;
        }
        bytes += size;
        take += 1;
    }
    take
}

fn wire_len(msg: &Message) -> usize {
    // Serializing plain strings cannot fail; fall back to the body length
    // rather than panic if it ever did.
    serde_json::to_vec(msg).map_or(msg.body.len(), |v| v.len())
}

/// A `messages` row selected as `id, sender, recipient, body, created_at,
/// read_at`.
fn message_row(r: &rusqlite::Row) -> rusqlite::Result<Message> {
    Ok(Message {
        id: r.get(0)?,
        from: r.get(1)?,
        to: r.get(2)?,
        body: r.get(3)?,
        created_at: r.get(4)?,
        read_at: r.get(5)?,
    })
}

/// The oldest unread rows for `name`, at most [`MAX_BATCH`].
fn load_unread(g: &Inner, name: &str) -> Result<Vec<Message>, MailErr> {
    let mut stmt = g
        .conn
        .prepare(
            "SELECT id, sender, recipient, body, created_at, read_at FROM messages
             WHERE recipient = ?1 AND read_at IS NULL ORDER BY seq ASC LIMIT ?2",
        )
        .map_err(sqlite_storage)?;
    let rows = stmt
        .query_map(params![name, MAX_BATCH], message_row)
        .map_err(sqlite_storage)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(sqlite_storage)?;
    Ok(rows)
}
