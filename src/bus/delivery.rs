//! History: a page of what an agent sent and has read, newest first off the
//! conversation logs, returned oldest first.
//!
//! Delivery itself (taking unread mail and marking it read) is
//! `State::take`, which `Bus::check_inbox` calls under its lock.

use std::sync::Arc;

use super::log::Msg;
use super::state::{convo_has, convo_key, fitting, Agent, Key, Page, State};
use super::{HistoryPage, MAX_BATCH};

/// One history page's messages for `me` (see `Bus::history`) and the
/// `next` cursor: the newest messages with an id below `before`, the prefix
/// that fits the batch caps, returned oldest first.
///
/// Only mail created since `me`'s registration began counts, so an agent
/// that takes a name another agent used before sees none of its mail. With
/// `with` (a folded name) only that conversation is read; without it every
/// conversation `me` is in is merged by id, newest first, stopping after the
/// one message past a full page that says whether older mail exists.
pub(super) fn history_rows(
    state: &State,
    me: &Agent,
    with: Option<&str>,
    before: Option<i64>,
) -> (Vec<Arc<Msg>>, Option<i64>) {
    let me_key = Key::fold(&me.name);
    let registered_ms = me.inner.lock().registered_ms;
    let before = before.map(|b| u64::try_from(b).unwrap_or(0));
    let visible = |m: &&Arc<Msg>| {
        m.created_ms >= registered_ms
            && (m.from.eq_ignore_ascii_case(&me.name)
                || (m.to.eq_ignore_ascii_case(&me.name) && m.read_at_ms().is_some()))
    };
    let convos: Vec<_> = match with {
        Some(peer) => state
            .convos
            .get(convo_key(&me_key, &Key::fold(peer)).as_str())
            .into_iter()
            .collect(),
        None => state
            .convos
            .values_where(|key| convo_has(key, me_key.as_str())),
    };
    // One row past a full page says whether older messages exist; each log
    // is locked just long enough to copy its newest candidates out.
    let want = MAX_BATCH as usize + 1;
    let mut streams: Vec<_> = convos
        .iter()
        .map(|convo| {
            let log = convo.log.lock();
            let newest: Vec<_> = log
                .newest_first(before)
                .filter(visible)
                .take(want)
                .cloned()
                .collect();
            newest.into_iter().peekable()
        })
        .collect();
    let mut rows = Vec::with_capacity(want);
    while rows.len() < want {
        let newest = streams
            .iter_mut()
            .enumerate()
            .filter_map(|(i, s)| s.peek().map(|m| (m.id, i)))
            .max();
        let Some((_, i)) = newest else { break };
        rows.extend(streams[i].next());
    }
    let take = fitting(rows.iter().map(|m| m.page_len()), Page::DEFAULT);
    let next = (rows.len() > take).then(|| rows[take - 1].id as i64);
    rows.truncate(take);
    rows.reverse();
    (rows, next)
}

/// The wire page for `me` from [`history_rows`]' output. Builds the strings,
/// so call it after releasing the bus lock.
pub(super) fn history_page(me: &str, rows: &[Arc<Msg>], next: Option<i64>) -> HistoryPage {
    HistoryPage {
        agent: me.to_string(),
        messages: rows.iter().map(|m| m.to_wire()).collect(),
        next,
    }
}
