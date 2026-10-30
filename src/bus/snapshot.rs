//! The optional on-disk snapshot (`MENTISCHAT_PERSIST=1`): the whole mailbox
//! as JSON, written once on a clean stop and read, then deleted, at the next
//! start. Without persistence nothing is ever written.
//!
//! `{data_dir}/snapshot.json` is created `0600` inside a `0700` directory.
//! A crash leaves no snapshot: the mailbox lives only in memory between
//! starts and stops.

use std::collections::HashMap;
use std::fmt;
use std::fs;
use std::io::{self, BufWriter};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use super::log::{json_len, Msg, Target};
use super::rooms::{Member, Room, RoomState};
use super::state::{convo_key, Agent, Hold, Key, State};
use super::{
    MAX_PENDING_PER_AGENT, MAX_ROOMS, MAX_ROOMS_PER_AGENT, MAX_ROOM_MEMBERS, MAX_ROSTER,
    MAX_TOTAL_MESSAGES,
};
use crate::names::{AgentName, RoomName};

/// File name of the snapshot inside the data directory.
const SNAPSHOT_FILE: &str = "snapshot.json";
/// Layout version written to and required of a snapshot.
const VERSION: u32 = 1;

/// Failure modes of writing, reading or deleting the snapshot.
#[derive(Debug)]
pub enum SnapshotError {
    /// Filesystem error creating the directory, or reading, writing or
    /// deleting the file.
    Io(io::Error),
    /// The file is not a snapshot this build can read.
    Corrupt(serde_json::Error),
    /// The file was written by a build with a different layout.
    Version(u32),
}

impl fmt::Display for SnapshotError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "data dir: {e}"),
            Self::Corrupt(e) => write!(
                f,
                "{SNAPSHOT_FILE} is not readable ({e}); delete it to start empty"
            ),
            Self::Version(v) => write!(
                f,
                "{SNAPSHOT_FILE} has layout version {v}, expected {VERSION}; delete it to start empty"
            ),
        }
    }
}

impl std::error::Error for SnapshotError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            Self::Corrupt(e) => Some(e),
            Self::Version(_) => None,
        }
    }
}

/// Delete the snapshot under `data_dir`, if there is one, so the next start
/// is empty.
///
/// # Errors
///
/// Any filesystem error other than a missing file.
pub fn remove_snapshot(data_dir: &Path) -> Result<(), SnapshotError> {
    match fs::remove_file(path(data_dir)) {
        Err(e) if e.kind() != io::ErrorKind::NotFound => Err(SnapshotError::Io(e)),
        _ => Ok(()),
    }
}

fn path(data_dir: &Path) -> PathBuf {
    data_dir.join(SNAPSHOT_FILE)
}

/// The mailbox as it is written to disk.
#[derive(Debug, Serialize, Deserialize)]
pub(super) struct Snapshot {
    version: u32,
    next_id: u64,
    messages_sent: u64,
    agents: Vec<AgentRec>,
    holds: Vec<HoldRec>,
    /// Live messages, oldest first.
    messages: Vec<MsgRec>,
    /// Rooms with their retained messages and members' cursors. Absent in a
    /// snapshot from before rooms.
    #[serde(default)]
    rooms: Vec<RoomRec>,
}

#[derive(Debug, Serialize, Deserialize)]
struct RoomRec {
    name: String,
    private: bool,
    description: Option<String>,
    members: Vec<MemberRec>,
    /// Folded names invited and not yet joined.
    invited: Vec<String>,
    /// Retained messages, oldest first; `to` and `read_ms` are unused.
    messages: Vec<MsgRec>,
}

#[derive(Debug, Serialize, Deserialize)]
struct MemberRec {
    name: String,
    cursor: u64,
    skipped: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct AgentRec {
    name: String,
    provider: Option<String>,
    description: Option<String>,
    registered_ms: u64,
    last_seen_ms: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct HoldRec {
    /// Folded name.
    key: String,
    until_ms: u64,
    registered_ms: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct MsgRec {
    id: u64,
    from: String,
    to: String,
    body: String,
    created_ms: u64,
    read_ms: u64,
}

/// Write `snapshot` to `{data_dir}/snapshot.json`, creating the directory
/// `0700` and the file `0600`. Goes through a temporary file and a rename, so
/// a failure never leaves half a snapshot.
pub(super) fn save(data_dir: &Path, snapshot: &Snapshot) -> Result<(), SnapshotError> {
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
    fs::create_dir_all(data_dir).map_err(SnapshotError::Io)?;
    fs::set_permissions(data_dir, fs::Permissions::from_mode(0o700)).map_err(SnapshotError::Io)?;
    let tmp = data_dir.join(format!("{SNAPSHOT_FILE}.tmp"));
    let file = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(&tmp)
        .map_err(SnapshotError::Io)?;
    serde_json::to_writer(BufWriter::new(file), snapshot)
        .map_err(|e| SnapshotError::Io(io::Error::other(e)))?;
    fs::rename(&tmp, path(data_dir)).map_err(SnapshotError::Io)
}

/// Read the snapshot under `data_dir` and delete the file; `None` when there
/// is none. A snapshot that cannot be read is left in place and reported.
pub(super) fn restore(data_dir: &Path) -> Result<Option<State>, SnapshotError> {
    let bytes = match fs::read(path(data_dir)) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(SnapshotError::Io(e)),
    };
    let snapshot: Snapshot = serde_json::from_slice(&bytes).map_err(SnapshotError::Corrupt)?;
    if snapshot.version != VERSION {
        return Err(SnapshotError::Version(snapshot.version));
    }
    remove_snapshot(data_dir)?;
    Ok(Some(rebuild(snapshot)))
}

/// Everything in `state` worth keeping.
pub(super) fn capture(state: &State) -> Snapshot {
    let convos = state.convos.values_where(|_| true);
    let mut messages: Vec<MsgRec> = convos
        .iter()
        .flat_map(|convo| {
            let log = convo.log.lock();
            log.live().map(msg_rec).collect::<Vec<_>>()
        })
        .collect();
    messages.sort_unstable_by_key(|m| m.id);
    let holds = state.holds.lock();
    Snapshot {
        version: VERSION,
        next_id: state.next_id.0.load(Relaxed),
        messages_sent: state.messages_sent(),
        agents: state
            .agents_sorted()
            .iter()
            .map(|a| {
                let st = a.inner.lock();
                AgentRec {
                    name: a.name.to_string(),
                    provider: st.provider.clone(),
                    description: st.description.clone(),
                    registered_ms: st.registered_ms,
                    last_seen_ms: st.last_seen_ms,
                }
            })
            .collect(),
        holds: holds
            .iter()
            .map(|(key, h)| HoldRec {
                key: key.clone(),
                until_ms: h.until_ms,
                registered_ms: h.registered_ms,
            })
            .collect(),
        messages,
        rooms: state
            .rooms
            .values_where(|_| true)
            .iter()
            .filter_map(|room| room_rec(room))
            .collect(),
    }
}

fn room_rec(room: &Room) -> Option<RoomRec> {
    let st = room.inner.lock();
    (!st.retired).then(|| RoomRec {
        name: room.name.to_string(),
        private: room.private,
        description: room.description.clone(),
        members: st
            .members
            .values()
            .map(|m| MemberRec {
                name: m.name.to_string(),
                cursor: m.cursor,
                skipped: m.skipped,
            })
            .collect(),
        invited: st.invited.iter().cloned().collect(),
        messages: st.log.iter().map(msg_rec).collect(),
    })
}

fn msg_rec(m: &Arc<Msg>) -> MsgRec {
    MsgRec {
        id: m.id,
        from: m.from.to_string(),
        to: m.to.to_string(),
        body: m.body.to_string(),
        created_ms: m.created_ms,
        read_ms: m.read_at_ms().unwrap_or(0),
    }
}

/// The largest message id a snapshot may carry. Ids beyond it would leave no
/// room to number what comes next, so such messages are dropped.
const ID_LIMIT: u64 = u64::MAX / 2;

/// A [`State`] holding what `snapshot` described. The file is trusted to be
/// ours but not to be sane: names are validated, messages sorted by id and
/// deduplicated, and every cap a live daemon enforces (roster, rooms,
/// per-agent and per-room limits, the unread and total message counts, the
/// byte budget) is applied again, skipping what does not fit and logging one
/// warning. Mail addressed to an agent that is not on the roster is dropped:
/// it would have been discarded with the agent.
fn rebuild(snapshot: Snapshot) -> State {
    let state = State::new();
    let mut skipped = 0usize;
    state.sent_by_gone.store(snapshot.messages_sent, Relaxed);
    skipped += restore_agents(&state, snapshot.agents);
    {
        let mut holds = state.holds.lock();
        for rec in snapshot.holds.into_iter().take(MAX_ROSTER as usize) {
            holds.insert(
                rec.key,
                Hold {
                    until_ms: rec.until_ms,
                    registered_ms: rec.registered_ms,
                },
            );
        }
    }
    let mut messages = snapshot.messages;
    sort_dedupe(&mut messages);
    let top = messages.last().map_or(0, |m| m.id.min(ID_LIMIT));
    state.next_id.0.store(
        snapshot.next_id.min(ID_LIMIT).max(top.saturating_add(1)),
        Relaxed,
    );
    for rec in snapshot.rooms {
        skipped += restore_room(&state, rec);
    }
    let mut names: HashMap<String, Arc<str>> = HashMap::new();
    let mut read = Vec::new();
    for rec in messages {
        if !restore_message(&state, &mut names, &mut read, rec) {
            skipped += 1;
        }
    }
    read.sort_by_key(|(_, m): &(String, Arc<Msg>)| m.read_at_ms());
    for (key, msg) in read {
        if let Some(agent) = state.roster.get(&key) {
            let mut st = agent.inner.lock();
            st.read.push_back(msg);
            agent.publish_oldest_read(&st);
            if !agent.indexed.swap(true, Relaxed) {
                state.readers.insert(key.clone(), agent.clone());
            }
        }
    }
    if skipped > 0 {
        tracing::warn!(
            skipped,
            "the snapshot held entries this build does not allow (limits, names, duplicate ids); they were dropped"
        );
    }
    state
}

/// Order messages by id and keep one of each id.
fn sort_dedupe(messages: &mut Vec<MsgRec>) {
    messages.sort_by_key(|m| m.id);
    messages.dedup_by_key(|m| m.id);
}

/// Put the snapshot's agents on the roster, up to [`MAX_ROSTER`], skipping
/// invalid or repeated names. Returns how many were skipped.
fn restore_agents(state: &State, agents: Vec<AgentRec>) -> usize {
    let mut skipped = 0;
    for rec in agents {
        let key = rec.name.to_ascii_lowercase();
        if AgentName::parse(&rec.name).is_err()
            || state.roster.get(&key).is_some()
            || state.roster.len() >= MAX_ROSTER as usize
        {
            skipped += 1;
            continue;
        }
        let gen = state.next_gen.fetch_add(1, Relaxed);
        let agent = Agent::new(
            rec.name.into(),
            gen,
            rec.provider,
            rec.description,
            rec.registered_ms,
            rec.last_seen_ms,
        );
        state.add_agent(key, Arc::new(agent));
    }
    skipped
}

/// File one snapshot message where `State::send` would have, as read or
/// unread as it was. Read ones are collected into `read`, with their
/// recipient's key, for the caller to queue in read order. False when it was
/// dropped: unknown or invalid names, an id out of range, or a cap reached.
fn restore_message(
    state: &State,
    names: &mut HashMap<String, Arc<str>>,
    read: &mut Vec<(String, Arc<Msg>)>,
    rec: MsgRec,
) -> bool {
    let (from_key, to_key) = (rec.from.to_ascii_lowercase(), rec.to.to_ascii_lowercase());
    let Some(recipient) = state.roster.get(&to_key) else {
        return false;
    };
    if rec.id > ID_LIMIT || AgentName::parse(&rec.from).is_err() {
        return false;
    }
    let unread = rec.read_ms == 0;
    let cost = rec.body.len() + super::MESSAGE_OVERHEAD_BYTES;
    if state.total.0.load(Relaxed) >= MAX_TOTAL_MESSAGES as usize
        || (unread
            && (state.unread.0.load(Relaxed) >= MAX_TOTAL_MESSAGES as usize
                || recipient.inner.lock().unread.len() >= MAX_PENDING_PER_AGENT as usize))
        || !state.try_reserve_bytes(cost)
    {
        return false;
    }
    let from: Arc<str> = state.roster.get(&from_key).map_or_else(
        || {
            names
                .entry(from_key.clone())
                .or_insert(rec.from.into())
                .clone()
        },
        |a| a.name.clone(),
    );
    let body: Box<str> = rec.body.into();
    let body_len = json_len(&body);
    let msg = Arc::new(Msg::new(
        rec.id,
        from,
        Target {
            to: recipient.name.clone(),
            to_gen: recipient.gen,
            room: None,
        },
        body,
        body_len,
        rec.created_ms,
    ));
    msg.read_ms.store(rec.read_ms, Relaxed);
    state
        .convos
        .get_or_insert_with(
            convo_key(&Key::fold(&from_key), &Key::fold(&to_key)).as_str(),
            Arc::default,
        )
        .log
        .lock()
        .push(msg.clone());
    state.total.0.fetch_add(1, Relaxed);
    if unread {
        state.unread.0.fetch_add(1, Relaxed);
        recipient.inner.lock().unread.push_back(msg);
    } else {
        read.push((to_key, msg));
    }
    true
}

/// Rebuild one room: its members that are still on the roster (each told
/// where to look), its retained messages, and the unread counts those and
/// the saved cursors imply. Returns how many entries it dropped for being
/// invalid or over a cap (the whole room counts as one).
fn restore_room(state: &State, rec: RoomRec) -> usize {
    let Ok(room_name) = RoomName::parse(&rec.name) else {
        return 1;
    };
    let key = room_name.folded();
    if state.rooms.get(&key).is_some() || state.rooms.len() >= MAX_ROOMS {
        return 1;
    }
    let mut skipped = 0;
    let room_is_private = rec.private;
    let name: Arc<str> = room_name.as_str().into();
    let mut messages = rec.messages;
    sort_dedupe(&mut messages);
    messages.retain(|m| m.id <= ID_LIMIT && AgentName::parse(&m.from).is_ok());
    // Keep the newest a room holds.
    let excess = messages.len().saturating_sub(state.room_capacity);
    skipped += excess;
    let mut st = RoomState::default();
    let mut reserved = 0;
    for m in messages.into_iter().skip(excess) {
        let cost = m.body.len() + super::MESSAGE_OVERHEAD_BYTES;
        if !state.try_reserve_bytes(cost) {
            skipped += 1;
            continue;
        }
        reserved += cost;
        let body: Box<str> = m.body.into();
        let body_len = json_len(&body);
        let target = Target {
            to: state.no_agent.clone(),
            to_gen: 0,
            room: Some(name.clone()),
        };
        st.log.push_back(Arc::new(Msg::new(
            m.id,
            m.from.into(),
            target,
            body,
            body_len,
            m.created_ms,
        )));
    }
    let mut joined = Vec::new();
    for m in rec.members {
        let Some(agent) = state.roster.get(&m.name.to_ascii_lowercase()) else {
            continue;
        };
        if st.members.len() >= MAX_ROOM_MEMBERS
            || agent.inner.lock().rooms.len() >= MAX_ROOMS_PER_AGENT
        {
            skipped += 1;
            continue;
        }
        let unread = st
            .log
            .iter()
            .filter(|msg| msg.id > m.cursor && !msg.from.eq_ignore_ascii_case(&agent.name))
            .count();
        let member = Member {
            name: agent.name.clone(),
            signal: agent.signal.clone(),
            cursor: m.cursor,
            unread,
            skipped: m.skipped,
        };
        st.members
            .insert(Key::fold(&agent.name).as_str().to_string(), member);
        joined.push(agent);
    }
    if st.members.is_empty() {
        state.release_bytes(reserved);
        return skipped + 1;
    }
    let room_for_invites = MAX_ROOM_MEMBERS - st.members.len();
    st.invited = rec
        .invited
        .into_iter()
        .filter(|who| AgentName::parse(who).is_ok())
        .take(room_for_invites)
        .collect();
    st.publish_oldest();
    let room = Arc::new(Room {
        name,
        private: rec.private,
        description: rec.description,
        oldest: st.oldest.clone(),
        inner: parking_lot::Mutex::new(st),
    });
    for agent in joined {
        agent.inner.lock().rooms.push(room.clone());
    }
    let invited: Vec<String> = room.inner.lock().invited.iter().cloned().collect();
    for who in invited {
        if let Some(agent) = state.roster.get(&who) {
            let mut st = agent.inner.lock();
            if st.invited_to.len() < MAX_ROOMS_PER_AGENT {
                st.invited_to.insert(key.clone());
            }
        }
    }
    if state.rooms.try_insert(&key, room).is_ok() {
        if !room_is_private {
            state.public_names.write().insert(key);
        }
    } else {
        state.release_bytes(reserved);
        skipped += 1;
    }
    skipped
}
