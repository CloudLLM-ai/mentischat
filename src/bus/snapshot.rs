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

/// A [`State`] holding what `snapshot` described. Mail addressed to an agent
/// that is not on the roster is dropped: it would have been discarded with
/// the agent.
fn rebuild(snapshot: Snapshot) -> State {
    let state = State::new();
    state.sent_by_gone.store(snapshot.messages_sent, Relaxed);
    for rec in snapshot.agents {
        let key = rec.name.to_ascii_lowercase();
        let gen = state.next_gen.fetch_add(1, Relaxed);
        let agent = Agent::new(
            rec.name.into(),
            gen,
            rec.provider,
            rec.description,
            rec.registered_ms,
            rec.last_seen_ms,
        );
        state.roster.insert(key, Arc::new(agent));
    }
    {
        let mut holds = state.holds.lock();
        for rec in snapshot.holds {
            let hold = Hold {
                until_ms: rec.until_ms,
                registered_ms: rec.registered_ms,
            };
            holds.insert(rec.key, hold);
        }
    }
    let mut messages = snapshot.messages;
    messages.sort_unstable_by_key(|m| m.id);
    let next_id = snapshot
        .next_id
        .max(messages.last().map_or(1, |m| m.id + 1));
    state.next_id.0.store(next_id, Relaxed);
    let mut names: HashMap<String, Arc<str>> = HashMap::new();
    let mut read = Vec::new();
    let next_after_rooms = snapshot
        .rooms
        .iter()
        .flat_map(|r| r.messages.iter().map(|m| m.id + 1))
        .max()
        .unwrap_or(1);
    state.next_id.0.fetch_max(next_after_rooms, Relaxed);
    for rec in snapshot.rooms {
        restore_room(&state, rec);
    }
    for rec in messages {
        restore_message(&state, &mut names, &mut read, rec);
    }
    read.sort_by_key(|(_, m): &(String, Arc<Msg>)| m.read_at_ms());
    for (key, msg) in read {
        if let Some(agent) = state.roster.get(&key) {
            let mut st = agent.inner.lock();
            st.read.push_back(msg);
            agent.publish_oldest_read(&st);
        }
    }
    state
}

/// File one snapshot message where `State::send` would have, as read or
/// unread as it was. Read ones are collected into `read`, with their recipient's key, for
/// the caller to queue in read order.
fn restore_message(
    state: &State,
    names: &mut HashMap<String, Arc<str>>,
    read: &mut Vec<(String, Arc<Msg>)>,
    rec: MsgRec,
) {
    let (from_key, to_key) = (rec.from.to_ascii_lowercase(), rec.to.to_ascii_lowercase());
    let Some(recipient) = state.roster.get(&to_key) else {
        return;
    };
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
    if rec.read_ms == 0 {
        state.unread.0.fetch_add(1, Relaxed);
        recipient.inner.lock().unread.push_back(msg);
    } else {
        read.push((to_key, msg));
    }
}

/// Rebuild one room: its members that are still on the roster (each told
/// where to look), its retained messages, and the unread counts those and
/// the saved cursors imply.
fn restore_room(state: &State, rec: RoomRec) {
    let name: Arc<str> = rec.name.into();
    let mut st = RoomState {
        invited: rec.invited.into_iter().collect(),
        ..RoomState::default()
    };
    for m in rec.messages {
        let body: Box<str> = m.body.into();
        let body_len = json_len(&body);
        let from: Arc<str> = m.from.into();
        let target = Target {
            to: state.no_agent.clone(),
            to_gen: 0,
            room: Some(name.clone()),
        };
        st.log.push_back(Arc::new(Msg::new(
            m.id,
            from,
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
        return;
    }
    let room = Arc::new(Room {
        name: name.clone(),
        private: rec.private,
        description: rec.description,
        inner: parking_lot::Mutex::new(st),
    });
    for agent in joined {
        agent.inner.lock().rooms.push(room.clone());
    }
    let key = name.to_ascii_lowercase();
    // A duplicate room name in a hand-edited snapshot keeps the first.
    let _ = state.rooms.try_insert(&key, room);
}
