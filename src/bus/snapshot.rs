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
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use super::log::{json_len, Msg};
use super::state::{convo_key, Agent, Hold, State};

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
    let mut messages: Vec<MsgRec> = state
        .convos
        .values()
        .flat_map(|convo| convo.live())
        .map(|m| MsgRec {
            id: m.id,
            from: m.from.to_string(),
            to: m.to.to_string(),
            body: m.body.to_string(),
            created_ms: m.created_ms,
            read_ms: m.read_at_ms().unwrap_or(0),
        })
        .collect();
    messages.sort_unstable_by_key(|m| m.id);
    Snapshot {
        version: VERSION,
        next_id: state.next_id,
        messages_sent: state.messages_sent,
        agents: state
            .agents_sorted()
            .map(|a| AgentRec {
                name: a.name.to_string(),
                provider: a.provider.clone(),
                description: a.description.clone(),
                registered_ms: a.registered_ms,
                last_seen_ms: a.last_seen_ms,
            })
            .collect(),
        holds: state
            .holds
            .iter()
            .map(|(key, h)| HoldRec {
                key: key.clone(),
                until_ms: h.until_ms,
                registered_ms: h.registered_ms,
            })
            .collect(),
        messages,
    }
}

/// A [`State`] holding what `snapshot` described. Mail addressed to an agent
/// that is not on the roster is dropped: it would have been discarded with
/// the agent.
fn rebuild(snapshot: Snapshot) -> State {
    let mut state = State::new();
    state.messages_sent = snapshot.messages_sent;
    for rec in snapshot.agents {
        let key = rec.name.to_ascii_lowercase();
        let name: Arc<str> = rec.name.into();
        let mut agent = Agent::new(name, rec.provider, rec.description, rec.registered_ms, 0);
        agent.last_seen_ms = rec.last_seen_ms;
        state.agents.insert(key, agent);
    }
    for rec in snapshot.holds {
        let hold = Hold {
            until_ms: rec.until_ms,
            registered_ms: rec.registered_ms,
        };
        state.holds.insert(rec.key, hold);
    }
    let mut messages = snapshot.messages;
    messages.sort_unstable_by_key(|m| m.id);
    state.next_id = snapshot
        .next_id
        .max(messages.last().map_or(1, |m| m.id + 1));
    let mut names: HashMap<String, Arc<str>> = HashMap::new();
    for rec in messages {
        restore_message(&mut state, &mut names, rec);
    }
    let mut read: Vec<_> = state.read_q.drain(..).collect();
    read.sort_by_key(|m| m.read_at_ms());
    state.read_q = read.into();
    state
}

/// File one snapshot message where `State::append` would have, as read or
/// unread as it was.
fn restore_message(state: &mut State, names: &mut HashMap<String, Arc<str>>, rec: MsgRec) {
    let (from_key, to_key) = (rec.from.to_ascii_lowercase(), rec.to.to_ascii_lowercase());
    if !state.agents.contains_key(&to_key) {
        return;
    }
    let mut spelled = |key: &str, name: String| {
        let roster = state.agents.get(key).map(|a| a.name.clone());
        roster.unwrap_or_else(|| names.entry(key.to_string()).or_insert(name.into()).clone())
    };
    let (from, to) = (spelled(&from_key, rec.from), spelled(&to_key, rec.to));
    let body: Box<str> = rec.body.into();
    let body_len = json_len(&body);
    let msg = Arc::new(Msg::new(rec.id, from, to, body, body_len, rec.created_ms));
    msg.read_ms
        .store(rec.read_ms, std::sync::atomic::Ordering::Relaxed);
    state
        .convos
        .entry(convo_key(&from_key, &to_key))
        .or_default()
        .push(msg.clone());
    state.total += 1;
    if rec.read_ms == 0 {
        state.unread += 1;
        if let Some(agent) = state.agents.get_mut(&to_key) {
            agent.unread.push_back(msg);
        }
    } else {
        state.read_q.push_back(msg);
    }
}
