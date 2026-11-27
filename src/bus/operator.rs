//! The operator's view of the whole mailbox, for the web dashboard: every
//! agent with its key fingerprint, and every room (private ones included)
//! with its members, roles, invitations and bans.
//!
//! Read-only: building it never delivers anything, moves a cursor or touches
//! `last_seen`. It holds no message text.

use serde::Serialize;

use super::signed::{Ban, Role};
use super::state::Key;
use super::{roster_row, Bus, MailErr, Stats, MAX_OVERVIEW_AGENTS};

/// One agent as the operator sees it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OperatorAgent {
    /// The name as registered.
    pub name: String,
    /// The harness or app, if it said.
    pub provider: Option<String>,
    /// What it said it is, if anything.
    pub description: Option<String>,
    /// Whole seconds since it was last seen.
    pub seen_secs_ago: i64,
    /// Unread direct messages addressed to it.
    pub queued: i64,
    /// Its bound key's fingerprint; `None` for a keyless name.
    pub fingerprint: Option<String>,
    /// How many rooms it is in.
    pub rooms: usize,
}

/// One member of a room as the operator sees it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OperatorMember {
    /// The member's name.
    pub agent: String,
    /// Admin or participant; `None` in a room from before rooms were signed.
    pub role: Option<Role>,
    /// The fingerprint of the key pinned for it in the room.
    pub fingerprint: Option<String>,
    /// Room messages it has not taken yet.
    pub unread: usize,
}

/// One room as the operator sees it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OperatorRoom {
    /// The room's name.
    pub room: String,
    /// What it is for, if its creator said.
    pub description: Option<String>,
    /// Listed for everyone.
    pub public: bool,
    /// Its operations and messages are signed.
    pub signed: bool,
    /// It carries MLS Commits: its bodies are end-to-end encrypted.
    pub encrypted: bool,
    /// Members sorted by name.
    pub members: Vec<OperatorMember>,
    /// Names invited and not yet joined, sorted.
    pub invited: Vec<String>,
    /// Bans, sorted by name.
    pub banned: Vec<Ban>,
    /// Messages the room still holds.
    pub messages: usize,
}

/// Everything the dashboard's overview shows, in one read.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OperatorView {
    /// The first [`MAX_OVERVIEW_AGENTS`] agents, sorted by name.
    pub agents: Vec<OperatorAgent>,
    /// Agents on the roster in all.
    pub agents_total: usize,
    /// Every room, sorted by name.
    pub rooms: Vec<OperatorRoom>,
    /// Messages sent since the daemon started.
    pub messages_sent: u64,
    /// Unread messages held.
    pub unread: i64,
    /// Read messages kept for `history`.
    pub read: i64,
    /// Whether every write must be signed.
    pub signatures_required: bool,
}

impl Bus {
    /// The operator's view of every agent and room. Read-only.
    ///
    /// # Errors
    ///
    /// `shutting_down` once the bus is closed.
    pub async fn operator_view(&self) -> Result<OperatorView, MailErr> {
        let now = self.now_ms();
        let Stats { unread, read, .. } = self.stats().await?;
        let state = self.open()?;
        let agents = state
            .agents_page(None, MAX_OVERVIEW_AGENTS)
            .0
            .iter()
            .map(|a| {
                let row = roster_row(state, a);
                let st = a.inner.lock();
                OperatorAgent {
                    name: row.name,
                    provider: row.provider,
                    description: row.description,
                    seen_secs_ago: (now / 1000).saturating_sub(row.last_seen_ms / 1000) as i64,
                    queued: row.queued,
                    fingerprint: st.key.map(|k| k.fingerprint()),
                    rooms: st.rooms.len(),
                }
            })
            .collect();
        let mut rooms: Vec<OperatorRoom> = state
            .rooms
            .entries()
            .into_iter()
            .filter_map(|(_, room)| {
                let st = room.inner.lock();
                if st.retired {
                    return None;
                }
                let signed = st.signed.as_ref();
                let mut members: Vec<OperatorMember> = st
                    .members
                    .iter()
                    .map(|(folded, m)| OperatorMember {
                        agent: m.name.to_string(),
                        role: signed.map(|s| {
                            if s.is_admin(folded) {
                                Role::Admin
                            } else {
                                Role::Participant
                            }
                        }),
                        fingerprint: signed
                            .and_then(|s| s.pins.get(folded))
                            .map(|k| k.fingerprint()),
                        unread: m.unread,
                    })
                    .collect();
                members.sort_by_key(|m| Key::fold(&m.agent).as_str().to_string());
                let mut invited: Vec<String> = st.invited.iter().cloned().collect();
                invited.sort();
                Some(OperatorRoom {
                    room: room.name.to_string(),
                    description: room.description.clone(),
                    public: !room.private,
                    signed: signed.is_some(),
                    encrypted: signed.is_some_and(|s| s.epoch > 0),
                    members,
                    invited,
                    banned: signed
                        .map(|s| {
                            s.banned
                                .iter()
                                .map(|(name, key)| Ban {
                                    agent: name.clone(),
                                    public_key: key.map(|k| k.to_hex()),
                                })
                                .collect()
                        })
                        .unwrap_or_default(),
                    messages: st.log.len(),
                })
            })
            .collect();
        rooms.sort_by_key(|r| r.room.to_ascii_lowercase());
        Ok(OperatorView {
            agents,
            agents_total: state.roster.len(),
            rooms,
            messages_sent: state.messages_sent(),
            unread,
            read,
            signatures_required: state.require_signatures,
        })
    }
}
