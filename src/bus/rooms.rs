//! Rooms: named conversations with any number of members.
//!
//! A room is an append-only ring of `Arc`'d messages (the last
//! [`State::room_capacity`] of them, oldest pushed out first) and a cursor
//! per member: the id of the last room message that member has taken. What a
//! member has not read is the room's messages past its cursor that others
//! sent, kept as a count so a pull never scans to size its answer. A send is
//! one append and one `notify_one` per other member; a member that fell
//! behind far enough to lose messages is told how many (`skipped`) on its
//! next pull. New members start at the head of the log: they see what comes
//! next, and `history` shows the retained past.
//!
//! Locks: a room has one `Mutex`, taken after an agent's (a pull reads and
//! advances its cursors under its own agent lock) and before the
//! conversation map. No path holds two room locks, and nothing takes an
//! agent's lock while holding a room's. An agent's `rooms` list is a hint:
//! the room's `members` map decides membership, so the two being briefly out
//! of step (between the two halves of a join or leave) is harmless.
//!
//! Identity is the caller-supplied name, like every other call. A private
//! room is invite-only and unlisted for everyone else; any local process can
//! still claim a member's name, so it is etiquette, not a security boundary.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;

use parking_lot::Mutex;

use super::log::{Msg, Target};
use super::park::Signal;
use super::state::{fitting, sorted_by_key, Agent, Key, State};
use super::{MailErr, RoomEntry, RoomInfo, MAX_BATCH, MAX_ROOMS};
use crate::names::RoomName;

/// One agent's place in a room.
#[derive(Debug)]
pub(super) struct Member {
    /// The agent's name as registered.
    pub name: Arc<str>,
    /// What a send to the room pokes.
    pub signal: Arc<Signal>,
    /// Id of the last room message this member has taken, or the log's head
    /// when it joined.
    pub cursor: u64,
    /// Room messages past the cursor that others sent.
    pub unread: usize,
    /// Messages pushed out of the room before this member took them, not yet
    /// reported.
    pub skipped: u64,
}

/// What changes about a room.
#[derive(Debug, Default)]
pub(super) struct RoomState {
    /// The retained messages, sorted by id.
    pub log: VecDeque<Arc<Msg>>,
    /// Members by folded name.
    pub members: HashMap<String, Member>,
    /// Folded names invited to a private room and not yet joined.
    pub invited: HashSet<String>,
    /// Set when the room was removed from the room map; whoever finds it
    /// treats the room as gone.
    pub retired: bool,
}

/// A room.
#[derive(Debug)]
pub(super) struct Room {
    /// The name as the room was created.
    pub name: Arc<str>,
    /// Invite-only and unlisted.
    pub private: bool,
    pub description: Option<String>,
    pub inner: Mutex<RoomState>,
}

impl RoomState {
    /// Id of the newest retained message, 0 when there is none.
    fn head(&self) -> u64 {
        self.log.back().map_or(0, |m| m.id)
    }

    fn member(&self, agent: &Agent) -> Option<&Member> {
        self.members.get(Key::fold(&agent.name).as_str())
    }

    /// A new member at the head of the log.
    fn member_for(&self, agent: &Agent) -> Member {
        Member {
            name: agent.name.clone(),
            signal: agent.signal.clone(),
            cursor: self.head(),
            unread: 0,
            skipped: 0,
        }
    }

    /// Append `msg` (its id exceeds every id in the log), pushing the oldest
    /// message out at `capacity`, and count it unread for every member but
    /// its sender, waking a pull parked on each.
    fn append(&mut self, msg: Arc<Msg>, capacity: usize) {
        while self.log.len() >= capacity {
            let Some(oldest) = self.log.pop_front() else {
                break;
            };
            for member in self.members.values_mut() {
                if oldest.id > member.cursor && !oldest.from.eq_ignore_ascii_case(&member.name) {
                    member.unread -= 1;
                    member.skipped += 1;
                }
            }
        }
        for member in self.members.values_mut() {
            if msg.from.eq_ignore_ascii_case(&member.name) {
                continue;
            }
            member.unread += 1;
            if member.signal.parked() > 0 {
                member.signal.notify.notify_one();
            }
        }
        self.log.push_back(msg);
    }

    /// The room as it goes on the wire.
    fn info(&self, room: &Room) -> RoomInfo {
        let mut names: Vec<(&String, &Member)> = self.members.iter().collect();
        names.sort_unstable_by(|a, b| a.0.cmp(b.0));
        RoomInfo {
            room: room.name.to_string(),
            description: room.description.clone(),
            private: room.private,
            members: names.into_iter().map(|(_, m)| m.name.to_string()).collect(),
        }
    }
}

/// Room mail waiting for one agent, as one pull sees it.
pub(super) struct Waiting {
    /// The oldest unread messages, at most a page's worth, oldest first.
    pub messages: Vec<Arc<Msg>>,
    /// All the agent's unread messages in the room.
    pub unread: usize,
    /// Messages it lost to the room filling up.
    pub skipped: u64,
}

impl Room {
    /// What `me` has waiting in this room; `None` when it is not a member.
    pub(super) fn waiting(&self, me: &Agent) -> Option<Waiting> {
        let st = self.inner.lock();
        let key = Key::fold(&me.name);
        let member = st.members.get(key.as_str())?;
        let start = st.log.partition_point(|m| m.id <= member.cursor);
        let messages = st
            .log
            .range(start..)
            .filter(|m| !m.from.eq_ignore_ascii_case(&me.name))
            .take(MAX_BATCH as usize)
            .cloned()
            .collect();
        Some(Waiting {
            messages,
            unread: member.unread,
            skipped: member.skipped,
        })
    }

    /// Record that `me` took the messages with these `ids` (ascending) from
    /// this room, and report its skipped count. Returns what is still unread
    /// and how many messages were skipped.
    ///
    /// A message pushed out between the pull's look and this call was already
    /// counted as skipped, so it is not taken off the unread count again.
    pub(super) fn commit(&self, me: &Agent, ids: &[u64]) -> (usize, u64) {
        let mut st = self.inner.lock();
        let front = st.log.front().map_or(u64::MAX, |m| m.id);
        let key = Key::fold(&me.name);
        let Some(member) = st.members.get_mut(key.as_str()) else {
            return (0, 0);
        };
        let kept = ids.iter().filter(|&&id| id >= front).count();
        member.unread = member.unread.saturating_sub(kept);
        if let Some(&last) = ids.last() {
            member.cursor = member.cursor.max(last);
        }
        (member.unread, std::mem::take(&mut member.skipped))
    }
}

impl State {
    /// A room under `key`, treating a retired one as absent.
    fn live_room(&self, room: &RoomName) -> Result<Arc<Room>, MailErr> {
        self.rooms
            .get(&room.folded())
            .ok_or_else(|| MailErr::RoomNotFound {
                room: room.to_string(),
            })
    }

    /// Create `room` with `agent` as its first member.
    pub(super) fn create_room(
        &self,
        agent: &Arc<Agent>,
        room: &RoomName,
        private: bool,
        description: Option<String>,
    ) -> Result<RoomInfo, MailErr> {
        if self.rooms.len() >= MAX_ROOMS {
            return Err(MailErr::RoomsFull {
                rooms: self.rooms.len() as i64,
            });
        }
        let made = Arc::new(Room {
            name: room.as_str().into(),
            private,
            description,
            inner: Mutex::default(),
        });
        {
            let mut st = made.inner.lock();
            let member = st.member_for(agent);
            st.members
                .insert(Key::fold(&agent.name).as_str().to_string(), member);
        }
        self.rooms
            .try_insert(&room.folded(), made.clone())
            .map_err(|_| MailErr::RoomExists {
                room: room.to_string(),
            })?;
        self.remember_room(agent, &made);
        let info = made.inner.lock().info(&made);
        Ok(info)
    }

    /// Put `agent` in `room`: a public room takes anyone, a private one
    /// takes the invited. Already a member is fine.
    pub(super) fn join_room(
        &self,
        agent: &Arc<Agent>,
        room: &RoomName,
    ) -> Result<RoomInfo, MailErr> {
        let found = self.live_room(room)?;
        let key = Key::fold(&agent.name);
        let info = {
            let mut st = found.inner.lock();
            if st.retired {
                return Err(MailErr::RoomNotFound {
                    room: room.to_string(),
                });
            }
            if !st.members.contains_key(key.as_str()) {
                if found.private && !st.invited.remove(key.as_str()) {
                    return Err(MailErr::PrivateRoom {
                        room: found.name.to_string(),
                    });
                }
                let member = st.member_for(agent);
                st.members.insert(key.as_str().to_string(), member);
            }
            st.info(&found)
        };
        self.remember_room(agent, &found);
        Ok(info)
    }

    /// Let `invitee` (a roster agent) join the private room `room`; only a
    /// member may.
    pub(super) fn invite(
        &self,
        agent: &Agent,
        room: &RoomName,
        invitee: &Agent,
    ) -> Result<RoomInfo, MailErr> {
        let found = self.live_room(room)?;
        let mut st = found.inner.lock();
        if st.retired || st.member(agent).is_none() {
            return Err(MailErr::NotAMember {
                agent: agent.name.to_string(),
                room: found.name.to_string(),
            });
        }
        let invitee_key = Key::fold(&invitee.name);
        if !st.members.contains_key(invitee_key.as_str()) {
            st.invited.insert(invitee_key.as_str().to_string());
        }
        Ok(st.info(&found))
    }

    /// Take `agent` out of `room`, deleting the room with its last member.
    pub(super) fn leave_room(&self, agent: &Arc<Agent>, room: &RoomName) -> Result<bool, MailErr> {
        let found = self.live_room(room)?;
        let emptied = {
            let mut st = found.inner.lock();
            if st.retired || st.members.remove(Key::fold(&agent.name).as_str()).is_none() {
                return Err(MailErr::NotAMember {
                    agent: agent.name.to_string(),
                    room: found.name.to_string(),
                });
            }
            st.members.is_empty()
        };
        agent.inner.lock().rooms.retain(|r| !Arc::ptr_eq(r, &found));
        if emptied {
            self.reap_empty_rooms();
        }
        Ok(emptied)
    }

    /// Take a departing agent out of every room it is in. `rooms` is the list
    /// the agent held, taken under its lock; none is held here.
    pub(super) fn leave_all_rooms(&self, agent: &Agent, rooms: Vec<Arc<Room>>) {
        let key = Key::fold(&agent.name);
        let mut emptied = false;
        for room in rooms {
            let mut st = room.inner.lock();
            if st.members.remove(key.as_str()).is_some() {
                emptied |= st.members.is_empty();
            }
        }
        if emptied {
            self.reap_empty_rooms();
        }
    }

    /// Remove the rooms that have no members.
    pub(super) fn reap_empty_rooms(&self) {
        self.rooms.retain(|room| {
            let mut st = room.inner.lock();
            st.retired = st.members.is_empty();
            !st.retired
        });
    }

    /// Note on the agent that it is in `room`, so a pull knows where to look.
    /// An agent that left the roster meanwhile is taken out again.
    fn remember_room(&self, agent: &Arc<Agent>, room: &Arc<Room>) {
        let gone = {
            let mut st = agent.inner.lock();
            if st.removed {
                true
            } else {
                if !st.rooms.iter().any(|r| Arc::ptr_eq(r, room)) {
                    st.rooms.push(room.clone());
                }
                false
            }
        };
        if gone {
            self.leave_all_rooms(agent, vec![room.clone()]);
        }
    }

    /// Append `body` from `sender` to `room`. The sender must be a member.
    /// Nothing is counted against the direct-mail caps: a room is bounded by
    /// its own capacity.
    pub(super) fn send_room(
        &self,
        sender: &Arc<Agent>,
        room: &RoomName,
        body: Box<str>,
        body_json_len: usize,
        now_ms: u64,
    ) -> Result<Arc<Msg>, MailErr> {
        let found = self.live_room(room)?;
        let msg = {
            let mut st = found.inner.lock();
            if st.retired || st.member(sender).is_none() {
                return Err(MailErr::NotAMember {
                    agent: sender.name.to_string(),
                    room: found.name.to_string(),
                });
            }
            // Under the room's lock, so the log stays sorted by id.
            let id = self.next_id.0.fetch_add(1, Relaxed);
            let msg = Arc::new(Msg::new(
                id,
                sender.name.clone(),
                Target {
                    to: self.no_agent.clone(),
                    to_gen: 0,
                    room: Some(found.name.clone()),
                },
                body,
                body_json_len,
                now_ms,
            ));
            st.append(msg.clone(), self.room_capacity);
            msg
        };
        let mut from_state = sender.inner.lock();
        if from_state.removed {
            self.sent_by_gone.fetch_add(1, Relaxed);
        } else {
            from_state.last_seen_ms = now_ms;
            from_state.sent += 1;
        }
        Ok(msg)
    }

    /// The rooms `agent` can see: every public room, and the private ones it
    /// is in or was invited to.
    pub(super) fn list_rooms(&self, agent: &Agent) -> Vec<RoomEntry> {
        let key = Key::fold(&agent.name);
        let mut entries = Vec::new();
        for (folded, room) in self.rooms.entries() {
            let st = room.inner.lock();
            let member = st.members.get(key.as_str());
            if st.retired
                || (room.private && member.is_none() && !st.invited.contains(key.as_str()))
            {
                continue;
            }
            entries.push((
                folded,
                RoomEntry {
                    room: room.name.to_string(),
                    description: room.description.clone(),
                    private: room.private,
                    members: st.members.len(),
                    joined: member.is_some(),
                    unread: member.map_or(0, |m| m.unread),
                },
            ));
        }
        sorted_by_key(entries)
    }

    /// One page of the retained messages of `room`, for a member: the
    /// newest with an id below `before`, the prefix that fits the batch caps,
    /// oldest first, and the `next` cursor.
    pub(super) fn room_history(
        &self,
        agent: &Agent,
        room: &RoomName,
        before: Option<i64>,
    ) -> Result<(Vec<Arc<Msg>>, Option<i64>), MailErr> {
        let found = self.live_room(room)?;
        let st = found.inner.lock();
        if st.retired || st.member(agent).is_none() {
            return Err(MailErr::NotAMember {
                agent: agent.name.to_string(),
                room: found.name.to_string(),
            });
        }
        let before = before.map(|b| u64::try_from(b).unwrap_or(0));
        let end = before.map_or(st.log.len(), |b| st.log.partition_point(|m| m.id < b));
        let mut rows: Vec<Arc<Msg>> = st
            .log
            .range(..end)
            .rev()
            .take(MAX_BATCH as usize + 1)
            .cloned()
            .collect();
        drop(st);
        let take = fitting(rows.iter().map(|m| m.page_len()));
        let next = (rows.len() > take).then(|| rows[take - 1].id as i64);
        rows.truncate(take);
        rows.reverse();
        Ok((rows, next))
    }

    /// Unread room messages for `agent`, over every room it is in.
    pub(super) fn room_unread(&self, agent: &Agent, rooms: &[Arc<Room>]) -> usize {
        let key = Key::fold(&agent.name);
        rooms
            .iter()
            .filter_map(|r| r.inner.lock().members.get(key.as_str()).map(|m| m.unread))
            .sum()
    }
}
