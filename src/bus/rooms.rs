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

use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::ops::Bound;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Arc;

use parking_lot::Mutex;

use super::log::{Msg, Target};
use super::park::Signal;
use super::state::{page_from_newest, Agent, Key, State};
use super::{
    MailErr, RoomEntry, RoomInfo, MAX_BATCH, MAX_ROOMS, MAX_ROOMS_PER_AGENT, MAX_ROOM_MEMBERS,
};
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
#[derive(Debug)]
pub(super) struct RoomState {
    /// The retained messages, sorted by id.
    pub log: VecDeque<Arc<Msg>>,
    /// Members by folded name.
    pub members: HashMap<String, Member>,
    /// Folded names invited to a private room and not yet joined.
    pub invited: HashSet<String>,
    /// Id of the oldest retained message ([`NO_MESSAGE`] when empty),
    /// published so the byte budget can find the room whose oldest message is
    /// oldest without locking every room. Shared with [`Room::oldest`].
    pub oldest: Arc<AtomicU64>,
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
    /// [`RoomState::oldest`], readable without the room's lock.
    pub oldest: Arc<AtomicU64>,
    pub inner: Mutex<RoomState>,
}

/// [`RoomState::oldest`] of a room with no messages.
pub(super) const NO_MESSAGE: u64 = u64::MAX;

impl Default for RoomState {
    fn default() -> Self {
        Self {
            log: VecDeque::new(),
            members: HashMap::new(),
            invited: HashSet::new(),
            oldest: Arc::new(AtomicU64::new(NO_MESSAGE)),
            retired: false,
        }
    }
}

impl RoomState {
    /// Publish the id of the oldest retained message.
    pub(super) fn publish_oldest(&self) {
        let id = self.log.front().map_or(NO_MESSAGE, |m| m.id);
        self.oldest.store(id, Relaxed);
    }

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

    /// Push the oldest message out, telling every member that had not taken
    /// it that it missed one. Returns the bytes freed; `None` when the log is
    /// empty.
    fn evict_oldest(&mut self) -> Option<usize> {
        let oldest = self.log.pop_front()?;
        for member in self.members.values_mut() {
            if oldest.id > member.cursor && !oldest.from.eq_ignore_ascii_case(&member.name) {
                member.unread -= 1;
                member.skipped += 1;
            }
        }
        self.publish_oldest();
        Some(oldest.cost())
    }

    /// Append `msg` (its id exceeds every id in the log; the caller has made
    /// room), and count it unread for every member but its sender, waking a
    /// pull parked on each.
    fn append(&mut self, msg: Arc<Msg>) {
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
        self.publish_oldest();
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

/// The error for `agent` using a room it is not in. A private room answers
/// `room_not_found` unless the agent was invited, so asking about a name
/// cannot tell a private room from none; `create_room`'s `room_exists` can
/// still reveal that a name is taken.
fn not_in_room(room: &Room, st: &RoomState, agent: &Agent, asked: &RoomName) -> MailErr {
    let invited = st.invited.contains(Key::fold(&agent.name).as_str());
    if st.retired || (room.private && !invited) {
        MailErr::RoomNotFound {
            room: asked.to_string(),
        }
    } else {
        MailErr::NotAMember {
            agent: agent.name.to_string(),
            room: room.name.to_string(),
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
    /// What `me` has waiting in this room (up to `limit` messages); `None` when it is not a member.
    pub(super) fn waiting(&self, me: &Agent, limit: usize) -> Option<Waiting> {
        let st = self.inner.lock();
        let key = Key::fold(&me.name);
        let member = st.members.get(key.as_str())?;
        let start = st.log.partition_point(|m| m.id <= member.cursor);
        let messages = st
            .log
            .range(start..)
            .filter(|m| !m.from.eq_ignore_ascii_case(&me.name))
            .take(limit)
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

impl Room {
    /// Take back what a stream had leased from this room and not yet
    /// acknowledged: `ids` ascending. The member's cursor goes back before
    /// the first of them and they count as unread again, except those pushed
    /// out of the room meanwhile, which are reported as skipped instead.
    pub(super) fn rewind(&self, me: &Agent, ids: &[u64]) {
        let mut st = self.inner.lock();
        let front = st.log.front().map_or(u64::MAX, |m| m.id);
        let key = Key::fold(&me.name);
        let Some(member) = st.members.get_mut(key.as_str()) else {
            return;
        };
        let Some(&first) = ids.first() else { return };
        let retained = ids.iter().filter(|&&id| id >= front).count();
        member.cursor = member.cursor.min(first.saturating_sub(1));
        member.unread += retained;
        member.skipped += (ids.len() - retained) as u64;
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
        if agent.inner.lock().rooms.len() >= MAX_ROOMS_PER_AGENT {
            return Err(MailErr::TooManyRooms {
                agent: agent.name.to_string(),
                rooms: MAX_ROOMS_PER_AGENT as i64,
            });
        }
        let state = RoomState::default();
        let made = Arc::new(Room {
            name: room.as_str().into(),
            private,
            description,
            oldest: state.oldest.clone(),
            inner: Mutex::new(state),
        });
        {
            let mut st = made.inner.lock();
            let member = st.member_for(agent);
            st.members
                .insert(Key::fold(&agent.name).as_str().to_string(), member);
        }
        let key = room.folded();
        loop {
            match self.rooms.try_insert(&key, made.clone()) {
                Ok(()) => break,
                Err(_) => match self.rooms.get(&key) {
                    // A room deleted a moment ago that has not left the map
                    // yet is not a name in use.
                    Some(old) if old.inner.lock().retired => {
                        self.rooms.remove_if(&key, |r| Arc::ptr_eq(r, &old));
                    }
                    _ => {
                        return Err(MailErr::RoomExists {
                            room: room.to_string(),
                        })
                    }
                },
            }
        }
        if !private {
            self.public_names.write().insert(key);
        }
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
                if st.members.len() >= MAX_ROOM_MEMBERS {
                    return Err(MailErr::RoomFull {
                        room: found.name.to_string(),
                        members: st.members.len() as i64,
                    });
                }
                if agent.inner.lock().rooms.len() >= MAX_ROOMS_PER_AGENT {
                    return Err(MailErr::TooManyRooms {
                        agent: agent.name.to_string(),
                        rooms: MAX_ROOMS_PER_AGENT as i64,
                    });
                }
                if found.private && !st.invited.remove(key.as_str()) {
                    // A private room does not admit it exists to an agent
                    // that was not invited.
                    return Err(MailErr::RoomNotFound {
                        room: room.to_string(),
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
            return Err(not_in_room(&found, &st, agent, room));
        }
        let invitee_key = Key::fold(&invitee.name);
        if !st.members.contains_key(invitee_key.as_str())
            && !st.invited.contains(invitee_key.as_str())
        {
            // Invitations count toward the room's size, so a room cannot be
            // made to remember an unbounded list of names.
            if st.members.len() + st.invited.len() >= MAX_ROOM_MEMBERS {
                return Err(MailErr::RoomFull {
                    room: found.name.to_string(),
                    members: st.members.len() as i64,
                });
            }
            st.invited.insert(invitee_key.as_str().to_string());
            let info = st.info(&found);
            drop(st);
            // Tell the invitee, so list_rooms finds the room without
            // searching; the agent's lock is taken after the room's is
            // released. A full list just means the room is not listed: the
            // invitation itself still stands.
            let mut inv = invitee.inner.lock();
            if inv.invited_to.len() < MAX_ROOMS_PER_AGENT {
                inv.invited_to
                    .insert(Key::fold(&found.name).as_str().to_string());
            }
            return Ok(info);
        }
        Ok(st.info(&found))
    }

    /// Take `agent` out of `room`, deleting the room with its last member.
    pub(super) fn leave_room(&self, agent: &Arc<Agent>, room: &RoomName) -> Result<bool, MailErr> {
        let found = self.live_room(room)?;
        let emptied = {
            let mut st = found.inner.lock();
            if st.retired || st.member(agent).is_none() {
                return Err(not_in_room(&found, &st, agent, room));
            }
            st.members.remove(Key::fold(&agent.name).as_str());
            st.members.is_empty()
        };
        agent.inner.lock().rooms.retain(|r| !Arc::ptr_eq(r, &found));
        if emptied {
            self.reap_room(&found);
        }
        Ok(emptied)
    }

    /// Take a departing agent out of every room it is in. `rooms` is the list
    /// the agent held, taken under its lock; none is held here.
    pub(super) fn leave_all_rooms(&self, agent: &Agent, rooms: Vec<Arc<Room>>) {
        let key = Key::fold(&agent.name);
        for room in rooms {
            let emptied = {
                let mut st = room.inner.lock();
                st.members.remove(key.as_str()).is_some() && st.members.is_empty()
            };
            if emptied {
                self.reap_room(&room);
            }
        }
    }

    /// Push out the oldest message of the room whose oldest message is the
    /// oldest anywhere, giving its memory back. Room mail is lossy by design,
    /// so a direct send does this before failing `storage_full` when the
    /// budget is held by rooms. False when no room holds a message.
    pub(super) fn evict_from_rooms(&self) -> bool {
        loop {
            let oldest = self.rooms.min_by_rank(|r| {
                let id = r.oldest.load(Relaxed);
                (id != NO_MESSAGE).then_some(id)
            });
            let Some(room) = oldest else { return false };
            let mut st = room.inner.lock();
            if let Some(freed) = st.evict_oldest() {
                self.release_bytes(freed);
                return true;
            }
        }
    }

    /// Forget invitations to agents that have left the roster. The sweep calls
    /// this, so churn of invited names cannot grow a room without bound.
    pub(super) fn prune_invites(&self) {
        for (_, room) in self.rooms.entries() {
            let invited: Vec<String> = room.inner.lock().invited.iter().cloned().collect();
            if invited.is_empty() {
                continue;
            }
            // The roster is read with no room lock held (it comes first in the
            // lock order).
            let gone: Vec<String> = invited
                .into_iter()
                .filter(|name| self.roster.get(name).is_none())
                .collect();
            if !gone.is_empty() {
                let mut st = room.inner.lock();
                for name in &gone {
                    st.invited.remove(name);
                }
            }
        }
    }

    /// Delete `room` if it has no members: retire it, give its bytes back and
    /// take it out of the maps. Looks at this room only, so deleting rooms
    /// costs the same however many exist.
    fn reap_room(&self, room: &Arc<Room>) {
        {
            let mut st = room.inner.lock();
            if !st.members.is_empty() || st.retired {
                return;
            }
            st.retired = true;
            self.release_bytes(st.log.iter().map(|m| m.cost()).sum());
        }
        let key = Key::fold(&room.name);
        self.rooms.remove_if(key.as_str(), |r| Arc::ptr_eq(r, room));
        self.public_names.write().remove(key.as_str());
    }

    /// Note on the agent that it is in `room`, so a pull knows where to look.
    /// An agent that left the roster meanwhile is taken out again.
    fn remember_room(&self, agent: &Arc<Agent>, room: &Arc<Room>) {
        let gone = {
            let mut st = agent.inner.lock();
            if st.removed {
                true
            } else {
                st.invited_to.remove(Key::fold(&room.name).as_str());
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

    /// Make room in `st`, the locked log of one room, for a message of `len`
    /// bytes: first the room's own capacity, then the byte budget, each by
    /// pushing out this room's oldest message, and reserve the bytes. Fails
    /// `storage_full` when the budget is held elsewhere or the room has
    /// nothing left to push out.
    fn make_room(&self, st: &mut RoomState, len: usize) -> Result<(), MailErr> {
        while st.log.len() >= self.room_capacity {
            let freed = st.evict_oldest().unwrap_or(0);
            self.release_bytes(freed);
        }
        let mut checked = false;
        while !self.try_reserve_bytes(len) {
            // Pushing out this whole room cannot help when the rest of
            // the budget is held elsewhere: fail before evicting anything.
            if !checked {
                checked = true;
                let own: usize = st.log.iter().map(|m| m.cost()).sum();
                let others = self.bytes.0.load(Relaxed).saturating_sub(own);
                if others.saturating_add(len) > self.byte_budget {
                    return Err(self.storage_full());
                }
            }
            match st.evict_oldest() {
                Some(n) => self.release_bytes(n),
                None => return Err(self.storage_full()),
            }
        }
        Ok(())
    }

    fn storage_full(&self) -> MailErr {
        MailErr::StorageFull {
            pending: self.unread.0.load(Relaxed) as i64,
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
                return Err(not_in_room(&found, &st, sender, room));
            }
            self.make_room(&mut st, body.len() + super::MESSAGE_OVERHEAD_BYTES)?;
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
            st.append(msg.clone());
            msg
        };
        self.note_sent(sender, now_ms);
        Ok(msg)
    }

    /// The rooms `agent` can see: every public room, and the private ones it
    /// is in or was invited to.
    pub(super) fn list_rooms(
        &self,
        agent: &Agent,
        after: Option<&str>,
        limit: usize,
    ) -> (Vec<RoomEntry>, bool) {
        let key = Key::fold(&agent.name);
        let own = own_room_keys(agent);
        let mut entries = Vec::new();
        let mut cursor: Option<String> = after.map(str::to_string);
        // Walk the public index and the agent's own rooms together, in name
        // order, so a page costs O(page + the agent's rooms), not O(rooms).
        loop {
            let chunk = self.name_chunk(&own, cursor.as_deref(), limit + 1);
            let Some(last) = chunk.last().cloned() else {
                return (entries, false);
            };
            let exhausted = chunk.len() <= limit;
            for folded in chunk {
                let Some(room) = self.rooms.get(&folded) else {
                    continue;
                };
                let Some(entry) = visible_entry(&room, key.as_str()) else {
                    continue;
                };
                if entries.len() == limit {
                    return (entries, true);
                }
                entries.push(entry);
            }
            if exhausted {
                return (entries, false);
            }
            cursor = Some(last);
        }
    }

    /// The next `n` folded room names after `cursor`, in name order, from the
    /// public index merged with `own` (the agent's private rooms).
    fn name_chunk(&self, own: &BTreeSet<String>, cursor: Option<&str>, n: usize) -> Vec<String> {
        let bound = cursor.map_or(Bound::Unbounded, Bound::Excluded);
        let mut merged: BTreeSet<String> = self
            .public_names
            .read()
            .range::<str, _>((bound, Bound::Unbounded))
            .take(n)
            .cloned()
            .collect();
        merged.extend(
            own.range::<str, _>((bound, Bound::Unbounded))
                .take(n)
                .cloned(),
        );
        merged.into_iter().take(n).collect()
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
            return Err(not_in_room(&found, &st, agent, room));
        }
        let before = before.map(|b| u64::try_from(b).unwrap_or(0));
        let end = before.map_or(st.log.len(), |b| st.log.partition_point(|m| m.id < b));
        let rows: Vec<Arc<Msg>> = st
            .log
            .range(..end)
            .rev()
            .take(MAX_BATCH as usize + 1)
            .cloned()
            .collect();
        drop(st);
        Ok(page_from_newest(rows))
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

/// The folded names of the rooms `agent` can see besides the public ones:
/// those it is in and those it was invited to, both short lists it keeps.
fn own_room_keys(agent: &Agent) -> BTreeSet<String> {
    let st = agent.inner.lock();
    st.rooms
        .iter()
        .map(|r| Key::fold(&r.name).as_str().to_string())
        .chain(st.invited_to.iter().cloned())
        .collect()
}

/// `room` as the agent folded to `key` sees it; `None` when the room is gone
/// or private and the agent is neither in it nor invited.
fn visible_entry(room: &Room, key: &str) -> Option<RoomEntry> {
    let st = room.inner.lock();
    let member = st.members.get(key);
    let hidden = room.private && member.is_none() && !st.invited.contains(key);
    if st.retired || hidden {
        return None;
    }
    Some(RoomEntry {
        room: room.name.to_string(),
        description: room.description.clone(),
        private: room.private,
        members: st.members.len(),
        joined: member.is_some(),
        unread: member.map_or(0, |m| m.unread),
    })
}
