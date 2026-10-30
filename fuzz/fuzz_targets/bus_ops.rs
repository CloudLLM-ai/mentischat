//! Model-based fuzzing of the whole bus: a bounded sequence of operations
//! (register, deregister, send, rooms, pull, history, sweep, snapshot and
//! reopen) is run against the real `Bus` and against a naive reference model
//! of the documented semantics. Every result must agree: the success or the
//! error code, the messages a pull hands out and their order, `remaining`,
//! history pages and their `next` cursors, and `stats` after every step.
//! That covers at-most-once delivery (a read message never returns), no loss
//! (everything unread comes out of a drain), ids ascending within a page, and
//! history pages that concatenate to the whole log.
//!
//! Determinism: the bus runs on an injected clock that the harness advances
//! one millisecond per operation, on a paused current-thread runtime, and
//! only pulls with `wait_ms` 0 are issued.
//!
//! Not reached in 128 operations, so not modelled: the numeric caps (a
//! thousand unread, a thousand room members), room eviction (`skipped`), the
//! byte budget, parked pulls, streams, and expiry by a sweep that spares some
//! agents (`SweepFar` drops everyone, `SweepNow` no one).
#![no_main]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, UNIX_EPOCH};

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use mentischat::bus::{Bus, Registration};
use mentischat::errors::MailboxError;
use mentischat::names::{AgentName, RoomName};

const AGENTS: usize = 4;
const ROOMS: usize = 3;
const MAX_OPS: usize = 128;
const BASE_MS: u64 = 1_700_000_000_000;
const DAY_MS: u64 = 86_400_000;

#[derive(Arbitrary, Debug, Clone, Copy)]
enum Before {
    None,
    /// The id of the n-th message ever sent (modulo how many there are).
    Seen(u8),
    Zero,
    Negative,
}

#[derive(Arbitrary, Debug)]
enum Op {
    Register {
        a: u8,
        up: bool,
        reclaim: bool,
    },
    Deregister {
        a: u8,
        up: bool,
    },
    Send {
        from: u8,
        to: u8,
        up: bool,
        blank: bool,
    },
    SendRoom {
        from: u8,
        room: u8,
        up: bool,
        blank: bool,
    },
    CreateRoom {
        a: u8,
        room: u8,
        up: bool,
        private: bool,
    },
    JoinRoom {
        a: u8,
        room: u8,
        up: bool,
    },
    Invite {
        a: u8,
        room: u8,
        who: u8,
    },
    LeaveRoom {
        a: u8,
        room: u8,
    },
    Pull {
        a: u8,
        limit: Option<u8>,
    },
    History {
        a: u8,
        with: Option<u8>,
        before: Before,
    },
    RoomHistory {
        a: u8,
        room: u8,
        before: Before,
    },
    SweepNow,
    SweepFar,
    Reopen,
}

#[derive(Arbitrary, Debug)]
struct Ops(Vec<Op>);

/// A name spelled in lower case, or in upper case when `up`: the roster must
/// treat both as one agent.
fn spell(base: &str, up: bool) -> String {
    if up {
        base.to_ascii_uppercase()
    } else {
        base.to_ascii_lowercase()
    }
}

fn agent(i: u8, up: bool) -> AgentName {
    AgentName::parse(&spell(&format!("ag{}", usize::from(i) % AGENTS), up)).unwrap()
}

fn room(i: u8, up: bool) -> RoomName {
    RoomName::parse(&spell(&format!("ro{}", usize::from(i) % ROOMS), up)).unwrap()
}

/// One observable outcome, in the shape both sides are reduced to.
#[derive(Debug, PartialEq, Clone)]
enum Out {
    Err(&'static str),
    Registered {
        created: bool,
    },
    Sent(u64),
    Done,
    Left {
        deleted: bool,
    },
    Discarded(i64),
    Page {
        rows: Vec<(u64, String)>,
        remaining_or_next: i64,
    },
    Swept {
        dropped: Vec<String>,
    },
}

#[derive(Clone, Debug)]
struct Msg {
    id: u64,
    from: usize,
    /// Recipient of direct mail; `None` for a room message.
    to: Option<usize>,
    body: String,
    read: bool,
}

#[derive(Default, Debug)]
struct RoomM {
    private: bool,
    /// Member index to cursor (the id of the last message taken).
    members: BTreeMap<usize, u64>,
    invited: BTreeSet<usize>,
    log: Vec<Msg>,
}

/// The reference: a few maps and vectors, no locks, no counters.
#[derive(Default, Debug)]
struct Model {
    roster: BTreeSet<usize>,
    /// Held names; true when a far sweep made the hold, which outlasts the
    /// next far sweep (it runs two days ahead but holds last ten minutes
    /// past the sweep that made them), while a deregister's hold does not.
    holds: BTreeMap<usize, bool>,
    /// Direct messages still alive, in id order.
    direct: Vec<Msg>,
    rooms: BTreeMap<usize, RoomM>,
    /// Every id ever issued, in order.
    ids: Vec<u64>,
}

fn idx(i: u8, n: usize) -> usize {
    usize::from(i) % n
}

fn code(e: &MailboxError) -> Out {
    Out::Err(e.code())
}

impl Model {
    fn unregistered(&self, a: usize) -> Option<Out> {
        (!self.roster.contains(&a)).then_some(Out::Err("unregistered"))
    }

    /// The error for `a` using room `r` it is not in (or that is not there).
    fn room_gate(&self, a: usize, r: usize) -> Option<Out> {
        let Some(room) = self.rooms.get(&r) else {
            return Some(Out::Err("room_not_found"));
        };
        if room.members.contains_key(&a) {
            return None;
        }
        Some(if room.private && !room.invited.contains(&a) {
            Out::Err("room_not_found")
        } else {
            Out::Err("not_a_member")
        })
    }

    fn unread_direct(&self, a: usize) -> impl Iterator<Item = &Msg> {
        self.direct
            .iter()
            .filter(move |m| m.to == Some(a) && !m.read)
    }

    fn room_unread(&self, a: usize, r: &RoomM) -> usize {
        r.members.get(&a).map_or(0, |cursor| {
            r.log
                .iter()
                .filter(|m| m.id > *cursor && m.from != a)
                .count()
        })
    }

    fn remaining(&self, a: usize) -> usize {
        self.unread_direct(a).count()
            + self
                .rooms
                .values()
                .map(|r| self.room_unread(a, r))
                .sum::<usize>()
    }

    fn pull(&mut self, a: usize, limit: Option<u8>) -> Out {
        let items = limit.map_or(8, |n| usize::from(n).clamp(1, 256));
        let mut cands: Vec<(Msg, Option<usize>)> = self
            .unread_direct(a)
            .take(items)
            .map(|m| (m.clone(), None))
            .collect();
        for (r, room) in &self.rooms {
            let Some(cursor) = room.members.get(&a) else {
                continue;
            };
            cands.extend(
                room.log
                    .iter()
                    .filter(|m| m.id > *cursor && m.from != a)
                    .take(items)
                    .map(|m| (m.clone(), Some(*r))),
            );
        }
        cands.sort_by_key(|(m, _)| m.id);
        cands.truncate(items);
        for (m, r) in &cands {
            match r {
                None => {
                    if let Some(d) = self.direct.iter_mut().find(|d| d.id == m.id) {
                        d.read = true;
                    }
                }
                Some(r) => {
                    let cursor = self.rooms.get_mut(r).unwrap().members.get_mut(&a).unwrap();
                    *cursor = (*cursor).max(m.id);
                }
            }
        }
        Out::Page {
            rows: cands.into_iter().map(|(m, _)| (m.id, m.body)).collect(),
            remaining_or_next: self.remaining(a) as i64,
        }
    }

    /// A page from candidates, newest first: at most 8, and the cursor when
    /// more exist.
    fn page(mut newest_first: Vec<Msg>) -> Out {
        let more = newest_first.len() > 8;
        newest_first.truncate(8);
        let next = if more { newest_first[7].id as i64 } else { 0 };
        newest_first.reverse();
        Out::Page {
            rows: newest_first.into_iter().map(|m| (m.id, m.body)).collect(),
            remaining_or_next: next,
        }
    }

    fn before(&self, b: Before) -> Option<i64> {
        match b {
            Before::None => None,
            Before::Seen(n) if self.ids.is_empty() => Some(i64::from(n)),
            Before::Seen(n) => Some(self.ids[usize::from(n) % self.ids.len()] as i64),
            Before::Zero => Some(0),
            Before::Negative => Some(-5),
        }
    }

    fn history(&self, a: usize, with: Option<usize>, before: Option<i64>) -> Out {
        let limit = before.map_or(u64::MAX, |b| u64::try_from(b).unwrap_or(0));
        let rows: Vec<Msg> = self
            .direct
            .iter()
            .rev()
            .filter(|m| m.id < limit)
            .filter(|m| m.from == a || (m.to == Some(a) && m.read))
            .filter(|m| {
                with.is_none_or(|p| {
                    (m.from == a && m.to == Some(p)) || (m.from == p && m.to == Some(a))
                })
            })
            .take(9)
            .cloned()
            .collect();
        Self::page(rows)
    }

    /// What the operation must do on the model, given what the real bus said
    /// about ids it issued. Returns the expected outcome.
    fn step(&mut self, op: &Op, issued: Option<(u64, String)>) -> Out {
        match *op {
            Op::Register { a, reclaim, .. } => {
                let a = idx(a, AGENTS);
                if self.roster.contains(&a) {
                    return if reclaim {
                        Out::Registered { created: false }
                    } else {
                        Out::Err("name_taken")
                    };
                }
                if self.holds.contains_key(&a) && !reclaim {
                    return Out::Err("name_taken");
                }
                self.roster.insert(a);
                self.holds.remove(&a);
                Out::Registered { created: true }
            }
            Op::Deregister { a, .. } => {
                let a = idx(a, AGENTS);
                if let Some(e) = self.unregistered(a) {
                    return e;
                }
                let before = self.direct.len();
                self.direct.retain(|m| m.to != Some(a));
                let discarded = (before - self.direct.len()) as i64;
                self.roster.remove(&a);
                self.holds.insert(a, false);
                for room in self.rooms.values_mut() {
                    room.members.remove(&a);
                }
                self.rooms.retain(|_, r| !r.members.is_empty());
                Out::Discarded(discarded)
            }
            Op::Send {
                from, to, blank, ..
            } => {
                let (from, to) = (idx(from, AGENTS), idx(to, AGENTS));
                if blank {
                    return Out::Err("invalid_body");
                }
                if let Some(e) = self.unregistered(from).or_else(|| self.unregistered(to)) {
                    return e;
                }
                let (id, body) = issued.expect("the bus accepted a send the model accepts");
                self.record(id);
                self.direct.push(Msg {
                    id,
                    from,
                    to: Some(to),
                    body,
                    read: false,
                });
                Out::Sent(id)
            }
            Op::SendRoom {
                from, room, blank, ..
            } => {
                let (from, r) = (idx(from, AGENTS), idx(room, ROOMS));
                if blank {
                    return Out::Err("invalid_body");
                }
                if let Some(e) = self.unregistered(from).or_else(|| self.room_gate(from, r)) {
                    return e;
                }
                let (id, body) = issued.expect("the bus accepted a room send the model accepts");
                self.record(id);
                self.rooms.get_mut(&r).unwrap().log.push(Msg {
                    id,
                    from,
                    to: None,
                    body,
                    read: false,
                });
                Out::Sent(id)
            }
            Op::CreateRoom {
                a, room, private, ..
            } => {
                let (a, r) = (idx(a, AGENTS), idx(room, ROOMS));
                if let Some(e) = self.unregistered(a) {
                    return e;
                }
                if self.rooms.contains_key(&r) {
                    return Out::Err("room_exists");
                }
                let mut state = RoomM {
                    private,
                    ..RoomM::default()
                };
                state.members.insert(a, 0);
                self.rooms.insert(r, state);
                Out::Done
            }
            Op::JoinRoom { a, room, .. } => {
                let (a, r) = (idx(a, AGENTS), idx(room, ROOMS));
                if let Some(e) = self.unregistered(a) {
                    return e;
                }
                let Some(state) = self.rooms.get_mut(&r) else {
                    return Out::Err("room_not_found");
                };
                if state.members.contains_key(&a) {
                    return Out::Done;
                }
                if state.private && !state.invited.remove(&a) {
                    return Out::Err("room_not_found");
                }
                let head = state.log.last().map_or(0, |m| m.id);
                state.members.insert(a, head);
                Out::Done
            }
            Op::Invite { a, room, who } => {
                let (a, r, who) = (idx(a, AGENTS), idx(room, ROOMS), idx(who, AGENTS));
                if let Some(e) = self.unregistered(a).or_else(|| self.unregistered(who)) {
                    return e;
                }
                if let Some(e) = self.room_gate(a, r) {
                    return e;
                }
                let state = self.rooms.get_mut(&r).unwrap();
                if !state.members.contains_key(&who) {
                    state.invited.insert(who);
                }
                Out::Done
            }
            Op::LeaveRoom { a, room } => {
                let (a, r) = (idx(a, AGENTS), idx(room, ROOMS));
                if let Some(e) = self.unregistered(a).or_else(|| self.room_gate(a, r)) {
                    return e;
                }
                let state = self.rooms.get_mut(&r).unwrap();
                state.members.remove(&a);
                let deleted = state.members.is_empty();
                if deleted {
                    self.rooms.remove(&r);
                }
                Out::Left { deleted }
            }
            Op::Pull { a, limit } => {
                let a = idx(a, AGENTS);
                self.unregistered(a).unwrap_or_else(|| self.pull(a, limit))
            }
            Op::History { a, with, before } => {
                let a = idx(a, AGENTS);
                let before = self.before(before);
                self.unregistered(a)
                    .unwrap_or_else(|| self.history(a, with.map(|w| idx(w, AGENTS)), before))
            }
            Op::RoomHistory { a, room, before } => {
                let (a, r) = (idx(a, AGENTS), idx(room, ROOMS));
                let before = self.before(before);
                if let Some(e) = self.unregistered(a).or_else(|| self.room_gate(a, r)) {
                    return e;
                }
                let limit = before.map_or(u64::MAX, |b| u64::try_from(b).unwrap_or(0));
                let rows = self.rooms[&r]
                    .log
                    .iter()
                    .rev()
                    .filter(|m| m.id < limit)
                    .take(9)
                    .cloned()
                    .collect();
                Self::page(rows)
            }
            Op::SweepNow => {
                // A sweep forgets invitations to agents that are gone.
                for room in self.rooms.values_mut() {
                    let roster = &self.roster;
                    room.invited.retain(|w| roster.contains(w));
                }
                Out::Swept {
                    dropped: Vec::new(),
                }
            }
            Op::SweepFar => {
                let dropped = self.roster.iter().map(|a| agent_key(*a)).collect();
                self.holds.retain(|_, far| *far);
                self.holds.extend(
                    std::mem::take(&mut self.roster)
                        .into_iter()
                        .map(|a| (a, true)),
                );
                self.direct.clear();
                self.rooms.clear();
                Out::Swept { dropped }
            }
            Op::Reopen => {
                // A snapshot keeps invitations only for agents not yet in.
                for room in self.rooms.values_mut() {
                    let members = &room.members;
                    room.invited.retain(|w| !members.contains_key(w));
                }
                Out::Done
            }
        }
    }

    fn record(&mut self, id: u64) {
        assert!(
            self.ids.last().is_none_or(|last| id > *last),
            "id {id} does not increase"
        );
        self.ids.push(id);
    }
}

fn agent_key(i: usize) -> String {
    format!("ag{i}")
}

fn runtime() -> &'static tokio::runtime::Runtime {
    static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
            .expect("runtime")
    })
}

fn with_clock(clock: &Arc<AtomicU64>, bus: Bus) -> Bus {
    let clock = clock.clone();
    bus.with_clock(move || clock.load(Relaxed))
}

fn fold(names: Vec<String>) -> Vec<String> {
    names.into_iter().map(|n| n.to_ascii_lowercase()).collect()
}

fn rows(p: &[mentischat::bus::Message]) -> Vec<(u64, String)> {
    p.iter()
        .map(|m| (m.id.parse().unwrap(), m.body.clone()))
        .collect()
}

/// Run `op` on the real bus. Returns what it did and, for a send that
/// succeeded, the id and body it was given, which the model records.
async fn apply(
    bus: &Bus,
    clock: &AtomicU64,
    sends: &mut u64,
    op: &Op,
    model: &Model,
) -> (Out, Option<(u64, String)>) {
    let mut body = |blank: bool| {
        *sends += 1;
        if blank {
            "  ".to_string()
        } else {
            format!("msg{sends}")
        }
    };
    let out = match *op {
        Op::Register { a, up, reclaim } => {
            let opts = Registration {
                reclaim,
                ..Registration::default()
            };
            match bus.register_with(&agent(a, up), opts).await {
                Ok(o) => Out::Registered { created: o.created },
                Err(e) => code(&e),
            }
        }
        Op::Deregister { a, up } => match bus.deregister(&agent(a, up)).await {
            Ok(o) => Out::Discarded(o.discarded),
            Err(e) => code(&e),
        },
        Op::Send {
            from,
            to,
            up,
            blank,
        } => {
            let body = body(blank);
            match bus
                .send(&agent(from, up), &agent(to, !up), body.clone())
                .await
            {
                Ok(r) => {
                    let id: u64 = r.id.parse().expect("numeric id");
                    return (Out::Sent(id), Some((id, body)));
                }
                Err(e) => code(&e),
            }
        }
        Op::SendRoom {
            from,
            room: r,
            up,
            blank,
        } => {
            let body = body(blank);
            match bus
                .send_room(&agent(from, up), &room(r, !up), body.clone())
                .await
            {
                Ok(r) => {
                    let id: u64 = r.id.parse().expect("numeric id");
                    return (Out::Sent(id), Some((id, body)));
                }
                Err(e) => code(&e),
            }
        }
        Op::CreateRoom {
            a,
            room: r,
            up,
            private,
        } => {
            match bus
                .create_room(&agent(a, up), &room(r, !up), private, None)
                .await
            {
                Ok(_) => Out::Done,
                Err(e) => code(&e),
            }
        }
        Op::JoinRoom { a, room: r, up } => {
            match bus.join_room(&agent(a, up), &room(r, !up)).await {
                Ok(_) => Out::Done,
                Err(e) => code(&e),
            }
        }
        Op::Invite { a, room: r, who } => {
            match bus
                .invite(&agent(a, false), &room(r, true), &agent(who, true))
                .await
            {
                Ok(_) => Out::Done,
                Err(e) => code(&e),
            }
        }
        Op::LeaveRoom { a, room: r } => {
            match bus.leave_room(&agent(a, true), &room(r, false)).await {
                Ok(o) => Out::Left { deleted: o.deleted },
                Err(e) => code(&e),
            }
        }
        Op::Pull { a, limit } => match bus
            .check_inbox_limit(&agent(a, false), 0, limit.map(usize::from))
            .await
        {
            Ok(p) => {
                let ids: Vec<u64> = p.messages.iter().map(|m| m.id.parse().unwrap()).collect();
                assert!(ids.windows(2).all(|w| w[0] < w[1]), "ids ascend in a page");
                assert_eq!(p.skipped, 0);
                Out::Page {
                    rows: rows(&p.messages),
                    remaining_or_next: p.remaining,
                }
            }
            Err(e) => code(&e),
        },
        Op::History { a, with, before } => {
            let with = with.map(|w| agent(w, true));
            match bus
                .history(&agent(a, false), with.as_ref(), model.before(before))
                .await
            {
                Ok(p) => Out::Page {
                    rows: rows(&p.messages),
                    remaining_or_next: p.next.unwrap_or(0),
                },
                Err(e) => code(&e),
            }
        }
        Op::RoomHistory { a, room: r, before } => {
            match bus
                .room_history(&agent(a, false), &room(r, false), model.before(before))
                .await
            {
                Ok(p) => Out::Page {
                    rows: rows(&p.messages),
                    remaining_or_next: p.next.unwrap_or(0),
                },
                Err(e) => code(&e),
            }
        }
        Op::SweepNow => match bus.sweep().await {
            Ok(o) => {
                assert!(o.is_empty(), "nothing is old: {o:?}");
                Out::Swept {
                    dropped: Vec::new(),
                }
            }
            Err(e) => code(&e),
        },
        Op::SweepFar => {
            let at = UNIX_EPOCH + Duration::from_millis(clock.load(Relaxed) + 2 * DAY_MS);
            match bus.sweep_at(at).await {
                Ok(o) => Out::Swept {
                    dropped: fold(o.dropped_agents),
                },
                Err(e) => code(&e),
            }
        }
        Op::Reopen => unreachable!("handled by the caller"),
    };
    (out, None)
}

fuzz_target!(|ops: Ops| {
    runtime().block_on(async {
        let clock = Arc::new(AtomicU64::new(BASE_MS));
        let fresh = Bus::new(std::path::Path::new("/nonexistent/mentischat-fuzz")).unwrap();
        let mut bus = with_clock(&clock, fresh);
        let mut model = Model::default();
        let mut sends = 0u64;
        for (step, op) in ops.0.iter().take(MAX_OPS).enumerate() {
            clock.store(BASE_MS + step as u64, Relaxed);
            let got = if matches!(op, Op::Reopen) {
                // Close and open again: the snapshot restores to a bus that
                // behaves as the one it was taken from.
                let again =
                    Bus::from_snapshot_bytes(&bus.snapshot_bytes()).expect("our snapshot restores");
                bus = with_clock(&clock, again);
                (Out::Done, None)
            } else {
                apply(&bus, &clock, &mut sends, op, &model).await
            };
            let want = model.step(op, got.1);
            assert_eq!(got.0, want, "step {step}: {op:?}");
            let stats = bus.stats().await.unwrap();
            assert_eq!(
                stats.agents as usize,
                model.roster.len(),
                "agents after {op:?}"
            );
            let unread = model.direct.iter().filter(|m| !m.read).count();
            assert_eq!(stats.unread as usize, unread, "unread after {op:?}");
            assert_eq!(
                stats.read as usize,
                model.direct.len() - unread,
                "read after {op:?}"
            );
        }
    });
});
