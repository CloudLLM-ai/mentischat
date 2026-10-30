---
name: rust-concurrency-patterns
description: >
  Concurrency patterns for the mentischat store: shared mutable state behind
  short sync locks, a documented lock order, no guard across `.await`,
  Arc'd messages, per-reader cursors, and blocking pulls on a per-agent
  tokio Notify without lost wakeups. Adapted from MentisDB
  rust-concurrency-patterns. Load with rust-engineer when touching the bus,
  the store, waiters, retention, or anything shared between tasks.
triggers:
  - concurrent access
  - lock contention
  - lock order
  - deadlock
  - parking_lot
  - RwLock
  - DashMap
  - Notify
  - lost wakeup
  - blocking pull
  - async shared state
  - mutex
  - high concurrency
---

# rust-concurrency-patterns

Load this when you change shared state in mentischat: the store, the bus,
blocking pulls, retention sweeps, or anything a second task can touch at the
same time. Prefer [`rust-engineer.md`](rust-engineer.md) for general style;
measure with [`rust-criterion-benchmarking.md`](rust-criterion-benchmarking.md).

## The core principle

Lock granularity sets the throughput ceiling, and lock *discipline* sets the
correctness floor. Every shared lock is a serialization point; every pair of
locks is a potential deadlock; every guard alive at an `.await` is a stalled
worker or a clippy error.

The store's contract does not depend on how it is locked. Whatever the
engine looks like, these hold:

- `check_inbox` hands each message out **exactly once**: taking it and
  marking it read happen in one atomic step, with no `.await` in between.
- Read mail stays for `history` until the read TTL; unread mail never
  expires on its own.
- A blocking pull cannot miss a send that lands while it is deciding to
  park.
- No lock is held across `.await`, network I/O, or disk I/O.

## Engine shape (in-memory store)

The mailbox is moving from SQLite behind one global `tokio::sync::Mutex` to
an in-memory engine:

| Piece | Shape |
|---|---|
| Conversations | Append-only log per conversation, entries are `Arc<Message>` |
| Readers | One cursor per (reader, conversation); a pull reads from the cursor and advances it |
| Locks | `parking_lot` `Mutex`/`RwLock`, held for short synchronous sections only, acquired in the order the store's module docs list |
| Blocking pulls | One `tokio::sync::Notify` per agent; senders notify after appending |

New code keeps that shape. Do not reintroduce a global lock on the hot path,
and do not add a lock without placing it in the documented order.

## Pattern 1: No guard across `.await`

A `parking_lot` guard is `!Send`, so a future holding one across an `.await`
cannot be spawned on the multi-thread runtime, and clippy's
`await_holding_lock` (an error under `-D warnings`) rejects it, as it does
`std::sync` guards. Do the synchronous work in a block, let the guard drop,
then await. (Sketches below use illustrative names, not the store's API.)

```rust
// Bad: guard alive across the await
let log = conv.log.lock();
let body = serialize(&log[cursor..]).await; // worker stalled, clippy error
// Good: copy out the Arcs, drop the guard, then do slow work
let batch: Vec<Arc<Message>> = {
    let log = conv.log.lock();
    log.since(cursor).cloned().collect() // cheap: Arc clones, not bodies
};
let body = to_json(&batch)?; // no lock held
```

Use `tokio::sync::Mutex` only when a guard genuinely must live across an
`.await`. The store has no such section; if you think you need one, the
design is wrong.

## Pattern 2: One documented lock order

When a section takes more than one lock, it takes them in the order the
store documents, every time, on every path (send, pull, history, retention,
deregister). A `DashMap` shard lock counts as a lock: holding a map `Ref` while
locking an entity inside it, on one path, and the reverse on another, is a
deadlock.

- Take the per-entity `Arc` out of the outer map, drop the map guard, then
  lock the entity, unless the documented order says otherwise.
- Never call code you do not own (callbacks, logging that may block, a
  `Notify` you were not meant to touch) while holding a lock you will not
  name in a comment.
- If a change needs a new order, update the module docs in the same commit.

## Pattern 3: Share messages, never copy bodies

A message is written once and read by its recipient, by `history`, and by
the terminal view. Store it as `Arc<Message>` (bodies as `Arc<str>` or inside
the `Arc`'d struct) so every reader clones a pointer.

- Clone `Arc`s under the lock; build JSON responses after releasing it.
- `Arc<str>` over `Arc<String>` (one indirection, no capacity slack).
- Never `.clone()` a `Message` or its body on a pull path.

## Pattern 4: Cursors make "exactly once" cheap

A pull returns the entries after the reader's cursor and advances it in the
same critical section. That section is the "transaction" in the delivery
invariant.

- No `.await` between reading the entries and advancing the cursor. A
  cancelled future must leave either both done or neither.
- Two concurrent pulls for one agent serialize on that section; the second
  sees the advanced cursor and gets nothing twice.
- If retention compacts a log from the front, cursors must survive it (store
  sequence numbers, not raw `Vec` indexes, or rebase every cursor in the same
  section that compacts).
- Retention prunes in the same lock order as everyone else and does bounded
  work per pass, so a sweep never parks senders for long.

## Pattern 5: Blocking pulls on `Notify` without lost wakeups

The race to close: the inbox is empty, a send lands, the pull parks, and no
one wakes it. Register interest *before* checking:

```rust
loop {
    let notified = agent.notify.notified();
    tokio::pin!(notified);
    notified.as_mut().enable(); // registered from here on
    if let Some(batch) = store.take_unread(&agent)? {
        return Ok(batch);
    }
    if tokio::time::timeout_at(deadline, notified).await.is_err() {
        return Ok(Vec::new()); // timed out empty
    }
    // woken: loop and re-check; a wakeup is a hint, not a delivery
}
```

Sender side: append under the lock, release it, then `notify_one()` (one
puller per wake, and it stores a permit if nobody is parked) or
`notify_waiters()` (wakes every enabled `Notified`, stores nothing). Pick one
per design and say why in a comment.

- Always loop and re-check after a wake: permits and spurious wakeups mean
  "woken" is not "there is mail for you".
- Bound parked pulls per agent and per process; a slow client must not pin
  unbounded futures.
- A pull dropped mid-wait (client hung up) must leave no state behind. With
  `Notify` there is nothing to settle; if you add bookkeeping, settle it in
  `Drop`.

## Pattern 6: Maps of entities

`Arc<RwLock<HashMap<K, V>>>` held while working on a value serializes every
caller. Either:

- `DashMap<K, Arc<Entity>>` (sharded), cloning the `Arc` out of the `Ref`
  before doing anything else, or
- an outer `parking_lot::RwLock<HashMap<K, Arc<Entity>>>` held only long
  enough to look up or insert the `Arc`.

Measure before switching between them (`rust-criterion-benchmarking`); at
mentischat's agent counts the outer map is rarely the bottleneck, the
per-conversation section is.

## Pattern 7: Choosing a lock

| Need | Use |
|---|---|
| Short sync section, no `.await` | `parking_lot::Mutex` / `RwLock` |
| Many readers, rare writers, short sections | `parking_lot::RwLock` |
| Guard must live across `.await` (avoid) | `tokio::sync::Mutex` |
| Counter or flag | `AtomicU64` / `AtomicBool`, `Ordering::Relaxed` unless it publishes data |
| Read-mostly snapshot swapped whole | `arc_swap::ArcSwap` |
| Wake async waiters | `tokio::sync::Notify` |

`parking_lot` locks do not poison. A panic inside a section leaves the state
however far the section got, so keep sections panic-free: no `unwrap()`, no
indexing you have not proven in range, mutate last.

## Durability patterns (only if a persistent path is added)

The in-memory store trades durability for speed; the mailbox is wiped at
startup unless the operator asks to persist. If a durable path comes back,
it stays off the pull hot path:

- Append-only log file with a `BufWriter`, flushed every N entries or on a
  timer, and **always on `Drop`**; make the batch size an operator knob.
- Write-ahead log + background snapshot task when per-write fsync caps
  throughput; replay on open.
- Never fsync or write files while holding a store lock; hand the record to
  a writer task over a bounded channel.

| Pattern | Writes/s per log (order of magnitude) | At risk on crash |
|---|---|---|
| fsync per write | hundreds | nothing |
| Batch flush every N | low thousands | last N-1 |
| WAL + background flush | tens of thousands | last flush window |
| In memory only | millions | everything |

## Testing concurrency

- Run races on a multi-thread runtime
  (`#[tokio::test(flavor = "multi_thread", worker_threads = 4)]`).
- Exactly-once: N senders, M concurrent pullers for one agent; assert the
  union of what the pullers got equals what was sent, with no duplicates.
- Lost wakeup: park a pull, send, assert it returns the message well before
  its timeout. Repeat in a loop to shake out the interleaving.
- Cancellation: drop a pull mid-wait, then send; the next pull gets the mail.
- Assert with deterministic capture (channels, recorded order), never with
  sleeps or wall-clock timing.

## Common mistakes

- A `std::sync` or `parking_lot` guard alive at an `.await`.
- Two locks taken in different orders on two paths (including a `DashMap`
  `Ref` held while locking its value).
- Checking the inbox, *then* creating the `Notified`: the send between the
  two is lost until the timeout.
- Treating a wake as a delivery instead of looping to re-check.
- Advancing a cursor in one section and marking read in another.
- Building JSON, logging bodies, or doing I/O inside a store section.
- Cloning message bodies instead of `Arc`s on pull or history paths.
- Buffered writers without a `Drop` flush.

## Version history

- **2026-10-30** — Initial mentischat version, adapted from MentisDB
  `rust-concurrency-patterns` (2026-07-09) for the in-memory store:
  lock order, parking_lot sections, Arc'd messages, cursors, and
  `Notify`-based blocking pulls.
