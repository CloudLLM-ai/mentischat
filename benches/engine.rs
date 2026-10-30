//! Hot paths of the engine other than send and pull-one-message, all through
//! the public [`Bus`] API and name types (no socket).
//!
//! Run: `cargo bench --bench engine` (one group: `-- history`)
//!
//! * `history`: one page of history, with and without `with`, as the number
//!   of conversations grows. `mine/{n}` is `n` conversations of the reader;
//!   `others/{n}` is one conversation of the reader among `n` held by other
//!   agents, which shows whether a page costs time in conversations that are
//!   not its own.
//! * `sweep`: `read_ttl` expiry over M read messages (`expire/{m}`, fresh
//!   mailbox per iteration) and a sweep with nothing to expire (`idle/{agents}`).
//! * `stream`: a send, `stream_push` and `stream_ack` of k messages.
//! * `pull`: one `check_inbox` with `p` messages pending, by an agent in `r`
//!   rooms; the pending count is topped back up outside the timed region.
//! * `snapshot`: `close` (capture and save) and `Bus::new` (restore) of a
//!   mailbox at the row cap and at the byte budget, i.e. shutdown and startup
//!   latency with `MENTISCHAT_PERSIST=1`.
//! * `names`: `AgentName::parse` and `RoomName::parse`.

use std::hint::black_box;
use std::path::Path;
use std::time::{Duration, Instant, SystemTime};

use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use mentischat::bus::{
    Bus, Retention, MAX_BATCH, MAX_PENDING_PER_AGENT, MAX_RETAINED_BYTES, MAX_TOTAL_MESSAGES,
    MESSAGE_OVERHEAD_BYTES,
};
use mentischat::names::{AgentName, RoomName};
use tokio::runtime::{Builder, Runtime};

fn name(s: &str) -> AgentName {
    AgentName::parse(s).expect("valid bench name")
}

fn room(s: &str) -> RoomName {
    RoomName::parse(s).expect("valid bench room")
}

fn runtime() -> Runtime {
    Builder::new_current_thread().build().expect("runtime")
}

fn body(bytes: usize) -> String {
    "x".repeat(bytes)
}

/// A group with the short sampling these benches use.
fn quick<'a>(
    c: &'a mut Criterion,
    group: &str,
) -> criterion::BenchmarkGroup<'a, criterion::measurement::WallTime> {
    let mut g = c.benchmark_group(group);
    g.sample_size(10);
    g.warm_up_time(Duration::from_millis(500));
    g.measurement_time(Duration::from_secs(3));
    g
}

/// A mailbox where `me` has exchanged four read messages with each of `n`
/// peers (`mine`), or has one such peer while `n` other pairs hold a message
/// each (`others`).
async fn history_bus(dir: &Path, n: usize, mine: bool) -> Bus {
    let bus = Bus::new(dir).expect("bus opens");
    let me = name("me");
    bus.register(&me).await.expect("register");
    let text = body(256);
    let (peers, strangers) = if mine { (n, 0) } else { (1, n) };
    for i in 0..peers {
        let peer = name(&format!("peer{i}"));
        bus.register(&peer).await.expect("register");
        for _ in 0..4 {
            bus.send(&me, &peer, text.clone()).await.expect("send");
        }
    }
    for i in 0..strangers {
        let (a, b) = (name(&format!("a{i}")), name(&format!("b{i}")));
        bus.register(&a).await.expect("register");
        bus.register(&b).await.expect("register");
        bus.send(&a, &b, text.clone()).await.expect("send");
    }
    bus
}

fn bench_history(c: &mut Criterion) {
    let rt = runtime();
    let mut group = quick(c, "history");
    group.throughput(Throughput::Elements(1));
    let me = name("me");
    for (label, mine) in [("mine", true), ("others", false)] {
        for n in [10usize, 100, 1000] {
            let dir = tempfile::tempdir().expect("tempdir");
            let bus = rt.block_on(history_bus(dir.path(), n, mine));
            let peer = name("peer0");
            for (kind, with) in [("all", None), ("with", Some(&peer))] {
                group.bench_function(format!("{label}/{kind}/{n}"), |b| {
                    b.iter_custom(|iters| {
                        rt.block_on(async {
                            let started = Instant::now();
                            for _ in 0..iters {
                                black_box(bus.history(&me, with, None).await.expect("history"));
                            }
                            started.elapsed()
                        })
                    });
                });
            }
        }
    }
    group.finish();
}

/// A mailbox with `m` messages sent to 16 recipients and read, the read
/// timestamps being "now".
async fn read_mailbox(dir: &Path, m: usize) -> Bus {
    let retention = Retention {
        read_ttl: Duration::from_secs(60),
        ..Retention::default()
    };
    let bus = Bus::with_retention(dir, retention).expect("bus opens");
    let sender = name("sender");
    bus.register(&sender).await.expect("register");
    let text = body(256);
    let readers: Vec<_> = (0..16).map(|i| name(&format!("r{i}"))).collect();
    for reader in &readers {
        bus.register(reader).await.expect("register");
    }
    for i in 0..m {
        bus.send(&sender, &readers[i % 16], text.clone())
            .await
            .expect("send");
    }
    for reader in &readers {
        while bus.check_inbox(reader, 0).await.expect("drain").remaining > 0 {}
    }
    bus
}

fn bench_sweep(c: &mut Criterion) {
    let rt = runtime();
    let mut group = quick(c, "sweep");
    let later = SystemTime::now() + Duration::from_secs(120);
    for m in [100usize, 1000, MAX_TOTAL_MESSAGES as usize] {
        group.throughput(Throughput::Elements(m as u64));
        group.bench_function(format!("expire/{m}"), |b| {
            b.iter_batched(
                || {
                    let dir = tempfile::tempdir().expect("tempdir");
                    let bus = rt.block_on(read_mailbox(dir.path(), m));
                    (dir, bus)
                },
                |(dir, bus)| {
                    let out = rt.block_on(bus.sweep_at(later)).expect("sweep");
                    assert_eq!(out.expired_read as usize, m);
                    (dir, bus)
                },
                BatchSize::PerIteration,
            );
        });
    }
    // The daemon's timer tick on a mailbox with nothing to expire: every
    // agent is looked at, and the unread mail is not touched.
    for agents in [16usize, 1024] {
        let dir = tempfile::tempdir().expect("tempdir");
        let bus = rt.block_on(async {
            let bus = Bus::new(dir.path()).expect("bus opens");
            let sender = name("sender");
            bus.register(&sender).await.expect("register");
            for i in 0..agents {
                let who = name(&format!("a{i}"));
                bus.register(&who).await.expect("register");
                bus.send(&sender, &who, body(256)).await.expect("send");
            }
            bus
        });
        group.throughput(Throughput::Elements(agents as u64));
        group.bench_function(format!("idle/{agents}"), |b| {
            b.iter_custom(|iters| {
                rt.block_on(async {
                    let started = Instant::now();
                    for _ in 0..iters {
                        black_box(bus.sweep().await.expect("sweep"));
                    }
                    started.elapsed()
                })
            });
        });
    }
    group.finish();
}

fn bench_stream(c: &mut Criterion) {
    let rt = runtime();
    let mut group = quick(c, "stream");
    let dir = tempfile::tempdir().expect("tempdir");
    let bus = Bus::new(dir.path()).expect("bus opens");
    let (sender, reader) = (name("sender"), name("reader"));
    let session = rt.block_on(async {
        bus.register(&sender).await.expect("register");
        bus.register(&reader).await.expect("register");
        bus.open_stream(&reader).expect("open stream")
    });
    let text = body(256);
    for k in [1usize, 8, 64] {
        group.throughput(Throughput::Elements(k as u64));
        group.bench_function(format!("send_push_ack/{k}"), |b| {
            b.iter_custom(|iters| {
                rt.block_on(async {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        for _ in 0..k {
                            bus.send(&sender, &reader, text.clone())
                                .await
                                .expect("send");
                        }
                        let started = Instant::now();
                        let (mut got, mut last) = (0usize, 0u64);
                        while got < k {
                            let push = bus.stream_push(&session).expect("push").expect("mail");
                            got += push.messages.len();
                            last = push
                                .messages
                                .last()
                                .map_or(last, |m| m.id.parse().unwrap_or(last));
                            black_box(&push);
                        }
                        black_box(bus.stream_ack(&session, last).expect("ack"));
                        total += started.elapsed();
                    }
                    total
                })
            });
        });
    }
    group.finish();
}

/// `agent` in `rooms` rooms (one owner `owner` creates them all, up to the
/// per-agent room cap) with `pending` unread direct messages.
async fn pull_bus(dir: &Path, pending: usize, rooms: usize) -> Bus {
    let bus = Bus::new(dir).expect("bus opens");
    let (sender, agent) = (name("sender"), name("agent"));
    for who in [&sender, &agent] {
        bus.register(who).await.expect("register");
    }
    for i in 0..rooms {
        // One owner per 128 rooms keeps each below the per-agent cap.
        let owner = name(&format!("owner{}", i / 128));
        if i % 128 == 0 {
            bus.register(&owner).await.expect("register owner");
        }
        let r = room(&format!("room{i}"));
        bus.create_room(&owner, &r, false, None)
            .await
            .expect("create");
        bus.join_room(&agent, &r).await.expect("join");
    }
    for _ in 0..pending {
        bus.send(&sender, &agent, body(256)).await.expect("send");
    }
    bus
}

fn bench_pull(c: &mut Criterion) {
    let rt = runtime();
    let mut group = quick(c, "pull");
    let (sender, agent) = (name("sender"), name("agent"));
    let text = body(256);
    for pending in [1usize, 10, 100, 1000] {
        for rooms in [1usize, 16, 256] {
            assert!(pending as i64 <= MAX_PENDING_PER_AGENT);
            let dir = tempfile::tempdir().expect("tempdir");
            let bus = rt.block_on(pull_bus(dir.path(), pending, rooms));
            group.throughput(Throughput::Elements(pending.min(MAX_BATCH as usize) as u64));
            group.bench_function(format!("pending_{pending}/rooms_{rooms}"), |b| {
                b.iter_custom(|iters| {
                    rt.block_on(async {
                        let mut total = Duration::ZERO;
                        for _ in 0..iters {
                            let started = Instant::now();
                            let page = bus.check_inbox(&agent, 0).await.expect("pull");
                            total += started.elapsed();
                            // Back to `pending` unread, outside the clock.
                            for _ in 0..page.messages.len() {
                                bus.send(&sender, &agent, text.clone()).await.expect("send");
                            }
                            black_box(page);
                        }
                        total
                    })
                });
            });
        }
    }
    group.finish();
}

/// A mailbox of unread mail from `sender` to eight recipients, `bytes` per
/// body, up to the row cap or the byte budget, whichever is first.
async fn full_mailbox(dir: &Path, bytes: usize) -> Bus {
    let bus = Bus::new(dir).expect("bus opens");
    let sender = name("sender");
    bus.register(&sender).await.expect("register");
    let rows = (MAX_TOTAL_MESSAGES as usize)
        .min(MAX_RETAINED_BYTES / (bytes + MESSAGE_OVERHEAD_BYTES) - 8);
    let recipients: Vec<_> = (0..8).map(|i| name(&format!("r{i}"))).collect();
    for r in &recipients {
        bus.register(r).await.expect("register");
    }
    let text = body(bytes);
    for i in 0..rows {
        bus.send(&sender, &recipients[i % 8], text.clone())
            .await
            .expect("fill");
    }
    bus
}

fn bench_snapshot(c: &mut Criterion) {
    let rt = runtime();
    let mut group = quick(c, "snapshot");
    group.sample_size(10);
    // 256-byte bodies hit the row cap; 31 KiB bodies the byte budget.
    for (label, bytes) in [("row_cap_256B", 256usize), ("byte_budget_31KiB", 31 * 1024)] {
        group.bench_function(format!("close/{label}"), |b| {
            b.iter_batched(
                || {
                    let dir = tempfile::tempdir().expect("tempdir");
                    let bus = rt.block_on(full_mailbox(dir.path(), bytes));
                    (dir, bus)
                },
                |(dir, bus)| {
                    rt.block_on(bus.close());
                    (dir, bus)
                },
                BatchSize::PerIteration,
            );
        });
        group.bench_function(format!("restore/{label}"), |b| {
            b.iter_batched(
                || {
                    let dir = tempfile::tempdir().expect("tempdir");
                    let bus = rt.block_on(full_mailbox(dir.path(), bytes));
                    rt.block_on(bus.close());
                    dir
                },
                |dir| {
                    let restored = Bus::new(dir.path()).expect("restore");
                    (dir, restored)
                },
                BatchSize::PerIteration,
            );
        });
    }
    group.finish();
}

fn bench_names(c: &mut Criterion) {
    let mut group = c.benchmark_group("names");
    group.throughput(Throughput::Elements(1));
    let long = "a".repeat(32);
    for (label, input) in [
        ("short", "Vega"),
        ("long", long.as_str()),
        ("invalid", "no spaces"),
    ] {
        group.bench_function(format!("agent/{label}"), |b| {
            b.iter(|| black_box(AgentName::parse(black_box(input))));
        });
    }
    group.bench_function("room/short", |b| {
        b.iter(|| black_box(RoomName::parse(black_box("planning"))));
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_history,
    bench_sweep,
    bench_stream,
    bench_pull,
    bench_snapshot,
    bench_names
);
criterion_main!(benches);
