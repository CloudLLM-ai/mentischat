//! Mailbox throughput through the [`Bus`] API, no HTTP in the way: one send
//! followed by one pull, on an empty mailbox and on one holding the row cap.
//!
//! Run: `cargo bench --bench bus`
//!
//! `empty` keeps the mailbox near empty: the mail is wiped every
//! [`EMPTY_CHUNK`] messages, outside the timed region, so the row count never
//! builds up. `at_cap` first fills the mailbox to
//! [`MAX_TOTAL_MESSAGES`] rows and reads them all, so every send in the timed
//! loop has to make room by dropping the oldest read message: the steady
//! state of a busy daemon. One iteration is one message delivered, so the
//! reported throughput is messages per second. `contention` runs the same
//! loop for 1, 2, 4 and 8 independent pairs at once on a multi-thread runtime.

use std::hint::black_box;
use std::time::{Duration, Instant};

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use mentischat::bus::{Bus, MAX_PENDING_PER_AGENT, MAX_TOTAL_MESSAGES};
use mentischat::names::AgentName;

/// Messages sent between wipes in the `empty` case; small enough that the
/// mailbox stays far from the row cap.
const EMPTY_CHUNK: u64 = 256;
/// A message body of the size agents typically send: a few hundred bytes.
const BODY_BYTES: usize = 256;

fn name(s: &str) -> AgentName {
    AgentName::parse(s).expect("valid bench name")
}

/// Register `sender` and `reader` on a fresh mailbox.
async fn register_pair(bus: &Bus) {
    for who in ["sender", "reader"] {
        bus.register(&name(who)).await.expect("register");
    }
}

/// A bus with `sender` and `reader` registered, keeping its files (if the
/// engine writes any) under `dir`.
async fn bus_with_pair(dir: &std::path::Path) -> Bus {
    let bus = Bus::new(dir).expect("bus opens");
    register_pair(&bus).await;
    bus
}

/// Like [`send_and_pull`], but wipes the mailbox every [`EMPTY_CHUNK`]
/// messages without counting the wipe, so it measures a near-empty mailbox.
async fn send_and_pull_near_empty(bus: &Bus, iters: u64, body: &str) -> Duration {
    let mut total = Duration::ZERO;
    let mut left = iters;
    while left > 0 {
        let chunk = left.min(EMPTY_CHUNK);
        total += send_and_pull(bus, chunk, body).await;
        left -= chunk;
        bus.clear().await.expect("clear");
        register_pair(bus).await;
    }
    total
}

/// Fill the mailbox to the global row cap, then read every message, leaving
/// `MAX_TOTAL_MESSAGES` read rows behind.
async fn fill_to_cap(bus: &Bus, body: &str) {
    let sender = name("sender");
    let victims = MAX_TOTAL_MESSAGES / MAX_PENDING_PER_AGENT;
    for i in 0..victims {
        let victim = name(&format!("victim{i}"));
        bus.register(&victim).await.expect("register victim");
        for _ in 0..MAX_PENDING_PER_AGENT {
            bus.send(&sender, &victim, body.to_string())
                .await
                .expect("fill");
        }
        while bus.check_inbox(&victim, 0).await.expect("drain").remaining > 0 {}
    }
}

/// Send `iters` messages `sender` -> `reader`, pulling each one back, and
/// return the time it took.
async fn send_and_pull(bus: &Bus, iters: u64, body: &str) -> Duration {
    let (sender, reader) = (name("sender"), name("reader"));
    let started = Instant::now();
    for _ in 0..iters {
        bus.send(&sender, &reader, body.to_string())
            .await
            .expect("send");
        black_box(bus.check_inbox(&reader, 0).await.expect("pull"));
    }
    started.elapsed()
}

/// `tasks` pairs of agents on worker threads, each pair sending and pulling
/// `iters / tasks` messages at the same time; the time the slowest took.
async fn contended(bus: std::sync::Arc<Bus>, tasks: u64, iters: u64, body: &str) -> Duration {
    let started = Instant::now();
    let workers: Vec<_> = (0..tasks)
        .map(|t| {
            let (bus, body) = (bus.clone(), body.to_string());
            tokio::spawn(async move {
                let (sender, reader) = (name(&format!("s{t}")), name(&format!("r{t}")));
                for who in [&sender, &reader] {
                    bus.register(who).await.expect("register");
                }
                for _ in 0..iters / tasks {
                    bus.send(&sender, &reader, body.clone())
                        .await
                        .expect("send");
                    black_box(bus.check_inbox(&reader, 0).await.expect("pull"));
                }
            })
        })
        .collect();
    for worker in workers {
        worker.await.expect("worker");
    }
    started.elapsed()
}

fn bench_contention(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .build()
        .expect("runtime");
    let body = "x".repeat(BODY_BYTES);
    let dir = tempfile::tempdir().expect("tempdir");
    let bus = std::sync::Arc::new(Bus::new(dir.path()).expect("bus opens"));
    let mut group = c.benchmark_group("contention");
    group.throughput(Throughput::Elements(1));
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(3));
    for tasks in [1u64, 2, 4, 8] {
        group.bench_function(format!("{tasks}_pairs"), |b| {
            b.iter_custom(|iters| {
                rt.block_on(contended(bus.clone(), tasks, iters.max(tasks), &body))
            });
            // Read mail piles up toward the row cap otherwise.
            rt.block_on(bus.clear()).expect("clear");
        });
    }
    group.finish();
}

fn bench_send_pull(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("runtime");
    let body = "x".repeat(BODY_BYTES);
    let mut group = c.benchmark_group("send_pull");
    group.throughput(Throughput::Elements(1));
    // The old engine slows to a few hundred messages a second at the cap, so
    // keep the sampling short enough to finish.
    group.sample_size(10);
    group.warm_up_time(Duration::from_millis(500));
    group.measurement_time(Duration::from_secs(3));

    let (empty_dir, full_dir) = (
        tempfile::tempdir().expect("tempdir"),
        tempfile::tempdir().expect("tempdir"),
    );
    let empty = rt.block_on(bus_with_pair(empty_dir.path()));
    group.bench_function("empty", |b| {
        b.iter_custom(|iters| rt.block_on(send_and_pull_near_empty(&empty, iters, &body)));
    });

    let full = rt.block_on(async {
        let bus = bus_with_pair(full_dir.path()).await;
        fill_to_cap(&bus, &body).await;
        bus
    });
    group.bench_function("at_cap", |b| {
        b.iter_custom(|iters| rt.block_on(send_and_pull(&full, iters, &body)));
    });
    group.finish();
}

criterion_group!(benches, bench_send_pull, bench_contention);
criterion_main!(benches);
