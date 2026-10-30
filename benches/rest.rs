//! REST handler cost without the socket: requests go straight into the router
//! (`tower::ServiceExt::oneshot`), so this is parsing, the engine call and the
//! JSON response, and nothing of TCP or HTTP framing.
//!
//! Run: `cargo bench --bench rest`
//!
//! `send` is one `POST /messages` to a room; `batch_64` is `POST
//! /messages/batch` of 64 (throughput counts messages); `pull` is one
//! `GET /inboxes/{agent}` returning a full page of 8, the messages having
//! been sent straight to the bus outside the timed region (on a mailbox of its own, so
//! the room backlog of the send benches does not count).

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::Body;
use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use http::Request;
use mentischat::bus::Bus;
use mentischat::names::{AgentName, RoomName};
use tower::ServiceExt;

const BODY: &str = "Reply when you can: load test message";

fn post(path: &str, body: String) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(path)
        .header("content-type", "application/json")
        .body(Body::from(body))
        .expect("request")
}

fn bench_rest(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("runtime");
    let dir = tempfile::tempdir().expect("tempdir");
    let bus = Arc::new(Bus::new(dir.path()).expect("bus opens"));
    let (s, r) = (
        AgentName::parse("sender").expect("name"),
        AgentName::parse("reader").expect("name"),
    );
    let room = RoomName::parse("bench").expect("room");
    rt.block_on(async {
        bus.register(&s).await.expect("register");
        bus.register(&r).await.expect("register");
        bus.create_room(&s, &room, false, None)
            .await
            .expect("create");
        bus.join_room(&r, &room).await.expect("join");
    });
    let app = mentischat::rest::router(bus.clone());
    let single = format!(r#"{{"from":"sender","room":"bench","body":"{BODY}"}}"#);
    let item = single.clone();
    let batch = format!("[{}]", vec![item; 64].join(","));

    let mut group = c.benchmark_group("rest");
    group.sample_size(10);
    group.warm_up_time(Duration::from_millis(500));
    group.measurement_time(Duration::from_secs(3));

    // Requests (and the router handle each one consumes) are built before the
    // clock starts, so the timed region is the handler and nothing else.
    let prebuilt = |path: &'static str, body: &str, n: u64| -> Vec<_> {
        (0..n)
            .map(|_| (app.clone(), post(path, body.to_string())))
            .collect()
    };
    let drive = |reqs: Vec<(axum::Router, Request<Body>)>| {
        rt.block_on(async {
            let started = Instant::now();
            for (app, req) in reqs {
                std::hint::black_box(app.oneshot(req).await.expect("response"));
            }
            started.elapsed()
        })
    };

    group.throughput(Throughput::Elements(1));
    group.bench_function("send", |b| {
        b.iter_custom(|iters| drive(prebuilt("/messages", &single, iters)));
    });
    group.throughput(Throughput::Elements(64));
    group.bench_function("batch_64", |b| {
        b.iter_custom(|iters| drive(prebuilt("/messages/batch", &batch, iters)));
    });
    // A mailbox of its own, so the room backlog the send benches leave behind
    // does not turn this into a pull from a deep backlog.
    let pull_dir = tempfile::tempdir().expect("tempdir");
    let pull_bus = Arc::new(Bus::new(pull_dir.path()).expect("bus opens"));
    rt.block_on(async {
        pull_bus.register(&s).await.expect("register");
        pull_bus.register(&r).await.expect("register");
    });
    let pull_app = mentischat::rest::router(pull_bus.clone());
    group.throughput(Throughput::Elements(8));
    group.bench_function("pull_8", |b| {
        b.iter_batched(
            || {
                rt.block_on(async {
                    for _ in 0..8 {
                        pull_bus
                            .send(&s, &r, BODY.to_string())
                            .await
                            .expect("send");
                    }
                });
                let req = Request::builder()
                    .uri("/inboxes/reader")
                    .body(Body::empty())
                    .expect("request");
                (pull_app.clone(), req)
            },
            |(app, req)| {
                rt.block_on(async { std::hint::black_box(app.oneshot(req).await.expect("response")) })
            },
            BatchSize::PerIteration,
        );
    });
    group.finish();
}

criterion_group!(benches, bench_rest);
criterion_main!(benches);
