//! REST handler cost without the socket: requests go straight into the router
//! (`tower::ServiceExt::oneshot`), so this is parsing, the engine call and the
//! JSON response, and nothing of TCP or HTTP framing.
//!
//! Run: `cargo bench --bench rest`
//!
//! `send` is one `POST /messages` to a room; `batch_64` is `POST
//! /messages/batch` of 64 (throughput counts messages); `pull` is one
//! `GET /inboxes/{agent}` returning a full page of 8, the messages having
//! been sent straight to the bus outside the timed region.

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::Body;
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
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

    group.throughput(Throughput::Elements(1));
    group.bench_function("send", |b| {
        b.iter_custom(|iters| {
            rt.block_on(async {
                let started = Instant::now();
                for _ in 0..iters {
                    let resp = app.clone().oneshot(post("/messages", single.clone())).await;
                    std::hint::black_box(resp.expect("response"));
                }
                started.elapsed()
            })
        });
    });
    group.throughput(Throughput::Elements(64));
    group.bench_function("batch_64", |b| {
        b.iter_custom(|iters| {
            rt.block_on(async {
                let started = Instant::now();
                for _ in 0..iters {
                    let resp = app
                        .clone()
                        .oneshot(post("/messages/batch", batch.clone()))
                        .await;
                    std::hint::black_box(resp.expect("response"));
                }
                started.elapsed()
            })
        });
    });
    group.throughput(Throughput::Elements(8));
    group.bench_function("pull_8", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                rt.block_on(async {
                    for _ in 0..8 {
                        bus.send_room(&s, &room, BODY.to_string())
                            .await
                            .expect("send");
                    }
                    let req = Request::builder()
                        .uri("/inboxes/reader")
                        .body(Body::empty())
                        .expect("request");
                    let started = Instant::now();
                    let resp = app.clone().oneshot(req).await;
                    std::hint::black_box(resp.expect("response"));
                    total += started.elapsed();
                });
            }
            total
        });
    });
    group.finish();
}

criterion_group!(benches, bench_rest);
criterion_main!(benches);
