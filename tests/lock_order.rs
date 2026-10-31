//! Lock-order regressions: paths that, run concurrently, used to take the
//! same locks in opposite orders and could deadlock.

mod common;

use common::{fresh_bus, name, room};
use mentischat::bus::Bus;
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::Duration;

/// Run `f` to completion on its own current-thread runtime.
fn block_on<F: std::future::Future>(f: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime")
        .block_on(f)
}

/// Keep `room` busy so the puller's next pull looks at it, and pull for
/// `agent` over and over: each pull holds `agent`'s lock while it locks the
/// rooms with mail for it.
fn pull_loop(bus: Arc<Bus>, agent: &'static str, busy: &'static str, rounds: usize) {
    block_on(async {
        for i in 0..rounds {
            if i % 4 == 0 {
                let _ = bus.send_room(&name("a"), &room(busy), "x".into()).await;
            }
            let _ = bus.check_inbox(&name(agent), 0).await;
        }
    });
}

/// Join `agent` to `target` and leave again, over and over: each join of a
/// room it is not in locks the room, and used to lock `agent` under it.
fn join_loop(bus: Arc<Bus>, agent: &'static str, target: &'static str, rounds: usize) {
    block_on(async {
        for _ in 0..rounds {
            let _ = bus.join_room(&name(agent), &room(target)).await;
            let _ = bus.leave_room(&name(agent), &room(target)).await;
        }
    });
}

/// A pull holds its agent's lock while it locks that agent's rooms;
/// `join_room` used to hold a room's lock while it locked the joining agent.
/// With two agents that closes a cycle: b pulls (holds b, wants x), c joins
/// x (holds x, wants c), c pulls (holds c, wants z), b joins z (holds z,
/// wants b). Racing those four must never deadlock.
#[test]
fn joining_rooms_while_pulling_does_not_deadlock() {
    const ROUNDS: usize = 50_000;
    let (_dir, bus) = fresh_bus("lock_order");
    block_on(async {
        for n in ["a", "b", "c"] {
            bus.register(&name(n)).await.unwrap();
        }
        for r in ["x", "z"] {
            bus.create_room(&name("a"), &room(r), false, None)
                .await
                .unwrap();
        }
        bus.join_room(&name("b"), &room("x")).await.unwrap();
        bus.join_room(&name("c"), &room("z")).await.unwrap();
    });
    let (done_tx, done_rx) = mpsc::channel();
    let workers: Vec<_> = [
        Box::new({
            let bus = bus.clone();
            move || pull_loop(bus, "b", "x", ROUNDS)
        }) as Box<dyn FnOnce() + Send>,
        Box::new({
            let bus = bus.clone();
            move || pull_loop(bus, "c", "z", ROUNDS)
        }),
        Box::new({
            let bus = bus.clone();
            move || join_loop(bus, "c", "x", ROUNDS)
        }),
        Box::new({
            let bus = bus.clone();
            move || join_loop(bus, "b", "z", ROUNDS)
        }),
    ]
    .into_iter()
    .map(|work| {
        let done = done_tx.clone();
        thread::spawn(move || {
            work();
            done.send(()).unwrap();
        })
    })
    .collect();
    // A deadlock parks the workers forever; fail on a timeout instead of
    // hanging the suite.
    for _ in 0..workers.len() {
        done_rx
            .recv_timeout(Duration::from_secs(60))
            .expect("join_room and check_inbox deadlocked");
    }
    for w in workers {
        w.join().unwrap();
    }
}
