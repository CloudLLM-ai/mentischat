//! Ping-pong: two agents with their own hand-written clients count `1`, `2`,
//! `3`, ... through a real daemon over real sockets, each number answering the
//! last. Every number must arrive once, in order, from the peer.
//!
//! The players are `examples/ping_pong/agent.rs`, the same code the
//! `ping_pong` benchmark runs as two processes, so the benchmark and this test
//! cannot drift apart.
//!
//! - `ws` and `poll` count to 100 and must finish well inside a second: on
//!   loopback a hop costs about 0.1 ms (measured: 100 numbers in 10-15 ms).
//!   The bound is loose so it fails only on a real regression, such as Nagle
//!   holding each frame 40 ms (`a298aea`), which would take 4 s.
//! - `notify` counts to 4 and checks the wake-up socket's designed pacing:
//!   the first frame comes `SETTLE` after the mail, and a later one never
//!   within `COOLDOWN` of the previous frame on that socket, so a harness
//!   agent is not woken per message.

#[path = "../examples/ping_pong/agent.rs"]
mod agent;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use agent::{Hop, Mode, Player, Summary};
use mentischat::bus::Bus;
use mentischat::notify::{COOLDOWN, SETTLE};

/// A daemon on an ephemeral loopback port, served the way `main.rs` serves it.
async fn daemon() -> (String, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let bus = Arc::new(Bus::new(&dir.path().join("pp")).unwrap());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = mentischat::build_router(bus);
    tokio::spawn(async move {
        axum::serve(
            mentischat::low_latency_listener(listener),
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });
    (addr.to_string(), dir)
}

/// Play one game; both players' hops, sorted by number.
async fn game(mode: Mode, to: u32) -> Vec<Hop> {
    let (addr, _dir) = daemon().await;
    // The answerer listens before the opener sends `1`.
    let pong = Player::join(&addr, "pong", "ping", mode).await.unwrap();
    let ping = Player::join(&addr, "ping", "pong", mode).await.unwrap();
    let (a, b) = tokio::join!(ping.play(true, to), pong.play(false, to));
    let (a, b) = (a.unwrap(), b.unwrap());

    // The opener received the even numbers, the answerer the odd ones.
    assert!(a.iter().all(|h| h.n % 2 == 0), "{a:?}");
    assert!(b.iter().all(|h| h.n % 2 == 1), "{b:?}");
    let mut hops = a;
    hops.extend(b);
    hops.sort_by_key(|h| h.n);
    let numbers: Vec<u32> = hops.iter().map(|h| h.n).collect();
    assert_eq!(
        numbers,
        (1..=to).collect::<Vec<_>>(),
        "every number once, in order"
    );
    hops
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ping_pong_to_100_over_ws_streams() {
    let s = Summary::of(game(Mode::Ws, 100).await).unwrap();
    assert!(
        s.total < Duration::from_secs(1),
        "100 numbers over /ws took {:?}",
        s.total
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ping_pong_to_100_over_parked_pulls() {
    let s = Summary::of(game(Mode::Poll, 100).await).unwrap();
    assert!(
        s.total < Duration::from_secs(1),
        "100 numbers over parked pulls took {:?}",
        s.total
    );
}

/// The wake-up socket paces itself, per socket: every number waits at least
/// `SETTLE` (the burst window) before its frame, and one player's frames are
/// never closer together than `COOLDOWN`. Because the cooldown is per socket,
/// a player that waited out its cooldown answers into a peer whose own
/// cooldown has already expired, so the game runs at about two numbers per
/// `COOLDOWN` (measured to 100: hop p50 208 ms, max 2.80 s, 147 s in all).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ping_pong_over_notify_follows_the_wake_up_pacing() {
    let hops = game(Mode::Notify, 4).await;
    let tolerance = Duration::from_millis(20);
    for h in &hops {
        assert!(
            h.latency() + tolerance >= SETTLE,
            "hop {} beat the settle window: {:?}",
            h.n,
            h.latency()
        );
    }
    // Hop 3 is the answerer's second frame, sent soon after its first, so it
    // is the one that waits for the cooldown.
    assert!(
        hops[2].latency() > Duration::from_secs(1),
        "hop 3 did not wait for the cooldown: {:?}",
        hops[2].latency()
    );
    for parity in [0, 1] {
        let frames: Vec<u64> = hops
            .iter()
            .filter(|h| h.n % 2 == parity)
            .map(|h| h.recv_us)
            .collect();
        for pair in frames.windows(2) {
            let gap = Duration::from_micros(pair[1] - pair[0]);
            assert!(
                gap + tolerance >= COOLDOWN,
                "two frames on one socket {gap:?} apart"
            );
        }
    }
    let s = Summary::of(hops).unwrap();
    assert!(
        s.total < Duration::from_secs(5),
        "4 numbers over /notify took {:?}",
        s.total
    );
}
