//! Soak test: the real daemon binary under sustained mixed traffic, with a
//! restart in the middle.
//!
//! Six agents in one room send each other direct mail and room messages as
//! fast as a few milliseconds of pacing allows, pulling their inboxes as they
//! go. Halfway through, the workers pause, the daemon is stopped with SIGTERM
//! under `MENTISCHAT_PERSIST=1` and started again on the same port and data
//! directory, and the workers resume. At the end everything is drained and the
//! run asserts:
//!
//! 1. **No loss, no duplicates.** Every accepted direct message was delivered
//!    to its recipient exactly once, and every accepted room message to each
//!    other member exactly once, across the restart.
//! 2. **No leaks.** After a warm-up, the daemon's resident memory and open
//!    file descriptors stay within a fixed allowance of their early level in
//!    each half of the run (the restart resets the process, so each half
//!    stands alone). A leak per request would grow with the request count.
//! 3. **Clean life.** The daemon logs no panic and no error line, answers
//!    `/health`, and exits 0 on the final SIGTERM with its snapshot kept.
//!
//! Two entry points share the scenario. `soak_smoke` runs a few seconds in the
//! normal test run so the harness cannot rot, and checks 1 and 3 only (a few
//! seconds say nothing about growth). `soak_long` is `#[ignore]`d: run it with
//! `make soak SOAK_SECONDS=600`, or let the weekly `soak.yml` run it for an
//! hour. Memory and file-descriptor samples are printed so a failing run shows
//! the curve. See docs/ENGINEERING.md, "Soak test".

mod common;

use std::collections::{BTreeSet, HashMap};
use std::net::{SocketAddr, TcpListener};
use std::process::Command;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use common::process::{command, try_http, Running};
use serde_json::{json, Value};

const AGENTS: [&str; 6] = ["Soak0", "Soak1", "Soak2", "Soak3", "Soak4", "Soak5"];
const ROOM: &str = "soakroom";
/// Pause between one worker's operations; keeps the daemon busy without
/// outrunning the queue caps.
const PACE: Duration = Duration::from_millis(3);
/// How often resources are sampled.
const SAMPLE_EVERY: Duration = Duration::from_secs(5);
/// Fraction of each half of the run treated as warm-up before the baseline.
const WARMUP_FRACTION: f64 = 0.25;
/// Resident memory may grow by this factor plus [`RSS_SLACK_KB`] over the
/// early level of a half before the run calls it a leak.
const RSS_FACTOR: f64 = 1.5;
const RSS_SLACK_KB: u64 = 48 * 1024;
/// Open file descriptors may exceed the early maximum by this many. Each
/// request opens and closes its own connection, so a leak is linear in
/// requests and dwarfs this.
const FD_SLACK: usize = 24;

/// What the workers did, shared across threads.
#[derive(Default)]
struct Ledger {
    /// Accepted direct messages: body -> recipient.
    direct: Mutex<HashMap<String, String>>,
    /// Accepted room messages: body -> sender.
    room: Mutex<HashMap<String, String>>,
    /// `(recipient, body)` -> how many times it was delivered.
    delivered: Mutex<HashMap<(String, String), u32>>,
    /// Room messages each agent was told it missed (`skipped`): a member
    /// that falls more than a room's capacity behind loses the oldest.
    skipped: Mutex<HashMap<String, u64>>,
    requests: AtomicU64,
    rejected: AtomicU64,
}

/// A request to the daemon at the address in `addr`, retried briefly while the
/// daemon is restarting (connection refused). A reply of any status is final.
fn call(addr: &Mutex<SocketAddr>, method: &str, path: &str, body: Option<&str>) -> (u16, String) {
    for _ in 0..200 {
        let target = *addr.lock().expect("addr lock");
        if let Ok(reply) = try_http(target, method, path, body) {
            return reply;
        }
        thread::sleep(Duration::from_millis(50));
    }
    panic!("the daemon never answered {method} {path}");
}

/// Pull `agent`'s inbox once and record every message.
fn pull(addr: &Mutex<SocketAddr>, ledger: &Ledger, agent: &str) -> usize {
    let (status, text) = call(addr, "GET", &format!("/inboxes/{agent}?wait_ms=0"), None);
    ledger.requests.fetch_add(1, Ordering::Relaxed);
    assert_eq!(status, 200, "pull for {agent}: {text}");
    let reply: Value = serde_json::from_str(&text).expect("inbox JSON");
    let messages = reply["messages"].as_array().expect("messages array");
    if let Some(skipped) = reply["skipped"].as_u64() {
        *ledger
            .skipped
            .lock()
            .expect("ledger")
            .entry(agent.to_string())
            .or_default() += skipped;
    }
    let mut delivered = ledger.delivered.lock().expect("ledger");
    for message in messages {
        let body = message["body"].as_str().expect("body").to_string();
        *delivered.entry((agent.to_string(), body)).or_default() += 1;
    }
    messages.len()
}

/// One worker's loop: a mix of direct sends, room sends and pulls until
/// `stop`, holding still while `paused`.
fn worker(
    index: usize,
    addr: Arc<Mutex<SocketAddr>>,
    ledger: Arc<Ledger>,
    stop: Arc<AtomicBool>,
    paused: Arc<AtomicBool>,
    idle: Arc<AtomicU64>,
) {
    let me = AGENTS[index];
    let mut seq = 0u64;
    let mut parked = false;
    while !stop.load(Ordering::Relaxed) {
        if paused.load(Ordering::SeqCst) {
            if !parked {
                idle.fetch_add(1, Ordering::SeqCst);
                parked = true;
            }
            thread::sleep(Duration::from_millis(10));
            continue;
        }
        if parked {
            idle.fetch_sub(1, Ordering::SeqCst);
            parked = false;
        }
        seq += 1;
        match seq % 5 {
            0 | 1 => send_direct(&addr, &ledger, me, index, seq),
            2 | 3 => send_room(&addr, &ledger, me, seq),
            _ => while pull(&addr, &ledger, me) > 0 {},
        }
        if seq.is_multiple_of(40) {
            let (status, _) = call(&addr, "GET", &format!("/history/{me}?limit=5"), None);
            ledger.requests.fetch_add(1, Ordering::Relaxed);
            assert_eq!(status, 200, "history for {me}");
        }
        thread::sleep(PACE);
    }
    if parked {
        idle.fetch_sub(1, Ordering::SeqCst);
    }
}

fn send_direct(addr: &Mutex<SocketAddr>, ledger: &Ledger, me: &str, index: usize, seq: u64) {
    let peer = AGENTS[(index + 1 + (seq as usize % (AGENTS.len() - 1))) % AGENTS.len()];
    let body = format!("direct|{me}|{peer}|{seq}|{}", "x".repeat(150));
    let request = json!({ "from": me, "to": peer, "body": body }).to_string();
    let (status, text) = call(addr, "POST", "/messages", Some(&request));
    ledger.requests.fetch_add(1, Ordering::Relaxed);
    match status {
        200 => {
            ledger
                .direct
                .lock()
                .expect("ledger")
                .insert(body, peer.to_string());
        }
        // A full queue is the documented backpressure, not a failure.
        409 if text.contains("queue_full") => {
            ledger.rejected.fetch_add(1, Ordering::Relaxed);
        }
        other => panic!("direct send {me} -> {peer}: {other} {text}"),
    }
}

fn send_room(addr: &Mutex<SocketAddr>, ledger: &Ledger, me: &str, seq: u64) {
    let body = format!("room|{me}|{seq}|{}", "y".repeat(150));
    let request = json!({ "from": me, "room": ROOM, "body": body }).to_string();
    let (status, text) = call(addr, "POST", "/messages", Some(&request));
    ledger.requests.fetch_add(1, Ordering::Relaxed);
    match status {
        200 => {
            ledger
                .room
                .lock()
                .expect("ledger")
                .insert(body, me.to_string());
        }
        409 if text.contains("queue_full") => {
            ledger.rejected.fetch_add(1, Ordering::Relaxed);
        }
        other => panic!("room send by {me}: {other} {text}"),
    }
}

/// Resident set size in KiB and open file descriptors of process `pid`.
fn sample(pid: u32) -> (u64, usize) {
    let rss = Command::new("ps")
        .args(["-o", "rss=", "-p", &pid.to_string()])
        .output()
        .ok()
        .and_then(|o| String::from_utf8_lossy(&o.stdout).trim().parse().ok())
        .unwrap_or(0);
    let fds = match std::fs::read_dir(format!("/proc/{pid}/fd")) {
        Ok(dir) => dir.count(),
        Err(_) => Command::new("lsof")
            .args(["-p", &pid.to_string(), "-Ff"])
            .output()
            .map(|o| String::from_utf8_lossy(&o.stdout).lines().count())
            .unwrap_or(0),
    };
    (rss, fds)
}

/// Samples taken during one half of the run: `(seconds into the half, rss
/// KiB, fds)`.
type Curve = Vec<(f64, u64, usize)>;

/// Leak check of one half: the late samples may not exceed the early level by
/// more than the allowances above. The curve is printed either way.
fn assert_no_growth(label: &str, curve: &Curve, half: Duration) {
    println!("{label}: seconds  rss_kib  fds");
    for (t, rss, fds) in curve {
        println!("{label}: {t:7.1}  {rss:8}  {fds:4}");
    }
    let warm = half.as_secs_f64() * WARMUP_FRACTION;
    let early: Vec<_> = curve.iter().filter(|s| s.0 >= warm).take(3).collect();
    let late: Vec<_> = curve.iter().rev().take(3).collect();
    assert!(
        early.len() >= 3 && late.len() >= 3,
        "{label}: too few samples ({}) to judge growth",
        curve.len()
    );
    let early_rss = early.iter().map(|s| s.1).max().expect("early");
    let late_rss = late.iter().map(|s| s.1).max().expect("late");
    let allowed = (early_rss as f64 * RSS_FACTOR) as u64 + RSS_SLACK_KB;
    assert!(
        late_rss <= allowed,
        "{label}: resident memory grew from {early_rss} KiB to {late_rss} KiB (allowed {allowed})"
    );
    let early_fds = early.iter().map(|s| s.2).max().expect("early");
    let late_fds = late.iter().map(|s| s.2).max().expect("late");
    assert!(
        late_fds <= early_fds + FD_SLACK,
        "{label}: open descriptors grew from {early_fds} to {late_fds}"
    );
}

/// A loopback port nobody holds right now; reused for both daemon runs so the
/// workers keep one address across the restart.
fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    listener.local_addr().expect("addr").port()
}

fn start_daemon(dir: &std::path::Path, port: u16) -> Running {
    let mut cmd = command(dir);
    cmd.env("MENTISCHAT_PERSIST", "1")
        .env("MENTISCHAT_LISTEN_PORT", port.to_string())
        // Short enough that read mail expires several times during a long run,
        // so a leak in the expiry path would show.
        .env("MENTISCHAT_READ_TTL", "20");
    Running::start(cmd)
}

/// Sample the daemon until `until`, returning the curve of this half.
fn sample_until(daemon: &Running, started: Instant, until: Instant) -> Curve {
    let mut curve = Curve::new();
    while Instant::now() < until {
        let (rss, fds) = sample(daemon.child.id());
        curve.push((started.elapsed().as_secs_f64(), rss, fds));
        thread::sleep(SAMPLE_EVERY.min(until.saturating_duration_since(Instant::now())));
    }
    curve
}

/// Let every worker park (`paused` is already set) so nothing is in flight.
fn wait_until_idle(idle: &AtomicU64) {
    let deadline = Instant::now() + Duration::from_secs(60);
    while idle.load(Ordering::SeqCst) < AGENTS.len() as u64 {
        assert!(Instant::now() < deadline, "workers did not pause");
        thread::sleep(Duration::from_millis(10));
    }
}

/// The scenario. `check_growth` is false for the short run.
fn run_soak(total: Duration, check_growth: bool) {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = free_port();
    let addr = Arc::new(Mutex::new(SocketAddr::from(([127, 0, 0, 1], port))));
    let mut daemon = start_daemon(dir.path(), port);

    for agent in AGENTS {
        let (status, text) = call(
            &addr,
            "POST",
            "/agents",
            Some(&json!({ "agent": agent }).to_string()),
        );
        assert_eq!(status, 200, "register {agent}: {text}");
    }
    let (status, text) = call(
        &addr,
        "POST",
        "/rooms",
        Some(&json!({ "agent": AGENTS[0], "room": ROOM }).to_string()),
    );
    assert_eq!(status, 200, "create room: {text}");
    for agent in &AGENTS[1..] {
        let path = format!("/rooms/{ROOM}/members");
        let (status, text) = call(
            &addr,
            "POST",
            &path,
            Some(&json!({ "agent": agent }).to_string()),
        );
        assert_eq!(status, 200, "join {agent}: {text}");
    }

    let ledger = Arc::new(Ledger::default());
    let stop = Arc::new(AtomicBool::new(false));
    let paused = Arc::new(AtomicBool::new(false));
    let idle = Arc::new(AtomicU64::new(0));
    let workers: Vec<_> = (0..AGENTS.len())
        .map(|i| {
            let (a, l, s, p, n) = (
                addr.clone(),
                ledger.clone(),
                stop.clone(),
                paused.clone(),
                idle.clone(),
            );
            thread::spawn(move || worker(i, a, l, s, p, n))
        })
        .collect();

    let started = Instant::now();
    let half = total / 2;
    let first = sample_until(&daemon, started, started + half);

    // The restart: pause, let in-flight calls finish, stop cleanly (the
    // snapshot is written), start again on the same port and directory.
    paused.store(true, Ordering::SeqCst);
    wait_until_idle(&idle);
    let mut log = daemon.drain_log();
    let status = daemon.terminate();
    assert_eq!(status.code(), Some(0), "the first run stops cleanly");
    assert!(
        dir.path().join("snapshot.json").exists(),
        "persist kept the mailbox"
    );
    daemon = start_daemon(dir.path(), port);
    let (status, health) = call(&addr, "GET", "/health", None);
    assert_eq!(status, 200, "{health}");
    let health: Value = serde_json::from_str(&health).expect("health JSON");
    assert_eq!(
        health["agents"],
        AGENTS.len(),
        "the roster survived the restart: {health}"
    );
    paused.store(false, Ordering::SeqCst);

    let second_started = Instant::now();
    let second = sample_until(&daemon, second_started, second_started + (total - half));

    stop.store(true, Ordering::SeqCst);
    for w in workers {
        w.join().expect("a worker panicked");
    }
    // Drain: pull until every inbox is empty so nothing is left in flight.
    for agent in AGENTS {
        while pull(&addr, &ledger, agent) > 0 {}
    }
    let (status, health) = call(&addr, "GET", "/health", None);
    assert_eq!(status, 200, "{health}");

    log.extend(daemon.drain_log());
    let status = daemon.terminate();
    assert_eq!(status.code(), Some(0), "the second run stops cleanly");
    for line in &log {
        assert!(
            !line.contains("panicked") && !line.contains("ERROR"),
            "the daemon logged a problem: {line}"
        );
    }

    assert_delivered_exactly_once(&ledger);
    println!(
        "soak: {} requests, {} direct and {} room messages accepted, {} refused as queue_full",
        ledger.requests.load(Ordering::Relaxed),
        ledger.direct.lock().expect("ledger").len(),
        ledger.room.lock().expect("ledger").len(),
        ledger.rejected.load(Ordering::Relaxed)
    );
    if check_growth {
        assert_no_growth("before restart", &first, half);
        assert_no_growth("after restart", &second, total - half);
    }
}

/// Every accepted message reached exactly the agents it was addressed to, once.
fn assert_delivered_exactly_once(ledger: &Ledger) {
    let direct = ledger.direct.lock().expect("ledger");
    let room = ledger.room.lock().expect("ledger");
    let delivered = ledger.delivered.lock().expect("ledger");
    assert!(
        !direct.is_empty() && !room.is_empty(),
        "the workers did nothing"
    );

    let mut expected: BTreeSet<(String, String)> = BTreeSet::new();
    for (body, to) in direct.iter() {
        expected.insert((to.clone(), body.clone()));
    }
    for (body, from) in room.iter() {
        for member in AGENTS.iter().filter(|m| *m != from) {
            expected.insert((member.to_string(), body.clone()));
        }
    }
    // Direct mail is never lost: an unread queue only refuses (queue_full).
    // Room mail may be pushed out of a ring that a member fell too far behind
    // on, and the daemon must say so: per recipient, the room messages that
    // never arrived are exactly the ones its pulls reported as `skipped`.
    let skipped = ledger.skipped.lock().expect("ledger");
    let mut lost_direct = Vec::new();
    let mut lost_room: HashMap<&str, u64> = HashMap::new();
    for key in expected.iter().filter(|k| !delivered.contains_key(*k)) {
        if direct.contains_key(&key.1) {
            lost_direct.push(key);
        } else {
            *lost_room.entry(key.0.as_str()).or_default() += 1;
        }
    }
    assert!(
        lost_direct.is_empty(),
        "{} accepted direct messages never delivered, e.g. {:?}",
        lost_direct.len(),
        lost_direct
            .iter()
            .take(3)
            .map(|k| &k.1[..24])
            .collect::<Vec<_>>()
    );
    for agent in AGENTS {
        let lost = lost_room.get(agent).copied().unwrap_or(0);
        let told = skipped.get(agent).copied().unwrap_or(0);
        assert_eq!(
            lost, told,
            "{agent}: {lost} room messages never arrived but {told} were reported skipped"
        );
    }
    let duplicated: Vec<_> = delivered.iter().filter(|(_, n)| **n > 1).take(5).collect();
    assert!(
        duplicated.is_empty(),
        "messages delivered more than once, e.g. {duplicated:?}"
    );
    let stray: Vec<_> = delivered
        .keys()
        .filter(|k| !expected.contains(*k))
        .take(5)
        .collect();
    assert!(
        stray.is_empty(),
        "messages delivered to someone they were not for, e.g. {stray:?}"
    );
}

/// A few seconds of the scenario in the ordinary test run: delivery across a
/// restart and a clean life, which is all a short run can show.
#[test]
fn soak_smoke() {
    run_soak(Duration::from_secs(8), false);
}

/// The long run, for `make soak` and the weekly workflow. Duration comes from
/// `SOAK_SECONDS` (default 600). `#[ignore]` keeps it out of `cargo test`.
#[test]
#[ignore = "long-running; run with `make soak`"]
fn soak_long() {
    let seconds: u64 = std::env::var("SOAK_SECONDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(600);
    assert!(
        seconds >= 120,
        "SOAK_SECONDS must be at least 120 to judge growth"
    );
    run_soak(Duration::from_secs(seconds), true);
}
