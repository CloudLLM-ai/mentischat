//! Helpers shared by the integration tests. Each test crate uses a different
//! subset, so the ones a crate leaves unused are allowed rather than warned
//! about.
#![allow(dead_code)]

pub mod process;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mentischat::bus::{Bus, KeyProof, Registration, Retention, Stats, TrafficLog};
use mentischat::names::{AgentName, RoomName};
use mentischat::signed::{deregister_payload, reclaim_payload, register_key_payload, Keypair};
use mentischat::tui::LogBuffer;

pub fn name(s: &str) -> AgentName {
    AgentName::parse(s).expect("valid test name")
}

pub fn room(s: &str) -> RoomName {
    RoomName::parse(s).expect("valid test room")
}

/// Register every name in `names` on `bus`.
pub async fn register(bus: &Bus, names: &[&str]) {
    for n in names {
        bus.register(&name(n)).await.unwrap();
    }
}

/// A bus with the default retention in a fresh temp directory.
pub fn fresh_bus(tag: &str) -> (tempfile::TempDir, Arc<Bus>) {
    bus_with(tag, Retention::default())
}

/// A bus with `retention` in a fresh temp directory.
pub fn bus_with(tag: &str, retention: Retention) -> (tempfile::TempDir, Arc<Bus>) {
    let dir = tempfile::tempdir().expect("tempdir");
    let bus = Bus::with_retention(&dir.path().join(tag), retention).expect("bus opens");
    (dir, Arc::new(bus))
}

/// A bus in a fresh temp directory whose traffic lines show `traffic`.
pub fn traffic_bus(traffic: TrafficLog) -> (tempfile::TempDir, Arc<Bus>) {
    let dir = tempfile::tempdir().expect("tempdir");
    let bus = Bus::new(&dir.path().join("traffic"))
        .expect("bus opens")
        .with_traffic_log(traffic);
    (dir, Arc::new(bus))
}

/// Capture this thread's log output, formatted as the terminal view formats
/// it, until the guard drops.
pub fn capture_logs() -> (LogBuffer, tracing::subscriber::DefaultGuard) {
    let logs = LogBuffer::new();
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .with_target(false)
        .without_time()
        .compact()
        .with_writer(logs.clone())
        .finish();
    (logs, tracing::subscriber::set_default(subscriber))
}

/// The traffic lines captured so far, without their time and level.
pub fn traffic_lines(logs: &LogBuffer) -> Vec<String> {
    logs.lines()
        .iter()
        .filter_map(|l| l.split_once(" INFO ").map(|(_, rest)| rest.to_string()))
        .filter(|l| l.starts_with("sent ") || l.starts_with("read "))
        .collect()
}

/// The default retention with agents dropped after `secs` idle.
pub fn idle_after(secs: u64) -> Retention {
    Retention {
        agent_ttl: Duration::from_secs(secs),
        ..Retention::default()
    }
}

/// Bind a deterministic key (`seed` fills the 32-byte secret) to `agent`,
/// which must be registered, and return it.
pub async fn bind_key(bus: &Bus, agent: &str, seed: u8) -> Keypair {
    let keys = Keypair::from_seed([seed; 32]);
    let key = keys.public();
    bus.register_key(
        &name(agent),
        key,
        &keys.sign(&register_key_payload(agent, &key)),
    )
    .await
    .expect("bind key");
    keys
}

/// A reclaim proof for `agent` at `counter`, signed by `keys`.
pub fn proof(keys: &Keypair, agent: &str, counter: u64) -> KeyProof {
    KeyProof {
        counter,
        signature: keys.sign(&reclaim_payload(agent, counter)),
    }
}

/// A deregister proof for `agent` at `counter`, signed by `keys`.
pub fn deregister_proof(keys: &Keypair, agent: &str, counter: u64) -> KeyProof {
    KeyProof {
        counter,
        signature: keys.sign(&deregister_payload(agent, counter)),
    }
}

/// Register options that take `agent` back with a proof signed by `keys` at
/// `counter`.
pub fn proven_reclaim(keys: &Keypair, agent: &str, counter: u64) -> Registration {
    Registration {
        reclaim: true,
        proof: Some(proof(keys, agent, counter)),
        ..Registration::default()
    }
}

/// Register options that take a held keyless name back (no proof).
pub fn reclaim() -> Registration {
    Registration {
        reclaim: true,
        ..Registration::default()
    }
}

pub fn stats(agents: i64, unread: i64, read: i64) -> Stats {
    Stats {
        agents,
        unread,
        read,
    }
}

/// `secs` from now, for `Bus::sweep_at`.
pub fn later(secs: u64) -> SystemTime {
    SystemTime::now() + Duration::from_secs(secs)
}

/// A clock a test advances by hand, for `Bus::with_clock`: timestamps and
/// name holds move only when the test says so, never with the wall clock.
#[derive(Clone)]
pub struct ManualClock(Arc<AtomicU64>);

impl ManualClock {
    /// A clock that starts at the real time now.
    pub fn new() -> Self {
        let ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("after the epoch")
            .as_millis() as u64;
        Self(Arc::new(AtomicU64::new(ms)))
    }

    /// Move the clock forward by `by`.
    pub fn advance(&self, by: Duration) {
        self.0.fetch_add(by.as_millis() as u64, Ordering::SeqCst);
    }

    /// Milliseconds since the Unix epoch, as `Bus::with_clock` wants them.
    pub fn now_ms(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}

/// A bus with `retention` whose clock is the returned [`ManualClock`].
pub fn bus_with_clock(
    tag: &str,
    retention: Retention,
) -> (tempfile::TempDir, Arc<Bus>, ManualClock) {
    let dir = tempfile::tempdir().expect("tempdir");
    let clock = ManualClock::new();
    let c = clock.clone();
    let bus = Bus::with_retention(&dir.path().join(tag), retention)
        .expect("bus opens")
        .with_clock(move || c.now_ms());
    (dir, Arc::new(bus), clock)
}
