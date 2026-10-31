//! Helpers shared by the integration tests. Each test crate uses a different
//! subset, so the ones a crate leaves unused are allowed rather than warned
//! about.
#![allow(dead_code)]

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mentischat::bus::{Bus, Registration, Retention, Stats};
use mentischat::names::{AgentName, RoomName};

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

/// The default retention with agents dropped after `secs` idle.
pub fn idle_after(secs: u64) -> Retention {
    Retention {
        agent_ttl: Duration::from_secs(secs),
        ..Retention::default()
    }
}

/// Register options that take a registered or held name back.
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
