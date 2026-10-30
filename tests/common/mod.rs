//! Helpers shared by the integration tests. Each test crate uses a different
//! subset, so the ones a crate leaves unused are allowed rather than warned
//! about.
#![allow(dead_code)]

use std::sync::Arc;
use std::time::{Duration, SystemTime};

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
