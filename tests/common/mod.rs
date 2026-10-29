//! Helpers shared by the integration tests. Each test crate uses a different
//! subset, so the ones a crate leaves unused are allowed rather than warned
//! about.
#![allow(dead_code)]

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use mentischat::bus::{Bus, Registration, Retention, Stats};
use mentischat::names::AgentName;

pub fn name(s: &str) -> AgentName {
    AgentName::parse(s).expect("valid test name")
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

/// Whether `needle` appears anywhere under the data directory: database,
/// `-wal` or `-shm`.
pub fn on_disk(dir: &Path, needle: &str) -> bool {
    std::fs::read_dir(dir)
        .unwrap()
        .flat_map(|entry| std::fs::read(entry.unwrap().path()).unwrap_or_default())
        .collect::<Vec<u8>>()
        .windows(needle.len())
        .any(|w| w == needle.as_bytes())
}
