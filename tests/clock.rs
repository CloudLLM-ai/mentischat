//! `Bus::with_clock`: the engine reads the time from the supplied clock, so
//! expiry is a matter of moving a counter, not of sleeping.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use mentischat::bus::{Bus, Retention};
use mentischat::names::AgentName;

fn name(s: &str) -> AgentName {
    AgentName::parse(s).expect("valid test name")
}

/// A bus whose clock is the returned counter (milliseconds), with 1 s read
/// and agent TTLs.
fn clocked(dir: &std::path::Path) -> (Bus, Arc<AtomicU64>) {
    let now = Arc::new(AtomicU64::new(1_000_000));
    let tick = now.clone();
    let retention = Retention {
        read_ttl: Duration::from_secs(1),
        agent_ttl: Duration::from_secs(1),
        name_hold: Duration::from_secs(60),
    };
    let bus = Bus::with_retention(dir, retention)
        .expect("fresh bus")
        .with_clock(move || tick.load(Ordering::Relaxed));
    (bus, now)
}

/// Read mail expires when the clock passes its TTL, with no sleeping, and
/// not before.
#[tokio::test]
async fn test_sweep_expires_read_mail_by_the_injected_clock() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (bus, now) = clocked(&dir.path().join("bus"));
    let (a, b) = (name("alpha"), name("beta"));
    bus.register(&a).await.unwrap();
    bus.register(&b).await.unwrap();
    bus.send(&a, &b, "hi".into()).await.unwrap();
    bus.check_inbox(&b, 0).await.unwrap();

    now.fetch_add(500, Ordering::Relaxed);
    assert_eq!(bus.sweep().await.unwrap().expired_read, 0);
    now.fetch_add(600, Ordering::Relaxed);
    assert_eq!(bus.sweep().await.unwrap().expired_read, 1);
}

/// An agent idle past its TTL by the injected clock is dropped by `sweep`.
#[tokio::test]
async fn test_sweep_drops_idle_agents_by_the_injected_clock() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (bus, now) = clocked(&dir.path().join("bus"));
    bus.register(&name("alpha")).await.unwrap();
    now.fetch_add(2_000, Ordering::Relaxed);
    let out = bus.sweep().await.unwrap();
    assert_eq!(out.dropped_agents, vec!["alpha".to_string()]);
}
