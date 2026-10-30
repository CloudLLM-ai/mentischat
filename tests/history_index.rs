//! The per-agent conversation index behind `history` without `with`: it must
//! keep finding a conversation that was swept away and started again.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use mentischat::bus::{Bus, Retention};
use mentischat::names::AgentName;

fn name(s: &str) -> AgentName {
    AgentName::parse(s).expect("valid test name")
}

/// After every message of a conversation expired and the conversation was
/// dropped, a new message between the same pair shows in both histories, and
/// an unrelated pair never does.
#[tokio::test]
async fn test_history_finds_a_conversation_recreated_after_expiry() {
    let dir = tempfile::tempdir().expect("tempdir");
    let now = Arc::new(AtomicU64::new(1_000_000));
    let tick = now.clone();
    let retention = Retention {
        read_ttl: Duration::from_secs(1),
        ..Retention::default()
    };
    let bus = Bus::with_retention(&dir.path().join("bus"), retention)
        .expect("bus")
        .with_clock(move || tick.load(Ordering::Relaxed));
    let (a, b, c, d) = (name("a"), name("b"), name("c"), name("d"));
    for n in [&a, &b, &c, &d] {
        bus.register(n).await.unwrap();
    }
    bus.send(&c, &d, "unrelated".into()).await.unwrap();
    bus.send(&a, &b, "first".into()).await.unwrap();
    bus.check_inbox(&b, 0).await.unwrap();

    now.fetch_add(2_000, Ordering::Relaxed);
    assert_eq!(bus.sweep().await.unwrap().expired_read, 1);
    assert!(bus
        .history(&a, None, None)
        .await
        .unwrap()
        .messages
        .is_empty());

    now.fetch_add(10, Ordering::Relaxed);
    bus.send(&a, &b, "second".into()).await.unwrap();
    bus.check_inbox(&b, 0).await.unwrap();
    for who in [&a, &b] {
        let page = bus.history(who, None, None).await.unwrap();
        let bodies: Vec<_> = page.messages.iter().map(|m| m.body.as_str()).collect();
        assert_eq!(bodies, ["second"]);
    }
}
