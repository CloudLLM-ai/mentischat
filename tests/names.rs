//! Chosen names: uniqueness without regard to case, `name_taken` with
//! suggestions, `reclaim`, and the hold on a name that leaves the roster.

mod common;

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use common::{bus_with, fresh_bus, name, reclaim};
use mentischat::bus::{rfc3339, Bus, Retention};
use mentischat::errors::{MailboxError, TakenReason};
use mentischat::names::{AgentName, SUGGESTED_NAMES, SUGGESTION_COUNT};

fn holding(tag: &str, hold: Duration) -> (tempfile::TempDir, Arc<Bus>) {
    bus_with(
        tag,
        Retention {
            name_hold: hold,
            ..Retention::default()
        },
    )
}

fn folded(names: &[String]) -> HashSet<String> {
    names.iter().map(|n| n.to_ascii_lowercase()).collect()
}

/// The suggestions of a `name_taken`, after checking its name and reason.
fn taken(err: MailboxError, agent: &str, held: bool) -> Vec<String> {
    match err {
        MailboxError::NameTaken {
            agent: got,
            reason,
            suggestions,
        } => {
            assert_eq!(got, agent);
            assert_eq!(
                matches!(reason, TakenReason::Held { .. }),
                held,
                "{reason:?}"
            );
            suggestions
        }
        other => panic!("expected name_taken, got {other:?}"),
    }
}

#[test]
fn suggested_names_are_valid_and_distinct_ignoring_case() {
    for n in SUGGESTED_NAMES {
        assert!(AgentName::parse(n).is_ok(), "{n}");
    }
    let distinct: HashSet<String> = SUGGESTED_NAMES
        .iter()
        .map(|n| n.to_ascii_lowercase())
        .collect();
    assert_eq!(distinct.len(), SUGGESTED_NAMES.len());
}

#[tokio::test]
async fn names_are_unique_without_regard_to_case() {
    let (_d, bus) = fresh_bus("case");
    let out = bus.register(&name("Vega")).await.unwrap();
    assert_eq!((out.agent.as_str(), out.created), ("Vega", true));

    let err = bus.register(&name("vEGA")).await.unwrap_err();
    let suggestions = taken(err, "Vega", false);
    assert_eq!(suggestions.len(), SUGGESTION_COUNT);
    assert!(!folded(&suggestions).contains("vega"), "{suggestions:?}");

    bus.register(&name("Orion")).await.unwrap();
    let receipt = bus
        .send(&name("orion"), &name("VEGA"), "hi".into())
        .await
        .unwrap();
    assert_eq!(
        (receipt.from.as_str(), receipt.to.as_str()),
        ("Orion", "Vega"),
        "responses use the roster's spelling"
    );
    let page = bus.check_inbox(&name("vega"), 0).await.unwrap();
    assert_eq!(page.agent, "Vega");
    assert_eq!(
        (page.messages[0].from.as_str(), page.messages[0].to.as_str()),
        ("Orion", "Vega")
    );
    let roster = bus.list_agents().await.unwrap().agents;
    let names: Vec<&str> = roster.iter().map(|e| e.name.as_str()).collect();
    assert_eq!(names, ["Orion", "Vega"]);

    let out = bus.deregister(&name("VEGA")).await.unwrap();
    assert_eq!(out.agent, "Vega");
}

#[tokio::test]
async fn a_pull_parked_under_one_spelling_is_woken_by_mail_to_another() {
    let (_d, bus) = fresh_bus("case_wake");
    bus.register(&name("Vega")).await.unwrap();
    bus.register(&name("Orion")).await.unwrap();
    let b = bus.clone();
    let parked = tokio::spawn(async move { b.check_inbox(&name("vega"), 10_000).await });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let started = std::time::Instant::now();
    bus.send(&name("Orion"), &name("VEGA"), "wake".into())
        .await
        .unwrap();
    let page = parked.await.unwrap().unwrap();
    assert_eq!(page.messages.len(), 1);
    assert!(started.elapsed() < Duration::from_secs(2));
}

#[tokio::test]
async fn concurrent_registrations_of_one_name_have_one_winner() {
    let (_d, bus) = fresh_bus("race");
    let tasks: Vec<_> = (0..16)
        .map(|i| {
            let b = bus.clone();
            let spelling = if i % 2 == 0 { "Lyra" } else { "lyra" };
            tokio::spawn(async move { b.register(&name(spelling)).await })
        })
        .collect();
    let mut created = 0;
    for t in tasks {
        match t.await.unwrap() {
            Ok(out) => {
                assert!(out.created);
                created += 1;
            }
            Err(MailboxError::NameTaken { .. }) => {}
            Err(other) => panic!("unexpected {other:?}"),
        }
    }
    assert_eq!(created, 1);
    assert_eq!(bus.list_agents().await.unwrap().agents.len(), 1);
}

#[tokio::test]
async fn list_agents_offers_distinct_free_suggested_names() {
    let (_d, bus) = fresh_bus("suggest");
    let mut first = HashSet::new();
    for _ in 0..20 {
        let suggestions = bus.list_agents().await.unwrap().suggestions;
        assert_eq!(suggestions.len(), SUGGESTION_COUNT);
        assert_eq!(folded(&suggestions).len(), SUGGESTION_COUNT, "distinct");
        assert!(suggestions
            .iter()
            .all(|s| SUGGESTED_NAMES.contains(&s.as_str())));
        first.insert(suggestions[0].clone());
    }
    assert!(
        first.len() > 1,
        "agents connecting together see different names first"
    );
}

#[tokio::test]
async fn suggestions_skip_registered_and_held_names() {
    let (_d, bus) = fresh_bus("suggest_free");
    for n in &SUGGESTED_NAMES[3..] {
        // A lowercased registration still takes the suggested spelling.
        bus.register(&name(&n.to_ascii_lowercase())).await.unwrap();
    }
    bus.deregister(&name(SUGGESTED_NAMES[10])).await.unwrap(); // now held

    let free: HashSet<String> = SUGGESTED_NAMES[..3]
        .iter()
        .map(|n| n.to_ascii_lowercase())
        .collect();
    let listed = bus.list_agents().await.unwrap().suggestions;
    assert_eq!(folded(&listed), free, "{listed:?}");
    let err = bus.register(&name(SUGGESTED_NAMES[5])).await.unwrap_err();
    assert_eq!(
        folded(&taken(err, &SUGGESTED_NAMES[5].to_ascii_lowercase(), false)),
        free
    );

    for n in &SUGGESTED_NAMES[..3] {
        bus.register(&name(n)).await.unwrap();
    }
    assert!(
        bus.list_agents().await.unwrap().suggestions.is_empty(),
        "nothing left to suggest"
    );
    // Names outside the list are still fine.
    assert!(bus.register(&name("reviewer-2")).await.unwrap().created);
}

#[tokio::test]
async fn a_released_name_is_held_and_only_reclaim_takes_it() {
    let (_d, bus) = fresh_bus("held");
    bus.register(&name("Vega")).await.unwrap();
    let released = SystemTime::now();
    bus.deregister(&name("Vega")).await.unwrap();

    for spelling in ["Vega", "VEGA"] {
        match bus.register(&name(spelling)).await.unwrap_err() {
            MailboxError::NameTaken {
                agent,
                reason: TakenReason::Held { until },
                suggestions,
            } => {
                assert_eq!(agent, spelling);
                let hold_end = rfc3339(released + Duration::from_secs(600));
                assert!(until >= hold_end, "{until} < {hold_end}");
                assert!(!folded(&suggestions).contains("vega"));
            }
            other => panic!("expected a held name_taken, got {other:?}"),
        }
    }
    assert!(
        !folded(&bus.list_agents().await.unwrap().suggestions).contains("vega"),
        "a held name is not suggested"
    );
    assert!(matches!(
        bus.check_inbox(&name("Vega"), 0).await,
        Err(MailboxError::Unregistered { .. })
    ));

    let back = bus.register_with(&name("Vega"), reclaim()).await.unwrap();
    assert!(back.created, "a held name comes back as a new roster entry");
    assert!(
        bus.register(&name("Orion")).await.unwrap().created,
        "the hold covers only its own name"
    );
}

#[tokio::test]
async fn a_stale_client_cannot_read_the_next_holders_mail_during_the_hold() {
    let (_d, bus) = fresh_bus("stale");
    bus.register(&name("Vega")).await.unwrap();
    bus.register(&name("Orion")).await.unwrap();
    bus.deregister(&name("Vega")).await.unwrap();

    // A different agent wants the name; it is refused, so mail sent to
    // Vega cannot reach a client that still believes it is Vega.
    assert!(matches!(
        bus.register(&name("Vega")).await,
        Err(MailboxError::NameTaken { .. })
    ));
    assert!(matches!(
        bus.send(&name("Orion"), &name("Vega"), "for whom?".into())
            .await,
        Err(MailboxError::Unregistered { .. })
    ));
}

#[tokio::test]
async fn a_hold_ends_after_the_name_hold() {
    let (_d, bus) = holding("hold_ends", Duration::from_millis(150));
    bus.register(&name("Vega")).await.unwrap();
    bus.deregister(&name("Vega")).await.unwrap();
    assert!(matches!(
        bus.register(&name("Vega")).await,
        Err(MailboxError::NameTaken { .. })
    ));
    tokio::time::sleep(Duration::from_millis(250)).await;
    let out = bus.register(&name("vega")).await.unwrap();
    assert!(out.created);
    assert_eq!(out.agent, "vega", "a new holder picks its own spelling");
}

#[tokio::test]
async fn an_idle_drop_holds_the_name_from_the_sweep() {
    let (_d, bus) = bus_with(
        "idle_hold",
        Retention {
            agent_ttl: Duration::from_secs(3600),
            ..Retention::default()
        },
    );
    bus.register(&name("Vega")).await.unwrap();
    let swept_at = SystemTime::now() + Duration::from_secs(2 * 3600);
    let out = bus.sweep_at(swept_at).await.unwrap();
    assert_eq!(out.dropped_agents, ["Vega"]);

    match bus.register(&name("Vega")).await.unwrap_err() {
        MailboxError::NameTaken {
            reason: TakenReason::Held { until },
            ..
        } => assert_eq!(until, rfc3339(swept_at + Duration::from_secs(600))),
        other => panic!("expected a held name_taken, got {other:?}"),
    }
    assert!(
        bus.register_with(&name("Vega"), reclaim())
            .await
            .unwrap()
            .created
    );
}

#[tokio::test]
async fn a_later_sweep_forgets_ended_holds() {
    let (_d, bus) = fresh_bus("forget");
    bus.register(&name("Vega")).await.unwrap();
    bus.deregister(&name("Vega")).await.unwrap();
    bus.sweep_at(SystemTime::now() + Duration::from_secs(601))
        .await
        .unwrap();
    assert!(bus.register(&name("Vega")).await.unwrap().created);
}

#[tokio::test]
async fn reclaim_of_a_free_name_simply_creates_it() {
    let (_d, bus) = fresh_bus("reclaim_free");
    let out = bus.register_with(&name("Castor"), reclaim()).await.unwrap();
    assert!(out.created);
    assert_eq!(out.agent, "Castor");
}

#[tokio::test]
async fn reclaim_of_a_registered_name_keeps_its_spelling() {
    let (_d, bus) = fresh_bus("reclaim_spelling");
    bus.register(&name("Castor")).await.unwrap();
    let out = bus.register_with(&name("CASTOR"), reclaim()).await.unwrap();
    assert!(!out.created);
    assert_eq!(out.agent, "Castor");
}

#[tokio::test]
async fn clear_and_shutdown_forget_holds() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("holds_wiped");
    let bus = Bus::new(&data).unwrap();
    bus.register(&name("Vega")).await.unwrap();
    bus.deregister(&name("Vega")).await.unwrap();
    bus.clear().await.unwrap();
    assert!(bus.register(&name("Vega")).await.unwrap().created);

    bus.deregister(&name("Vega")).await.unwrap();
    bus.shutdown().await.unwrap();
    drop(bus);
    let bus = Bus::new(&data).unwrap();
    assert!(
        bus.register(&name("Vega")).await.unwrap().created,
        "a restart starts with no held names"
    );
}

#[tokio::test]
async fn a_full_roster_refuses_a_reclaimed_held_name() {
    let (_d, bus) = fresh_bus("full_reclaim");
    bus.register(&name("Vega")).await.unwrap();
    bus.deregister(&name("Vega")).await.unwrap();
    for i in 0..mentischat::bus::MAX_ROSTER {
        bus.register(&name(&format!("agent_{i}"))).await.unwrap();
    }
    assert!(matches!(
        bus.register_with(&name("Vega"), reclaim()).await,
        Err(MailboxError::RosterFull { .. })
    ));
    bus.deregister(&name("agent_0")).await.unwrap();
    assert!(
        bus.register_with(&name("Vega"), reclaim())
            .await
            .unwrap()
            .created,
        "the failed reclaim kept the hold for a retry"
    );
}

#[tokio::test]
async fn too_many_waiters_names_the_roster_spelling() {
    let (_d, bus) = fresh_bus("waiters_case");
    bus.register(&name("Vega")).await.unwrap();
    let parked: Vec<_> = (0..8)
        .map(|i| {
            let b = bus.clone();
            let spelling = if i % 2 == 0 { "vega" } else { "VEGA" };
            tokio::spawn(async move { b.check_inbox(&name(spelling), 1_000).await })
        })
        .collect();
    tokio::time::sleep(Duration::from_millis(100)).await;

    match bus.check_inbox(&name("vEgA"), 1_000).await {
        Err(MailboxError::TooManyWaiters { agent, waiting }) => {
            assert_eq!(agent, "Vega", "every spelling shares one waiter queue");
            assert_eq!(waiting, 8);
        }
        other => panic!("expected too_many_waiters, got {other:?}"),
    }
    for t in parked {
        assert!(t.await.unwrap().unwrap().messages.is_empty());
    }
}
