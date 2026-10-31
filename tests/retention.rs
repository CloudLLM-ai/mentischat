//! Lifetimes: unread mail that waits, read mail and idle agents that expire,
//! the sweep, wipes, and that the mailbox never touches the disk unless it is
//! persisted.

mod common;

use common::{bus_with, bus_with_clock, idle_after, later, name, reclaim, stats};
use mentischat::bus::{Bus, Retention};
use mentischat::errors::MailboxError;

/// Every body `agent`'s history shows, newest page first, walking all pages.
async fn history_bodies(bus: &Bus, agent: &str, with: Option<&str>) -> Vec<String> {
    let with = with.map(name);
    let mut bodies = Vec::new();
    let mut before = None;
    loop {
        let page = bus
            .history(&name(agent), with.as_ref(), before)
            .await
            .unwrap();
        bodies.extend(page.messages.into_iter().rev().map(|m| m.body));
        before = page.next;
        if before.is_none() {
            return bodies;
        }
    }
}

#[tokio::test]
async fn unread_mail_waits_as_long_as_its_recipient_stays() {
    let (_d, bus) = bus_with("no_expiry", idle_after(30 * 86_400));
    bus.register(&name("a")).await.unwrap();
    bus.register(&name("b")).await.unwrap();
    bus.send(&name("a"), &name("b"), "read me whenever".into())
        .await
        .unwrap();

    let out = bus.sweep_at(later(7 * 86_400)).await.unwrap();
    assert!(
        out.is_empty(),
        "a week on, unread mail is still there: {out:?}"
    );
    let page = bus.check_inbox(&name("b"), 0).await.unwrap();
    assert_eq!(page.messages[0].body, "read me whenever");
}

#[tokio::test]
async fn idle_agents_drop_off_the_roster_with_their_unread_mail() {
    let (_d, bus) = bus_with("idle", idle_after(3600));
    bus.register(&name("a")).await.unwrap();
    bus.register(&name("b")).await.unwrap();
    bus.send(&name("a"), &name("b"), "never read".into())
        .await
        .unwrap();

    let out = bus.sweep_at(later(2 * 3600)).await.unwrap();
    assert_eq!(out.dropped_agents, ["a", "b"]);
    assert_eq!(out.discarded, 1);
    assert_eq!(bus.stats().await.unwrap(), stats(0, 0, 0));
    assert!(matches!(
        bus.check_inbox(&name("b"), 0).await,
        Err(MailboxError::Unregistered { .. })
    ));
}

#[tokio::test]
async fn an_agent_with_a_parked_pull_is_never_dropped() {
    let (_d, bus) = bus_with("parked", idle_after(3600));
    bus.register(&name("a")).await.unwrap();
    bus.register(&name("b")).await.unwrap();
    let b = bus.clone();
    let parked = tokio::spawn(async move { b.check_inbox(&name("b"), 2_000).await });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let out = bus.sweep_at(later(2 * 3600)).await.unwrap();
    assert_eq!(out.dropped_agents, ["a"], "b is waiting right now");
    bus.send(&name("b"), &name("b"), "still here".into())
        .await
        .unwrap();
    assert_eq!(parked.await.unwrap().unwrap().messages.len(), 1);
}

#[tokio::test]
async fn clear_wipes_everything_and_close_keeps_it() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("lifecycle");

    let bus = Bus::new(&data).unwrap();
    bus.register(&name("a")).await.unwrap();
    bus.register(&name("b")).await.unwrap();
    bus.send(&name("a"), &name("b"), "kept".into())
        .await
        .unwrap();
    bus.close().await; // MENTISCHAT_PERSIST=1: nothing is wiped
    assert!(matches!(bus.stats().await, Err(MailboxError::ShuttingDown)));
    drop(bus);

    let bus = Bus::new(&data).unwrap();
    assert_eq!(
        bus.stats().await.unwrap(),
        stats(2, 1, 0),
        "close kept the mailbox"
    );
    assert_eq!(bus.clear().await.unwrap(), (1, 2));
    assert_eq!(bus.stats().await.unwrap(), stats(0, 0, 0));
}

#[tokio::test]
async fn expired_and_shutdown_mail_leaves_nothing_behind() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("scrub");
    let retention = mentischat::bus::Retention {
        read_ttl: std::time::Duration::from_secs(2 * 3600),
        agent_ttl: std::time::Duration::from_secs(3600),
        ..mentischat::bus::Retention::default()
    };
    let bus = Bus::with_retention(&data, retention).unwrap();
    bus.register(&name("a")).await.unwrap();
    bus.register(&name("b")).await.unwrap();

    bus.send(&name("a"), &name("b"), "READ-CANARY-4f1c".into())
        .await
        .unwrap();
    bus.check_inbox(&name("b"), 0).await.unwrap();
    assert_eq!(
        history_bodies(&bus, "b", None).await,
        ["READ-CANARY-4f1c"],
        "read mail is kept for history"
    );
    let out = bus.sweep_at(later(2 * 3600 + 60)).await.unwrap();
    assert_eq!(out.expired_read, 1);
    assert_eq!(bus.stats().await.unwrap(), stats(0, 0, 0));

    // The sweep also dropped both idle agents; register again.
    for n in ["a", "b"] {
        bus.register_with(&name(n), reclaim()).await.unwrap();
    }
    assert!(
        history_bodies(&bus, "a", None).await.is_empty(),
        "expired read mail lingered in the sender's history"
    );

    bus.send(&name("a"), &name("b"), "SHUTDOWN-CANARY-77aa".into())
        .await
        .unwrap();
    bus.check_inbox(&name("b"), 0).await.unwrap();
    bus.send(&name("a"), &name("b"), "SHUTDOWN-CANARY-unread".into())
        .await
        .unwrap();
    bus.shutdown().await.unwrap();
    assert!(
        !data.exists(),
        "an in-memory mailbox writes nothing to disk"
    );
    let bus = Bus::with_retention(&data, retention).unwrap();
    assert_eq!(bus.stats().await.unwrap(), stats(0, 0, 0));
}

#[tokio::test]
async fn read_mail_ages_from_when_it_was_read() {
    let (_d, bus) = bus_with("read_age", idle_after(30 * 86_400));
    bus.register(&name("a")).await.unwrap();
    bus.register(&name("b")).await.unwrap();
    bus.send(&name("a"), &name("b"), "kept a day".into())
        .await
        .unwrap();
    bus.check_inbox(&name("b"), 0).await.unwrap();

    let out = bus.sweep_at(later(23 * 3600)).await.unwrap();
    assert!(out.is_empty(), "inside the read TTL: {out:?}");
    let out = bus.sweep_at(later(25 * 3600)).await.unwrap();
    assert_eq!(out.expired_read, 1);
    assert_eq!(bus.stats().await.unwrap(), stats(2, 0, 0));
}

#[tokio::test]
async fn deregister_and_idle_drop_delete_read_mail_too() {
    let (_d, bus) = bus_with("dereg_read", Retention::default());
    for n in ["a", "b", "c"] {
        bus.register(&name(n)).await.unwrap();
    }
    bus.send(&name("a"), &name("b"), "DEREG-READ-CANARY".into())
        .await
        .unwrap();
    bus.check_inbox(&name("b"), 0).await.unwrap();
    bus.send(&name("a"), &name("b"), "unread".into())
        .await
        .unwrap();
    bus.send(&name("b"), &name("c"), "b wrote this".into())
        .await
        .unwrap();

    let out = bus.deregister(&name("b")).await.unwrap();
    assert_eq!(out.discarded, 2, "read and unread mail addressed to b");
    assert!(
        !history_bodies(&bus, "a", None)
            .await
            .contains(&"DEREG-READ-CANARY".to_string()),
        "the discarded read mail still shows in the sender's history"
    );
    assert_eq!(
        bus.check_inbox(&name("c"), 0).await.unwrap().messages[0].body,
        "b wrote this",
        "mail b sent stays with its recipient"
    );

    let (_d, bus) = bus_with("idle_read", idle_after(3600));
    bus.register(&name("a")).await.unwrap();
    bus.register(&name("b")).await.unwrap();
    bus.send(&name("a"), &name("b"), "read".into())
        .await
        .unwrap();
    bus.check_inbox(&name("b"), 0).await.unwrap();
    let out = bus.sweep_at(later(2 * 3600)).await.unwrap();
    assert_eq!(out.dropped_agents, ["a", "b"]);
    assert_eq!(out.discarded, 1);
    assert_eq!(bus.stats().await.unwrap(), stats(0, 0, 0));
}

#[tokio::test]
async fn a_full_mailbox_makes_room_by_deleting_the_oldest_read_mail() {
    let (_d, bus) = bus_with("prune", Retention::default());
    let filler = name("filler");
    bus.register(&filler).await.unwrap();
    for i in 0..8 {
        let victim = name(&format!("v{i}"));
        bus.register(&victim).await.unwrap();
        for j in 0..1024 {
            let body = if (i, j) == (0, 0) {
                "OLDEST-READ-CANARY".to_string()
            } else {
                "x".to_string()
            };
            bus.send(&filler, &victim, body).await.unwrap();
        }
    }
    // Read everything sent to v0..v6; v7's 1024 stay unread.
    for i in 0..7 {
        let victim = name(&format!("v{i}"));
        while bus.check_inbox(&victim, 0).await.unwrap().remaining > 0 {}
    }
    assert_eq!(bus.stats().await.unwrap(), stats(9, 1024, 7168));
    let canary = "OLDEST-READ-CANARY".to_string();
    assert!(history_bodies(&bus, "v0", Some("filler"))
        .await
        .contains(&canary));

    let extra = name("extra");
    bus.register(&extra).await.unwrap();
    bus.send(&filler, &extra, "fits".into())
        .await
        .expect("read mail never blocks a send");
    assert_eq!(bus.stats().await.unwrap(), stats(10, 1025, 7167));
    assert!(
        !history_bodies(&bus, "v0", Some("filler"))
            .await
            .contains(&canary),
        "the oldest read message made room"
    );
}

#[test]
fn durations_parse_with_units_and_reject_junk() {
    use mentischat::bus::parse_duration;
    use std::time::Duration;
    assert_eq!(parse_duration("90"), Ok(Duration::from_secs(90)));
    assert_eq!(parse_duration("45s"), Ok(Duration::from_secs(45)));
    assert_eq!(parse_duration("30m"), Ok(Duration::from_secs(1800)));
    assert_eq!(parse_duration(" 1h "), Ok(Duration::from_secs(3600)));
    assert_eq!(parse_duration("2d"), Ok(Duration::from_secs(172_800)));
    for bad in [
        "",
        "0",
        "0h",
        "h",
        "1.5h",
        "-1",
        "1w",
        "1 h",
        "99999999999999999999d",
    ] {
        assert!(parse_duration(bad).is_err(), "{bad:?} should not parse");
    }
}

#[test]
fn retention_defaults_and_sweep_interval() {
    use mentischat::bus::Retention;
    use std::time::Duration;
    let only = |var: &'static str, value: &'static str| {
        move |v: &str| (v == var).then(|| value.to_string())
    };
    let r = Retention::from_settings(only("MENTISCHAT_AGENT_TTL", "")).unwrap();
    assert_eq!(r, Retention::default());
    assert_eq!(r.read_ttl, Duration::from_secs(86_400));
    assert_eq!(r.agent_ttl, Duration::from_secs(86_400));
    assert_eq!(r.name_hold, Duration::from_secs(600));
    assert_eq!(r.sweep_interval(), Duration::from_secs(60));
    let short = Retention::from_settings(only("MENTISCHAT_AGENT_TTL", "20s")).unwrap();
    assert_eq!(short.sweep_interval(), Duration::from_secs(5));
    let short_read = Retention::from_settings(only("MENTISCHAT_READ_TTL", "8s")).unwrap();
    assert_eq!(short_read.read_ttl, Duration::from_secs(8));
    assert_eq!(short_read.sweep_interval(), Duration::from_secs(2));
    let hold = Retention::from_settings(only("MENTISCHAT_NAME_HOLD", "5m")).unwrap();
    assert_eq!(hold.name_hold, Duration::from_secs(300));
    assert_eq!(
        hold.sweep_interval(),
        Duration::from_secs(60),
        "holds do not set the sweep pace"
    );
    for var in [
        "MENTISCHAT_READ_TTL",
        "MENTISCHAT_AGENT_TTL",
        "MENTISCHAT_NAME_HOLD",
    ] {
        let err = Retention::from_settings(only(var, "forever")).unwrap_err();
        assert!(err.contains(var), "{err}");
    }
}

#[tokio::test]
async fn sending_counts_as_activity_for_the_agent_ttl() {
    let (_d, bus, clock) = bus_with_clock("sender_alive", idle_after(3600));
    bus.register(&name("talker")).await.unwrap();
    bus.register(&name("listener")).await.unwrap();
    let before = bus.list_agents().await.unwrap().agents[1].last_seen.clone();
    clock.advance(std::time::Duration::from_millis(5));
    let sent_at = std::time::UNIX_EPOCH + std::time::Duration::from_millis(clock.now_ms());
    bus.send(&name("talker"), &name("listener"), "hi".into())
        .await
        .unwrap();
    let after = bus.list_agents().await.unwrap().agents[1].last_seen.clone();
    assert!(after > before, "send refreshed the sender's lastSeen");
    // `listener` never called anything after register; `talker` sent at or
    // after `sent_at`. Sweeping one agent TTL past `sent_at` puts the cutoff
    // between them however long the test took to get here.
    let at = sent_at + std::time::Duration::from_secs(3600);
    let out = bus.sweep_at(at).await.unwrap();
    assert_eq!(
        out.dropped_agents,
        ["listener"],
        "talker stays: it just sent"
    );
}

#[tokio::test]
async fn deregistered_inbox_mail_leaves_nothing_behind() {
    let (_d, bus) = bus_with("dereg_scrub", Retention::default());
    bus.register(&name("a")).await.unwrap();
    bus.register(&name("b")).await.unwrap();
    bus.send(&name("a"), &name("b"), "DEREG-CANARY-31b0".into())
        .await
        .unwrap();
    assert_eq!(history_bodies(&bus, "a", None).await, ["DEREG-CANARY-31b0"]);
    assert_eq!(bus.deregister(&name("b")).await.unwrap().discarded, 1);
    assert!(
        history_bodies(&bus, "a", None).await.is_empty(),
        "discarded inbox mail lingered"
    );
}
