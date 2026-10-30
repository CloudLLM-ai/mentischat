//! The optional snapshot (`MENTISCHAT_PERSIST=1`): what `Bus::close` writes,
//! what the next `Bus::new` brings back, and what it refuses.

mod common;

use std::os::unix::fs::PermissionsExt;

use common::{name, reclaim, stats};
use mentischat::bus::{remove_snapshot, Bus, SnapshotError};
use mentischat::errors::MailboxError;

#[tokio::test]
async fn nothing_is_written_until_the_bus_is_closed() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("lazy");
    let bus = Bus::new(&data).unwrap();
    bus.register(&name("a")).await.unwrap();
    bus.register(&name("b")).await.unwrap();
    bus.send(&name("a"), &name("b"), "hello".into())
        .await
        .unwrap();
    assert!(!data.exists(), "a running mailbox never touches the disk");
    bus.shutdown().await.unwrap();
    assert!(!data.exists(), "nor does a wiping shutdown");
}

#[tokio::test]
async fn close_writes_a_private_snapshot_and_the_next_start_consumes_it() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("private");
    let bus = Bus::new(&data).unwrap();
    bus.register(&name("a")).await.unwrap();
    bus.close().await;

    let file = data.join("snapshot.json");
    let mode = |p: &std::path::Path| std::fs::metadata(p).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode(&data), 0o700);
    assert_eq!(mode(&file), 0o600);

    let bus = Bus::new(&data).unwrap();
    assert!(!file.exists(), "a snapshot is read once, then deleted");
    assert_eq!(bus.stats().await.unwrap(), stats(1, 0, 0));
}

#[tokio::test]
async fn a_restart_restores_roster_mail_read_state_history_and_holds() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("restore");
    let bus = Bus::new(&data).unwrap();
    for n in ["a", "b", "gone"] {
        bus.register(&name(n)).await.unwrap();
    }
    bus.send(&name("a"), &name("b"), "read before".into())
        .await
        .unwrap();
    bus.check_inbox(&name("b"), 0).await.unwrap();
    bus.send(&name("a"), &name("b"), "unread".into())
        .await
        .unwrap();
    bus.deregister(&name("gone")).await.unwrap();
    let sent = bus.overview().await.unwrap().messages_sent;
    bus.close().await;

    let bus = Bus::new(&data).unwrap();
    assert_eq!(bus.stats().await.unwrap(), stats(2, 1, 1));
    assert_eq!(bus.overview().await.unwrap().messages_sent, sent);
    let history = bus.history(&name("a"), None, None).await.unwrap();
    let seen: Vec<_> = history.messages.iter().map(|m| m.body.as_str()).collect();
    assert_eq!(seen, ["read before", "unread"]);
    assert!(history.messages[0].read_at.is_some());
    assert!(history.messages[1].read_at.is_none());
    let page = bus.check_inbox(&name("b"), 0).await.unwrap();
    assert_eq!(page.messages[0].body, "unread");
    assert!(matches!(
        bus.register(&name("gone")).await,
        Err(MailboxError::NameTaken { .. })
    ));
    bus.register_with(&name("gone"), reclaim()).await.unwrap();
}

#[tokio::test]
async fn a_snapshot_that_cannot_be_read_is_refused_and_kept() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("corrupt");
    std::fs::create_dir_all(&data).unwrap();
    let file = data.join("snapshot.json");

    std::fs::write(&file, "not json").unwrap();
    assert!(matches!(Bus::new(&data), Err(SnapshotError::Corrupt(_))));
    assert!(file.exists(), "left in place for the operator");

    std::fs::write(
        &file,
        r#"{"version":99,"next_id":1,"messages_sent":0,"agents":[],"holds":[],"messages":[]}"#,
    )
    .unwrap();
    assert!(matches!(Bus::new(&data), Err(SnapshotError::Version(99))));

    remove_snapshot(&data).unwrap();
    remove_snapshot(&data).expect("removing a missing snapshot is fine");
    assert!(Bus::new(&data).is_ok());
}

#[tokio::test]
async fn close_fails_parked_pulls_with_shutting_down() {
    let dir = tempfile::tempdir().unwrap();
    let bus = std::sync::Arc::new(Bus::new(&dir.path().join("closing")).unwrap());
    bus.register(&name("a")).await.unwrap();
    let puller = bus.clone();
    let parked = tokio::spawn(async move { puller.check_inbox(&name("a"), 10_000).await });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    bus.close().await;
    assert!(matches!(
        parked.await.unwrap(),
        Err(MailboxError::ShuttingDown)
    ));
}
