//! Traffic lines: a log line for every message sent and read, previews only
//! when asked for, and no change to the mailbox from writing them.

mod common;

use common::{capture_logs, fresh_bus, name, register, room, traffic_bus, traffic_lines};
use mentischat::bus::{TrafficLog, PREVIEW_CHARS};

/// A direct send logs sender, recipient, id and size but no part of the body,
/// and leaves the message unread until the recipient takes it.
#[tokio::test]
async fn a_direct_send_is_logged_without_its_body_and_stays_unread() {
    let (logs, _guard) = capture_logs();
    let (_d, bus) = traffic_bus(TrafficLog::Metadata);
    register(&bus, &["Alpha", "Beta"]).await;

    let receipt = bus
        .send(&name("Alpha"), &name("Beta"), "secret plan".into())
        .await
        .unwrap();
    let id = receipt.id;
    assert_eq!(
        traffic_lines(&logs),
        [format!("sent Alpha → Beta (id {id}, 11 B)")]
    );
    assert!(logs.lines().iter().all(|l| !l.contains("secret")));

    let sent = bus.history(&name("Alpha"), None, None).await.unwrap();
    assert!(sent.messages[0].read_at.is_none(), "logging read nothing");

    let page = bus.check_inbox(&name("Beta"), 0).await.unwrap();
    assert_eq!(page.messages.len(), 1, "the message is still there to take");
    assert_eq!(
        traffic_lines(&logs),
        [
            format!("sent Alpha → Beta (id {id}, 11 B)"),
            format!("read Beta ← Alpha (id {id})"),
        ]
    );
}

/// With previews on, a sent line carries the first line of the body, control
/// characters as spaces, cut to `PREVIEW_CHARS` characters with an ellipsis.
#[tokio::test]
async fn previews_show_the_start_of_the_first_line() {
    let (logs, _guard) = capture_logs();
    let (_d, bus) = traffic_bus(TrafficLog::WithPreviews);
    register(&bus, &["Alpha", "Beta"]).await;

    let long = "é".repeat(PREVIEW_CHARS + 20);
    for body in ["Reply when you can", "first\nsecond", "tab\there", &long] {
        bus.send(&name("Alpha"), &name("Beta"), body.into())
            .await
            .unwrap();
    }
    let previews: Vec<String> = traffic_lines(&logs)
        .iter()
        .map(|l| l.split_once(") ").expect("a preview").1.to_string())
        .collect();
    assert_eq!(
        previews,
        [
            "\"Reply when you can\"".to_string(),
            "\"first…\"".to_string(),
            "\"tab here\"".to_string(),
            format!("\"{}…\"", "é".repeat(PREVIEW_CHARS)),
        ]
    );
}

/// Room messages name the room when sent and the room when read, and the
/// size switches to KB from 1 KiB.
#[tokio::test]
async fn room_messages_are_logged_with_the_room() {
    let (logs, _guard) = capture_logs();
    let (_d, bus) = traffic_bus(TrafficLog::Metadata);
    register(&bus, &["Alpha", "Beta"]).await;
    bus.create_room(&name("Alpha"), &room("release"), false, None)
        .await
        .unwrap();
    bus.join_room(&name("Beta"), &room("release"))
        .await
        .unwrap();

    let body = "x".repeat(1536);
    let id = bus
        .send_room(&name("Alpha"), &room("release"), body)
        .await
        .unwrap()
        .id;
    bus.check_inbox(&name("Beta"), 0).await.unwrap();
    assert_eq!(
        traffic_lines(&logs),
        [
            format!("sent Alpha → #release (id {id}, 1.5 KB)"),
            format!("read Beta ← Alpha in #release (id {id})"),
        ]
    );
}

/// A stream push leases without a read line; the acknowledgement logs it.
#[tokio::test]
async fn a_streamed_message_is_logged_as_read_when_acknowledged() {
    let (logs, _guard) = capture_logs();
    let (_d, bus) = traffic_bus(TrafficLog::Metadata);
    register(&bus, &["Alpha", "Beta"]).await;
    let stream = bus.open_stream(&name("Beta")).unwrap();
    let id = bus
        .send(&name("Alpha"), &name("Beta"), "hi".into())
        .await
        .unwrap()
        .id;

    bus.stream_push(&stream).unwrap().expect("mail to push");
    assert_eq!(traffic_lines(&logs).len(), 1, "a lease is not a read");

    assert_eq!(bus.stream_ack(&stream, id.parse().unwrap()).unwrap(), 1);
    assert_eq!(
        traffic_lines(&logs)[1],
        format!("read Beta ← Alpha (id {id})")
    );
}

/// A failed send logs nothing.
#[tokio::test]
async fn a_refused_send_is_not_logged() {
    let (logs, _guard) = capture_logs();
    let (_d, bus) = traffic_bus(TrafficLog::WithPreviews);
    register(&bus, &["Alpha"]).await;
    assert!(bus
        .send(&name("Alpha"), &name("Nobody"), "hello".into())
        .await
        .is_err());
    assert!(traffic_lines(&logs).is_empty());
}

/// A bus built without a traffic setting writes no traffic lines.
#[tokio::test]
async fn the_default_writes_no_traffic_lines() {
    let (logs, _guard) = capture_logs();
    let (_d, bus) = fresh_bus("quiet");
    register(&bus, &["Alpha", "Beta"]).await;
    bus.send(&name("Alpha"), &name("Beta"), "hi".into())
        .await
        .unwrap();
    bus.check_inbox(&name("Beta"), 0).await.unwrap();
    assert!(traffic_lines(&logs).is_empty());
}
