//! The log pane's store: what it keeps, in what order, and what it refuses to
//! pass on to the operator's terminal.

use std::io::Write;

use mentischat::tui::LogBuffer;
use tracing_subscriber::fmt::MakeWriter;

/// A line is retained only once its newline arrives; a write that ends
/// mid-line holds the rest back, and one write may carry several lines.
#[test]
fn test_lines_complete_on_newline_across_writes() {
    let mut logs = LogBuffer::default();
    assert!(logs.lines().is_empty());

    write!(logs, " INFO first").unwrap();
    assert!(logs.lines().is_empty(), "no newline yet");
    write!(logs, " half\n INFO second\n INFO thi").unwrap();
    let lines = logs.lines();
    assert_eq!(lines.len(), 2);
    assert!(lines[0].ends_with("INFO first half"), "{lines:?}");
    assert!(lines[1].ends_with("INFO second"), "{lines:?}");
    logs.flush().unwrap();
    write!(logs, "rd\n").unwrap();
    assert!(logs.lines()[2].ends_with("INFO third"));
}

/// Only the newest 1000 lines are kept, oldest dropped first.
#[test]
fn test_only_the_newest_thousand_lines_are_kept() {
    let mut logs = LogBuffer::new();
    for i in 0..1005 {
        writeln!(logs, "line {i}").unwrap();
    }
    let lines = logs.lines();
    assert_eq!(lines.len(), 1000);
    assert!(lines[0].ends_with("line 5"), "{}", lines[0]);
    assert!(lines[999].ends_with("line 1004"), "{}", lines[999]);
}

/// A name chosen by a peer cannot paint escape sequences: control characters
/// never reach the stored line.
#[test]
fn test_control_characters_are_replaced() {
    let mut logs = LogBuffer::new();
    writeln!(logs, "agent \x1b[2J\x07evil\r name").unwrap();
    let line = &logs.lines()[0];
    assert!(!line.chars().any(char::is_control), "{line:?}");
    assert!(line.contains("evil"));
}

/// Bytes that are not UTF-8 are replaced, not dropped or panicked over, and
/// every writer made from a buffer feeds the same lines.
#[test]
fn test_writers_share_one_store_and_tolerate_bad_utf8() {
    let logs = LogBuffer::new();
    let mut a = logs.make_writer();
    let mut b = logs.clone();
    a.write_all(b"from a \xff\xfe\n").unwrap();
    b.write_all(b"from b\n").unwrap();
    let lines = logs.lines();
    assert_eq!(lines.len(), 2);
    assert!(lines[0].contains("from a"));
    assert!(lines[1].ends_with("from b"));
}
