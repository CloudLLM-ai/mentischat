//! Terminal view tests: rendering into a test backend, key handling, log
//! capture, and the guard that stops the daemon when the view ends.

use std::io::Write;
use std::time::Duration;

use mentischat::bus::{AgentOverview, Overview};
use mentischat::tui::{run_guarded, Dashboard, Focus, Info, LogBuffer};
use ratatui::backend::TestBackend;
use ratatui::crossterm::event::{KeyCode, KeyModifiers};
use ratatui::Terminal;
use tokio::sync::Notify;

fn dashboard() -> Dashboard {
    Dashboard::new(Info {
        version: "9.9.9".into(),
        url: "http://127.0.0.1:9480".into(),
        data_path: "/tmp/mentischat".into(),
    })
}

fn agent(name: &str, secs: i64, queued: i64) -> AgentOverview {
    AgentOverview {
        name: name.into(),
        seen_secs_ago: secs,
        queued,
    }
}

fn overview(sent: u64, agents: Vec<AgentOverview>) -> Overview {
    Overview {
        agents,
        messages_sent: sent,
    }
}

fn rendered(dash: &mut Dashboard, w: u16, h: u16) -> String {
    let mut term = Terminal::new(TestBackend::new(w, h)).unwrap();
    term.draw(|f| dash.render(f)).unwrap();
    let buf = term.backend().buffer().clone();
    (0..h)
        .map(|y| (0..w).map(|x| buf[(x, y)].symbol()).collect::<String>())
        .collect::<Vec<_>>()
        .join("\n")
}

async fn notified(n: &Notify) -> bool {
    tokio::time::timeout(Duration::from_millis(50), n.notified())
        .await
        .is_ok()
}

#[test]
fn log_buffer_splits_lines_and_strips_escapes() {
    let mut logs = LogBuffer::new();
    logs.write_all(b" INFO first\n INFO sec").unwrap();
    logs.write_all(b"ond \x1b[31mred\x1b[0m\n").unwrap();
    let lines = logs.lines();
    assert_eq!(lines.len(), 2);
    assert!(lines[0].ends_with("INFO first"));
    assert!(lines[1].ends_with("INFO second ?[31mred?[0m"));
}

#[test]
fn log_buffer_keeps_only_the_newest_thousand_lines() {
    let mut logs = LogBuffer::new();
    for i in 0..1005 {
        writeln!(logs, " INFO {i}").unwrap();
    }
    let lines = logs.lines();
    assert_eq!(lines.len(), 1000);
    assert!(lines[0].ends_with("INFO 5"));
}

#[test]
fn renders_banner_tiles_logs_and_agents_in_order() {
    let mut logs = LogBuffer::new();
    writeln!(logs, " INFO POST /messages 200").unwrap();
    let mut dash = dashboard();
    dash.update(
        logs.lines(),
        overview(
            37,
            vec![
                agent("claude_code", 4, 2),
                agent("grok\x1b[2Jbuild", 125, 0),
            ],
        ),
    );
    let out = rendered(&mut dash, 100, 30);
    let at = |s: &str| {
        out.find(s)
            .unwrap_or_else(|| panic!("missing {s:?}\n{out}"))
    };
    assert!(at("█") < at("LISTEN"));
    assert!(at("LISTEN") < at("MESSAGES SENT") && at("MESSAGES SENT") < at("AGENTS"));
    assert!(at("AGENTS") < at("UPTIME") && at("UPTIME") < at("Logs"));
    assert!(at("Logs") < at("POST /messages 200"));
    assert!(at("POST /messages 200") < at("Registered agents (2)"));
    assert!(at("Registered agents (2)") < at("claude_code"));
    assert!(out.contains("4s ago") && out.contains("2m ago"), "{out}");
    assert!(out.contains("v9.9.9"));
    assert!(out.contains("grok?[2Jbuild"), "{out}");
    assert!(!out.contains('\x1b'));
}

#[test]
fn each_tile_shows_its_own_value() {
    let mut dash = dashboard();
    dash.update(
        vec![],
        overview(37, vec![agent("a", 0, 11), agent("b", 0, 13)]),
    );
    let out = rendered(&mut dash, 100, 30);
    let labels = out
        .lines()
        .find(|l| l.contains("LISTEN"))
        .expect("tile labels");
    let values = out
        .lines()
        .nth(out.lines().position(|l| l.contains("LISTEN")).unwrap() + 1)
        .expect("tile values");
    // Character columns, not byte offsets: box-drawing characters are 3 bytes.
    let col = |line: &str, needle: &str| {
        let byte = line
            .find(needle)
            .unwrap_or_else(|| panic!("missing {needle:?} in {line:?}"));
        line[..byte].chars().count()
    };
    let between = |line: &str, from: usize, to: usize| -> String {
        line.chars().skip(from).take(to - from).collect()
    };
    let (sent_label, agents_label, uptime_label) = (
        col(labels, "MESSAGES SENT"),
        col(labels, "AGENTS"),
        col(labels, "UPTIME"),
    );
    assert!(col(values, "http://127.0.0.1:9480") < sent_label, "{out}");
    assert!(
        between(values, sent_label, agents_label).contains("37"),
        "{out}"
    );
    assert!(
        between(values, agents_label, uptime_label).contains('2'),
        "{out}"
    );
    assert!(
        values
            .chars()
            .skip(uptime_label)
            .collect::<String>()
            .contains("00:00:0"),
        "{out}"
    );
}

#[test]
fn the_view_never_mentions_waking() {
    let mut dash = dashboard();
    dash.update(vec![], overview(1, vec![agent("claude_code", 3, 1)]));
    let out = rendered(&mut dash, 100, 30).to_lowercase();
    for word in ["wake", "woken", "wrapper", "nudge"] {
        assert!(!out.contains(word), "{word:?} on screen\n{out}");
    }
    let header = out
        .lines()
        .find(|l| l.contains("last seen"))
        .expect("table header");
    assert!(
        header.contains("name") && header.contains("queued"),
        "{header}"
    );
}

#[test]
fn short_terminal_uses_compact_title() {
    let mut dash = dashboard();
    let out = rendered(&mut dash, 100, 24);
    assert!(out.contains("M E N T I S   C H A T"), "{out}");
    assert!(!out.contains('█'));
    assert!(rendered(&mut dash, 100, 36).contains('█'));
}

#[test]
fn empty_roster_and_tiny_terminals_do_not_panic() {
    let mut dash = dashboard();
    assert!(rendered(&mut dash, 100, 30).contains("no agents registered yet"));
    for (w, h) in [(1, 1), (20, 5), (40, 12)] {
        rendered(&mut dash, w, h);
    }
}

#[test]
fn keys_scroll_focus_and_quit() {
    let mut logs = LogBuffer::new();
    for i in 0..100 {
        writeln!(logs, " INFO line {i}").unwrap();
    }
    let mut dash = dashboard();
    dash.update(logs.lines(), Overview::default());
    let none = KeyModifiers::NONE;

    assert!(!dash.on_key(KeyCode::Up, none));
    assert!(rendered(&mut dash, 100, 30).contains("scrolled back 1"));
    dash.on_key(KeyCode::End, none);
    assert!(!rendered(&mut dash, 100, 30).contains("scrolled back"));

    assert_eq!(dash.focus(), Focus::Logs);
    dash.on_key(KeyCode::Tab, none);
    assert_eq!(dash.focus(), Focus::Agents);

    assert!(dash.on_key(KeyCode::Char('q'), none));
    assert!(dash.on_key(KeyCode::Esc, none));
    assert!(dash.on_key(KeyCode::Char('c'), KeyModifiers::CONTROL));
    assert!(!dash.on_key(KeyCode::Char('c'), none));
}

#[test]
fn agent_selection_stays_in_range_when_the_roster_shrinks() {
    let mut dash = dashboard();
    let three = vec![agent("a", 0, 0), agent("b", 0, 0), agent("c", 0, 0)];
    dash.update(vec![], overview(0, three));
    dash.on_key(KeyCode::Tab, KeyModifiers::NONE);
    dash.on_key(KeyCode::PageDown, KeyModifiers::NONE);
    dash.update(vec![], overview(0, vec![agent("a", 0, 0)]));
    assert!(rendered(&mut dash, 100, 30).contains("Registered agents (1)"));
}

#[tokio::test]
async fn a_panicking_view_stops_the_server() {
    let stop = Notify::new();
    let quit = run_guarded(|| panic!("boom"), &stop);
    assert_eq!(quit.unwrap_err().to_string(), "terminal view panicked");
    assert!(notified(&stop).await);
}

#[tokio::test]
async fn view_quit_or_error_stops_the_server_but_server_stop_does_not() {
    let stop = Notify::new();
    assert!(run_guarded(|| Ok(true), &stop).unwrap());
    assert!(notified(&stop).await);
    assert!(run_guarded(|| Err(std::io::Error::other("tty gone")), &stop).is_err());
    assert!(notified(&stop).await);
    assert!(!run_guarded(|| Ok(false), &stop).unwrap());
    assert!(!notified(&stop).await);
}
