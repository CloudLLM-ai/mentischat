//! Terminal view tests: rendering into a test backend, key handling, log
//! capture, and the guard that stops the daemon when the view ends.

use std::io::Write;
use std::time::Duration;

use mentischat::bus::{AgentOverview, Overview};
use mentischat::tui::{run_guarded, Dashboard, Focus, Info, LogBuffer};
use ratatui::backend::TestBackend;
use ratatui::buffer::Buffer;
use ratatui::crossterm::event::{KeyCode, KeyModifiers};
use ratatui::style::{Color, Modifier};
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
        provider: None,
        description: None,
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

fn painted(dash: &mut Dashboard, w: u16, h: u16) -> Buffer {
    let mut term = Terminal::new(TestBackend::new(w, h)).unwrap();
    term.draw(|f| dash.render(f)).unwrap();
    term.backend().buffer().clone()
}

fn row_text(buf: &Buffer, y: u16) -> String {
    (0..buf.area.width)
        .map(|x| buf[(x, y)].symbol())
        .collect::<String>()
}

fn rendered(dash: &mut Dashboard, w: u16, h: u16) -> String {
    let buf = painted(dash, w, h);
    (0..h)
        .map(|y| row_text(&buf, y))
        .collect::<Vec<_>>()
        .join("\n")
}

/// Cell position of the first occurrence of `needle` (all cells are one column
/// wide, so character index equals column).
fn find(buf: &Buffer, needle: &str) -> (u16, u16) {
    for y in 0..buf.area.height {
        let row = row_text(buf, y);
        if let Some(byte) = row.find(needle) {
            return (row[..byte].chars().count() as u16, y);
        }
    }
    panic!("missing {needle:?}");
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
    let out = rendered(&mut dash, 100, 32);
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
        header.contains("name") && header.contains("queued") && header.contains("description"),
        "{header}"
    );
}

#[test]
fn agent_table_shows_descriptions_without_control_characters() {
    let mut dash = dashboard();
    let mut reviewer = agent("claude_code", 3, 0);
    reviewer.description = Some("reviews PRs\u{1b}[31m".into());
    dash.update(vec![], overview(0, vec![reviewer, agent("grok", 5, 0)]));
    let out = rendered(&mut dash, 100, 30);
    let row = out
        .lines()
        .find(|l| l.contains("claude_code"))
        .expect("agent row");
    assert!(row.contains("reviews PRs?[31m"), "{row}");
    assert!(!out.contains('\u{1b}'), "escape reached the screen");
    let bare = out.lines().find(|l| l.contains("grok")).expect("agent row");
    assert!(
        bare.contains('—'),
        "a missing description shows a dash: {bare}"
    );
}

#[test]
fn agent_table_shows_the_provider_or_a_dash() {
    let mut dash = dashboard();
    let mut vega = agent("Vega", 3, 0);
    vega.provider = Some("claude_code".into());
    dash.update(vec![], overview(0, vec![vega, agent("Orion", 5, 0)]));
    let out = rendered(&mut dash, 110, 30);
    let header = out
        .lines()
        .find(|l| l.contains("LAST SEEN"))
        .expect("table header");
    assert!(header.contains("PROVIDER"), "{header}");
    let row = out.lines().find(|l| l.contains("Vega")).expect("agent row");
    assert!(row.contains("claude_code"), "{row}");
    let row = out
        .lines()
        .find(|l| l.contains("Orion"))
        .expect("agent row");
    assert!(row.contains('—'), "{row}");
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

fn sample() -> Dashboard {
    let mut logs = LogBuffer::new();
    writeln!(logs, " INFO served").unwrap();
    writeln!(logs, " WARN careful").unwrap();
    writeln!(logs, " ERROR broke").unwrap();
    let mut dash = dashboard();
    dash.update(
        logs.lines(),
        overview(
            3,
            vec![agent("claude_code", 4, 2), agent("grok_build", 9, 0)],
        ),
    );
    dash
}

#[test]
fn banner_paints_mentis_green_and_chat_cyan_with_a_gap_on_every_row() {
    let mut dash = sample();
    let buf = painted(&mut dash, 110, 36);
    let (_, top) = find(&buf, "███╗   ███╗");
    for y in top..top + 6 {
        let colored = |color: Color| -> Vec<u16> {
            (0..buf.area.width)
                .filter(|&x| buf[(x, y)].fg == color && buf[(x, y)].symbol() != " ")
                .collect()
        };
        let (green, cyan) = (colored(Color::Green), colored(Color::Cyan));
        assert!(
            !green.is_empty() && !cyan.is_empty(),
            "row {y}: both words painted"
        );
        let (last_green, first_cyan) = (*green.last().unwrap(), cyan[0]);
        assert!(
            first_cyan - last_green >= 4,
            "row {y}: MENTIS and CHAT need a gap, got {} blank cells",
            first_cyan - last_green - 1
        );
    }
}

#[test]
fn compact_title_is_green_then_cyan() {
    let mut dash = sample();
    let buf = painted(&mut dash, 100, 24);
    let (mx, my) = find(&buf, "M E N T I S");
    let (cx, cy) = find(&buf, "C H A T");
    assert_eq!(my, cy);
    assert_eq!(buf[(mx, my)].fg, Color::Green);
    assert_eq!(buf[(cx, cy)].fg, Color::Cyan);
    assert!(buf[(mx, my)].modifier.contains(Modifier::BOLD));
}

#[test]
fn only_terminal_palette_colors_are_used() {
    let mut dash = sample();
    for (w, h) in [(110, 36), (100, 24)] {
        let buf = painted(&mut dash, w, h);
        for cell in &buf.content {
            for color in [cell.fg, cell.bg] {
                assert!(
                    !matches!(color, Color::Indexed(_) | Color::Rgb(..)),
                    "fixed-palette color {color:?} breaks light themes"
                );
            }
        }
    }
}

#[test]
fn panels_are_dark_gray_and_the_focused_one_is_bold_yellow() {
    let mut dash = sample();
    let border = |dash: &mut Dashboard, title: &str| {
        let buf = painted(dash, 110, 36);
        let (_, y) = find(&buf, title);
        buf[(0, y)].clone()
    };
    for title in [
        "╭ LISTEN",
        "╭ MESSAGES SENT",
        "╭ Logs",
        "╭ Registered agents",
    ] {
        let (x, y) = find(&painted(&mut dash, 110, 36), title);
        let cell = painted(&mut dash, 110, 36)[(x, y)].clone();
        let expect = if title == "╭ Logs" {
            Color::Yellow
        } else {
            Color::DarkGray
        };
        assert_eq!(cell.fg, expect, "{title}");
    }
    let logs = border(&mut dash, "╭ Logs");
    assert!(
        logs.modifier.contains(Modifier::BOLD),
        "focused border is bold"
    );

    dash.on_key(KeyCode::Tab, KeyModifiers::NONE);
    assert_eq!(border(&mut dash, "╭ Logs").fg, Color::DarkGray);
    let agents = border(&mut dash, "╭ Registered agents");
    assert_eq!(agents.fg, Color::Yellow);
    assert!(agents.modifier.contains(Modifier::BOLD));
}

#[test]
fn table_header_is_bold_yellow_and_the_selected_row_is_reversed() {
    let mut dash = sample();
    let buf = painted(&mut dash, 110, 36);
    let (x, y) = find(&buf, "NAME");
    assert_eq!(buf[(x, y)].fg, Color::Yellow);
    assert!(buf[(x, y)].modifier.contains(Modifier::BOLD));

    let (x, y) = find(&buf, "claude_code");
    let selected = buf[(x, y)].modifier;
    assert!(selected.contains(Modifier::REVERSED) && selected.contains(Modifier::BOLD));
    let (x, y) = find(&buf, "grok_build");
    assert!(!buf[(x, y)].modifier.contains(Modifier::REVERSED));
}

#[test]
fn queued_is_yellow_when_mail_waits_and_dark_gray_when_empty() {
    let mut dash = sample();
    let buf = painted(&mut dash, 110, 36);
    // The QUEUED digit is the first digit after the "ago" in the agent's row.
    let queued_color = |name: &str| {
        let (_, y) = find(&buf, name);
        let row: Vec<char> = row_text(&buf, y).chars().collect();
        let ago = row
            .windows(3)
            .position(|w| w == ['a', 'g', 'o'])
            .expect("age column");
        let digit = (ago..row.len())
            .find(|&x| row[x].is_ascii_digit())
            .expect("queued digit");
        buf[(digit as u16, y)].fg
    };
    assert_eq!(queued_color("claude_code"), Color::Yellow);
    assert_eq!(queued_color("grok_build"), Color::DarkGray);
}

#[test]
fn log_lines_are_colored_by_level() {
    let mut dash = sample();
    let buf = painted(&mut dash, 110, 36);
    let fg = |word: &str| {
        let (x, y) = find(&buf, word);
        buf[(x, y)].fg
    };
    assert_eq!(fg("served"), Color::DarkGray);
    assert_eq!(fg("careful"), Color::Yellow);
    assert_eq!(fg("broke"), Color::Red);
}

/// The screen without its footer, whose key hints legitimately contain arrows.
fn panes_only(dash: &mut Dashboard, w: u16, h: u16) -> String {
    let buf = painted(dash, w, h);
    (0..h - 1)
        .map(|y| row_text(&buf, y))
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
fn scrollbars_appear_only_when_content_overflows() {
    let mut dash = sample();
    let calm = panes_only(&mut dash, 110, 36);
    assert!(!calm.contains('↑') && !calm.contains('↓'), "{calm}");

    let mut logs = LogBuffer::new();
    for i in 0..200 {
        writeln!(logs, " INFO line {i}").unwrap();
    }
    let many: Vec<AgentOverview> = (0..30)
        .map(|i| agent(&format!("agent_{i}"), 1, 0))
        .collect();
    dash.update(logs.lines(), overview(0, many));
    let busy = panes_only(&mut dash, 110, 36);
    assert_eq!(
        busy.matches('↑').count(),
        2,
        "one per overflowing pane\n{busy}"
    );
    assert_eq!(
        busy.matches('↓').count(),
        2,
        "one per overflowing pane\n{busy}"
    );
}

#[test]
fn key_guide_is_white_keys_and_gray_descriptions_with_no_bar() {
    let mut dash = sample();
    let buf = painted(&mut dash, 110, 36);
    let y = buf.area.height - 1;
    let text = row_text(&buf, y);
    let col = |needle: &str| -> u16 {
        let byte = text
            .find(needle)
            .unwrap_or_else(|| panic!("{needle:?} not in footer: {text:?}"));
        text[..byte].chars().count() as u16
    };

    for key in ["↑↓", "PgUp/PgDn", "End", "Tab", "q"] {
        assert_eq!(buf[(col(key), y)].fg, Color::White, "key {key:?}");
    }
    for description in ["scroll", "page", "follow", "next pane", "quit"] {
        assert_eq!(
            buf[(col(description), y)].fg,
            Color::DarkGray,
            "{description:?}"
        );
    }
    assert_eq!(buf[(col("·"), y)].fg, Color::DarkGray, "separator");

    // No bar, chip or key caps: nothing in the footer has a background.
    assert!(
        (0..buf.area.width).all(|x| buf[(x, y)].bg == Color::Reset),
        "footer has no background color: {text:?}"
    );
    assert!(!text.contains("Logs") && !text.contains('│'), "{text:?}");

    dash.on_key(KeyCode::Tab, KeyModifiers::NONE);
    let text = row_text(&painted(&mut dash, 110, 36), y);
    assert!(
        text.contains("select row") && !text.contains("follow"),
        "{text:?}"
    );
    assert!(text.contains("Tab") && text.contains("quit"));
}
