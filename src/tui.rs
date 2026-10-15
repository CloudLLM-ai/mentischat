//! Live terminal view for the daemon: banner and server info on top, then a
//! log pane and the agent roster.
//!
//! The daemon shows this only when started from a terminal; under launchd, a
//! pipe, or with `MENTISCHAT_PLAIN=1` it logs plain lines instead. It reads
//! the mailbox through [`crate::bus::Bus::overview`] and never writes to it.
//! Log lines carry names, ids and byte counts, never message bodies.
//!
//! Colors come from the terminal's own palette so the view reads on dark and
//! light backgrounds.

use std::{
    collections::VecDeque,
    io::{self, Write},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

use ratatui::{
    crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers},
    layout::{Alignment, Constraint, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{
        Block, BorderType, Borders, Cell, Paragraph, Row, Scrollbar, ScrollbarOrientation,
        ScrollbarState, Table, TableState,
    },
    Frame,
};
use tokio::sync::Notify;
use tracing_subscriber::fmt::MakeWriter;

use crate::bus::Overview;

const MAX_LOG_LINES: usize = 1000;
const REFRESH: Duration = Duration::from_millis(500);
const POLL: Duration = Duration::from_millis(200);

fn elapsed_clock(d: Duration) -> String {
    let s = d.as_secs();
    format!("{:02}:{:02}:{:02}", s / 3600, s / 60 % 60, s % 60)
}

/// Replaces control characters so client-supplied text cannot inject terminal
/// escapes.
fn plain(s: &str) -> String {
    s.chars()
        .map(|c| if c.is_control() { '?' } else { c })
        .collect()
}

#[derive(Default)]
struct LogState {
    lines: VecDeque<String>,
    partial: String,
}

/// Collects formatted log output for the log pane; hand it to
/// `tracing_subscriber` as the writer.
///
/// Keeps the newest 1000 lines, each prefixed with the time since the buffer
/// was created, and replaces control characters so a peer-supplied name cannot
/// paint escape sequences onto the operator's terminal.
///
/// # Examples
///
/// ```
/// use std::io::Write;
/// use mentischat::tui::LogBuffer;
///
/// let mut logs = LogBuffer::new();
/// writeln!(logs, " INFO hello").unwrap();
/// assert!(logs.lines()[0].ends_with("INFO hello"));
/// ```
#[derive(Clone)]
pub struct LogBuffer {
    state: Arc<Mutex<LogState>>,
    started: Instant,
}

impl LogBuffer {
    /// An empty buffer whose line timestamps count from now.
    pub fn new() -> Self {
        Self {
            state: Arc::default(),
            started: Instant::now(),
        }
    }

    /// The retained lines, oldest first.
    pub fn lines(&self) -> Vec<String> {
        let s = self.state.lock().unwrap_or_else(|e| e.into_inner());
        s.lines.iter().cloned().collect()
    }
}

impl Default for LogBuffer {
    fn default() -> Self {
        Self::new()
    }
}

impl Write for LogBuffer {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let stamp = elapsed_clock(self.started.elapsed());
        let mut s = self.state.lock().unwrap_or_else(|e| e.into_inner());
        s.partial.push_str(&String::from_utf8_lossy(buf));
        while let Some(i) = s.partial.find('\n') {
            let line: String = s.partial.drain(..=i).collect();
            let line = format!("{stamp}  {}", plain(line.trim_end()));
            if s.lines.len() == MAX_LOG_LINES {
                s.lines.pop_front();
            }
            s.lines.push_back(line);
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for LogBuffer {
    type Writer = LogBuffer;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

/// Static facts shown in the banner.
pub struct Info {
    /// Crate version, shown top right of the banner.
    pub version: String,
    /// Listen URL, for example `http://127.0.0.1:9480`.
    pub url: String,
    /// Data directory holding `pending.sqlite`, shown bottom right.
    pub data_path: String,
}

fn ago(secs: i64) -> String {
    let s = secs.max(0) as u64;
    match s {
        0..=59 => format!("{s}s ago"),
        60..=3599 => format!("{}m ago", s / 60),
        3600..=86_399 => format!("{}h ago", s / 3600),
        _ => format!("{}d ago", s / 86_400),
    }
}

/// Which pane the arrow keys scroll.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Focus {
    /// The log pane; scrolling moves back through history.
    Logs,
    /// The agent table; scrolling moves the row highlight.
    Agents,
}

/// Colors come from the terminal's own 8-color palette, never a fixed-palette
/// index or RGB value, so the view stays readable on dark and light themes.
const HIGHLIGHT: Style = Style::new()
    .add_modifier(Modifier::REVERSED)
    .add_modifier(Modifier::BOLD);

const TITLE: &str = "MENTIS CHAT";

/// Rows in the block-letter banner.
const BANNER_ROWS: usize = 6;

/// `MENTIS` in the same block letters as the MentisDB banner.
const MENTIS_PART: [&str; BANNER_ROWS] = [
    "███╗   ███╗███████╗███╗   ██╗████████╗██╗███████╗",
    "████╗ ████║██╔════╝████╗  ██║╚══██╔══╝██║██╔════╝",
    "██╔████╔██║█████╗  ██╔██╗ ██║   ██║   ██║███████╗",
    "██║╚██╔╝██║██╔══╝  ██║╚██╗██║   ██║   ██║╚════██║",
    "██║ ╚═╝ ██║███████╗██║ ╚████║   ██║   ██║███████║",
    "╚═╝     ╚═╝╚══════╝╚═╝  ╚═══╝   ╚═╝   ╚═╝╚══════╝",
];

/// `CHAT` in the same font, led by the gap between the two words.
const CHAT_PART: [&str; BANNER_ROWS] = [
    "    ██████╗██╗  ██╗ █████╗ ████████╗",
    "   ██╔════╝██║  ██║██╔══██╗╚══██╔══╝",
    "   ██║     ███████║███████║   ██║   ",
    "   ██║     ██╔══██║██╔══██║   ██║   ",
    "   ╚██████╗██║  ██║██║  ██║   ██║   ",
    "    ╚═════╝╚═╝  ╚═╝╚═╝  ╚═╝   ╚═╝   ",
];

fn banner_width() -> u16 {
    (MENTIS_PART[0].chars().count() + CHAT_PART[0].chars().count()) as u16
}

fn bold(color: Color) -> Style {
    Style::new().fg(color).add_modifier(Modifier::BOLD)
}

/// A rounded panel: dark gray at rest, bold yellow while it has focus.
fn panel(focused: bool) -> Block<'static> {
    let border = if focused {
        bold(Color::Yellow)
    } else {
        Style::new().fg(Color::DarkGray)
    };
    Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(border)
}

fn secondary() -> Style {
    Style::new().fg(Color::DarkGray)
}

fn log_style(line: &str) -> Style {
    let level = line.split_whitespace().nth(1).unwrap_or("");
    match level {
        "ERROR" => Style::new().fg(Color::Red),
        "WARN" => Style::new().fg(Color::Yellow),
        _ => secondary(),
    }
}

fn scrollbar() -> Scrollbar<'static> {
    Scrollbar::new(ScrollbarOrientation::VerticalRight)
        .begin_symbol(Some("↑"))
        .end_symbol(Some("↓"))
}

/// The screen state: what to draw and which pane has focus. It owns no I/O,
/// so it can be driven by [`run`] on a real terminal or rendered into a test
/// backend.
///
/// # Examples
///
/// ```
/// use mentischat::tui::{Dashboard, Info};
/// use ratatui::{backend::TestBackend, Terminal};
///
/// let mut dash = Dashboard::new(Info {
///     version: "0.1.0".into(),
///     url: "http://127.0.0.1:9480".into(),
///     data_path: "/tmp/mentischat".into(),
/// });
/// let mut term = Terminal::new(TestBackend::new(100, 30)).unwrap();
/// term.draw(|f| dash.render(f)).unwrap();
/// ```
pub struct Dashboard {
    info: Info,
    started: Instant,
    logs: Vec<String>,
    overview: Overview,
    focus: Focus,
    /// Lines scrolled back from the newest log line; 0 follows the tail.
    log_scroll: usize,
    table: TableState,
}

impl Dashboard {
    /// A view with no logs or agents yet, focused on the log pane.
    pub fn new(info: Info) -> Self {
        Self {
            info,
            started: Instant::now(),
            logs: Vec::new(),
            overview: Overview::default(),
            focus: Focus::Logs,
            log_scroll: 0,
            table: TableState::default(),
        }
    }

    /// Replaces the displayed logs, roster and sent-message count, keeping the
    /// selected row in range.
    pub fn update(&mut self, logs: Vec<String>, overview: Overview) {
        self.logs = logs;
        self.overview = overview;
        let sel = self.table.selected().unwrap_or(0);
        self.table
            .select(Some(sel.min(self.overview.agents.len().saturating_sub(1))));
    }

    /// The pane the arrow keys currently scroll.
    pub fn focus(&self) -> Focus {
        self.focus
    }

    /// Draws one frame. Layout adapts to the terminal size: the block-letter
    /// banner needs 31 rows and enough width, otherwise a one-line title is
    /// used.
    pub fn render(&mut self, f: &mut Frame) {
        let big = f.area().height >= 31 && f.area().width >= banner_width() + 4;
        let [banner, tiles, logs, agents, footer] = Layout::vertical([
            Constraint::Length(if big { BANNER_ROWS as u16 + 2 } else { 3 }),
            Constraint::Length(3),
            Constraint::Fill(3),
            Constraint::Fill(2),
            Constraint::Length(1),
        ])
        .areas(f.area());
        self.draw_banner(f, banner, big);
        self.draw_tiles(f, tiles);
        self.draw_logs(f, logs);
        self.draw_agents(f, agents);
        self.draw_hints(f, footer);
    }

    fn draw_banner(&self, f: &mut Frame, area: Rect, big: bool) {
        let lines: Vec<Line> = if big {
            MENTIS_PART
                .iter()
                .zip(CHAT_PART)
                .map(|(mentis, chat)| {
                    Line::from(vec![
                        Span::styled(*mentis, Style::new().fg(Color::Green)),
                        Span::styled(chat, Style::new().fg(Color::Cyan)),
                    ])
                })
                .collect()
        } else {
            let spaced = |word: &str| word.chars().map(String::from).collect::<Vec<_>>().join(" ");
            let (mentis, chat) = TITLE.split_once(' ').unwrap_or((TITLE, ""));
            vec![Line::from(vec![
                Span::styled(spaced(mentis), bold(Color::Green)),
                Span::raw("   "),
                Span::styled(spaced(chat), bold(Color::Cyan)),
            ])]
        };
        let block = panel(false)
            .title_top(
                Line::from(Span::styled(
                    format!(" v{} ", self.info.version),
                    Style::new().add_modifier(Modifier::BOLD),
                ))
                .right_aligned(),
            )
            .title_bottom(
                Line::from(Span::styled(
                    format!(" {} ", self.info.data_path),
                    secondary(),
                ))
                .right_aligned(),
            );
        f.render_widget(
            Paragraph::new(lines)
                .alignment(Alignment::Center)
                .block(block),
            area,
        );
    }

    fn draw_tiles(&self, f: &mut Frame, area: Rect) {
        let tiles = [
            ("LISTEN", self.info.url.clone()),
            ("MESSAGES SENT", self.overview.messages_sent.to_string()),
            ("AGENTS", self.overview.agents.len().to_string()),
            ("UPTIME", elapsed_clock(self.started.elapsed())),
        ];
        let areas: [Rect; 4] = Layout::horizontal([
            Constraint::Fill(5),
            Constraint::Fill(4),
            Constraint::Fill(3),
            Constraint::Fill(3),
        ])
        .areas(area);
        for ((label, value), area) in tiles.into_iter().zip(areas) {
            let value = if label == "LISTEN" {
                Line::from(vec![
                    Span::styled("● ", bold(Color::Green)),
                    Span::styled(value, Style::new().add_modifier(Modifier::BOLD)),
                ])
            } else {
                Line::from(Span::styled(
                    value,
                    Style::new().add_modifier(Modifier::BOLD),
                ))
            };
            f.render_widget(
                Paragraph::new(value)
                    .alignment(Alignment::Center)
                    .block(panel(false).title(format!(" {label} "))),
                area,
            );
        }
    }

    fn draw_logs(&mut self, f: &mut Frame, area: Rect) {
        let title = if self.log_scroll == 0 {
            " Logs ".to_string()
        } else {
            format!(" Logs (scrolled back {}) ", self.log_scroll)
        };
        let block = panel(self.focus == Focus::Logs).title(title);
        let height = block.inner(area).height as usize;
        let total = self.logs.len();
        self.log_scroll = self.log_scroll.min(total.saturating_sub(height));
        let end = total - self.log_scroll;
        let start = end.saturating_sub(height);
        let lines: Vec<Line> = self.logs[start..end]
            .iter()
            .map(|l| Line::from(Span::styled(l.clone(), log_style(l))))
            .collect();
        f.render_widget(Paragraph::new(lines).block(block), area);
        if total > height {
            let mut bar = ScrollbarState::new(total - height).position(start);
            f.render_stateful_widget(scrollbar(), area, &mut bar);
        }
    }

    fn draw_agents(&mut self, f: &mut Frame, area: Rect) {
        let count = self.overview.agents.len();
        let block =
            panel(self.focus == Focus::Agents).title(format!(" Registered agents ({count}) "));
        if count == 0 {
            let msg =
                Paragraph::new(Span::styled("no agents registered yet", secondary())).block(block);
            f.render_widget(msg, area);
            return;
        }
        // One row of the inner area is the header.
        let visible = block.inner(area).height.saturating_sub(1) as usize;
        let rows = self.overview.agents.iter().map(|a| {
            let queued = if a.queued > 0 {
                Span::styled(a.queued.to_string(), Style::new().fg(Color::Yellow))
            } else {
                Span::styled("0", secondary())
            };
            Row::new([
                Cell::from(plain(&a.name)),
                Cell::from(ago(a.seen_secs_ago)),
                Cell::from(queued),
            ])
        });
        let header = Row::new(["NAME", "LAST SEEN", "QUEUED"]).style(bold(Color::Yellow));
        let table = Table::new(
            rows,
            [
                Constraint::Length(28),
                Constraint::Length(12),
                Constraint::Fill(1),
            ],
        )
        .header(header)
        .block(block)
        .row_highlight_style(HIGHLIGHT);
        f.render_stateful_widget(table, area, &mut self.table);
        if count > visible {
            let mut bar = ScrollbarState::new(count - visible)
                .position(self.table.selected().unwrap_or(0).min(count - visible));
            f.render_stateful_widget(scrollbar(), area, &mut bar);
        }
    }

    /// The footer: the focused pane's keys in white, each followed by its
    /// description in gray, with no bar behind them.
    fn draw_hints(&self, f: &mut Frame, area: Rect) {
        let key = Style::new().fg(Color::White);
        let sep = Span::styled("  ·  ", secondary());
        let hint = |keys: &'static str, what: &'static str| {
            [
                Span::styled(keys, key),
                Span::styled(format!(" {what}"), secondary()),
            ]
        };
        let pane_hints: Vec<[Span; 2]> = match self.focus {
            Focus::Logs => vec![
                hint("↑↓", "scroll"),
                hint("PgUp/PgDn", "page"),
                hint("End", "follow"),
            ],
            Focus::Agents => vec![hint("↑↓", "select row")],
        };
        let mut spans = Vec::new();
        for pair in pane_hints
            .into_iter()
            .chain([hint("Tab", "next pane"), hint("q", "quit")])
        {
            if !spans.is_empty() {
                spans.push(sep.clone());
            }
            spans.extend(pair);
        }
        f.render_widget(Paragraph::new(Line::from(spans)), area);
    }

    fn scroll(&mut self, up: bool, by: usize) {
        match self.focus {
            Focus::Logs => {
                self.log_scroll = if up {
                    self.log_scroll.saturating_add(by)
                } else {
                    self.log_scroll.saturating_sub(by)
                };
            }
            Focus::Agents => {
                let last = self.overview.agents.len().saturating_sub(1);
                let cur = self.table.selected().unwrap_or(0);
                let next = if up {
                    cur.saturating_sub(by)
                } else {
                    (cur + by).min(last)
                };
                self.table.select(Some(next));
            }
        }
    }

    /// Applies one key press. Returns `true` when the user asked to quit
    /// (`q`, `Esc` or `Ctrl-C`).
    pub fn on_key(&mut self, code: KeyCode, mods: KeyModifiers) -> bool {
        match code {
            KeyCode::Char('q') | KeyCode::Esc => return true,
            KeyCode::Char('c') if mods.contains(KeyModifiers::CONTROL) => return true,
            KeyCode::Tab | KeyCode::BackTab => {
                self.focus = match self.focus {
                    Focus::Logs => Focus::Agents,
                    Focus::Agents => Focus::Logs,
                };
            }
            KeyCode::Up | KeyCode::Char('k') => self.scroll(true, 1),
            KeyCode::Down | KeyCode::Char('j') => self.scroll(false, 1),
            KeyCode::PageUp => self.scroll(true, 10),
            KeyCode::PageDown => self.scroll(false, 10),
            KeyCode::End => self.log_scroll = 0,
            _ => {}
        }
        false
    }
}

/// Runs the terminal view until the user quits or `running` is cleared.
///
/// `overview` is polled every 500 ms for the tiles and agent table; when it
/// returns `None` (the mailbox is shutting down) the previous values stay on
/// screen.
/// Takes over the terminal (raw mode, alternate screen) and restores it before
/// returning, including on error.
///
/// Returns `Ok(true)` if the user asked to quit and `Ok(false)` if `running`
/// was cleared first.
///
/// # Errors
///
/// Any terminal I/O failure.
pub fn run(
    mut overview: impl FnMut() -> Option<Overview>,
    logs: &LogBuffer,
    info: Info,
    running: &AtomicBool,
) -> io::Result<bool> {
    let mut dash = Dashboard::new(info);
    let mut terminal = ratatui::init();
    let result = (|| {
        let mut latest = Overview::default();
        let mut last_refresh: Option<Instant> = None;
        while running.load(Ordering::Relaxed) {
            if last_refresh.is_none_or(|t| t.elapsed() >= REFRESH) {
                if let Some(fresh) = overview() {
                    latest = fresh;
                }
                dash.update(logs.lines(), latest.clone());
                last_refresh = Some(Instant::now());
            }
            terminal.draw(|f| dash.render(f))?;
            if event::poll(POLL)? {
                if let Event::Key(k) = event::read()? {
                    if k.kind == KeyEventKind::Press && dash.on_key(k.code, k.modifiers) {
                        return Ok(true);
                    }
                }
            }
        }
        Ok(false)
    })();
    ratatui::restore();
    result
}

/// Runs `view` and stops the server when the view ends on its own: the user
/// quit, it returned an error, or it panicked. Without this a panic would
/// leave the daemon running headless until SIGTERM.
///
/// Returns the view's result, with a panic reported as an error. `stop` is
/// notified unless `view` returned `Ok(false)`, which means the server stopped
/// first and needs no shutdown.
///
/// # Errors
///
/// The view's own error, or `terminal view panicked`.
pub fn run_guarded(view: impl FnOnce() -> io::Result<bool>, stop: &Notify) -> io::Result<bool> {
    // ratatui's panic hook has already restored the terminal by the time this
    // catches.
    let quit = std::panic::catch_unwind(std::panic::AssertUnwindSafe(view))
        .unwrap_or_else(|_| Err(io::Error::other("terminal view panicked")));
    if !matches!(quit, Ok(false)) {
        stop.notify_one();
    }
    quit
}
