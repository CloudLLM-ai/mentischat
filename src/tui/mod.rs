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

use std::time::{Duration, Instant};

use ratatui::{
    crossterm::event::{KeyCode, KeyModifiers},
    layout::{Alignment, Constraint, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{
        Block, BorderType, Borders, Cell, Paragraph, Row, Scrollbar, ScrollbarOrientation,
        ScrollbarState, Table, TableState,
    },
    Frame,
};

use crate::bus::Overview;

mod banner;
mod log_buffer;
mod terminal;

pub use log_buffer::LogBuffer;
pub use terminal::{run, run_guarded, scoped_panic_hook, PanicHook};

use banner::{banner_width, BANNER_ROWS, CHAT_PART, MENTIS_PART, TITLE};

pub(super) fn elapsed_clock(d: Duration) -> String {
    let s = d.as_secs();
    format!("{:02}:{:02}:{:02}", s / 3600, s / 60 % 60, s % 60)
}

/// Replaces control characters so client-supplied text cannot inject terminal
/// escapes.
pub(super) fn plain(s: &str) -> String {
    s.chars()
        .map(|c| if c.is_control() { '?' } else { c })
        .collect()
}

/// Static facts shown in the banner.
pub struct Info {
    /// Crate version, shown top right of the banner.
    pub version: String,
    /// Listen URL, for example `http://127.0.0.1:9480`.
    pub url: String,
    /// Data directory (where `snapshot.json` goes under `MENTISCHAT_PERSIST=1`),
    /// shown bottom right.
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
            let or_dash = |text: &Option<String>| {
                text.as_deref()
                    .map_or_else(|| Span::styled("—", secondary()), |t| Span::raw(plain(t)))
            };
            Row::new([
                Cell::from(plain(&a.name)),
                Cell::from(or_dash(&a.provider)),
                Cell::from(ago(a.seen_secs_ago)),
                Cell::from(queued),
                Cell::from(or_dash(&a.description)),
            ])
        });
        let header = Row::new(["NAME", "PROVIDER", "LAST SEEN", "QUEUED", "DESCRIPTION"])
            .style(bold(Color::Yellow));
        let table = Table::new(
            rows,
            [
                Constraint::Length(24),
                Constraint::Length(16),
                Constraint::Length(12),
                Constraint::Length(8),
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
