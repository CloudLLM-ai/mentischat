//! The interactive painter: takes over a real terminal and draws the dashboard
//! until the user quits. It is the one part of the crate that cannot run in CI
//! (it needs a TTY), so it is kept apart from the logic around it, which
//! [`super::terminal`] holds and tests cover. The coverage gate excludes
//! exactly this file; see docs/ENGINEERING.md.

use std::{
    io,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use ratatui::crossterm::event::{self, Event, KeyEventKind};

use super::terminal::{forward, scoped_panic_hook, PanicHook};
use super::{Dashboard, Info, LogBuffer};
use crate::bus::Overview;

const REFRESH: Duration = Duration::from_millis(500);
const POLL: Duration = Duration::from_millis(200);

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
    // ratatui's hook restores the terminal on a panic on *any* thread; the
    // view keeps drawing after a request handler panics, so scope it to this
    // thread and give every other thread the hook that was there before.
    let previous: Arc<PanicHook> = Arc::from(std::panic::take_hook());
    std::panic::set_hook(forward(previous.clone()));
    let mut terminal = ratatui::init();
    std::panic::set_hook(scoped_panic_hook(
        std::thread::current().id(),
        std::panic::take_hook(),
        previous.clone(),
    ));
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
    std::panic::set_hook(forward(previous));
    result
}
