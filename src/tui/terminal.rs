//! Running the dashboard on a real terminal, and tying its lifetime to the
//! daemon's.

use std::{
    io,
    panic::PanicHookInfo,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread::ThreadId,
    time::{Duration, Instant},
};

use ratatui::crossterm::event::{self, Event, KeyEventKind};
use tokio::sync::Notify;

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

/// A panic hook, as [`std::panic::set_hook`] takes it.
pub type PanicHook = dyn Fn(&PanicHookInfo<'_>) + Sync + Send + 'static;

/// A panic hook that runs `on_view` for panics on thread `view` and
/// `elsewhere` for every other thread. [`run`] installs one so only a panic in
/// the terminal view restores the terminal.
pub fn scoped_panic_hook(
    view: ThreadId,
    on_view: Box<PanicHook>,
    elsewhere: Arc<PanicHook>,
) -> Box<PanicHook> {
    Box::new(move |info| {
        if std::thread::current().id() == view {
            on_view(info);
        } else {
            elsewhere(info);
        }
    })
}

fn forward(hook: Arc<PanicHook>) -> Box<PanicHook> {
    Box::new(move |info| hook(info))
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
