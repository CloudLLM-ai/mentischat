//! The terminal view's lifetime: the panic hook scoped to the view thread and
//! the guard that stops the daemon when the view ends on its own. The painter
//! itself is in [`super::painter`].

use std::{io, panic::PanicHookInfo, sync::Arc, thread::ThreadId};

use tokio::sync::Notify;

/// A panic hook, as [`std::panic::set_hook`] takes it.
pub type PanicHook = dyn Fn(&PanicHookInfo<'_>) + Sync + Send + 'static;

/// A panic hook that runs `on_view` for panics on thread `view` and
/// `elsewhere` for every other thread. [`super::run`] installs one so only a panic in
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

pub(super) fn forward(hook: Arc<PanicHook>) -> Box<PanicHook> {
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
