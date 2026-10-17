//! The terminal view's panic hook is scoped to its own thread. This lives in
//! its own test binary because it replaces the process-wide panic hook.

use std::panic;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;

use mentischat::tui::scoped_panic_hook;

#[test]
fn only_panics_on_the_view_thread_run_the_view_hook() {
    let on_view = Arc::new(AtomicUsize::new(0));
    let elsewhere = Arc::new(AtomicUsize::new(0));
    let (v, e) = (on_view.clone(), elsewhere.clone());
    panic::set_hook(scoped_panic_hook(
        thread::current().id(),
        Box::new(move |_| {
            v.fetch_add(1, Ordering::SeqCst);
        }),
        Arc::new(move |_| {
            e.fetch_add(1, Ordering::SeqCst);
        }),
    ));
    let counts = || {
        (
            on_view.load(Ordering::SeqCst),
            elsewhere.load(Ordering::SeqCst),
        )
    };

    thread::spawn(|| panic!("a request handler"))
        .join()
        .unwrap_err();
    assert_eq!(
        counts(),
        (0, 1),
        "another thread's panic leaves the terminal alone"
    );

    panic::catch_unwind(|| panic!("the view")).unwrap_err();
    assert_eq!(counts(), (1, 1), "the view thread's panic restores it");

    drop(panic::take_hook());
}
