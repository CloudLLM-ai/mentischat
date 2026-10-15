//! The log pane's backing store: a `tracing` writer that keeps the newest
//! lines in memory.

use std::{
    collections::VecDeque,
    io::{self, Write},
    sync::{Arc, Mutex},
    time::Instant,
};

use tracing_subscriber::fmt::MakeWriter;

use super::{elapsed_clock, plain};

const MAX_LOG_LINES: usize = 1000;

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
