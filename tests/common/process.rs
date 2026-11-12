//! Running the built `mentischat` binary as a child process, for the suites
//! that test the daemon as users meet it (`daemon_process.rs`, `soak.rs`).
//!
//! Every daemon binds `127.0.0.1` through `MENTISCHAT_LISTEN_PORT` (port 0 by
//! default: never the real 9480, which may belong to a running daemon) and
//! reports the address it got on its "serving" log line.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::Path;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::time::{Duration, Instant};

pub const BIN: &str = env!("CARGO_BIN_EXE_mentischat");
pub const STARTUP: Duration = Duration::from_secs(30);

/// A command for the daemon with the test environment: plain logs without
/// colour, a private data directory, an OS-chosen port, no inherited knobs.
pub fn command(data_dir: &Path) -> Command {
    let mut cmd = Command::new(BIN);
    for var in [
        "MENTISCHAT_PERSIST",
        "MENTISCHAT_READ_TTL",
        "MENTISCHAT_AGENT_TTL",
        "MENTISCHAT_NAME_HOLD",
        "MENTISCHAT_OBSERVE",
    ] {
        cmd.env_remove(var);
    }
    cmd.env("MENTISCHAT_PLAIN", "1")
        .env("NO_COLOR", "1")
        .env("MENTISCHAT_DIR", data_dir)
        .env("MENTISCHAT_LISTEN_PORT", "0")
        .stdin(Stdio::null());
    cmd
}

/// A running daemon: its child process, the address it reported, the lines it
/// logged before it reported it, and the channel carrying the later ones.
pub struct Running {
    pub child: Child,
    pub addr: SocketAddr,
    startup_log: Vec<String>,
    lines: Receiver<String>,
}

impl Running {
    /// Start the daemon from `cmd` and wait for its "serving" line.
    pub fn start(mut cmd: Command) -> Self {
        let mut child = cmd
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn mentischat");
        let stdout = child.stdout.take().expect("piped stdout");
        let (tx, lines) = mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                if tx.send(line).is_err() {
                    break;
                }
            }
        });
        // Owned by a `Running` from here on, so `Drop` reaps it on any panic.
        let mut running = Self {
            child,
            addr: SocketAddr::from(([127, 0, 0, 1], 0)),
            startup_log: Vec::new(),
            lines,
        };
        let deadline = Instant::now() + STARTUP;
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            let line = running
                .lines
                .recv_timeout(left)
                .expect("the daemon never logged that it is serving");
            let served = served_addr(&line);
            running.startup_log.push(line);
            if let Some(addr) = served {
                running.addr = addr;
                return running;
            }
        }
    }

    /// Send SIGTERM and wait for the process to exit.
    pub fn terminate(mut self) -> ExitStatus {
        let status = Command::new("kill")
            .args(["-TERM", &self.child.id().to_string()])
            .status()
            .expect("run kill");
        assert!(status.success(), "kill -TERM failed");
        wait_exit(&mut self.child)
    }

    /// Every line logged since the last call (and, the first time, at startup).
    pub fn drain_log(&mut self) -> Vec<String> {
        let mut lines = std::mem::take(&mut self.startup_log);
        lines.extend(self.lines.try_iter());
        lines
    }

    /// Block until a logged line (at startup or later) contains `needle`.
    pub fn wait_for_log(&self, needle: &str) {
        if self.startup_log.iter().any(|l| l.contains(needle)) {
            return;
        }
        let deadline = Instant::now() + STARTUP;
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            let line = self
                .lines
                .recv_timeout(left)
                .unwrap_or_else(|_| panic!("the daemon never logged '{needle}'"));
            if line.contains(needle) {
                return;
            }
        }
    }
}

impl Drop for Running {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// The `addr=` field of the daemon's "serving MCP" line, if `line` is that line.
pub fn served_addr(line: &str) -> Option<SocketAddr> {
    if !line.contains("serving MCP") {
        return None;
    }
    let rest = line.split("addr=").nth(1)?;
    rest.split_whitespace().next()?.parse().ok()
}

/// Wait for `child` to exit on its own, failing the test after [`STARTUP`].
pub fn wait_exit(child: &mut Child) -> ExitStatus {
    let deadline = Instant::now() + STARTUP;
    loop {
        if let Some(status) = child.try_wait().expect("poll child") {
            return status;
        }
        assert!(Instant::now() < deadline, "the daemon did not exit in time");
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// One HTTP/1.1 request with `Connection: close`: `(status, body)`.
///
/// # Panics
///
/// On any I/O failure; use [`try_http`] where the daemon may be down.
pub fn http(addr: SocketAddr, method: &str, path: &str, body: Option<&str>) -> (u16, String) {
    try_http(addr, method, path, body).expect("the daemon answers")
}

/// [`http`] that reports a refused connection or a broken reply as an error.
pub fn try_http(
    addr: SocketAddr,
    method: &str,
    path: &str,
    body: Option<&str>,
) -> std::io::Result<(u16, String)> {
    let mut stream = TcpStream::connect(addr)?;
    stream.set_read_timeout(Some(STARTUP))?;
    let body = body.unwrap_or("");
    write!(
        stream,
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )?;
    let mut raw = String::new();
    stream.read_to_string(&mut raw)?;
    let malformed = || std::io::Error::new(std::io::ErrorKind::InvalidData, "malformed reply");
    let (head, body) = raw.split_once("\r\n\r\n").ok_or_else(malformed)?;
    let status = head
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .ok_or_else(malformed)?;
    Ok((status, body.to_string()))
}
