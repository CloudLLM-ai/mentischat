//! The built `mentischat` binary as a process: argument handling, startup, the
//! single-instance lock, the shutdown wipe on SIGTERM, persistence across a
//! restart, and the refusals at startup. Everything else tests the library in
//! process; this is the only suite that runs `main`.
//!
//! Every daemon here binds `127.0.0.1:0` through `MENTISCHAT_LISTEN_PORT=0`
//! (never the real 9480, which may belong to a running daemon) and reads the
//! address it got from its own "serving" log line.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::Path;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::time::{Duration, Instant};

const BIN: &str = env!("CARGO_BIN_EXE_mentischat");
const STARTUP: Duration = Duration::from_secs(30);

/// A command for the daemon with the test environment: plain logs without
/// colour, a private data directory, an OS-chosen port, no inherited knobs.
fn command(data_dir: &Path) -> Command {
    let mut cmd = Command::new(BIN);
    for var in [
        "MENTISCHAT_PERSIST",
        "MENTISCHAT_READ_TTL",
        "MENTISCHAT_AGENT_TTL",
        "MENTISCHAT_NAME_HOLD",
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
struct Running {
    child: Child,
    addr: SocketAddr,
    startup_log: Vec<String>,
    lines: Receiver<String>,
}

impl Running {
    /// Start the daemon from `cmd` and wait for its "serving" line.
    fn start(mut cmd: Command) -> Self {
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
    fn terminate(mut self) -> ExitStatus {
        let status = Command::new("kill")
            .args(["-TERM", &self.child.id().to_string()])
            .status()
            .expect("run kill");
        assert!(status.success(), "kill -TERM failed");
        wait_exit(&mut self.child)
    }

    /// Block until a logged line (at startup or later) contains `needle`.
    fn wait_for_log(&self, needle: &str) {
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
fn served_addr(line: &str) -> Option<SocketAddr> {
    if !line.contains("serving MCP") {
        return None;
    }
    let rest = line.split("addr=").nth(1)?;
    rest.split_whitespace().next()?.parse().ok()
}

/// Wait for `child` to exit on its own, failing the test after [`STARTUP`].
fn wait_exit(child: &mut Child) -> ExitStatus {
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
fn http(addr: SocketAddr, method: &str, path: &str, body: Option<&str>) -> (u16, String) {
    let mut stream = TcpStream::connect(addr).expect("connect to the daemon");
    stream
        .set_read_timeout(Some(STARTUP))
        .expect("read timeout");
    let body = body.unwrap_or("");
    write!(
        stream,
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
    .expect("write request");
    let mut raw = String::new();
    stream.read_to_string(&mut raw).expect("read reply");
    let (head, body) = raw.split_once("\r\n\r\n").expect("a complete reply");
    let status = head
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .expect("status code");
    (status, body.to_string())
}

/// Run `mentischat <args>` to completion: `(exit code, stdout, stderr)`.
fn run_args(args: &[&str]) -> (Option<i32>, String, String) {
    let out = Command::new(BIN)
        .args(args)
        .stdin(Stdio::null())
        .output()
        .expect("run mentischat");
    (
        out.status.code(),
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

/// Run the daemon with `cmd` until it exits by itself (a startup refusal):
/// `(exit code, stderr)`.
fn run_to_exit(mut cmd: Command) -> (Option<i32>, String) {
    let mut child = cmd
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn mentischat");
    let mut stderr = child.stderr.take().expect("piped stderr");
    let reader = std::thread::spawn(move || {
        let mut text = String::new();
        let _ = stderr.read_to_string(&mut text);
        text
    });
    let status = wait_exit(&mut child);
    (status.code(), reader.join().expect("stderr reader"))
}

fn register(addr: SocketAddr, agent: &str) -> (u16, String) {
    http(
        addr,
        "POST",
        "/agents",
        Some(&format!(r#"{{"agent":"{agent}"}}"#)),
    )
}

fn snapshot_file(dir: &Path) -> std::path::PathBuf {
    dir.join("snapshot.json")
}

/// `--help` and `--version` print and exit 0; an unknown argument prints the
/// usage to stderr and exits 2, so a typo never starts a daemon.
#[test]
fn test_help_version_and_unknown_argument() {
    let (code, out, _) = run_args(&["--help"]);
    assert_eq!(code, Some(0));
    assert!(out.contains("mentischat: local mailbox daemon"));

    let (code, out, _) = run_args(&["-V"]);
    assert_eq!(code, Some(0));
    assert_eq!(
        out.trim(),
        format!("mentischat {}", env!("CARGO_PKG_VERSION"))
    );

    let (code, _, err) = run_args(&["--frobnicate"]);
    assert_eq!(code, Some(2));
    assert!(err.contains("unexpected argument '--frobnicate'"));
}

/// The `client` subcommand is reachable from the binary: `--help` prints the
/// client usage, and a bad client command exits 2 with the reason.
#[cfg(feature = "e2e")]
#[test]
fn test_client_subcommand_help_and_usage_error() {
    let (code, out, _) = run_args(&["client", "--help"]);
    assert_eq!(code, Some(0));
    assert!(out.contains("client"));

    let (code, _, err) = run_args(&["client", "no-such-command"]);
    assert_eq!(code, Some(2));
    assert!(err.starts_with("mentischat client:"));
}

/// A started daemon answers on the port it logged, takes mail, and a SIGTERM
/// makes it exit 0 having wiped the mailbox from disk (no `MENTISCHAT_PERSIST`).
#[test]
fn test_sigterm_wipes_the_mailbox_and_exits_zero() {
    let dir = tempfile::tempdir().expect("tempdir");
    let daemon = Running::start(command(dir.path()));
    let addr = daemon.addr;
    assert!(addr.ip().is_loopback());
    assert_ne!(addr.port(), 0, "the log reports the port actually bound");

    let (status, body) = http(addr, "GET", "/health", None);
    assert_eq!(status, 200, "{body}");
    assert_eq!(register(addr, "Vega").0, 200);
    assert_eq!(register(addr, "Orion").0, 200);
    let (status, body) = http(
        addr,
        "POST",
        "/messages",
        Some(r#"{"from":"Vega","to":"Orion","body":"hello"}"#),
    );
    assert_eq!(status, 200, "{body}");

    let status = daemon.terminate();
    assert_eq!(status.code(), Some(0), "a clean stop exits zero");
    assert!(
        !snapshot_file(dir.path()).exists(),
        "a clean stop wipes the mailbox on disk"
    );
}

/// Binding the port is the single-instance lock: a second daemon on the same
/// port exits non-zero naming the address, and the first keeps serving and
/// keeps its data.
#[test]
fn test_second_instance_exits_and_leaves_the_first_alone() {
    let dir = tempfile::tempdir().expect("tempdir");
    let first = Running::start(command(dir.path()));
    assert_eq!(register(first.addr, "Vega").0, 200);

    let other_dir = tempfile::tempdir().expect("tempdir");
    let mut second = command(other_dir.path());
    second.env("MENTISCHAT_LISTEN_PORT", first.addr.port().to_string());
    let (code, stderr) = run_to_exit(second);
    assert_eq!(code, Some(1));
    assert!(stderr.contains("already in use"), "{stderr}");
    assert!(stderr.contains(&first.addr.to_string()), "{stderr}");

    let (status, body) = http(first.addr, "GET", "/agents", None);
    assert_eq!(status, 200, "{body}");
    assert!(
        body.contains("Vega"),
        "the first instance still has its roster"
    );
}

/// With `MENTISCHAT_PERSIST=1` a clean stop keeps the mailbox and the next
/// start serves it again.
#[test]
fn test_persist_survives_a_restart() {
    let dir = tempfile::tempdir().expect("tempdir");
    let persisted = || {
        let mut cmd = command(dir.path());
        cmd.env("MENTISCHAT_PERSIST", "1");
        cmd
    };
    let first = Running::start(persisted());
    assert_eq!(register(first.addr, "Vega").0, 200);
    assert_eq!(register(first.addr, "Orion").0, 200);
    assert_eq!(
        http(
            first.addr,
            "POST",
            "/messages",
            Some(r#"{"from":"Vega","to":"Orion","body":"survive me"}"#),
        )
        .0,
        200
    );
    assert_eq!(first.terminate().code(), Some(0));
    assert!(snapshot_file(dir.path()).exists(), "persist keeps the file");

    let second = Running::start(persisted());
    let (status, body) = http(second.addr, "GET", "/inboxes/Orion", None);
    assert_eq!(status, 200, "{body}");
    assert!(body.contains("survive me"), "mail came back: {body}");
}

/// Without persistence a start clears whatever a previous persistent run left
/// behind (its snapshot, or a crashed run's), and says so.
#[test]
fn test_start_clears_what_a_crashed_run_left() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut cmd = command(dir.path());
    cmd.env("MENTISCHAT_PERSIST", "1");
    let left_behind = Running::start(cmd);
    assert_eq!(register(left_behind.addr, "Ghost").0, 200);
    assert_eq!(left_behind.terminate().code(), Some(0));
    assert!(snapshot_file(dir.path()).exists());

    let fresh = Running::start(command(dir.path()));
    fresh.wait_for_log("cleared mail and agents left by a previous run");
    let (_, body) = http(fresh.addr, "GET", "/agents", None);
    assert!(!body.contains("Ghost"), "{body}");
}

/// A snapshot this build cannot read is replaced at a normal start, but a
/// persistent start refuses to run over it and names the file to delete.
#[test]
fn test_corrupt_snapshot_is_replaced_or_refused() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(snapshot_file(dir.path()), b"{ not a snapshot").expect("plant snapshot");

    let mut persistent = command(dir.path());
    persistent.env("MENTISCHAT_PERSIST", "1");
    let (code, stderr) = run_to_exit(persistent);
    assert_eq!(code, Some(1));
    assert!(stderr.contains("snapshot.json"), "names the file: {stderr}");
    assert!(snapshot_file(dir.path()).exists(), "the file is kept");

    let daemon = Running::start(command(dir.path()));
    let (status, _) = http(daemon.addr, "GET", "/health", None);
    assert_eq!(
        status, 200,
        "a normal start replaces the unreadable snapshot"
    );
}

/// Bad configuration is refused before anything binds: retention values that do
/// not parse, and a listen port that is not a port, exit 2 with the reason.
#[test]
fn test_bad_configuration_exits_two() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut cmd = command(dir.path());
    cmd.env("MENTISCHAT_READ_TTL", "soon");
    let (code, stderr) = run_to_exit(cmd);
    assert_eq!(code, Some(2));
    assert!(stderr.contains("MENTISCHAT_READ_TTL"), "{stderr}");

    let mut cmd = command(dir.path());
    cmd.env("MENTISCHAT_LISTEN_PORT", "http");
    let (code, stderr) = run_to_exit(cmd);
    assert_eq!(code, Some(2));
    assert!(stderr.contains("MENTISCHAT_LISTEN_PORT"), "{stderr}");
}

/// The daemon needs a data directory: with neither `MENTISCHAT_DIR` nor `HOME`
/// it refuses to start instead of guessing one.
#[test]
fn test_missing_home_and_dir_refuses_to_start() {
    let mut cmd = Command::new(BIN);
    cmd.env_remove("MENTISCHAT_DIR")
        .env_remove("HOME")
        .env("MENTISCHAT_PLAIN", "1")
        .env("MENTISCHAT_LISTEN_PORT", "0")
        .stdin(Stdio::null());
    let (code, stderr) = run_to_exit(cmd);
    assert_eq!(code, Some(1));
    assert!(stderr.contains("HOME is not set"), "{stderr}");
}

/// `listen_addr` keeps the documented address unless the port variable says
/// otherwise, treats an empty value as unset, never leaves loopback, and
/// rejects anything that is not a `u16` with a message naming the variable.
#[test]
fn test_listen_addr_rules() {
    use mentischat::{listen_addr, BIND_ADDR, LISTEN_PORT_VAR};

    assert_eq!(listen_addr(|_| None).unwrap(), BIND_ADDR);
    assert_eq!(listen_addr(|_| Some(String::new())).unwrap(), BIND_ADDR);
    let moved = listen_addr(|var| (var == LISTEN_PORT_VAR).then(|| "65535".to_string())).unwrap();
    assert_eq!(moved.port(), 65535);
    assert_eq!(moved.ip(), BIND_ADDR.ip());
    for bad in ["65536", "-1", "9480 ", "0x10", "localhost:80"] {
        let err = listen_addr(|_| Some(bad.to_string())).unwrap_err();
        assert!(err.to_string().contains(LISTEN_PORT_VAR), "{err}");
        assert!(err.to_string().contains(bad), "{err}");
    }
}

/// Run `mentischat client <args>` with its key directory in `keys`, feeding
/// `stdin`: `(exit code, stdout, stderr)`.
#[cfg(feature = "e2e")]
fn run_client(keys: &Path, args: &[&str], stdin: &str) -> (Option<i32>, String, String) {
    let mut child = Command::new(BIN)
        .arg("client")
        .args(args)
        .env("MENTISCHAT_CLIENT_DIR", keys)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn mentischat client");
    child
        .stdin
        .take()
        .expect("piped stdin")
        .write_all(stdin.as_bytes())
        .expect("write stdin");
    let out = child.wait_with_output().expect("client output");
    (
        out.status.code(),
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

/// `client key init` creates an identity and prints its public key; `key show`
/// prints the same one; `key show` for an agent with no identity exits 1 with
/// the command that creates one, and an invalid agent name is a usage error.
#[cfg(feature = "e2e")]
#[test]
fn test_client_key_init_and_show() {
    let keys = tempfile::tempdir().expect("tempdir");
    let (code, init, _) = run_client(keys.path(), &["key", "init", "--agent", "Vega"], "");
    assert_eq!(code, Some(0));
    assert!(init.contains("agent: Vega"), "{init}");
    assert!(init.contains("public key (hex): "), "{init}");
    assert!(keys.path().join("vega/identity.key").exists());

    let (code, shown, _) = run_client(keys.path(), &["key", "show", "--agent", "Vega"], "");
    assert_eq!(code, Some(0));
    let key_line = |text: &str| {
        text.lines()
            .find(|l| l.starts_with("public key (hex):"))
            .map(str::to_string)
    };
    assert_eq!(key_line(&init), key_line(&shown));

    let (code, _, err) = run_client(keys.path(), &["key", "show", "--agent", "Orion"], "");
    assert_eq!(code, Some(1));
    assert!(err.contains("key init --agent Orion"), "{err}");

    let (code, _, err) = run_client(keys.path(), &["key", "init", "--agent", "no spaces"], "");
    assert_eq!(code, Some(2));
    assert!(err.contains("--agent is not a valid agent name"), "{err}");
}

/// `client run` is the stdio MCP proxy: against a live daemon it answers
/// `initialize` and `tools/list` on stdout and exits 0 when stdin closes.
#[cfg(feature = "e2e")]
#[test]
fn test_client_run_serves_mcp_over_stdio() {
    let data = tempfile::tempdir().expect("tempdir");
    let keys = tempfile::tempdir().expect("tempdir");
    let daemon = Running::start(command(data.path()));
    let addr = daemon.addr.to_string();

    let requests = concat!(
        r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"t","version":"0"}}}"#,
        "\n",
        r#"{"jsonrpc":"2.0","id":2,"method":"tools/list"}"#,
        "\n",
    );
    let (code, out, err) = run_client(
        keys.path(),
        &["run", "--agent", "Vega", "--addr", &addr],
        requests,
    );
    assert_eq!(code, Some(0), "{err}");
    let replies: Vec<serde_json::Value> = out
        .lines()
        .map(|l| serde_json::from_str(l).expect("one JSON reply per line"))
        .collect();
    assert_eq!(replies.len(), 2, "{out}");
    assert_eq!(replies[0]["id"], 1);
    assert!(replies[0]["result"]["serverInfo"].is_object(), "{out}");
    assert!(
        replies[1]["result"]["tools"]
            .as_array()
            .is_some_and(|t| !t.is_empty()),
        "{out}"
    );
}

/// A `--addr` that is not loopback is refused before anything connects.
#[cfg(feature = "e2e")]
#[test]
fn test_client_run_refuses_a_non_loopback_daemon() {
    let keys = tempfile::tempdir().expect("tempdir");
    let (code, _, err) = run_client(
        keys.path(),
        &["run", "--agent", "Vega", "--addr", "10.0.0.5:9480"],
        "",
    );
    assert_eq!(code, Some(2));
    assert!(err.contains("loopback"), "{err}");
}
