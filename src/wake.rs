//! PTY harness wrapper: long-poll the daemon and inject the nudge pointer.
//!
//! Used by `mentischat opencode` and the legacy `mentis-wake` binary.

use std::io::{Read, Write};
use std::process::ExitCode;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use portable_pty::{native_pty_system, CommandBuilder, PtySize};
use serde_json::Value;
use tokio::signal::unix::{signal, SignalKind};

use crate::names::AgentName;
use crate::nudge::WAKE_ENV;

/// Mirror the daemon cooldown so reconnects do not double-inject.
const COOLDOWN: Duration = Duration::from_secs(60);
/// How long each nudge long-poll blocks on the server.
const POLL_WAIT_MS: u32 = 30_000;
/// Retry gap for transient daemon/registration failures.
const RETRY: Duration = Duration::from_secs(2);

/// Options for [`run_harness`].
pub struct WakeConfig {
    /// Prefix for stderr messages (e.g. `"mentischat"`).
    pub program: &'static str,
    pub agent: Option<String>,
    pub url: String,
    pub no_register: bool,
    pub newline: String,
    pub command: Vec<String>,
}

enum PollTarget {
    Agent(String),
    Wake(String),
}

/// Run a TUI harness on a PTY with idle-wake long-polling.
pub async fn run_harness(config: WakeConfig) -> ExitCode {
    let program = config.program;
    let poll_target = match &config.agent {
        Some(a) => match AgentName::parse(a) {
            Ok(name) => PollTarget::Agent(name.to_string()),
            Err(_) => {
                eprintln!("{program}: invalid agent name {a:?}");
                return ExitCode::from(2);
            }
        },
        None => PollTarget::Wake(uuid::Uuid::new_v4().to_string()),
    };
    if config.command.is_empty() {
        eprintln!("{program}: no harness command given");
        return ExitCode::from(2);
    }
    let newline: &[u8] = match parse_submit_sequence(&config.newline) {
        Ok(b) => b,
        Err(msg) => {
            eprintln!("{program}: {msg}");
            return ExitCode::from(2);
        }
    };

    let url = config.url.trim_end_matches('/').to_string();
    let wake_id_for_cleanup = match &poll_target {
        PollTarget::Wake(id) => Some(id.clone()),
        PollTarget::Agent(_) => None,
    };
    let agent_name_for_cleanup = match &poll_target {
        PollTarget::Agent(a) => Some(a.clone()),
        PollTarget::Wake(_) => None,
    };

    let (rows, cols) = terminal_size().unwrap_or((24, 80));
    let pty_system = native_pty_system();
    let pair = match pty_system.openpty(PtySize {
        rows,
        cols,
        pixel_width: 0,
        pixel_height: 0,
    }) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("{program}: openpty failed: {e}");
            return ExitCode::from(1);
        }
    };
    let master = pair.master;
    let mut cmd = CommandBuilder::new(&config.command[0]);
    for a in &config.command[1..] {
        cmd.arg(a);
    }
    if let PollTarget::Wake(ref wake_id) = poll_target {
        cmd.env(WAKE_ENV, wake_id);
    }
    let mut child = match pair.slave.spawn_command(cmd) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("{program}: spawn failed: {e}");
            return ExitCode::from(1);
        }
    };
    drop(pair.slave);

    let saved_termios = set_raw();

    let mut reader = match master.try_clone_reader() {
        Ok(r) => r,
        Err(e) => {
            eprintln!("{program}: reader failed: {e}");
            restore_raw(saved_termios);
            return ExitCode::from(1);
        }
    };
    let writer: Box<dyn Write + Send> = match master.take_writer() {
        Ok(w) => w,
        Err(e) => {
            eprintln!("{program}: writer failed: {e}");
            restore_raw(saved_termios);
            return ExitCode::from(1);
        }
    };
    let writer = Arc::new(Mutex::new(writer));

    std::thread::spawn(move || forward_reader(&mut reader));

    {
        let writer = Arc::clone(&writer);
        std::thread::spawn(move || forward_stdin(&writer));
    }

    let bind_token: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let mut registration_task: Option<tokio::task::JoinHandle<()>> = None;
    let client = reqwest::Client::new();

    if let PollTarget::Agent(ref agent_name) = poll_target {
        if !config.no_register {
            let url = url.clone();
            let agent_name = agent_name.clone();
            let bind_token = Arc::clone(&bind_token);
            let client = client.clone();
            registration_task = Some(tokio::spawn(async move {
                let pid = std::process::id();
                loop {
                    match client
                        .post(format!("{url}/agents/{agent_name}/wrapper"))
                        .json(&serde_json::json!({ "pid": pid }))
                        .send()
                        .await
                    {
                        Ok(r) if r.status().is_success() => {
                            // The daemon returns the binding's ownership
                            // token; only its holder may unbind later.
                            if let Ok(v) = r.json::<Value>().await {
                                if let Some(tok) = v.get("wakeId").and_then(Value::as_str) {
                                    *bind_token.lock().unwrap() = Some(tok.to_string());
                                }
                            }
                            return;
                        }
                        Ok(_) => {}
                        Err(_) => {}
                    }
                    tokio::time::sleep(RETRY).await;
                }
            }));
        }
    }

    {
        let url = url.clone();
        let writer = Arc::clone(&writer);
        let submit = newline.to_vec();
        let target = poll_target;
        tokio::spawn(async move { poll_loop(&url, target, &writer, &submit).await });
    }

    let mut sigterm = signal(SignalKind::terminate()).unwrap_or_else(|e| {
        eprintln!("{program}: signal setup failed: {e}");
        std::process::exit(1);
    });
    let mut sigint = signal(SignalKind::interrupt()).unwrap_or_else(|e| {
        eprintln!("{program}: signal setup failed: {e}");
        std::process::exit(1);
    });
    let mut winch = signal(SignalKind::window_change()).ok();

    let exit_code: u8 = 'main: loop {
        tokio::select! {
            _ = sigterm.recv() => {
                let _ = child.kill();
                break 'main 143;
            }
            _ = sigint.recv() => {
                let _ = child.kill();
                break 'main 130;
            }
            _ = async {
                if let Some(w) = winch.as_mut() {
                    w.recv().await;
                } else {
                    std::future::pending::<()>().await;
                }
            } => {
                if let Some((rows, cols)) = terminal_size() {
                    let _ = master.resize(PtySize { rows, cols, pixel_width: 0, pixel_height: 0 });
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(150)) => {
                match child.try_wait() {
                    Ok(Some(status)) => break 'main status.exit_code().min(255) as u8,
                    Ok(None) => {}
                    Err(_) => break 'main 1,
                }
            }
        }
    };

    if let Some(wake_id) = wake_id_for_cleanup {
        let _ = client.delete(format!("{url}/wake/{wake_id}")).send().await;
    } else if let Some(agent_name) = agent_name_for_cleanup {
        // Stop the registration retry loop first so it cannot bind after we
        // have decided there is nothing to unbind, then unbind with the
        // ownership token (a no-op when bind never succeeded).
        if let Some(task) = registration_task.take() {
            task.abort();
            let _ = task.await;
        }
        let token = bind_token.lock().unwrap().take();
        if let Some(token) = token {
            let _ = client
                .delete(format!("{url}/agents/{agent_name}/wrapper"))
                .json(&serde_json::json!({ "wakeId": token }))
                .send()
                .await;
        }
    }
    if let Some(pid) = child.process_id() {
        unsafe {
            libc::kill(-(pid as i32), libc::SIGTERM);
        }
    }
    let _ = child.kill();
    restore_raw(saved_termios);

    ExitCode::from(exit_code)
}

async fn poll_loop(
    url: &str,
    target: PollTarget,
    writer: &Mutex<Box<dyn Write + Send>>,
    submit: &[u8],
) {
    let client = reqwest::Client::new();
    let mut last: Option<(String, Instant)> = None;
    loop {
        let peek_url = match &target {
            PollTarget::Agent(agent) => format!("{url}/nudge/{agent}?wait_ms={POLL_WAIT_MS}"),
            PollTarget::Wake(wake_id) => format!("{url}/wake/{wake_id}?wait_ms={POLL_WAIT_MS}"),
        };
        let resp = match client
            .get(&peek_url)
            .timeout(Duration::from_secs(POLL_WAIT_MS as u64 + 5))
            .send()
            .await
        {
            Ok(r) => r,
            Err(_) => {
                tokio::time::sleep(RETRY).await;
                continue;
            }
        };
        if resp.status() != reqwest::StatusCode::OK {
            tokio::time::sleep(RETRY).await;
            continue;
        }
        let v: Value = match resp.json().await {
            Ok(v) => v,
            Err(_) => {
                tokio::time::sleep(RETRY).await;
                continue;
            }
        };
        if matches!(&target, PollTarget::Wake(_))
            && v.get("bound").and_then(Value::as_bool) != Some(true)
        {
            continue;
        }
        if v.get("pending").and_then(Value::as_bool) != Some(true) {
            continue;
        }
        let ids: Vec<String> = v
            .get("ids")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(|x| x.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();
        let key = ids.join(",");
        let now = Instant::now();
        let should_inject = match &last {
            Some((prev, at)) => *prev != key || now.duration_since(*at) >= COOLDOWN,
            None => true,
        };
        if should_inject {
            let pointer = v
                .get("pointer")
                .and_then(Value::as_str)
                .unwrap_or(crate::nudge::POINTER_TEXT);
            // Text and submit are separate writes: TUIs like OpenCode bind
            // Enter (CR) as submit; a trailing LF in the same write is treated
            // as a literal newline in the input field.
            let injected = if let Ok(mut w) = writer.lock() {
                w.write_all(pointer.as_bytes()).is_ok()
                    && w.flush().is_ok()
                    && inject_submit(&mut w, submit).is_ok()
            } else {
                false
            };
            if injected {
                last = Some((key, now));
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

fn inject_submit(w: &mut Box<dyn Write + Send>, submit: &[u8]) -> std::io::Result<()> {
    w.write_all(submit)?;
    w.flush()
}

/// Map `--newline` (submit key) to PTY bytes.
pub fn parse_submit_sequence(name: &str) -> Result<&'static [u8], String> {
    match name {
        "cr" | "return" | "enter" => Ok(b"\r"),
        "lf" | "newline" => Ok(b"\n"),
        "crlf" => Ok(b"\r\n"),
        "ctrl-enter" | "ctrl_return" => Ok(b"\x1b[13;5u"),
        other => Err(format!(
            "--newline must be cr, lf, crlf, or ctrl-enter (got {other:?})"
        )),
    }
}

fn forward_reader(reader: &mut Box<dyn Read + Send>) {
    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    let mut buf = [0u8; 8192];
    loop {
        match reader.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => {
                if out.write_all(&buf[..n]).is_err() {
                    break;
                }
                let _ = out.flush();
            }
            Err(_) => break,
        }
    }
}

fn forward_stdin(writer: &Mutex<Box<dyn Write + Send>>) {
    let mut buf = [0u8; 4096];
    loop {
        let n = unsafe { libc::read(libc::STDIN_FILENO, buf.as_mut_ptr() as *mut _, buf.len()) };
        if n <= 0 {
            break;
        }
        if let Ok(mut w) = writer.lock() {
            if w.write_all(&buf[..n as usize]).is_err() {
                break;
            }
            let _ = w.flush();
        }
    }
}

#[cfg(unix)]
fn set_raw() -> Option<libc::termios> {
    unsafe {
        let mut orig: libc::termios = std::mem::zeroed();
        if libc::tcgetattr(libc::STDIN_FILENO, &mut orig) != 0 {
            return None;
        }
        let mut raw = orig;
        libc::cfmakeraw(&mut raw);
        if libc::tcsetattr(libc::STDIN_FILENO, libc::TCSANOW, &raw) != 0 {
            return None;
        }
        Some(orig)
    }
}

#[cfg(unix)]
fn restore_raw(orig: Option<libc::termios>) {
    if let Some(orig) = orig {
        unsafe {
            libc::tcsetattr(libc::STDIN_FILENO, libc::TCSANOW, &orig);
        }
    }
}

#[cfg(unix)]
fn terminal_size() -> Option<(u16, u16)> {
    unsafe {
        let mut ws: libc::winsize = std::mem::zeroed();
        if libc::ioctl(libc::STDIN_FILENO, libc::TIOCGWINSZ, &mut ws) == 0 && ws.ws_col > 0 {
            Some((ws.ws_row, ws.ws_col))
        } else {
            None
        }
    }
}
