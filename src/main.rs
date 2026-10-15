//! Process entry: mailbox daemon or TUI harness launcher.

use std::io::IsTerminal;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tokio::sync::Notify;

use clap::Parser;
use mentischat::bus::Bus;
use mentischat::tui::{self, LogBuffer};
use mentischat::wake::WakeConfig;
use mentischat::BIND_ADDR;

/// Set when graceful shutdown could not wipe the mailbox on disk.
static SHUTDOWN_WIPE_FAILED: AtomicBool = AtomicBool::new(false);

#[derive(Parser)]
#[command(
    name = "mentischat",
    about = "Local mailbox daemon and TUI harness launcher",
    version,
    after_help = "\
Examples:
  mentischat              Start the mailbox daemon
  mentischat serve        Start the mailbox daemon
  mentischat opencode     Run OpenCode with idle wake
  mentischat grok         Run Grok Build with idle wake
  mentischat claude       Run Claude Code with idle wake

The agent picks its roster name on first register; pass MENTISCHAT_WAKE_ID as wakeId."
)]
struct Cli {
    /// Run the mailbox daemon instead of a harness.
    #[arg(long, short = 's', conflicts_with = "harness")]
    serve: bool,

    /// TUI harness to run (alternative to a positional name).
    #[arg(long = "harness", conflicts_with = "serve")]
    harness: Option<String>,

    /// Pin roster name up front (legacy; omit for auto-bind).
    #[arg(long)]
    agent: Option<String>,

    /// Daemon base URL (harness mode).
    #[arg(long, default_value = "http://127.0.0.1:9480")]
    url: String,

    /// Skip wrapper bind (legacy --agent mode only).
    #[arg(long)]
    no_register: bool,

    /// Submit key after the pointer: cr (Enter, default), lf, crlf, or
    /// ctrl-enter (OpenCode when input_submit is ctrl+return).
    #[arg(long, default_value = "cr")]
    newline: String,

    /// Harness command and args (e.g. `opencode` or `claude -- model opus`).
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    rest: Vec<String>,
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    // Harness-only flags in daemon mode are a misconfiguration, not a no-op:
    // say so rather than starting a daemon that ignores half its arguments.
    let command = harness_command(&cli);
    if command.is_none() && (cli.agent.is_some() || cli.no_register) {
        eprintln!(
            "mentischat: --agent and --no-register apply to harness mode only \
             (e.g. `mentischat opencode`); refusing to start the daemon with them"
        );
        return ExitCode::from(2);
    }
    if let Some(command) = command {
        return mentischat::wake::run_harness(WakeConfig {
            program: "mentischat",
            agent: cli.agent,
            url: cli.url,
            no_register: cli.no_register,
            newline: cli.newline,
            command,
        })
        .await;
    }
    run_daemon().await
}

/// Resolve harness argv, or `None` to start the daemon.
fn harness_command(cli: &Cli) -> Option<Vec<String>> {
    if cli.serve {
        return None;
    }
    if let Some(name) = &cli.harness {
        let mut command = vec![name.clone()];
        command.extend(cli.rest.clone());
        return Some(command);
    }
    if cli.rest.is_empty() {
        return None;
    }
    match cli.rest[0].as_str() {
        "serve" | "daemon" => {
            if cli.rest.len() > 1 {
                eprintln!("mentischat: unexpected arguments after `{}`", cli.rest[0]);
                std::process::exit(2);
            }
            None
        }
        _ => Some(cli.rest.clone()),
    }
}

/// The live view needs a terminal on both ends; under launchd, a pipe, or with
/// `MENTISCHAT_PLAIN` set, the daemon logs plain lines instead.
fn wants_terminal_view() -> bool {
    std::env::var_os("MENTISCHAT_PLAIN").is_none()
        && std::io::stdin().is_terminal()
        && std::io::stdout().is_terminal()
}

async fn run_daemon() -> ExitCode {
    let use_view = wants_terminal_view();
    let logs = LogBuffer::new();
    if use_view {
        tracing_subscriber::fmt()
            .with_ansi(false)
            .with_target(false)
            .without_time()
            .compact()
            .with_writer(logs.clone())
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_target(false)
            .compact()
            .init();
    }

    let data_dir = match std::env::var("MENTISCHAT_DIR") {
        Ok(dir) if !dir.is_empty() => PathBuf::from(dir),
        _ => match std::env::var("HOME") {
            Ok(home) => PathBuf::from(home).join(".cloudllm").join("mentischat"),
            Err(_) => {
                eprintln!("mentischat: HOME is not set; cannot locate data dir");
                return ExitCode::FAILURE;
            }
        },
    };

    let bus = Arc::new(match Bus::new(&data_dir) {
        Ok(bus) => bus,
        Err(e) => {
            eprintln!("mentischat: failed to open {}: {e}", data_dir.display());
            return ExitCode::FAILURE;
        }
    });

    let listener = match tokio::net::TcpListener::bind(BIND_ADDR).await {
        Ok(l) => l,
        Err(e) if e.kind() == std::io::ErrorKind::AddrInUse => {
            eprintln!(
                "mentischat: {} already in use; another instance owns it",
                BIND_ADDR
            );
            return ExitCode::FAILURE;
        }
        Err(e) => {
            eprintln!("mentischat: bind {BIND_ADDR} failed: {e}");
            return ExitCode::FAILURE;
        }
    };

    let app = mentischat::build_router(bus.clone());
    let shutdown_bus = bus.clone();
    let quit = Arc::new(Notify::new());

    tracing::info!(
        addr = %BIND_ADDR,
        data = %data_dir.display(),
        "serving MCP at /mcp and REST"
    );

    let running = Arc::new(AtomicBool::new(true));
    let view = use_view.then(|| {
        let (bus, logs, running, quit) = (bus.clone(), logs, running.clone(), quit.clone());
        let handle = tokio::runtime::Handle::current();
        let info = tui::Info {
            version: env!("CARGO_PKG_VERSION").into(),
            url: format!("http://{BIND_ADDR}"),
            data_path: data_dir.display().to_string(),
        };
        // The view runs on its own OS thread, so it may block on the bus.
        std::thread::spawn(move || {
            tui::run_guarded(
                || {
                    tui::run(
                        || handle.block_on(bus.overview()).ok(),
                        &logs,
                        info,
                        &running,
                    )
                },
                &quit,
            )
        })
    });

    let served = axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown_signal(shutdown_bus, quit))
    .await;
    running.store(false, Ordering::Relaxed);
    if let Some(view) = view {
        match view.join() {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => eprintln!("mentischat: terminal view failed: {e}"),
            Err(_) => eprintln!("mentischat: terminal view thread panicked"),
        }
    }
    if let Err(e) = served {
        eprintln!("mentischat: server error: {e}");
        return ExitCode::FAILURE;
    }
    if SHUTDOWN_WIPE_FAILED.load(Ordering::SeqCst) {
        eprintln!("mentischat: shutdown could not discard mailbox on disk");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}

async fn shutdown_signal(bus: Arc<Bus>, quit: Arc<Notify>) {
    use tokio::signal::unix::{signal, SignalKind};
    let terminate = async {
        match signal(SignalKind::terminate()) {
            Ok(mut sig) => {
                sig.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = terminate => {}
        _ = quit.notified() => {}
    }
    tracing::info!("shutting down");
    if let Err(e) = bus.shutdown().await {
        tracing::error!(error = %e, "mailbox wipe failed during shutdown");
        SHUTDOWN_WIPE_FAILED.store(true, Ordering::SeqCst);
    }
}
