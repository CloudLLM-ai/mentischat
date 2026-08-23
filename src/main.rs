//! Process entry: bind the single loopback listener, serve, shut down clean.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

use mentischat::bus::Bus;
use mentischat::BIND_ADDR;

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_target(false)
        .compact()
        .init();

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
            // Single-instance lock: a healthy mentischat owns 9480. Never steal.
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

    tracing::info!(
        addr = %BIND_ADDR,
        data = %data_dir.display(),
        "serving MCP at /mcp and REST"
    );

    let serve = axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown_signal(shutdown_bus));

    if let Err(e) = serve.await {
        eprintln!("mentischat: server error: {e}");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}

/// Resolve on SIGINT or SIGTERM, then close the bus so parked waiters fail
/// `shutting_down` before the process exits.
async fn shutdown_signal(bus: Arc<Bus>) {
    #[cfg(unix)]
    {
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
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
    tracing::info!("shutting down");
    bus.shutdown().await;
}
