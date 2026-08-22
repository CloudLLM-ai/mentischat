//! Process entry: bind the single loopback listener, serve, shut down clean.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

use llm_chat::bus::Bus;

const BIND_ADDR: &str = "127.0.0.1:9480";

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_target(false)
        .compact()
        .init();

    let data_dir = match std::env::var("HOME") {
        Ok(home) => PathBuf::from(home).join(".llm_chat"),
        Err(_) => {
            eprintln!("llm_chat: HOME is not set; cannot locate data dir");
            return ExitCode::FAILURE;
        }
    };

    let bus = Arc::new(match Bus::new(&data_dir) {
        Ok(bus) => bus,
        Err(e) => {
            eprintln!("llm_chat: failed to open {}: {e}", data_dir.display());
            return ExitCode::FAILURE;
        }
    });

    let listener = match tokio::net::TcpListener::bind(BIND_ADDR).await {
        Ok(l) => l,
        Err(e) if e.kind() == std::io::ErrorKind::AddrInUse => {
            // Single-instance lock: a healthy llm_chat owns 9480. Never steal.
            eprintln!(
                "llm_chat: {} already in use; another instance owns it",
                BIND_ADDR
            );
            return ExitCode::FAILURE;
        }
        Err(e) => {
            eprintln!("llm_chat: bind {BIND_ADDR} failed: {e}");
            return ExitCode::FAILURE;
        }
    };

    let app = llm_chat::build_router(bus.clone());
    let shutdown_bus = bus.clone();

    println!(
        "llm_chat: serving http://{BIND_ADDR}/mcp and REST on {}",
        listener
            .local_addr()
            .map(|a| a.to_string())
            .unwrap_or_default()
    );

    let serve = axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown_signal(shutdown_bus));

    if let Err(e) = serve.await {
        eprintln!("llm_chat: server error: {e}");
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
    bus.shutdown().await;
}
