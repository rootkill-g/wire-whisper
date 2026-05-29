//! wire-whisper server binary
//!
//! A paper-thin shim around `whisper_server::serve`. All the engineering is in
//! the library; this just wires up CLI parsing, tracing, signal handling
//! (SIGINT and SIGTERM on Unix), and graceful shutdown with a bounded drain.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use tokio::net::TcpListener;
use tracing::{error, info, warn};

use whisper_server::{CancellationToken, Hub, SESSION_QUEUE_DEPTH, ServerConfig};

const BANNER: &str = r#"
    ╔══════════════════════════════════════════╗
    ║   ✦  wire-whisper broadcast server  ✦    ║
    ╚══════════════════════════════════════════╝
"#;

/// Upper bound on the graceful drain after a shutdown signal. Sessions
/// still active after this deadline are terminated by runtime drop.
const GRACEFUL_DRAIN_BUDGET: Duration = Duration::from_secs(5);

#[derive(Parser, Debug)]
#[command(
    name = "whisper-server",
    about = "the wire-whisper broadcast server",
    version
)]
struct Args {
    /// Address to bind to. May also be set via `SIMPLE_CHAT_BIND`.
    #[arg(long, env = "WIRE_WHISPER_BIND", default_value = "0.0.0.0:7878")]
    bind: SocketAddr,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    init_tracing();
    let args = Args::parse();

    let config = Arc::new(ServerConfig::default());

    eprintln!("{BANNER}");
    eprintln!("    binding       : {}", args.bind);
    eprintln!("    session queue : {SESSION_QUEUE_DEPTH}");
    eprintln!(
        "    heartbeat     : ping every {}s, idle timeout {}s",
        config.ping_interval.as_secs(),
        config.idle_timeout.as_secs(),
    );
    eprintln!(
        "    rate limit    : {:.0} burst, {:.0}/s sustained",
        config.rate_limit_burst, config.rate_limit_refill_per_sec,
    );
    eprintln!();

    let listener = TcpListener::bind(args.bind).await?;
    info!(addr = %args.bind, "listening");

    let hub = Hub::new();
    let shutdown = CancellationToken::new();
    let server_task = tokio::spawn(whisper_server::serve(
        listener,
        hub,
        shutdown.clone(),
        config,
    ));

    wait_for_signal().await;
    info!("graceful shutdown beginning; clients will be notified");
    shutdown.cancel();

    match tokio::time::timeout(GRACEFUL_DRAIN_BUDGET, server_task).await {
        Ok(Ok(Ok(()))) => {
            info!("graceful shutdown complete");
            Ok(())
        }
        Ok(Ok(Err(e))) => {
            error!(error = %e, "server returned an error during shutdown");
            Err(e.into())
        }
        Ok(Err(join_err)) => {
            error!(error = %join_err, "server task ended unexpectedly");
            Err(anyhow::anyhow!("server task ended: {join_err}"))
        }
        Err(_elapsed) => {
            warn!(
                budget_secs = GRACEFUL_DRAIN_BUDGET.as_secs(),
                "graceful drain timed out; sessions may have been cut short"
            );
            Ok(())
        }
    }
}

/// Resolve when the process receives any of the platform's shutdown
/// signals. On Unix that is SIGINT *or* SIGTERM (Docker, K8s, systemd all
/// send SIGTERM, not SIGINT). On other platforms we fall back to Ctrl-C.
#[cfg(unix)]
async fn wait_for_signal() {
    use tokio::signal::unix::{SignalKind, signal};
    let mut sigterm = signal(SignalKind::terminate()).expect("install SIGTERM handler");
    let mut sigint = signal(SignalKind::interrupt()).expect("install SIGINT handler");
    tokio::select! {
        _ = sigterm.recv() => info!(signal = "SIGTERM", "shutdown signal received"),
        _ = sigint.recv()  => info!(signal = "SIGINT",  "shutdown signal received"),
    }
}

#[cfg(not(unix))]
async fn wait_for_signal() {
    if tokio::signal::ctrl_c().await.is_ok() {
        info!(signal = "ctrl-c", "shutdown signal received");
    }
}

fn init_tracing() {
    use tracing_subscriber::EnvFilter;
    // `EnvFilter::try_new("info")` parses a literal we control, so the
    // fallback is infallible — `.expect` documents that invariant.
    let filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new("info"))
        .expect("'info' is a valid EnvFilter directive");
    tracing_subscriber::fmt().with_env_filter(filter).init();
}
