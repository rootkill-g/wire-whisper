//! wire-whisper client binary
//!
//! Connects to the server, performs the `Hello` handshake, then drops into
//! an async REPL that multiplexes stdin and the network in a single
//! `select!` loop.

#![forbid(unsafe_code)]

mod repl;

use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use clap::Parser;
use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_util::codec::Framed;

use whisper_protocol::{ClientFrame, ClientSideCodec, PROTOCOL_VERSION, ServerFrame};

#[derive(Parser, Debug)]
#[command(
    name = "whisper-client",
    about = "an async cli client for wire-whisper",
    version
)]
struct Args {
    /// Server host. May also be set via `WIRE_WHISPER_HOST`.
    #[arg(long, env = "WIRE_WHISPER_HOST", default_value = "127.0.0.1")]
    host: String,

    /// Server port. May also be set via `WIRE_WHISPER_PORT`.
    #[arg(long, env = "WIRE_WHISPER_PORT", default_value_t = 7878)]
    port: u16,

    /// Username to identify as on the server. May also be set via
    /// `WIRE_WHISPER_USERNAME`.
    #[arg(long, env = "WIRE_WHISPER_USERNAME")]
    username: String,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    init_tracing();
    let args = Args::parse();

    let addr = format!("{}:{}", args.host, args.port);
    let stream = timeout(Duration::from_secs(10), TcpStream::connect(&addr))
        .await
        .context("connect timed out")?
        .with_context(|| format!("connecting to {addr}"))?;
    stream.set_nodelay(true)?;

    let mut socket = Framed::new(stream, ClientSideCodec::new());

    socket
        .send(ClientFrame::Hello {
            version: PROTOCOL_VERSION,
            username: args.username.clone(),
        })
        .await
        .context("sending Hello")?;

    let first = socket
        .next()
        .await
        .ok_or_else(|| anyhow!("server closed connection before responding to Hello"))?
        .context("decoding handshake response")?;
    // `Welcome` is server-side content (same category as Joined/Message),
    // so it belongs on stdout — keep it consistent with how the REPL
    // renders the rest of the room events. Diagnostics (connect failures,
    // rejection, etc.) stay on stderr via `bail!`/`anyhow`.
    match first {
        ServerFrame::Welcome { motd, occupancy } => {
            println!("[server] {motd} (room occupancy: {occupancy})");
        }
        ServerFrame::Rejected { reason } => {
            bail!("server rejected handshake: {reason}");
        }
        other => bail!("unexpected first frame from server: {other:?}"),
    }

    repl::run(socket, &args.username).await
}

fn init_tracing() {
    use tracing_subscriber::EnvFilter;
    // Default to warn so the REPL's stdout isn't polluted by info logs.
    let filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new("warn"))
        .expect("'warn' is a valid EnvFilter directive");
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .init();
}
