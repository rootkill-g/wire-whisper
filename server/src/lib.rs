//! Whisper Server
//!
//! Library entry point. The `main.rs` is a paper-thin shim around [`serve`].
//! Exposing the guts as a library lets integration tests drive the real
//! server in-process — no subprocess gymnastics, no port-scraping shenanigans.
//!
//! # Module map
//!
//! - [`config`] — `ServerConfig`: tunable timings, rate limits, mailbox depth.
//! - [`actor`]  — the actor model formalised: [`Session`] and [`Hub`]
//!   actors, [`actor::Mailbox`] / [`actor::Address`] aliases, the
//!   [`actor::RateLimiter`] used by each session.
//! - This file — the orchestration entry point ([`serve`]) plus its
//!   shutdown contract.
//!
//! # Shutdown contract
//!
//! [`serve`] takes a [`CancellationToken`]. Cancelling that token:
//!
//! 1. Breaks the accept loop (no new connections).
//! 2. Propagates to every in-flight [`Session`] actor (each holds a child token).
//! 3. Awaits all session tasks before returning.
//!
//! Each session, on cancellation, makes a best-effort attempt to send a
//! parting `ServerFrame::Error` so clients learn the reason for the
//! disconnect, then publishes its `Departed` and releases its username.
//! Net effect: clean shutdown with no orphaned usernames and no clients
//! left guessing why they got an RST.
//!
//! # Performance shape
//!
//! - **Username registry**: `papaya::HashMap` — lock-free reads, lock-free
//!   writes. No shard locks.
//! - **Occupancy**: `AtomicUsize` — wait-free O(1) reads.
//! - **Fan-out**: per-session bounded [`actor::Mailbox`]. Slow clients
//!   drop frames *for themselves only*; the rest of the room is untouched.
//! - **Encoding**: every broadcast payload is produced once via
//!   [`chat_protocol::encode_frame`] into a [`bytes::Bytes`]; each
//!   recipient clones the `Bytes` (refcount bump) and writes the same
//!   buffer to its socket. Zero re-encoding, zero `String` clones on the
//!   broadcast path.
//! - **Sender-self filter**: [`Arc::ptr_eq`] — one pointer compare, no
//!   string equality.
//!
//! [`Arc::ptr_eq`]: std::sync::Arc::ptr_eq

#![forbid(unsafe_code)]
#![deny(missing_docs)]

pub mod actor;
pub mod config;

pub use actor::{ClaimHandle, Hub, SESSION_QUEUE_DEPTH, Session};
pub use config::ServerConfig;
pub use tokio_util::sync::CancellationToken;

use std::io;
use std::sync::Arc;

use tokio::net::TcpListener;
use tokio::task::JoinSet;
use tracing::{debug, error, info};

/// Accept connections on `listener` until `shutdown` is cancelled or the
/// accept loop hits a hard error, spawning one [`Session`] actor task
/// per connection.
///
/// Sessions inherit a child of `shutdown`, so cancelling the parent
/// notifies every in-flight actor as well as the accept loop. Before
/// returning, `serve` drains all session tasks — graceful shutdown.
///
/// `config` is shared across all sessions in this run; the binary uses
/// [`ServerConfig::default()`], integration tests dial timings down.
///
/// # Errors
/// Returns the originating [`io::Error`] if `accept()` fails irrecoverably.
/// Returns `Ok(())` on graceful shutdown.
pub async fn serve(
    listener: TcpListener,
    hub: Hub,
    shutdown: CancellationToken,
    config: Arc<ServerConfig>,
) -> io::Result<()> {
    let mut sessions: JoinSet<()> = JoinSet::new();

    let accept_result: io::Result<()> = loop {
        tokio::select! {
            // `biased`: prefer shutdown over accept so a flood of new
            // connections cannot starve a pending shutdown.
            biased;

            _ = shutdown.cancelled() => {
                info!("shutdown signalled; closing listener");
                break Ok(());
            }

            accept = listener.accept() => {
                let (stream, peer) = match accept {
                    Ok(pair) => pair,
                    Err(e) => {
                        error!(error = %e, "accept failed; bailing");
                        break Err(e);
                    }
                };
                if let Err(e) = stream.set_nodelay(true) {
                    error!(error = %e, peer = %peer, "set_nodelay failed; continuing");
                }
                let hub = hub.clone();
                let child = shutdown.child_token();
                let cfg = config.clone();
                sessions.spawn(async move {
                    let session = Session::new(hub, stream, peer, child, cfg);
                    if let Err(e) = session.run().await {
                        debug!(peer = %peer, error = ?e, "session ended with error");
                    }
                });
            }
        }
    };

    info!(active = sessions.len(), "draining in-flight sessions");
    while sessions.join_next().await.is_some() {}
    info!("all sessions drained");

    accept_result
}
