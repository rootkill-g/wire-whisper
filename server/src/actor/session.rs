//! The [`Session`] actor — one per connected client.
//!
//! See [`super`] (the `actor` module's docs) for the model overview.
//!
//! # Lifecycle
//!
//! 1. `handshake()`: read `Hello`, check protocol version, validate
//!    username, register with the [`Hub`] (atomic claim + bounded
//!    mailbox allocation), send `Welcome` (or `Rejected` and close).
//! 2. `serve_loop`: `select!` between:
//!    - **shutdown** (highest priority — server-wide graceful stop),
//!    - **socket reads** (client frames; updates the activity timestamp),
//!    - **mailbox receives** (pre-encoded outgoing bytes from the Hub),
//!    - **heartbeat tick** (idle-timeout check, then `Ping`).
//! 3. Broadcast `Departed`, drop the claim. The post-loop cleanup runs
//!    even when `serve_loop` errors out, so a crashed peer never leaks
//!    a username or a "joined" event without a matching "departed".
//!
//! The actor owns:
//! - a [`FramedRead<OwnedReadHalf, ServerSideCodec>`] (codec-driven reads),
//! - a raw [`OwnedWriteHalf`] (so the broadcast hot path can write
//!   pre-encoded bytes from the mailbox straight to the wire — no
//!   re-encoding, no `String` clones),
//! - its private [`RateLimiter`] state,
//! - shared [`Arc<ServerConfig>`] (read-only).

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use bytes::Bytes;
use futures::StreamExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::time::{Instant, MissedTickBehavior, timeout};
use tokio_util::codec::FramedRead;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use whisper_protocol::{
    ClientFrame, PROTOCOL_VERSION, ServerFrame, ServerSideCodec, UserName, encode_frame,
};

use super::hub::{ClaimHandle, Hub};
use super::mailbox::Mailbox;
use super::ratelimit::RateLimiter;
use crate::config::ServerConfig;

/// A single client session actor, ready to be `.run()`.
pub struct Session {
    hub: Hub,
    reader: FramedRead<OwnedReadHalf, ServerSideCodec>,
    writer: OwnedWriteHalf,
    peer: SocketAddr,
    shutdown: CancellationToken,
    config: Arc<ServerConfig>,
}

impl Session {
    /// Build a session that will operate over `stream`.
    ///
    /// `shutdown` is the server-wide cancellation handle; on cancel the
    /// session sends a parting `Error` frame and exits cleanly. `config`
    /// supplies heartbeat and rate-limit timings — shared across all
    /// sessions in a single server.
    pub fn new(
        hub: Hub,
        stream: TcpStream,
        peer: SocketAddr,
        shutdown: CancellationToken,
        config: Arc<ServerConfig>,
    ) -> Self {
        let (rd, wr) = stream.into_split();
        let reader = FramedRead::new(rd, ServerSideCodec::new());
        Self {
            hub,
            reader,
            writer: wr,
            peer,
            shutdown,
            config,
        }
    }

    /// Drive the session to completion.
    pub async fn run(mut self) -> Result<()> {
        let (claim, mut rx) = self.handshake().await?;
        let username = claim.username().clone();

        info!(peer = %self.peer, user = %username, "user joined");
        // Pre-encode once; every recipient gets a `Bytes::clone`
        // (refcount bump only).
        let joined = encode_frame(&ServerFrame::Joined {
            username: username.to_string(),
        })
        .context("encoding Joined")?;
        self.hub.broadcast(&username, joined);

        let result = self.serve_loop(&username, &mut rx).await;

        // Always announce departure, even if `serve_loop` errored.
        match encode_frame(&ServerFrame::Departed {
            username: username.to_string(),
        }) {
            Ok(bytes) => self.hub.broadcast(&username, bytes),
            Err(e) => warn!(error = ?e, "failed to encode Departed; skipping broadcast"),
        }
        info!(peer = %self.peer, user = %username, "user left");

        // `claim` drops here, releasing the username + decrementing
        // occupancy; `rx` drops, closing the actor's mailbox.
        drop(claim);
        result
    }

    async fn handshake(&mut self) -> Result<(ClaimHandle, Mailbox<Bytes>)> {
        // Race the Hello read against the shutdown token so a stalling
        // peer cannot delay a server-wide shutdown.
        let first = tokio::select! {
            biased;
            _ = self.shutdown.cancelled() => {
                bail!("shutdown signalled during handshake");
            }
            frame = timeout(self.config.hello_timeout, self.reader.next()) => {
                frame
                    .context("client did not send Hello within timeout")?
                    .ok_or_else(|| anyhow!("client disconnected before Hello"))?
                    .context("malformed Hello frame")?
            }
        };

        let (version, raw_username) = match first {
            ClientFrame::Hello { version, username } => (version, username),
            other => {
                self.send_frame(ServerFrame::Rejected {
                    reason: "first frame must be Hello".into(),
                })
                .await?;
                bail!("client sent {:?} as first frame", other);
            }
        };

        if version != PROTOCOL_VERSION {
            self.send_frame(ServerFrame::Rejected {
                reason: format!(
                    "protocol version mismatch: server={PROTOCOL_VERSION}, client={version}"
                ),
            })
            .await?;
            bail!("protocol version mismatch: client={version}");
        }

        let username = match UserName::new(&raw_username) {
            Ok(u) => u,
            Err(e) => {
                self.send_frame(ServerFrame::Rejected {
                    reason: e.to_string(),
                })
                .await?;
                bail!("invalid username '{raw_username}': {e}");
            }
        };

        let username_arc = username.into_arc();
        let (claim, rx) = match self.hub.try_register(username_arc.clone()) {
            Some(pair) => pair,
            None => {
                self.send_frame(ServerFrame::Rejected {
                    reason: format!("username '{raw_username}' is already taken"),
                })
                .await?;
                bail!("username collision: {raw_username}");
            }
        };

        let occupancy = u32::try_from(self.hub.occupancy()).unwrap_or(u32::MAX);
        self.send_frame(ServerFrame::Welcome {
            motd: format!("welcome, {raw_username}."),
            occupancy,
        })
        .await?;
        Ok((claim, rx))
    }

    async fn serve_loop(&mut self, me: &Arc<str>, rx: &mut Mailbox<Bytes>) -> Result<()> {
        let mut ping = tokio::time::interval(self.config.ping_interval);
        ping.set_missed_tick_behavior(MissedTickBehavior::Delay);
        // The first tick fires immediately — consume it so the loop
        // doesn't ping a brand-new session right away.
        ping.tick().await;

        let mut last_activity = Instant::now();
        let mut rate_limiter = RateLimiter::new(
            self.config.rate_limit_burst,
            self.config.rate_limit_refill_per_sec,
        );
        let idle_timeout = self.config.idle_timeout;
        let shutdown_goodbye = self.config.shutdown_goodbye_timeout;

        loop {
            tokio::select! {
                // `biased`: shutdown over everything else; then the
                // socket (so `Leave` is honoured promptly); then the
                // mailbox; then the heartbeat (lowest priority — it's
                // a timer, it can wait).
                biased;

                _ = self.shutdown.cancelled() => {
                    let _ = timeout(
                        shutdown_goodbye,
                        self.send_frame(ServerFrame::Error {
                            reason: "server shutting down".into(),
                        }),
                    ).await;
                    break;
                }

                from_client = self.reader.next() => {
                    let Some(frame) = from_client else { break };
                    let frame = frame.context("decoding client frame")?;
                    last_activity = Instant::now();
                    match frame {
                        ClientFrame::Send { body } => {
                            // Validation: empty / oversize bodies get a
                            // structured `Error` rather than disconnect.
                            if let Err(e) = whisper_protocol::validate_body_bytes(&body) {
                                self.send_frame(ServerFrame::Error {
                                    reason: e.to_string(),
                                }).await?;
                                continue;
                            }
                            if !rate_limiter.try_consume(1.0) {
                                self.send_frame(ServerFrame::Error {
                                    reason: "rate limit exceeded; slow down".into(),
                                }).await?;
                                continue;
                            }
                            // Encode once, broadcast many.
                            let payload = encode_frame(&ServerFrame::Message {
                                from: me.to_string(),
                                body,
                            }).context("encoding Message")?;
                            self.hub.broadcast(me, payload);
                        }
                        ClientFrame::Pong => {
                            // Activity timestamp already updated above.
                        }
                        ClientFrame::Leave => break,
                        ClientFrame::Hello { .. } => {
                            // Hello after a successful handshake is a
                            // protocol violation — disconnect rather
                            // than accommodate misbehaving peers.
                            let _ = self.send_frame(ServerFrame::Error {
                                reason: "protocol violation: Hello after handshake".into(),
                            }).await;
                            bail!("protocol violation: second Hello from peer");
                        }
                    }
                }

                from_mailbox = rx.recv() => {
                    let Some(bytes) = from_mailbox else { break };
                    // `OwnedWriteHalf` has no user-space buffer; this
                    // hits the kernel send buffer directly. With
                    // `TCP_NODELAY` set, small frames go on the wire
                    // promptly.
                    self.writer
                        .write_all(&bytes)
                        .await
                        .context("writing broadcast frame to peer")?;
                }

                _ = ping.tick() => {
                    if last_activity.elapsed() > idle_timeout {
                        let _ = self.send_frame(ServerFrame::Error {
                            reason: "idle timeout".into(),
                        }).await;
                        warn!(user = %me, "idle timeout; disconnecting");
                        break;
                    }
                    self.send_frame(ServerFrame::Ping).await?;
                }
            }
        }
        Ok(())
    }

    async fn send_frame(&mut self, frame: ServerFrame) -> Result<()> {
        let bytes = encode_frame(&frame).context("encoding server frame")?;
        self.writer
            .write_all(&bytes)
            .await
            .context("writing to peer")?;
        Ok(())
    }
}
