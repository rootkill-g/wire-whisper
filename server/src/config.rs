//! Tunable server-side configuration.
//!
//! Shared across all sessions via `Arc<ServerConfig>` so the binary can
//! set one set of timings while tests dial them down for fast feedback.
//!
//! Behavioural knobs only — actor types and primitives live under
//! [`crate::actor`].

use std::time::Duration;

/// Tunable knobs for a running server.
///
/// # Invariants
///
/// All `Duration` fields **must be strictly positive** — `tokio::time::interval`
/// panics on `Duration::ZERO`, and the rest of the timings would behave
/// pathologically at zero (idle disconnect every iteration, etc.). All
/// `f64` rate-limit fields **must be finite and non-negative** to avoid
/// NaN propagating through the token-bucket math.
///
/// [`ServerConfig::default()`] satisfies all invariants. Custom
/// constructions are the caller's responsibility — the struct is `pub`
/// with `pub` fields for ergonomics; we trust the caller to keep the
/// invariants rather than adding a validation layer that would never
/// fire in practice.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// How often the server sends a [`Ping`] to each connected client.
    ///
    /// **Must be `> Duration::ZERO`** (tokio's `interval` panics on zero).
    /// Pings are unconditional (sent even when traffic is active); the
    /// cost is negligible and it keeps NAT mappings alive on idle
    /// connections.
    ///
    /// [`Ping`]: whisper_protocol::ServerFrame::Ping
    pub ping_interval: Duration,

    /// A session is disconnected if it has not produced *any* inbound
    /// frame (including `Pong`) in this window. Detects half-open TCP
    /// connections that would otherwise sit in the registry forever
    /// holding a username hostage.
    ///
    /// **Must be `> Duration::ZERO`** to be useful (a zero idle timeout
    /// disconnects every session on the first heartbeat tick).
    pub idle_timeout: Duration,

    /// `Hello`-handshake budget. Clients that don't `Hello` within this
    /// window are dropped.
    ///
    /// **Must be `> Duration::ZERO`**.
    pub hello_timeout: Duration,

    /// Best-effort budget for delivering the parting "server shutting
    /// down" frame to each session during a graceful shutdown. A wedged
    /// peer cannot delay the rest of the drain past this.
    ///
    /// **Must be `> Duration::ZERO`**.
    pub shutdown_goodbye_timeout: Duration,

    /// Token-bucket capacity for `Send` frames.
    ///
    /// **Must be finite and `>= 0.0`**.
    pub rate_limit_burst: f64,

    /// Token-bucket refill rate, tokens per second.
    ///
    /// **Must be finite and `>= 0.0`**. A value of `0.0` permits exactly
    /// `rate_limit_burst` sends per session lifetime (no refill).
    pub rate_limit_refill_per_sec: f64,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            ping_interval: Duration::from_secs(30),
            idle_timeout: Duration::from_secs(60),
            hello_timeout: Duration::from_secs(10),
            shutdown_goodbye_timeout: Duration::from_millis(500),
            rate_limit_burst: 10.0,
            rate_limit_refill_per_sec: 2.0,
        }
    }
}
