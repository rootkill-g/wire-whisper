//! A pure token-bucket rate limiter — the [`super::session::Session`]
//! actor's defence against a peer that floods the room with
//! [`ClientFrame::Send`](chat_protocol::ClientFrame::Send) requests.
//!
//! Owned exclusively by one [`super::session::Session`]: no sharing, no
//! synchronisation. The actor model's "no shared mutable state"
//! invariant holds here for free.
//!
//! The bucket's refill clock is `tokio::time::Instant`, *not*
//! `std::time::Instant` — this is the only tokio touch-point in this
//! module, and it's deliberate so the limiter can be unit-tested with
//! `tokio::time::pause()`.

use tokio::time::Instant;

/// A simple token-bucket rate limiter. Single-producer.
pub struct RateLimiter {
    tokens: f64,
    last_refill: Instant,
    capacity: f64,
    refill_per_sec: f64,
}

impl RateLimiter {
    /// Construct a limiter with the given burst capacity (`capacity`
    /// tokens to start) and steady-state refill rate.
    pub fn new(capacity: f64, refill_per_sec: f64) -> Self {
        Self {
            tokens: capacity,
            last_refill: Instant::now(),
            capacity,
            refill_per_sec,
        }
    }

    /// Attempt to consume `n` tokens. Refills first based on elapsed
    /// time, then either decrements and returns `true`, or leaves the
    /// state alone and returns `false`.
    pub fn try_consume(&mut self, n: f64) -> bool {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        self.tokens = (self.tokens + elapsed * self.refill_per_sec).min(self.capacity);
        self.last_refill = now;
        if self.tokens >= n {
            self.tokens -= n;
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn burst_and_refill() {
        let mut rl = RateLimiter::new(3.0, 1.0);
        // Burst capacity: three back-to-back.
        assert!(rl.try_consume(1.0));
        assert!(rl.try_consume(1.0));
        assert!(rl.try_consume(1.0));
        // Fourth is rejected (bucket empty, no time elapsed).
        assert!(!rl.try_consume(1.0));
        // After 1 simulated second, one token refilled.
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(rl.try_consume(1.0));
        assert!(!rl.try_consume(1.0));
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn caps_at_capacity() {
        let mut rl = RateLimiter::new(2.0, 1.0);
        // Drain.
        assert!(rl.try_consume(2.0));
        // Wait long enough to "earn" 10 tokens, but capacity is 2.
        tokio::time::advance(Duration::from_secs(10)).await;
        assert!(rl.try_consume(2.0));
        assert!(!rl.try_consume(0.001));
    }
}
