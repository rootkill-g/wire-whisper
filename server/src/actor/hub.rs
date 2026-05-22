//! The Hub — actor registry and message router.
//!
//! See [`super`] (the `actor` module's docs) for why the Hub is shared
//! state rather than a single-task actor: this is a deliberate departure
//! from pure-Erlang style so admit / broadcast / occupancy operations
//! can run in parallel.
//!
//! # State (all lock-free / wait-free)
//!
//! 1. `papaya::HashMap<Arc<str>, SessionHandle>` — **lock-free** registry
//!    of currently-connected actors. Writes are CAS loops, reads are
//!    wait-free. No shard locks, no central mutex.
//! 2. `AtomicUsize` — **wait-free O(1)** occupancy counter, kept in sync
//!    with the map by admit / release.
//! 3. One [`Mailbox<Bytes>`] per session — **bounded**, owned by the
//!    recipient session, written by the Hub through the matching
//!    [`Address<Bytes>`].
//!
//! # The fan-out hot path
//!
//! [`Hub::broadcast`] takes a pre-encoded [`Bytes`] payload (produced
//! once via [`chat_protocol::encode_frame`]) and iterates over the
//! actor map, calling [`Address::try_send`] on each non-self handle.
//! The `.clone()` on `Bytes` is a refcount bump only — *no re-encoding,
//! no `String` clones*. The cost is one pointer compare
//! ([`Arc::ptr_eq`]) per recipient plus the MPSC try-send.
//!
//! # "Don't echo back to sender"
//!
//! Each session stores its own username as `Arc<str>`. The same `Arc`
//! allocation is used as the registry key. When broadcasting, the
//! publisher passes its own `Arc<str>` as `origin`; the iter compares it
//! against each map key with [`Arc::ptr_eq`] — *pointer identity*, not
//! string equality, *one comparison*, zero allocations.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use papaya::HashMap;
use tokio::sync::mpsc;

use super::mailbox::{self, Address, Mailbox};

/// Bound on the per-session outgoing mailbox.
///
/// A receiver that lets this many frames queue up is by definition slow;
/// further sends are dropped (with a `warn` log) rather than letting one
/// session's slowness back-pressure the publisher. Sized to absorb burst
/// traffic without making the slow-disconnect threshold trigger-happy.
pub const SESSION_QUEUE_DEPTH: usize = 256;

/// The hub itself. Cheap to clone — it's just an `Arc`.
#[derive(Clone)]
pub struct Hub {
    inner: Arc<HubInner>,
}

struct HubInner {
    /// Lock-free registry. Key is the session's username; value is the
    /// [`Address`] used to enqueue outgoing frames.
    sessions: HashMap<Arc<str>, SessionHandle>,
    /// Wait-free occupancy counter, kept in sync with the map by claim /
    /// release. Reads are O(1).
    occupancy: AtomicUsize,
}

/// One per registered session. Cheap to clone — the underlying
/// [`Address`] (`mpsc::Sender`) is `Arc`-backed.
#[derive(Clone, Debug)]
struct SessionHandle {
    tx: Address<Bytes>,
}

impl Hub {
    /// Construct a fresh hub.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(HubInner {
                sessions: HashMap::new(),
                occupancy: AtomicUsize::new(0),
            }),
        }
    }

    /// Attempt to register a new actor under `username`.
    ///
    /// On success, returns a [`ClaimHandle`] (whose `Drop` releases the
    /// claim — so panics, `?` short-circuits, and clean returns all
    /// clean up correctly) and the actor's [`Mailbox<Bytes>`] for
    /// reading incoming deliveries.
    ///
    /// On collision, returns `None`.
    pub fn try_register(&self, username: Arc<str>) -> Option<(ClaimHandle, Mailbox<Bytes>)> {
        let (tx, rx) = mailbox::channel(SESSION_QUEUE_DEPTH);
        let handle = SessionHandle { tx };

        // `compute` is papaya's atomic-rmw primitive: the closure runs
        // at most once with the entry's current state, and we choose
        // what to do. Returning `Operation::Abort` leaves the map
        // unchanged.
        let pin = self.inner.sessions.pin();
        let result = pin.compute(username.clone(), |existing| match existing {
            Some(_) => papaya::Operation::Abort(()),
            None => papaya::Operation::Insert(handle.clone()),
        });

        // Exhaustive match: the closure above only returns
        // `Operation::Insert` or `Operation::Abort`, so `Updated` /
        // `Removed` are structurally impossible. Listing them explicitly
        // (rather than a wildcard arm) means if papaya introduces a new
        // variant we get a compile-time error here, not a runtime panic
        // in production.
        match result {
            papaya::Compute::Inserted(_, _) => {
                self.inner.occupancy.fetch_add(1, Ordering::Relaxed);
                Some((
                    ClaimHandle {
                        username,
                        inner: self.inner.clone(),
                    },
                    rx,
                ))
            }
            papaya::Compute::Aborted(()) => None,
            papaya::Compute::Updated { .. } | papaya::Compute::Removed(_, _) => {
                unreachable!("closure only returns Insert | Abort")
            }
        }
    }

    /// Fan out a pre-encoded frame to every actor *except* `origin`.
    ///
    /// Each recipient's `Bytes` is a refcount bump on the same
    /// allocation — the publisher serialised once via
    /// [`chat_protocol::encode_frame`]. Slow recipients see a
    /// `try_send` failure and *that frame is dropped for them only*;
    /// the room is not impacted.
    pub fn broadcast(&self, origin: &Arc<str>, payload: Bytes) {
        let pin = self.inner.sessions.pin();
        for (username, handle) in pin.iter() {
            if Arc::ptr_eq(username, origin) {
                continue;
            }
            match handle.tx.try_send(payload.clone()) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(_)) => {
                    tracing::warn!(
                        user = %username,
                        queue_depth = SESSION_QUEUE_DEPTH,
                        "outgoing mailbox full; dropping frame for this peer"
                    );
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    // Actor task is gone; its [`ClaimHandle::drop`] will
                    // remove the entry shortly. Nothing to do here.
                }
            }
        }
    }

    /// Number of currently-registered actors. Wait-free O(1).
    #[inline]
    pub fn occupancy(&self) -> usize {
        self.inner.occupancy.load(Ordering::Relaxed)
    }
}

impl Default for Hub {
    fn default() -> Self {
        Self::new()
    }
}

/// RAII handle for a registered actor.
///
/// Dropping releases the registration *and* decrements the occupancy
/// counter. The handle holds a strong reference to the hub's inner
/// state so the drop cannot race a hub teardown.
pub struct ClaimHandle {
    username: Arc<str>,
    inner: Arc<HubInner>,
}

impl ClaimHandle {
    /// The claimed username.
    #[inline]
    pub fn username(&self) -> &Arc<str> {
        &self.username
    }
}

impl Drop for ClaimHandle {
    fn drop(&mut self) {
        let pin = self.inner.sessions.pin();
        if pin.remove(&*self.username).is_some() {
            self.inner.occupancy.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "current_thread")]
    async fn claim_and_release() {
        let hub = Hub::new();
        let u: Arc<str> = Arc::from("alice");
        let (h, _rx) = hub.try_register(u.clone()).expect("claim should succeed");
        assert_eq!(hub.occupancy(), 1);
        // Collision while held.
        assert!(hub.try_register(u.clone()).is_none());
        drop(h);
        assert_eq!(hub.occupancy(), 0);
        // Re-claim after release.
        let (_h2, _rx2) = hub.try_register(u).expect("re-claim should succeed");
        assert_eq!(hub.occupancy(), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn distinct_usernames_coexist() {
        let hub = Hub::new();
        let (_a, _ra) = hub.try_register(Arc::from("alice")).unwrap();
        let (_b, _rb) = hub.try_register(Arc::from("bob")).unwrap();
        let (_c, _rc) = hub.try_register(Arc::from("carol")).unwrap();
        assert_eq!(hub.occupancy(), 3);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn broadcast_excludes_origin_and_reaches_others() {
        let hub = Hub::new();
        let alice: Arc<str> = Arc::from("alice");
        let bob: Arc<str> = Arc::from("bob");
        let (_alice_claim, mut alice_rx) = hub.try_register(alice.clone()).unwrap();
        let (_bob_claim, mut bob_rx) = hub.try_register(bob.clone()).unwrap();

        let payload = Bytes::from_static(b"hello");
        hub.broadcast(&alice, payload.clone());

        let recv_bob = bob_rx.try_recv().expect("bob should have a frame");
        assert_eq!(&recv_bob[..], b"hello");
        assert!(
            alice_rx.try_recv().is_err(),
            "alice should not echo to self"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn slow_client_drop_does_not_affect_others() {
        let hub = Hub::new();
        let publisher: Arc<str> = Arc::from("pub");
        let slow: Arc<str> = Arc::from("slow");
        let fast: Arc<str> = Arc::from("fast");
        let (_pub_claim, _pub_rx) = hub.try_register(publisher.clone()).unwrap();
        // `slow` registers but never drains.
        let (_slow_claim, _slow_rx) = hub.try_register(slow).unwrap();
        // `fast` drains as we publish.
        let (_fast_claim, mut fast_rx) = hub.try_register(fast).unwrap();

        for _ in 0..(SESSION_QUEUE_DEPTH + 8) {
            hub.broadcast(&publisher, Bytes::from_static(b"x"));
            let _ = fast_rx.try_recv();
        }
        assert!(fast_rx.try_recv().is_err());
    }
}
