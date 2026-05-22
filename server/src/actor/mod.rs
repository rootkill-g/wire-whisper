//! Actor model
//!
//! `wire-whisper`'s concurrency design, formalised.
//!
//! # What's an actor in our system
//!
//! ## [`session::Session`] — one per connected client
//!
//! Owns its private state (socket halves, rate limiter, last-activity
//! timestamp, shared `Arc<ServerConfig>`) and processes one event at a
//! time via a single `tokio::select!` loop. Inputs are:
//!
//! - bytes from the client socket (decoded into a [`ClientFrame`]),
//! - pre-encoded [`Bytes`] from its [`mailbox::Mailbox`] (deliveries
//!   from other actors via the Hub),
//! - a heartbeat tick (timer),
//! - a server-wide shutdown cancellation token.
//!
//! Outputs are: bytes to the client socket, and broadcasts published
//! through the Hub. The session never holds a lock across an `await`
//! and never allocates on the broadcast hot path beyond the single
//! `encode_frame` that produces the wire-ready `Bytes`.
//!
//! # What's *not* an actor: the [`hub::Hub`]
//!
//! The Hub is the actor *registry* and message *router*, not itself a
//! single-task actor. We deliberately depart from pure-Erlang style
//! here: a single-task Hub would serialise admit / broadcast / occupancy
//! through one mailbox, tanking throughput. Instead the Hub is a
//! shared service backed by lock-free / wait-free primitives:
//!
//! - `papaya::HashMap<Arc<str>, SessionHandle>` — **lock-free** address book.
//! - `AtomicUsize` — **wait-free** occupancy.
//! - one [`mailbox::Mailbox<Bytes>`] per session — **bounded**, owned by
//!   the recipient, written by the Hub via [`mailbox::Address::try_send`].
//!   A slow recipient's mailbox fills and that recipient drops the
//!   frame; the rest of the room is unaffected.
//!
//! The trade we make: shared lock-free state in exchange for fully
//! parallel admit / broadcast operations. The per-recipient mailboxes
//! preserve the rest of the actor-model invariants (private state,
//! message-passing, no shared mutable session state).
//!
//! # The wire-ready contract
//!
//! Actors communicate by sending [`Bytes`] to each other's mailboxes.
//! The `Bytes` are pre-encoded wire frames (see
//! [`chat_protocol::encode_frame`]); recipients write them straight to
//! their sockets with no re-encoding. One `encode_frame` per broadcast,
//! N refcount bumps for N recipients.
//!
//! [`Bytes`]: bytes::Bytes
//! [`ClientFrame`]: chat_protocol::ClientFrame

pub mod hub;
pub mod mailbox;
pub mod ratelimit;
pub mod session;

pub use hub::{ClaimHandle, Hub, SESSION_QUEUE_DEPTH};
pub use mailbox::{Address, Mailbox, channel};
pub use ratelimit::RateLimiter;
pub use session::Session;
