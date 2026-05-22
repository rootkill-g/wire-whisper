//! Type-level vocabulary for actor mailboxes.
//!
//! [`Mailbox<M>`] is an actor's inbox — owned by the actor task, written
//! by others. [`Address<M>`] is the handle others use to reach that
//! actor. Both are thin aliases over `tokio::sync::mpsc` so the
//! underlying channel implementation can be swapped (flume, kanal,
//! something custom) in one file if we ever need to.
//!
//! Capacity is bounded: a slow actor's mailbox fills,
//! [`Address::try_send`] starts failing with `Full`, and the publisher
//! decides what to do. In our case the [`super::hub::Hub`] drops the
//! frame for that recipient only — the rest of the room is unaffected.

use tokio::sync::mpsc;

/// One actor's inbox. Owned by the actor task, written by others via an
/// [`Address`].
pub type Mailbox<M> = mpsc::Receiver<M>;

/// A handle to send messages to an actor. Cheap to clone (the underlying
/// `mpsc::Sender` is `Arc`-backed).
pub type Address<M> = mpsc::Sender<M>;

/// Construct a fresh address / mailbox pair with bounded capacity.
///
/// The capacity governs how many in-flight messages a slow actor can
/// queue before [`Address::try_send`] starts to fail with `Full`. It is
/// the *only* knob protecting a slow receiver from unbounded memory
/// growth.
pub fn channel<M>(capacity: usize) -> (Address<M>, Mailbox<M>) {
    mpsc::channel(capacity)
}
