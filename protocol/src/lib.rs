//! Whisper Protocol
//!
//! The wire format for the `wire-whisper`. Frames are length-limited Postcard payloads:
//!
//! ```text
//! ┌──────────────┬───────────────────────────┐
//! │ u32 BE len   │ postcard-encoded payload  │
//! └──────────────┴───────────────────────────┘
//! ```
//!
//! Hardening against adversarial peers:
//!
//! - `MAX_FRAME_BYTES` is enforced *before* allocation in `decode`, so a
//!   forged length prefix cannot coax the server into reserving large
//!   buffers.
//! - The codec is symmetric: server and client both use a `PostcardCodec`
//!   parameterised by what each side decodes and encodes. Type aliases
//!   give each side a clean name.
//! - [`encode_frame`] produces a wire-ready [`Bytes`] in a single
//!   allocation that the server can fan-out to many recipients without
//!   re-encoding.
//!
//! # Type-system enforcement
//!
//! [`UserName`] and [`MessageBody`] are validated-at-construction newtypes.
//! Their `Deserialize` impls run the validation rules, so an invalid value
//! cannot exist as one of these types — not even briefly after a
//! successful frame decode. The wire `String`-typed fields remain on the
//! frame enums so the server can produce structured `Rejected` responses
//! on validation failure instead of opaque decoder errors.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::io;
use std::marker::PhantomData;
use std::sync::Arc;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use tokio_util::codec::{Decoder, Encoder};

/// Wire protocol version. Bumped on any backwards-incompatible change.
///
/// Servers reject [`ClientFrame::Hello`]s with a different version.
pub const PROTOCOL_VERSION: u8 = 1;

/// Hard cap for the payload of a single frame, in bytes.
///
/// Sized to comfortably accommodate the worst legal frame (a
/// `ServerFrame::Message` with a max-length username and max-length body —
/// roughly `MAX_USERNAME_BYTES + MAX_BODY_BYTES` plus postcard envelope
/// overhead, well under 5 KiB) with a healthy margin, but no larger.
/// An attacker writing a forged length prefix can claim at most this many
/// bytes of buffer before we reject the frame.
pub const MAX_FRAME_BYTES: usize = 8 * 1024;

/// Hard cap for a chat message body, in bytes.
pub const MAX_BODY_BYTES: usize = 4096;

/// Hard cap for a username, in bytes (and code-points; usernames are ASCII).
pub const MAX_USERNAME_BYTES: usize = 32;

/// Minimum number of bytes in a username.
pub const MIN_USERNAME_BYTES: usize = 1;

// ─────────────────────────────────────────────────────────────────────────
// Wire frames
// ─────────────────────────────────────────────────────────────────────────

/// Frames the client sends to the server.
///
/// Fields are `String`-typed so the server can produce structured
/// `Rejected`/`Error` responses on validation failure rather than opaque
/// decoder errors. Conversion to validated [`UserName`] / [`MessageBody`]
/// happens explicitly at the server's boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClientFrame {
    /// Handshake. `version` MUST equal [`PROTOCOL_VERSION`].
    Hello {
        /// Client's protocol version.
        version: u8,
        /// Desired username. Validated by [`UserName::new`].
        username: String,
    },
    /// Broadcast `body` to every other connected user.
    Send {
        /// UTF-8 body. Validated by [`MessageBody::new`].
        body: String,
    },
    /// Reply to a server [`ServerFrame::Ping`]. The server uses incoming
    /// frames as an activity heartbeat; sending `Pong` keeps an otherwise
    /// idle session alive.
    Pong,
    /// Politely disconnect.
    Leave,
}

/// Frames the server sends to the client.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ServerFrame {
    /// Handshake accepted.
    Welcome {
        /// Free-form welcome banner.
        motd: String,
        /// Current room occupancy after this user joined.
        occupancy: u32,
    },
    /// Handshake rejected. The server closes the socket after sending this.
    Rejected {
        /// Why the handshake was rejected.
        reason: String,
    },
    /// Someone else joined the room.
    Joined {
        /// The username that joined.
        username: String,
    },
    /// Someone else left the room.
    Departed {
        /// The username that left.
        username: String,
    },
    /// A chat message from `from`. Never delivered back to `from`.
    Message {
        /// The sender.
        from: String,
        /// The body.
        body: String,
    },
    /// Liveness probe. Client should reply with [`ClientFrame::Pong`].
    Ping,
    /// A non-fatal server-side error notice.
    Error {
        /// Human-readable error description.
        reason: String,
    },
}

// ─────────────────────────────────────────────────────────────────────────
// Validation errors
// ─────────────────────────────────────────────────────────────────────────

/// Errors raised when constructing a validated newtype from raw input.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ValidationError {
    /// Username string was empty.
    #[error("username is empty (must be {MIN_USERNAME_BYTES}..={MAX_USERNAME_BYTES} bytes)")]
    UsernameEmpty,
    /// Username exceeded the configured maximum.
    #[error("username is {actual} bytes; max is {max}")]
    UsernameTooLong {
        /// Length of the offending string.
        actual: usize,
        /// Configured maximum ([`MAX_USERNAME_BYTES`]).
        max: usize,
    },
    /// Username contained characters outside the allowed set.
    #[error("username contains an invalid byte (allowed: ASCII alnum, '_', '-')")]
    UsernameInvalidByte,
    /// Body string was empty.
    #[error("body is empty")]
    BodyEmpty,
    /// Body exceeded the configured maximum.
    #[error("body is {actual} bytes; max is {max}")]
    BodyTooLong {
        /// Length of the offending body.
        actual: usize,
        /// Configured maximum ([`MAX_BODY_BYTES`]).
        max: usize,
    },
}

// ─────────────────────────────────────────────────────────────────────────
// UserName & MessageBody — validated newtypes
// ─────────────────────────────────────────────────────────────────────────

/// A validated chat username.
///
/// `Arc<str>`-backed so cloning is a refcount bump rather than an allocation.
/// The `Deserialize` impl validates on the wire, so an invalid `UserName`
/// cannot exist even transiently after a decode.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct UserName(Arc<str>);

impl UserName {
    /// Validate and construct.
    pub fn new(input: impl AsRef<str>) -> Result<Self, ValidationError> {
        let s = input.as_ref();
        validate_username_bytes(s)?;
        Ok(Self(Arc::from(s)))
    }

    /// Construct from an already-validated `Arc<str>`. Useful when the
    /// same allocation is shared across multiple sites (the hub key, the
    /// session's own copy, etc.) — clones are then refcount bumps.
    ///
    /// # Errors
    /// Same as [`UserName::new`]; the input is re-validated defensively.
    pub fn from_arc(arc: Arc<str>) -> Result<Self, ValidationError> {
        validate_username_bytes(&arc)?;
        Ok(Self(arc))
    }

    /// Borrow as `&str`.
    #[inline]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Borrow as `&Arc<str>` — useful for sharing the underlying allocation
    /// without bumping the refcount yet.
    #[inline]
    pub fn as_arc(&self) -> &Arc<str> {
        &self.0
    }

    /// Consume into the inner `Arc<str>`.
    #[inline]
    pub fn into_arc(self) -> Arc<str> {
        self.0
    }
}

impl Serialize for UserName {
    fn serialize<S: Serializer>(&self, ser: S) -> Result<S::Ok, S::Error> {
        self.0.as_ref().serialize(ser)
    }
}

impl<'de> Deserialize<'de> for UserName {
    fn deserialize<D: Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
        let s = String::deserialize(de)?;
        Self::new(s).map_err(serde::de::Error::custom)
    }
}

impl std::fmt::Display for UserName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// A validated chat message body.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MessageBody(Arc<str>);

impl MessageBody {
    /// Validate and construct.
    pub fn new(input: impl AsRef<str>) -> Result<Self, ValidationError> {
        let s = input.as_ref();
        validate_body_bytes(s)?;
        Ok(Self(Arc::from(s)))
    }

    /// Borrow as `&str`.
    #[inline]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consume into the inner `Arc<str>`.
    #[inline]
    pub fn into_arc(self) -> Arc<str> {
        self.0
    }
}

impl Serialize for MessageBody {
    fn serialize<S: Serializer>(&self, ser: S) -> Result<S::Ok, S::Error> {
        self.0.as_ref().serialize(ser)
    }
}

impl<'de> Deserialize<'de> for MessageBody {
    fn deserialize<D: Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
        let s = String::deserialize(de)?;
        Self::new(s).map_err(serde::de::Error::custom)
    }
}

impl std::fmt::Display for MessageBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

// ─────────────────────────────────────────────────────────────────────────
// Free-standing validators (used by the newtypes and exposed for callers
// who want to validate without constructing a newtype).
// ─────────────────────────────────────────────────────────────────────────

/// Validate a username string against the protocol's rules.
///
/// Allowed: ASCII alphanumeric, `_`, `-`, length in `1..=32`.
#[inline]
pub fn validate_username_bytes(s: &str) -> Result<(), ValidationError> {
    let n = s.len();
    if n < MIN_USERNAME_BYTES {
        return Err(ValidationError::UsernameEmpty);
    }
    if n > MAX_USERNAME_BYTES {
        return Err(ValidationError::UsernameTooLong {
            actual: n,
            max: MAX_USERNAME_BYTES,
        });
    }
    if !s
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    {
        return Err(ValidationError::UsernameInvalidByte);
    }
    Ok(())
}

/// Validate a message body against the protocol's rules.
#[inline]
pub fn validate_body_bytes(s: &str) -> Result<(), ValidationError> {
    if s.is_empty() {
        return Err(ValidationError::BodyEmpty);
    }
    if s.len() > MAX_BODY_BYTES {
        return Err(ValidationError::BodyTooLong {
            actual: s.len(),
            max: MAX_BODY_BYTES,
        });
    }
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────
// Codec
// ─────────────────────────────────────────────────────────────────────────

/// A length-delimited Postcard codec.
///
/// Generic over the type we **D**ecode (incoming) and the type we **E**ncode
/// (outgoing). The two sides of the wire pick opposite parameterisations.
pub struct PostcardCodec<D, E> {
    _phantom: PhantomData<fn(D) -> E>,
}

impl<D, E> PostcardCodec<D, E> {
    /// Construct a fresh codec.
    pub const fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

impl<D, E> Default for PostcardCodec<D, E> {
    fn default() -> Self {
        Self::new()
    }
}

impl<D, E> Decoder for PostcardCodec<D, E>
where
    D: for<'de> serde::Deserialize<'de>,
{
    type Item = D;
    type Error = ProtocolError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<D>, Self::Error> {
        if src.len() < 4 {
            return Ok(None);
        }
        let len = u32::from_be_bytes([src[0], src[1], src[2], src[3]]) as usize;
        if len > MAX_FRAME_BYTES {
            return Err(ProtocolError::OversizeFrame { len });
        }
        if src.len() < 4 + len {
            // Reserve enough space so the next read can fill the whole
            // frame in one syscall.
            src.reserve(4 + len - src.len());
            return Ok(None);
        }
        src.advance(4);
        let payload = src.split_to(len);
        let item = postcard::from_bytes(&payload)?;
        Ok(Some(item))
    }
}

impl<D, E> Encoder<E> for PostcardCodec<D, E>
where
    E: Serialize,
{
    type Error = ProtocolError;

    fn encode(&mut self, item: E, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let payload = postcard::to_allocvec(&item)?;
        if payload.len() > MAX_FRAME_BYTES {
            return Err(ProtocolError::OversizeFrame { len: payload.len() });
        }
        dst.reserve(4 + payload.len());
        dst.put_u32(payload.len() as u32);
        dst.extend_from_slice(&payload);
        Ok(())
    }
}

/// The codec the server uses: decodes [`ClientFrame`], encodes [`ServerFrame`].
pub type ServerSideCodec = PostcardCodec<ClientFrame, ServerFrame>;

/// The codec the client uses: decodes [`ServerFrame`], encodes [`ClientFrame`].
pub type ClientSideCodec = PostcardCodec<ServerFrame, ClientFrame>;

/// Encode `frame` into a wire-ready `[u32 BE len][postcard payload]` buffer
/// suitable for direct `write_all` to the socket.
///
/// The returned [`Bytes`] is reference-counted, so the server can fan it
/// out to many recipients with `Bytes::clone()` (refcount bump only) and
/// every recipient writes the same buffer to its socket — *no
/// per-recipient re-encoding*, *no `String` clones*.
///
/// # Errors
/// Returns [`ProtocolError::Postcard`] if `frame` cannot be serialised, or
/// [`ProtocolError::OversizeFrame`] if the encoded length would exceed
/// [`MAX_FRAME_BYTES`].
pub fn encode_frame<F: Serialize>(frame: &F) -> Result<Bytes, ProtocolError> {
    let payload = postcard::to_allocvec(frame)?;
    if payload.len() > MAX_FRAME_BYTES {
        return Err(ProtocolError::OversizeFrame { len: payload.len() });
    }
    let mut buf = BytesMut::with_capacity(4 + payload.len());
    buf.put_u32(payload.len() as u32);
    buf.extend_from_slice(&payload);
    Ok(buf.freeze())
}

// ─────────────────────────────────────────────────────────────────────────
// Errors
// ─────────────────────────────────────────────────────────────────────────

/// Errors the codec and framing layer can raise.
#[derive(Debug, thiserror::Error)]
pub enum ProtocolError {
    /// A frame's declared length exceeds [`MAX_FRAME_BYTES`].
    #[error("frame size {len} exceeds MAX_FRAME_BYTES ({})", MAX_FRAME_BYTES)]
    OversizeFrame {
        /// The declared (or actual) length.
        len: usize,
    },
    /// Postcard refused to (de)serialise the payload. Note that custom
    /// `Deserialize` impls on [`UserName`] / [`MessageBody`] surface
    /// validation failures through this variant.
    #[error("postcard codec failure: {0}")]
    Postcard(#[from] postcard::Error),
    /// Underlying I/O failed.
    #[error("io error: {0}")]
    Io(#[from] io::Error),
}

// ─────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;
    use proptest::prelude::*;

    type LoopbackCodec = PostcardCodec<ClientFrame, ClientFrame>;

    #[test]
    fn client_frame_roundtrip() {
        let mut codec = LoopbackCodec::new();
        let mut buf = BytesMut::new();
        let frame = ClientFrame::Send {
            body: "hello, world".into(),
        };
        codec.encode(frame.clone(), &mut buf).unwrap();
        let decoded = codec.decode(&mut buf).unwrap().expect("frame");
        assert_eq!(frame, decoded);
        assert!(buf.is_empty(), "decoder should consume the entire frame");
    }

    #[test]
    fn server_frame_roundtrip() {
        let mut codec = PostcardCodec::<ServerFrame, ServerFrame>::new();
        let mut buf = BytesMut::new();
        let frame = ServerFrame::Message {
            from: "alice".into(),
            body: "hi bob".into(),
        };
        codec.encode(frame.clone(), &mut buf).unwrap();
        let decoded = codec.decode(&mut buf).unwrap().expect("frame");
        assert_eq!(frame, decoded);
    }

    #[test]
    fn rejects_oversize_declared_length() {
        let mut codec = LoopbackCodec::new();
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&u32::to_be_bytes((MAX_FRAME_BYTES + 1) as u32));
        assert!(matches!(
            codec.decode(&mut buf),
            Err(ProtocolError::OversizeFrame { .. })
        ));
    }

    #[test]
    fn partial_frame_yields_none() {
        let mut codec = LoopbackCodec::new();
        let mut buf = BytesMut::new();
        codec
            .encode(ClientFrame::Send { body: "abc".into() }, &mut buf)
            .unwrap();
        let n = buf.len();
        let mut partial = buf.split_to(n - 1);
        assert!(codec.decode(&mut partial).unwrap().is_none());
    }

    #[test]
    fn two_frames_back_to_back() {
        let mut codec = LoopbackCodec::new();
        let mut buf = BytesMut::new();
        codec
            .encode(
                ClientFrame::Hello {
                    version: PROTOCOL_VERSION,
                    username: "alice".into(),
                },
                &mut buf,
            )
            .unwrap();
        codec
            .encode(ClientFrame::Send { body: "hi".into() }, &mut buf)
            .unwrap();
        let a = codec.decode(&mut buf).unwrap().unwrap();
        let b = codec.decode(&mut buf).unwrap().unwrap();
        assert!(matches!(a, ClientFrame::Hello { ref username, version }
        if username == "alice" && version == PROTOCOL_VERSION));
        assert!(matches!(b, ClientFrame::Send { ref body } if body == "hi"));
        assert!(buf.is_empty());
    }

    #[test]
    fn encode_frame_matches_codec_output() {
        let frame = ServerFrame::Joined {
            username: "alice".into(),
        };
        let via_helper = encode_frame(&frame).unwrap();

        let mut codec = PostcardCodec::<ServerFrame, ServerFrame>::new();
        let mut buf = BytesMut::new();
        codec.encode(frame, &mut buf).unwrap();

        assert_eq!(&via_helper[..], &buf[..]);
    }

    #[test]
    fn username_validation() {
        assert!(UserName::new("alice").is_ok());
        assert!(UserName::new("Alice_Smith-9").is_ok());
        assert_eq!(UserName::new(""), Err(ValidationError::UsernameEmpty));
        assert!(matches!(
            UserName::new("hi!"),
            Err(ValidationError::UsernameInvalidByte)
        ));
        assert!(matches!(
            UserName::new("with space"),
            Err(ValidationError::UsernameInvalidByte)
        ));
        assert!(UserName::new("x".repeat(MAX_USERNAME_BYTES)).is_ok());
        assert!(matches!(
            UserName::new("x".repeat(MAX_USERNAME_BYTES + 1)),
            Err(ValidationError::UsernameTooLong { .. })
        ));
    }

    #[test]
    fn body_validation() {
        assert!(MessageBody::new("hi").is_ok());
        assert_eq!(MessageBody::new(""), Err(ValidationError::BodyEmpty));
        assert!(MessageBody::new("x".repeat(MAX_BODY_BYTES)).is_ok());
        assert!(matches!(
            MessageBody::new("x".repeat(MAX_BODY_BYTES + 1)),
            Err(ValidationError::BodyTooLong { .. })
        ));
    }

    #[test]
    fn username_deserialize_validates() {
        // A serialized valid UserName decodes fine.
        let valid = UserName::new("alice").unwrap();
        let bytes = postcard::to_allocvec(&valid).unwrap();
        let parsed: UserName = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(parsed.as_str(), "alice");

        // A raw string that's invalid for the rules fails when decoded
        // as a UserName.
        let raw_bytes = postcard::to_allocvec("bad space").unwrap();
        let err = postcard::from_bytes::<UserName>(&raw_bytes);
        assert!(err.is_err(), "invalid usernames must not deserialize");
    }

    // ── Property-based tests ────────────────────────────────────────────

    proptest! {
        /// Any valid `ClientFrame` survives an encode-decode round-trip
        /// byte-for-byte.
        #[test]
        fn prop_client_frame_roundtrip(frame in any_client_frame()) {
            let bytes = encode_frame(&frame).unwrap();
            // Strip the 4-byte length prefix to get the postcard payload.
            let payload = &bytes[4..];
            let decoded: ClientFrame = postcard::from_bytes(payload).unwrap();
            prop_assert_eq!(frame, decoded);
        }

        /// Any valid `ServerFrame` survives an encode-decode round-trip.
        #[test]
        fn prop_server_frame_roundtrip(frame in any_server_frame()) {
            let bytes = encode_frame(&frame).unwrap();
            let payload = &bytes[4..];
            let decoded: ServerFrame = postcard::from_bytes(payload).unwrap();
            prop_assert_eq!(frame, decoded);
        }

        /// Any string that passes `validate_username_bytes` survives a
        /// `UserName` round-trip; any string that fails it does not.
        #[test]
        fn prop_username_validation_matches_newtype(s in any::<String>()) {
            let direct = validate_username_bytes(&s);
            let typed = UserName::new(&s);
            match (direct, typed) {
                (Ok(()), Ok(u))  => prop_assert_eq!(u.as_str(), s),
                (Err(e1), Err(e2)) => prop_assert_eq!(e1, e2),
                (a, b) => prop_assert!(false, "validators disagree: {a:?} vs {b:?}"),
            }
        }

        /// The codec's `decode(encode(x))` is identity for any valid
        /// `ClientFrame`. This catches off-by-one errors in framing.
        #[test]
        fn prop_codec_streaming_roundtrip(frame in any_client_frame()) {
            let mut codec = LoopbackCodec::new();
            let mut buf = BytesMut::new();
            codec.encode(frame.clone(), &mut buf).unwrap();
            let decoded = codec.decode(&mut buf).unwrap().expect("frame");
            prop_assert_eq!(frame, decoded);
            prop_assert!(buf.is_empty());
        }
    }

    /// Generates a `ClientFrame` whose contents satisfy the protocol's
    /// validation rules (so we don't waste cycles on shrinks that
    /// validation would just reject).
    fn any_client_frame() -> impl Strategy<Value = ClientFrame> {
        prop_oneof![
            (any_username_string(), prop::num::u8::ANY)
                .prop_map(|(username, version)| ClientFrame::Hello { version, username }),
            any_body_string().prop_map(|body| ClientFrame::Send { body }),
            Just(ClientFrame::Pong),
            Just(ClientFrame::Leave),
        ]
    }

    fn any_server_frame() -> impl Strategy<Value = ServerFrame> {
        prop_oneof![
            (".*", any::<u32>())
                .prop_map(|(motd, occupancy)| ServerFrame::Welcome { motd, occupancy }),
            ".*".prop_map(|reason| ServerFrame::Rejected { reason }),
            any_username_string().prop_map(|username| ServerFrame::Joined { username }),
            any_username_string().prop_map(|username| ServerFrame::Departed { username }),
            (any_username_string(), any_body_string())
                .prop_map(|(from, body)| ServerFrame::Message { from, body }),
            Just(ServerFrame::Ping),
            ".*".prop_map(|reason| ServerFrame::Error { reason }),
        ]
    }

    fn any_username_string() -> impl Strategy<Value = String> {
        "[A-Za-z0-9_-]{1,32}".prop_map(String::from)
    }

    fn any_body_string() -> impl Strategy<Value = String> {
        prop::collection::vec(any::<char>(), 1..=64).prop_map(|v| v.into_iter().collect())
    }
}
