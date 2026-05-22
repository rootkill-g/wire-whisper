# Architecture

`wire-whisper` is an asynchronous single-room chat server and CLI client. The
server prioritises high concurrency, a small per-connection memory footprint,
and a zero-copy broadcast hot path; the client is a tiny async REPL over a
length-delimited binary protocol.

## Workspace layout

```
wire-whisper/
├── protocol/   # wire format, validated types, codec     (whisper-protocol)
├── server/     # accept loop, Hub, Session actor          (whisper-server)
└── client/     # CLI binary, REPL                         (whisper-client)
```

Three crates in a Cargo workspace so the protocol can be a hard library
dependency for both sides — encoder and decoder are derived from the same
`enum` definitions, eliminating wire drift.

## Wire protocol (`protocol/`)

Frames are length-prefixed [postcard](https://postcard.jamesmunns.com/)
payloads:

```
┌──────────────┬───────────────────────────┐
│ u32 BE len   │ postcard-encoded payload  │
└──────────────┴───────────────────────────┘
```

Two enums, `ClientFrame` and `ServerFrame`, define every legal message. A
single generic `PostcardCodec<D, E>` is specialised at both ends:

- `ServerSideCodec = PostcardCodec<ClientFrame, ServerFrame>`
- `ClientSideCodec = PostcardCodec<ServerFrame, ClientFrame>`

**Hardening.** `MAX_FRAME_BYTES = 8 KiB` is checked against the declared
length *before* any allocation, so a forged length prefix cannot coax the
server into reserving large buffers. `PROTOCOL_VERSION` is enforced at
handshake.

**Validated newtypes.** `UserName` (1–32 bytes, `[A-Za-z0-9_-]`) and
`MessageBody` (1–4 KiB) are `Arc<str>`-backed and validate inside their
`Deserialize` impls — an invalid value cannot exist as one of these types,
even transiently. The wire enums keep raw `String` fields so the server can
respond with a structured `Rejected` / `Error` instead of an opaque decoder
error.

**Encode once, broadcast many.** `encode_frame()` returns a `Bytes` — a
refcount-shared buffer the server hands to N recipients with zero
re-encoding.

## Server concurrency model (`server/`)

Each connection is owned by exactly one **`Session`** actor task. Sessions
coordinate through a shared **`Hub`** — a lock-free *registry and router*,
not a single-task actor (a single-task Hub would serialise admit / broadcast
/ occupancy through one mailbox and tank throughput).

```
                 ┌────────────────────────────────────┐
                 │  Hub (shared, lock-free)           │
                 │  ├─ papaya::HashMap<Arc<str>, …>   │
                 │  └─ AtomicUsize occupancy          │
                 └────────────────────────────────────┘
                       ▲                  │
            try_register / broadcast()    │ try_send(Bytes)
                       │                  ▼
   ┌──────────────────────────┐    ┌──────────────────────────┐
   │  Session actor (alice)   │    │  Session actor (bob)     │
   │  - FramedRead (codec)    │    │  - FramedRead (codec)    │
   │  - OwnedWriteHalf (raw)  │    │  - OwnedWriteHalf (raw)  │
   │  - RateLimiter           │    │  - RateLimiter           │
   │  - Mailbox<Bytes>        │    │  - Mailbox<Bytes>        │
   └──────────────────────────┘    └──────────────────────────┘
```

### Hub (`actor/hub.rs`)

- **`papaya::HashMap<Arc<str>, SessionHandle>`** — wait-free reads, CAS
  writes, no shard locks. `try_register` uses `papaya::compute` for atomic
  insert-or-abort on username collision.
- **`AtomicUsize` occupancy** — O(1) wait-free reads, kept in sync with the
  map by `claim` / `release`.
- **Per-recipient bounded mailbox** (`SESSION_QUEUE_DEPTH = 256`) — a slow
  receiver fills its own queue and drops frames *for itself only*; the
  publisher never blocks and the rest of the room is untouched.
- **RAII `ClaimHandle`** — dropping it releases the username and decrements
  occupancy, so panics, `?` short-circuits, and normal returns all clean up
  correctly.

### Session actor (`actor/session.rs`)

A single `tokio::select!` loop, `biased` in priority order:

1. **Shutdown token** — server-wide graceful stop wins over everything.
2. **Socket reads** — incoming `ClientFrame`s; updates the activity clock.
3. **Mailbox receives** — pre-encoded `Bytes` from the Hub, written
   straight to the wire (raw `OwnedWriteHalf`, no re-encoding).
4. **Heartbeat tick** — idle-timeout check, then `Ping`.

The actor owns its socket halves, a private `RateLimiter`, last-activity
timestamp, and a shared `Arc<ServerConfig>`. It never holds a lock across
an `await` and never shares mutable state with another session.

### Broadcast hot path

For each `ClientFrame::Send`:

1. Rate-limit check (token bucket; capacity 10, refill 2/s by default).
2. `encode_frame(ServerFrame::Message { … })` → `Bytes` (one allocation).
3. `Hub::broadcast(origin, bytes)` iterates the map and `try_send`s a
   `Bytes::clone()` (refcount bump) to every non-origin recipient.
4. Sender filtering uses `Arc::ptr_eq` on the username `Arc<str>` — one
   pointer compare, no string equality.

Net cost per broadcast: **1 encode + N refcount bumps + N MPSC try-sends**.
Zero per-recipient encoding, zero `String` clones.

### Backpressure & defences

| Threat                                  | Defence                                                                                  |
| --------------------------------------- | ---------------------------------------------------------------------------------------- |
| Slow consumer wedges the room           | Per-session bounded mailbox; drops the frame for that peer only                          |
| Peer floods the room with `Send`s       | Per-session token-bucket `RateLimiter` (`actor/ratelimit.rs`)                            |
| Half-open TCP holds a username hostage  | `idle_timeout` (60s) + server `Ping` heartbeat (30s)                                     |
| Client never sends `Hello`              | `hello_timeout` (10s) raced against shutdown                                             |
| Forged frame length                     | `MAX_FRAME_BYTES` checked pre-allocation in the codec                                    |
| Invalid username / oversize body        | `UserName::new` / `validate_body_bytes` → `Rejected` / `Error` frame, not a disconnect   |
| Stalling peer delays shutdown           | All session awaits race the `CancellationToken`; goodbye write bounded by `shutdown_goodbye_timeout` (500ms) |

Tunables live in `ServerConfig` (`server/src/config.rs`), shared via
`Arc<ServerConfig>` — production uses defaults, integration tests dial
timings down.

## Lifecycle

### Session

```
TCP accept ─► Session::new ─► handshake ─► serve_loop ─► cleanup
                              │             │            │
                              │             │            └─ broadcast Departed
                              │             │               drop ClaimHandle
                              │             │               (releases username,
                              │             │                decrements occupancy)
                              │             └─ select!: shutdown │ read │ mailbox │ tick
                              └─ Hello → version check → UserName::new →
                                 Hub::try_register → Welcome (or Rejected + close)
```

The post-loop cleanup runs even when `serve_loop` errors, so a crashed peer
never leaks a username or a "joined" without a matching "departed".

### Server shutdown contract

`serve()` takes a `CancellationToken`. Cancelling it:

1. Breaks the accept loop (no new connections).
2. Propagates to every in-flight session (each holds a child token).
3. Each session makes a best-effort `ServerFrame::Error { reason: "server
   shutting down" }` (bounded by `shutdown_goodbye_timeout`), then publishes
   its `Departed` and releases its claim.
4. `serve()` drains all session tasks via `JoinSet::join_next` before
   returning.

Result: graceful shutdown, no orphaned usernames, no clients left guessing
why they got an RST.

## Client (`client/`)

`whisper-client` is a `current_thread` tokio binary with two phases:

1. **Connect & handshake** (`main.rs`) — TCP connect with a 10s timeout,
   `set_nodelay(true)`, send `Hello`, expect `Welcome` (or bail on
   `Rejected`).
2. **REPL** (`repl.rs`) — a single `select!` over stdin lines and the
   framed socket.

```
stdin lines ──► parse_command ──► ClientFrame::{Send, Leave}
                                  │
   ServerFrame ◄── socket ────────┘
       │
       ├─ Ping            → auto-Pong (invisible)
       ├─ Message/Joined/Departed/Error/Welcome  → erase prompt, print, redraw
       └─ Rejected        → print and exit
```

A small UX trick keeps the input line clean: when a frame arrives
mid-prompt, the REPL emits `\r\x1b[K` (CR + clear-to-end-of-line), prints
the event, and re-draws the prompt. No raw mode required.

Configuration is via flags or env vars: `--host` / `WIRE_WHISPER_HOST`,
`--port` / `WIRE_WHISPER_PORT`, `--username` / `WIRE_WHISPER_USERNAME`.

Tracing is initialised at `warn` by default and writes to **stderr**, so
chat content (stdout) is never polluted by logs — `RUST_LOG=info` enables
verbose diagnostics without breaking pipes or screen-scraping.

## Performance summary

| Operation                  | Cost                                                       |
| -------------------------- | ---------------------------------------------------------- |
| Username lookup / insert   | Lock-free (papaya CAS)                                     |
| Occupancy read             | Wait-free O(1) atomic load                                 |
| Broadcast of N recipients  | 1 encode, N refcount bumps, N `mpsc::try_send`             |
| "Don't echo to sender"     | 1 `Arc::ptr_eq` per recipient                              |
| Per-session memory         | One bounded mailbox (256 × `Bytes` handle) + actor stack   |
| Idle session bookkeeping   | Heartbeat tick + activity timestamp; no Hub-side traffic   |

## Testing

- **Unit tests** alongside each module (`protocol/src/lib.rs`,
  `actor/hub.rs`, `actor/ratelimit.rs`, `client/src/repl.rs`).
- **Property tests** (`proptest`) for codec round-tripping and validator
  agreement.
- **Simulated time** (`tokio::time::pause`) for the rate limiter so tests
  don't sleep.
- `server::serve` is exposed as a library entry point so integration tests
  can drive the real server in-process — no subprocess wrangling, no port
  scraping.
