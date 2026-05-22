//! End-to-end integration tests for the chat server.
//!
//! Spins up the real server in-process on a random port, then drives one
//! or more clients against it using the same codec the production client
//! uses. Verifies:
//!
//!   1. Broadcast: messages reach *other* connected users, never the sender.
//!   2. Usernames are unique; collisions are `Rejected`.
//!   3. `Leave` / socket-close releases the username for reuse.
//!   4. Invalid usernames and bodies are `Rejected`/`Error`d.
//!   5. Protocol-version mismatch is `Rejected`.
//!   6. A second `Hello` after handshake is a protocol violation —
//!      `Error` then disconnect.
//!   7. Graceful shutdown notifies connected clients.
//!   8. Heartbeat: server sends `Ping`; idle timeout disconnects.
//!   9. Rate limiting: bursts past the bucket capacity get `Error`.

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use whisper_protocol::{ClientFrame, ClientSideCodec, ServerFrame, PROTOCOL_VERSION};
use whisper_server::{serve, CancellationToken, Hub, ServerConfig};
use futures::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_util::codec::Framed;

type Client = Framed<TcpStream, ClientSideCodec>;

/// Default per-frame timeout. Generous enough to absorb slow CI runners.
const T: Duration = Duration::from_millis(500);

struct ServerHandle {
    addr: SocketAddr,
    shutdown: CancellationToken,
    task: JoinHandle<io::Result<()>>,
}

impl ServerHandle {
    /// Cheap shutdown — used by tests that don't care about clean drain.
    fn abort(self) {
        self.task.abort();
    }
}

async fn spawn_server() -> ServerHandle {
    spawn_server_with(ServerConfig::default()).await
}

async fn spawn_server_with(config: ServerConfig) -> ServerHandle {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let hub = Hub::new();
    let shutdown = CancellationToken::new();
    let task = tokio::spawn(serve(listener, hub, shutdown.clone(), Arc::new(config)));
    ServerHandle {
        addr,
        shutdown,
        task,
    }
}

async fn connect(addr: SocketAddr) -> Client {
    let stream = TcpStream::connect(addr).await.unwrap();
    stream.set_nodelay(true).unwrap();
    Framed::new(stream, ClientSideCodec::new())
}

async fn handshake(addr: SocketAddr, name: &str) -> Client {
    let mut c = connect(addr).await;
    c.send(ClientFrame::Hello {
        version: PROTOCOL_VERSION,
        username: name.into(),
    })
    .await
    .unwrap();
    let welcome = timeout(T, c.next()).await.unwrap().unwrap().unwrap();
    assert!(
        matches!(welcome, ServerFrame::Welcome { .. }),
        "expected Welcome, got {welcome:?}"
    );
    c
}

/// Handshake, retrying on `Rejected` until the username slot opens up.
async fn handshake_until_available(addr: SocketAddr, name: &str) -> Client {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let mut c = connect(addr).await;
        c.send(ClientFrame::Hello {
            version: PROTOCOL_VERSION,
            username: name.into(),
        })
        .await
        .unwrap();
        let resp = timeout(T, c.next()).await.unwrap().unwrap().unwrap();
        match resp {
            ServerFrame::Welcome { .. } => return c,
            ServerFrame::Rejected { .. } if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(10)).await;
                continue;
            }
            other => panic!("handshake_until_available({name}): unexpected {other:?}"),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────
// Core broadcast invariants
// ─────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn broadcast_excludes_sender() {
    let server = spawn_server().await;
    let mut alice = handshake(server.addr, "alice").await;
    let mut bob = handshake(server.addr, "bob").await;

    // bob joined after alice; alice sees bob's Joined.
    let bob_joined = timeout(T, alice.next()).await.unwrap().unwrap().unwrap();
    assert!(
        matches!(bob_joined, ServerFrame::Joined { ref username } if username == "bob"),
        "expected alice to see bob's Joined, got {bob_joined:?}"
    );

    alice
        .send(ClientFrame::Send {
            body: "hello world".into(),
        })
        .await
        .unwrap();

    let msg = timeout(T, bob.next()).await.unwrap().unwrap().unwrap();
    match msg {
        ServerFrame::Message { from, body } => {
            assert_eq!(from, "alice");
            assert_eq!(body, "hello world");
        }
        other => panic!("expected Message from alice, got {other:?}"),
    }

    // Alice does NOT receive her own message.
    assert!(
        timeout(Duration::from_millis(200), alice.next())
            .await
            .is_err(),
        "alice should not see her own message"
    );

    server.abort();
}

#[tokio::test]
async fn duplicate_username_is_rejected() {
    let server = spawn_server().await;
    let _alice = handshake(server.addr, "alice").await;

    let mut intruder = connect(server.addr).await;
    intruder
        .send(ClientFrame::Hello {
            version: PROTOCOL_VERSION,
            username: "alice".into(),
        })
        .await
        .unwrap();
    let resp = timeout(T, intruder.next()).await.unwrap().unwrap().unwrap();
    assert!(matches!(resp, ServerFrame::Rejected { .. }));

    server.abort();
}

#[tokio::test]
async fn leave_releases_username() {
    let server = spawn_server().await;

    {
        let mut alice = handshake(server.addr, "alice").await;
        alice.send(ClientFrame::Leave).await.unwrap();
        // Wait for the server to close the socket — confirms the
        // departure was processed.
        while alice.next().await.is_some() {}
    }

    let _alice_again = handshake_until_available(server.addr, "alice").await;
    server.abort();
}

#[tokio::test]
async fn socket_close_releases_username() {
    let server = spawn_server().await;
    {
        let _alice = handshake(server.addr, "alice").await;
    }
    let _alice_again = handshake_until_available(server.addr, "alice").await;
    server.abort();
}

#[tokio::test]
async fn invalid_username_is_rejected() {
    let server = spawn_server().await;
    let mut c = connect(server.addr).await;
    c.send(ClientFrame::Hello {
        version: PROTOCOL_VERSION,
        username: "no spaces allowed".into(),
    })
    .await
    .unwrap();
    let resp = timeout(T, c.next()).await.unwrap().unwrap().unwrap();
    assert!(matches!(resp, ServerFrame::Rejected { .. }));
    server.abort();
}

#[tokio::test]
async fn three_way_broadcast() {
    let server = spawn_server().await;
    let mut alice = handshake(server.addr, "alice").await;
    let mut bob = handshake(server.addr, "bob").await;
    let mut carol = handshake(server.addr, "carol").await;

    // Drain join notifications.
    let _ = timeout(T, alice.next()).await.unwrap().unwrap().unwrap();
    let _ = timeout(T, alice.next()).await.unwrap().unwrap().unwrap();
    let _ = timeout(T, bob.next()).await.unwrap().unwrap().unwrap();

    bob.send(ClientFrame::Send {
        body: "hello everyone".into(),
    })
    .await
    .unwrap();

    let to_alice = timeout(T, alice.next()).await.unwrap().unwrap().unwrap();
    let to_carol = timeout(T, carol.next()).await.unwrap().unwrap().unwrap();
    for (who, frame) in [("alice", to_alice), ("carol", to_carol)] {
        match frame {
            ServerFrame::Message { from, body } => {
                assert_eq!(from, "bob", "wrong sender to {who}");
                assert_eq!(body, "hello everyone", "wrong body to {who}");
            }
            other => panic!("expected Message at {who}, got {other:?}"),
        }
    }

    // bob should not see his own message.
    assert!(timeout(Duration::from_millis(200), bob.next())
        .await
        .is_err());

    server.abort();
}

#[tokio::test]
async fn empty_body_is_rejected() {
    let server = spawn_server().await;
    let mut alice = handshake(server.addr, "alice").await;
    alice
        .send(ClientFrame::Send { body: "".into() })
        .await
        .unwrap();
    let resp = timeout(T, alice.next()).await.unwrap().unwrap().unwrap();
    assert!(matches!(resp, ServerFrame::Error { .. }));
    server.abort();
}

// ─────────────────────────────────────────────────────────────────────
// Protocol hygiene
// ─────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn protocol_version_mismatch_is_rejected() {
    let server = spawn_server().await;
    let mut c = connect(server.addr).await;
    c.send(ClientFrame::Hello {
        version: PROTOCOL_VERSION.wrapping_add(99),
        username: "alice".into(),
    })
    .await
    .unwrap();
    let resp = timeout(T, c.next()).await.unwrap().unwrap().unwrap();
    assert!(
        matches!(resp, ServerFrame::Rejected { ref reason } if reason.contains("version")),
        "expected version-mismatch Rejected, got {resp:?}"
    );
    server.abort();
}

#[tokio::test]
async fn second_hello_disconnects_peer() {
    let server = spawn_server().await;
    let mut alice = handshake(server.addr, "alice").await;

    alice
        .send(ClientFrame::Hello {
            version: PROTOCOL_VERSION,
            username: "alice2".into(),
        })
        .await
        .unwrap();

    let resp = timeout(T, alice.next()).await.unwrap().unwrap().unwrap();
    assert!(
        matches!(resp, ServerFrame::Error { ref reason } if reason.contains("protocol violation")),
        "expected protocol-violation Error, got {resp:?}"
    );
    let next = timeout(T, alice.next()).await.unwrap();
    assert!(next.is_none(), "expected socket close, got {next:?}");
    server.abort();
}

#[tokio::test]
async fn graceful_shutdown_notifies_connected_clients() {
    let server = spawn_server().await;
    let mut alice = handshake(server.addr, "alice").await;

    server.shutdown.cancel();

    let goodbye = timeout(T, alice.next()).await.unwrap().unwrap().unwrap();
    assert!(
        matches!(goodbye, ServerFrame::Error { ref reason } if reason.contains("shutting down")),
        "expected shutdown Error, got {goodbye:?}"
    );

    while alice.next().await.is_some() {}

    let server_result = timeout(Duration::from_secs(5), server.task).await;
    assert!(
        matches!(server_result, Ok(Ok(Ok(())))),
        "serve() did not return Ok within the drain budget: {server_result:?}"
    );
}

// ─────────────────────────────────────────────────────────────────────
// Heartbeat
// ─────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn server_sends_ping_after_idle_interval() {
    let server = spawn_server_with(ServerConfig {
        ping_interval: Duration::from_millis(100),
        idle_timeout: Duration::from_secs(60), // generous; we want Ping, not disconnect
        ..ServerConfig::default()
    })
    .await;

    let mut alice = handshake(server.addr, "alice").await;

    // Within a few ping intervals we should see a Ping. The server has
    // no reason to send anything else to alice here — no other clients
    // are joining or talking.
    let outcome = timeout(Duration::from_secs(2), async {
        match alice.next().await.unwrap().unwrap() {
            ServerFrame::Ping => (),
            other => panic!("unexpected frame while waiting for Ping: {other:?}"),
        }
    })
    .await;
    assert!(outcome.is_ok(), "did not receive Ping within 2s");

    server.abort();
}

#[tokio::test]
async fn idle_timeout_disconnects_silent_client() {
    let server = spawn_server_with(ServerConfig {
        ping_interval: Duration::from_millis(50),
        idle_timeout: Duration::from_millis(150),
        ..ServerConfig::default()
    })
    .await;

    let mut alice = handshake(server.addr, "alice").await;

    // Don't send any frames (including Pong). The server should:
    //   1. Send a few Pings.
    //   2. Notice idleness past 150 ms.
    //   3. Send a final Error frame and close.
    let outcome = timeout(Duration::from_secs(2), async {
        loop {
            match alice.next().await {
                Some(Ok(ServerFrame::Ping)) => continue,
                Some(Ok(ServerFrame::Error { reason })) if reason.contains("idle") => {
                    return "idle_error_then_close";
                }
                Some(Ok(other)) => panic!("unexpected: {other:?}"),
                Some(Err(e)) => panic!("decode error: {e}"),
                None => return "closed_without_error",
            }
        }
    })
    .await;
    let outcome = outcome.expect("server never disconnected idle session");
    assert!(
        outcome == "idle_error_then_close" || outcome == "closed_without_error",
        "unexpected outcome: {outcome}"
    );

    server.abort();
}

// ─────────────────────────────────────────────────────────────────────
// Rate limiting
// ─────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn rate_limit_kicks_in_past_burst() {
    let server = spawn_server_with(ServerConfig {
        rate_limit_burst: 3.0,
        rate_limit_refill_per_sec: 0.0001, // effectively no refill in test span
        ..ServerConfig::default()
    })
    .await;

    let mut alice = handshake(server.addr, "alice").await;
    let mut bob = handshake(server.addr, "bob").await;
    // Drain bob's Joined for alice.
    let _ = timeout(T, alice.next()).await.unwrap().unwrap().unwrap();

    // Three messages should pass.
    for i in 0..3 {
        alice
            .send(ClientFrame::Send {
                body: format!("ok {i}"),
            })
            .await
            .unwrap();
        let m = timeout(T, bob.next()).await.unwrap().unwrap().unwrap();
        assert!(matches!(m, ServerFrame::Message { .. }), "msg {i}: {m:?}");
    }

    // The fourth blows the bucket.
    alice
        .send(ClientFrame::Send {
            body: "too fast".into(),
        })
        .await
        .unwrap();
    let resp = timeout(T, alice.next()).await.unwrap().unwrap().unwrap();
    assert!(
        matches!(resp, ServerFrame::Error { ref reason } if reason.contains("rate limit")),
        "expected rate-limit Error, got {resp:?}"
    );

    server.abort();
}
g
