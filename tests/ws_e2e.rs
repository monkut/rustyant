//! End-to-end WebSocket integration tests.
//!
//! Spawns a local TCP WebSocket server that stands in for API Gateway's
//! WebSocket front-door: for each incoming WS frame it builds a synthetic
//! `ApiGatewayWebsocketProxyRequest`, runs `rustyant::ws::handle` against
//! shared state, and sends the reply back on the same connection. Uses
//! `InMemoryStorage` so the tests are hermetic — no AWS, no floci.
//!
//! This closes the transport-level coverage gap left by `src/ws.rs::tests`:
//! those tests call `handle` directly with hand-constructed events; here we
//! round-trip real WebSocket frames over a TCP socket, which is what a
//! redis-py-style client (or any `tokio-tungstenite`-based caller) actually
//! does in production.

use std::net::SocketAddr;
use std::sync::Arc;

use aws_lambda_events::apigw::{ApiGatewayWebsocketProxyRequest, ApiGatewayWebsocketProxyRequestContext};
use base64::Engine;
use futures_util::{SinkExt, StreamExt};
use rustyant::State;
use rustyant::storage::InMemoryStorage;
use rustyant::ws::{WsOutcome, handle};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{WebSocketStream, accept_async, connect_async};

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

fn test_state() -> State {
    let settings = rustyant::Settings {
        bucket: "test".to_string(),
        key_prefix: "e2e/".to_string(),
        aws_region: None,
        aws_endpoint_url: None,
        emf_namespace: None,
    };
    State::with_storage(settings, Arc::new(InMemoryStorage::new()))
}

/// Spin up a local WS server that mimics API Gateway. Returns the
/// `ws://127.0.0.1:PORT` URL a test client should connect to.
async fn spawn_ws_server(state: State) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr: SocketAddr = listener.local_addr().expect("local_addr");

    tokio::spawn(async move {
        let mut conn_counter: u64 = 0;
        while let Ok((stream, _)) = listener.accept().await {
            conn_counter += 1;
            let conn_id = format!("sim-{conn_counter}");
            let state = state.clone();
            tokio::spawn(async move {
                if let Ok(ws) = accept_async(stream).await {
                    serve_connection(state, ws, conn_id).await;
                }
            });
        }
    });

    format!("ws://{addr}")
}

async fn serve_connection(state: State, ws: WebSocketStream<TcpStream>, connection_id: String) {
    let (mut sender, mut receiver) = ws.split();
    while let Some(msg) = receiver.next().await {
        let Ok(msg) = msg else { break };
        let bytes = match msg {
            Message::Binary(b) => b.to_vec(),
            Message::Text(s) => s.as_str().as_bytes().to_vec(),
            Message::Close(_) => break,
            _ => continue,
        };

        let event = build_default_event(&connection_id, &bytes);
        match handle(&state, event).await {
            Ok(WsOutcome::Reply { data, .. }) => {
                if sender.send(Message::Binary(data.into())).await.is_err() {
                    break;
                }
            }
            Ok(WsOutcome::Empty) => {}
            Err(_) => break,
        }
    }
}

fn build_default_event(connection_id: &str, body: &[u8]) -> ApiGatewayWebsocketProxyRequest {
    // API Gateway delivers binary WS frames as base64-encoded strings with
    // is_base64_encoded=true. Mirror that here so the handler's decode path
    // is exercised the same way as in production.
    let b64 = base64::engine::general_purpose::STANDARD.encode(body);
    let ctx = ApiGatewayWebsocketProxyRequestContext {
        route_key: Some("$default".to_string()),
        connection_id: Some(connection_id.to_string()),
        domain_name: Some("sim.local".to_string()),
        stage: Some("test".to_string()),
        ..ApiGatewayWebsocketProxyRequestContext::default()
    };
    ApiGatewayWebsocketProxyRequest {
        request_context: ctx,
        body: Some(b64),
        is_base64_encoded: true,
        ..ApiGatewayWebsocketProxyRequest::default()
    }
}

/// Connect to the simulator and return (sink, stream) halves.
async fn connect(url: &str) -> WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>> {
    let (ws, _) = connect_async(url).await.expect("ws connect");
    ws
}

/// Send one RESP command and receive one reply as raw bytes.
async fn roundtrip(url: &str, command: &[u8]) -> Vec<u8> {
    let mut ws = connect(url).await;
    ws.send(Message::Binary(command.to_vec().into())).await.expect("send");
    let msg = ws.next().await.expect("recv some").expect("recv ok");
    match msg {
        Message::Binary(b) => b.to_vec(),
        Message::Text(s) => s.as_str().as_bytes().to_vec(),
        other => panic!("unexpected frame: {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ws_ping_returns_pong() {
    let state = test_state();
    let url = spawn_ws_server(state).await;
    let reply = roundtrip(&url, b"*1\r\n$4\r\nPING\r\n").await;
    assert_eq!(&reply, b"+PONG\r\n");
}

#[tokio::test]
async fn ws_set_then_get_persists_across_frames() {
    let state = test_state();
    let url = spawn_ws_server(state).await;

    // Two separate WS connections would break persistence because each
    // connection gets a fresh dispatch chain — but the InMemoryStorage
    // is shared through `state`, so state survives across connections.
    let set = roundtrip(&url, b"*3\r\n$3\r\nSET\r\n$5\r\nhello\r\n$5\r\nworld\r\n").await;
    assert_eq!(&set, b"+OK\r\n");

    let got = roundtrip(&url, b"*2\r\n$3\r\nGET\r\n$5\r\nhello\r\n").await;
    assert_eq!(&got, b"$5\r\nworld\r\n");
}

#[tokio::test]
async fn ws_pipeline_three_commands_on_one_connection() {
    let state = test_state();
    let url = spawn_ws_server(state).await;
    let mut ws = connect(&url).await;

    let commands: &[&[u8]] = &[
        b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\n1\r\n",
        b"*2\r\n$4\r\nINCR\r\n$1\r\nk\r\n",
        b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n",
    ];
    for cmd in commands {
        ws.send(Message::Binary(cmd.to_vec().into())).await.expect("send");
    }

    // Replies arrive in order on the same connection.
    let expected: &[&[u8]] = &[b"+OK\r\n", b":2\r\n", b"$1\r\n2\r\n"];
    for want in expected {
        let msg = ws.next().await.expect("frame").expect("frame ok");
        let bytes = match msg {
            Message::Binary(b) => b.to_vec(),
            other => panic!("unexpected: {other:?}"),
        };
        assert_eq!(&bytes, want);
    }
}

#[tokio::test]
async fn ws_hash_roundtrip() {
    let state = test_state();
    let url = spawn_ws_server(state).await;

    let hset = roundtrip(&url, b"*6\r\n$4\r\nHSET\r\n$1\r\nh\r\n$1\r\na\r\n$2\r\nv1\r\n$1\r\nb\r\n$2\r\nv2\r\n").await;
    assert_eq!(&hset, b":2\r\n");

    let hgetall = roundtrip(&url, b"*2\r\n$7\r\nHGETALL\r\n$1\r\nh\r\n").await;
    // BTreeMap → keys ascending: a, b.
    assert_eq!(&hgetall, b"*4\r\n$1\r\na\r\n$2\r\nv1\r\n$1\r\nb\r\n$2\r\nv2\r\n");
}

#[tokio::test]
async fn ws_preserves_binary_bytes_in_values() {
    let state = test_state();
    let url = spawn_ws_server(state).await;

    // Value contains a NUL byte and a high byte — must survive the round trip
    // intact (base64 transport handles non-UTF-8).
    let binary_value = b"\x00\xff\x7f";
    let mut cmd = Vec::new();
    cmd.extend_from_slice(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$");
    cmd.extend_from_slice(binary_value.len().to_string().as_bytes());
    cmd.extend_from_slice(b"\r\n");
    cmd.extend_from_slice(binary_value);
    cmd.extend_from_slice(b"\r\n");
    let set = roundtrip(&url, &cmd).await;
    assert_eq!(&set, b"+OK\r\n");

    let got = roundtrip(&url, b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n").await;
    let mut expected = Vec::new();
    expected.extend_from_slice(b"$");
    expected.extend_from_slice(binary_value.len().to_string().as_bytes());
    expected.extend_from_slice(b"\r\n");
    expected.extend_from_slice(binary_value);
    expected.extend_from_slice(b"\r\n");
    assert_eq!(got, expected);
}

#[tokio::test]
async fn ws_unknown_command_returns_resp_error_not_connection_close() {
    let state = test_state();
    let url = spawn_ws_server(state).await;
    let mut ws = connect(&url).await;

    ws.send(Message::Binary(b"*1\r\n$4\r\nNOPE\r\n".to_vec().into())).await.expect("send");
    let first = ws.next().await.expect("frame").expect("ok");
    let bytes = match first {
        Message::Binary(b) => b.to_vec(),
        other => panic!("unexpected: {other:?}"),
    };
    assert!(bytes.starts_with(b"-"), "expected RESP error, got {:?}", String::from_utf8_lossy(&bytes));

    // Connection must still be usable afterwards.
    ws.send(Message::Binary(b"*1\r\n$4\r\nPING\r\n".to_vec().into())).await.expect("send ping");
    let pong = ws.next().await.expect("frame").expect("ok");
    let pong_bytes = match pong {
        Message::Binary(b) => b.to_vec(),
        other => panic!("unexpected: {other:?}"),
    };
    assert_eq!(&pong_bytes, b"+PONG\r\n");
}
