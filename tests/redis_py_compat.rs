//! redis-py compatibility tests.
//!
//! Spawns a local TCP server that speaks RESP2 and dispatches each incoming
//! frame through `rustyant::commands::dispatch` against a floci-backed
//! `S3Storage`. Then drives a real `redis-py` (`redis.Redis(host='127.0.0.1',
//! port=N)`) via a Python subprocess against the TCP port. This proves
//! rustyant's command dispatch is wire-compatible with the de-facto-standard
//! Python client.
//!
//! The tests skip cleanly when Python 3 with `redis` is not installed.
//! They also require `RUSTYANT_FLOCI_URL` — see the module doc in
//! `tests/integration.rs` for the rationale.
//!
//! Scope caveat: this test harness is a TCP RESP2 server, NOT a rustyant
//! production deployment. Real deployments run the handler behind API
//! Gateway (WebSocket) or a Lambda Function URL (HTTP) — neither of which
//! speaks raw TCP. Plugging redis-py directly into a deployed rustyant
//! requires a TCP-to-HTTP/WS bridge (NLB + Fargate on the server side, or
//! a local shim on the client side). This harness verifies rustyant's
//! business logic accepts exactly what redis-py emits and returns exactly
//! what redis-py parses.

use std::net::SocketAddr;
use std::process::{Command, Output};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use redis_protocol::resp2::decode::decode_bytes_mut;
use redis_protocol::resp2::types::BytesFrame;
use rustyant::commands;
use rustyant::resp::RespReply;
use rustyant::state::State;
use rustyant::test_support::floci_state;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

// ---------------------------------------------------------------------------
// Server harness — TCP, streaming RESP2 decode, delegates to rustyant dispatch
// ---------------------------------------------------------------------------

fn test_state() -> State {
    floci_state("redis-py")
}

async fn spawn_tcp_server(state: State) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let state = state.clone();
            tokio::spawn(async move { serve_connection(state, stream).await });
        }
    });
    addr
}

async fn serve_connection(state: State, stream: TcpStream) {
    let (mut reader, mut writer) = stream.into_split();
    let mut buf = BytesMut::with_capacity(4096);
    let mut chunk = [0u8; 4096];

    loop {
        // Drain every complete frame currently in the buffer.
        loop {
            let parsed = match decode_bytes_mut(&mut buf) {
                Ok(Some((frame, _consumed, _remaining))) => frame,
                Ok(None) => break,
                Err(_) => return,
            };
            let Some(argv) = frame_to_argv(parsed) else {
                return;
            };
            let reply = dispatch_with_stubs(&state, argv).await;
            let Ok(encoded) = reply.encode() else {
                return;
            };
            if writer.write_all(&encoded).await.is_err() {
                return;
            }
        }

        // Need more bytes. EOF (Ok(0)) and read errors both end the session.
        let n = match reader.read(&mut chunk).await {
            Ok(n) if n > 0 => n,
            _ => return,
        };
        buf.extend_from_slice(&chunk[..n]);
    }
}

fn frame_to_argv(frame: BytesFrame) -> Option<Vec<Bytes>> {
    match frame {
        BytesFrame::Array(items) => {
            let mut argv = Vec::with_capacity(items.len());
            for item in items {
                match item {
                    BytesFrame::BulkString(b) | BytesFrame::SimpleString(b) => argv.push(b),
                    _ => return None,
                }
            }
            Some(argv)
        }
        _ => None,
    }
}

/// redis-py issues a few bookkeeping commands on connection setup that
/// rustyant doesn't implement: `HELLO` for protocol negotiation and
/// `CLIENT SETINFO` to register client library metadata. Neither is a real
/// rustyant concern, but returning our generic `-ERR unknown command` on
/// them makes the redis-py `Retry`-on-connect logic noisy. Stub them here in
/// the test harness only; production dispatch is untouched.
async fn dispatch_with_stubs(state: &State, argv: Vec<Bytes>) -> RespReply {
    if let Some(first) = argv.first() {
        if first.eq_ignore_ascii_case(b"HELLO") {
            // Returning an error lets redis-py fall back to RESP2 without
            // erroring up to the caller. This matches what a pre-HELLO
            // Redis server (< 6.0) returns.
            return RespReply::err("ERR unknown command 'HELLO'");
        }
        if first.eq_ignore_ascii_case(b"CLIENT") {
            // CLIENT SETINFO / CLIENT GETNAME etc. — just return OK.
            return RespReply::ok();
        }
    }
    commands::dispatch(state, argv).await
}

// ---------------------------------------------------------------------------
// Python subprocess helpers
// ---------------------------------------------------------------------------

fn python_with_redis_available() -> bool {
    Command::new("python3").args(["-c", "import redis"]).output().is_ok_and(|o| o.status.success())
}

fn run_py(script: &str) -> Output {
    Command::new("python3").arg("-c").arg(script).output().expect("exec python3")
}

fn assert_py_ok(script: &str) {
    let out = run_py(script);
    assert!(
        out.status.success(),
        "python failed\n---- stderr ----\n{}\n---- stdout ----\n{}",
        String::from_utf8_lossy(&out.stderr),
        String::from_utf8_lossy(&out.stdout),
    );
    assert_eq!(
        String::from_utf8_lossy(&out.stdout).trim(),
        "ok",
        "unexpected stdout; stderr was:\n{}",
        String::from_utf8_lossy(&out.stderr),
    );
}

/// Wraps a test body with the common boilerplate: skip when Python / redis-py
/// are missing, otherwise spin up a server and feed the given Python script
/// (with `{port}` filled in).
async fn run_redis_py_script(script_template: &str) {
    if !python_with_redis_available() {
        eprintln!("SKIP: python3 with redis-py not available");
        return;
    }
    let state = test_state();
    let addr = spawn_tcp_server(state).await;
    // Give the listener a tick to be ready before redis-py dials.
    tokio::time::sleep(Duration::from_millis(25)).await;
    let script = script_template.replace("{port}", &addr.port().to_string());
    assert_py_ok(&script);
}

// ---------------------------------------------------------------------------
// Tests — real redis-py against rustyant dispatch
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redis_py_ping_returns_true() {
    run_redis_py_script(
        r"
import redis
r = redis.Redis(host='127.0.0.1', port={port}, socket_timeout=5)
assert r.ping() is True
print('ok')
",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redis_py_string_set_get_delete_exists() {
    run_redis_py_script(
        r"
import redis
r = redis.Redis(host='127.0.0.1', port={port}, socket_timeout=5)
assert r.set('hello', 'world') is True
assert r.get('hello') == b'world'
assert r.exists('hello', 'missing') == 1
assert r.delete('hello') == 1
assert r.get('hello') is None
print('ok')
",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redis_py_incr_incrby() {
    run_redis_py_script(
        r"
import redis
r = redis.Redis(host='127.0.0.1', port={port}, socket_timeout=5)
assert r.incr('counter') == 1
assert r.incr('counter') == 2
assert r.incrby('counter', 10) == 12
assert r.incrby('counter', -5) == 7
print('ok')
",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redis_py_ttl_expire_setex() {
    run_redis_py_script(
        r"
import redis
r = redis.Redis(host='127.0.0.1', port={port}, socket_timeout=5)
r.set('k', 'v')
assert r.ttl('k') == -1
assert r.expire('k', 120) == 1
t = r.ttl('k')
assert 110 <= t <= 120, 'ttl=' + repr(t)
r.setex('se', 60, 'v2')
assert r.get('se') == b'v2'
print('ok')
",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redis_py_mget_mset() {
    run_redis_py_script(
        r"
import redis
r = redis.Redis(host='127.0.0.1', port={port}, socket_timeout=5)
r.mset({'a': '1', 'b': '2', 'c': '3'})
assert r.mget('a', 'b', 'missing', 'c') == [b'1', b'2', None, b'3']
print('ok')
",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redis_py_setnx_create_only() {
    // SETNX's "second call returns False" branch depends on S3's
    // `If-None-Match: *` conditional write, which floci does not enforce.
    // Same gate as `tests/floci.rs::s3_concurrent_incr_converges`.
    if std::env::var("RUSTYANT_S3_CAS").is_err() {
        eprintln!("SKIP: RUSTYANT_S3_CAS not set (floci does not enforce If-None-Match)");
        return;
    }
    run_redis_py_script(
        r"
import redis
r = redis.Redis(host='127.0.0.1', port={port}, socket_timeout=5)
assert r.setnx('k', 'first') == True
assert r.setnx('k', 'second') == False
assert r.get('k') == b'first'
print('ok')
",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redis_py_hash_ops() {
    run_redis_py_script(
        r"
import redis
r = redis.Redis(host='127.0.0.1', port={port}, socket_timeout=5)
r.hset('profile', mapping={'name': 'alice', 'age': '30'})
assert r.hget('profile', 'name') == b'alice'
assert r.hgetall('profile') == {b'name': b'alice', b'age': b'30'}
assert r.hlen('profile') == 2
assert sorted(r.hkeys('profile')) == [b'age', b'name']
assert r.hexists('profile', 'name') == 1
assert r.hexists('profile', 'missing') == 0
assert r.hincrby('profile', 'age', 1) == 31
assert r.hmget('profile', 'name', 'missing', 'age') == [b'alice', None, b'31']
assert r.hdel('profile', 'age') == 1
assert r.hlen('profile') == 1
print('ok')
",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redis_py_list_ops() {
    run_redis_py_script(
        r"
import redis
r = redis.Redis(host='127.0.0.1', port={port}, socket_timeout=5)
r.rpush('q', 'a', 'b', 'c')
r.lpush('q', 'zero')
assert r.lrange('q', 0, -1) == [b'zero', b'a', b'b', b'c']
assert r.llen('q') == 4
assert r.lpop('q') == b'zero'
assert r.rpop('q') == b'c'
assert r.lrange('q', 0, -1) == [b'a', b'b']
print('ok')
",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redis_py_set_ops() {
    run_redis_py_script(
        r"
import redis
r = redis.Redis(host='127.0.0.1', port={port}, socket_timeout=5)
assert r.sadd('s', 'a', 'b', 'c', 'a') == 3
assert r.scard('s') == 3
assert sorted(r.smembers('s')) == [b'a', b'b', b'c']
assert r.sismember('s', 'a') == 1
assert r.sismember('s', 'z') == 0
assert r.srem('s', 'a', 'missing') == 1
assert r.scard('s') == 2
print('ok')
",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redis_py_zset_ops() {
    run_redis_py_script(
        r"
import redis
r = redis.Redis(host='127.0.0.1', port={port}, socket_timeout=5)
assert r.zadd('scores', {'alice': 10, 'bob': 5, 'carol': 20}) == 3
assert r.zrange('scores', 0, -1) == [b'bob', b'alice', b'carol']
assert r.zcard('scores') == 3
assert float(r.zscore('scores', 'alice')) == 10.0
assert r.zscore('scores', 'missing') is None
r.zincrby('scores', 100, 'bob')
assert r.zrange('scores', 0, -1) == [b'alice', b'carol', b'bob']
assert r.zrem('scores', 'bob', 'missing') == 1
print('ok')
",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redis_py_server_housekeeping() {
    // Cover the new server / keyspace housekeeping commands through redis-py
    // so any encoding mismatch in ECHO / TIME / DBSIZE / FLUSHDB / RANDOMKEY /
    // UNLINK / COPY surfaces against the real client's parser.
    run_redis_py_script(
        r"
import redis
r = redis.Redis(host='127.0.0.1', port={port}, socket_timeout=5)
assert r.echo('ping me') == b'ping me'
secs, micros = r.time()
assert isinstance(secs, int) and secs > 1_700_000_000
assert isinstance(micros, int) and 0 <= micros < 1_000_000
r.set('a', '1'); r.set('b', '2')
assert r.dbsize() == 2
key = r.randomkey()
assert key in (b'a', b'b')
assert r.unlink('a', 'b', 'missing') == 2
assert r.dbsize() == 0
r.set('src', 'v')
assert r.copy('src', 'dst') is True
assert r.get('dst') == b'v'
assert r.copy('src', 'dst') is False
assert r.copy('src', 'dst', replace=True) is True
r.flushdb()
assert r.dbsize() == 0
print('ok')
",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redis_py_bit_ops() {
    // Drive every new bit-op surface through redis-py so any encoding
    // mismatch in GETBIT / SETBIT / BITCOUNT / BITPOS / BITOP shows up.
    run_redis_py_script(
        r"
import redis
r = redis.Redis(host='127.0.0.1', port={port}, socket_timeout=5)
# SETBIT auto-creates and zero-pads; previous value reported.
assert r.setbit('k', 7, 1) == 0
assert r.setbit('k', 7, 1) == 1
assert r.getbit('k', 7) == 1
assert r.getbit('k', 0) == 0
assert r.getbit('k', 100) == 0
r.set('s', 'foobar')
assert r.bitcount('s') == 26
assert r.bitcount('s', 0, 0) == 4
r.set('a', b'\xff\xff')
r.set('b', b'\x0f')
assert r.bitop('AND', 'dst', 'a', 'b') == 2
assert r.get('dst') == b'\x0f\x00'
assert r.bitop('NOT', 'inv', 'b') == 1
assert r.get('inv') == b'\xf0'
r.set('z', b'\xff\xf0')
assert r.bitpos('z', 0) == 12
print('ok')
",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redis_py_collection_scans() {
    // redis-py's `*scan_iter` helpers call the underlying command until the
    // cursor returns to 0 — the contract we implement. Running through them
    // catches cursor-string / pair-flattening / score-formatting mismatches
    // that a unit test against our own parser would miss.
    run_redis_py_script(
        r"
import redis
r = redis.Redis(host='127.0.0.1', port={port}, socket_timeout=5)

r.hset('h', mapping={'user:1': 'a', 'user:2': 'b', 'other': 'c'})
assert dict(r.hscan_iter('h')) == {b'user:1': b'a', b'user:2': b'b', b'other': b'c'}
assert dict(r.hscan_iter('h', match='user:*')) == {b'user:1': b'a', b'user:2': b'b'}
cursor, page = r.hscan('h', 0, count=100)
assert cursor == 0
assert dict(zip(page.keys(), page.values())) == {b'user:1': b'a', b'user:2': b'b', b'other': b'c'}

r.sadd('s', 'alpha', 'beta', 'gamma')
assert set(r.sscan_iter('s')) == {b'alpha', b'beta', b'gamma'}
assert set(r.sscan_iter('s', match='a*')) == {b'alpha'}

r.zadd('z', {'alice': 1.5, 'bob': 2, 'carol': 3.25})
assert dict(r.zscan_iter('z')) == {b'alice': 1.5, b'bob': 2.0, b'carol': 3.25}
print('ok')
",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redis_py_pipeline() {
    // redis-py's pipeline batches commands and reads replies in order.
    // This exercises the TCP streaming path with multiple frames in flight.
    run_redis_py_script(
        r"
import redis
r = redis.Redis(host='127.0.0.1', port={port}, socket_timeout=5)
with r.pipeline(transaction=False) as p:
    p.set('k', 'v')
    p.incr('counter')
    p.incr('counter')
    p.get('k')
    results = p.execute()
assert results[0] is True
assert results[1] == 1
assert results[2] == 2
assert results[3] == b'v'
print('ok')
",
    )
    .await;
}
