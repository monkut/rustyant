//! End-to-end tests for the RESP-over-HTTP Lambda handler backed by an
//! in-memory `Storage` implementation. The S3-backed path is exercised
//! manually via `just floci-up` + `just rustyant-dev`.

use std::sync::Arc;

use bytes::Bytes;
use lambda_http::http::Request as HttpRequest;
use lambda_http::{Body, Request, Response};
use rustyant::Settings;
use rustyant::handler::handle;
use rustyant::resp::{RespReply, parse_command};
use rustyant::state::State;
use rustyant::storage::InMemoryStorage;

// ---------------------------------------------------------------------------
// Test harness helpers
// ---------------------------------------------------------------------------

fn test_state() -> State {
    let settings = Settings {
        bucket: "test-bucket".to_string(),
        key_prefix: "test/".to_string(),
        aws_region: None,
        aws_endpoint_url: None,
        emf_namespace: None,
    };
    State::with_storage(settings, Arc::new(InMemoryStorage::new()))
}

/// Build a RESP-array request from a slice of arg byte-slices.
fn resp_request(args: &[&[u8]]) -> Request {
    let mut body = Vec::new();
    body.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
    for a in args {
        body.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
        body.extend_from_slice(a);
        body.extend_from_slice(b"\r\n");
    }
    HttpRequest::builder()
        .method("POST")
        .uri("/")
        .header("content-type", "application/resp")
        .body(Body::Binary(body))
        .expect("build request")
}

/// Drive one command through the handler and decode the reply.
async fn call(state: &State, args: &[&[u8]]) -> DecodedReply {
    let resp: Response<Body> = handle(state.clone(), resp_request(args)).await.expect("handler");
    let status = resp.status().as_u16();
    let body: Vec<u8> = match resp.into_body() {
        Body::Empty => Vec::new(),
        Body::Text(s) => s.into_bytes(),
        Body::Binary(b) => b,
    };
    DecodedReply::from_bytes(status, &body)
}

#[derive(Debug)]
struct DecodedReply {
    status: u16,
    raw: Vec<u8>,
}

impl DecodedReply {
    fn from_bytes(status: u16, body: &[u8]) -> Self {
        Self { status, raw: body.to_vec() }
    }

    fn expect_simple(&self, want: &str) {
        let expected = format!("+{want}\r\n");
        assert_eq!(
            self.raw,
            expected.as_bytes(),
            "status={} body={:?}",
            self.status,
            String::from_utf8_lossy(&self.raw)
        );
    }

    fn expect_integer(&self, want: i64) {
        let expected = format!(":{want}\r\n");
        assert_eq!(self.raw, expected.as_bytes(), "body={:?}", String::from_utf8_lossy(&self.raw));
    }

    fn expect_bulk(&self, want: &[u8]) {
        let mut expected = format!("${}\r\n", want.len()).into_bytes();
        expected.extend_from_slice(want);
        expected.extend_from_slice(b"\r\n");
        assert_eq!(self.raw, expected, "body={:?}", String::from_utf8_lossy(&self.raw));
    }

    fn expect_nil(&self) {
        assert_eq!(self.raw, b"$-1\r\n", "body={:?}", String::from_utf8_lossy(&self.raw));
    }

    fn expect_error_prefix(&self, prefix: &str) {
        assert!(self.raw.starts_with(b"-"), "not an error: {:?}", String::from_utf8_lossy(&self.raw));
        let as_str = String::from_utf8_lossy(&self.raw);
        assert!(as_str.contains(prefix), "expected error containing {prefix:?}, got {as_str:?}");
    }

    /// Decode as an array of bulk strings.
    fn into_bulk_array(self) -> Vec<Bytes> {
        parse_command(&self.raw).expect("parse array")
    }
}

// ---------------------------------------------------------------------------
// Server / connection
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ping_returns_pong() {
    let state = test_state();
    call(&state, &[b"PING"]).await.expect_simple("PONG");
}

#[tokio::test]
async fn unknown_command_returns_error() {
    let state = test_state();
    call(&state, &[b"NOPE"]).await.expect_error_prefix("ERR");
}

#[tokio::test]
async fn malformed_body_returns_parse_error() {
    let state = test_state();
    let resp = handle(
        state,
        HttpRequest::builder()
            .method("POST")
            .uri("/")
            .body(Body::Binary(b"*2\r\n$3\r\nSET\r\n".to_vec()))
            .expect("build"),
    )
    .await
    .expect("handler");
    assert_eq!(resp.status().as_u16(), 400);
    let body: Vec<u8> = match resp.into_body() {
        Body::Binary(b) => b,
        _ => panic!("expected binary body"),
    };
    assert!(body.starts_with(b"-"), "expected error reply, got {:?}", String::from_utf8_lossy(&body));
}

// ---------------------------------------------------------------------------
// Strings
// ---------------------------------------------------------------------------

#[tokio::test]
async fn set_then_get_roundtrip() {
    let state = test_state();
    call(&state, &[b"SET", b"hello", b"world"]).await.expect_simple("OK");
    call(&state, &[b"GET", b"hello"]).await.expect_bulk(b"world");
}

#[tokio::test]
async fn get_missing_key_returns_nil() {
    let state = test_state();
    call(&state, &[b"GET", b"missing"]).await.expect_nil();
}

#[tokio::test]
async fn del_counts_existing_keys() {
    let state = test_state();
    call(&state, &[b"SET", b"a", b"1"]).await;
    call(&state, &[b"SET", b"b", b"2"]).await;
    call(&state, &[b"DEL", b"a", b"b", b"missing"]).await.expect_integer(2);
    call(&state, &[b"EXISTS", b"a", b"b"]).await.expect_integer(0);
}

#[tokio::test]
async fn exists_counts_present_keys() {
    let state = test_state();
    call(&state, &[b"SET", b"a", b"1"]).await;
    call(&state, &[b"SET", b"b", b"2"]).await;
    call(&state, &[b"EXISTS", b"a", b"b", b"missing", b"a"]).await.expect_integer(3);
}

#[tokio::test]
async fn incr_on_missing_key_starts_at_one() {
    let state = test_state();
    call(&state, &[b"INCR", b"counter"]).await.expect_integer(1);
    call(&state, &[b"INCR", b"counter"]).await.expect_integer(2);
    call(&state, &[b"INCRBY", b"counter", b"10"]).await.expect_integer(12);
}

#[tokio::test]
async fn incr_on_non_integer_errors() {
    let state = test_state();
    call(&state, &[b"SET", b"k", b"notanumber"]).await;
    call(&state, &[b"INCR", b"k"]).await.expect_error_prefix("ERR");
}

#[tokio::test]
async fn decr_on_missing_key_starts_at_minus_one() {
    let state = test_state();
    call(&state, &[b"DECR", b"c"]).await.expect_integer(-1);
    call(&state, &[b"DECR", b"c"]).await.expect_integer(-2);
    call(&state, &[b"DECRBY", b"c", b"10"]).await.expect_integer(-12);
}

#[tokio::test]
async fn decrby_negative_value_increments() {
    // Redis semantics: DECRBY k -5 is the same as INCRBY k 5.
    let state = test_state();
    call(&state, &[b"SET", b"n", b"10"]).await;
    call(&state, &[b"DECRBY", b"n", b"-5"]).await.expect_integer(15);
}

#[tokio::test]
async fn getdel_returns_value_and_removes_key() {
    let state = test_state();
    call(&state, &[b"SET", b"token", b"abc123"]).await;
    call(&state, &[b"GETDEL", b"token"]).await.expect_bulk(b"abc123");
    call(&state, &[b"EXISTS", b"token"]).await.expect_integer(0);
    call(&state, &[b"GET", b"token"]).await.expect_nil();
}

#[tokio::test]
async fn getdel_on_missing_key_returns_nil() {
    let state = test_state();
    call(&state, &[b"GETDEL", b"ghost"]).await.expect_nil();
}

#[tokio::test]
async fn getdel_on_wrong_type_errors_and_preserves_key() {
    let state = test_state();
    call(&state, &[b"LPUSH", b"queue", b"item"]).await;
    call(&state, &[b"GETDEL", b"queue"]).await.expect_error_prefix("ERR");
    call(&state, &[b"EXISTS", b"queue"]).await.expect_integer(1);
}

#[tokio::test]
async fn strlen_returns_byte_length() {
    let state = test_state();
    call(&state, &[b"SET", b"k", b"hello"]).await;
    call(&state, &[b"STRLEN", b"k"]).await.expect_integer(5);
}

#[tokio::test]
async fn strlen_on_missing_key_returns_zero() {
    let state = test_state();
    call(&state, &[b"STRLEN", b"ghost"]).await.expect_integer(0);
}

#[tokio::test]
async fn strlen_on_wrong_type_errors() {
    let state = test_state();
    call(&state, &[b"LPUSH", b"queue", b"item"]).await;
    call(&state, &[b"STRLEN", b"queue"]).await.expect_error_prefix("ERR");
}

#[tokio::test]
async fn append_to_missing_key_creates_string() {
    let state = test_state();
    call(&state, &[b"APPEND", b"k", b"hello"]).await.expect_integer(5);
    call(&state, &[b"GET", b"k"]).await.expect_bulk(b"hello");
}

#[tokio::test]
async fn append_to_existing_string_concatenates() {
    let state = test_state();
    call(&state, &[b"SET", b"k", b"hello"]).await;
    call(&state, &[b"APPEND", b"k", b" world"]).await.expect_integer(11);
    call(&state, &[b"GET", b"k"]).await.expect_bulk(b"hello world");
}

#[tokio::test]
async fn append_on_wrong_type_errors_and_preserves_key() {
    let state = test_state();
    call(&state, &[b"LPUSH", b"queue", b"item"]).await;
    call(&state, &[b"APPEND", b"queue", b"x"]).await.expect_error_prefix("ERR");
    call(&state, &[b"LLEN", b"queue"]).await.expect_integer(1);
}

#[tokio::test]
async fn set_with_ex_sets_ttl() {
    let state = test_state();
    call(&state, &[b"SET", b"k", b"v", b"EX", b"60"]).await.expect_simple("OK");
    let reply = call(&state, &[b"TTL", b"k"]).await;
    // TTL returns seconds remaining — we just set 60s so expect ~60.
    let raw = String::from_utf8_lossy(&reply.raw);
    assert!(raw.starts_with(':'), "expected integer reply, got {raw:?}");
    let n: i64 = raw.trim_start_matches(':').trim_end().parse().expect("parse int");
    assert!((55..=60).contains(&n), "TTL out of expected range: {n}");
}

#[tokio::test]
async fn ttl_returns_minus_two_for_missing_key() {
    let state = test_state();
    call(&state, &[b"TTL", b"missing"]).await.expect_integer(-2);
}

#[tokio::test]
async fn ttl_returns_minus_one_for_no_expiry() {
    let state = test_state();
    call(&state, &[b"SET", b"k", b"v"]).await;
    call(&state, &[b"TTL", b"k"]).await.expect_integer(-1);
}

#[tokio::test]
async fn expire_on_existing_key_returns_one() {
    let state = test_state();
    call(&state, &[b"SET", b"k", b"v"]).await;
    call(&state, &[b"EXPIRE", b"k", b"100"]).await.expect_integer(1);
}

#[tokio::test]
async fn expire_on_missing_key_returns_zero() {
    let state = test_state();
    call(&state, &[b"EXPIRE", b"nope", b"100"]).await.expect_integer(0);
}

#[tokio::test]
async fn expireat_accepts_absolute_unix_seconds() {
    let state = test_state();
    call(&state, &[b"SET", b"k", b"v"]).await;
    // 100s from now → unix seconds roughly now + 100.
    let target = (rustyant::storage::now_ms() / 1000 + 100).to_string();
    call(&state, &[b"EXPIREAT", b"k", target.as_bytes()]).await.expect_integer(1);
    // TTL should be in (0, 100].
    let reply = call(&state, &[b"TTL", b"k"]).await;
    let raw = String::from_utf8_lossy(&reply.raw);
    let n: i64 = raw.trim_start_matches(':').trim_end().parse().expect("parse int");
    assert!((1..=100).contains(&n), "TTL out of range: {n}");
}

#[tokio::test]
async fn pexpireat_accepts_absolute_unix_milliseconds() {
    let state = test_state();
    call(&state, &[b"SET", b"k", b"v"]).await;
    // Past timestamp → key expires immediately.
    call(&state, &[b"PEXPIREAT", b"k", b"1"]).await.expect_integer(1);
    call(&state, &[b"GET", b"k"]).await.expect_nil();
}

#[tokio::test]
async fn expireat_on_missing_key_returns_zero() {
    let state = test_state();
    let future = (rustyant::storage::now_ms() / 1000 + 60).to_string();
    call(&state, &[b"EXPIREAT", b"missing", future.as_bytes()]).await.expect_integer(0);
}

// ---------------------------------------------------------------------------
// Hashes
// ---------------------------------------------------------------------------

#[tokio::test]
async fn hset_returns_new_field_count() {
    let state = test_state();
    call(&state, &[b"HSET", b"h", b"f1", b"v1", b"f2", b"v2"]).await.expect_integer(2);
    // Overwriting existing fields doesn't count as new.
    call(&state, &[b"HSET", b"h", b"f1", b"v1b", b"f3", b"v3"]).await.expect_integer(1);
}

#[tokio::test]
async fn hget_returns_value_or_nil() {
    let state = test_state();
    call(&state, &[b"HSET", b"h", b"name", b"alice"]).await;
    call(&state, &[b"HGET", b"h", b"name"]).await.expect_bulk(b"alice");
    call(&state, &[b"HGET", b"h", b"missing"]).await.expect_nil();
}

#[tokio::test]
async fn hdel_returns_removed_count() {
    let state = test_state();
    call(&state, &[b"HSET", b"h", b"a", b"1", b"b", b"2", b"c", b"3"]).await;
    call(&state, &[b"HDEL", b"h", b"a", b"b", b"missing"]).await.expect_integer(2);
}

#[tokio::test]
async fn hgetall_returns_flat_array() {
    let state = test_state();
    call(&state, &[b"HSET", b"h", b"b", b"two", b"a", b"one"]).await;
    let items = call(&state, &[b"HGETALL", b"h"]).await.into_bulk_array();
    // BTreeMap ordering → keys ascending: a, b.
    assert_eq!(items.len(), 4);
    assert_eq!(items[0].as_ref(), b"a");
    assert_eq!(items[1].as_ref(), b"one");
    assert_eq!(items[2].as_ref(), b"b");
    assert_eq!(items[3].as_ref(), b"two");
}

#[tokio::test]
async fn hget_on_string_errors_wrong_type() {
    let state = test_state();
    call(&state, &[b"SET", b"k", b"v"]).await;
    call(&state, &[b"HGET", b"k", b"field"]).await.expect_error_prefix("ERR");
}

// ---------------------------------------------------------------------------
// Lists
// ---------------------------------------------------------------------------

#[tokio::test]
async fn lpush_inserts_at_head() {
    let state = test_state();
    // LPUSH with multiple values: each gets inserted at head in order.
    call(&state, &[b"LPUSH", b"l", b"a", b"b", b"c"]).await.expect_integer(3);
    // After LPUSH a, b, c the list is: c, b, a
    let items = call(&state, &[b"LRANGE", b"l", b"0", b"-1"]).await.into_bulk_array();
    assert_eq!(items.len(), 3);
    assert_eq!(items[0].as_ref(), b"c");
    assert_eq!(items[1].as_ref(), b"b");
    assert_eq!(items[2].as_ref(), b"a");
}

#[tokio::test]
async fn rpush_appends_to_tail() {
    let state = test_state();
    call(&state, &[b"RPUSH", b"l", b"a", b"b", b"c"]).await.expect_integer(3);
    let items = call(&state, &[b"LRANGE", b"l", b"0", b"-1"]).await.into_bulk_array();
    assert_eq!(items.len(), 3);
    assert_eq!(items[0].as_ref(), b"a");
    assert_eq!(items[2].as_ref(), b"c");
}

#[tokio::test]
async fn lpop_returns_head_element() {
    let state = test_state();
    call(&state, &[b"RPUSH", b"l", b"a", b"b", b"c"]).await;
    call(&state, &[b"LPOP", b"l"]).await.expect_bulk(b"a");
}

#[tokio::test]
async fn rpop_returns_tail_element() {
    let state = test_state();
    call(&state, &[b"RPUSH", b"l", b"a", b"b", b"c"]).await;
    call(&state, &[b"RPOP", b"l"]).await.expect_bulk(b"c");
}

#[tokio::test]
async fn lpop_with_count_returns_array() {
    let state = test_state();
    call(&state, &[b"RPUSH", b"l", b"a", b"b", b"c", b"d"]).await;
    let items = call(&state, &[b"LPOP", b"l", b"2"]).await.into_bulk_array();
    assert_eq!(items.len(), 2);
    assert_eq!(items[0].as_ref(), b"a");
    assert_eq!(items[1].as_ref(), b"b");
    // Remaining list should be c, d.
    let tail = call(&state, &[b"LRANGE", b"l", b"0", b"-1"]).await.into_bulk_array();
    assert_eq!(tail.len(), 2);
    assert_eq!(tail[0].as_ref(), b"c");
}

#[tokio::test]
async fn lrange_negative_indices() {
    let state = test_state();
    call(&state, &[b"RPUSH", b"l", b"a", b"b", b"c", b"d", b"e"]).await;
    // Last two elements.
    let items = call(&state, &[b"LRANGE", b"l", b"-2", b"-1"]).await.into_bulk_array();
    assert_eq!(items.len(), 2);
    assert_eq!(items[0].as_ref(), b"d");
    assert_eq!(items[1].as_ref(), b"e");
}

#[tokio::test]
async fn lpop_empty_list_returns_nil() {
    let state = test_state();
    call(&state, &[b"LPOP", b"missing"]).await.expect_nil();
}

// ---------------------------------------------------------------------------
// Sets
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sadd_deduplicates() {
    let state = test_state();
    call(&state, &[b"SADD", b"s", b"x", b"y", b"z"]).await.expect_integer(3);
    // Adding existing members returns 0 new additions.
    call(&state, &[b"SADD", b"s", b"y", b"w"]).await.expect_integer(1);
}

#[tokio::test]
async fn sadd_on_string_errors_wrong_type() {
    let state = test_state();
    call(&state, &[b"SET", b"k", b"v"]).await;
    call(&state, &[b"SADD", b"k", b"member"]).await.expect_error_prefix("ERR");
}

// ---------------------------------------------------------------------------
// Sorted sets
// ---------------------------------------------------------------------------

#[tokio::test]
async fn zadd_zrange_sorts_by_score() {
    let state = test_state();
    call(&state, &[b"ZADD", b"z", b"3", b"c", b"1", b"a", b"2", b"b"]).await.expect_integer(3);
    let members = call(&state, &[b"ZRANGE", b"z", b"0", b"-1"]).await.into_bulk_array();
    assert_eq!(members.len(), 3);
    assert_eq!(members[0].as_ref(), b"a");
    assert_eq!(members[1].as_ref(), b"b");
    assert_eq!(members[2].as_ref(), b"c");
}

#[tokio::test]
async fn zadd_updates_existing_score() {
    let state = test_state();
    call(&state, &[b"ZADD", b"z", b"1", b"a", b"2", b"b"]).await.expect_integer(2);
    // Updating an existing member's score doesn't count as new.
    call(&state, &[b"ZADD", b"z", b"10", b"a"]).await.expect_integer(0);
    let members = call(&state, &[b"ZRANGE", b"z", b"0", b"-1"]).await.into_bulk_array();
    // After re-scoring: b (2), a (10) — order flipped.
    assert_eq!(members[0].as_ref(), b"b");
    assert_eq!(members[1].as_ref(), b"a");
}

// ---------------------------------------------------------------------------
// Arity / error surface
// ---------------------------------------------------------------------------

#[tokio::test]
async fn set_without_value_errors() {
    let state = test_state();
    call(&state, &[b"SET", b"k"]).await.expect_error_prefix("ERR");
}

#[tokio::test]
async fn hget_wrong_arity_errors() {
    let state = test_state();
    call(&state, &[b"HGET", b"k"]).await.expect_error_prefix("ERR");
}

#[tokio::test]
async fn zadd_odd_args_errors() {
    let state = test_state();
    call(&state, &[b"ZADD", b"z", b"1"]).await.expect_error_prefix("ERR");
}

// ---------------------------------------------------------------------------
// String multi-key + NX/EX (SETNX, SETEX, MGET, MSET)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn setnx_sets_missing_key() {
    let state = test_state();
    call(&state, &[b"SETNX", b"k", b"v1"]).await.expect_integer(1);
    call(&state, &[b"GET", b"k"]).await.expect_bulk(b"v1");
    // Second SETNX must not overwrite.
    call(&state, &[b"SETNX", b"k", b"v2"]).await.expect_integer(0);
    call(&state, &[b"GET", b"k"]).await.expect_bulk(b"v1");
}

#[tokio::test]
async fn setex_sets_value_with_ttl() {
    let state = test_state();
    call(&state, &[b"SETEX", b"k", b"120", b"v"]).await.expect_simple("OK");
    call(&state, &[b"GET", b"k"]).await.expect_bulk(b"v");
    // TTL should be ~120s.
    let reply = call(&state, &[b"TTL", b"k"]).await;
    let raw = String::from_utf8_lossy(&reply.raw);
    let n: i64 = raw.trim_start_matches(':').trim_end().parse().expect("int");
    assert!((110..=120).contains(&n), "TTL out of range: {n}");
}

#[tokio::test]
async fn setex_rejects_non_positive_seconds() {
    let state = test_state();
    call(&state, &[b"SETEX", b"k", b"0", b"v"]).await.expect_error_prefix("ERR");
    call(&state, &[b"SETEX", b"k", b"-5", b"v"]).await.expect_error_prefix("ERR");
}

#[tokio::test]
async fn mget_returns_nil_for_missing_keys() {
    let state = test_state();
    call(&state, &[b"SET", b"a", b"1"]).await;
    call(&state, &[b"SET", b"c", b"3"]).await;
    let raw = call(&state, &[b"MGET", b"a", b"b", b"c"]).await.raw;
    // Array of 3: bulk "1", nil, bulk "3"
    assert_eq!(&raw, b"*3\r\n$1\r\n1\r\n$-1\r\n$1\r\n3\r\n");
}

#[tokio::test]
async fn mset_sets_all_pairs() {
    let state = test_state();
    call(&state, &[b"MSET", b"a", b"1", b"b", b"2", b"c", b"3"]).await.expect_simple("OK");
    call(&state, &[b"GET", b"a"]).await.expect_bulk(b"1");
    call(&state, &[b"GET", b"b"]).await.expect_bulk(b"2");
    call(&state, &[b"GET", b"c"]).await.expect_bulk(b"3");
}

#[tokio::test]
async fn mset_odd_args_errors() {
    let state = test_state();
    call(&state, &[b"MSET", b"a", b"1", b"b"]).await.expect_error_prefix("ERR");
}

// ---------------------------------------------------------------------------
// Additional read-only commands (HLEN / HKEYS / HVALS / HEXISTS / HMGET,
// LLEN, SMEMBERS / SISMEMBER / SCARD, ZSCORE / ZCARD)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn hlen_counts_fields() {
    let state = test_state();
    call(&state, &[b"HLEN", b"missing"]).await.expect_integer(0);
    call(&state, &[b"HSET", b"h", b"a", b"1", b"b", b"2", b"c", b"3"]).await;
    call(&state, &[b"HLEN", b"h"]).await.expect_integer(3);
}

#[tokio::test]
async fn hkeys_returns_field_names_sorted() {
    let state = test_state();
    call(&state, &[b"HSET", b"h", b"b", b"2", b"a", b"1"]).await;
    let items = call(&state, &[b"HKEYS", b"h"]).await.into_bulk_array();
    assert_eq!(items.len(), 2);
    assert_eq!(items[0].as_ref(), b"a");
    assert_eq!(items[1].as_ref(), b"b");
}

#[tokio::test]
async fn hvals_returns_values() {
    let state = test_state();
    call(&state, &[b"HSET", b"h", b"a", b"x", b"b", b"y"]).await;
    let items = call(&state, &[b"HVALS", b"h"]).await.into_bulk_array();
    assert_eq!(items.len(), 2);
    // Keys are BTreeMap-ordered (a, b); values follow the same order.
    assert_eq!(items[0].as_ref(), b"x");
    assert_eq!(items[1].as_ref(), b"y");
}

#[tokio::test]
async fn hexists_returns_0_or_1() {
    let state = test_state();
    call(&state, &[b"HSET", b"h", b"name", b"alice"]).await;
    call(&state, &[b"HEXISTS", b"h", b"name"]).await.expect_integer(1);
    call(&state, &[b"HEXISTS", b"h", b"missing"]).await.expect_integer(0);
    call(&state, &[b"HEXISTS", b"nope", b"field"]).await.expect_integer(0);
}

#[tokio::test]
async fn hsetnx_sets_only_when_field_absent() {
    let state = test_state();
    call(&state, &[b"HSETNX", b"h", b"name", b"alice"]).await.expect_integer(1);
    call(&state, &[b"HGET", b"h", b"name"]).await.expect_bulk(b"alice");
    // Second HSETNX on the same field is a no-op.
    call(&state, &[b"HSETNX", b"h", b"name", b"bob"]).await.expect_integer(0);
    call(&state, &[b"HGET", b"h", b"name"]).await.expect_bulk(b"alice");
}

#[tokio::test]
async fn hsetnx_on_wrong_type_errors() {
    let state = test_state();
    call(&state, &[b"SET", b"k", b"v"]).await;
    call(&state, &[b"HSETNX", b"k", b"f", b"x"]).await.expect_error_prefix("ERR");
}

#[tokio::test]
async fn hstrlen_returns_field_byte_length() {
    let state = test_state();
    call(&state, &[b"HSET", b"h", b"name", b"alice"]).await;
    call(&state, &[b"HSTRLEN", b"h", b"name"]).await.expect_integer(5);
    // Missing field: 0.
    call(&state, &[b"HSTRLEN", b"h", b"ghost"]).await.expect_integer(0);
    // Missing key: 0.
    call(&state, &[b"HSTRLEN", b"nope", b"f"]).await.expect_integer(0);
}

#[tokio::test]
async fn hstrlen_on_wrong_type_errors() {
    let state = test_state();
    call(&state, &[b"SET", b"k", b"v"]).await;
    call(&state, &[b"HSTRLEN", b"k", b"f"]).await.expect_error_prefix("ERR");
}

#[tokio::test]
async fn hmget_returns_nil_for_missing_fields() {
    let state = test_state();
    call(&state, &[b"HSET", b"h", b"a", b"1", b"c", b"3"]).await;
    let raw = call(&state, &[b"HMGET", b"h", b"a", b"b", b"c"]).await.raw;
    // Array of 3: bulk "1", nil, bulk "3"
    assert_eq!(&raw, b"*3\r\n$1\r\n1\r\n$-1\r\n$1\r\n3\r\n");
}

#[tokio::test]
async fn llen_counts_elements() {
    let state = test_state();
    call(&state, &[b"LLEN", b"missing"]).await.expect_integer(0);
    call(&state, &[b"RPUSH", b"l", b"a", b"b", b"c"]).await;
    call(&state, &[b"LLEN", b"l"]).await.expect_integer(3);
    call(&state, &[b"LPOP", b"l"]).await;
    call(&state, &[b"LLEN", b"l"]).await.expect_integer(2);
}

#[tokio::test]
async fn smembers_returns_all() {
    let state = test_state();
    call(&state, &[b"SADD", b"s", b"z", b"a", b"m"]).await;
    let items = call(&state, &[b"SMEMBERS", b"s"]).await.into_bulk_array();
    assert_eq!(items.len(), 3);
    // BTreeSet ordering → alphabetical.
    assert_eq!(items[0].as_ref(), b"a");
    assert_eq!(items[1].as_ref(), b"m");
    assert_eq!(items[2].as_ref(), b"z");
}

#[tokio::test]
async fn sismember_returns_0_or_1() {
    let state = test_state();
    call(&state, &[b"SADD", b"s", b"alice", b"bob"]).await;
    call(&state, &[b"SISMEMBER", b"s", b"alice"]).await.expect_integer(1);
    call(&state, &[b"SISMEMBER", b"s", b"carol"]).await.expect_integer(0);
    call(&state, &[b"SISMEMBER", b"nope", b"x"]).await.expect_integer(0);
}

#[tokio::test]
async fn scard_counts_members() {
    let state = test_state();
    call(&state, &[b"SCARD", b"missing"]).await.expect_integer(0);
    call(&state, &[b"SADD", b"s", b"a", b"b", b"c", b"a"]).await;
    call(&state, &[b"SCARD", b"s"]).await.expect_integer(3);
}

#[tokio::test]
async fn zscore_returns_score_or_nil() {
    let state = test_state();
    call(&state, &[b"ZADD", b"z", b"1", b"a", b"2.5", b"b"]).await;
    call(&state, &[b"ZSCORE", b"z", b"a"]).await.expect_bulk(b"1");
    call(&state, &[b"ZSCORE", b"z", b"b"]).await.expect_bulk(b"2.5");
    call(&state, &[b"ZSCORE", b"z", b"missing"]).await.expect_nil();
    call(&state, &[b"ZSCORE", b"nope", b"m"]).await.expect_nil();
}

#[tokio::test]
async fn zcard_counts_members() {
    let state = test_state();
    call(&state, &[b"ZCARD", b"missing"]).await.expect_integer(0);
    call(&state, &[b"ZADD", b"z", b"1", b"a", b"2", b"b", b"3", b"c"]).await;
    call(&state, &[b"ZCARD", b"z"]).await.expect_integer(3);
}

#[tokio::test]
async fn zrank_returns_ascending_index() {
    let state = test_state();
    call(&state, &[b"ZADD", b"z", b"1", b"a", b"2", b"b", b"3", b"c"]).await;
    call(&state, &[b"ZRANK", b"z", b"a"]).await.expect_integer(0);
    call(&state, &[b"ZRANK", b"z", b"b"]).await.expect_integer(1);
    call(&state, &[b"ZRANK", b"z", b"c"]).await.expect_integer(2);
}

#[tokio::test]
async fn zrevrank_returns_descending_index() {
    let state = test_state();
    call(&state, &[b"ZADD", b"z", b"1", b"a", b"2", b"b", b"3", b"c"]).await;
    call(&state, &[b"ZREVRANK", b"z", b"a"]).await.expect_integer(2);
    call(&state, &[b"ZREVRANK", b"z", b"b"]).await.expect_integer(1);
    call(&state, &[b"ZREVRANK", b"z", b"c"]).await.expect_integer(0);
}

#[tokio::test]
async fn zrank_ties_break_lexicographically() {
    // Equal scores: ascending tie-break is member-asc, descending flips it.
    let state = test_state();
    call(&state, &[b"ZADD", b"z", b"1", b"b", b"1", b"a", b"1", b"c"]).await;
    call(&state, &[b"ZRANK", b"z", b"a"]).await.expect_integer(0);
    call(&state, &[b"ZRANK", b"z", b"b"]).await.expect_integer(1);
    call(&state, &[b"ZRANK", b"z", b"c"]).await.expect_integer(2);
    call(&state, &[b"ZREVRANK", b"z", b"a"]).await.expect_integer(2);
    call(&state, &[b"ZREVRANK", b"z", b"c"]).await.expect_integer(0);
}

#[tokio::test]
async fn zrank_on_missing_member_or_key_returns_nil() {
    let state = test_state();
    call(&state, &[b"ZRANK", b"ghost", b"m"]).await.expect_nil();
    call(&state, &[b"ZREVRANK", b"ghost", b"m"]).await.expect_nil();
    call(&state, &[b"ZADD", b"z", b"1", b"a"]).await;
    call(&state, &[b"ZRANK", b"z", b"missing"]).await.expect_nil();
    call(&state, &[b"ZREVRANK", b"z", b"missing"]).await.expect_nil();
}

#[tokio::test]
async fn zrank_on_wrong_type_errors() {
    let state = test_state();
    call(&state, &[b"SET", b"k", b"v"]).await;
    call(&state, &[b"ZRANK", b"k", b"m"]).await.expect_error_prefix("ERR");
    call(&state, &[b"ZREVRANK", b"k", b"m"]).await.expect_error_prefix("ERR");
}

#[tokio::test]
async fn zcount_counts_inclusive_range() {
    let state = test_state();
    call(&state, &[b"ZADD", b"z", b"1", b"a", b"2", b"b", b"3", b"c", b"4", b"d"]).await;
    call(&state, &[b"ZCOUNT", b"z", b"2", b"3"]).await.expect_integer(2);
    call(&state, &[b"ZCOUNT", b"z", b"1", b"4"]).await.expect_integer(4);
    call(&state, &[b"ZCOUNT", b"z", b"-inf", b"+inf"]).await.expect_integer(4);
}

#[tokio::test]
async fn zcount_honors_exclusive_bounds() {
    // "(N" = exclusive; Redis ZRANGEBYSCORE / ZCOUNT share this syntax.
    let state = test_state();
    call(&state, &[b"ZADD", b"z", b"1", b"a", b"2", b"b", b"3", b"c"]).await;
    call(&state, &[b"ZCOUNT", b"z", b"(1", b"3"]).await.expect_integer(2);
    call(&state, &[b"ZCOUNT", b"z", b"1", b"(3"]).await.expect_integer(2);
    call(&state, &[b"ZCOUNT", b"z", b"(1", b"(3"]).await.expect_integer(1);
}

#[tokio::test]
async fn zcount_on_missing_key_returns_zero() {
    let state = test_state();
    call(&state, &[b"ZCOUNT", b"ghost", b"-inf", b"+inf"]).await.expect_integer(0);
}

#[tokio::test]
async fn zcount_on_wrong_type_errors() {
    let state = test_state();
    call(&state, &[b"SET", b"k", b"v"]).await;
    call(&state, &[b"ZCOUNT", b"k", b"0", b"1"]).await.expect_error_prefix("ERR");
}

#[tokio::test]
async fn zmscore_returns_scores_or_nil_per_member() {
    let state = test_state();
    call(&state, &[b"ZADD", b"z", b"1", b"a", b"2.5", b"b"]).await;
    let raw = call(&state, &[b"ZMSCORE", b"z", b"a", b"missing", b"b"]).await.raw;
    // Array of 3: bulk "1", nil, bulk "2.5"
    assert_eq!(&raw, b"*3\r\n$1\r\n1\r\n$-1\r\n$3\r\n2.5\r\n");
}

#[tokio::test]
async fn zmscore_on_missing_key_returns_all_nils() {
    let state = test_state();
    let raw = call(&state, &[b"ZMSCORE", b"ghost", b"a", b"b"]).await.raw;
    assert_eq!(&raw, b"*2\r\n$-1\r\n$-1\r\n");
}

#[tokio::test]
async fn zmscore_on_wrong_type_errors() {
    let state = test_state();
    call(&state, &[b"SET", b"k", b"v"]).await;
    call(&state, &[b"ZMSCORE", b"k", b"m"]).await.expect_error_prefix("ERR");
}

// ---------------------------------------------------------------------------
// Additional mutating commands (HINCRBY, SREM, ZREM, ZINCRBY)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn hincrby_creates_field_at_zero() {
    let state = test_state();
    call(&state, &[b"HINCRBY", b"h", b"counter", b"5"]).await.expect_integer(5);
    call(&state, &[b"HINCRBY", b"h", b"counter", b"3"]).await.expect_integer(8);
    call(&state, &[b"HINCRBY", b"h", b"counter", b"-10"]).await.expect_integer(-2);
}

#[tokio::test]
async fn hincrby_on_non_integer_field_errors() {
    let state = test_state();
    call(&state, &[b"HSET", b"h", b"name", b"alice"]).await;
    call(&state, &[b"HINCRBY", b"h", b"name", b"1"]).await.expect_error_prefix("ERR");
}

#[tokio::test]
async fn srem_returns_count_removed() {
    let state = test_state();
    call(&state, &[b"SADD", b"s", b"a", b"b", b"c"]).await;
    call(&state, &[b"SREM", b"s", b"a", b"b", b"missing"]).await.expect_integer(2);
    call(&state, &[b"SCARD", b"s"]).await.expect_integer(1);
}

#[tokio::test]
async fn srem_empties_and_deletes_key() {
    let state = test_state();
    call(&state, &[b"SADD", b"s", b"x"]).await;
    call(&state, &[b"SREM", b"s", b"x"]).await.expect_integer(1);
    call(&state, &[b"EXISTS", b"s"]).await.expect_integer(0);
}

#[tokio::test]
async fn zrem_returns_count_removed() {
    let state = test_state();
    call(&state, &[b"ZADD", b"z", b"1", b"a", b"2", b"b", b"3", b"c"]).await;
    call(&state, &[b"ZREM", b"z", b"a", b"c", b"missing"]).await.expect_integer(2);
    call(&state, &[b"ZCARD", b"z"]).await.expect_integer(1);
}

#[tokio::test]
async fn zrem_empties_and_deletes_key() {
    let state = test_state();
    call(&state, &[b"ZADD", b"z", b"1", b"a"]).await;
    call(&state, &[b"ZREM", b"z", b"a"]).await.expect_integer(1);
    call(&state, &[b"EXISTS", b"z"]).await.expect_integer(0);
}

#[tokio::test]
async fn zincrby_creates_member_at_zero() {
    let state = test_state();
    call(&state, &[b"ZINCRBY", b"z", b"5", b"member"]).await.expect_bulk(b"5");
    call(&state, &[b"ZINCRBY", b"z", b"2.5", b"member"]).await.expect_bulk(b"7.5");
    call(&state, &[b"ZSCORE", b"z", b"member"]).await.expect_bulk(b"7.5");
}

#[tokio::test]
async fn zincrby_reorders_in_zrange() {
    let state = test_state();
    call(&state, &[b"ZADD", b"z", b"1", b"a", b"2", b"b"]).await;
    // Push a past b by +10.
    call(&state, &[b"ZINCRBY", b"z", b"10", b"a"]).await.expect_bulk(b"11");
    let items = call(&state, &[b"ZRANGE", b"z", b"0", b"-1"]).await.into_bulk_array();
    assert_eq!(items[0].as_ref(), b"b");
    assert_eq!(items[1].as_ref(), b"a");
}

#[tokio::test]
async fn new_write_commands_error_on_wrong_type() {
    let state = test_state();
    call(&state, &[b"SET", b"k", b"v"]).await;
    call(&state, &[b"HINCRBY", b"k", b"f", b"1"]).await.expect_error_prefix("ERR");
    call(&state, &[b"SREM", b"k", b"x"]).await.expect_error_prefix("ERR");
    call(&state, &[b"ZREM", b"k", b"x"]).await.expect_error_prefix("ERR");
    call(&state, &[b"ZINCRBY", b"k", b"1", b"m"]).await.expect_error_prefix("ERR");
}

#[tokio::test]
async fn new_read_commands_all_error_on_wrong_type() {
    let state = test_state();
    call(&state, &[b"SET", b"k", b"v"]).await;
    for cmd in [b"HLEN" as &[u8], b"HKEYS", b"HVALS", b"LLEN", b"SMEMBERS", b"SCARD", b"ZCARD"] {
        call(&state, &[cmd, b"k"]).await.expect_error_prefix("ERR");
    }
}

// ---------------------------------------------------------------------------
// Additional commands: GETSET, PERSIST, LINDEX, LSET, LREM, ZRANGEBYSCORE
// ---------------------------------------------------------------------------

#[tokio::test]
async fn getset_returns_old_and_clears_ttl() {
    let state = test_state();
    // No prior value → GETSET returns nil.
    call(&state, &[b"GETSET", b"k", b"v1"]).await.expect_nil();
    call(&state, &[b"GET", b"k"]).await.expect_bulk(b"v1");
    // Second GETSET returns the prior value.
    call(&state, &[b"GETSET", b"k", b"v2"]).await.expect_bulk(b"v1");
    call(&state, &[b"GET", b"k"]).await.expect_bulk(b"v2");
}

#[tokio::test]
async fn getset_clears_existing_ttl() {
    let state = test_state();
    call(&state, &[b"SET", b"k", b"v", b"EX", b"120"]).await;
    call(&state, &[b"GETSET", b"k", b"new"]).await.expect_bulk(b"v");
    // TTL cleared — GETSET matches SET's overwrite semantics.
    call(&state, &[b"TTL", b"k"]).await.expect_integer(-1);
}

#[tokio::test]
async fn persist_removes_ttl() {
    let state = test_state();
    call(&state, &[b"SET", b"k", b"v", b"EX", b"60"]).await;
    call(&state, &[b"PERSIST", b"k"]).await.expect_integer(1);
    call(&state, &[b"TTL", b"k"]).await.expect_integer(-1);
}

#[tokio::test]
async fn persist_returns_zero_when_no_ttl_or_missing() {
    let state = test_state();
    call(&state, &[b"PERSIST", b"missing"]).await.expect_integer(0);
    call(&state, &[b"SET", b"k", b"v"]).await;
    call(&state, &[b"PERSIST", b"k"]).await.expect_integer(0);
}

#[tokio::test]
async fn lindex_positive_and_negative() {
    let state = test_state();
    call(&state, &[b"RPUSH", b"l", b"a", b"b", b"c", b"d"]).await;
    call(&state, &[b"LINDEX", b"l", b"0"]).await.expect_bulk(b"a");
    call(&state, &[b"LINDEX", b"l", b"3"]).await.expect_bulk(b"d");
    call(&state, &[b"LINDEX", b"l", b"-1"]).await.expect_bulk(b"d");
    call(&state, &[b"LINDEX", b"l", b"-4"]).await.expect_bulk(b"a");
    // Out of range → nil.
    call(&state, &[b"LINDEX", b"l", b"4"]).await.expect_nil();
    call(&state, &[b"LINDEX", b"l", b"-5"]).await.expect_nil();
    // Missing key → nil.
    call(&state, &[b"LINDEX", b"missing", b"0"]).await.expect_nil();
}

#[tokio::test]
async fn lset_updates_element() {
    let state = test_state();
    call(&state, &[b"RPUSH", b"l", b"a", b"b", b"c"]).await;
    call(&state, &[b"LSET", b"l", b"1", b"B"]).await.expect_simple("OK");
    let items = call(&state, &[b"LRANGE", b"l", b"0", b"-1"]).await.into_bulk_array();
    assert_eq!(items[0].as_ref(), b"a");
    assert_eq!(items[1].as_ref(), b"B");
    assert_eq!(items[2].as_ref(), b"c");
    // Negative index.
    call(&state, &[b"LSET", b"l", b"-1", b"C"]).await.expect_simple("OK");
    let items = call(&state, &[b"LRANGE", b"l", b"0", b"-1"]).await.into_bulk_array();
    assert_eq!(items[2].as_ref(), b"C");
}

#[tokio::test]
async fn lset_errors_on_missing_key_or_bad_index() {
    let state = test_state();
    call(&state, &[b"LSET", b"missing", b"0", b"x"]).await.expect_error_prefix("ERR");
    call(&state, &[b"RPUSH", b"l", b"a"]).await;
    call(&state, &[b"LSET", b"l", b"5", b"x"]).await.expect_error_prefix("ERR");
    call(&state, &[b"LSET", b"l", b"-5", b"x"]).await.expect_error_prefix("ERR");
}

#[tokio::test]
async fn lrem_positive_count_removes_from_head() {
    let state = test_state();
    call(&state, &[b"RPUSH", b"l", b"a", b"b", b"a", b"c", b"a"]).await;
    call(&state, &[b"LREM", b"l", b"2", b"a"]).await.expect_integer(2);
    let items = call(&state, &[b"LRANGE", b"l", b"0", b"-1"]).await.into_bulk_array();
    assert_eq!(items[0].as_ref(), b"b");
    assert_eq!(items[1].as_ref(), b"c");
    assert_eq!(items[2].as_ref(), b"a"); // third occurrence survives
}

#[tokio::test]
async fn lrem_negative_count_removes_from_tail() {
    let state = test_state();
    call(&state, &[b"RPUSH", b"l", b"a", b"b", b"a", b"c", b"a"]).await;
    call(&state, &[b"LREM", b"l", b"-2", b"a"]).await.expect_integer(2);
    let items = call(&state, &[b"LRANGE", b"l", b"0", b"-1"]).await.into_bulk_array();
    assert_eq!(items[0].as_ref(), b"a"); // first survives (removed last two)
    assert_eq!(items[1].as_ref(), b"b");
    assert_eq!(items[2].as_ref(), b"c");
}

#[tokio::test]
async fn lrem_zero_count_removes_all() {
    let state = test_state();
    call(&state, &[b"RPUSH", b"l", b"a", b"b", b"a", b"a"]).await;
    call(&state, &[b"LREM", b"l", b"0", b"a"]).await.expect_integer(3);
    let items = call(&state, &[b"LRANGE", b"l", b"0", b"-1"]).await.into_bulk_array();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].as_ref(), b"b");
}

#[tokio::test]
async fn lrem_empties_and_deletes_key() {
    let state = test_state();
    call(&state, &[b"RPUSH", b"l", b"a", b"a"]).await;
    call(&state, &[b"LREM", b"l", b"0", b"a"]).await.expect_integer(2);
    call(&state, &[b"EXISTS", b"l"]).await.expect_integer(0);
}

#[tokio::test]
async fn zrangebyscore_inclusive_bounds() {
    let state = test_state();
    call(&state, &[b"ZADD", b"z", b"1", b"a", b"2", b"b", b"3", b"c", b"4", b"d"]).await;
    let m = call(&state, &[b"ZRANGEBYSCORE", b"z", b"2", b"3"]).await.into_bulk_array();
    assert_eq!(m.len(), 2);
    assert_eq!(m[0].as_ref(), b"b");
    assert_eq!(m[1].as_ref(), b"c");
}

#[tokio::test]
async fn zrangebyscore_exclusive_bounds() {
    let state = test_state();
    call(&state, &[b"ZADD", b"z", b"1", b"a", b"2", b"b", b"3", b"c"]).await;
    // (1 3 means score > 1 and score <= 3.
    let m = call(&state, &[b"ZRANGEBYSCORE", b"z", b"(1", b"3"]).await.into_bulk_array();
    assert_eq!(m.len(), 2);
    assert_eq!(m[0].as_ref(), b"b");
    assert_eq!(m[1].as_ref(), b"c");
    // Fully exclusive.
    let m = call(&state, &[b"ZRANGEBYSCORE", b"z", b"(1", b"(3"]).await.into_bulk_array();
    assert_eq!(m.len(), 1);
    assert_eq!(m[0].as_ref(), b"b");
}

#[tokio::test]
async fn zrangebyscore_infinity_bounds() {
    let state = test_state();
    call(&state, &[b"ZADD", b"z", b"1", b"a", b"2", b"b", b"3", b"c"]).await;
    let m = call(&state, &[b"ZRANGEBYSCORE", b"z", b"-inf", b"+inf"]).await.into_bulk_array();
    assert_eq!(m.len(), 3);
    let m = call(&state, &[b"ZRANGEBYSCORE", b"z", b"-inf", b"2"]).await.into_bulk_array();
    assert_eq!(m.len(), 2);
    assert_eq!(m[1].as_ref(), b"b");
}

// ---------------------------------------------------------------------------
// KEYS and SCAN
// ---------------------------------------------------------------------------

fn bulks_to_strs(items: &[Bytes]) -> Vec<String> {
    items.iter().map(|b| String::from_utf8(b.to_vec()).expect("utf8")).collect()
}

#[tokio::test]
async fn keys_matches_everything() {
    let state = test_state();
    call(&state, &[b"SET", b"alpha", b"1"]).await;
    call(&state, &[b"SET", b"beta", b"2"]).await;
    call(&state, &[b"HSET", b"gamma", b"f", b"v"]).await;
    let mut keys = bulks_to_strs(&call(&state, &[b"KEYS", b"*"]).await.into_bulk_array());
    keys.sort();
    assert_eq!(keys, vec!["alpha".to_string(), "beta".to_string(), "gamma".to_string()]);
}

#[tokio::test]
async fn keys_glob_star_and_question() {
    let state = test_state();
    for k in ["user:1", "user:2", "user:10", "other"] {
        call(&state, &[b"SET", k.as_bytes(), b"v"]).await;
    }
    let mut m = bulks_to_strs(&call(&state, &[b"KEYS", b"user:*"]).await.into_bulk_array());
    m.sort();
    assert_eq!(m, vec!["user:1".to_string(), "user:10".to_string(), "user:2".to_string()]);
    let mut m = bulks_to_strs(&call(&state, &[b"KEYS", b"user:?"]).await.into_bulk_array());
    m.sort();
    assert_eq!(m, vec!["user:1".to_string(), "user:2".to_string()]);
}

#[tokio::test]
async fn keys_excludes_expired() {
    let state = test_state();
    // PX=1 with short sleep guarantees expiry.
    call(&state, &[b"SET", b"will-expire", b"v", b"PX", b"1"]).await;
    call(&state, &[b"SET", b"stays", b"v"]).await;
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let keys = bulks_to_strs(&call(&state, &[b"KEYS", b"*"]).await.into_bulk_array());
    assert_eq!(keys, vec!["stays".to_string()]);
}

#[tokio::test]
async fn scan_paginates_full_keyspace() {
    // SCAN's reply is a nested [cursor, [keys...]] array that the flat
    // `into_bulk_array` helper can't parse; exercise scan semantics through
    // the storage layer directly instead.
    let state = test_state();
    for i in 0..15 {
        let k = format!("k{i:02}");
        call(&state, &[b"SET", k.as_bytes(), b"v"]).await;
    }

    let mut seen: Vec<String> = Vec::new();
    let (first, next) = state.storage.scan(None, None, 5).await.expect("scan");
    assert_eq!(first.len(), 5);
    seen.extend(first);
    let mut cursor = next.expect("more pages");
    let (second, next) = state.storage.scan(Some(&cursor), None, 5).await.expect("scan");
    assert_eq!(second.len(), 5);
    seen.extend(second);
    cursor = next.expect("more pages");
    let (third, next) = state.storage.scan(Some(&cursor), None, 5).await.expect("scan");
    assert_eq!(third.len(), 5);
    seen.extend(third);
    assert!(next.is_none(), "expected scan exhausted");
    seen.sort();
    let expected: Vec<String> = (0..15).map(|i| format!("k{i:02}")).collect();
    assert_eq!(seen, expected);
}

#[tokio::test]
async fn scan_with_match_pattern_filters() {
    let state = test_state();
    call(&state, &[b"SET", b"user:1", b"v"]).await;
    call(&state, &[b"SET", b"user:2", b"v"]).await;
    call(&state, &[b"SET", b"other", b"v"]).await;

    let (matched, _next) = state.storage.scan(None, Some("user:*"), 100).await.expect("scan");
    let mut m: Vec<String> = matched;
    m.sort();
    assert_eq!(m, vec!["user:1".to_string(), "user:2".to_string()]);
}

#[tokio::test]
async fn scan_empty_store_returns_done() {
    let state = test_state();
    let (keys, next) = state.storage.scan(None, None, 10).await.expect("scan");
    assert_eq!(keys.len(), 0);
    assert!(next.is_none());
}

#[tokio::test]
async fn type_returns_none_for_missing_key() {
    let state = test_state();
    call(&state, &[b"TYPE", b"missing"]).await.expect_simple("none");
}

#[tokio::test]
async fn type_reports_each_value_kind() {
    let state = test_state();
    call(&state, &[b"SET", b"s", b"v"]).await;
    call(&state, &[b"HSET", b"h", b"f", b"v"]).await;
    call(&state, &[b"LPUSH", b"l", b"x"]).await;
    call(&state, &[b"SADD", b"set1", b"m"]).await;
    call(&state, &[b"ZADD", b"z", b"1", b"m"]).await;

    call(&state, &[b"TYPE", b"s"]).await.expect_simple("string");
    call(&state, &[b"TYPE", b"h"]).await.expect_simple("hash");
    call(&state, &[b"TYPE", b"l"]).await.expect_simple("list");
    call(&state, &[b"TYPE", b"set1"]).await.expect_simple("set");
    call(&state, &[b"TYPE", b"z"]).await.expect_simple("zset");
}

#[tokio::test]
async fn type_returns_none_after_expiry() {
    let state = test_state();
    call(&state, &[b"SET", b"k", b"v", b"PX", b"10"]).await;
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    call(&state, &[b"TYPE", b"k"]).await.expect_simple("none");
}

// ---------------------------------------------------------------------------
// LINSERT / LTRIM / LPUSHX / RPUSHX
// ---------------------------------------------------------------------------

#[tokio::test]
async fn linsert_before_and_after_pivot() {
    let state = test_state();
    call(&state, &[b"RPUSH", b"l", b"a", b"b", b"c"]).await;
    call(&state, &[b"LINSERT", b"l", b"BEFORE", b"b", b"X"]).await.expect_integer(4);
    let items = call(&state, &[b"LRANGE", b"l", b"0", b"-1"]).await.into_bulk_array();
    assert_eq!(items[0].as_ref(), b"a");
    assert_eq!(items[1].as_ref(), b"X");
    assert_eq!(items[2].as_ref(), b"b");
    call(&state, &[b"LINSERT", b"l", b"AFTER", b"b", b"Y"]).await.expect_integer(5);
    let items = call(&state, &[b"LRANGE", b"l", b"0", b"-1"]).await.into_bulk_array();
    assert_eq!(items[3].as_ref(), b"Y");
    // Lowercase direction still accepted (case-insensitive match).
    call(&state, &[b"LINSERT", b"l", b"before", b"a", b"Z"]).await.expect_integer(6);
    let items = call(&state, &[b"LRANGE", b"l", b"0", b"0"]).await.into_bulk_array();
    assert_eq!(items[0].as_ref(), b"Z");
}

#[tokio::test]
async fn linsert_missing_pivot_returns_minus_one() {
    let state = test_state();
    call(&state, &[b"RPUSH", b"l", b"a", b"b"]).await;
    call(&state, &[b"LINSERT", b"l", b"BEFORE", b"missing", b"X"]).await.expect_integer(-1);
}

#[tokio::test]
async fn linsert_missing_key_returns_zero() {
    let state = test_state();
    call(&state, &[b"LINSERT", b"missing", b"BEFORE", b"p", b"v"]).await.expect_integer(0);
}

#[tokio::test]
async fn linsert_invalid_direction_errors() {
    let state = test_state();
    call(&state, &[b"RPUSH", b"l", b"a"]).await;
    call(&state, &[b"LINSERT", b"l", b"AROUND", b"a", b"X"]).await.expect_error_prefix("ERR");
}

#[tokio::test]
async fn linsert_on_wrong_type_errors() {
    let state = test_state();
    call(&state, &[b"SET", b"k", b"v"]).await;
    call(&state, &[b"LINSERT", b"k", b"BEFORE", b"p", b"x"]).await.expect_error_prefix("ERR");
}

#[tokio::test]
async fn ltrim_keeps_inclusive_range() {
    let state = test_state();
    call(&state, &[b"RPUSH", b"l", b"a", b"b", b"c", b"d", b"e"]).await;
    call(&state, &[b"LTRIM", b"l", b"1", b"3"]).await.expect_simple("OK");
    let items = call(&state, &[b"LRANGE", b"l", b"0", b"-1"]).await.into_bulk_array();
    assert_eq!(items.len(), 3);
    assert_eq!(items[0].as_ref(), b"b");
    assert_eq!(items[2].as_ref(), b"d");
}

#[tokio::test]
async fn ltrim_negative_indices() {
    let state = test_state();
    call(&state, &[b"RPUSH", b"l", b"a", b"b", b"c", b"d"]).await;
    // Keep last two elements.
    call(&state, &[b"LTRIM", b"l", b"-2", b"-1"]).await.expect_simple("OK");
    let items = call(&state, &[b"LRANGE", b"l", b"0", b"-1"]).await.into_bulk_array();
    assert_eq!(items.len(), 2);
    assert_eq!(items[0].as_ref(), b"c");
    assert_eq!(items[1].as_ref(), b"d");
}

#[tokio::test]
async fn ltrim_empty_range_deletes_key() {
    let state = test_state();
    call(&state, &[b"RPUSH", b"l", b"a", b"b", b"c"]).await;
    // start > stop after normalization → resulting list is empty.
    call(&state, &[b"LTRIM", b"l", b"5", b"10"]).await.expect_simple("OK");
    call(&state, &[b"EXISTS", b"l"]).await.expect_integer(0);
}

#[tokio::test]
async fn ltrim_on_missing_key_is_noop_ok() {
    let state = test_state();
    call(&state, &[b"LTRIM", b"missing", b"0", b"0"]).await.expect_simple("OK");
}

#[tokio::test]
async fn ltrim_on_wrong_type_errors() {
    let state = test_state();
    call(&state, &[b"SET", b"k", b"v"]).await;
    call(&state, &[b"LTRIM", b"k", b"0", b"1"]).await.expect_error_prefix("ERR");
}

#[tokio::test]
async fn lpushx_only_when_key_exists() {
    let state = test_state();
    call(&state, &[b"LPUSHX", b"missing", b"a"]).await.expect_integer(0);
    call(&state, &[b"EXISTS", b"missing"]).await.expect_integer(0);
    call(&state, &[b"RPUSH", b"l", b"a"]).await;
    call(&state, &[b"LPUSHX", b"l", b"b", b"c"]).await.expect_integer(3);
    let items = call(&state, &[b"LRANGE", b"l", b"0", b"-1"]).await.into_bulk_array();
    // LPUSHX b c → inserts b then c at head → final: c, b, a
    assert_eq!(items[0].as_ref(), b"c");
    assert_eq!(items[1].as_ref(), b"b");
    assert_eq!(items[2].as_ref(), b"a");
}

#[tokio::test]
async fn rpushx_only_when_key_exists() {
    let state = test_state();
    call(&state, &[b"RPUSHX", b"missing", b"a"]).await.expect_integer(0);
    call(&state, &[b"RPUSH", b"l", b"a"]).await;
    call(&state, &[b"RPUSHX", b"l", b"b", b"c"]).await.expect_integer(3);
    let items = call(&state, &[b"LRANGE", b"l", b"0", b"-1"]).await.into_bulk_array();
    assert_eq!(items[0].as_ref(), b"a");
    assert_eq!(items[2].as_ref(), b"c");
}

#[tokio::test]
async fn pushx_on_wrong_type_errors() {
    let state = test_state();
    call(&state, &[b"SET", b"k", b"v"]).await;
    call(&state, &[b"LPUSHX", b"k", b"x"]).await.expect_error_prefix("ERR");
    call(&state, &[b"RPUSHX", b"k", b"x"]).await.expect_error_prefix("ERR");
}

// ---------------------------------------------------------------------------
// SINTER / SUNION / SDIFF / SMISMEMBER / SPOP / SRANDMEMBER
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sinter_returns_common_members() {
    let state = test_state();
    call(&state, &[b"SADD", b"a", b"x", b"y", b"z"]).await;
    call(&state, &[b"SADD", b"b", b"y", b"z", b"w"]).await;
    call(&state, &[b"SADD", b"c", b"z", b"y", b"q"]).await;
    let mut members = bulks_to_strs(&call(&state, &[b"SINTER", b"a", b"b", b"c"]).await.into_bulk_array());
    members.sort();
    assert_eq!(members, vec!["y".to_string(), "z".to_string()]);
}

#[tokio::test]
async fn sinter_with_missing_key_returns_empty() {
    let state = test_state();
    call(&state, &[b"SADD", b"a", b"x", b"y"]).await;
    let members = call(&state, &[b"SINTER", b"a", b"missing"]).await.into_bulk_array();
    assert!(members.is_empty());
}

#[tokio::test]
async fn sunion_combines_sets() {
    let state = test_state();
    call(&state, &[b"SADD", b"a", b"x", b"y"]).await;
    call(&state, &[b"SADD", b"b", b"y", b"z"]).await;
    let mut members = bulks_to_strs(&call(&state, &[b"SUNION", b"a", b"b"]).await.into_bulk_array());
    members.sort();
    assert_eq!(members, vec!["x".to_string(), "y".to_string(), "z".to_string()]);
}

#[tokio::test]
async fn sdiff_returns_first_minus_rest() {
    let state = test_state();
    call(&state, &[b"SADD", b"a", b"x", b"y", b"z"]).await;
    call(&state, &[b"SADD", b"b", b"y"]).await;
    call(&state, &[b"SADD", b"c", b"z"]).await;
    let mut members = bulks_to_strs(&call(&state, &[b"SDIFF", b"a", b"b", b"c"]).await.into_bulk_array());
    members.sort();
    assert_eq!(members, vec!["x".to_string()]);
}

#[tokio::test]
async fn sinter_on_wrong_type_errors() {
    let state = test_state();
    call(&state, &[b"SADD", b"s", b"x"]).await;
    call(&state, &[b"SET", b"k", b"v"]).await;
    call(&state, &[b"SINTER", b"s", b"k"]).await.expect_error_prefix("ERR");
}

#[tokio::test]
async fn smismember_reports_per_member_presence() {
    let state = test_state();
    call(&state, &[b"SADD", b"s", b"x", b"y"]).await;
    // Reply shape is an array of RESP integers (:0 / :1). The bulk-array
    // helper only decodes bulk strings, so check the raw RESP encoding here
    // and the storage contract below.
    let reply = call(&state, &[b"SMISMEMBER", b"s", b"x", b"y", b"z"]).await;
    assert_eq!(reply.raw, b"*3\r\n:1\r\n:1\r\n:0\r\n");
    let got = state.storage.smismember("s", &["x".into(), "y".into(), "z".into()]).await.expect("smismember");
    assert_eq!(got, vec![true, true, false]);
}

#[tokio::test]
async fn smismember_missing_key_returns_all_zero() {
    let state = test_state();
    let got = state.storage.smismember("missing", &["x".into(), "y".into()]).await.expect("smismember");
    assert_eq!(got, vec![false, false]);
}

#[tokio::test]
async fn smismember_on_wrong_type_errors() {
    let state = test_state();
    call(&state, &[b"SET", b"k", b"v"]).await;
    call(&state, &[b"SMISMEMBER", b"k", b"x"]).await.expect_error_prefix("ERR");
}

#[tokio::test]
async fn spop_single_removes_and_returns_member() {
    let state = test_state();
    call(&state, &[b"SADD", b"s", b"only"]).await;
    call(&state, &[b"SPOP", b"s"]).await.expect_bulk(b"only");
    call(&state, &[b"EXISTS", b"s"]).await.expect_integer(0);
}

#[tokio::test]
async fn spop_count_returns_array_and_removes() {
    let state = test_state();
    call(&state, &[b"SADD", b"s", b"a", b"b", b"c", b"d"]).await;
    let popped = call(&state, &[b"SPOP", b"s", b"3"]).await.into_bulk_array();
    assert_eq!(popped.len(), 3);
    call(&state, &[b"SCARD", b"s"]).await.expect_integer(1);
}

#[tokio::test]
async fn spop_missing_key_returns_nil_or_empty_array() {
    let state = test_state();
    call(&state, &[b"SPOP", b"missing"]).await.expect_nil();
    let arr = call(&state, &[b"SPOP", b"missing", b"3"]).await.into_bulk_array();
    assert!(arr.is_empty());
}

#[tokio::test]
async fn spop_count_zero_is_noop() {
    let state = test_state();
    call(&state, &[b"SADD", b"s", b"a", b"b"]).await;
    let arr = call(&state, &[b"SPOP", b"s", b"0"]).await.into_bulk_array();
    assert!(arr.is_empty());
    call(&state, &[b"SCARD", b"s"]).await.expect_integer(2);
}

#[tokio::test]
async fn spop_negative_count_errors() {
    let state = test_state();
    call(&state, &[b"SADD", b"s", b"a"]).await;
    call(&state, &[b"SPOP", b"s", b"-1"]).await.expect_error_prefix("ERR");
}

#[tokio::test]
async fn srandmember_single_returns_member_without_removing() {
    let state = test_state();
    call(&state, &[b"SADD", b"s", b"only"]).await;
    call(&state, &[b"SRANDMEMBER", b"s"]).await.expect_bulk(b"only");
    call(&state, &[b"SCARD", b"s"]).await.expect_integer(1);
}

#[tokio::test]
async fn srandmember_missing_key_returns_nil() {
    let state = test_state();
    call(&state, &[b"SRANDMEMBER", b"missing"]).await.expect_nil();
}

#[tokio::test]
async fn srandmember_positive_count_returns_unique() {
    let state = test_state();
    call(&state, &[b"SADD", b"s", b"a", b"b", b"c"]).await;
    // Positive count 5 ≥ cardinality 3 → all 3 members, no duplicates.
    let picked = call(&state, &[b"SRANDMEMBER", b"s", b"5"]).await.into_bulk_array();
    assert_eq!(picked.len(), 3);
    let mut unique: Vec<&[u8]> = picked.iter().map(AsRef::as_ref).collect();
    unique.sort_unstable();
    unique.dedup();
    assert_eq!(unique.len(), 3);
}

#[tokio::test]
async fn srandmember_negative_count_allows_duplicates() {
    let state = test_state();
    call(&state, &[b"SADD", b"s", b"only"]).await;
    // Negative count 5 against a 1-member set → 5 repeats of "only".
    let picked = call(&state, &[b"SRANDMEMBER", b"s", b"-5"]).await.into_bulk_array();
    assert_eq!(picked.len(), 5);
    for b in &picked {
        assert_eq!(b.as_ref(), b"only");
    }
}

#[tokio::test]
async fn srandmember_on_wrong_type_errors() {
    let state = test_state();
    call(&state, &[b"SET", b"k", b"v"]).await;
    call(&state, &[b"SRANDMEMBER", b"k"]).await.expect_error_prefix("ERR");
}

// ---------------------------------------------------------------------------
// PEXPIRE / PTTL / RENAME / RENAMENX
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pexpire_sets_millisecond_ttl() {
    let state = test_state();
    call(&state, &[b"SET", b"k", b"v"]).await;
    call(&state, &[b"PEXPIRE", b"k", b"60000"]).await.expect_integer(1);
    let reply = call(&state, &[b"PTTL", b"k"]).await;
    // 60 seconds in ms ≈ 60000; allow some drift below for test latency.
    let pttl_raw = String::from_utf8_lossy(&reply.raw).into_owned();
    let ms: i64 = pttl_raw.trim_start_matches(':').trim_end().parse().expect("pttl");
    assert!((0..=60_000).contains(&ms), "pttl out of band: {ms}");
}

#[tokio::test]
async fn pexpire_missing_key_returns_zero() {
    let state = test_state();
    call(&state, &[b"PEXPIRE", b"missing", b"1000"]).await.expect_integer(0);
}

#[tokio::test]
async fn pttl_returns_minus_two_for_missing_key() {
    let state = test_state();
    call(&state, &[b"PTTL", b"missing"]).await.expect_integer(-2);
}

#[tokio::test]
async fn pttl_returns_minus_one_without_expiry() {
    let state = test_state();
    call(&state, &[b"SET", b"k", b"v"]).await;
    call(&state, &[b"PTTL", b"k"]).await.expect_integer(-1);
}

#[tokio::test]
async fn rename_moves_value_and_deletes_source() {
    let state = test_state();
    call(&state, &[b"SET", b"a", b"v"]).await;
    call(&state, &[b"RENAME", b"a", b"b"]).await.expect_simple("OK");
    call(&state, &[b"EXISTS", b"a"]).await.expect_integer(0);
    call(&state, &[b"GET", b"b"]).await.expect_bulk(b"v");
}

#[tokio::test]
async fn rename_overwrites_destination() {
    let state = test_state();
    call(&state, &[b"SET", b"a", b"new"]).await;
    call(&state, &[b"SET", b"b", b"old"]).await;
    call(&state, &[b"RENAME", b"a", b"b"]).await.expect_simple("OK");
    call(&state, &[b"GET", b"b"]).await.expect_bulk(b"new");
    call(&state, &[b"EXISTS", b"a"]).await.expect_integer(0);
}

#[tokio::test]
async fn rename_missing_source_errors() {
    let state = test_state();
    call(&state, &[b"RENAME", b"missing", b"b"]).await.expect_error_prefix("ERR");
}

#[tokio::test]
async fn rename_to_self_is_noop() {
    let state = test_state();
    call(&state, &[b"SET", b"a", b"v"]).await;
    call(&state, &[b"RENAME", b"a", b"a"]).await.expect_simple("OK");
    call(&state, &[b"GET", b"a"]).await.expect_bulk(b"v");
}

#[tokio::test]
async fn renamenx_moves_when_dest_absent() {
    let state = test_state();
    call(&state, &[b"SET", b"a", b"v"]).await;
    call(&state, &[b"RENAMENX", b"a", b"b"]).await.expect_integer(1);
    call(&state, &[b"GET", b"b"]).await.expect_bulk(b"v");
}

#[tokio::test]
async fn renamenx_refuses_when_dest_present() {
    let state = test_state();
    call(&state, &[b"SET", b"a", b"v1"]).await;
    call(&state, &[b"SET", b"b", b"v2"]).await;
    call(&state, &[b"RENAMENX", b"a", b"b"]).await.expect_integer(0);
    call(&state, &[b"GET", b"a"]).await.expect_bulk(b"v1");
    call(&state, &[b"GET", b"b"]).await.expect_bulk(b"v2");
}

#[tokio::test]
async fn renamenx_missing_source_errors() {
    let state = test_state();
    call(&state, &[b"RENAMENX", b"missing", b"b"]).await.expect_error_prefix("ERR");
}

// ---------------------------------------------------------------------------
// INCRBYFLOAT / GETRANGE / SETRANGE / MSETNX
// ---------------------------------------------------------------------------

#[tokio::test]
async fn incrbyfloat_creates_from_zero() {
    let state = test_state();
    call(&state, &[b"INCRBYFLOAT", b"k", b"3.14"]).await.expect_bulk(b"3.14");
    call(&state, &[b"INCRBYFLOAT", b"k", b"0.86"]).await.expect_bulk(b"4");
    call(&state, &[b"GET", b"k"]).await.expect_bulk(b"4");
}

#[tokio::test]
async fn incrbyfloat_on_non_numeric_errors() {
    let state = test_state();
    call(&state, &[b"SET", b"k", b"abc"]).await;
    call(&state, &[b"INCRBYFLOAT", b"k", b"1.0"]).await.expect_error_prefix("ERR");
}

#[tokio::test]
async fn incrbyfloat_preserves_ttl() {
    let state = test_state();
    call(&state, &[b"SET", b"k", b"1", b"EX", b"60"]).await;
    call(&state, &[b"INCRBYFLOAT", b"k", b"0.5"]).await.expect_bulk(b"1.5");
    let reply = call(&state, &[b"TTL", b"k"]).await;
    let raw = String::from_utf8_lossy(&reply.raw).into_owned();
    let ttl: i64 = raw.trim_start_matches(':').trim_end().parse().expect("ttl");
    assert!(ttl > 0, "ttl should be preserved, got {ttl}");
}

#[tokio::test]
async fn getrange_basic_and_negative_indices() {
    let state = test_state();
    call(&state, &[b"SET", b"k", b"Hello World"]).await;
    call(&state, &[b"GETRANGE", b"k", b"0", b"4"]).await.expect_bulk(b"Hello");
    call(&state, &[b"GETRANGE", b"k", b"6", b"10"]).await.expect_bulk(b"World");
    call(&state, &[b"GETRANGE", b"k", b"-5", b"-1"]).await.expect_bulk(b"World");
    call(&state, &[b"GETRANGE", b"k", b"0", b"-1"]).await.expect_bulk(b"Hello World");
}

#[tokio::test]
async fn getrange_out_of_bounds_returns_empty() {
    let state = test_state();
    call(&state, &[b"SET", b"k", b"abc"]).await;
    call(&state, &[b"GETRANGE", b"k", b"10", b"20"]).await.expect_bulk(b"");
    call(&state, &[b"GETRANGE", b"missing", b"0", b"5"]).await.expect_bulk(b"");
}

#[tokio::test]
async fn setrange_pads_and_overwrites() {
    let state = test_state();
    call(&state, &[b"SETRANGE", b"k", b"5", b"Hello"]).await.expect_integer(10);
    call(&state, &[b"GET", b"k"]).await.expect_bulk(b"\0\0\0\0\0Hello");
    call(&state, &[b"SETRANGE", b"k", b"0", b"World"]).await.expect_integer(10);
    // Overwriting the leading NULs with "World" leaves "WorldHello" — no
    // padding between the new prefix and the old trailing "Hello".
    call(&state, &[b"GET", b"k"]).await.expect_bulk(b"WorldHello");
}

#[tokio::test]
async fn setrange_empty_value_on_missing_is_noop() {
    let state = test_state();
    call(&state, &[b"SETRANGE", b"missing", b"0", b""]).await.expect_integer(0);
    call(&state, &[b"EXISTS", b"missing"]).await.expect_integer(0);
}

#[tokio::test]
async fn setrange_on_wrong_type_errors() {
    let state = test_state();
    call(&state, &[b"LPUSH", b"l", b"x"]).await;
    call(&state, &[b"SETRANGE", b"l", b"0", b"v"]).await.expect_error_prefix("ERR");
}

#[tokio::test]
async fn msetnx_all_or_nothing() {
    let state = test_state();
    call(&state, &[b"MSETNX", b"a", b"1", b"b", b"2"]).await.expect_integer(1);
    call(&state, &[b"GET", b"a"]).await.expect_bulk(b"1");
    call(&state, &[b"GET", b"b"]).await.expect_bulk(b"2");
    // One existing key → entire MSETNX fails.
    call(&state, &[b"MSETNX", b"c", b"3", b"b", b"4"]).await.expect_integer(0);
    call(&state, &[b"EXISTS", b"c"]).await.expect_integer(0);
    call(&state, &[b"GET", b"b"]).await.expect_bulk(b"2");
}

// ---------------------------------------------------------------------------
// ZREVRANGE / ZREVRANGEBYSCORE / ZREMRANGEBYRANK / ZREMRANGEBYSCORE / ZPOP*
// ---------------------------------------------------------------------------

#[tokio::test]
async fn zrevrange_returns_descending_slice() {
    let state = test_state();
    call(&state, &[b"ZADD", b"z", b"1", b"a", b"2", b"b", b"3", b"c", b"4", b"d"]).await;
    let m = call(&state, &[b"ZREVRANGE", b"z", b"0", b"1"]).await.into_bulk_array();
    assert_eq!(m.len(), 2);
    assert_eq!(m[0].as_ref(), b"d");
    assert_eq!(m[1].as_ref(), b"c");
    let full = call(&state, &[b"ZREVRANGE", b"z", b"0", b"-1"]).await.into_bulk_array();
    let names: Vec<&[u8]> = full.iter().map(AsRef::as_ref).collect();
    assert_eq!(names, vec![&b"d"[..], &b"c"[..], &b"b"[..], &b"a"[..]]);
}

#[tokio::test]
async fn zrevrangebyscore_descending_arg_order() {
    let state = test_state();
    call(&state, &[b"ZADD", b"z", b"1", b"a", b"2", b"b", b"3", b"c"]).await;
    // Redis semantics: max before min.
    let m = call(&state, &[b"ZREVRANGEBYSCORE", b"z", b"3", b"1"]).await.into_bulk_array();
    assert_eq!(m.len(), 3);
    assert_eq!(m[0].as_ref(), b"c");
    assert_eq!(m[2].as_ref(), b"a");
    let m = call(&state, &[b"ZREVRANGEBYSCORE", b"z", b"(3", b"1"]).await.into_bulk_array();
    assert_eq!(m.len(), 2);
    assert_eq!(m[0].as_ref(), b"b");
}

#[tokio::test]
async fn zremrangebyrank_removes_inclusive_window() {
    let state = test_state();
    call(&state, &[b"ZADD", b"z", b"1", b"a", b"2", b"b", b"3", b"c", b"4", b"d"]).await;
    call(&state, &[b"ZREMRANGEBYRANK", b"z", b"0", b"1"]).await.expect_integer(2);
    let remaining = call(&state, &[b"ZRANGE", b"z", b"0", b"-1"]).await.into_bulk_array();
    assert_eq!(remaining.len(), 2);
    assert_eq!(remaining[0].as_ref(), b"c");
    assert_eq!(remaining[1].as_ref(), b"d");
}

#[tokio::test]
async fn zremrangebyrank_empties_and_deletes_key() {
    let state = test_state();
    call(&state, &[b"ZADD", b"z", b"1", b"a", b"2", b"b"]).await;
    call(&state, &[b"ZREMRANGEBYRANK", b"z", b"0", b"-1"]).await.expect_integer(2);
    call(&state, &[b"EXISTS", b"z"]).await.expect_integer(0);
}

#[tokio::test]
async fn zremrangebyscore_removes_by_score_range() {
    let state = test_state();
    call(&state, &[b"ZADD", b"z", b"1", b"a", b"2", b"b", b"3", b"c", b"4", b"d"]).await;
    call(&state, &[b"ZREMRANGEBYSCORE", b"z", b"(1", b"3"]).await.expect_integer(2);
    let remaining = call(&state, &[b"ZRANGE", b"z", b"0", b"-1"]).await.into_bulk_array();
    let names: Vec<&[u8]> = remaining.iter().map(AsRef::as_ref).collect();
    assert_eq!(names, vec![&b"a"[..], &b"d"[..]]);
}

#[tokio::test]
async fn zpopmin_returns_lowest_score_with_score() {
    let state = test_state();
    call(&state, &[b"ZADD", b"z", b"1", b"a", b"2", b"b", b"3", b"c"]).await;
    let r = call(&state, &[b"ZPOPMIN", b"z"]).await.into_bulk_array();
    assert_eq!(r.len(), 2);
    assert_eq!(r[0].as_ref(), b"a");
    assert_eq!(r[1].as_ref(), b"1");
    call(&state, &[b"ZCARD", b"z"]).await.expect_integer(2);
}

#[tokio::test]
async fn zpopmax_returns_highest_score_with_score() {
    let state = test_state();
    call(&state, &[b"ZADD", b"z", b"1", b"a", b"2", b"b", b"3", b"c"]).await;
    let r = call(&state, &[b"ZPOPMAX", b"z"]).await.into_bulk_array();
    assert_eq!(r.len(), 2);
    assert_eq!(r[0].as_ref(), b"c");
    assert_eq!(r[1].as_ref(), b"3");
}

#[tokio::test]
async fn zpop_count_flattens_pairs() {
    let state = test_state();
    call(&state, &[b"ZADD", b"z", b"1", b"a", b"2", b"b", b"3", b"c"]).await;
    let r = call(&state, &[b"ZPOPMIN", b"z", b"2"]).await.into_bulk_array();
    // Flat RESP array: member, score, member, score.
    assert_eq!(r.len(), 4);
    assert_eq!(r[0].as_ref(), b"a");
    assert_eq!(r[1].as_ref(), b"1");
    assert_eq!(r[2].as_ref(), b"b");
    assert_eq!(r[3].as_ref(), b"2");
}

#[tokio::test]
async fn zpop_empties_and_deletes_key() {
    let state = test_state();
    call(&state, &[b"ZADD", b"z", b"1", b"a"]).await;
    call(&state, &[b"ZPOPMIN", b"z"]).await;
    call(&state, &[b"EXISTS", b"z"]).await.expect_integer(0);
}

#[tokio::test]
async fn zpop_missing_key_returns_empty_array() {
    let state = test_state();
    let r = call(&state, &[b"ZPOPMIN", b"missing"]).await.into_bulk_array();
    assert!(r.is_empty());
}

#[tokio::test]
async fn zpop_on_wrong_type_errors() {
    let state = test_state();
    call(&state, &[b"SET", b"k", b"v"]).await;
    call(&state, &[b"ZPOPMIN", b"k"]).await.expect_error_prefix("ERR");
    call(&state, &[b"ZPOPMAX", b"k"]).await.expect_error_prefix("ERR");
}

// Use RespReply publicly to check the crate re-export surface compiles.
#[test]
fn reply_encode_simple_works_from_tests() {
    let r = RespReply::ok();
    let enc = r.encode().expect("encode");
    assert_eq!(&enc[..], b"+OK\r\n");
}
