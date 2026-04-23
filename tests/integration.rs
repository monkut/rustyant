//! End-to-end tests for the RESP-over-HTTP Lambda handler backed by the real
//! `S3Storage` against a local floci emulator.
//!
//! Requires `RUSTYANT_FLOCI_URL` to be set (e.g. via `just floci-up` locally or
//! the `floci` service container in CI). Tests panic with a clear message if
//! floci is unreachable — silent skips have masked real coverage gaps before.
//!
//! Each test gets a unique key prefix (`it/<pid>/<counter>/`) so parallel
//! nextest runs don't share state. The bucket defaults to `rustyant-ci`
//! (override with `RUSTYANT_FLOCI_BUCKET`).

use bytes::Bytes;
use lambda_http::http::Request as HttpRequest;
use lambda_http::{Body, Request, Response};
use rustyant::handler::handle;
use rustyant::resp::{RespReply, parse_command};
use rustyant::state::State;
use rustyant::test_support::floci_state;

// ---------------------------------------------------------------------------
// Test harness helpers
// ---------------------------------------------------------------------------

fn test_state() -> State {
    floci_state("it")
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
// ZADD flag extensions — NX / XX / GT / LT / CH / INCR
// ---------------------------------------------------------------------------

#[tokio::test]
async fn zadd_nx_does_not_update_existing_member() {
    let state = test_state();
    call(&state, &[b"ZADD", b"z-nx", b"1", b"a"]).await.expect_integer(1);
    // NX: existing member is untouched; no new added → 0.
    call(&state, &[b"ZADD", b"z-nx", b"NX", b"9", b"a"]).await.expect_integer(0);
    // Score remains 1.
    let reply = call(&state, &[b"ZSCORE", b"z-nx", b"a"]).await;
    let s = String::from_utf8_lossy(&reply.raw);
    assert!(s.contains('1'), "score should still be 1:\n{s}");
}

#[tokio::test]
async fn zadd_xx_refuses_to_create_new_member() {
    let state = test_state();
    // Empty key; XX should refuse to create it.
    call(&state, &[b"ZADD", b"z-xx", b"XX", b"1", b"a"]).await.expect_integer(0);
    call(&state, &[b"EXISTS", b"z-xx"]).await.expect_integer(0);
}

#[tokio::test]
async fn zadd_gt_only_updates_when_new_score_is_higher() {
    let state = test_state();
    call(&state, &[b"ZADD", b"z-gt", b"5", b"a"]).await.expect_integer(1);
    // 3 is lower — update suppressed.
    call(&state, &[b"ZADD", b"z-gt", b"GT", b"CH", b"3", b"a"]).await.expect_integer(0);
    // 10 is higher — update passes.
    call(&state, &[b"ZADD", b"z-gt", b"GT", b"CH", b"10", b"a"]).await.expect_integer(1);
    // GT doesn't block fresh inserts.
    call(&state, &[b"ZADD", b"z-gt", b"GT", b"1", b"new"]).await.expect_integer(1);
}

#[tokio::test]
async fn zadd_lt_only_updates_when_new_score_is_lower() {
    let state = test_state();
    call(&state, &[b"ZADD", b"z-lt", b"5", b"a"]).await.expect_integer(1);
    // 10 is higher — suppressed.
    call(&state, &[b"ZADD", b"z-lt", b"LT", b"CH", b"10", b"a"]).await.expect_integer(0);
    // 3 is lower — passes.
    call(&state, &[b"ZADD", b"z-lt", b"LT", b"CH", b"3", b"a"]).await.expect_integer(1);
    // LT doesn't block fresh inserts.
    call(&state, &[b"ZADD", b"z-lt", b"LT", b"100", b"new"]).await.expect_integer(1);
}

#[tokio::test]
async fn zadd_rejects_gt_and_lt_together() {
    let state = test_state();
    call(&state, &[b"ZADD", b"z-mutex", b"GT", b"LT", b"1", b"a"]).await.expect_error_prefix("ERR");
}

#[tokio::test]
async fn zadd_rejects_nx_with_gt_or_lt() {
    let state = test_state();
    call(&state, &[b"ZADD", b"z-mutex2", b"NX", b"GT", b"1", b"a"]).await.expect_error_prefix("ERR");
    call(&state, &[b"ZADD", b"z-mutex3", b"NX", b"LT", b"1", b"a"]).await.expect_error_prefix("ERR");
}

#[tokio::test]
async fn zadd_rejects_nx_with_xx() {
    let state = test_state();
    call(&state, &[b"ZADD", b"z-mutex4", b"NX", b"XX", b"1", b"a"]).await.expect_error_prefix("ERR");
}

#[tokio::test]
async fn zadd_incr_on_missing_member_sets_score_to_delta() {
    let state = test_state();
    let reply = call(&state, &[b"ZADD", b"z-incr", b"INCR", b"5", b"a"]).await;
    reply.expect_bulk(b"5");
}

#[tokio::test]
async fn zadd_incr_on_existing_member_adds_to_score() {
    let state = test_state();
    call(&state, &[b"ZADD", b"z-incr2", b"5", b"a"]).await.expect_integer(1);
    let reply = call(&state, &[b"ZADD", b"z-incr2", b"INCR", b"3", b"a"]).await;
    reply.expect_bulk(b"8");
}

#[tokio::test]
async fn zadd_incr_nx_on_existing_returns_nil() {
    let state = test_state();
    call(&state, &[b"ZADD", b"z-incr-nx", b"5", b"a"]).await.expect_integer(1);
    let reply = call(&state, &[b"ZADD", b"z-incr-nx", b"NX", b"INCR", b"3", b"a"]).await;
    reply.expect_nil();
    // Score untouched.
    let reply = call(&state, &[b"ZSCORE", b"z-incr-nx", b"a"]).await;
    let s = String::from_utf8_lossy(&reply.raw);
    assert!(s.contains('5'), "score should still be 5:\n{s}");
}

#[tokio::test]
async fn zadd_incr_xx_on_missing_returns_nil() {
    let state = test_state();
    let reply = call(&state, &[b"ZADD", b"z-incr-xx", b"XX", b"INCR", b"3", b"a"]).await;
    reply.expect_nil();
    call(&state, &[b"EXISTS", b"z-incr-xx"]).await.expect_integer(0);
}

#[tokio::test]
async fn zadd_incr_gt_suppresses_decrement() {
    let state = test_state();
    call(&state, &[b"ZADD", b"z-incr-gt", b"5", b"a"]).await.expect_integer(1);
    // +(-2) = 3 which is < 5 → GT suppresses.
    let reply = call(&state, &[b"ZADD", b"z-incr-gt", b"GT", b"INCR", b"-2", b"a"]).await;
    reply.expect_nil();
    // +2 = 7 which is > 5 → GT passes.
    let reply = call(&state, &[b"ZADD", b"z-incr-gt", b"GT", b"INCR", b"2", b"a"]).await;
    reply.expect_bulk(b"7");
}

#[tokio::test]
async fn zadd_incr_lt_suppresses_increment() {
    let state = test_state();
    call(&state, &[b"ZADD", b"z-incr-lt", b"5", b"a"]).await.expect_integer(1);
    // +2 = 7, LT requires lower → suppressed.
    let reply = call(&state, &[b"ZADD", b"z-incr-lt", b"LT", b"INCR", b"2", b"a"]).await;
    reply.expect_nil();
    // -2 = 3 < 5 → passes.
    let reply = call(&state, &[b"ZADD", b"z-incr-lt", b"LT", b"INCR", b"-2", b"a"]).await;
    reply.expect_bulk(b"3");
}

#[tokio::test]
async fn zadd_incr_requires_single_pair() {
    let state = test_state();
    call(&state, &[b"ZADD", b"z-multi", b"INCR", b"1", b"a", b"2", b"b"]).await.expect_error_prefix("ERR");
}

#[tokio::test]
async fn zadd_ch_counts_score_updates() {
    let state = test_state();
    call(&state, &[b"ZADD", b"z-ch", b"1", b"a", b"2", b"b"]).await.expect_integer(2);
    // Without CH: update to an existing member doesn't count.
    call(&state, &[b"ZADD", b"z-ch", b"10", b"a"]).await.expect_integer(0);
    // With CH: score change counts + one fresh add.
    call(&state, &[b"ZADD", b"z-ch", b"CH", b"20", b"a", b"99", b"new"]).await.expect_integer(2);
}

#[tokio::test]
async fn zadd_ch_with_gt_counts_only_passing_updates() {
    let state = test_state();
    call(&state, &[b"ZADD", b"z-ch-gt", b"5", b"a", b"5", b"b"]).await.expect_integer(2);
    // Only the 'b' update passes GT (6 > 5); 'a' blocked (3 <= 5).
    call(&state, &[b"ZADD", b"z-ch-gt", b"GT", b"CH", b"3", b"a", b"6", b"b"]).await.expect_integer(1);
}

// ---------------------------------------------------------------------------
// String multi-key + NX/EX (SETNX, SETEX, MGET, MSET)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn setnx_sets_missing_key() {
    // SETNX relies on S3's `If-None-Match: *` conditional write to enforce
    // "only create if absent". Floci (our test emulator) silently ignores
    // the header and returns a fresh ETag on every PUT, so the second SETNX
    // appears to succeed. Same gate as `tests/floci.rs::s3_concurrent_incr_converges`.
    if std::env::var("RUSTYANT_S3_CAS").is_err() {
        eprintln!("SKIP: RUSTYANT_S3_CAS not set (floci does not enforce If-None-Match)");
        return;
    }
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
// ZRANGEBYLEX / ZREVRANGEBYLEX / ZLEXCOUNT / ZREMRANGEBYLEX
// Lex bounds assume equal scores across members (the canonical Redis use case).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn zrangebylex_inclusive_and_exclusive() {
    let state = test_state();
    call(&state, &[b"ZADD", b"z", b"0", b"a", b"0", b"b", b"0", b"c", b"0", b"d"]).await;
    // [b ... [c → inclusive both ends → b, c
    let m = call(&state, &[b"ZRANGEBYLEX", b"z", b"[b", b"[c"]).await.into_bulk_array();
    assert_eq!(m.iter().map(|b| b.to_vec()).collect::<Vec<_>>(), vec![b"b".to_vec(), b"c".to_vec()]);
    // (a ... (d → exclusive → b, c
    let m = call(&state, &[b"ZRANGEBYLEX", b"z", b"(a", b"(d"]).await.into_bulk_array();
    assert_eq!(m.iter().map(|b| b.to_vec()).collect::<Vec<_>>(), vec![b"b".to_vec(), b"c".to_vec()]);
}

#[tokio::test]
async fn zrangebylex_unbounded() {
    let state = test_state();
    call(&state, &[b"ZADD", b"z", b"0", b"a", b"0", b"b", b"0", b"c"]).await;
    // - ... + → all members in lex order
    let m = call(&state, &[b"ZRANGEBYLEX", b"z", b"-", b"+"]).await.into_bulk_array();
    assert_eq!(m.iter().map(|b| b.to_vec()).collect::<Vec<_>>(), vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
}

#[tokio::test]
async fn zrangebylex_limit() {
    let state = test_state();
    call(&state, &[b"ZADD", b"z", b"0", b"a", b"0", b"b", b"0", b"c", b"0", b"d", b"0", b"e"]).await;
    // offset 1, count 2 → b, c
    let m = call(&state, &[b"ZRANGEBYLEX", b"z", b"-", b"+", b"LIMIT", b"1", b"2"]).await.into_bulk_array();
    assert_eq!(m.iter().map(|b| b.to_vec()).collect::<Vec<_>>(), vec![b"b".to_vec(), b"c".to_vec()]);
    // Negative count → no cap (returns rest after offset).
    let m = call(&state, &[b"ZRANGEBYLEX", b"z", b"-", b"+", b"LIMIT", b"2", b"-1"]).await.into_bulk_array();
    assert_eq!(m.iter().map(|b| b.to_vec()).collect::<Vec<_>>(), vec![b"c".to_vec(), b"d".to_vec(), b"e".to_vec()]);
}

#[tokio::test]
async fn zrevrangebylex_reverse_order() {
    let state = test_state();
    call(&state, &[b"ZADD", b"z", b"0", b"a", b"0", b"b", b"0", b"c", b"0", b"d"]).await;
    // Reverse: note Redis takes (max, min) in that order.
    let m = call(&state, &[b"ZREVRANGEBYLEX", b"z", b"[c", b"[a"]).await.into_bulk_array();
    assert_eq!(m.iter().map(|b| b.to_vec()).collect::<Vec<_>>(), vec![b"c".to_vec(), b"b".to_vec(), b"a".to_vec()]);
    // Unbounded reverse.
    let m = call(&state, &[b"ZREVRANGEBYLEX", b"z", b"+", b"-"]).await.into_bulk_array();
    assert_eq!(m.len(), 4);
    assert_eq!(m[0].as_ref(), b"d");
}

#[tokio::test]
async fn zlexcount_counts_window() {
    let state = test_state();
    call(&state, &[b"ZADD", b"z", b"0", b"a", b"0", b"b", b"0", b"c"]).await;
    call(&state, &[b"ZLEXCOUNT", b"z", b"-", b"+"]).await.expect_integer(3);
    call(&state, &[b"ZLEXCOUNT", b"z", b"[a", b"[b"]).await.expect_integer(2);
    call(&state, &[b"ZLEXCOUNT", b"z", b"(a", b"(c"]).await.expect_integer(1);
    call(&state, &[b"ZLEXCOUNT", b"missing", b"-", b"+"]).await.expect_integer(0);
}

#[tokio::test]
async fn zremrangebylex_removes_and_empties() {
    let state = test_state();
    call(&state, &[b"ZADD", b"z", b"0", b"a", b"0", b"b", b"0", b"c"]).await;
    // Remove `b` only.
    call(&state, &[b"ZREMRANGEBYLEX", b"z", b"[b", b"[b"]).await.expect_integer(1);
    call(&state, &[b"ZCARD", b"z"]).await.expect_integer(2);
    // Remove the rest — key should disappear.
    call(&state, &[b"ZREMRANGEBYLEX", b"z", b"-", b"+"]).await.expect_integer(2);
    call(&state, &[b"EXISTS", b"z"]).await.expect_integer(0);
}

#[tokio::test]
async fn zlex_missing_prefix_errors() {
    let state = test_state();
    call(&state, &[b"ZADD", b"z", b"0", b"a"]).await;
    // Without a [ ( - + prefix, it's a syntax error.
    call(&state, &[b"ZRANGEBYLEX", b"z", b"a", b"b"]).await.expect_error_prefix("ERR");
    call(&state, &[b"ZLEXCOUNT", b"z", b"a", b"b"]).await.expect_error_prefix("ERR");
}

#[tokio::test]
async fn zlex_wrong_type_errors() {
    let state = test_state();
    call(&state, &[b"SET", b"k", b"v"]).await;
    call(&state, &[b"ZRANGEBYLEX", b"k", b"-", b"+"]).await.expect_error_prefix("ERR");
    call(&state, &[b"ZLEXCOUNT", b"k", b"-", b"+"]).await.expect_error_prefix("ERR");
    call(&state, &[b"ZREMRANGEBYLEX", b"k", b"-", b"+"]).await.expect_error_prefix("ERR");
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
async fn keys_excludes_expired_after_lazy_gc() {
    let state = test_state();
    // PX=1 with short sleep guarantees expiry in wall-clock terms. On S3 the
    // object itself lingers until something touches the key and triggers
    // lazy GC — matching the documented keyspace semantic in README.md. A
    // GET on the expired key evicts it; the following KEYS must then omit it.
    call(&state, &[b"SET", b"will-expire", b"v", b"PX", b"1"]).await;
    call(&state, &[b"SET", b"stays", b"v"]).await;
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    call(&state, &[b"GET", b"will-expire"]).await.expect_nil();
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

// ---------------------------------------------------------------------------
// HSCAN / SSCAN / ZSCAN
// ---------------------------------------------------------------------------

#[tokio::test]
async fn hscan_paginates_full_hash() {
    let state = test_state();
    let mut args: Vec<&[u8]> = vec![b"HSET", b"h"];
    let pairs: Vec<(String, String)> = (0..12).map(|i| (format!("f{i:02}"), format!("v{i:02}"))).collect();
    let owned: Vec<Vec<u8>> = pairs.iter().flat_map(|(k, v)| [k.as_bytes().to_vec(), v.as_bytes().to_vec()]).collect();
    for b in &owned {
        args.push(b.as_slice());
    }
    call(&state, &args).await;

    let mut seen: Vec<(String, Bytes)> = Vec::new();
    let mut cursor: u64 = 0;
    loop {
        let (next, batch) = state.storage.hscan("h", cursor, None, 5).await.expect("hscan");
        seen.extend(batch);
        if next == 0 {
            break;
        }
        cursor = next;
    }
    seen.sort_by(|a, b| a.0.cmp(&b.0));
    let want: Vec<(String, Bytes)> = pairs.into_iter().map(|(k, v)| (k, Bytes::from(v.into_bytes()))).collect();
    assert_eq!(seen, want);
}

#[tokio::test]
async fn hscan_match_filter_narrows_batch() {
    let state = test_state();
    call(&state, &[b"HSET", b"h", b"user:1", b"a", b"user:2", b"b", b"other", b"c"]).await;
    let (next, batch) = state.storage.hscan("h", 0, Some("user:*"), 100).await.expect("hscan");
    assert_eq!(next, 0);
    let mut fields: Vec<String> = batch.into_iter().map(|(f, _)| f).collect();
    fields.sort();
    assert_eq!(fields, vec!["user:1".to_string(), "user:2".to_string()]);
}

#[tokio::test]
async fn hscan_missing_key_returns_done() {
    let state = test_state();
    let (next, batch) = state.storage.hscan("nope", 0, None, 10).await.expect("hscan");
    assert_eq!(next, 0);
    assert!(batch.is_empty());
}

#[tokio::test]
async fn hscan_wrong_type_errors() {
    let state = test_state();
    call(&state, &[b"SET", b"s", b"v"]).await;
    let res = state.storage.hscan("s", 0, None, 10).await;
    assert!(res.is_err(), "expected WRONGTYPE, got {res:?}");
}

#[tokio::test]
async fn sscan_paginates_full_set() {
    let state = test_state();
    let mut args: Vec<&[u8]> = vec![b"SADD", b"s"];
    let members: Vec<String> = (0..12).map(|i| format!("m{i:02}")).collect();
    let owned: Vec<Vec<u8>> = members.iter().map(|m| m.as_bytes().to_vec()).collect();
    for b in &owned {
        args.push(b.as_slice());
    }
    call(&state, &args).await;

    let mut seen: Vec<String> = Vec::new();
    let mut cursor: u64 = 0;
    loop {
        let (next, batch) = state.storage.sscan("s", cursor, None, 5).await.expect("sscan");
        seen.extend(batch);
        if next == 0 {
            break;
        }
        cursor = next;
    }
    seen.sort();
    let mut want = members;
    want.sort();
    assert_eq!(seen, want);
}

#[tokio::test]
async fn sscan_match_filter_narrows_batch() {
    let state = test_state();
    call(&state, &[b"SADD", b"s", b"user:1", b"user:2", b"other"]).await;
    let (next, batch) = state.storage.sscan("s", 0, Some("user:*"), 100).await.expect("sscan");
    assert_eq!(next, 0);
    let mut m = batch;
    m.sort();
    assert_eq!(m, vec!["user:1".to_string(), "user:2".to_string()]);
}

#[tokio::test]
async fn zscan_paginates_with_scores() {
    let state = test_state();
    for (score, member) in [(1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e"), (6, "f")] {
        call(&state, &[b"ZADD", b"z", score.to_string().as_bytes(), member.as_bytes()]).await;
    }
    let mut seen: Vec<(String, f64)> = Vec::new();
    let mut cursor: u64 = 0;
    loop {
        let (next, batch) = state.storage.zscan("z", cursor, None, 2).await.expect("zscan");
        seen.extend(batch);
        if next == 0 {
            break;
        }
        cursor = next;
    }
    seen.sort_by(|a, b| a.0.cmp(&b.0));
    let want: Vec<(String, f64)> = vec![("a", 1.0), ("b", 2.0), ("c", 3.0), ("d", 4.0), ("e", 5.0), ("f", 6.0)]
        .into_iter()
        .map(|(m, s)| (m.to_string(), s))
        .collect();
    assert_eq!(seen, want);
}

// ---------------------------------------------------------------------------
// ZINTERSTORE / ZUNIONSTORE / ZDIFFSTORE — sorted-set aggregates into dst.
// Inputs can be SET (contributing score 1.0) or ZSET. WEIGHTS multiply each
// input's scores before AGGREGATE combines overlapping members.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn zinterstore_sums_scores_by_default() {
    let state = test_state();
    call(&state, &[b"ZADD", b"zi-a", b"1", b"x", b"2", b"y"]).await;
    call(&state, &[b"ZADD", b"zi-b", b"3", b"x", b"4", b"z"]).await;
    call(&state, &[b"ZINTERSTORE", b"zi-dst", b"2", b"zi-a", b"zi-b"]).await.expect_integer(1);
    // x is the only common member; SUM(1, 3) = 4.
    let reply = call(&state, &[b"ZSCORE", b"zi-dst", b"x"]).await;
    reply.expect_bulk(b"4");
}

#[tokio::test]
async fn zinterstore_with_weights_multiplies_scores() {
    let state = test_state();
    call(&state, &[b"ZADD", b"ziw-a", b"1", b"x"]).await;
    call(&state, &[b"ZADD", b"ziw-b", b"1", b"x"]).await;
    call(&state, &[b"ZINTERSTORE", b"ziw-dst", b"2", b"ziw-a", b"ziw-b", b"WEIGHTS", b"2", b"3"])
        .await
        .expect_integer(1);
    // 1*2 + 1*3 = 5.
    call(&state, &[b"ZSCORE", b"ziw-dst", b"x"]).await.expect_bulk(b"5");
}

#[tokio::test]
async fn zinterstore_aggregate_min_picks_smaller() {
    let state = test_state();
    call(&state, &[b"ZADD", b"zim-a", b"10", b"x"]).await;
    call(&state, &[b"ZADD", b"zim-b", b"3", b"x"]).await;
    call(&state, &[b"ZINTERSTORE", b"zim-dst", b"2", b"zim-a", b"zim-b", b"AGGREGATE", b"MIN"]).await.expect_integer(1);
    call(&state, &[b"ZSCORE", b"zim-dst", b"x"]).await.expect_bulk(b"3");
}

#[tokio::test]
async fn zinterstore_aggregate_max_picks_larger() {
    let state = test_state();
    call(&state, &[b"ZADD", b"zix-a", b"10", b"x"]).await;
    call(&state, &[b"ZADD", b"zix-b", b"3", b"x"]).await;
    call(&state, &[b"ZINTERSTORE", b"zix-dst", b"2", b"zix-a", b"zix-b", b"AGGREGATE", b"MAX"]).await.expect_integer(1);
    call(&state, &[b"ZSCORE", b"zix-dst", b"x"]).await.expect_bulk(b"10");
}

#[tokio::test]
async fn zinterstore_accepts_set_input_with_score_one() {
    let state = test_state();
    call(&state, &[b"ZADD", b"zis-z", b"5", b"x", b"7", b"y"]).await;
    call(&state, &[b"SADD", b"zis-s", b"x"]).await;
    // Intersection: x only; SUM(5, 1) = 6.
    call(&state, &[b"ZINTERSTORE", b"zis-dst", b"2", b"zis-z", b"zis-s"]).await.expect_integer(1);
    call(&state, &[b"ZSCORE", b"zis-dst", b"x"]).await.expect_bulk(b"6");
}

#[tokio::test]
async fn zunionstore_combines_scores_across_sources() {
    let state = test_state();
    call(&state, &[b"ZADD", b"zu-a", b"1", b"x", b"2", b"y"]).await;
    call(&state, &[b"ZADD", b"zu-b", b"10", b"y", b"3", b"z"]).await;
    call(&state, &[b"ZUNIONSTORE", b"zu-dst", b"2", b"zu-a", b"zu-b"]).await.expect_integer(3);
    // x only in a → 1; y in both → SUM(2, 10) = 12; z only in b → 3.
    call(&state, &[b"ZSCORE", b"zu-dst", b"x"]).await.expect_bulk(b"1");
    call(&state, &[b"ZSCORE", b"zu-dst", b"y"]).await.expect_bulk(b"12");
    call(&state, &[b"ZSCORE", b"zu-dst", b"z"]).await.expect_bulk(b"3");
}

#[tokio::test]
async fn zdiffstore_keeps_members_only_in_first_source() {
    let state = test_state();
    call(&state, &[b"ZADD", b"zd-a", b"1", b"x", b"2", b"y", b"3", b"z"]).await;
    call(&state, &[b"ZADD", b"zd-b", b"9", b"y"]).await;
    call(&state, &[b"ZADD", b"zd-c", b"9", b"z"]).await;
    call(&state, &[b"ZDIFFSTORE", b"zd-dst", b"3", b"zd-a", b"zd-b", b"zd-c"]).await.expect_integer(1);
    // Only x survives; first-set score preserved.
    call(&state, &[b"ZSCORE", b"zd-dst", b"x"]).await.expect_bulk(b"1");
}

#[tokio::test]
async fn zdiffstore_rejects_weights_or_aggregate() {
    let state = test_state();
    call(&state, &[b"ZADD", b"zdw-a", b"1", b"x"]).await;
    call(&state, &[b"ZADD", b"zdw-b", b"1", b"x"]).await;
    call(&state, &[b"ZDIFFSTORE", b"zdw-dst", b"2", b"zdw-a", b"zdw-b", b"WEIGHTS", b"1", b"1"])
        .await
        .expect_error_prefix("ERR");
}

#[tokio::test]
async fn zstore_empty_result_deletes_destination() {
    let state = test_state();
    call(&state, &[b"ZADD", b"ze1-a", b"1", b"x"]).await;
    call(&state, &[b"ZADD", b"ze1-b", b"1", b"y"]).await;
    // Pre-populate the destination; empty intersection should wipe it.
    call(&state, &[b"ZADD", b"ze1-dst", b"1", b"leftover"]).await;
    call(&state, &[b"ZINTERSTORE", b"ze1-dst", b"2", b"ze1-a", b"ze1-b"]).await.expect_integer(0);
    call(&state, &[b"EXISTS", b"ze1-dst"]).await.expect_integer(0);
}

#[tokio::test]
async fn zstore_overwrites_destination_of_any_type() {
    let state = test_state();
    call(&state, &[b"ZADD", b"zo-a", b"1", b"x"]).await;
    // Destination is a string; ZINTERSTORE overwrites it unconditionally.
    call(&state, &[b"SET", b"zo-dst", b"ignored"]).await.expect_simple("OK");
    call(&state, &[b"ZINTERSTORE", b"zo-dst", b"1", b"zo-a"]).await.expect_integer(1);
    call(&state, &[b"TYPE", b"zo-dst"]).await.expect_simple("zset");
}

#[tokio::test]
async fn zstore_numkeys_zero_errors() {
    let state = test_state();
    call(&state, &[b"ZINTERSTORE", b"dst", b"0"]).await.expect_error_prefix("ERR");
}

#[tokio::test]
async fn zstore_numkeys_exceeds_arg_count_errors() {
    let state = test_state();
    call(&state, &[b"ZINTERSTORE", b"dst", b"3", b"a", b"b"]).await.expect_error_prefix("ERR");
}

#[tokio::test]
async fn zinterstore_rejects_unknown_aggregate_mode() {
    let state = test_state();
    call(&state, &[b"ZADD", b"za-a", b"1", b"x"]).await;
    call(&state, &[b"ZINTERSTORE", b"dst", b"1", b"za-a", b"AGGREGATE", b"MEDIAN"]).await.expect_error_prefix("ERR");
}

#[tokio::test]
async fn zinterstore_weights_count_mismatch_errors() {
    let state = test_state();
    call(&state, &[b"ZADD", b"zwm-a", b"1", b"x"]).await;
    call(&state, &[b"ZADD", b"zwm-b", b"1", b"y"]).await;
    // Two inputs but only one weight provided.
    call(&state, &[b"ZINTERSTORE", b"dst", b"2", b"zwm-a", b"zwm-b", b"WEIGHTS", b"2"])
        .await
        .expect_error_prefix("ERR");
}

#[tokio::test]
async fn zstore_commands_registered_in_command_meta() {
    let state = test_state();
    for name in [
        b"SINTERSTORE".as_ref(),
        b"SUNIONSTORE".as_ref(),
        b"SDIFFSTORE".as_ref(),
        b"ZINTERSTORE".as_ref(),
        b"ZUNIONSTORE".as_ref(),
        b"ZDIFFSTORE".as_ref(),
    ] {
        let reply = call(&state, &[b"COMMAND", b"INFO", name]).await;
        let s = String::from_utf8_lossy(&reply.raw);
        assert!(s.starts_with("*1\r\n*6\r\n"), "{name:?} not in COMMAND INFO:\n{s}");
    }
}

#[tokio::test]
async fn hscan_wire_reply_has_cursor_and_pairs() {
    // End-to-end wire check: a single-page HSCAN must return
    // `[b"0", [field, value, field, value, ...]]` as a nested array.
    let state = test_state();
    call(&state, &[b"HSET", b"h", b"f1", b"v1", b"f2", b"v2"]).await;
    let decoded = call(&state, &[b"HSCAN", b"h", b"0"]).await;
    // Reply shape: *2\r\n$1\r\n0\r\n*4\r\n$...
    assert!(
        decoded.raw.starts_with(b"*2\r\n$1\r\n0\r\n*4\r\n"),
        "unexpected wire: {:?}",
        String::from_utf8_lossy(&decoded.raw)
    );
}

#[tokio::test]
async fn sscan_wrong_arity_errors() {
    let state = test_state();
    call(&state, &[b"SSCAN"]).await.expect_error_prefix("wrong number");
    call(&state, &[b"SSCAN", b"k"]).await.expect_error_prefix("wrong number");
}

#[tokio::test]
async fn zscan_negative_cursor_rejected() {
    let state = test_state();
    call(&state, &[b"ZADD", b"z", b"1", b"a"]).await;
    call(&state, &[b"ZSCAN", b"z", b"-1"]).await.expect_error_prefix("cursor");
}

#[tokio::test]
async fn hscan_unknown_option_rejected() {
    let state = test_state();
    call(&state, &[b"HSET", b"h", b"f", b"v"]).await;
    call(&state, &[b"HSCAN", b"h", b"0", b"NOVALUES"]).await.expect_error_prefix("unsupported HSCAN");
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
// LMOVE / RPOPLPUSH / LPOS
// ---------------------------------------------------------------------------

#[tokio::test]
async fn lmove_right_to_left_moves_element() {
    let state = test_state();
    call(&state, &[b"RPUSH", b"src", b"a", b"b", b"c"]).await;
    call(&state, &[b"RPUSH", b"dst", b"x", b"y"]).await;
    call(&state, &[b"LMOVE", b"src", b"dst", b"RIGHT", b"LEFT"]).await.expect_bulk(b"c");
    let src = call(&state, &[b"LRANGE", b"src", b"0", b"-1"]).await.into_bulk_array();
    assert_eq!(bulks_to_strs(&src), vec!["a", "b"]);
    let dst = call(&state, &[b"LRANGE", b"dst", b"0", b"-1"]).await.into_bulk_array();
    assert_eq!(bulks_to_strs(&dst), vec!["c", "x", "y"]);
}

#[tokio::test]
async fn lmove_all_direction_combos() {
    let state = test_state();
    // LEFT → LEFT: pop head of src, push head of dst.
    call(&state, &[b"RPUSH", b"s1", b"a", b"b"]).await;
    call(&state, &[b"RPUSH", b"d1", b"y"]).await;
    call(&state, &[b"LMOVE", b"s1", b"d1", b"LEFT", b"LEFT"]).await.expect_bulk(b"a");
    let d1 = call(&state, &[b"LRANGE", b"d1", b"0", b"-1"]).await.into_bulk_array();
    assert_eq!(bulks_to_strs(&d1), vec!["a", "y"]);
    // LEFT → RIGHT
    call(&state, &[b"RPUSH", b"s2", b"a", b"b"]).await;
    call(&state, &[b"RPUSH", b"d2", b"y"]).await;
    call(&state, &[b"LMOVE", b"s2", b"d2", b"LEFT", b"RIGHT"]).await.expect_bulk(b"a");
    let d2 = call(&state, &[b"LRANGE", b"d2", b"0", b"-1"]).await.into_bulk_array();
    assert_eq!(bulks_to_strs(&d2), vec!["y", "a"]);
    // RIGHT → RIGHT
    call(&state, &[b"RPUSH", b"s3", b"a", b"b"]).await;
    call(&state, &[b"RPUSH", b"d3", b"y"]).await;
    call(&state, &[b"LMOVE", b"s3", b"d3", b"RIGHT", b"RIGHT"]).await.expect_bulk(b"b");
    let d3 = call(&state, &[b"LRANGE", b"d3", b"0", b"-1"]).await.into_bulk_array();
    assert_eq!(bulks_to_strs(&d3), vec!["y", "b"]);
}

#[tokio::test]
async fn lmove_same_key_rotates() {
    let state = test_state();
    call(&state, &[b"RPUSH", b"l", b"a", b"b", b"c"]).await;
    // RIGHT → LEFT rotates: c becomes new head.
    call(&state, &[b"LMOVE", b"l", b"l", b"RIGHT", b"LEFT"]).await.expect_bulk(b"c");
    let items = call(&state, &[b"LRANGE", b"l", b"0", b"-1"]).await.into_bulk_array();
    assert_eq!(bulks_to_strs(&items), vec!["c", "a", "b"]);
}

#[tokio::test]
async fn lmove_creates_missing_destination() {
    let state = test_state();
    call(&state, &[b"RPUSH", b"src", b"a", b"b"]).await;
    call(&state, &[b"LMOVE", b"src", b"newdst", b"LEFT", b"RIGHT"]).await.expect_bulk(b"a");
    call(&state, &[b"TYPE", b"newdst"]).await.expect_simple("list");
    let dst = call(&state, &[b"LRANGE", b"newdst", b"0", b"-1"]).await.into_bulk_array();
    assert_eq!(bulks_to_strs(&dst), vec!["a"]);
}

#[tokio::test]
async fn lmove_empty_source_returns_nil_and_leaves_dst() {
    let state = test_state();
    call(&state, &[b"RPUSH", b"dst", b"y"]).await;
    call(&state, &[b"LMOVE", b"missing", b"dst", b"LEFT", b"LEFT"]).await.expect_nil();
    let dst = call(&state, &[b"LRANGE", b"dst", b"0", b"-1"]).await.into_bulk_array();
    assert_eq!(bulks_to_strs(&dst), vec!["y"]);
}

#[tokio::test]
async fn lmove_wrong_source_type_errors() {
    let state = test_state();
    call(&state, &[b"SET", b"s", b"v"]).await;
    call(&state, &[b"RPUSH", b"d", b"x"]).await;
    call(&state, &[b"LMOVE", b"s", b"d", b"LEFT", b"LEFT"]).await.expect_error_prefix("ERR");
}

#[tokio::test]
async fn lmove_wrong_destination_type_errors_without_popping() {
    let state = test_state();
    call(&state, &[b"RPUSH", b"src", b"a", b"b"]).await;
    call(&state, &[b"SET", b"dst", b"v"]).await;
    call(&state, &[b"LMOVE", b"src", b"dst", b"LEFT", b"LEFT"]).await.expect_error_prefix("ERR");
    // Source untouched.
    let src = call(&state, &[b"LRANGE", b"src", b"0", b"-1"]).await.into_bulk_array();
    assert_eq!(bulks_to_strs(&src), vec!["a", "b"]);
}

#[tokio::test]
async fn lmove_rejects_invalid_side() {
    let state = test_state();
    call(&state, &[b"RPUSH", b"l", b"a"]).await;
    call(&state, &[b"LMOVE", b"l", b"l", b"UP", b"LEFT"]).await.expect_error_prefix("ERR");
}

#[tokio::test]
async fn rpoplpush_moves_tail_to_head() {
    let state = test_state();
    call(&state, &[b"RPUSH", b"src", b"a", b"b", b"c"]).await;
    call(&state, &[b"RPUSH", b"dst", b"y"]).await;
    call(&state, &[b"RPOPLPUSH", b"src", b"dst"]).await.expect_bulk(b"c");
    let dst = call(&state, &[b"LRANGE", b"dst", b"0", b"-1"]).await.into_bulk_array();
    assert_eq!(bulks_to_strs(&dst), vec!["c", "y"]);
}

#[tokio::test]
async fn rpoplpush_same_key_rotates() {
    let state = test_state();
    call(&state, &[b"RPUSH", b"l", b"a", b"b", b"c"]).await;
    call(&state, &[b"RPOPLPUSH", b"l", b"l"]).await.expect_bulk(b"c");
    let items = call(&state, &[b"LRANGE", b"l", b"0", b"-1"]).await.into_bulk_array();
    assert_eq!(bulks_to_strs(&items), vec!["c", "a", "b"]);
}

#[tokio::test]
async fn rpoplpush_missing_source_returns_nil() {
    let state = test_state();
    call(&state, &[b"RPOPLPUSH", b"ghost", b"dst"]).await.expect_nil();
    call(&state, &[b"EXISTS", b"dst"]).await.expect_integer(0);
}

#[tokio::test]
async fn lpos_returns_first_match_index() {
    let state = test_state();
    call(&state, &[b"RPUSH", b"l", b"a", b"b", b"c", b"b", b"d"]).await;
    call(&state, &[b"LPOS", b"l", b"b"]).await.expect_integer(1);
}

#[tokio::test]
async fn lpos_missing_element_returns_nil() {
    let state = test_state();
    call(&state, &[b"RPUSH", b"l", b"a", b"b"]).await;
    call(&state, &[b"LPOS", b"l", b"z"]).await.expect_nil();
}

#[tokio::test]
async fn lpos_missing_key_returns_nil() {
    let state = test_state();
    call(&state, &[b"LPOS", b"ghost", b"x"]).await.expect_nil();
}

#[tokio::test]
async fn lpos_with_rank_skips_occurrences() {
    let state = test_state();
    call(&state, &[b"RPUSH", b"l", b"a", b"b", b"c", b"b", b"d", b"b"]).await;
    // 2nd occurrence forward.
    call(&state, &[b"LPOS", b"l", b"b", b"RANK", b"2"]).await.expect_integer(3);
    // 1st occurrence backward.
    call(&state, &[b"LPOS", b"l", b"b", b"RANK", b"-1"]).await.expect_integer(5);
    // 2nd occurrence backward.
    call(&state, &[b"LPOS", b"l", b"b", b"RANK", b"-2"]).await.expect_integer(3);
}

#[tokio::test]
async fn lpos_rank_zero_errors() {
    let state = test_state();
    call(&state, &[b"RPUSH", b"l", b"a"]).await;
    call(&state, &[b"LPOS", b"l", b"a", b"RANK", b"0"]).await.expect_error_prefix("ERR");
}

#[tokio::test]
async fn lpos_with_count_zero_returns_all_matches() {
    let state = test_state();
    call(&state, &[b"RPUSH", b"l", b"a", b"b", b"a", b"c", b"a"]).await;
    let reply = call(&state, &[b"LPOS", b"l", b"a", b"COUNT", b"0"]).await;
    // Array of integers :0 :2 :4.
    assert_eq!(reply.raw, b"*3\r\n:0\r\n:2\r\n:4\r\n");
}

#[tokio::test]
async fn lpos_with_count_n_limits_output() {
    let state = test_state();
    call(&state, &[b"RPUSH", b"l", b"a", b"b", b"a", b"c", b"a"]).await;
    let reply = call(&state, &[b"LPOS", b"l", b"a", b"COUNT", b"2"]).await;
    assert_eq!(reply.raw, b"*2\r\n:0\r\n:2\r\n");
}

#[tokio::test]
async fn lpos_count_with_no_matches_returns_empty_array() {
    let state = test_state();
    call(&state, &[b"RPUSH", b"l", b"a", b"b"]).await;
    let reply = call(&state, &[b"LPOS", b"l", b"z", b"COUNT", b"0"]).await;
    assert_eq!(reply.raw, b"*0\r\n");
}

#[tokio::test]
async fn lpos_count_with_negative_rank_returns_backward_order() {
    let state = test_state();
    call(&state, &[b"RPUSH", b"l", b"a", b"b", b"a", b"c", b"a"]).await;
    let reply = call(&state, &[b"LPOS", b"l", b"a", b"RANK", b"-1", b"COUNT", b"0"]).await;
    // Backward walk yields indices 4, 2, 0.
    assert_eq!(reply.raw, b"*3\r\n:4\r\n:2\r\n:0\r\n");
}

#[tokio::test]
async fn lpos_maxlen_bounds_the_scan() {
    let state = test_state();
    call(&state, &[b"RPUSH", b"l", b"a", b"b", b"c", b"d", b"b"]).await;
    // MAXLEN 2 only looks at indices 0 and 1 — finds b at 1.
    call(&state, &[b"LPOS", b"l", b"b", b"MAXLEN", b"2"]).await.expect_integer(1);
    // MAXLEN 1 only looks at index 0 — no match.
    call(&state, &[b"LPOS", b"l", b"b", b"MAXLEN", b"1"]).await.expect_nil();
}

#[tokio::test]
async fn lpos_on_wrong_type_errors() {
    let state = test_state();
    call(&state, &[b"SET", b"k", b"v"]).await;
    call(&state, &[b"LPOS", b"k", b"v"]).await.expect_error_prefix("ERR");
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

// ---------------------------------------------------------------------------
// SINTERSTORE / SUNIONSTORE / SDIFFSTORE — write set aggregates into a dest.
// Empty results delete the destination; overwriting is unconditional (no
// WRONGTYPE check on the destination), matching Redis.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sinterstore_stores_common_members() {
    let state = test_state();
    call(&state, &[b"SADD", b"ss-a", b"x", b"y", b"z"]).await;
    call(&state, &[b"SADD", b"ss-b", b"y", b"z", b"w"]).await;
    call(&state, &[b"SINTERSTORE", b"ss-dst", b"ss-a", b"ss-b"]).await.expect_integer(2);
    call(&state, &[b"TYPE", b"ss-dst"]).await.expect_simple("set");
    let mut members = bulks_to_strs(&call(&state, &[b"SMEMBERS", b"ss-dst"]).await.into_bulk_array());
    members.sort();
    assert_eq!(members, vec!["y".to_string(), "z".into()]);
}

#[tokio::test]
async fn sunionstore_stores_combined_members() {
    let state = test_state();
    call(&state, &[b"SADD", b"su-a", b"x", b"y"]).await;
    call(&state, &[b"SADD", b"su-b", b"y", b"z"]).await;
    call(&state, &[b"SUNIONSTORE", b"su-dst", b"su-a", b"su-b"]).await.expect_integer(3);
    let mut members = bulks_to_strs(&call(&state, &[b"SMEMBERS", b"su-dst"]).await.into_bulk_array());
    members.sort();
    assert_eq!(members, vec!["x".to_string(), "y".into(), "z".into()]);
}

#[tokio::test]
async fn sdiffstore_stores_first_minus_rest() {
    let state = test_state();
    call(&state, &[b"SADD", b"sd-a", b"x", b"y", b"z"]).await;
    call(&state, &[b"SADD", b"sd-b", b"y"]).await;
    call(&state, &[b"SADD", b"sd-c", b"z"]).await;
    call(&state, &[b"SDIFFSTORE", b"sd-dst", b"sd-a", b"sd-b", b"sd-c"]).await.expect_integer(1);
    let members = bulks_to_strs(&call(&state, &[b"SMEMBERS", b"sd-dst"]).await.into_bulk_array());
    assert_eq!(members, vec!["x".to_string()]);
}

#[tokio::test]
async fn sinterstore_empty_result_deletes_destination() {
    let state = test_state();
    call(&state, &[b"SADD", b"se1-a", b"x"]).await;
    call(&state, &[b"SADD", b"se1-b", b"y"]).await;
    // Pre-populate destination so we can see it get cleared.
    call(&state, &[b"SADD", b"se1-dst", b"old"]).await;
    call(&state, &[b"SINTERSTORE", b"se1-dst", b"se1-a", b"se1-b"]).await.expect_integer(0);
    call(&state, &[b"EXISTS", b"se1-dst"]).await.expect_integer(0);
}

#[tokio::test]
async fn sinterstore_overwrites_destination_of_any_type() {
    let state = test_state();
    call(&state, &[b"SADD", b"so-a", b"x", b"y"]).await;
    call(&state, &[b"SADD", b"so-b", b"y"]).await;
    // Destination is a string; should be overwritten without WRONGTYPE.
    call(&state, &[b"SET", b"so-dst", b"ignored"]).await.expect_simple("OK");
    call(&state, &[b"SINTERSTORE", b"so-dst", b"so-a", b"so-b"]).await.expect_integer(1);
    call(&state, &[b"TYPE", b"so-dst"]).await.expect_simple("set");
}

#[tokio::test]
async fn sinterstore_rejects_non_set_input() {
    let state = test_state();
    call(&state, &[b"SET", b"sw-str", b"x"]).await;
    call(&state, &[b"SADD", b"sw-set", b"y"]).await;
    call(&state, &[b"SINTERSTORE", b"sw-dst", b"sw-str", b"sw-set"]).await.expect_error_prefix("ERR");
}

#[tokio::test]
async fn sinterstore_arity_errors() {
    let state = test_state();
    // Missing source key.
    call(&state, &[b"SINTERSTORE", b"dst"]).await.expect_error_prefix("ERR");
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

// ---------------------------------------------------------------------------
// ECHO / TIME / DBSIZE / FLUSHDB / FLUSHALL / RANDOMKEY / UNLINK / COPY
// ---------------------------------------------------------------------------

#[tokio::test]
async fn echo_returns_input_bulk() {
    let state = test_state();
    call(&state, &[b"ECHO", b"hello world"]).await.expect_bulk(b"hello world");
}

#[tokio::test]
async fn echo_passes_binary_payload_unchanged() {
    let state = test_state();
    call(&state, &[b"ECHO", b"\x00\xff\x01\x02"]).await.expect_bulk(b"\x00\xff\x01\x02");
}

#[tokio::test]
async fn echo_wrong_arity_errors() {
    let state = test_state();
    call(&state, &[b"ECHO"]).await.expect_error_prefix("ERR");
    call(&state, &[b"ECHO", b"a", b"b"]).await.expect_error_prefix("ERR");
}

#[tokio::test]
async fn time_returns_seconds_and_microseconds_pair() {
    let state = test_state();
    let parts = call(&state, &[b"TIME"]).await.into_bulk_array();
    assert_eq!(parts.len(), 2, "expected [secs, micros]");
    let secs: i64 = std::str::from_utf8(&parts[0]).expect("utf8 secs").parse().expect("parse secs");
    let micros: i64 = std::str::from_utf8(&parts[1]).expect("utf8 micros").parse().expect("parse micros");
    // Sanity: rustyant only ever ships forward in time, so the seconds field
    // should be at least past the 2026 epoch boundary (1_767_225_600).
    assert!(secs > 1_767_225_600, "secs implausibly small: {secs}");
    assert!((0..1_000_000).contains(&micros), "micros out of band: {micros}");
}

#[tokio::test]
async fn time_rejects_extra_args() {
    let state = test_state();
    call(&state, &[b"TIME", b"now"]).await.expect_error_prefix("ERR");
}

#[tokio::test]
async fn dbsize_empty_returns_zero() {
    let state = test_state();
    call(&state, &[b"DBSIZE"]).await.expect_integer(0);
}

#[tokio::test]
async fn dbsize_counts_every_value_kind() {
    let state = test_state();
    call(&state, &[b"SET", b"s", b"v"]).await;
    call(&state, &[b"HSET", b"h", b"f", b"v"]).await;
    call(&state, &[b"LPUSH", b"l", b"v"]).await;
    call(&state, &[b"SADD", b"set", b"v"]).await;
    call(&state, &[b"ZADD", b"z", b"1", b"v"]).await;
    call(&state, &[b"DBSIZE"]).await.expect_integer(5);
}

#[tokio::test]
async fn dbsize_drops_after_del() {
    let state = test_state();
    call(&state, &[b"SET", b"a", b"1"]).await;
    call(&state, &[b"SET", b"b", b"2"]).await;
    call(&state, &[b"DEL", b"a"]).await;
    call(&state, &[b"DBSIZE"]).await.expect_integer(1);
}

#[tokio::test]
async fn flushdb_clears_every_key() {
    let state = test_state();
    call(&state, &[b"SET", b"a", b"1"]).await;
    call(&state, &[b"HSET", b"h", b"f", b"v"]).await;
    call(&state, &[b"FLUSHDB"]).await.expect_simple("OK");
    call(&state, &[b"DBSIZE"]).await.expect_integer(0);
    call(&state, &[b"GET", b"a"]).await.expect_nil();
}

#[tokio::test]
async fn flushall_is_alias_of_flushdb() {
    let state = test_state();
    call(&state, &[b"SET", b"a", b"1"]).await;
    call(&state, &[b"FLUSHALL"]).await.expect_simple("OK");
    call(&state, &[b"DBSIZE"]).await.expect_integer(0);
}

#[tokio::test]
async fn flushdb_accepts_async_modifier() {
    let state = test_state();
    call(&state, &[b"SET", b"a", b"1"]).await;
    // Redis 4+ added the ASYNC / SYNC modifier; rustyant ignores the choice
    // (always synchronous) but must not error on it.
    call(&state, &[b"FLUSHDB", b"ASYNC"]).await.expect_simple("OK");
    call(&state, &[b"DBSIZE"]).await.expect_integer(0);
}

#[tokio::test]
async fn flushdb_rejects_unknown_modifier() {
    let state = test_state();
    call(&state, &[b"FLUSHDB", b"BOGUS"]).await.expect_error_prefix("ERR");
}

#[tokio::test]
async fn randomkey_empty_returns_nil() {
    let state = test_state();
    call(&state, &[b"RANDOMKEY"]).await.expect_nil();
}

#[tokio::test]
async fn randomkey_returns_one_of_existing_keys() {
    let state = test_state();
    call(&state, &[b"SET", b"alpha", b"1"]).await;
    call(&state, &[b"SET", b"beta", b"2"]).await;
    call(&state, &[b"SET", b"gamma", b"3"]).await;
    let reply = call(&state, &[b"RANDOMKEY"]).await;
    let raw = String::from_utf8_lossy(&reply.raw).into_owned();
    assert!(
        raw.contains("alpha") || raw.contains("beta") || raw.contains("gamma"),
        "RANDOMKEY did not return a known key: {raw:?}"
    );
}

#[tokio::test]
async fn unlink_behaves_like_del() {
    let state = test_state();
    call(&state, &[b"SET", b"a", b"1"]).await;
    call(&state, &[b"SET", b"b", b"2"]).await;
    call(&state, &[b"UNLINK", b"a", b"b", b"missing"]).await.expect_integer(2);
    call(&state, &[b"EXISTS", b"a", b"b"]).await.expect_integer(0);
}

#[tokio::test]
async fn copy_basic_creates_destination() {
    let state = test_state();
    call(&state, &[b"SET", b"src", b"v"]).await;
    call(&state, &[b"COPY", b"src", b"dst"]).await.expect_integer(1);
    call(&state, &[b"GET", b"dst"]).await.expect_bulk(b"v");
    // Source still present — COPY is not a move.
    call(&state, &[b"GET", b"src"]).await.expect_bulk(b"v");
}

#[tokio::test]
async fn copy_missing_source_returns_zero() {
    let state = test_state();
    call(&state, &[b"COPY", b"missing", b"dst"]).await.expect_integer(0);
    call(&state, &[b"EXISTS", b"dst"]).await.expect_integer(0);
}

#[tokio::test]
async fn copy_dest_exists_without_replace_refuses() {
    let state = test_state();
    call(&state, &[b"SET", b"src", b"new"]).await;
    call(&state, &[b"SET", b"dst", b"old"]).await;
    call(&state, &[b"COPY", b"src", b"dst"]).await.expect_integer(0);
    call(&state, &[b"GET", b"dst"]).await.expect_bulk(b"old");
}

#[tokio::test]
async fn copy_with_replace_overwrites_destination() {
    let state = test_state();
    call(&state, &[b"SET", b"src", b"new"]).await;
    call(&state, &[b"SET", b"dst", b"old"]).await;
    call(&state, &[b"COPY", b"src", b"dst", b"REPLACE"]).await.expect_integer(1);
    call(&state, &[b"GET", b"dst"]).await.expect_bulk(b"new");
}

#[tokio::test]
async fn copy_self_returns_zero() {
    let state = test_state();
    call(&state, &[b"SET", b"k", b"v"]).await;
    call(&state, &[b"COPY", b"k", b"k"]).await.expect_integer(0);
    call(&state, &[b"COPY", b"k", b"k", b"REPLACE"]).await.expect_integer(0);
}

#[tokio::test]
async fn copy_preserves_ttl_on_destination() {
    let state = test_state();
    call(&state, &[b"SET", b"src", b"v", b"EX", b"60"]).await;
    call(&state, &[b"COPY", b"src", b"dst"]).await.expect_integer(1);
    let reply = call(&state, &[b"TTL", b"dst"]).await;
    let raw = String::from_utf8_lossy(&reply.raw).into_owned();
    let ttl: i64 = raw.trim_start_matches(':').trim_end().parse().expect("ttl");
    assert!((50..=60).contains(&ttl), "TTL not preserved on copy: {ttl}");
}

#[tokio::test]
async fn copy_preserves_value_kind_for_collections() {
    let state = test_state();
    call(&state, &[b"HSET", b"src", b"f", b"v"]).await;
    call(&state, &[b"COPY", b"src", b"dst"]).await.expect_integer(1);
    call(&state, &[b"HGET", b"dst", b"f"]).await.expect_bulk(b"v");
    call(&state, &[b"HLEN", b"dst"]).await.expect_integer(1);
}

#[tokio::test]
async fn copy_db_zero_is_accepted() {
    let state = test_state();
    call(&state, &[b"SET", b"src", b"v"]).await;
    call(&state, &[b"COPY", b"src", b"dst", b"DB", b"0"]).await.expect_integer(1);
}

#[tokio::test]
async fn copy_other_db_errors() {
    let state = test_state();
    call(&state, &[b"SET", b"src", b"v"]).await;
    call(&state, &[b"COPY", b"src", b"dst", b"DB", b"1"]).await.expect_error_prefix("ERR");
}

#[tokio::test]
async fn copy_unknown_option_errors() {
    let state = test_state();
    call(&state, &[b"SET", b"src", b"v"]).await;
    call(&state, &[b"COPY", b"src", b"dst", b"NUKE"]).await.expect_error_prefix("ERR");
}

// ---------------------------------------------------------------------------
// Bit ops on Strings: GETBIT / SETBIT / BITCOUNT / BITPOS / BITOP
// ---------------------------------------------------------------------------

#[tokio::test]
async fn getbit_on_missing_key_returns_zero() {
    let state = test_state();
    call(&state, &[b"GETBIT", b"k", b"0"]).await.expect_integer(0);
    call(&state, &[b"GETBIT", b"k", b"100"]).await.expect_integer(0);
}

#[tokio::test]
async fn getbit_reads_msb_first_within_byte() {
    let state = test_state();
    // 0x80 = 1000_0000 — bit 0 is set, rest cleared.
    call(&state, &[b"SET", b"k", b"\x80"]).await;
    call(&state, &[b"GETBIT", b"k", b"0"]).await.expect_integer(1);
    call(&state, &[b"GETBIT", b"k", b"1"]).await.expect_integer(0);
    call(&state, &[b"GETBIT", b"k", b"7"]).await.expect_integer(0);
    // Past end → 0, no error.
    call(&state, &[b"GETBIT", b"k", b"100"]).await.expect_integer(0);
}

#[tokio::test]
async fn getbit_on_wrong_type_errors() {
    let state = test_state();
    call(&state, &[b"LPUSH", b"l", b"x"]).await;
    call(&state, &[b"GETBIT", b"l", b"0"]).await.expect_error_prefix("ERR");
}

#[tokio::test]
async fn setbit_creates_and_extends_string() {
    let state = test_state();
    // First SETBIT on a missing key creates the string and zero-pads.
    call(&state, &[b"SETBIT", b"k", b"7", b"1"]).await.expect_integer(0);
    call(&state, &[b"GET", b"k"]).await.expect_bulk(b"\x01");
    call(&state, &[b"STRLEN", b"k"]).await.expect_integer(1);
    // Setting bit 16 forces a 3-byte string with the new bit at MSB of byte 2.
    call(&state, &[b"SETBIT", b"k", b"16", b"1"]).await.expect_integer(0);
    call(&state, &[b"GET", b"k"]).await.expect_bulk(b"\x01\x00\x80");
}

#[tokio::test]
async fn setbit_returns_previous_bit_value() {
    let state = test_state();
    call(&state, &[b"SETBIT", b"k", b"3", b"1"]).await.expect_integer(0);
    // Setting the same bit again should report the previous value (1).
    call(&state, &[b"SETBIT", b"k", b"3", b"1"]).await.expect_integer(1);
    call(&state, &[b"SETBIT", b"k", b"3", b"0"]).await.expect_integer(1);
    call(&state, &[b"SETBIT", b"k", b"3", b"0"]).await.expect_integer(0);
}

#[tokio::test]
async fn setbit_rejects_invalid_bit_value() {
    let state = test_state();
    call(&state, &[b"SETBIT", b"k", b"0", b"2"]).await.expect_error_prefix("ERR");
    call(&state, &[b"SETBIT", b"k", b"0", b"-1"]).await.expect_error_prefix("ERR");
}

#[tokio::test]
async fn setbit_negative_offset_errors() {
    let state = test_state();
    call(&state, &[b"SETBIT", b"k", b"-1", b"1"]).await.expect_error_prefix("ERR");
}

#[tokio::test]
async fn setbit_on_wrong_type_errors() {
    let state = test_state();
    call(&state, &[b"LPUSH", b"l", b"x"]).await;
    call(&state, &[b"SETBIT", b"l", b"0", b"1"]).await.expect_error_prefix("ERR");
}

#[tokio::test]
async fn bitcount_whole_key_counts_set_bits() {
    let state = test_state();
    call(&state, &[b"SET", b"k", b"foobar"]).await;
    // Real Redis reports 26 set bits in "foobar".
    call(&state, &[b"BITCOUNT", b"k"]).await.expect_integer(26);
}

#[tokio::test]
async fn bitcount_byte_range_inclusive() {
    let state = test_state();
    call(&state, &[b"SET", b"k", b"foobar"]).await;
    // Bytes 0..=0 = "f" (0x66 = 0110_0110) = 4 set bits.
    call(&state, &[b"BITCOUNT", b"k", b"0", b"0"]).await.expect_integer(4);
    // Negative indices: last two bytes = "ar" — 3 + 4 = 7 set bits.
    call(&state, &[b"BITCOUNT", b"k", b"-2", b"-1"]).await.expect_integer(7);
}

#[tokio::test]
async fn bitcount_bit_range_unit() {
    let state = test_state();
    call(&state, &[b"SET", b"k", b"\xff\x00"]).await;
    // First 8 bits are all 1.
    call(&state, &[b"BITCOUNT", b"k", b"0", b"7", b"BIT"]).await.expect_integer(8);
    // Bits 8..=15 are all 0.
    call(&state, &[b"BITCOUNT", b"k", b"8", b"15", b"BIT"]).await.expect_integer(0);
    // Bits 4..=11 straddle the boundary: 4 ones then 4 zeros.
    call(&state, &[b"BITCOUNT", b"k", b"4", b"11", b"BIT"]).await.expect_integer(4);
}

#[tokio::test]
async fn bitcount_missing_key_returns_zero() {
    let state = test_state();
    call(&state, &[b"BITCOUNT", b"missing"]).await.expect_integer(0);
    call(&state, &[b"BITCOUNT", b"missing", b"0", b"10"]).await.expect_integer(0);
}

#[tokio::test]
async fn bitcount_empty_range_returns_zero() {
    let state = test_state();
    call(&state, &[b"SET", b"k", b"foo"]).await;
    call(&state, &[b"BITCOUNT", b"k", b"5", b"10"]).await.expect_integer(0);
}

#[tokio::test]
async fn bitcount_arity_errors() {
    let state = test_state();
    // Missing end argument when start is given.
    call(&state, &[b"BITCOUNT", b"k", b"0"]).await.expect_error_prefix("ERR");
}

#[tokio::test]
async fn bitpos_finds_first_set_bit() {
    let state = test_state();
    // 0x00 0x0f 0xff — first set bit is at index 12.
    call(&state, &[b"SET", b"k", b"\x00\x0f\xff"]).await;
    call(&state, &[b"BITPOS", b"k", b"1"]).await.expect_integer(12);
}

#[tokio::test]
async fn bitpos_finds_first_clear_bit() {
    let state = test_state();
    // 0xff 0xf0 — first clear bit is at index 12.
    call(&state, &[b"SET", b"k", b"\xff\xf0"]).await;
    call(&state, &[b"BITPOS", b"k", b"0"]).await.expect_integer(12);
}

#[tokio::test]
async fn bitpos_zero_on_all_ones_with_no_end_returns_past_end() {
    let state = test_state();
    // Three bytes all 1s — Redis returns the position one past the last bit
    // (24) when no end is pinned, treating the trailing string as zero-padded.
    call(&state, &[b"SET", b"k", b"\xff\xff\xff"]).await;
    call(&state, &[b"BITPOS", b"k", b"0"]).await.expect_integer(24);
}

#[tokio::test]
async fn bitpos_zero_with_explicit_end_returns_minus_one_when_not_found() {
    let state = test_state();
    call(&state, &[b"SET", b"k", b"\xff\xff\xff"]).await;
    // With an explicit end the trailing-zeros fiction does NOT apply.
    call(&state, &[b"BITPOS", b"k", b"0", b"0", b"-1"]).await.expect_integer(-1);
}

#[tokio::test]
async fn bitpos_one_on_all_zeros_returns_minus_one() {
    let state = test_state();
    call(&state, &[b"SET", b"k", b"\x00\x00"]).await;
    call(&state, &[b"BITPOS", b"k", b"1"]).await.expect_integer(-1);
}

#[tokio::test]
async fn bitpos_with_byte_range() {
    let state = test_state();
    // First set bit is in byte 0; restricting to byte 1 onward should skip it.
    call(&state, &[b"SET", b"k", b"\x80\x40"]).await;
    call(&state, &[b"BITPOS", b"k", b"1", b"1"]).await.expect_integer(9);
}

#[tokio::test]
async fn bitpos_with_bit_range_unit() {
    let state = test_state();
    call(&state, &[b"SET", b"k", b"\x80\x40"]).await;
    // Search bit-range 5..=15 — first 1-bit is at index 9 (MSB of byte 1 is
    // clear, the set bit is the second-MSB).
    call(&state, &[b"BITPOS", b"k", b"1", b"5", b"15", b"BIT"]).await.expect_integer(9);
}

#[tokio::test]
async fn bitpos_missing_key() {
    let state = test_state();
    // Missing / empty key: searching for 0 starts at position 0; for 1, -1.
    call(&state, &[b"BITPOS", b"missing", b"0"]).await.expect_integer(0);
    call(&state, &[b"BITPOS", b"missing", b"1"]).await.expect_integer(-1);
}

#[tokio::test]
async fn bitop_and_pads_shorter_with_zeros() {
    let state = test_state();
    call(&state, &[b"SET", b"a", b"\xff\xff"]).await;
    call(&state, &[b"SET", b"b", b"\x0f"]).await;
    // AND result is "\x0f\x00" (length 2, padded shorter with zeros).
    call(&state, &[b"BITOP", b"AND", b"dst", b"a", b"b"]).await.expect_integer(2);
    call(&state, &[b"GET", b"dst"]).await.expect_bulk(b"\x0f\x00");
}

#[tokio::test]
async fn bitop_or_combines_sources() {
    let state = test_state();
    call(&state, &[b"SET", b"a", b"\xf0"]).await;
    call(&state, &[b"SET", b"b", b"\x0f"]).await;
    call(&state, &[b"BITOP", b"OR", b"dst", b"a", b"b"]).await.expect_integer(1);
    call(&state, &[b"GET", b"dst"]).await.expect_bulk(b"\xff");
}

#[tokio::test]
async fn bitop_xor_three_sources() {
    let state = test_state();
    call(&state, &[b"SET", b"a", b"\xff"]).await;
    call(&state, &[b"SET", b"b", b"\x0f"]).await;
    call(&state, &[b"SET", b"c", b"\xf0"]).await;
    // 0xff ^ 0x0f ^ 0xf0 = 0x00
    call(&state, &[b"BITOP", b"XOR", b"dst", b"a", b"b", b"c"]).await.expect_integer(1);
    call(&state, &[b"GET", b"dst"]).await.expect_bulk(b"\x00");
}

#[tokio::test]
async fn bitop_not_inverts_single_source() {
    let state = test_state();
    call(&state, &[b"SET", b"a", b"\x0f\x55"]).await;
    call(&state, &[b"BITOP", b"NOT", b"dst", b"a"]).await.expect_integer(2);
    call(&state, &[b"GET", b"dst"]).await.expect_bulk(b"\xf0\xaa");
}

#[tokio::test]
async fn bitop_not_rejects_multiple_sources() {
    let state = test_state();
    call(&state, &[b"SET", b"a", b"\x0f"]).await;
    call(&state, &[b"SET", b"b", b"\xf0"]).await;
    call(&state, &[b"BITOP", b"NOT", b"dst", b"a", b"b"]).await.expect_error_prefix("ERR");
}

#[tokio::test]
async fn bitop_unknown_operation_errors() {
    let state = test_state();
    call(&state, &[b"SET", b"a", b"v"]).await;
    call(&state, &[b"BITOP", b"NUKE", b"dst", b"a"]).await.expect_error_prefix("ERR");
}

#[tokio::test]
async fn bitop_missing_sources_yield_empty_dest_and_delete() {
    let state = test_state();
    // Pre-existing dest should be removed when the result collapses to empty.
    call(&state, &[b"SET", b"dst", b"old"]).await;
    call(&state, &[b"BITOP", b"AND", b"dst", b"missing1", b"missing2"]).await.expect_integer(0);
    call(&state, &[b"EXISTS", b"dst"]).await.expect_integer(0);
}

#[tokio::test]
async fn bitop_on_wrong_type_errors() {
    let state = test_state();
    call(&state, &[b"LPUSH", b"l", b"x"]).await;
    call(&state, &[b"BITOP", b"OR", b"dst", b"l"]).await.expect_error_prefix("ERR");
}

// Use RespReply publicly to check the crate re-export surface compiles.
#[test]
fn reply_encode_simple_works_from_tests() {
    let r = RespReply::ok();
    let enc = r.encode().expect("encode");
    assert_eq!(&enc[..], b"+OK\r\n");
}

// ---------------------------------------------------------------------------
// EXPIRETIME / PEXPIRETIME
// ---------------------------------------------------------------------------

#[tokio::test]
async fn expiretime_missing_key_returns_minus_two() {
    let state = test_state();
    call(&state, &[b"EXPIRETIME", b"missing"]).await.expect_integer(-2);
}

#[tokio::test]
async fn expiretime_no_expire_returns_minus_one() {
    let state = test_state();
    call(&state, &[b"SET", b"k", b"v"]).await;
    call(&state, &[b"EXPIRETIME", b"k"]).await.expect_integer(-1);
}

#[tokio::test]
async fn expiretime_returns_absolute_unix_seconds() {
    let state = test_state();
    let target_sec = rustyant::storage::now_ms() / 1000 + 60;
    call(&state, &[b"SET", b"k", b"v"]).await;
    call(&state, &[b"EXPIREAT", b"k", target_sec.to_string().as_bytes()]).await;
    let reply = call(&state, &[b"EXPIRETIME", b"k"]).await;
    let got: i64 = String::from_utf8_lossy(&reply.raw).trim_start_matches(':').trim_end().parse().expect("int");
    assert_eq!(got, target_sec, "EXPIRETIME should echo the absolute epoch-seconds we set");
}

#[tokio::test]
async fn pexpiretime_returns_absolute_unix_milliseconds() {
    let state = test_state();
    let target_ms = rustyant::storage::now_ms() + 60_000;
    call(&state, &[b"SET", b"k", b"v"]).await;
    call(&state, &[b"PEXPIREAT", b"k", target_ms.to_string().as_bytes()]).await;
    let reply = call(&state, &[b"PEXPIRETIME", b"k"]).await;
    let got: i64 = String::from_utf8_lossy(&reply.raw).trim_start_matches(':').trim_end().parse().expect("int");
    assert_eq!(got, target_ms);
}

#[tokio::test]
async fn pexpiretime_missing_key_returns_minus_two() {
    let state = test_state();
    call(&state, &[b"PEXPIRETIME", b"missing"]).await.expect_integer(-2);
}

// ---------------------------------------------------------------------------
// GETEX
// ---------------------------------------------------------------------------

#[tokio::test]
async fn getex_bare_returns_value_leaves_ttl() {
    let state = test_state();
    call(&state, &[b"SET", b"k", b"v", b"EX", b"60"]).await;
    call(&state, &[b"GETEX", b"k"]).await.expect_bulk(b"v");
    // TTL untouched.
    let reply = call(&state, &[b"TTL", b"k"]).await;
    let n: i64 = String::from_utf8_lossy(&reply.raw).trim_start_matches(':').trim_end().parse().expect("int");
    assert!((1..=60).contains(&n), "TTL changed: {n}");
}

#[tokio::test]
async fn getex_missing_key_returns_nil() {
    let state = test_state();
    call(&state, &[b"GETEX", b"missing"]).await.expect_nil();
    // With an option too.
    call(&state, &[b"GETEX", b"missing", b"EX", b"30"]).await.expect_nil();
}

#[tokio::test]
async fn getex_ex_sets_ttl() {
    let state = test_state();
    call(&state, &[b"SET", b"k", b"v"]).await;
    call(&state, &[b"GETEX", b"k", b"EX", b"120"]).await.expect_bulk(b"v");
    let reply = call(&state, &[b"TTL", b"k"]).await;
    let n: i64 = String::from_utf8_lossy(&reply.raw).trim_start_matches(':').trim_end().parse().expect("int");
    assert!((1..=120).contains(&n), "unexpected TTL: {n}");
}

#[tokio::test]
async fn getex_px_sets_ms_ttl() {
    let state = test_state();
    call(&state, &[b"SET", b"k", b"v"]).await;
    call(&state, &[b"GETEX", b"k", b"PX", b"90000"]).await.expect_bulk(b"v");
    let reply = call(&state, &[b"PTTL", b"k"]).await;
    let n: i64 = String::from_utf8_lossy(&reply.raw).trim_start_matches(':').trim_end().parse().expect("int");
    assert!((1..=90_000).contains(&n), "unexpected PTTL: {n}");
}

#[tokio::test]
async fn getex_exat_sets_absolute_expiry() {
    let state = test_state();
    let target_sec = rustyant::storage::now_ms() / 1000 + 120;
    call(&state, &[b"SET", b"k", b"v"]).await;
    call(&state, &[b"GETEX", b"k", b"EXAT", target_sec.to_string().as_bytes()]).await.expect_bulk(b"v");
    call(&state, &[b"EXPIRETIME", b"k"]).await.expect_integer(target_sec);
}

#[tokio::test]
async fn getex_pxat_sets_absolute_ms_expiry() {
    let state = test_state();
    let target_ms = rustyant::storage::now_ms() + 120_000;
    call(&state, &[b"SET", b"k", b"v"]).await;
    call(&state, &[b"GETEX", b"k", b"PXAT", target_ms.to_string().as_bytes()]).await.expect_bulk(b"v");
    call(&state, &[b"PEXPIRETIME", b"k"]).await.expect_integer(target_ms);
}

#[tokio::test]
async fn getex_persist_clears_ttl() {
    let state = test_state();
    call(&state, &[b"SET", b"k", b"v", b"EX", b"60"]).await;
    call(&state, &[b"GETEX", b"k", b"PERSIST"]).await.expect_bulk(b"v");
    call(&state, &[b"TTL", b"k"]).await.expect_integer(-1);
}

#[tokio::test]
async fn getex_wrong_type_errors() {
    let state = test_state();
    call(&state, &[b"LPUSH", b"l", b"x"]).await;
    call(&state, &[b"GETEX", b"l"]).await.expect_error_prefix("ERR");
    call(&state, &[b"GETEX", b"l", b"EX", b"10"]).await.expect_error_prefix("ERR");
}

#[tokio::test]
async fn getex_rejects_multiple_options() {
    let state = test_state();
    call(&state, &[b"SET", b"k", b"v"]).await;
    call(&state, &[b"GETEX", b"k", b"EX", b"10", b"PX", b"500"]).await.expect_error_prefix("ERR");
}

#[tokio::test]
async fn getex_unknown_option_errors() {
    let state = test_state();
    call(&state, &[b"SET", b"k", b"v"]).await;
    call(&state, &[b"GETEX", b"k", b"FOO", b"1"]).await.expect_error_prefix("ERR");
}

// ---------------------------------------------------------------------------
// INFO
// ---------------------------------------------------------------------------

fn info_as_string(reply: &DecodedReply) -> String {
    // Strip the bulk-string header `$N\r\n` and trailing `\r\n`.
    let raw = String::from_utf8_lossy(&reply.raw);
    let body = raw.split_once("\r\n").map_or(&raw[..], |(_, rest)| rest);
    body.trim_end_matches("\r\n").to_string()
}

#[tokio::test]
async fn info_returns_all_sections_by_default() {
    let state = test_state();
    let reply = call(&state, &[b"INFO"]).await;
    let body = info_as_string(&reply);
    for header in ["# Server", "# Clients", "# Stats", "# Keyspace"] {
        assert!(body.contains(header), "missing section {header:?} in:\n{body}");
    }
}

#[tokio::test]
async fn info_server_reports_uptime_and_versions() {
    let state = test_state();
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
    let body = info_as_string(&call(&state, &[b"INFO", b"server"]).await);
    assert!(body.contains("# Server"));
    assert!(body.contains("redis_version:"), "missing redis_version");
    assert!(body.contains("rustyant_version:"), "missing rustyant_version");
    let uptime = body
        .lines()
        .find_map(|l| l.strip_prefix("uptime_in_seconds:"))
        .and_then(|s| s.parse::<i64>().ok())
        .expect("uptime field");
    assert!(uptime >= 1, "uptime should be at least 1s, got {uptime}");
}

#[tokio::test]
async fn info_keyspace_reflects_stored_keys() {
    let state = test_state();
    // Empty keyspace → no db0 line.
    let empty = info_as_string(&call(&state, &[b"INFO", b"keyspace"]).await);
    assert!(empty.contains("# Keyspace"));
    assert!(!empty.contains("db0:"));
    // After a SET, db0 should report at least one key.
    call(&state, &[b"SET", b"a", b"1"]).await;
    call(&state, &[b"SET", b"b", b"2"]).await;
    let populated = info_as_string(&call(&state, &[b"INFO", b"keyspace"]).await);
    assert!(populated.contains("db0:keys="), "expected db0 line:\n{populated}");
}

#[tokio::test]
async fn info_single_section_filters_output() {
    let state = test_state();
    let body = info_as_string(&call(&state, &[b"INFO", b"clients"]).await);
    assert!(body.contains("# Clients"));
    assert!(!body.contains("# Server"), "clients section should not leak Server:\n{body}");
}

#[tokio::test]
async fn info_everything_is_full_output() {
    let state = test_state();
    let body = info_as_string(&call(&state, &[b"INFO", b"everything"]).await);
    for header in ["# Server", "# Clients", "# Stats", "# Keyspace"] {
        assert!(body.contains(header));
    }
}

#[tokio::test]
async fn info_rejects_extra_args() {
    let state = test_state();
    call(&state, &[b"INFO", b"server", b"clients"]).await.expect_error_prefix("ERR");
}

// ---------------------------------------------------------------------------
// COMMAND
// ---------------------------------------------------------------------------

#[tokio::test]
async fn command_count_returns_registered_command_count() {
    let state = test_state();
    let reply = call(&state, &[b"COMMAND", b"COUNT"]).await;
    let n: i64 = String::from_utf8_lossy(&reply.raw).trim_start_matches(':').trim_end().parse().expect("int");
    assert!(n > 100, "COMMAND COUNT unexpectedly small: {n}");
}

#[tokio::test]
async fn command_list_returns_all_names() {
    let state = test_state();
    let reply = call(&state, &[b"COMMAND", b"LIST"]).await;
    let names: Vec<String> =
        reply.into_bulk_array().into_iter().map(|b| String::from_utf8_lossy(&b).into_owned()).collect();
    for expected in ["get", "set", "info", "command", "getex", "expiretime"] {
        assert!(names.iter().any(|n| n == expected), "COMMAND LIST missing {expected}; got {names:?}");
    }
}

#[tokio::test]
async fn command_info_get_returns_expected_metadata() {
    let state = test_state();
    // Parse the full reply as raw bytes and eyeball the frame since
    // `into_bulk_array` flattens nested arrays. `COMMAND INFO GET` returns
    // an outer array of one element; that element is a 6-tuple starting
    // with the bulk string "get" and the integer 2 (exact arity).
    let reply = call(&state, &[b"COMMAND", b"INFO", b"GET"]).await;
    let raw = reply.raw;
    let s = String::from_utf8_lossy(&raw);
    // Outer array length 1 → `*1\r\n`; inner array length 6 → `*6\r\n`.
    assert!(s.starts_with("*1\r\n*6\r\n"), "unexpected frame header:\n{s}");
    assert!(s.contains("$3\r\nget\r\n"), "missing bulk 'get' in reply:\n{s}");
    assert!(s.contains(":2\r\n"), "missing integer 2 (arity) in reply:\n{s}");
}

#[tokio::test]
async fn command_info_unknown_command_returns_nil_slot() {
    let state = test_state();
    let reply = call(&state, &[b"COMMAND", b"INFO", b"NOPE"]).await;
    let s = String::from_utf8_lossy(&reply.raw);
    // Redis: each unknown-name slot is a bulk-string nil. Frame is `*1\r\n$-1\r\n`.
    assert!(s.starts_with("*1\r\n"), "outer frame wrong:\n{s}");
    assert!(s.contains("$-1\r\n"), "expected nil slot:\n{s}");
}

#[tokio::test]
async fn command_unknown_subcommand_errors() {
    let state = test_state();
    call(&state, &[b"COMMAND", b"DOCS"]).await.expect_error_prefix("ERR");
}

#[tokio::test]
async fn command_bare_returns_full_metadata_array() {
    let state = test_state();
    // Plain `COMMAND` is an alias for `COMMAND INFO` (no filter).
    let reply = call(&state, &[b"COMMAND"]).await;
    let s = String::from_utf8_lossy(&reply.raw);
    // Outer array starts with `*N\r\n` where N == total count (>100).
    assert!(s.starts_with('*'), "expected array:\n{s}");
}

// ---------------------------------------------------------------------------
// HELLO / CLIENT / RESET
// ---------------------------------------------------------------------------

#[tokio::test]
async fn hello_without_protover_returns_info_map() {
    let state = test_state();
    let reply = call(&state, &[b"HELLO"]).await;
    let s = String::from_utf8_lossy(&reply.raw);
    assert!(s.starts_with("*14\r\n"), "expected 14-element array:\n{s}");
    assert!(s.contains("$8\r\nrustyant\r\n"), "server name missing:\n{s}");
    assert!(s.contains("$5\r\nproto\r\n"), "proto field missing:\n{s}");
    assert!(s.contains(":2\r\n"), "proto version 2 missing:\n{s}");
}

#[tokio::test]
async fn hello_protover_2_succeeds() {
    let state = test_state();
    let reply = call(&state, &[b"HELLO", b"2"]).await;
    let s = String::from_utf8_lossy(&reply.raw);
    assert!(s.starts_with("*14\r\n"));
}

#[tokio::test]
async fn hello_protover_3_returns_noproto() {
    let state = test_state();
    let reply = call(&state, &[b"HELLO", b"3"]).await;
    let s = String::from_utf8_lossy(&reply.raw);
    assert!(s.starts_with('-'), "expected error reply:\n{s}");
    assert!(s.contains("NOPROTO"), "expected NOPROTO marker:\n{s}");
}

#[tokio::test]
async fn hello_auth_and_setname_are_accepted_and_ignored() {
    let state = test_state();
    // Both AUTH and SETNAME present -> rustyant accepts the syntax and returns
    // the info map. The auth credentials are ignored (no auth backend).
    let reply = call(&state, &[b"HELLO", b"2", b"AUTH", b"user", b"pass", b"SETNAME", b"redis-py"]).await;
    let s = String::from_utf8_lossy(&reply.raw);
    assert!(s.starts_with("*14\r\n"), "expected info map:\n{s}");
}

#[tokio::test]
async fn hello_invalid_protover_errors() {
    let state = test_state();
    call(&state, &[b"HELLO", b"not-a-number"]).await.expect_error_prefix("ERR");
}

#[tokio::test]
async fn hello_auth_missing_args_errors() {
    let state = test_state();
    // AUTH with no username/password -> syntax error.
    call(&state, &[b"HELLO", b"2", b"AUTH"]).await.expect_error_prefix("ERR");
    call(&state, &[b"HELLO", b"2", b"AUTH", b"user"]).await.expect_error_prefix("ERR");
}

#[tokio::test]
async fn client_setinfo_returns_ok() {
    let state = test_state();
    call(&state, &[b"CLIENT", b"SETINFO", b"lib-name", b"redis-py"]).await.expect_simple("OK");
    call(&state, &[b"CLIENT", b"SETINFO", b"lib-ver", b"5.0.0"]).await.expect_simple("OK");
}

#[tokio::test]
async fn client_setname_returns_ok() {
    let state = test_state();
    call(&state, &[b"CLIENT", b"SETNAME", b"my-client"]).await.expect_simple("OK");
}

#[tokio::test]
async fn client_id_returns_integer() {
    let state = test_state();
    call(&state, &[b"CLIENT", b"ID"]).await.expect_integer(1);
}

#[tokio::test]
async fn client_getname_returns_empty_bulk() {
    let state = test_state();
    // rustyant has no per-connection state, so name is always empty.
    let reply = call(&state, &[b"CLIENT", b"GETNAME"]).await;
    // Empty bulk is `$0\r\n\r\n`.
    assert_eq!(reply.raw, b"$0\r\n\r\n", "got {:?}", String::from_utf8_lossy(&reply.raw));
}

#[tokio::test]
async fn client_info_returns_summary_line() {
    let state = test_state();
    let reply = call(&state, &[b"CLIENT", b"INFO"]).await;
    let s = String::from_utf8_lossy(&reply.raw);
    assert!(s.starts_with('$'), "expected bulk string:\n{s}");
    assert!(s.contains("id=1"), "expected id field:\n{s}");
}

#[tokio::test]
async fn client_unknown_subcommand_errors() {
    let state = test_state();
    call(&state, &[b"CLIENT", b"KILL", b"127.0.0.1:6379"]).await.expect_error_prefix("ERR");
}

#[tokio::test]
async fn client_requires_a_subcommand() {
    let state = test_state();
    call(&state, &[b"CLIENT"]).await.expect_error_prefix("ERR");
}

#[tokio::test]
async fn reset_returns_simple_string() {
    let state = test_state();
    call(&state, &[b"RESET"]).await.expect_simple("RESET");
}

#[tokio::test]
async fn command_info_hello_client_reset_are_registered() {
    let state = test_state();
    for name in [b"HELLO".as_ref(), b"CLIENT".as_ref(), b"RESET".as_ref()] {
        let reply = call(&state, &[b"COMMAND", b"INFO", name]).await;
        let s = String::from_utf8_lossy(&reply.raw);
        assert!(s.starts_with("*1\r\n*6\r\n"), "{name:?} not in COMMAND INFO:\n{s}");
    }
}

// ---------------------------------------------------------------------------
// AUTH / WAIT / SAVE / BGSAVE / BGREWRITEAOF / LASTSAVE / LATENCY / DEBUG
// ---------------------------------------------------------------------------

#[tokio::test]
async fn auth_password_returns_ok() {
    let state = test_state();
    call(&state, &[b"AUTH", b"secret"]).await.expect_simple("OK");
}

#[tokio::test]
async fn auth_username_and_password_returns_ok() {
    let state = test_state();
    call(&state, &[b"AUTH", b"alice", b"secret"]).await.expect_simple("OK");
}

#[tokio::test]
async fn auth_arity_errors() {
    let state = test_state();
    call(&state, &[b"AUTH"]).await.expect_error_prefix("ERR");
    call(&state, &[b"AUTH", b"a", b"b", b"c"]).await.expect_error_prefix("ERR");
}

#[tokio::test]
async fn wait_returns_zero_replicas() {
    let state = test_state();
    call(&state, &[b"WAIT", b"0", b"100"]).await.expect_integer(0);
    call(&state, &[b"WAIT", b"5", b"500"]).await.expect_integer(0);
}

#[tokio::test]
async fn wait_rejects_non_integer_args() {
    let state = test_state();
    call(&state, &[b"WAIT", b"many", b"100"]).await.expect_error_prefix("ERR");
}

#[tokio::test]
async fn save_returns_ok() {
    let state = test_state();
    call(&state, &[b"SAVE"]).await.expect_simple("OK");
}

#[tokio::test]
async fn bgsave_returns_standard_acknowledgment() {
    let state = test_state();
    call(&state, &[b"BGSAVE"]).await.expect_simple("Background saving started");
    call(&state, &[b"BGSAVE", b"SCHEDULE"]).await.expect_simple("Background saving started");
}

#[tokio::test]
async fn bgsave_rejects_unknown_option() {
    let state = test_state();
    call(&state, &[b"BGSAVE", b"NOW"]).await.expect_error_prefix("ERR");
}

#[tokio::test]
async fn bgrewriteaof_returns_standard_acknowledgment() {
    let state = test_state();
    call(&state, &[b"BGREWRITEAOF"]).await.expect_simple("Background append only file rewriting started");
}

#[tokio::test]
async fn lastsave_returns_positive_epoch_seconds() {
    let state = test_state();
    let reply = call(&state, &[b"LASTSAVE"]).await;
    let n: i64 = String::from_utf8_lossy(&reply.raw).trim_start_matches(':').trim_end().parse().expect("int");
    assert!(n > 1_700_000_000, "LASTSAVE should be a real epoch, got {n}");
}

#[tokio::test]
async fn latency_reset_returns_zero() {
    let state = test_state();
    call(&state, &[b"LATENCY", b"RESET"]).await.expect_integer(0);
}

#[tokio::test]
async fn latency_history_returns_empty_array() {
    let state = test_state();
    let reply = call(&state, &[b"LATENCY", b"HISTORY", b"event"]).await;
    assert_eq!(reply.raw, b"*0\r\n", "got {:?}", String::from_utf8_lossy(&reply.raw));
}

#[tokio::test]
async fn latency_history_arity_errors_without_event() {
    let state = test_state();
    call(&state, &[b"LATENCY", b"HISTORY"]).await.expect_error_prefix("ERR");
}

#[tokio::test]
async fn latency_latest_returns_empty_array() {
    let state = test_state();
    let reply = call(&state, &[b"LATENCY", b"LATEST"]).await;
    assert_eq!(reply.raw, b"*0\r\n");
}

#[tokio::test]
async fn latency_doctor_returns_bulk_string() {
    let state = test_state();
    let reply = call(&state, &[b"LATENCY", b"DOCTOR"]).await;
    let s = String::from_utf8_lossy(&reply.raw);
    assert!(s.starts_with('$'), "expected bulk:\n{s}");
    assert!(s.contains("latency"), "expected doctor line:\n{s}");
}

#[tokio::test]
async fn latency_unknown_subcommand_errors() {
    let state = test_state();
    call(&state, &[b"LATENCY", b"OVERLORD"]).await.expect_error_prefix("ERR");
}

#[tokio::test]
async fn debug_sleep_actually_waits() {
    let state = test_state();
    let started = std::time::Instant::now();
    call(&state, &[b"DEBUG", b"SLEEP", b"0.2"]).await.expect_simple("OK");
    let elapsed = started.elapsed();
    assert!(elapsed.as_millis() >= 150, "DEBUG SLEEP returned early: {elapsed:?}");
}

#[tokio::test]
async fn debug_sleep_rejects_negative() {
    let state = test_state();
    call(&state, &[b"DEBUG", b"SLEEP", b"-1"]).await.expect_error_prefix("ERR");
}

#[tokio::test]
async fn debug_other_subcommands_error_explicitly() {
    let state = test_state();
    call(&state, &[b"DEBUG", b"OBJECT", b"k"]).await.expect_error_prefix("ERR");
    call(&state, &[b"DEBUG", b"RELOAD"]).await.expect_error_prefix("ERR");
}

#[tokio::test]
async fn new_server_stubs_registered_in_command_meta() {
    let state = test_state();
    for name in [
        b"AUTH".as_ref(),
        b"WAIT".as_ref(),
        b"SAVE".as_ref(),
        b"BGSAVE".as_ref(),
        b"BGREWRITEAOF".as_ref(),
        b"LASTSAVE".as_ref(),
        b"LATENCY".as_ref(),
        b"DEBUG".as_ref(),
    ] {
        let reply = call(&state, &[b"COMMAND", b"INFO", name]).await;
        let s = String::from_utf8_lossy(&reply.raw);
        assert!(s.starts_with("*1\r\n*6\r\n"), "{name:?} not in COMMAND INFO:\n{s}");
    }
}

// ---------------------------------------------------------------------------
// MULTI / EXEC / DISCARD / WATCH / UNWATCH — transaction-stub policy:
// MULTI and WATCH fail explicitly (no cross-request state); UNWATCH is a
// trivially-successful no-op; EXEC/DISCARD return Redis's standard
// "without MULTI" error.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn multi_returns_explicit_not_supported_error() {
    let state = test_state();
    let reply = call(&state, &[b"MULTI"]).await;
    reply.expect_error_prefix("ERR");
    let s = String::from_utf8_lossy(&reply.raw);
    assert!(s.contains("not supported"), "expected not-supported error, got {s:?}");
}

#[tokio::test]
async fn multi_rejects_extra_args() {
    let state = test_state();
    call(&state, &[b"MULTI", b"extra"]).await.expect_error_prefix("ERR");
}

#[tokio::test]
async fn exec_without_multi_returns_standard_error() {
    let state = test_state();
    let reply = call(&state, &[b"EXEC"]).await;
    reply.expect_error_prefix("ERR");
    let s = String::from_utf8_lossy(&reply.raw);
    assert!(s.contains("EXEC without MULTI"), "expected standard EXEC error, got {s:?}");
}

#[tokio::test]
async fn exec_rejects_extra_args() {
    let state = test_state();
    call(&state, &[b"EXEC", b"extra"]).await.expect_error_prefix("ERR");
}

#[tokio::test]
async fn discard_without_multi_returns_standard_error() {
    let state = test_state();
    let reply = call(&state, &[b"DISCARD"]).await;
    reply.expect_error_prefix("ERR");
    let s = String::from_utf8_lossy(&reply.raw);
    assert!(s.contains("DISCARD without MULTI"), "expected standard DISCARD error, got {s:?}");
}

#[tokio::test]
async fn discard_rejects_extra_args() {
    let state = test_state();
    call(&state, &[b"DISCARD", b"extra"]).await.expect_error_prefix("ERR");
}

#[tokio::test]
async fn watch_single_key_returns_not_supported_error() {
    let state = test_state();
    let reply = call(&state, &[b"WATCH", b"key1"]).await;
    reply.expect_error_prefix("ERR");
    let s = String::from_utf8_lossy(&reply.raw);
    assert!(s.contains("not supported"), "expected not-supported error, got {s:?}");
}

#[tokio::test]
async fn watch_multiple_keys_returns_not_supported_error() {
    let state = test_state();
    call(&state, &[b"WATCH", b"k1", b"k2", b"k3"]).await.expect_error_prefix("ERR");
}

#[tokio::test]
async fn watch_without_keys_is_arity_error() {
    let state = test_state();
    call(&state, &[b"WATCH"]).await.expect_error_prefix("ERR");
}

#[tokio::test]
async fn unwatch_returns_ok() {
    let state = test_state();
    call(&state, &[b"UNWATCH"]).await.expect_simple("OK");
}

#[tokio::test]
async fn unwatch_rejects_extra_args() {
    let state = test_state();
    call(&state, &[b"UNWATCH", b"extra"]).await.expect_error_prefix("ERR");
}

#[tokio::test]
async fn transaction_stubs_registered_in_command_meta() {
    let state = test_state();
    for name in [b"MULTI".as_ref(), b"EXEC".as_ref(), b"DISCARD".as_ref(), b"WATCH".as_ref(), b"UNWATCH".as_ref()] {
        let reply = call(&state, &[b"COMMAND", b"INFO", name]).await;
        let s = String::from_utf8_lossy(&reply.raw);
        assert!(s.starts_with("*1\r\n*6\r\n"), "{name:?} not in COMMAND INFO:\n{s}");
    }
}

// ---------------------------------------------------------------------------
// SUBSCRIBE / PSUBSCRIBE / UNSUBSCRIBE / PUNSUBSCRIBE / PUBLISH / PUBSUB
// Pub/sub-stub policy: the subscribe/unsubscribe surface errors explicitly
// (no long-lived push channel on stateless Lambda); PUBLISH returns :0
// (honest zero-subscribers count); PUBSUB CHANNELS/NUMSUB/NUMPAT return
// honest empty/zero replies.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn subscribe_returns_explicit_not_supported_error() {
    let state = test_state();
    let reply = call(&state, &[b"SUBSCRIBE", b"ch1"]).await;
    reply.expect_error_prefix("ERR");
    let s = String::from_utf8_lossy(&reply.raw);
    assert!(s.contains("not supported"), "expected not-supported error, got {s:?}");
}

#[tokio::test]
async fn subscribe_multiple_channels_errors() {
    let state = test_state();
    call(&state, &[b"SUBSCRIBE", b"a", b"b", b"c"]).await.expect_error_prefix("ERR");
}

#[tokio::test]
async fn subscribe_without_channels_is_arity_error() {
    let state = test_state();
    call(&state, &[b"SUBSCRIBE"]).await.expect_error_prefix("ERR");
}

#[tokio::test]
async fn psubscribe_returns_explicit_not_supported_error() {
    let state = test_state();
    let reply = call(&state, &[b"PSUBSCRIBE", b"news.*"]).await;
    reply.expect_error_prefix("ERR");
    let s = String::from_utf8_lossy(&reply.raw);
    assert!(s.contains("not supported"), "expected not-supported error, got {s:?}");
}

#[tokio::test]
async fn psubscribe_without_patterns_is_arity_error() {
    let state = test_state();
    call(&state, &[b"PSUBSCRIBE"]).await.expect_error_prefix("ERR");
}

#[tokio::test]
async fn unsubscribe_with_or_without_args_errors() {
    let state = test_state();
    call(&state, &[b"UNSUBSCRIBE"]).await.expect_error_prefix("ERR");
    call(&state, &[b"UNSUBSCRIBE", b"ch1"]).await.expect_error_prefix("ERR");
    call(&state, &[b"UNSUBSCRIBE", b"ch1", b"ch2"]).await.expect_error_prefix("ERR");
}

#[tokio::test]
async fn punsubscribe_with_or_without_args_errors() {
    let state = test_state();
    call(&state, &[b"PUNSUBSCRIBE"]).await.expect_error_prefix("ERR");
    call(&state, &[b"PUNSUBSCRIBE", b"news.*"]).await.expect_error_prefix("ERR");
}

#[tokio::test]
async fn publish_returns_zero_subscribers() {
    let state = test_state();
    call(&state, &[b"PUBLISH", b"ch1", b"hello"]).await.expect_integer(0);
    call(&state, &[b"PUBLISH", b"other", b"world"]).await.expect_integer(0);
}

#[tokio::test]
async fn publish_arity_errors() {
    let state = test_state();
    call(&state, &[b"PUBLISH"]).await.expect_error_prefix("ERR");
    call(&state, &[b"PUBLISH", b"ch1"]).await.expect_error_prefix("ERR");
    call(&state, &[b"PUBLISH", b"ch1", b"msg", b"extra"]).await.expect_error_prefix("ERR");
}

#[tokio::test]
async fn pubsub_channels_returns_empty_array() {
    let state = test_state();
    let reply = call(&state, &[b"PUBSUB", b"CHANNELS"]).await;
    assert_eq!(reply.raw, b"*0\r\n", "got {:?}", String::from_utf8_lossy(&reply.raw));
}

#[tokio::test]
async fn pubsub_channels_with_pattern_returns_empty_array() {
    let state = test_state();
    let reply = call(&state, &[b"PUBSUB", b"CHANNELS", b"news.*"]).await;
    assert_eq!(reply.raw, b"*0\r\n");
}

#[tokio::test]
async fn pubsub_channels_rejects_extra_args() {
    let state = test_state();
    call(&state, &[b"PUBSUB", b"CHANNELS", b"a", b"b"]).await.expect_error_prefix("ERR");
}

#[tokio::test]
async fn pubsub_numsub_without_channels_returns_empty_array() {
    let state = test_state();
    let reply = call(&state, &[b"PUBSUB", b"NUMSUB"]).await;
    assert_eq!(reply.raw, b"*0\r\n");
}

#[tokio::test]
async fn pubsub_numsub_returns_channel_zero_pairs() {
    let state = test_state();
    let reply = call(&state, &[b"PUBSUB", b"NUMSUB", b"ch1", b"ch2"]).await;
    let expected = b"*4\r\n$3\r\nch1\r\n:0\r\n$3\r\nch2\r\n:0\r\n";
    assert_eq!(reply.raw, expected, "got {:?}", String::from_utf8_lossy(&reply.raw));
}

#[tokio::test]
async fn pubsub_numpat_returns_zero() {
    let state = test_state();
    call(&state, &[b"PUBSUB", b"NUMPAT"]).await.expect_integer(0);
}

#[tokio::test]
async fn pubsub_numpat_rejects_extra_args() {
    let state = test_state();
    call(&state, &[b"PUBSUB", b"NUMPAT", b"extra"]).await.expect_error_prefix("ERR");
}

#[tokio::test]
async fn pubsub_unknown_subcommand_errors() {
    let state = test_state();
    call(&state, &[b"PUBSUB", b"OVERLORD"]).await.expect_error_prefix("ERR");
}

#[tokio::test]
async fn pubsub_without_subcommand_is_arity_error() {
    let state = test_state();
    call(&state, &[b"PUBSUB"]).await.expect_error_prefix("ERR");
}

#[tokio::test]
async fn pubsub_stubs_registered_in_command_meta() {
    let state = test_state();
    for name in [
        b"SUBSCRIBE".as_ref(),
        b"PSUBSCRIBE".as_ref(),
        b"UNSUBSCRIBE".as_ref(),
        b"PUNSUBSCRIBE".as_ref(),
        b"PUBLISH".as_ref(),
        b"PUBSUB".as_ref(),
    ] {
        let reply = call(&state, &[b"COMMAND", b"INFO", name]).await;
        let s = String::from_utf8_lossy(&reply.raw);
        assert!(s.starts_with("*1\r\n*6\r\n"), "{name:?} not in COMMAND INFO:\n{s}");
    }
}

// ---------------------------------------------------------------------------
// GEOADD / GEOPOS / GEODIST / GEOHASH — Core 4 geo surface layered on ZSETs.
// Reference values throughout come from Redis's own documentation example
// (Sicily: Palermo + Catania), so the tests double as a check against Redis's
// wire format.
// ---------------------------------------------------------------------------

const PALERMO_LON: &[u8] = b"13.361389";
const PALERMO_LAT: &[u8] = b"38.115556";
const CATANIA_LON: &[u8] = b"15.087269";
const CATANIA_LAT: &[u8] = b"37.502669";

#[tokio::test]
async fn geoadd_single_member_returns_one() {
    let state = test_state();
    call(&state, &[b"GEOADD", b"g1", PALERMO_LON, PALERMO_LAT, b"Palermo"]).await.expect_integer(1);
}

#[tokio::test]
async fn geoadd_two_members_returns_two() {
    let state = test_state();
    let added =
        call(&state, &[b"GEOADD", b"g2", PALERMO_LON, PALERMO_LAT, b"Palermo", CATANIA_LON, CATANIA_LAT, b"Catania"])
            .await;
    added.expect_integer(2);
}

#[tokio::test]
async fn geoadd_updating_existing_returns_zero_without_ch() {
    let state = test_state();
    call(&state, &[b"GEOADD", b"g3", PALERMO_LON, PALERMO_LAT, b"Palermo"]).await.expect_integer(1);
    // Same member, different coords — update, not an add.
    call(&state, &[b"GEOADD", b"g3", CATANIA_LON, CATANIA_LAT, b"Palermo"]).await.expect_integer(0);
}

#[tokio::test]
async fn geoadd_ch_counts_updates() {
    let state = test_state();
    call(&state, &[b"GEOADD", b"g4", PALERMO_LON, PALERMO_LAT, b"p"]).await.expect_integer(1);
    call(&state, &[b"GEOADD", b"g4", b"CH", CATANIA_LON, CATANIA_LAT, b"p"]).await.expect_integer(1);
}

#[tokio::test]
async fn geoadd_nx_preserves_existing_coords() {
    let state = test_state();
    call(&state, &[b"GEOADD", b"g5", PALERMO_LON, PALERMO_LAT, b"p"]).await.expect_integer(1);
    // NX: member exists, update is suppressed → no change counted.
    call(&state, &[b"GEOADD", b"g5", b"NX", CATANIA_LON, CATANIA_LAT, b"p"]).await.expect_integer(0);
    // GEOPOS confirms the coords are still Palermo's.
    let reply = call(&state, &[b"GEOPOS", b"g5", b"p"]).await;
    let raw = String::from_utf8_lossy(&reply.raw);
    assert!(raw.contains("13.36"), "NX leaked the update:\n{raw}");
}

#[tokio::test]
async fn geoadd_xx_refuses_to_create_new_member() {
    let state = test_state();
    call(&state, &[b"GEOADD", b"g6", b"XX", PALERMO_LON, PALERMO_LAT, b"p"]).await.expect_integer(0);
    call(&state, &[b"EXISTS", b"g6"]).await.expect_integer(0);
}

#[tokio::test]
async fn geoadd_xx_allows_updating_existing_member() {
    let state = test_state();
    call(&state, &[b"GEOADD", b"g7", PALERMO_LON, PALERMO_LAT, b"p"]).await.expect_integer(1);
    call(&state, &[b"GEOADD", b"g7", b"XX", b"CH", CATANIA_LON, CATANIA_LAT, b"p"]).await.expect_integer(1);
}

#[tokio::test]
async fn geoadd_rejects_combining_nx_and_xx() {
    let state = test_state();
    call(&state, &[b"GEOADD", b"g8", b"NX", b"XX", PALERMO_LON, PALERMO_LAT, b"p"]).await.expect_error_prefix("ERR");
}

#[tokio::test]
async fn geoadd_rejects_out_of_range_coordinates() {
    let state = test_state();
    // Longitude out of range.
    call(&state, &[b"GEOADD", b"g9", b"181.0", b"38.0", b"p"]).await.expect_error_prefix("ERR");
    // Latitude past Redis's Mercator cap.
    call(&state, &[b"GEOADD", b"g9", b"13.0", b"86.0", b"p"]).await.expect_error_prefix("ERR");
}

#[tokio::test]
async fn geoadd_wrong_arity_errors() {
    let state = test_state();
    call(&state, &[b"GEOADD"]).await.expect_error_prefix("ERR");
    call(&state, &[b"GEOADD", b"k"]).await.expect_error_prefix("ERR");
    // Triple doesn't divide: lon lat member lon (missing lat + member)
    call(&state, &[b"GEOADD", b"k", PALERMO_LON, PALERMO_LAT, b"p", CATANIA_LON]).await.expect_error_prefix("ERR");
}

#[tokio::test]
async fn geoadd_on_wrong_type_errors_with_wrong_type_message() {
    let state = test_state();
    call(&state, &[b"SET", b"g_string", b"x"]).await.expect_simple("OK");
    let reply = call(&state, &[b"GEOADD", b"g_string", PALERMO_LON, PALERMO_LAT, b"p"]).await;
    reply.expect_error_prefix("ERR");
    let s = String::from_utf8_lossy(&reply.raw);
    assert!(s.contains("wrong type"), "expected wrong-type error, got {s:?}");
}

#[tokio::test]
async fn geopos_returns_lon_lat_within_tolerance() {
    let state = test_state();
    call(&state, &[b"GEOADD", b"g10", PALERMO_LON, PALERMO_LAT, b"Palermo"]).await.expect_integer(1);
    let reply = call(&state, &[b"GEOPOS", b"g10", b"Palermo"]).await;
    let s = String::from_utf8_lossy(&reply.raw);
    // Expect an array of one array of two bulk strings.
    assert!(s.starts_with("*1\r\n*2\r\n$"), "unexpected shape:\n{s}");
    assert!(s.contains("13.36"), "longitude missing from reply:\n{s}");
    assert!(s.contains("38.1"), "latitude missing from reply:\n{s}");
}

#[tokio::test]
async fn geopos_missing_member_returns_nil_element() {
    let state = test_state();
    call(&state, &[b"GEOADD", b"g11", PALERMO_LON, PALERMO_LAT, b"Palermo"]).await.expect_integer(1);
    let reply = call(&state, &[b"GEOPOS", b"g11", b"Palermo", b"Nowhere"]).await;
    let s = String::from_utf8_lossy(&reply.raw);
    assert!(s.starts_with("*2\r\n*2\r\n$"), "unexpected shape:\n{s}");
    assert!(s.contains("$-1\r\n"), "expected nil for missing member:\n{s}");
}

#[tokio::test]
async fn geopos_on_missing_key_returns_all_nil() {
    let state = test_state();
    let reply = call(&state, &[b"GEOPOS", b"ghost", b"a", b"b"]).await;
    // Two nil elements.
    assert_eq!(reply.raw, b"*2\r\n$-1\r\n$-1\r\n");
}

#[tokio::test]
async fn geodist_palermo_catania_matches_redis_example_meters() {
    let state = test_state();
    call(&state, &[b"GEOADD", b"g12", PALERMO_LON, PALERMO_LAT, b"Palermo", CATANIA_LON, CATANIA_LAT, b"Catania"])
        .await
        .expect_integer(2);
    let reply = call(&state, &[b"GEODIST", b"g12", b"Palermo", b"Catania"]).await;
    let s = String::from_utf8_lossy(&reply.raw);
    // Redis reports "166274.1516" m for this exact example.
    assert!(s.contains("166274.15"), "expected Palermo→Catania ≈ 166274 m, got:\n{s}");
}

#[tokio::test]
async fn geodist_km_unit_converts() {
    let state = test_state();
    call(&state, &[b"GEOADD", b"g13", PALERMO_LON, PALERMO_LAT, b"Palermo", CATANIA_LON, CATANIA_LAT, b"Catania"])
        .await
        .expect_integer(2);
    let reply = call(&state, &[b"GEODIST", b"g13", b"Palermo", b"Catania", b"km"]).await;
    let s = String::from_utf8_lossy(&reply.raw);
    assert!(s.contains("166.27"), "expected Palermo→Catania ≈ 166.27 km, got:\n{s}");
}

#[tokio::test]
async fn geodist_missing_member_returns_nil() {
    let state = test_state();
    call(&state, &[b"GEOADD", b"g14", PALERMO_LON, PALERMO_LAT, b"Palermo"]).await.expect_integer(1);
    call(&state, &[b"GEODIST", b"g14", b"Palermo", b"Ghost"]).await.expect_nil();
}

#[tokio::test]
async fn geodist_rejects_unknown_unit() {
    let state = test_state();
    call(&state, &[b"GEOADD", b"g15", PALERMO_LON, PALERMO_LAT, b"Palermo", CATANIA_LON, CATANIA_LAT, b"Catania"])
        .await
        .expect_integer(2);
    call(&state, &[b"GEODIST", b"g15", b"Palermo", b"Catania", b"yd"]).await.expect_error_prefix("ERR");
}

#[tokio::test]
async fn geohash_matches_redis_reference_values() {
    let state = test_state();
    call(&state, &[b"GEOADD", b"g16", PALERMO_LON, PALERMO_LAT, b"Palermo", CATANIA_LON, CATANIA_LAT, b"Catania"])
        .await
        .expect_integer(2);
    let reply = call(&state, &[b"GEOHASH", b"g16", b"Palermo", b"Catania"]).await;
    let s = String::from_utf8_lossy(&reply.raw);
    // Redis's canonical reply: ["sqc8b49rny0", "sqdtr74hyu0"]
    assert!(s.contains("sqc8b49rny0"), "Palermo geohash wrong in:\n{s}");
    assert!(s.contains("sqdtr74hyu0"), "Catania geohash wrong in:\n{s}");
}

#[tokio::test]
async fn geohash_missing_member_returns_nil() {
    let state = test_state();
    call(&state, &[b"GEOADD", b"g17", PALERMO_LON, PALERMO_LAT, b"Palermo"]).await.expect_integer(1);
    let reply = call(&state, &[b"GEOHASH", b"g17", b"Palermo", b"Ghost"]).await;
    let s = String::from_utf8_lossy(&reply.raw);
    assert!(s.contains("sqc8b49rny0"), "expected Palermo hash:\n{s}");
    assert!(s.contains("$-1\r\n"), "expected nil for missing member:\n{s}");
}

#[tokio::test]
async fn geo_type_is_zset() {
    let state = test_state();
    call(&state, &[b"GEOADD", b"g18", PALERMO_LON, PALERMO_LAT, b"Palermo"]).await.expect_integer(1);
    call(&state, &[b"TYPE", b"g18"]).await.expect_simple("zset");
}

#[tokio::test]
async fn geo_stubs_registered_in_command_meta() {
    let state = test_state();
    for name in [b"GEOADD".as_ref(), b"GEOPOS".as_ref(), b"GEODIST".as_ref(), b"GEOHASH".as_ref()] {
        let reply = call(&state, &[b"COMMAND", b"INFO", name]).await;
        let s = String::from_utf8_lossy(&reply.raw);
        assert!(s.starts_with("*1\r\n*6\r\n"), "{name:?} not in COMMAND INFO:\n{s}");
    }
}

// ---------------------------------------------------------------------------
// GEOSEARCH / GEOSEARCHSTORE — spatial search on a geo ZSET.
// Uses the Sicily example (Palermo + Catania) plus a farther-away "Dublin"
// point for inclusion/exclusion tests, matching Redis's canonical docs
// setup.
// ---------------------------------------------------------------------------

const DUBLIN_LON: &[u8] = b"-6.2603"; // Dublin, Ireland — ~2000 km from Sicily
const DUBLIN_LAT: &[u8] = b"53.3498";

async fn seed_sicily_plus_dublin(state: &State, key: &[u8]) {
    call(
        state,
        &[
            b"GEOADD",
            key,
            PALERMO_LON,
            PALERMO_LAT,
            b"Palermo",
            CATANIA_LON,
            CATANIA_LAT,
            b"Catania",
            DUBLIN_LON,
            DUBLIN_LAT,
            b"Dublin",
        ],
    )
    .await
    .expect_integer(3);
}

#[tokio::test]
async fn geosearch_byradius_fromlonlat_returns_nearby_members() {
    let state = test_state();
    seed_sicily_plus_dublin(&state, b"gs1").await;
    let reply =
        call(&state, &[b"GEOSEARCH", b"gs1", b"FROMLONLAT", PALERMO_LON, PALERMO_LAT, b"BYRADIUS", b"200", b"km"])
            .await;
    let s = String::from_utf8_lossy(&reply.raw);
    assert!(s.contains("Palermo"), "expected Palermo within 200 km:\n{s}");
    assert!(s.contains("Catania"), "expected Catania within 200 km:\n{s}");
    assert!(!s.contains("Dublin"), "Dublin must not be within 200 km of Palermo:\n{s}");
}

#[tokio::test]
async fn geosearch_byradius_frommember_uses_members_location() {
    let state = test_state();
    seed_sicily_plus_dublin(&state, b"gs2").await;
    let reply = call(&state, &[b"GEOSEARCH", b"gs2", b"FROMMEMBER", b"Palermo", b"BYRADIUS", b"200", b"km"]).await;
    let s = String::from_utf8_lossy(&reply.raw);
    assert!(s.contains("Palermo"), "raw reply:\n{s}");
    assert!(s.contains("Catania"), "raw reply:\n{s}");
    assert!(!s.contains("Dublin"), "raw reply:\n{s}");
}

#[tokio::test]
async fn geosearch_frommember_missing_errors() {
    let state = test_state();
    seed_sicily_plus_dublin(&state, b"gs3").await;
    call(&state, &[b"GEOSEARCH", b"gs3", b"FROMMEMBER", b"Ghost", b"BYRADIUS", b"200", b"km"])
        .await
        .expect_error_prefix("ERR");
}

#[tokio::test]
async fn geosearch_bybox_filters_by_rectangle() {
    let state = test_state();
    seed_sicily_plus_dublin(&state, b"gs4").await;
    // 400 km wide × 200 km tall box around Palermo catches Catania (~166 km
    // east) but not Dublin (thousands of km away).
    let reply =
        call(&state, &[b"GEOSEARCH", b"gs4", b"FROMLONLAT", PALERMO_LON, PALERMO_LAT, b"BYBOX", b"400", b"200", b"km"])
            .await;
    let s = String::from_utf8_lossy(&reply.raw);
    assert!(s.contains("Palermo"));
    assert!(s.contains("Catania"));
    assert!(!s.contains("Dublin"));
}

#[tokio::test]
async fn geosearch_asc_sorts_closest_first() {
    let state = test_state();
    seed_sicily_plus_dublin(&state, b"gs5").await;
    // Centre on Catania; Catania should come before Palermo in ASC.
    let reply = call(
        &state,
        &[b"GEOSEARCH", b"gs5", b"FROMLONLAT", CATANIA_LON, CATANIA_LAT, b"BYRADIUS", b"300", b"km", b"ASC"],
    )
    .await;
    let s = String::from_utf8_lossy(&reply.raw);
    let catania_idx = s.find("Catania").expect("has Catania");
    let palermo_idx = s.find("Palermo").expect("has Palermo");
    assert!(catania_idx < palermo_idx, "ASC should put Catania first:\n{s}");
}

#[tokio::test]
async fn geosearch_desc_sorts_farthest_first() {
    let state = test_state();
    seed_sicily_plus_dublin(&state, b"gs6").await;
    let reply = call(
        &state,
        &[b"GEOSEARCH", b"gs6", b"FROMLONLAT", CATANIA_LON, CATANIA_LAT, b"BYRADIUS", b"300", b"km", b"DESC"],
    )
    .await;
    let s = String::from_utf8_lossy(&reply.raw);
    let catania_idx = s.find("Catania").expect("has Catania");
    let palermo_idx = s.find("Palermo").expect("has Palermo");
    assert!(palermo_idx < catania_idx, "DESC should put Palermo first:\n{s}");
}

#[tokio::test]
async fn geosearch_count_caps_results() {
    let state = test_state();
    seed_sicily_plus_dublin(&state, b"gs7").await;
    let reply = call(
        &state,
        &[
            b"GEOSEARCH",
            b"gs7",
            b"FROMLONLAT",
            PALERMO_LON,
            PALERMO_LAT,
            b"BYRADIUS",
            b"5000",
            b"km",
            b"ASC",
            b"COUNT",
            b"1",
        ],
    )
    .await;
    let s = String::from_utf8_lossy(&reply.raw);
    // Exactly one result.
    assert!(s.starts_with("*1\r\n"), "expected single-element array:\n{s}");
    assert!(s.contains("Palermo"), "closest to Palermo is Palermo itself:\n{s}");
}

#[tokio::test]
async fn geosearch_count_any_is_accepted() {
    let state = test_state();
    seed_sicily_plus_dublin(&state, b"gs8").await;
    let reply = call(
        &state,
        &[
            b"GEOSEARCH",
            b"gs8",
            b"FROMLONLAT",
            PALERMO_LON,
            PALERMO_LAT,
            b"BYRADIUS",
            b"300",
            b"km",
            b"COUNT",
            b"10",
            b"ANY",
        ],
    )
    .await;
    let s = String::from_utf8_lossy(&reply.raw);
    assert!(s.starts_with('*'), "should produce an array reply:\n{s}");
}

#[tokio::test]
async fn geosearch_withdist_appends_distance_per_match() {
    let state = test_state();
    seed_sicily_plus_dublin(&state, b"gs9").await;
    let reply = call(
        &state,
        &[
            b"GEOSEARCH",
            b"gs9",
            b"FROMLONLAT",
            PALERMO_LON,
            PALERMO_LAT,
            b"BYRADIUS",
            b"300",
            b"km",
            b"WITHDIST",
            b"ASC",
        ],
    )
    .await;
    let s = String::from_utf8_lossy(&reply.raw);
    // Nested arrays: each element is `*2\r\n<name>\r\n<dist>\r\n`.
    assert!(s.contains("*2\r\n"), "expected per-result nested arrays:\n{s}");
    // Palermo-to-Palermo self-distance is at most the 26-bit cell size
    // (~0.5 m), so in km it's 0.0000 or 0.0001 depending on rounding.
    assert!(s.contains("0.0000") || s.contains("0.0001"), "expected self-distance near 0 km:\n{s}");
    assert!(s.contains("166.27") || s.contains("166.28"), "expected Palermo→Catania ≈ 166.27 km:\n{s}");
}

#[tokio::test]
async fn geosearch_withhash_appends_integer_score_per_match() {
    let state = test_state();
    seed_sicily_plus_dublin(&state, b"gs10").await;
    let reply = call(
        &state,
        &[b"GEOSEARCH", b"gs10", b"FROMLONLAT", PALERMO_LON, PALERMO_LAT, b"BYRADIUS", b"50", b"km", b"WITHHASH"],
    )
    .await;
    let s = String::from_utf8_lossy(&reply.raw);
    // Has an integer reply `:NNN\r\n` inside the nested array.
    assert!(s.contains("\r\n:"), "expected integer hash in reply:\n{s}");
}

#[tokio::test]
async fn geosearch_withcoord_appends_lon_lat_per_match() {
    let state = test_state();
    seed_sicily_plus_dublin(&state, b"gs11").await;
    let reply = call(
        &state,
        &[b"GEOSEARCH", b"gs11", b"FROMLONLAT", PALERMO_LON, PALERMO_LAT, b"BYRADIUS", b"50", b"km", b"WITHCOORD"],
    )
    .await;
    let s = String::from_utf8_lossy(&reply.raw);
    assert!(s.contains("13.36"), "expected longitude in WITHCOORD reply:\n{s}");
    assert!(s.contains("38.1"), "expected latitude in WITHCOORD reply:\n{s}");
}

#[tokio::test]
async fn geosearch_requires_centre_and_shape() {
    let state = test_state();
    seed_sicily_plus_dublin(&state, b"gs12").await;
    // Missing centre.
    call(&state, &[b"GEOSEARCH", b"gs12", b"BYRADIUS", b"10", b"km"]).await.expect_error_prefix("ERR");
    // Missing shape.
    call(&state, &[b"GEOSEARCH", b"gs12", b"FROMLONLAT", PALERMO_LON, PALERMO_LAT]).await.expect_error_prefix("ERR");
}

#[tokio::test]
async fn geosearch_on_missing_key_returns_empty_array() {
    let state = test_state();
    let reply = call(
        &state,
        &[b"GEOSEARCH", b"ghost-key", b"FROMLONLAT", PALERMO_LON, PALERMO_LAT, b"BYRADIUS", b"100", b"km"],
    )
    .await;
    assert_eq!(reply.raw, b"*0\r\n");
}

#[tokio::test]
async fn geosearch_on_wrong_type_errors() {
    let state = test_state();
    call(&state, &[b"SET", b"gs-string", b"x"]).await.expect_simple("OK");
    call(&state, &[b"GEOSEARCH", b"gs-string", b"FROMLONLAT", PALERMO_LON, PALERMO_LAT, b"BYRADIUS", b"10", b"km"])
        .await
        .expect_error_prefix("ERR");
}

#[tokio::test]
async fn geosearchstore_copies_matches_to_destination_with_geohash_scores() {
    let state = test_state();
    seed_sicily_plus_dublin(&state, b"gss-src").await;
    let stored = call(
        &state,
        &[
            b"GEOSEARCHSTORE",
            b"gss-dst",
            b"gss-src",
            b"FROMLONLAT",
            PALERMO_LON,
            PALERMO_LAT,
            b"BYRADIUS",
            b"300",
            b"km",
        ],
    )
    .await;
    stored.expect_integer(2);
    // Destination should be a ZSET containing Palermo and Catania.
    call(&state, &[b"TYPE", b"gss-dst"]).await.expect_simple("zset");
    call(&state, &[b"ZCARD", b"gss-dst"]).await.expect_integer(2);
    // GEOHASH on the destination should produce Palermo's canonical string —
    // confirming scores were preserved (not replaced with distances).
    let reply = call(&state, &[b"GEOHASH", b"gss-dst", b"Palermo"]).await;
    let s = String::from_utf8_lossy(&reply.raw);
    assert!(s.contains("sqc8b49rny0"), "destination lost geohash score fidelity:\n{s}");
}

#[tokio::test]
async fn geosearchstore_storedist_uses_distance_as_score_in_request_unit() {
    let state = test_state();
    seed_sicily_plus_dublin(&state, b"gss2-src").await;
    // BYRADIUS in metres → stored scores are in metres (Redis's rule:
    // STOREDIST distances are in the unit requested by BYRADIUS/BYBOX).
    let stored = call(
        &state,
        &[
            b"GEOSEARCHSTORE",
            b"gss2-dst",
            b"gss2-src",
            b"FROMLONLAT",
            PALERMO_LON,
            PALERMO_LAT,
            b"BYRADIUS",
            b"300000",
            b"m",
            b"STOREDIST",
        ],
    )
    .await;
    stored.expect_integer(2);
    // Palermo's score should be 0 (distance to itself); Catania's ~166274 m.
    let reply = call(&state, &[b"ZSCORE", b"gss2-dst", b"Palermo"]).await;
    let s = String::from_utf8_lossy(&reply.raw);
    assert!(s.starts_with('$') && s.contains('0'), "expected 0-distance for self:\n{s}");
    let reply = call(&state, &[b"ZSCORE", b"gss2-dst", b"Catania"]).await;
    let s = String::from_utf8_lossy(&reply.raw);
    assert!(s.contains("166274"), "expected Catania distance ~166274 m:\n{s}");
}

#[tokio::test]
async fn geosearchstore_storedist_in_km_converts_unit() {
    let state = test_state();
    seed_sicily_plus_dublin(&state, b"gss3-src").await;
    let stored = call(
        &state,
        &[
            b"GEOSEARCHSTORE",
            b"gss3-dst",
            b"gss3-src",
            b"FROMLONLAT",
            PALERMO_LON,
            PALERMO_LAT,
            b"BYRADIUS",
            b"300",
            b"km",
            b"STOREDIST",
        ],
    )
    .await;
    stored.expect_integer(2);
    // With km unit, Catania's stored score should be ~166.27 (km).
    let reply = call(&state, &[b"ZSCORE", b"gss3-dst", b"Catania"]).await;
    let s = String::from_utf8_lossy(&reply.raw);
    assert!(s.contains("166.27") || s.contains("166.28"), "expected Catania ≈ 166.27 km as score:\n{s}");
}

#[tokio::test]
async fn geosearchstore_overwrites_existing_destination() {
    let state = test_state();
    seed_sicily_plus_dublin(&state, b"gss4-src").await;
    // Pre-populate destination with something different.
    call(&state, &[b"SET", b"gss4-dst", b"ignored"]).await.expect_simple("OK");
    call(
        &state,
        &[
            b"GEOSEARCHSTORE",
            b"gss4-dst",
            b"gss4-src",
            b"FROMLONLAT",
            PALERMO_LON,
            PALERMO_LAT,
            b"BYRADIUS",
            b"300",
            b"km",
        ],
    )
    .await
    .expect_integer(2);
    // Destination is now a ZSET, not a string.
    call(&state, &[b"TYPE", b"gss4-dst"]).await.expect_simple("zset");
}

#[tokio::test]
async fn geosearchstore_with_no_matches_returns_zero_and_removes_destination() {
    let state = test_state();
    seed_sicily_plus_dublin(&state, b"gss5-src").await;
    // Pre-populate destination so we can verify it gets cleared.
    call(&state, &[b"GEOADD", b"gss5-dst", PALERMO_LON, PALERMO_LAT, b"old"]).await.expect_integer(1);
    // Centre on the Pacific, thousands of km from every seeded member, and
    // cap radius at 10 m so nothing matches.
    call(
        &state,
        &[b"GEOSEARCHSTORE", b"gss5-dst", b"gss5-src", b"FROMLONLAT", b"-140.0", b"0.0", b"BYRADIUS", b"10", b"m"],
    )
    .await
    .expect_integer(0);
    // No members stored → destination should not exist.
    call(&state, &[b"EXISTS", b"gss5-dst"]).await.expect_integer(0);
}

#[tokio::test]
async fn geosearchstore_rejects_with_flags() {
    let state = test_state();
    seed_sicily_plus_dublin(&state, b"gss6-src").await;
    // WITHCOORD / WITHDIST / WITHHASH are GEOSEARCH-only.
    call(
        &state,
        &[
            b"GEOSEARCHSTORE",
            b"gss6-dst",
            b"gss6-src",
            b"FROMLONLAT",
            PALERMO_LON,
            PALERMO_LAT,
            b"BYRADIUS",
            b"300",
            b"km",
            b"WITHCOORD",
        ],
    )
    .await
    .expect_error_prefix("ERR");
}

#[tokio::test]
async fn geosearch_stubs_registered_in_command_meta() {
    let state = test_state();
    for name in [b"GEOSEARCH".as_ref(), b"GEOSEARCHSTORE".as_ref()] {
        let reply = call(&state, &[b"COMMAND", b"INFO", name]).await;
        let s = String::from_utf8_lossy(&reply.raw);
        assert!(s.starts_with("*1\r\n*6\r\n"), "{name:?} not in COMMAND INFO:\n{s}");
    }
}
