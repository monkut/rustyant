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

// Use RespReply publicly to check the crate re-export surface compiles.
#[test]
fn reply_encode_simple_works_from_tests() {
    let r = RespReply::ok();
    let enc = r.encode().expect("encode");
    assert_eq!(&enc[..], b"+OK\r\n");
}
