//! `DynamoDbBackend` round-trip tests against a local `amazon/dynamodb-local`
//! container.
//!
//! Gated on `RUSTYANT_DYNAMODB_URL`. Locally: `just dynamodb-up &&
//! just dynamodb-seed` then `just test-dynamodb`. Each test uses a
//! per-test key prefix so nextest's parallel runners don't collide.
//!
//! `DynamoDB` has first-class conditional writes (`ConditionExpression`), so
//! the concurrent-INCR convergence test runs here unconditionally — there
//! is no equivalent of the S3 backend's `RUSTYANT_S3_CAS` gate.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use aws_sdk_dynamodb::Client as DynamoClient;
use aws_sdk_dynamodb::config::{BehaviorVersion, Credentials, Region};
use bytes::Bytes;
use rustyant::dynamodb::{DynamoDbBackend, TableNames};
use rustyant::storage::{KVStorage, Storage};

const DEFAULT_PREFIX: &str = "rustyant-";

/// Worker count for the concurrent-INCR convergence test. Tuned to fit
/// inside the storage layer's `MAX_CAS_RETRIES` (5) — with 4 truly-parallel
/// workers and exponential backoff, the worst-case loser still gets ~3
/// retries to land its CAS. The S3 counterpart in `tests/floci.rs` runs
/// 8 workers because its convergence test only fires on real AWS S3 (where
/// network jitter naturally spreads the contention).
const TASKS: usize = 4;

/// Per-process counter so each test gets a unique key prefix even when
/// nextest runs them in parallel against the same shared `DynamoDB` Local.
static SUFFIX_SEQ: AtomicU64 = AtomicU64::new(0);

fn dynamodb_env() -> Option<(String, String)> {
    let url = std::env::var("RUSTYANT_DYNAMODB_URL").ok()?;
    let prefix = std::env::var("RUSTYANT_DYNAMODB_TABLE_PREFIX").unwrap_or_else(|_| DEFAULT_PREFIX.to_string());
    Some((url, prefix))
}

fn make_storage() -> Option<Arc<dyn Storage>> {
    let (url, prefix) = dynamodb_env()?;
    let creds = Credentials::new("test", "test", None, None, "ddb-test");
    let config = aws_sdk_dynamodb::config::Builder::new()
        .behavior_version(BehaviorVersion::latest())
        .credentials_provider(creds)
        .region(Region::new("us-east-1"))
        .endpoint_url(url)
        .build();
    let client = DynamoClient::from_conf(config);
    let tables = TableNames::with_prefix(&prefix);
    let backend = DynamoDbBackend::new(client, tables);
    Some(Arc::new(KVStorage::new(backend)))
}

/// Produce a unique key suffix per call: `{scope}-{pid}-{seq}-{tail}`.
/// Used to namespace each test's keys in the shared `DynamoDB` Local.
fn unique_key(scope: &str, tail: &str) -> String {
    let seq = SUFFIX_SEQ.fetch_add(1, Ordering::Relaxed);
    format!("ddb-{scope}-{}-{seq}-{tail}", std::process::id())
}

macro_rules! ddb_test {
    ($_scope:expr) => {{
        match make_storage() {
            Some(s) => s,
            None => {
                eprintln!("SKIP: RUSTYANT_DYNAMODB_URL not set");
                return;
            }
        }
    }};
}

#[tokio::test]
async fn ddb_string_roundtrip() {
    let storage = ddb_test!("string");
    let key = unique_key("string", "greeting");

    storage.set_string(&key, Bytes::from_static(b"hello"), None).await.expect("set");
    let got = storage.get_string(&key).await.expect("get");
    assert_eq!(got.as_deref(), Some(&b"hello"[..]));

    assert!(storage.delete(&key).await.expect("delete"));
    assert!(!storage.exists(&key).await.expect("exists"));
}

#[tokio::test]
async fn ddb_incr_persists_across_calls() {
    let storage = ddb_test!("incr");
    let key = unique_key("incr", "counter");

    assert_eq!(storage.incr_by(&key, 1).await.expect("incr"), 1);
    assert_eq!(storage.incr_by(&key, 5).await.expect("incr"), 6);
    assert_eq!(storage.incr_by(&key, -2).await.expect("incr"), 4);

    storage.delete(&key).await.expect("delete");
}

#[tokio::test]
async fn ddb_hash_roundtrip() {
    let storage = ddb_test!("hash");
    let key = unique_key("hash", "profile");

    let new = storage
        .hset(
            &key,
            vec![("name".to_string(), Bytes::from_static(b"alice")), ("age".to_string(), Bytes::from_static(b"30"))],
        )
        .await
        .expect("hset");
    assert_eq!(new, 2);

    let name = storage.hget(&key, "name").await.expect("hget");
    assert_eq!(name.as_deref(), Some(&b"alice"[..]));

    let all = storage.hgetall(&key).await.expect("hgetall");
    assert_eq!(all.len(), 2);

    let removed = storage.hdel(&key, &["name".to_string(), "missing".to_string()]).await.expect("hdel");
    assert_eq!(removed, 1);

    storage.delete(&key).await.expect("cleanup");
}

#[tokio::test]
async fn ddb_list_roundtrip() {
    let storage = ddb_test!("list");
    let key = unique_key("list", "queue");

    storage.list_push(&key, vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")], false).await.expect("rpush");
    storage.list_push(&key, vec![Bytes::from_static(b"zero")], true).await.expect("lpush");

    let range = storage.lrange(&key, 0, -1).await.expect("lrange");
    assert_eq!(range.len(), 3);
    assert_eq!(range[0].as_ref(), b"zero");
    assert_eq!(range[1].as_ref(), b"a");
    assert_eq!(range[2].as_ref(), b"b");

    let popped = storage.list_pop(&key, 2, true).await.expect("lpop");
    assert_eq!(popped.len(), 2);
    assert_eq!(popped[0].as_ref(), b"zero");
    assert_eq!(popped[1].as_ref(), b"a");

    storage.delete(&key).await.ok();
}

#[tokio::test]
async fn ddb_set_and_zset_roundtrip() {
    let storage = ddb_test!("set-zset");
    let set_key = unique_key("set-zset", "members");
    let zset_key = unique_key("set-zset", "scores");

    let added = storage.sadd(&set_key, vec!["alice".into(), "bob".into(), "alice".into()]).await.expect("sadd");
    assert_eq!(added, 2);

    let zadded = storage.zadd(&zset_key, vec![(10.0, "bob".into()), (5.0, "alice".into())]).await.expect("zadd");
    assert_eq!(zadded, 2);

    let ordered = storage.zrange(&zset_key, 0, -1).await.expect("zrange");
    assert_eq!(ordered, vec!["alice".to_string(), "bob".to_string()]);

    storage.delete(&set_key).await.ok();
    storage.delete(&zset_key).await.ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ddb_concurrent_incr_converges() {
    // DynamoDB enforces ConditionExpression natively, so this proves the
    // CAS retry loop end-to-end against real conditional-write semantics.
    // Compare with the S3 backend's `RUSTYANT_S3_CAS`-gated counterpart in
    // tests/floci.rs — that one only runs against real S3 because floci
    // ignores `If-Match`.
    let storage = ddb_test!("concurrent-incr");
    let key = unique_key("concurrent-incr", "counter");
    storage.delete(&key).await.ok();

    let mut handles = Vec::with_capacity(TASKS);
    for _ in 0..TASKS {
        let s = storage.clone();
        let k = key.clone();
        handles.push(tokio::spawn(async move { s.incr_by(&k, 1).await }));
    }
    for h in handles {
        h.await.expect("task").expect("incr_by ok");
    }

    let raw = storage.get_string(&key).await.expect("get").expect("some");
    let s = std::str::from_utf8(&raw).expect("utf8");
    let final_val: u64 = s.parse().expect("int");
    assert_eq!(final_val, TASKS as u64, "lost increments — CAS retry loop failed");

    storage.delete(&key).await.ok();
}

#[tokio::test]
async fn ddb_wrong_type_errors() {
    let storage = ddb_test!("wrong-type");
    let key = unique_key("wrong-type", "string-key");

    storage.set_string(&key, Bytes::from_static(b"v"), None).await.expect("set");

    // Reading the string key as a hash must fail with WrongType. The
    // message format follows storage::wrong_type.
    let err = storage.hget(&key, "field").await.expect_err("should error");
    let msg = format!("{err}");
    assert!(msg.contains("wrong type"), "expected WrongType, got {msg:?}");

    storage.delete(&key).await.ok();
}

#[tokio::test]
async fn ddb_setnx_first_writer_wins() {
    // Verifies the `WriteCondition::CreateOnly` path translates into a
    // DynamoDB `attribute_not_exists(pk)` ConditionExpression. SETNX is
    // the simplest commands.rs caller of that path.
    let storage = ddb_test!("setnx");
    let key = unique_key("setnx", "lock");

    let first = storage.set_string_nx(&key, Bytes::from_static(b"first"), None).await.expect("setnx1");
    let second = storage.set_string_nx(&key, Bytes::from_static(b"second"), None).await.expect("setnx2");
    assert!(first);
    assert!(!second);

    let got = storage.get_string(&key).await.expect("get");
    assert_eq!(got.as_deref(), Some(&b"first"[..]));

    storage.delete(&key).await.ok();
}
