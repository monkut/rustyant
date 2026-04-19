//! S3-backed `S3Storage` round-trip tests against a floci emulator.
//!
//! Gated on `RUSTYANT_FLOCI_URL`. Locally: `just floci-up && just floci-seed`
//! then `just test-floci`. CI: the `test` job in `.github/workflows/ci.yml`
//! runs floci as a service container and exports `RUSTYANT_FLOCI_URL` for
//! the whole suite.
//!
//! Each test uses a unique key prefix derived from its own name to avoid
//! collisions under nextest's parallel execution.

use std::sync::Arc;

use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use bytes::Bytes;
use rustyant::storage::{S3Storage, Storage};

const DEFAULT_BUCKET: &str = "rustyant-ci";

/// Worker count for the concurrent-INCR convergence test.
const TASKS: usize = 8;

fn floci_env() -> Option<(String, String)> {
    let url = std::env::var("RUSTYANT_FLOCI_URL").ok()?;
    let bucket = std::env::var("RUSTYANT_FLOCI_BUCKET").unwrap_or_else(|_| DEFAULT_BUCKET.to_string());
    Some((url, bucket))
}

fn make_storage(prefix: &str) -> Option<Arc<dyn Storage>> {
    let (url, bucket) = floci_env()?;
    let creds = Credentials::new("test", "test", None, None, "floci-test");
    let config = aws_sdk_s3::config::Builder::new()
        .behavior_version(BehaviorVersion::latest())
        .credentials_provider(creds)
        .region(Region::new("us-east-1"))
        .endpoint_url(url)
        .force_path_style(true)
        .build();
    let client = S3Client::from_conf(config);
    let storage = S3Storage::new(client, bucket, format!("floci-test/{prefix}/"));
    Some(Arc::new(storage))
}

macro_rules! floci_test {
    ($prefix:expr) => {{
        match make_storage($prefix) {
            Some(s) => s,
            None => {
                eprintln!("SKIP: RUSTYANT_FLOCI_URL not set");
                return;
            }
        }
    }};
}

#[tokio::test]
async fn s3_string_roundtrip() {
    let storage = floci_test!("string-roundtrip");
    let key = "greeting";

    storage.set_string(key, Bytes::from_static(b"hello"), None).await.expect("set");
    let got = storage.get_string(key).await.expect("get");
    assert_eq!(got.as_deref(), Some(&b"hello"[..]));

    assert!(storage.delete(key).await.expect("delete"));
    assert!(!storage.exists(key).await.expect("exists"));
}

#[tokio::test]
async fn s3_incr_persists_across_calls() {
    let storage = floci_test!("incr");
    let key = "counter";

    assert_eq!(storage.incr_by(key, 1).await.expect("incr"), 1);
    assert_eq!(storage.incr_by(key, 5).await.expect("incr"), 6);
    assert_eq!(storage.incr_by(key, -2).await.expect("incr"), 4);

    storage.delete(key).await.expect("delete");
}

#[tokio::test]
async fn s3_hash_roundtrip() {
    let storage = floci_test!("hash");
    let key = "profile";

    let new = storage
        .hset(
            key,
            vec![("name".to_string(), Bytes::from_static(b"alice")), ("age".to_string(), Bytes::from_static(b"30"))],
        )
        .await
        .expect("hset");
    assert_eq!(new, 2);

    let name = storage.hget(key, "name").await.expect("hget");
    assert_eq!(name.as_deref(), Some(&b"alice"[..]));

    let all = storage.hgetall(key).await.expect("hgetall");
    assert_eq!(all.len(), 2);

    let removed = storage.hdel(key, &["name".to_string(), "missing".to_string()]).await.expect("hdel");
    assert_eq!(removed, 1);

    storage.delete(key).await.expect("cleanup");
}

#[tokio::test]
async fn s3_list_roundtrip() {
    let storage = floci_test!("list");
    let key = "queue";

    storage.list_push(key, vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")], false).await.expect("rpush");
    storage.list_push(key, vec![Bytes::from_static(b"zero")], true).await.expect("lpush");

    let range = storage.lrange(key, 0, -1).await.expect("lrange");
    assert_eq!(range.len(), 3);
    assert_eq!(range[0].as_ref(), b"zero");
    assert_eq!(range[1].as_ref(), b"a");
    assert_eq!(range[2].as_ref(), b"b");

    let popped = storage.list_pop(key, 2, true).await.expect("lpop");
    assert_eq!(popped.len(), 2);
    assert_eq!(popped[0].as_ref(), b"zero");
    assert_eq!(popped[1].as_ref(), b"a");

    storage.delete(key).await.ok();
}

#[tokio::test]
async fn s3_set_and_zset_roundtrip() {
    let storage = floci_test!("set-zset");
    let set_key = "members";
    let zset_key = "scores";

    let added = storage.sadd(set_key, vec!["alice".into(), "bob".into(), "alice".into()]).await.expect("sadd");
    assert_eq!(added, 2);

    let zadded = storage.zadd(zset_key, vec![(10.0, "bob".into()), (5.0, "alice".into())]).await.expect("zadd");
    assert_eq!(zadded, 2);

    let ordered = storage.zrange(zset_key, 0, -1).await.expect("zrange");
    assert_eq!(ordered, vec!["alice".to_string(), "bob".to_string()]);

    storage.delete(set_key).await.ok();
    storage.delete(zset_key).await.ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s3_concurrent_incr_converges() {
    // Proves the CAS retry loop: N parallel INCR-by-1 tasks must each
    // succeed and the final value must equal N. Under last-writer-wins
    // semantics (pre-CAS) this test routinely lost increments.
    //
    // Gated on `RUSTYANT_S3_CAS=1` because floci silently ignores
    // `If-Match` / `If-None-Match` headers — a PUT against floci with a
    // deliberately wrong ETag returns 200 with a fresh ETag, not 412.
    // Real AWS S3 (which enforces conditional writes since November 2024)
    // is required to actually validate this path. Set the env var once
    // you're pointing at a real S3 endpoint.
    let storage = floci_test!("concurrent-incr");
    if std::env::var("RUSTYANT_S3_CAS").is_err() {
        eprintln!("SKIP: RUSTYANT_S3_CAS not set (floci does not enforce If-Match)");
        return;
    }

    let key = "counter";
    storage.delete(key).await.ok();

    let mut handles = Vec::with_capacity(TASKS);
    for _ in 0..TASKS {
        let s = storage.clone();
        let k = key.to_string();
        handles.push(tokio::spawn(async move { s.incr_by(&k, 1).await }));
    }
    for h in handles {
        h.await.expect("task").expect("incr_by ok");
    }

    let raw = storage.get_string(key).await.expect("get").expect("some");
    let s = std::str::from_utf8(&raw).expect("utf8");
    let final_val: u64 = s.parse().expect("int");
    assert_eq!(final_val, TASKS as u64, "lost increments — CAS retry loop failed");

    storage.delete(key).await.ok();
}

#[tokio::test]
async fn s3_wrong_type_errors() {
    let storage = floci_test!("wrong-type");
    let key = "string-key";

    storage.set_string(key, Bytes::from_static(b"v"), None).await.expect("set");

    // Reading the string key as a hash must fail with WrongType.
    let err = storage.hget(key, "field").await.expect_err("should error");
    let msg = format!("{err}");
    assert!(msg.contains("wrong type"), "expected WrongType, got {msg:?}");

    storage.delete(key).await.ok();
}
