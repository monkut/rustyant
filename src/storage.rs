use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::primitives::ByteStream;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::error::RustyAntError;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct StoredValue {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at_ms: Option<i64>,
    #[serde(flatten)]
    pub value: Value,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "type", content = "data")]
pub enum Value {
    String(Vec<u8>),
    Hash(BTreeMap<String, Vec<u8>>),
    List(Vec<Vec<u8>>),
    Set(BTreeSet<String>),
    ZSet(BTreeMap<String, f64>),
}

#[derive(Debug)]
pub enum TtlResult {
    NoKey,
    NoExpire,
    Ms(i64),
}

/// Score-bound for `ZRANGEBYSCORE` matching Redis's syntax: bare number is
/// inclusive, `(N` is exclusive, `+inf` / `-inf` are the extremes.
#[derive(Debug, Clone, Copy)]
pub enum ScoreBound {
    Inclusive(f64),
    Exclusive(f64),
    MinusInf,
    PlusInf,
}

impl ScoreBound {
    pub fn parse(s: &str) -> Result<Self, RustyAntError> {
        if s == "-inf" {
            return Ok(Self::MinusInf);
        }
        if s == "+inf" || s == "inf" {
            return Ok(Self::PlusInf);
        }
        if let Some(rest) = s.strip_prefix('(') {
            let v: f64 = rest.parse().map_err(|_| RustyAntError::Parse("score is not a float".into()))?;
            return Ok(Self::Exclusive(v));
        }
        let v: f64 = s.parse().map_err(|_| RustyAntError::Parse("score is not a float".into()))?;
        Ok(Self::Inclusive(v))
    }

    fn ge_min(self, score: f64) -> bool {
        match self {
            Self::Inclusive(v) => score >= v,
            Self::Exclusive(v) => score > v,
            Self::MinusInf => true,
            Self::PlusInf => false,
        }
    }

    fn le_max(self, score: f64) -> bool {
        match self {
            Self::Inclusive(v) => score <= v,
            Self::Exclusive(v) => score < v,
            Self::MinusInf => false,
            Self::PlusInf => true,
        }
    }
}

pub fn now_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}

fn is_expired(v: &StoredValue) -> bool {
    v.expires_at_ms.is_some_and(|exp| exp <= now_ms())
}

fn resolve_range(len: i64, start: i64, stop: i64) -> Option<(usize, usize)> {
    if len == 0 {
        return None;
    }
    let norm = |i: i64| -> i64 { if i < 0 { (len + i).max(0) } else { i.min(len - 1) } };
    let s = norm(start);
    let e = norm(stop);
    if s > e {
        return None;
    }
    Some((usize::try_from(s).unwrap_or(0), usize::try_from(e).unwrap_or(0)))
}

fn wrong_type(key: &str) -> RustyAntError {
    RustyAntError::WrongType { key: key.to_string() }
}

/// Redis `TYPE` reply tag for a stored value.
const fn value_kind(value: &Value) -> &'static str {
    match value {
        Value::String(_) => "string",
        Value::Hash(_) => "hash",
        Value::List(_) => "list",
        Value::Set(_) => "set",
        Value::ZSet(_) => "zset",
    }
}

/// Remove up to `count` occurrences of `target` from `list`. Redis semantics:
/// count > 0 removes from head, count < 0 removes from tail, count == 0
/// removes all. Returns the number of elements removed.
fn remove_list_occurrences(list: &mut Vec<Vec<u8>>, target: &[u8], count: i64) -> i64 {
    let mut removed: i64 = 0;
    if count >= 0 {
        let max = if count == 0 { i64::MAX } else { count };
        let mut i = 0;
        while i < list.len() && removed < max {
            if list[i].as_slice() == target {
                list.remove(i);
                removed += 1;
            } else {
                i += 1;
            }
        }
    } else {
        let max = -count;
        let mut i = list.len();
        while i > 0 && removed < max {
            i -= 1;
            if list[i].as_slice() == target {
                list.remove(i);
                removed += 1;
            }
        }
    }
    removed
}

/// Sort a `ZSet` by (score asc, member asc), then filter by the min/max
/// bounds. Shared between `S3Storage` and `InMemoryStorage`.
fn filter_zset_by_score(map: BTreeMap<String, f64>, min: ScoreBound, max: ScoreBound) -> Vec<String> {
    let mut sorted: Vec<(String, f64)> = map.into_iter().collect();
    sorted.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal).then_with(|| a.0.cmp(&b.0)));
    sorted.into_iter().filter(|(_, s)| min.ge_min(*s) && max.le_max(*s)).map(|(m, _)| m).collect()
}

// ---------------------------------------------------------------------------
// S3 optimistic-locking primitives.
//
// Every read-modify-write (INCR / HSET / HDEL / LPUSH / RPUSH / LPOP / RPOP /
// SADD / ZADD / EXPIRE) follows the same pattern: load the entry with its
// ETag, compute the new entry locally, then conditionally PutObject with
// If-Match: <etag> (or If-None-Match: * for creates). A 412 Precondition
// Failed means another writer landed first; we back off briefly and retry.
//
// After `MAX_CAS_RETRIES` unsuccessful attempts we return
// `RustyAntError::Contention`, which the command layer surfaces as RESP
// `-ERR`. In practice conflicts resolve within one retry under typical load.
// ---------------------------------------------------------------------------

const MAX_CAS_RETRIES: u32 = 5;

#[derive(Debug)]
enum CasCondition {
    CreateOnly,
    IfMatch(String),
}

/// Decision emitted by a CAS modify closure.
enum CasAction<R> {
    /// Write the given entry under the CAS condition, return `R` on success.
    Write(StoredValue, R),
    /// Unconditionally delete the key (used when a mutation empties a
    /// collection — Redis semantics require the key to disappear).
    Delete(R),
    /// No write needed; return `R` immediately.
    NoOp(R),
}

async fn cas_backoff(attempt: u32) {
    if attempt == 0 {
        return;
    }
    let shift = (attempt - 1).min(4);
    let ms = 10u64 * (1u64 << shift);
    tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
}

// ---------------------------------------------------------------------------
// Storage trait — defines the command-facing persistence API. Both the
// production S3-backed storage and the test in-memory storage implement this.
// ---------------------------------------------------------------------------

#[async_trait]
pub trait Storage: Send + Sync + std::fmt::Debug {
    async fn delete(&self, key: &str) -> Result<bool, RustyAntError>;
    async fn exists(&self, key: &str) -> Result<bool, RustyAntError>;
    async fn expire_at(&self, key: &str, expires_at_ms: i64) -> Result<bool, RustyAntError>;
    async fn ttl_ms(&self, key: &str) -> Result<TtlResult, RustyAntError>;
    /// Redis `TYPE` — returns the value-kind tag (`"string"`, `"hash"`,
    /// `"list"`, `"set"`, `"zset"`) or `None` when the key is missing/expired.
    async fn kind(&self, key: &str) -> Result<Option<&'static str>, RustyAntError>;

    async fn get_string(&self, key: &str) -> Result<Option<Bytes>, RustyAntError>;
    async fn set_string(&self, key: &str, value: Bytes, expires_at_ms: Option<i64>) -> Result<(), RustyAntError>;
    async fn set_string_nx(&self, key: &str, value: Bytes, expires_at_ms: Option<i64>) -> Result<bool, RustyAntError>;
    async fn getset(&self, key: &str, value: Bytes) -> Result<Option<Bytes>, RustyAntError>;
    async fn incr_by(&self, key: &str, delta: i64) -> Result<i64, RustyAntError>;
    async fn persist(&self, key: &str) -> Result<bool, RustyAntError>;

    /// Default: sequential `get_string` per key. Impls may override with a
    /// batched S3 request.
    async fn mget(&self, keys: &[String]) -> Result<Vec<Option<Bytes>>, RustyAntError> {
        let mut out = Vec::with_capacity(keys.len());
        for k in keys {
            out.push(self.get_string(k).await?);
        }
        Ok(out)
    }

    /// Default: sequential `set_string` per pair. Not atomic across keys —
    /// a failure midway leaves some keys set. Real Redis is atomic; that
    /// semantic isn't worth emulating over S3 without a dedicated transaction
    /// log, and the S3 backing makes the fire-and-forget variant fast enough.
    async fn mset(&self, pairs: Vec<(String, Bytes)>) -> Result<(), RustyAntError> {
        for (k, v) in pairs {
            self.set_string(&k, v, None).await?;
        }
        Ok(())
    }

    async fn hset(&self, key: &str, pairs: Vec<(String, Bytes)>) -> Result<i64, RustyAntError>;
    async fn hget(&self, key: &str, field: &str) -> Result<Option<Bytes>, RustyAntError>;
    async fn hdel(&self, key: &str, fields: &[String]) -> Result<i64, RustyAntError>;
    async fn hgetall(&self, key: &str) -> Result<Vec<(String, Bytes)>, RustyAntError>;
    async fn hlen(&self, key: &str) -> Result<i64, RustyAntError>;
    async fn hkeys(&self, key: &str) -> Result<Vec<String>, RustyAntError>;
    async fn hvals(&self, key: &str) -> Result<Vec<Bytes>, RustyAntError>;
    async fn hexists(&self, key: &str, field: &str) -> Result<bool, RustyAntError>;
    async fn hmget(&self, key: &str, fields: &[String]) -> Result<Vec<Option<Bytes>>, RustyAntError>;
    async fn hincr_by(&self, key: &str, field: &str, delta: i64) -> Result<i64, RustyAntError>;

    async fn list_push(&self, key: &str, values: Vec<Bytes>, left: bool) -> Result<i64, RustyAntError>;
    async fn list_pop(&self, key: &str, count: usize, left: bool) -> Result<Vec<Bytes>, RustyAntError>;
    async fn lrange(&self, key: &str, start: i64, stop: i64) -> Result<Vec<Bytes>, RustyAntError>;
    async fn llen(&self, key: &str) -> Result<i64, RustyAntError>;
    async fn lindex(&self, key: &str, index: i64) -> Result<Option<Bytes>, RustyAntError>;
    async fn lset(&self, key: &str, index: i64, value: Bytes) -> Result<(), RustyAntError>;
    async fn lrem(&self, key: &str, count: i64, value: Bytes) -> Result<i64, RustyAntError>;

    async fn sadd(&self, key: &str, members: Vec<String>) -> Result<i64, RustyAntError>;
    async fn srem(&self, key: &str, members: &[String]) -> Result<i64, RustyAntError>;
    async fn smembers(&self, key: &str) -> Result<Vec<String>, RustyAntError>;
    async fn sismember(&self, key: &str, member: &str) -> Result<bool, RustyAntError>;
    async fn scard(&self, key: &str) -> Result<i64, RustyAntError>;

    async fn zadd(&self, key: &str, pairs: Vec<(f64, String)>) -> Result<i64, RustyAntError>;
    async fn zrem(&self, key: &str, members: &[String]) -> Result<i64, RustyAntError>;
    async fn zincr_by(&self, key: &str, member: &str, delta: f64) -> Result<f64, RustyAntError>;
    async fn zrange(&self, key: &str, start: i64, stop: i64) -> Result<Vec<String>, RustyAntError>;
    async fn zrangebyscore(&self, key: &str, min: ScoreBound, max: ScoreBound) -> Result<Vec<String>, RustyAntError>;
    async fn zscore(&self, key: &str, member: &str) -> Result<Option<f64>, RustyAntError>;
    async fn zcard(&self, key: &str) -> Result<i64, RustyAntError>;

    /// Return every key matching `pattern` (Redis-style glob: `*`, `?`).
    /// On S3 this fans out to repeated `ListObjectsV2` calls until the
    /// prefix is exhausted; on large keyspaces prefer `scan`.
    async fn keys(&self, pattern: &str) -> Result<Vec<String>, RustyAntError>;

    /// Incremental key iteration. `cursor=None` starts a fresh scan; a
    /// `Some(token)` return means more pages remain, `None` means the
    /// scan is exhausted. `count` bounds the batch size.
    async fn scan(
        &self,
        cursor: Option<&str>,
        pattern: Option<&str>,
        count: usize,
    ) -> Result<(Vec<String>, Option<String>), RustyAntError>;
}

// ---------------------------------------------------------------------------
// S3-backed storage — one S3 object per Redis key under `${prefix}${key}`.
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct S3Storage {
    client: S3Client,
    bucket: String,
    prefix: String,
}

impl S3Storage {
    #[must_use]
    pub const fn new(client: S3Client, bucket: String, prefix: String) -> Self {
        Self { client, bucket, prefix }
    }

    fn key(&self, redis_key: &str) -> String {
        format!("{}{}", self.prefix, redis_key)
    }

    /// Load the current entry and its `ETag`. Returns `None` for missing or
    /// expired keys (expired keys are deleted best-effort here).
    async fn load_with_etag(&self, redis_key: &str) -> Result<Option<(StoredValue, String)>, RustyAntError> {
        let res = self.client.get_object().bucket(&self.bucket).key(self.key(redis_key)).send().await;
        match res {
            Ok(output) => {
                let etag = output.e_tag().unwrap_or("").to_string();
                let bytes = output
                    .body
                    .collect()
                    .await
                    .map_err(|e| RustyAntError::S3(format!("collect body: {e}")))?
                    .into_bytes();
                let entry: StoredValue = serde_json::from_slice(&bytes)?;
                if is_expired(&entry) {
                    // Best-effort GC. Swallowing the error is OK — the next
                    // access will notice the expiry and try again.
                    let _ = self.delete_raw(redis_key).await;
                    return Ok(None);
                }
                Ok(Some((entry, etag)))
            }
            Err(e) => {
                let svc = e.into_service_error();
                if svc.is_no_such_key() { Ok(None) } else { Err(RustyAntError::S3(svc.to_string())) }
            }
        }
    }

    /// Convenience for read-only callers that don't need the `ETag`.
    async fn load(&self, redis_key: &str) -> Result<Option<StoredValue>, RustyAntError> {
        Ok(self.load_with_etag(redis_key).await?.map(|(e, _)| e))
    }

    /// Unconditional PUT. Used for `set_string`, which has overwrite
    /// semantics and does not need CAS.
    async fn save(&self, redis_key: &str, entry: &StoredValue) -> Result<(), RustyAntError> {
        let body = serde_json::to_vec(entry)?;
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(self.key(redis_key))
            .body(ByteStream::from(body))
            .content_type("application/json")
            .send()
            .await
            .map_err(|e| RustyAntError::S3(e.to_string()))?;
        Ok(())
    }

    /// Conditional PUT. Returns `Err(Contention)` on HTTP 412, which the
    /// CAS retry loop turns into another read-modify-write attempt.
    async fn save_cas(&self, redis_key: &str, entry: &StoredValue, cond: CasCondition) -> Result<(), RustyAntError> {
        let body = serde_json::to_vec(entry)?;
        let mut req = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(self.key(redis_key))
            .body(ByteStream::from(body))
            .content_type("application/json");
        match cond {
            CasCondition::CreateOnly => req = req.if_none_match("*"),
            CasCondition::IfMatch(etag) => req = req.if_match(etag),
        }
        match req.send().await {
            Ok(_) => Ok(()),
            Err(e) => {
                let is_412 = e.raw_response().is_some_and(|r| r.status().as_u16() == 412);
                if is_412 {
                    Err(RustyAntError::Contention)
                } else {
                    Err(RustyAntError::S3(e.into_service_error().to_string()))
                }
            }
        }
    }

    async fn delete_raw(&self, redis_key: &str) -> Result<(), RustyAntError> {
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(self.key(redis_key))
            .send()
            .await
            .map_err(|e| RustyAntError::S3(e.to_string()))?;
        Ok(())
    }

    /// Conditional DELETE. Returns `Err(Contention)` on HTTP 412, which the
    /// CAS retry loop turns into another read-modify-write attempt. Used when
    /// a mutation empties a collection (HDEL/LPOP/RPOP/SREM/ZREM of the last
    /// member) so an unrelated concurrent writer's new value isn't clobbered.
    async fn delete_if_match(&self, redis_key: &str, etag: &str) -> Result<(), RustyAntError> {
        let res = self.client.delete_object().bucket(&self.bucket).key(self.key(redis_key)).if_match(etag).send().await;
        match res {
            Ok(_) => Ok(()),
            Err(e) => {
                let is_412 = e.raw_response().is_some_and(|r| r.status().as_u16() == 412);
                if is_412 {
                    Err(RustyAntError::Contention)
                } else {
                    Err(RustyAntError::S3(e.into_service_error().to_string()))
                }
            }
        }
    }

    /// Read-modify-write helper: runs `modify` against the latest entry,
    /// writes the result back under ETag-based optimistic locking, retrying
    /// up to `MAX_CAS_RETRIES` times on contention.
    async fn cas<F, R>(&self, redis_key: &str, mut modify: F) -> Result<R, RustyAntError>
    where
        F: FnMut(Option<&StoredValue>) -> Result<CasAction<R>, RustyAntError>,
    {
        for attempt in 0..MAX_CAS_RETRIES {
            cas_backoff(attempt).await;
            let loaded = self.load_with_etag(redis_key).await?;
            let (existing, etag) = match &loaded {
                Some((e, t)) => (Some(e), Some(t.clone())),
                None => (None, None),
            };
            match modify(existing)? {
                CasAction::NoOp(r) => return Ok(r),
                CasAction::Delete(r) => match etag {
                    Some(e) => match self.delete_if_match(redis_key, &e).await {
                        Ok(()) => return Ok(r),
                        Err(RustyAntError::Contention) => (),
                        Err(err) => return Err(err),
                    },
                    None => return Ok(r),
                },
                CasAction::Write(new_entry, r) => {
                    let cond = etag.map_or(CasCondition::CreateOnly, CasCondition::IfMatch);
                    match self.save_cas(redis_key, &new_entry, cond).await {
                        Ok(()) => return Ok(r),
                        Err(RustyAntError::Contention) => {}
                        Err(e) => return Err(e),
                    }
                }
            }
        }
        Err(RustyAntError::Contention)
    }
}

#[async_trait]
impl Storage for S3Storage {
    async fn delete(&self, redis_key: &str) -> Result<bool, RustyAntError> {
        let existed = self.load(redis_key).await?.is_some();
        if existed {
            self.delete_raw(redis_key).await?;
        }
        Ok(existed)
    }

    async fn exists(&self, key: &str) -> Result<bool, RustyAntError> {
        Ok(self.load(key).await?.is_some())
    }

    async fn kind(&self, key: &str) -> Result<Option<&'static str>, RustyAntError> {
        Ok(self.load(key).await?.map(|v| value_kind(&v.value)))
    }

    async fn expire_at(&self, key: &str, expires_at_ms: i64) -> Result<bool, RustyAntError> {
        self.cas(key, move |entry| {
            entry.map_or(Ok(CasAction::NoOp(false)), |existing| {
                let mut new_entry = existing.clone();
                new_entry.expires_at_ms = Some(expires_at_ms);
                Ok(CasAction::Write(new_entry, true))
            })
        })
        .await
    }

    async fn ttl_ms(&self, key: &str) -> Result<TtlResult, RustyAntError> {
        let Some(v) = self.load(key).await? else {
            return Ok(TtlResult::NoKey);
        };
        Ok(v.expires_at_ms.map_or(TtlResult::NoExpire, |exp| TtlResult::Ms((exp - now_ms()).max(0))))
    }

    async fn get_string(&self, key: &str) -> Result<Option<Bytes>, RustyAntError> {
        match self.load(key).await? {
            Some(StoredValue { value: Value::String(data), .. }) => Ok(Some(Bytes::from(data))),
            Some(_) => Err(wrong_type(key)),
            None => Ok(None),
        }
    }

    async fn set_string(&self, key: &str, value: Bytes, expires_at_ms: Option<i64>) -> Result<(), RustyAntError> {
        self.save(key, &StoredValue { expires_at_ms, value: Value::String(value.to_vec()) }).await
    }

    async fn set_string_nx(&self, key: &str, value: Bytes, expires_at_ms: Option<i64>) -> Result<bool, RustyAntError> {
        // Surface any expired entry so `If-None-Match: *` doesn't reject
        // a legitimate create because the zombie object hasn't been swept yet.
        let _ = self.load_with_etag(key).await?;
        let entry = StoredValue { expires_at_ms, value: Value::String(value.to_vec()) };
        match self.save_cas(key, &entry, CasCondition::CreateOnly).await {
            Ok(()) => Ok(true),
            Err(RustyAntError::Contention) => Ok(false),
            Err(e) => Err(e),
        }
    }

    async fn incr_by(&self, key: &str, delta: i64) -> Result<i64, RustyAntError> {
        self.cas(key, move |entry| {
            let (current, expires_at_ms) = match entry {
                Some(StoredValue { value: Value::String(data), expires_at_ms }) => {
                    let s = std::str::from_utf8(data)
                        .map_err(|_| RustyAntError::Parse("value is not an integer".into()))?;
                    let n: i64 = s.parse().map_err(|_| RustyAntError::Parse("value is not an integer".into()))?;
                    (n, *expires_at_ms)
                }
                Some(_) => return Err(wrong_type(key)),
                None => (0, None),
            };
            let new_val =
                current.checked_add(delta).ok_or_else(|| RustyAntError::Parse("increment overflow".into()))?;
            let new_entry = StoredValue { expires_at_ms, value: Value::String(new_val.to_string().into_bytes()) };
            Ok(CasAction::Write(new_entry, new_val))
        })
        .await
    }

    async fn hset(&self, key: &str, pairs: Vec<(String, Bytes)>) -> Result<i64, RustyAntError> {
        self.cas(key, move |entry| {
            let (mut map, expires_at_ms) = match entry {
                Some(StoredValue { value: Value::Hash(m), expires_at_ms }) => (m.clone(), *expires_at_ms),
                Some(_) => return Err(wrong_type(key)),
                None => (BTreeMap::new(), None),
            };
            let mut new_fields: i64 = 0;
            for (field, value) in &pairs {
                if !map.contains_key(field) {
                    new_fields += 1;
                }
                map.insert(field.clone(), value.to_vec());
            }
            let new_entry = StoredValue { expires_at_ms, value: Value::Hash(map) };
            Ok(CasAction::Write(new_entry, new_fields))
        })
        .await
    }

    async fn hget(&self, key: &str, field: &str) -> Result<Option<Bytes>, RustyAntError> {
        match self.load(key).await? {
            Some(StoredValue { value: Value::Hash(m), .. }) => Ok(m.get(field).map(|v| Bytes::from(v.clone()))),
            Some(_) => Err(wrong_type(key)),
            None => Ok(None),
        }
    }

    async fn hdel(&self, key: &str, fields: &[String]) -> Result<i64, RustyAntError> {
        let fields = fields.to_vec();
        self.cas(key, move |entry| {
            let (mut map, expires_at_ms) = match entry {
                Some(StoredValue { value: Value::Hash(m), expires_at_ms }) => (m.clone(), *expires_at_ms),
                Some(_) => return Err(wrong_type(key)),
                None => return Ok(CasAction::NoOp(0)),
            };
            let mut removed: i64 = 0;
            for f in &fields {
                if map.remove(f).is_some() {
                    removed += 1;
                }
            }
            if map.is_empty() {
                Ok(CasAction::Delete(removed))
            } else {
                let new_entry = StoredValue { expires_at_ms, value: Value::Hash(map) };
                Ok(CasAction::Write(new_entry, removed))
            }
        })
        .await
    }

    async fn hgetall(&self, key: &str) -> Result<Vec<(String, Bytes)>, RustyAntError> {
        match self.load(key).await? {
            Some(StoredValue { value: Value::Hash(m), .. }) => {
                Ok(m.into_iter().map(|(k, v)| (k, Bytes::from(v))).collect())
            }
            Some(_) => Err(wrong_type(key)),
            None => Ok(Vec::new()),
        }
    }

    async fn list_push(&self, key: &str, values: Vec<Bytes>, left: bool) -> Result<i64, RustyAntError> {
        self.cas(key, move |entry| {
            let (mut list, expires_at_ms) = match entry {
                Some(StoredValue { value: Value::List(l), expires_at_ms }) => (l.clone(), *expires_at_ms),
                Some(_) => return Err(wrong_type(key)),
                None => (Vec::new(), None),
            };
            for v in &values {
                if left {
                    list.insert(0, v.to_vec());
                } else {
                    list.push(v.to_vec());
                }
            }
            let len = i64::try_from(list.len()).unwrap_or(i64::MAX);
            let new_entry = StoredValue { expires_at_ms, value: Value::List(list) };
            Ok(CasAction::Write(new_entry, len))
        })
        .await
    }

    async fn list_pop(&self, key: &str, count: usize, left: bool) -> Result<Vec<Bytes>, RustyAntError> {
        self.cas(key, move |entry| {
            let (mut list, expires_at_ms) = match entry {
                Some(StoredValue { value: Value::List(l), expires_at_ms }) => (l.clone(), *expires_at_ms),
                Some(_) => return Err(wrong_type(key)),
                None => return Ok(CasAction::NoOp(Vec::new())),
            };
            let take = count.min(list.len());
            let mut out: Vec<Bytes> = Vec::with_capacity(take);
            for _ in 0..take {
                if left {
                    out.push(Bytes::from(list.remove(0)));
                } else {
                    out.push(Bytes::from(list.pop().expect("len checked above")));
                }
            }
            if list.is_empty() {
                Ok(CasAction::Delete(out))
            } else {
                let new_entry = StoredValue { expires_at_ms, value: Value::List(list) };
                Ok(CasAction::Write(new_entry, out))
            }
        })
        .await
    }

    async fn lrange(&self, key: &str, start: i64, stop: i64) -> Result<Vec<Bytes>, RustyAntError> {
        let list = match self.load(key).await? {
            Some(StoredValue { value: Value::List(l), .. }) => l,
            Some(_) => return Err(wrong_type(key)),
            None => return Ok(Vec::new()),
        };
        let len = i64::try_from(list.len()).unwrap_or(i64::MAX);
        let Some((s, e)) = resolve_range(len, start, stop) else {
            return Ok(Vec::new());
        };
        Ok(list[s..=e].iter().map(|v| Bytes::from(v.clone())).collect())
    }

    async fn sadd(&self, key: &str, members: Vec<String>) -> Result<i64, RustyAntError> {
        self.cas(key, move |entry| {
            let (mut set, expires_at_ms) = match entry {
                Some(StoredValue { value: Value::Set(s), expires_at_ms }) => (s.clone(), *expires_at_ms),
                Some(_) => return Err(wrong_type(key)),
                None => (BTreeSet::new(), None),
            };
            let mut added: i64 = 0;
            for m in &members {
                if set.insert(m.clone()) {
                    added += 1;
                }
            }
            let new_entry = StoredValue { expires_at_ms, value: Value::Set(set) };
            Ok(CasAction::Write(new_entry, added))
        })
        .await
    }

    async fn zadd(&self, key: &str, pairs: Vec<(f64, String)>) -> Result<i64, RustyAntError> {
        self.cas(key, move |entry| {
            let (mut map, expires_at_ms) = match entry {
                Some(StoredValue { value: Value::ZSet(m), expires_at_ms }) => (m.clone(), *expires_at_ms),
                Some(_) => return Err(wrong_type(key)),
                None => (BTreeMap::new(), None),
            };
            let mut added: i64 = 0;
            for (score, member) in &pairs {
                if !map.contains_key(member) {
                    added += 1;
                }
                map.insert(member.clone(), *score);
            }
            let new_entry = StoredValue { expires_at_ms, value: Value::ZSet(map) };
            Ok(CasAction::Write(new_entry, added))
        })
        .await
    }

    async fn zrange(&self, key: &str, start: i64, stop: i64) -> Result<Vec<String>, RustyAntError> {
        let map = match self.load(key).await? {
            Some(StoredValue { value: Value::ZSet(m), .. }) => m,
            Some(_) => return Err(wrong_type(key)),
            None => return Ok(Vec::new()),
        };
        let mut sorted: Vec<(String, f64)> = map.into_iter().collect();
        sorted.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal).then_with(|| a.0.cmp(&b.0)));
        let len = i64::try_from(sorted.len()).unwrap_or(i64::MAX);
        let Some((s, e)) = resolve_range(len, start, stop) else {
            return Ok(Vec::new());
        };
        Ok(sorted[s..=e].iter().map(|(m, _)| m.clone()).collect())
    }

    async fn hlen(&self, key: &str) -> Result<i64, RustyAntError> {
        match self.load(key).await? {
            Some(StoredValue { value: Value::Hash(m), .. }) => Ok(i64::try_from(m.len()).unwrap_or(i64::MAX)),
            Some(_) => Err(wrong_type(key)),
            None => Ok(0),
        }
    }

    async fn hkeys(&self, key: &str) -> Result<Vec<String>, RustyAntError> {
        match self.load(key).await? {
            Some(StoredValue { value: Value::Hash(m), .. }) => Ok(m.into_keys().collect()),
            Some(_) => Err(wrong_type(key)),
            None => Ok(Vec::new()),
        }
    }

    async fn hvals(&self, key: &str) -> Result<Vec<Bytes>, RustyAntError> {
        match self.load(key).await? {
            Some(StoredValue { value: Value::Hash(m), .. }) => Ok(m.into_values().map(Bytes::from).collect()),
            Some(_) => Err(wrong_type(key)),
            None => Ok(Vec::new()),
        }
    }

    async fn hexists(&self, key: &str, field: &str) -> Result<bool, RustyAntError> {
        match self.load(key).await? {
            Some(StoredValue { value: Value::Hash(m), .. }) => Ok(m.contains_key(field)),
            Some(_) => Err(wrong_type(key)),
            None => Ok(false),
        }
    }

    async fn hmget(&self, key: &str, fields: &[String]) -> Result<Vec<Option<Bytes>>, RustyAntError> {
        match self.load(key).await? {
            Some(StoredValue { value: Value::Hash(m), .. }) => {
                Ok(fields.iter().map(|f| m.get(f).map(|v| Bytes::from(v.clone()))).collect())
            }
            Some(_) => Err(wrong_type(key)),
            None => Ok(fields.iter().map(|_| None).collect()),
        }
    }

    async fn llen(&self, key: &str) -> Result<i64, RustyAntError> {
        match self.load(key).await? {
            Some(StoredValue { value: Value::List(l), .. }) => Ok(i64::try_from(l.len()).unwrap_or(i64::MAX)),
            Some(_) => Err(wrong_type(key)),
            None => Ok(0),
        }
    }

    async fn smembers(&self, key: &str) -> Result<Vec<String>, RustyAntError> {
        match self.load(key).await? {
            Some(StoredValue { value: Value::Set(s), .. }) => Ok(s.into_iter().collect()),
            Some(_) => Err(wrong_type(key)),
            None => Ok(Vec::new()),
        }
    }

    async fn sismember(&self, key: &str, member: &str) -> Result<bool, RustyAntError> {
        match self.load(key).await? {
            Some(StoredValue { value: Value::Set(s), .. }) => Ok(s.contains(member)),
            Some(_) => Err(wrong_type(key)),
            None => Ok(false),
        }
    }

    async fn scard(&self, key: &str) -> Result<i64, RustyAntError> {
        match self.load(key).await? {
            Some(StoredValue { value: Value::Set(s), .. }) => Ok(i64::try_from(s.len()).unwrap_or(i64::MAX)),
            Some(_) => Err(wrong_type(key)),
            None => Ok(0),
        }
    }

    async fn zscore(&self, key: &str, member: &str) -> Result<Option<f64>, RustyAntError> {
        match self.load(key).await? {
            Some(StoredValue { value: Value::ZSet(m), .. }) => Ok(m.get(member).copied()),
            Some(_) => Err(wrong_type(key)),
            None => Ok(None),
        }
    }

    async fn zcard(&self, key: &str) -> Result<i64, RustyAntError> {
        match self.load(key).await? {
            Some(StoredValue { value: Value::ZSet(m), .. }) => Ok(i64::try_from(m.len()).unwrap_or(i64::MAX)),
            Some(_) => Err(wrong_type(key)),
            None => Ok(0),
        }
    }

    async fn getset(&self, key: &str, value: Bytes) -> Result<Option<Bytes>, RustyAntError> {
        self.cas(key, move |entry| {
            let old = match entry {
                Some(StoredValue { value: Value::String(data), .. }) => Some(Bytes::from(data.clone())),
                Some(_) => return Err(wrong_type(key)),
                None => None,
            };
            // Redis: GETSET clears any existing TTL (matches SET semantics).
            let new_entry = StoredValue { expires_at_ms: None, value: Value::String(value.to_vec()) };
            Ok(CasAction::Write(new_entry, old))
        })
        .await
    }

    async fn persist(&self, key: &str) -> Result<bool, RustyAntError> {
        self.cas(key, move |entry| match entry {
            Some(existing) if existing.expires_at_ms.is_some() => {
                let mut new_entry = existing.clone();
                new_entry.expires_at_ms = None;
                Ok(CasAction::Write(new_entry, true))
            }
            _ => Ok(CasAction::NoOp(false)),
        })
        .await
    }

    async fn lindex(&self, key: &str, index: i64) -> Result<Option<Bytes>, RustyAntError> {
        match self.load(key).await? {
            Some(StoredValue { value: Value::List(l), .. }) => {
                let len = i64::try_from(l.len()).unwrap_or(i64::MAX);
                let actual = if index < 0 { len + index } else { index };
                if actual < 0 || actual >= len {
                    return Ok(None);
                }
                let i = usize::try_from(actual).unwrap_or(0);
                Ok(Some(Bytes::from(l[i].clone())))
            }
            Some(_) => Err(wrong_type(key)),
            None => Ok(None),
        }
    }

    async fn lset(&self, key: &str, index: i64, value: Bytes) -> Result<(), RustyAntError> {
        self.cas(key, move |entry| {
            let (mut list, expires_at_ms) = match entry {
                Some(StoredValue { value: Value::List(l), expires_at_ms }) => (l.clone(), *expires_at_ms),
                Some(_) => return Err(wrong_type(key)),
                None => return Err(RustyAntError::Parse("no such key".into())),
            };
            let len = i64::try_from(list.len()).unwrap_or(i64::MAX);
            let actual = if index < 0 { len + index } else { index };
            if actual < 0 || actual >= len {
                return Err(RustyAntError::Parse("index out of range".into()));
            }
            let i = usize::try_from(actual).unwrap_or(0);
            list[i] = value.to_vec();
            let new_entry = StoredValue { expires_at_ms, value: Value::List(list) };
            Ok(CasAction::Write(new_entry, ()))
        })
        .await
    }

    async fn lrem(&self, key: &str, count: i64, value: Bytes) -> Result<i64, RustyAntError> {
        self.cas(key, move |entry| {
            let (mut list, expires_at_ms) = match entry {
                Some(StoredValue { value: Value::List(l), expires_at_ms }) => (l.clone(), *expires_at_ms),
                Some(_) => return Err(wrong_type(key)),
                None => return Ok(CasAction::NoOp(0)),
            };
            let target = value.as_ref();
            let removed = remove_list_occurrences(&mut list, target, count);
            if list.is_empty() {
                Ok(CasAction::Delete(removed))
            } else {
                let new_entry = StoredValue { expires_at_ms, value: Value::List(list) };
                Ok(CasAction::Write(new_entry, removed))
            }
        })
        .await
    }

    async fn zrangebyscore(&self, key: &str, min: ScoreBound, max: ScoreBound) -> Result<Vec<String>, RustyAntError> {
        let map = match self.load(key).await? {
            Some(StoredValue { value: Value::ZSet(m), .. }) => m,
            Some(_) => return Err(wrong_type(key)),
            None => return Ok(Vec::new()),
        };
        Ok(filter_zset_by_score(map, min, max))
    }

    async fn keys(&self, pattern: &str) -> Result<Vec<String>, RustyAntError> {
        let wm = wildmatch::WildMatch::new(pattern);
        let mut out = Vec::new();
        let mut cursor: Option<String> = None;
        loop {
            let mut req = self.client.list_objects_v2().bucket(&self.bucket).prefix(&self.prefix);
            if let Some(c) = &cursor {
                req = req.continuation_token(c);
            }
            let resp = req.send().await.map_err(|e| RustyAntError::S3(e.to_string()))?;
            for obj in resp.contents() {
                if let Some(full) = obj.key() {
                    if let Some(rel) = full.strip_prefix(self.prefix.as_str()) {
                        if wm.matches(rel) {
                            out.push(rel.to_string());
                        }
                    }
                }
            }
            match resp.next_continuation_token() {
                Some(next) => cursor = Some(next.to_string()),
                None => break,
            }
        }
        Ok(out)
    }

    async fn scan(
        &self,
        cursor: Option<&str>,
        pattern: Option<&str>,
        count: usize,
    ) -> Result<(Vec<String>, Option<String>), RustyAntError> {
        let wm = pattern.map(wildmatch::WildMatch::new);
        let mut req = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(&self.prefix)
            .max_keys(i32::try_from(count).unwrap_or(i32::MAX));
        if let Some(c) = cursor {
            req = req.continuation_token(c);
        }
        let resp = req.send().await.map_err(|e| RustyAntError::S3(e.to_string()))?;
        let mut matched: Vec<String> = Vec::new();
        for obj in resp.contents() {
            if let Some(full) = obj.key() {
                if let Some(rel) = full.strip_prefix(self.prefix.as_str()) {
                    if wm.as_ref().is_none_or(|w| w.matches(rel)) {
                        matched.push(rel.to_string());
                    }
                }
            }
        }
        let next = resp.next_continuation_token().map(String::from);
        Ok((matched, next))
    }

    async fn hincr_by(&self, key: &str, field: &str, delta: i64) -> Result<i64, RustyAntError> {
        let field = field.to_string();
        self.cas(key, move |entry| {
            let (mut map, expires_at_ms) = match entry {
                Some(StoredValue { value: Value::Hash(m), expires_at_ms }) => (m.clone(), *expires_at_ms),
                Some(_) => return Err(wrong_type(key)),
                None => (BTreeMap::new(), None),
            };
            let current: i64 = map
                .get(&field)
                .map(|v| {
                    let s = std::str::from_utf8(v)
                        .map_err(|_| RustyAntError::Parse("hash value is not an integer".into()))?;
                    s.parse::<i64>().map_err(|_| RustyAntError::Parse("hash value is not an integer".into()))
                })
                .transpose()?
                .unwrap_or(0);
            let new_val =
                current.checked_add(delta).ok_or_else(|| RustyAntError::Parse("increment overflow".into()))?;
            map.insert(field.clone(), new_val.to_string().into_bytes());
            let new_entry = StoredValue { expires_at_ms, value: Value::Hash(map) };
            Ok(CasAction::Write(new_entry, new_val))
        })
        .await
    }

    async fn srem(&self, key: &str, members: &[String]) -> Result<i64, RustyAntError> {
        let members = members.to_vec();
        self.cas(key, move |entry| {
            let (mut set, expires_at_ms) = match entry {
                Some(StoredValue { value: Value::Set(s), expires_at_ms }) => (s.clone(), *expires_at_ms),
                Some(_) => return Err(wrong_type(key)),
                None => return Ok(CasAction::NoOp(0)),
            };
            let mut removed: i64 = 0;
            for m in &members {
                if set.remove(m) {
                    removed += 1;
                }
            }
            if set.is_empty() {
                Ok(CasAction::Delete(removed))
            } else {
                let new_entry = StoredValue { expires_at_ms, value: Value::Set(set) };
                Ok(CasAction::Write(new_entry, removed))
            }
        })
        .await
    }

    async fn zrem(&self, key: &str, members: &[String]) -> Result<i64, RustyAntError> {
        let members = members.to_vec();
        self.cas(key, move |entry| {
            let (mut map, expires_at_ms) = match entry {
                Some(StoredValue { value: Value::ZSet(m), expires_at_ms }) => (m.clone(), *expires_at_ms),
                Some(_) => return Err(wrong_type(key)),
                None => return Ok(CasAction::NoOp(0)),
            };
            let mut removed: i64 = 0;
            for m in &members {
                if map.remove(m).is_some() {
                    removed += 1;
                }
            }
            if map.is_empty() {
                Ok(CasAction::Delete(removed))
            } else {
                let new_entry = StoredValue { expires_at_ms, value: Value::ZSet(map) };
                Ok(CasAction::Write(new_entry, removed))
            }
        })
        .await
    }

    async fn zincr_by(&self, key: &str, member: &str, delta: f64) -> Result<f64, RustyAntError> {
        let member = member.to_string();
        self.cas(key, move |entry| {
            let (mut map, expires_at_ms) = match entry {
                Some(StoredValue { value: Value::ZSet(m), expires_at_ms }) => (m.clone(), *expires_at_ms),
                Some(_) => return Err(wrong_type(key)),
                None => (BTreeMap::new(), None),
            };
            let current = map.get(&member).copied().unwrap_or(0.0);
            let new_score = current + delta;
            if new_score.is_nan() {
                return Err(RustyAntError::Parse("resulting score is NaN".into()));
            }
            map.insert(member.clone(), new_score);
            let new_entry = StoredValue { expires_at_ms, value: Value::ZSet(map) };
            Ok(CasAction::Write(new_entry, new_score))
        })
        .await
    }
}

// ---------------------------------------------------------------------------
// In-memory storage — for integration tests. `std::sync::Mutex` is used even
// though this is an async trait: every critical section is trivially bounded,
// so there's no `.await` held across the lock.
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
pub struct InMemoryStorage {
    inner: Mutex<BTreeMap<String, StoredValue>>,
}

impl InMemoryStorage {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn with_entry_mut<T>(&self, f: impl FnOnce(&mut BTreeMap<String, StoredValue>) -> T) -> T {
        let mut guard = self.inner.lock().expect("poisoned");
        f(&mut guard)
    }

    fn load(&self, key: &str) -> Option<StoredValue> {
        self.with_entry_mut(|map| match map.get(key) {
            Some(v) if is_expired(v) => {
                map.remove(key);
                None
            }
            Some(v) => Some(v.clone()),
            None => None,
        })
    }
}

#[async_trait]
impl Storage for InMemoryStorage {
    async fn delete(&self, key: &str) -> Result<bool, RustyAntError> {
        // Expire-on-read semantics: expired key counts as already gone.
        let _ = self.load(key);
        Ok(self.with_entry_mut(|map| map.remove(key).is_some()))
    }

    async fn exists(&self, key: &str) -> Result<bool, RustyAntError> {
        Ok(self.load(key).is_some())
    }

    async fn kind(&self, key: &str) -> Result<Option<&'static str>, RustyAntError> {
        Ok(self.load(key).map(|v| value_kind(&v.value)))
    }

    async fn expire_at(&self, key: &str, expires_at_ms: i64) -> Result<bool, RustyAntError> {
        if self.load(key).is_none() {
            return Ok(false);
        }
        self.with_entry_mut(|map| {
            if let Some(entry) = map.get_mut(key) {
                entry.expires_at_ms = Some(expires_at_ms);
                true
            } else {
                false
            }
        });
        Ok(true)
    }

    async fn ttl_ms(&self, key: &str) -> Result<TtlResult, RustyAntError> {
        let Some(v) = self.load(key) else {
            return Ok(TtlResult::NoKey);
        };
        Ok(v.expires_at_ms.map_or(TtlResult::NoExpire, |exp| TtlResult::Ms((exp - now_ms()).max(0))))
    }

    async fn get_string(&self, key: &str) -> Result<Option<Bytes>, RustyAntError> {
        match self.load(key) {
            Some(StoredValue { value: Value::String(data), .. }) => Ok(Some(Bytes::from(data))),
            Some(_) => Err(wrong_type(key)),
            None => Ok(None),
        }
    }

    async fn set_string(&self, key: &str, value: Bytes, expires_at_ms: Option<i64>) -> Result<(), RustyAntError> {
        let entry = StoredValue { expires_at_ms, value: Value::String(value.to_vec()) };
        self.with_entry_mut(|map| {
            map.insert(key.to_string(), entry);
        });
        Ok(())
    }

    async fn set_string_nx(&self, key: &str, value: Bytes, expires_at_ms: Option<i64>) -> Result<bool, RustyAntError> {
        let entry = StoredValue { expires_at_ms, value: Value::String(value.to_vec()) };
        Ok(self.with_entry_mut(|map| {
            // Evict any expired occupant; then the emptiness check is honest.
            if let Some(existing) = map.get(key) {
                if is_expired(existing) {
                    map.remove(key);
                } else {
                    return false;
                }
            }
            map.insert(key.to_string(), entry);
            true
        }))
    }

    async fn incr_by(&self, key: &str, delta: i64) -> Result<i64, RustyAntError> {
        let (current, expires_at_ms) = match self.load(key) {
            Some(StoredValue { value: Value::String(data), expires_at_ms }) => {
                let s =
                    std::str::from_utf8(&data).map_err(|_| RustyAntError::Parse("value is not an integer".into()))?;
                let n: i64 = s.parse().map_err(|_| RustyAntError::Parse("value is not an integer".into()))?;
                (n, expires_at_ms)
            }
            Some(_) => return Err(wrong_type(key)),
            None => (0, None),
        };
        let new_val = current.checked_add(delta).ok_or_else(|| RustyAntError::Parse("increment overflow".into()))?;
        let entry = StoredValue { expires_at_ms, value: Value::String(new_val.to_string().into_bytes()) };
        self.with_entry_mut(|map| {
            map.insert(key.to_string(), entry);
        });
        Ok(new_val)
    }

    async fn hset(&self, key: &str, pairs: Vec<(String, Bytes)>) -> Result<i64, RustyAntError> {
        let (mut map, expires_at_ms) = match self.load(key) {
            Some(StoredValue { value: Value::Hash(m), expires_at_ms }) => (m, expires_at_ms),
            Some(_) => return Err(wrong_type(key)),
            None => (BTreeMap::new(), None),
        };
        let mut new_fields: i64 = 0;
        for (field, value) in pairs {
            if !map.contains_key(&field) {
                new_fields += 1;
            }
            map.insert(field, value.to_vec());
        }
        let entry = StoredValue { expires_at_ms, value: Value::Hash(map) };
        self.with_entry_mut(|store| {
            store.insert(key.to_string(), entry);
        });
        Ok(new_fields)
    }

    async fn hget(&self, key: &str, field: &str) -> Result<Option<Bytes>, RustyAntError> {
        match self.load(key) {
            Some(StoredValue { value: Value::Hash(m), .. }) => Ok(m.get(field).map(|v| Bytes::from(v.clone()))),
            Some(_) => Err(wrong_type(key)),
            None => Ok(None),
        }
    }

    async fn hdel(&self, key: &str, fields: &[String]) -> Result<i64, RustyAntError> {
        let (mut map, expires_at_ms) = match self.load(key) {
            Some(StoredValue { value: Value::Hash(m), expires_at_ms }) => (m, expires_at_ms),
            Some(_) => return Err(wrong_type(key)),
            None => return Ok(0),
        };
        let mut removed: i64 = 0;
        for f in fields {
            if map.remove(f).is_some() {
                removed += 1;
            }
        }
        if map.is_empty() {
            self.with_entry_mut(|store| {
                store.remove(key);
            });
        } else {
            let entry = StoredValue { expires_at_ms, value: Value::Hash(map) };
            self.with_entry_mut(|store| {
                store.insert(key.to_string(), entry);
            });
        }
        Ok(removed)
    }

    async fn hgetall(&self, key: &str) -> Result<Vec<(String, Bytes)>, RustyAntError> {
        match self.load(key) {
            Some(StoredValue { value: Value::Hash(m), .. }) => {
                Ok(m.into_iter().map(|(k, v)| (k, Bytes::from(v))).collect())
            }
            Some(_) => Err(wrong_type(key)),
            None => Ok(Vec::new()),
        }
    }

    async fn list_push(&self, key: &str, values: Vec<Bytes>, left: bool) -> Result<i64, RustyAntError> {
        let (mut list, expires_at_ms) = match self.load(key) {
            Some(StoredValue { value: Value::List(l), expires_at_ms }) => (l, expires_at_ms),
            Some(_) => return Err(wrong_type(key)),
            None => (Vec::new(), None),
        };
        for v in values {
            if left {
                list.insert(0, v.to_vec());
            } else {
                list.push(v.to_vec());
            }
        }
        let len = i64::try_from(list.len()).unwrap_or(i64::MAX);
        let entry = StoredValue { expires_at_ms, value: Value::List(list) };
        self.with_entry_mut(|store| {
            store.insert(key.to_string(), entry);
        });
        Ok(len)
    }

    async fn list_pop(&self, key: &str, count: usize, left: bool) -> Result<Vec<Bytes>, RustyAntError> {
        let (mut list, expires_at_ms) = match self.load(key) {
            Some(StoredValue { value: Value::List(l), expires_at_ms }) => (l, expires_at_ms),
            Some(_) => return Err(wrong_type(key)),
            None => return Ok(Vec::new()),
        };
        let take = count.min(list.len());
        let mut out: Vec<Bytes> = Vec::with_capacity(take);
        for _ in 0..take {
            if left {
                out.push(Bytes::from(list.remove(0)));
            } else {
                out.push(Bytes::from(list.pop().expect("len checked above")));
            }
        }
        if list.is_empty() {
            self.with_entry_mut(|store| {
                store.remove(key);
            });
        } else {
            let entry = StoredValue { expires_at_ms, value: Value::List(list) };
            self.with_entry_mut(|store| {
                store.insert(key.to_string(), entry);
            });
        }
        Ok(out)
    }

    async fn lrange(&self, key: &str, start: i64, stop: i64) -> Result<Vec<Bytes>, RustyAntError> {
        let list = match self.load(key) {
            Some(StoredValue { value: Value::List(l), .. }) => l,
            Some(_) => return Err(wrong_type(key)),
            None => return Ok(Vec::new()),
        };
        let len = i64::try_from(list.len()).unwrap_or(i64::MAX);
        let Some((s, e)) = resolve_range(len, start, stop) else {
            return Ok(Vec::new());
        };
        Ok(list[s..=e].iter().map(|v| Bytes::from(v.clone())).collect())
    }

    async fn sadd(&self, key: &str, members: Vec<String>) -> Result<i64, RustyAntError> {
        let (mut set, expires_at_ms) = match self.load(key) {
            Some(StoredValue { value: Value::Set(s), expires_at_ms }) => (s, expires_at_ms),
            Some(_) => return Err(wrong_type(key)),
            None => (BTreeSet::new(), None),
        };
        let mut added: i64 = 0;
        for m in members {
            if set.insert(m) {
                added += 1;
            }
        }
        let entry = StoredValue { expires_at_ms, value: Value::Set(set) };
        self.with_entry_mut(|store| {
            store.insert(key.to_string(), entry);
        });
        Ok(added)
    }

    async fn zadd(&self, key: &str, pairs: Vec<(f64, String)>) -> Result<i64, RustyAntError> {
        let (mut map, expires_at_ms) = match self.load(key) {
            Some(StoredValue { value: Value::ZSet(m), expires_at_ms }) => (m, expires_at_ms),
            Some(_) => return Err(wrong_type(key)),
            None => (BTreeMap::new(), None),
        };
        let mut added: i64 = 0;
        for (score, member) in pairs {
            if !map.contains_key(&member) {
                added += 1;
            }
            map.insert(member, score);
        }
        let entry = StoredValue { expires_at_ms, value: Value::ZSet(map) };
        self.with_entry_mut(|store| {
            store.insert(key.to_string(), entry);
        });
        Ok(added)
    }

    async fn zrange(&self, key: &str, start: i64, stop: i64) -> Result<Vec<String>, RustyAntError> {
        let map = match self.load(key) {
            Some(StoredValue { value: Value::ZSet(m), .. }) => m,
            Some(_) => return Err(wrong_type(key)),
            None => return Ok(Vec::new()),
        };
        let mut sorted: Vec<(String, f64)> = map.into_iter().collect();
        sorted.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal).then_with(|| a.0.cmp(&b.0)));
        let len = i64::try_from(sorted.len()).unwrap_or(i64::MAX);
        let Some((s, e)) = resolve_range(len, start, stop) else {
            return Ok(Vec::new());
        };
        Ok(sorted[s..=e].iter().map(|(m, _)| m.clone()).collect())
    }

    async fn hlen(&self, key: &str) -> Result<i64, RustyAntError> {
        match self.load(key) {
            Some(StoredValue { value: Value::Hash(m), .. }) => Ok(i64::try_from(m.len()).unwrap_or(i64::MAX)),
            Some(_) => Err(wrong_type(key)),
            None => Ok(0),
        }
    }

    async fn hkeys(&self, key: &str) -> Result<Vec<String>, RustyAntError> {
        match self.load(key) {
            Some(StoredValue { value: Value::Hash(m), .. }) => Ok(m.into_keys().collect()),
            Some(_) => Err(wrong_type(key)),
            None => Ok(Vec::new()),
        }
    }

    async fn hvals(&self, key: &str) -> Result<Vec<Bytes>, RustyAntError> {
        match self.load(key) {
            Some(StoredValue { value: Value::Hash(m), .. }) => Ok(m.into_values().map(Bytes::from).collect()),
            Some(_) => Err(wrong_type(key)),
            None => Ok(Vec::new()),
        }
    }

    async fn hexists(&self, key: &str, field: &str) -> Result<bool, RustyAntError> {
        match self.load(key) {
            Some(StoredValue { value: Value::Hash(m), .. }) => Ok(m.contains_key(field)),
            Some(_) => Err(wrong_type(key)),
            None => Ok(false),
        }
    }

    async fn hmget(&self, key: &str, fields: &[String]) -> Result<Vec<Option<Bytes>>, RustyAntError> {
        match self.load(key) {
            Some(StoredValue { value: Value::Hash(m), .. }) => {
                Ok(fields.iter().map(|f| m.get(f).map(|v| Bytes::from(v.clone()))).collect())
            }
            Some(_) => Err(wrong_type(key)),
            None => Ok(fields.iter().map(|_| None).collect()),
        }
    }

    async fn llen(&self, key: &str) -> Result<i64, RustyAntError> {
        match self.load(key) {
            Some(StoredValue { value: Value::List(l), .. }) => Ok(i64::try_from(l.len()).unwrap_or(i64::MAX)),
            Some(_) => Err(wrong_type(key)),
            None => Ok(0),
        }
    }

    async fn smembers(&self, key: &str) -> Result<Vec<String>, RustyAntError> {
        match self.load(key) {
            Some(StoredValue { value: Value::Set(s), .. }) => Ok(s.into_iter().collect()),
            Some(_) => Err(wrong_type(key)),
            None => Ok(Vec::new()),
        }
    }

    async fn sismember(&self, key: &str, member: &str) -> Result<bool, RustyAntError> {
        match self.load(key) {
            Some(StoredValue { value: Value::Set(s), .. }) => Ok(s.contains(member)),
            Some(_) => Err(wrong_type(key)),
            None => Ok(false),
        }
    }

    async fn scard(&self, key: &str) -> Result<i64, RustyAntError> {
        match self.load(key) {
            Some(StoredValue { value: Value::Set(s), .. }) => Ok(i64::try_from(s.len()).unwrap_or(i64::MAX)),
            Some(_) => Err(wrong_type(key)),
            None => Ok(0),
        }
    }

    async fn zscore(&self, key: &str, member: &str) -> Result<Option<f64>, RustyAntError> {
        match self.load(key) {
            Some(StoredValue { value: Value::ZSet(m), .. }) => Ok(m.get(member).copied()),
            Some(_) => Err(wrong_type(key)),
            None => Ok(None),
        }
    }

    async fn zcard(&self, key: &str) -> Result<i64, RustyAntError> {
        match self.load(key) {
            Some(StoredValue { value: Value::ZSet(m), .. }) => Ok(i64::try_from(m.len()).unwrap_or(i64::MAX)),
            Some(_) => Err(wrong_type(key)),
            None => Ok(0),
        }
    }

    async fn getset(&self, key: &str, value: Bytes) -> Result<Option<Bytes>, RustyAntError> {
        let old = match self.load(key) {
            Some(StoredValue { value: Value::String(data), .. }) => Some(Bytes::from(data)),
            Some(_) => return Err(wrong_type(key)),
            None => None,
        };
        let entry = StoredValue { expires_at_ms: None, value: Value::String(value.to_vec()) };
        self.with_entry_mut(|store| {
            store.insert(key.to_string(), entry);
        });
        Ok(old)
    }

    async fn persist(&self, key: &str) -> Result<bool, RustyAntError> {
        // Evict expired keys first so we don't "persist" a zombie.
        if self.load(key).is_none() {
            return Ok(false);
        }
        Ok(self.with_entry_mut(|map| {
            let Some(entry) = map.get_mut(key) else {
                return false;
            };
            if entry.expires_at_ms.is_some() {
                entry.expires_at_ms = None;
                true
            } else {
                false
            }
        }))
    }

    async fn lindex(&self, key: &str, index: i64) -> Result<Option<Bytes>, RustyAntError> {
        match self.load(key) {
            Some(StoredValue { value: Value::List(l), .. }) => {
                let len = i64::try_from(l.len()).unwrap_or(i64::MAX);
                let actual = if index < 0 { len + index } else { index };
                if actual < 0 || actual >= len {
                    return Ok(None);
                }
                let i = usize::try_from(actual).unwrap_or(0);
                Ok(Some(Bytes::from(l[i].clone())))
            }
            Some(_) => Err(wrong_type(key)),
            None => Ok(None),
        }
    }

    async fn lset(&self, key: &str, index: i64, value: Bytes) -> Result<(), RustyAntError> {
        let (mut list, expires_at_ms) = match self.load(key) {
            Some(StoredValue { value: Value::List(l), expires_at_ms }) => (l, expires_at_ms),
            Some(_) => return Err(wrong_type(key)),
            None => return Err(RustyAntError::Parse("no such key".into())),
        };
        let len = i64::try_from(list.len()).unwrap_or(i64::MAX);
        let actual = if index < 0 { len + index } else { index };
        if actual < 0 || actual >= len {
            return Err(RustyAntError::Parse("index out of range".into()));
        }
        let i = usize::try_from(actual).unwrap_or(0);
        list[i] = value.to_vec();
        let entry = StoredValue { expires_at_ms, value: Value::List(list) };
        self.with_entry_mut(|store| {
            store.insert(key.to_string(), entry);
        });
        Ok(())
    }

    async fn lrem(&self, key: &str, count: i64, value: Bytes) -> Result<i64, RustyAntError> {
        let (mut list, expires_at_ms) = match self.load(key) {
            Some(StoredValue { value: Value::List(l), expires_at_ms }) => (l, expires_at_ms),
            Some(_) => return Err(wrong_type(key)),
            None => return Ok(0),
        };
        let removed = remove_list_occurrences(&mut list, value.as_ref(), count);
        if list.is_empty() {
            self.with_entry_mut(|store| {
                store.remove(key);
            });
        } else {
            let entry = StoredValue { expires_at_ms, value: Value::List(list) };
            self.with_entry_mut(|store| {
                store.insert(key.to_string(), entry);
            });
        }
        Ok(removed)
    }

    async fn zrangebyscore(&self, key: &str, min: ScoreBound, max: ScoreBound) -> Result<Vec<String>, RustyAntError> {
        let map = match self.load(key) {
            Some(StoredValue { value: Value::ZSet(m), .. }) => m,
            Some(_) => return Err(wrong_type(key)),
            None => return Ok(Vec::new()),
        };
        Ok(filter_zset_by_score(map, min, max))
    }

    async fn keys(&self, pattern: &str) -> Result<Vec<String>, RustyAntError> {
        let wm = wildmatch::WildMatch::new(pattern);
        let now = now_ms();
        Ok(self.with_entry_mut(|map| {
            map.iter()
                .filter(|(_, v)| v.expires_at_ms.is_none_or(|exp| exp > now))
                .filter(|(k, _)| wm.matches(k))
                .map(|(k, _)| k.clone())
                .collect()
        }))
    }

    async fn scan(
        &self,
        cursor: Option<&str>,
        pattern: Option<&str>,
        count: usize,
    ) -> Result<(Vec<String>, Option<String>), RustyAntError> {
        let wm = pattern.map(wildmatch::WildMatch::new);
        let now = now_ms();
        // Cursor semantics for InMemoryStorage: the cursor is the last key
        // returned on the previous call. BTreeMap gives us ordered iteration,
        // so "keys strictly greater than cursor" is a stable continuation.
        Ok(self.with_entry_mut(|map| {
            let start_after = cursor;
            let live_matched: Vec<String> = map
                .iter()
                .filter(|(k, _)| start_after.is_none_or(|c| k.as_str() > c))
                .filter(|(_, v)| v.expires_at_ms.is_none_or(|exp| exp > now))
                .filter(|(k, _)| wm.as_ref().is_none_or(|w| w.matches(k)))
                .take(count + 1) // +1 peek to know whether more exist
                .map(|(k, _)| k.clone())
                .collect();
            let has_more = live_matched.len() > count;
            let batch = if has_more { live_matched[..count].to_vec() } else { live_matched };
            let next = if has_more { batch.last().cloned() } else { None };
            (batch, next)
        }))
    }

    async fn hincr_by(&self, key: &str, field: &str, delta: i64) -> Result<i64, RustyAntError> {
        let (mut map, expires_at_ms) = match self.load(key) {
            Some(StoredValue { value: Value::Hash(m), expires_at_ms }) => (m, expires_at_ms),
            Some(_) => return Err(wrong_type(key)),
            None => (BTreeMap::new(), None),
        };
        let current: i64 = map
            .get(field)
            .map(|v| {
                let s =
                    std::str::from_utf8(v).map_err(|_| RustyAntError::Parse("hash value is not an integer".into()))?;
                s.parse::<i64>().map_err(|_| RustyAntError::Parse("hash value is not an integer".into()))
            })
            .transpose()?
            .unwrap_or(0);
        let new_val = current.checked_add(delta).ok_or_else(|| RustyAntError::Parse("increment overflow".into()))?;
        map.insert(field.to_string(), new_val.to_string().into_bytes());
        let entry = StoredValue { expires_at_ms, value: Value::Hash(map) };
        self.with_entry_mut(|store| {
            store.insert(key.to_string(), entry);
        });
        Ok(new_val)
    }

    async fn srem(&self, key: &str, members: &[String]) -> Result<i64, RustyAntError> {
        let (mut set, expires_at_ms) = match self.load(key) {
            Some(StoredValue { value: Value::Set(s), expires_at_ms }) => (s, expires_at_ms),
            Some(_) => return Err(wrong_type(key)),
            None => return Ok(0),
        };
        let mut removed: i64 = 0;
        for m in members {
            if set.remove(m) {
                removed += 1;
            }
        }
        if set.is_empty() {
            self.with_entry_mut(|store| {
                store.remove(key);
            });
        } else {
            let entry = StoredValue { expires_at_ms, value: Value::Set(set) };
            self.with_entry_mut(|store| {
                store.insert(key.to_string(), entry);
            });
        }
        Ok(removed)
    }

    async fn zrem(&self, key: &str, members: &[String]) -> Result<i64, RustyAntError> {
        let (mut map, expires_at_ms) = match self.load(key) {
            Some(StoredValue { value: Value::ZSet(m), expires_at_ms }) => (m, expires_at_ms),
            Some(_) => return Err(wrong_type(key)),
            None => return Ok(0),
        };
        let mut removed: i64 = 0;
        for m in members {
            if map.remove(m).is_some() {
                removed += 1;
            }
        }
        if map.is_empty() {
            self.with_entry_mut(|store| {
                store.remove(key);
            });
        } else {
            let entry = StoredValue { expires_at_ms, value: Value::ZSet(map) };
            self.with_entry_mut(|store| {
                store.insert(key.to_string(), entry);
            });
        }
        Ok(removed)
    }

    async fn zincr_by(&self, key: &str, member: &str, delta: f64) -> Result<f64, RustyAntError> {
        let (mut map, expires_at_ms) = match self.load(key) {
            Some(StoredValue { value: Value::ZSet(m), expires_at_ms }) => (m, expires_at_ms),
            Some(_) => return Err(wrong_type(key)),
            None => (BTreeMap::new(), None),
        };
        let current = map.get(member).copied().unwrap_or(0.0);
        let new_score = current + delta;
        if new_score.is_nan() {
            return Err(RustyAntError::Parse("resulting score is NaN".into()));
        }
        map.insert(member.to_string(), new_score);
        let entry = StoredValue { expires_at_ms, value: Value::ZSet(map) };
        self.with_entry_mut(|store| {
            store.insert(key.to_string(), entry);
        });
        Ok(new_score)
    }
}

#[allow(dead_code)]
const fn _assert_trait_object_safe() {
    const fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<std::sync::Arc<dyn Storage>>();
}
