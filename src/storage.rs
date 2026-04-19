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

    async fn get_string(&self, key: &str) -> Result<Option<Bytes>, RustyAntError>;
    async fn set_string(&self, key: &str, value: Bytes, expires_at_ms: Option<i64>) -> Result<(), RustyAntError>;
    async fn incr_by(&self, key: &str, delta: i64) -> Result<i64, RustyAntError>;

    async fn hset(&self, key: &str, pairs: Vec<(String, Bytes)>) -> Result<i64, RustyAntError>;
    async fn hget(&self, key: &str, field: &str) -> Result<Option<Bytes>, RustyAntError>;
    async fn hdel(&self, key: &str, fields: &[String]) -> Result<i64, RustyAntError>;
    async fn hgetall(&self, key: &str) -> Result<Vec<(String, Bytes)>, RustyAntError>;

    async fn list_push(&self, key: &str, values: Vec<Bytes>, left: bool) -> Result<i64, RustyAntError>;
    async fn list_pop(&self, key: &str, count: usize, left: bool) -> Result<Vec<Bytes>, RustyAntError>;
    async fn lrange(&self, key: &str, start: i64, stop: i64) -> Result<Vec<Bytes>, RustyAntError>;

    async fn sadd(&self, key: &str, members: Vec<String>) -> Result<i64, RustyAntError>;

    async fn zadd(&self, key: &str, pairs: Vec<(f64, String)>) -> Result<i64, RustyAntError>;
    async fn zrange(&self, key: &str, start: i64, stop: i64) -> Result<Vec<String>, RustyAntError>;
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
                CasAction::Delete(r) => {
                    if etag.is_some() {
                        self.delete_raw(redis_key).await?;
                    }
                    return Ok(r);
                }
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
}

#[allow(dead_code)]
const fn _assert_trait_object_safe() {
    const fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<std::sync::Arc<dyn Storage>>();
}
