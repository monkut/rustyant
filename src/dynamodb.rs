//! `DynamoDB`-backed [`KVBackend`] implementation.
//!
//! # Layout
//!
//! Six tables, one per Redis value kind. Picking the right table at write
//! time is `O(1)` from the value itself; reads probe all six in parallel
//! when the kind is unknown to the caller. Each table has the same shape:
//!
//! | attribute    | type | role                                            |
//! |--------------|------|-------------------------------------------------|
//! | `pk`         | S    | the Redis key (partition key)                   |
//! | `data`       | B    | `serde_json::to_vec(&StoredValue)`              |
//! | `version`    | S    | per-write opaque token; powers CAS              |
//! | `ttl`        | N    | epoch seconds for native `DynamoDB` TTL (best-effort GC) |
//!
//! # Cross-kind divergence
//!
//! Writes go straight to the target kind table without probing the other
//! five. A `SET foo "bar"` after `HSET foo x 1` leaves the hashes-table row
//! alive — see `memory/project_dynamodb_backend.md` and the README scope
//! section for the full story. The [`KVBackend`] [`load`] surface resolves
//! divergent keys deterministically by the [`ValueKind::ALL`] order, which
//! happens to put `string` first so `GET foo` after a cross-kind `SET`
//! returns the string and `HGET foo x` returns `WRONGTYPE` — close enough
//! to Redis's "SET overwrote the hash" semantics for the queryable surface,
//! even though the hashes row is still on disk.
//!
//! # Version tokens
//!
//! On the wire the version is `"<kind>:<u64-hex>"` (e.g. `"string:a3f1c2"`).
//! That lets [`KVBackend::delete`] and [`KVBackend::save`] under
//! `WriteCondition::IfMatch` route to the right kind table — the version
//! came from a prior `load`, which knew which table answered. If the kind
//! prefix on an incoming `IfMatch` token doesn't match the kind of the
//! entry being saved, the save returns `Contention` so the outer CAS loop
//! re-reads.

use std::collections::HashMap;

use async_trait::async_trait;
use aws_sdk_dynamodb::Client as DynamoClient;
use aws_sdk_dynamodb::operation::delete_item::DeleteItemError;
use aws_sdk_dynamodb::operation::put_item::PutItemError;
use aws_sdk_dynamodb::types::AttributeValue;
use bytes::Bytes;

use crate::error::RustyAntError;
use crate::storage::{
    DeleteCondition, KVBackend, ListPage, StoredValue, ValueKind, WriteCondition, is_expired, pseudo_rand_u64,
};

/// Default prefix prepended to each kind table name when [`TableNames::with_prefix`]
/// is the configured constructor. Mirrors the S3 backend's `KEY_PREFIX`
/// convention.
pub const DEFAULT_TABLE_PREFIX: &str = "rustyant-";

/// `DynamoDB` attribute names. Defined as constants so tests, infra (SAM
/// `AttributeDefinitions`), and emergency `aws dynamodb` operators all use
/// the same spellings.
pub const ATTR_PK: &str = "pk";
pub const ATTR_DATA: &str = "data";
pub const ATTR_VERSION: &str = "version";
pub const ATTR_TTL: &str = "ttl";

/// `DynamoDB` single-item size limit. We refuse writes that would exceed it
/// upfront with an explicit error rather than letting `ValidationException`
/// bubble through. `380_000` leaves headroom for the four attribute names
/// and `DynamoDB`'s per-item bookkeeping.
const MAX_ITEM_BYTES: usize = 380_000;

// ---------------------------------------------------------------------------
// TableNames
// ---------------------------------------------------------------------------

/// The six per-kind table names. Constructed once at startup from a prefix
/// (production / floci-style local) or from explicit names (custom infra).
#[derive(Debug, Clone)]
pub struct TableNames {
    pub string: String,
    pub hash: String,
    pub list: String,
    pub set: String,
    pub zset: String,
    pub stream: String,
}

impl TableNames {
    /// Build from a shared prefix — `rustyant-` yields `rustyant-string`,
    /// `rustyant-hash`, etc. Matches the SAM template's default naming.
    #[must_use]
    pub fn with_prefix(prefix: &str) -> Self {
        Self {
            string: format!("{prefix}string"),
            hash: format!("{prefix}hash"),
            list: format!("{prefix}list"),
            set: format!("{prefix}set"),
            zset: format!("{prefix}zset"),
            stream: format!("{prefix}stream"),
        }
    }

    /// Look up the table for a given kind. `O(1)` match.
    #[must_use]
    pub fn for_kind(&self, kind: ValueKind) -> &str {
        match kind {
            ValueKind::String => &self.string,
            ValueKind::Hash => &self.hash,
            ValueKind::List => &self.list,
            ValueKind::Set => &self.set,
            ValueKind::ZSet => &self.zset,
            ValueKind::Stream => &self.stream,
        }
    }

    /// Iterate `(kind, table_name)` in [`ValueKind::ALL`] order. Used by
    /// [`DynamoDbBackend::load`] (probe order), [`flush_all`] (sweep order),
    /// and [`list_page`] (sequential walk order).
    pub fn iter(&self) -> impl Iterator<Item = (ValueKind, &str)> {
        [
            (ValueKind::String, self.string.as_str()),
            (ValueKind::Hash, self.hash.as_str()),
            (ValueKind::List, self.list.as_str()),
            (ValueKind::Set, self.set.as_str()),
            (ValueKind::ZSet, self.zset.as_str()),
            (ValueKind::Stream, self.stream.as_str()),
        ]
        .into_iter()
    }
}

impl Default for TableNames {
    fn default() -> Self {
        Self::with_prefix(DEFAULT_TABLE_PREFIX)
    }
}

// ---------------------------------------------------------------------------
// DynamoDbBackend
// ---------------------------------------------------------------------------

/// `DynamoDB`-backed [`KVBackend`].
///
/// Holds the SDK client and the resolved table names; everything else
/// (kind routing, version-token plumbing, expired-row GC, cursor encoding)
/// lives in the trait impl below.
#[derive(Debug)]
pub struct DynamoDbBackend {
    client: DynamoClient,
    tables: TableNames,
}

impl DynamoDbBackend {
    #[must_use]
    pub const fn new(client: DynamoClient, tables: TableNames) -> Self {
        Self { client, tables }
    }

    #[must_use]
    pub const fn tables(&self) -> &TableNames {
        &self.tables
    }

    /// Read one item by partition key. `None` for missing/expired entries;
    /// expired rows are GC'd best-effort on encounter.
    async fn get_one(
        &self,
        kind: ValueKind,
        table: &str,
        key: &str,
    ) -> Result<Option<(StoredValue, String)>, RustyAntError> {
        let res = self
            .client
            .get_item()
            .table_name(table)
            .key(ATTR_PK, AttributeValue::S(key.to_string()))
            .consistent_read(true)
            .send()
            .await
            .map_err(|e| RustyAntError::S3(format!("dynamodb get_item: {}", e.into_service_error())))?;
        let Some(item) = res.item else {
            return Ok(None);
        };
        let entry = decode_item(&item)?;
        if is_expired(&entry) {
            // Best-effort GC — drop it on encounter, ignore failures.
            let _ = self
                .client
                .delete_item()
                .table_name(table)
                .key(ATTR_PK, AttributeValue::S(key.to_string()))
                .send()
                .await;
            return Ok(None);
        }
        let version = read_version_attr(&item)?;
        Ok(Some((entry, encode_token(kind, &version))))
    }

    /// One Scan against `table`, returning at most `max_keys` partition keys.
    async fn scan_one_table(
        &self,
        table: &str,
        start_pk: Option<String>,
        max_keys: usize,
    ) -> Result<(Vec<String>, Option<String>), RustyAntError> {
        let mut req = self
            .client
            .scan()
            .table_name(table)
            .projection_expression(ATTR_PK)
            .limit(i32::try_from(max_keys).unwrap_or(i32::MAX));
        if let Some(pk) = start_pk {
            req = req.exclusive_start_key(ATTR_PK, AttributeValue::S(pk));
        }
        let resp =
            req.send().await.map_err(|e| RustyAntError::S3(format!("dynamodb scan: {}", e.into_service_error())))?;
        let keys: Vec<String> =
            resp.items().iter().filter_map(|item| item.get(ATTR_PK).and_then(|v| v.as_s().ok().cloned())).collect();
        let next_pk = resp.last_evaluated_key().and_then(|m| m.get(ATTR_PK).and_then(|v| v.as_s().ok().cloned()));
        Ok((keys, next_pk))
    }
}

// ---------------------------------------------------------------------------
// KVBackend impl
// ---------------------------------------------------------------------------

#[async_trait]
impl KVBackend for DynamoDbBackend {
    async fn load(&self, redis_key: &str) -> Result<Option<(StoredValue, String)>, RustyAntError> {
        // Six parallel GetItems. First non-None hit in fixed `ValueKind::ALL`
        // order wins on divergent keys.
        let (s, h, l, se, z, st) = tokio::try_join!(
            self.get_one(ValueKind::String, &self.tables.string, redis_key),
            self.get_one(ValueKind::Hash, &self.tables.hash, redis_key),
            self.get_one(ValueKind::List, &self.tables.list, redis_key),
            self.get_one(ValueKind::Set, &self.tables.set, redis_key),
            self.get_one(ValueKind::ZSet, &self.tables.zset, redis_key),
            self.get_one(ValueKind::Stream, &self.tables.stream, redis_key),
        )?;
        Ok(s.or(h).or(l).or(se).or(z).or(st))
    }

    async fn save(&self, redis_key: &str, entry: &StoredValue, cond: WriteCondition) -> Result<(), RustyAntError> {
        let kind = ValueKind::of(&entry.value);
        let table = self.tables.for_kind(kind);
        let body = serde_json::to_vec(entry)?;
        if body.len() > MAX_ITEM_BYTES {
            return Err(RustyAntError::Parse(format!(
                "value exceeds DynamoDB per-item limit ({} bytes > {MAX_ITEM_BYTES})",
                body.len()
            )));
        }
        let new_version = format!("{:016x}", pseudo_rand_u64());

        let mut req = self
            .client
            .put_item()
            .table_name(table)
            .item(ATTR_PK, AttributeValue::S(redis_key.to_string()))
            .item(ATTR_DATA, AttributeValue::B(body.into()))
            .item(ATTR_VERSION, AttributeValue::S(new_version));

        // Native TTL: epoch seconds. `DynamoDB`'s GC granularity is ~48h; the
        // authoritative expiry check is the lazy `is_expired` on read.
        if let Some(exp_ms) = entry.expires_at_ms {
            req = req.item(ATTR_TTL, AttributeValue::N((exp_ms / 1000).to_string()));
        }

        match cond {
            WriteCondition::Any => {}
            WriteCondition::CreateOnly => {
                req = req.condition_expression(format!("attribute_not_exists({ATTR_PK})"));
            }
            WriteCondition::IfMatch(token) => {
                let (token_kind, old_version) = parse_token(&token)?;
                if token_kind != kind {
                    // CAS token came from a different kind's row (a divergent
                    // key resolved through a different table). Treat as
                    // contention so the outer CAS loop re-reads.
                    return Err(RustyAntError::Contention);
                }
                req = req
                    .condition_expression(format!("{ATTR_VERSION} = :old"))
                    .expression_attribute_values(":old", AttributeValue::S(old_version));
            }
        }

        match req.send().await {
            Ok(_) => Ok(()),
            Err(e) => {
                let svc = e.into_service_error();
                if matches!(svc, PutItemError::ConditionalCheckFailedException(_)) {
                    Err(RustyAntError::Contention)
                } else {
                    Err(RustyAntError::S3(format!("dynamodb put_item: {svc}")))
                }
            }
        }
    }

    async fn delete(&self, redis_key: &str, cond: DeleteCondition) -> Result<(), RustyAntError> {
        match cond {
            DeleteCondition::Any => {
                // Sweep every kind's table — divergent keys may sit in more
                // than one. Unconditional DeleteItem is idempotent on
                // missing rows, so this is safe and cheap to overshoot.
                let key = redis_key.to_string();
                tokio::try_join!(
                    delete_one(&self.client, &self.tables.string, &key, None),
                    delete_one(&self.client, &self.tables.hash, &key, None),
                    delete_one(&self.client, &self.tables.list, &key, None),
                    delete_one(&self.client, &self.tables.set, &key, None),
                    delete_one(&self.client, &self.tables.zset, &key, None),
                    delete_one(&self.client, &self.tables.stream, &key, None),
                )?;
                Ok(())
            }
            DeleteCondition::IfMatch(token) => {
                let (token_kind, old_version) = parse_token(&token)?;
                let table = self.tables.for_kind(token_kind);
                delete_one(&self.client, table, redis_key, Some(&old_version)).await
            }
        }
    }

    async fn list_page(&self, cursor: Option<String>, max_keys: usize) -> Result<ListPage, RustyAntError> {
        let (table_idx, start_pk) = parse_cursor(cursor.as_deref());
        let tables: Vec<&str> = self.tables.iter().map(|(_, t)| t).collect();

        if table_idx >= tables.len() {
            return Ok(ListPage { keys: Vec::new(), next_cursor: None });
        }
        let (keys, next_pk) = self.scan_one_table(tables[table_idx], start_pk, max_keys).await?;
        let next_cursor = match next_pk {
            Some(pk) => Some(format_cursor(table_idx, Some(&pk))),
            None if table_idx + 1 < tables.len() => Some(format_cursor(table_idx + 1, None)),
            None => None,
        };
        // Even an empty page advances the cursor so the caller can drive
        // the walk to completion.
        Ok(ListPage { keys, next_cursor })
    }
}

/// Helper: one `DeleteItem` with optional version condition. Pulled out so
/// the `Any` branch of [`KVBackend::delete`] can fan out across all six
/// tables via `tokio::try_join!` without an awkward closure shape.
async fn delete_one(
    client: &DynamoClient,
    table: &str,
    key: &str,
    if_match_version: Option<&str>,
) -> Result<(), RustyAntError> {
    let mut req = client.delete_item().table_name(table).key(ATTR_PK, AttributeValue::S(key.to_string()));
    if let Some(v) = if_match_version {
        req = req
            .condition_expression(format!("{ATTR_VERSION} = :old"))
            .expression_attribute_values(":old", AttributeValue::S(v.to_string()));
    }
    match req.send().await {
        Ok(_) => Ok(()),
        Err(e) => {
            let svc = e.into_service_error();
            if matches!(svc, DeleteItemError::ConditionalCheckFailedException(_)) {
                Err(RustyAntError::Contention)
            } else {
                Err(RustyAntError::S3(format!("dynamodb delete_item: {svc}")))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Item / token / cursor codec
// ---------------------------------------------------------------------------

/// Parse a `DynamoDB` item map into a `StoredValue`. The `data` attribute
/// holds the JSON-serialized [`StoredValue`] (same shape as the S3 body),
/// so this is one `from_slice` call plus error-path wrapping.
fn decode_item(item: &HashMap<String, AttributeValue>) -> Result<StoredValue, RustyAntError> {
    let data = item
        .get(ATTR_DATA)
        .ok_or_else(|| RustyAntError::S3(format!("dynamodb item missing `{ATTR_DATA}` attribute")))?;
    let bytes = data.as_b().map_err(|_| RustyAntError::S3(format!("`{ATTR_DATA}` attribute is not Binary")))?;
    Ok(serde_json::from_slice(bytes.as_ref())?)
}

/// Read the per-row version token attribute. Used to mint the `<kind>:<v>`
/// CAS token returned by [`KVBackend::load`].
fn read_version_attr(item: &HashMap<String, AttributeValue>) -> Result<String, RustyAntError> {
    let v = item
        .get(ATTR_VERSION)
        .ok_or_else(|| RustyAntError::S3(format!("dynamodb item missing `{ATTR_VERSION}` attribute")))?;
    let s = v.as_s().map_err(|_| RustyAntError::S3(format!("`{ATTR_VERSION}` attribute is not String")))?;
    Ok(s.clone())
}

/// `"<kind>:<version>"` — the wire form of a CAS token. Saving `IfMatch`
/// parses this back to know which table to target.
fn encode_token(kind: ValueKind, version: &str) -> String {
    format!("{}:{version}", kind.as_str())
}

fn parse_token(token: &str) -> Result<(ValueKind, String), RustyAntError> {
    let (kind_str, version) =
        token.split_once(':').ok_or_else(|| RustyAntError::Parse(format!("malformed CAS token: {token}")))?;
    let kind = match kind_str {
        "string" => ValueKind::String,
        "hash" => ValueKind::Hash,
        "list" => ValueKind::List,
        "set" => ValueKind::Set,
        "zset" => ValueKind::ZSet,
        "stream" => ValueKind::Stream,
        other => return Err(RustyAntError::Parse(format!("unknown kind in CAS token: {other}"))),
    };
    Ok((kind, version.to_string()))
}

/// Cursor format: `<table_idx>` or `<table_idx>:<last_pk>`. `None` means
/// "start from the first table at the beginning."
fn parse_cursor(cursor: Option<&str>) -> (usize, Option<String>) {
    let Some(c) = cursor else {
        return (0, None);
    };
    if let Some((idx_str, pk)) = c.split_once(':') {
        let idx = idx_str.parse::<usize>().unwrap_or(0);
        return (idx, Some(pk.to_string()));
    }
    (c.parse::<usize>().unwrap_or(0), None)
}

fn format_cursor(idx: usize, pk: Option<&str>) -> String {
    pk.map_or_else(|| idx.to_string(), |p| format!("{idx}:{p}"))
}

// Suppress dead-code warning for `Bytes` import in non-test builds where
// nothing references it directly. The import is used implicitly via the
// trait's signature requirements.
#[allow(dead_code)]
const _BYTES_REFERENCE: Option<Bytes> = None;

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_names_with_prefix_yields_one_per_kind() {
        let tn = TableNames::with_prefix("rustyant-");
        assert_eq!(tn.string, "rustyant-string");
        assert_eq!(tn.hash, "rustyant-hash");
        assert_eq!(tn.list, "rustyant-list");
        assert_eq!(tn.set, "rustyant-set");
        assert_eq!(tn.zset, "rustyant-zset");
        assert_eq!(tn.stream, "rustyant-stream");
    }

    #[test]
    fn table_names_for_kind_picks_the_right_table() {
        let tn = TableNames::with_prefix("p-");
        assert_eq!(tn.for_kind(ValueKind::String), "p-string");
        assert_eq!(tn.for_kind(ValueKind::Hash), "p-hash");
        assert_eq!(tn.for_kind(ValueKind::Stream), "p-stream");
    }

    #[test]
    fn table_names_iter_walks_in_value_kind_all_order() {
        let tn = TableNames::with_prefix("p-");
        let kinds: Vec<ValueKind> = tn.iter().map(|(k, _)| k).collect();
        assert_eq!(kinds, ValueKind::ALL.to_vec());
    }

    #[test]
    fn token_round_trip() {
        for kind in ValueKind::ALL {
            let token = encode_token(kind, "abcd1234");
            let (parsed_kind, parsed_version) = parse_token(&token).unwrap();
            assert_eq!(parsed_kind, kind);
            assert_eq!(parsed_version, "abcd1234");
        }
    }

    #[test]
    fn parse_token_rejects_garbage() {
        assert!(parse_token("nodelimiter").is_err());
        assert!(parse_token("unknown:1").is_err());
        // Empty version is allowed at this layer — `DynamoDB`'s
        // ConditionExpression check will reject an empty match anyway.
        assert!(parse_token("string:").is_ok());
    }

    #[test]
    fn cursor_round_trip_at_table_start() {
        let (idx, pk) = parse_cursor(None);
        assert_eq!(idx, 0);
        assert!(pk.is_none());

        let s = format_cursor(2, None);
        assert_eq!(s, "2");
        let (idx, pk) = parse_cursor(Some("2"));
        assert_eq!(idx, 2);
        assert!(pk.is_none());
    }

    #[test]
    fn cursor_round_trip_mid_table() {
        let s = format_cursor(3, Some("foo:bar"));
        // The `pk` itself may contain colons — only the FIRST colon
        // separates idx from pk. Parsing must respect that.
        assert_eq!(s, "3:foo:bar");
        let (idx, pk) = parse_cursor(Some(&s));
        assert_eq!(idx, 3);
        assert_eq!(pk.as_deref(), Some("foo:bar"));
    }
}
