//! `DynamoDB`-backed [`KVBackend`] implementation.
//!
//! # Layout
//!
//! Seven tables — six per-kind data tables plus a canonical index that maps
//! the Redis key to its current kind:
//!
//! | table              | role                                                    |
//! |--------------------|---------------------------------------------------------|
//! | `{prefix}index`    | `pk → kind`, the single source of truth for "exists?"   |
//! | `{prefix}{kind}`   | the actual value, one row per Redis kind                |
//!
//! Data rows carry `pk` (S, partition key), `data` (B, the JSON-serialized
//! [`StoredValue`]), `version` (S, the per-write CAS token), and `ttl` (N,
//! epoch seconds — native `DynamoDB` GC). Index rows carry `pk` + `kind` (S)
//! + `ttl` (N); they have no `version` because CAS lives on the data row.
//!
//! # Atomicity
//!
//! Every write goes through `TransactWriteItems` so the data put/delete, the
//! index put/delete, and any cross-kind orphan cleanup all succeed-or-fail
//! together. After a `SET foo "bar"` that follows `HSET foo x 1`:
//!
//! 1. The hash row in `{prefix}hash` is deleted.
//! 2. The string row in `{prefix}string` is created.
//! 3. The index row flips from `kind=hash` to `kind=string`.
//!
//! …all in one transaction. There are no leaked rows.
//!
//! # Kind-unaware paths
//!
//! `EXISTS`, `TYPE`, `DEL`, `KEYS`, and `SCAN` all resolve through the index
//! table — one `GetItem` (or one `Scan`) instead of probing six tables in
//! parallel. The cost shows up on writes: every save/delete is a 2-or-3-item
//! `TransactWriteItems` (~2× WCU vs. an unconditional `PutItem`).
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
use aws_sdk_dynamodb::operation::transact_write_items::TransactWriteItemsError;
use aws_sdk_dynamodb::types::{AttributeValue, Delete, Put, TransactWriteItem};
use bytes::Bytes;

use crate::error::RustyAntError;
use crate::storage::{
    DeleteCondition, KVBackend, ListPage, StoredValue, ValueKind, WriteCondition, is_expired, pseudo_rand_u64,
};

/// Default prefix prepended to each table name when [`TableNames::with_prefix`]
/// is the configured constructor. Mirrors the S3 backend's `KEY_PREFIX`
/// convention.
pub const DEFAULT_TABLE_PREFIX: &str = "rustyant-";

/// `DynamoDB` attribute names. Defined as constants so tests, infra (SAM
/// `AttributeDefinitions`), and emergency `aws dynamodb` operators all use
/// the same spellings.
pub const ATTR_PK: &str = "pk";
pub const ATTR_DATA: &str = "data";
pub const ATTR_VERSION: &str = "version";
pub const ATTR_KIND: &str = "kind";
pub const ATTR_TTL: &str = "ttl";

/// `DynamoDB` single-item size limit. We refuse writes that would exceed it
/// upfront with an explicit error rather than letting `ValidationException`
/// bubble through. `380_000` leaves headroom for the four attribute names
/// and `DynamoDB`'s per-item bookkeeping.
const MAX_ITEM_BYTES: usize = 380_000;

// ---------------------------------------------------------------------------
// TableNames
// ---------------------------------------------------------------------------

/// Resolved table names — one per Redis kind plus the cross-kind index.
/// Constructed once at startup from a prefix (production / floci-style local)
/// or from explicit names (custom infra).
#[derive(Debug, Clone)]
pub struct TableNames {
    pub index: String,
    pub string: String,
    pub hash: String,
    pub list: String,
    pub set: String,
    pub zset: String,
    pub stream: String,
}

impl TableNames {
    /// Build from a shared prefix — `rustyant-` yields `rustyant-index`,
    /// `rustyant-string`, `rustyant-hash`, etc. Matches the SAM template's
    /// default naming.
    #[must_use]
    pub fn with_prefix(prefix: &str) -> Self {
        Self {
            index: format!("{prefix}index"),
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

    /// Iterate `(kind, table_name)` for the six per-kind tables in
    /// [`ValueKind::ALL`] order. The index table is intentionally absent —
    /// callers iterating data tables (admin scripts, future bulk-delete
    /// overrides) don't want to mix the index in.
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

    /// Strongly-consistent read of the index row. `Some(kind)` when the key
    /// is alive somewhere; `None` when it isn't (or the index row was reaped
    /// by native TTL but the data row is still around — treated as "missing"
    /// because the index is canonical).
    async fn read_index_kind(&self, key: &str) -> Result<Option<ValueKind>, RustyAntError> {
        let res = self
            .client
            .get_item()
            .table_name(&self.tables.index)
            .key(ATTR_PK, AttributeValue::S(key.to_string()))
            .consistent_read(true)
            .send()
            .await
            .map_err(|e| RustyAntError::S3(format!("dynamodb get_item (index): {}", e.into_service_error())))?;
        let Some(item) = res.item else {
            return Ok(None);
        };
        let Some(kind_attr) = item.get(ATTR_KIND) else {
            return Ok(None);
        };
        let kind_str =
            kind_attr.as_s().map_err(|_| RustyAntError::S3(format!("`{ATTR_KIND}` attribute is not String")))?;
        parse_kind(kind_str).map(Some)
    }

    /// Strongly-consistent read of the data row for a known kind. Expired
    /// rows are GC'd best-effort on encounter (data + index pair, conditional
    /// on the version we just read so we don't trash a racing writer's data).
    async fn get_one(&self, kind: ValueKind, key: &str) -> Result<Option<(StoredValue, String)>, RustyAntError> {
        let table = self.tables.for_kind(kind);
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
        let version = read_version_attr(&item)?;
        if is_expired(&entry) {
            // Best-effort GC under the version we just read. If a racing
            // writer already replaced the row, the conditional delete fails
            // harmlessly and the next access will retry.
            let _ = self.delete(key, DeleteCondition::IfMatch(encode_token(kind, &version))).await;
            return Ok(None);
        }
        Ok(Some((entry, encode_token(kind, &version))))
    }

    /// One `Scan` against the index table. Returns `(keys, last_pk)`; the
    /// caller wraps `last_pk` into the `next_cursor` field of [`ListPage`].
    async fn scan_index(
        &self,
        start_pk: Option<String>,
        max_keys: usize,
    ) -> Result<(Vec<String>, Option<String>), RustyAntError> {
        let mut req = self
            .client
            .scan()
            .table_name(&self.tables.index)
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
        // Index → kind → one GetItem on the right kind table. Two RTTs in
        // the worst case, no parallel probe of the other five tables.
        let Some(kind) = self.read_index_kind(redis_key).await? else {
            return Ok(None);
        };
        self.get_one(kind, redis_key).await
    }

    async fn save(&self, redis_key: &str, entry: &StoredValue, cond: WriteCondition) -> Result<(), RustyAntError> {
        let new_kind = ValueKind::of(&entry.value);
        let body = serde_json::to_vec(entry)?;
        if body.len() > MAX_ITEM_BYTES {
            return Err(RustyAntError::Parse(format!(
                "value exceeds DynamoDB per-item limit ({} bytes > {MAX_ITEM_BYTES})",
                body.len()
            )));
        }
        let new_version = format!("{:016x}", pseudo_rand_u64());
        let target_table = self.tables.for_kind(new_kind);

        // For Any/CreateOnly we read the index up front to learn whether a
        // cross-kind orphan delete needs to ride along; for IfMatch the
        // token already names the kind, and a kind-mismatch is auto-Contention.
        let observed_kind = match &cond {
            WriteCondition::Any | WriteCondition::CreateOnly => self.read_index_kind(redis_key).await?,
            WriteCondition::IfMatch(token) => {
                let (token_kind, _) = parse_token(token)?;
                if token_kind != new_kind {
                    return Err(RustyAntError::Contention);
                }
                Some(token_kind)
            }
        };

        // CreateOnly fast path — index already shows a row, no need to
        // dispatch a transaction we know will fail.
        if matches!(cond, WriteCondition::CreateOnly) && observed_kind.is_some() {
            return Err(RustyAntError::Contention);
        }

        // Build the data Put.
        let mut data_put = Put::builder()
            .table_name(target_table)
            .item(ATTR_PK, AttributeValue::S(redis_key.to_string()))
            .item(ATTR_DATA, AttributeValue::B(body.into()))
            .item(ATTR_VERSION, AttributeValue::S(new_version));
        if let Some(exp_ms) = entry.expires_at_ms {
            data_put = data_put.item(ATTR_TTL, AttributeValue::N((exp_ms / 1000).to_string()));
        }
        match &cond {
            WriteCondition::Any | WriteCondition::CreateOnly => {}
            WriteCondition::IfMatch(token) => {
                let (_, old_version) = parse_token(token)?;
                data_put = data_put
                    .condition_expression(format!("{ATTR_VERSION} = :old"))
                    .expression_attribute_values(":old", AttributeValue::S(old_version));
            }
        }
        let data_put = data_put.build().map_err(|e| RustyAntError::S3(format!("dynamodb build put (data): {e}")))?;

        // Build the index Put. Conditional on the index either being absent
        // (first write) or still showing the kind we observed (no concurrent
        // change since the read).
        let mut index_put = Put::builder()
            .table_name(&self.tables.index)
            .item(ATTR_PK, AttributeValue::S(redis_key.to_string()))
            .item(ATTR_KIND, AttributeValue::S(new_kind.as_str().to_string()));
        if let Some(exp_ms) = entry.expires_at_ms {
            index_put = index_put.item(ATTR_TTL, AttributeValue::N((exp_ms / 1000).to_string()));
        }
        match observed_kind {
            None => {
                index_put = index_put.condition_expression(format!("attribute_not_exists({ATTR_PK})"));
            }
            Some(k) => {
                index_put = index_put
                    .condition_expression(format!("{ATTR_KIND} = :ok"))
                    .expression_attribute_values(":ok", AttributeValue::S(k.as_str().to_string()));
            }
        }
        let index_put = index_put.build().map_err(|e| RustyAntError::S3(format!("dynamodb build put (index): {e}")))?;

        let mut items: Vec<TransactWriteItem> = Vec::with_capacity(3);
        items.push(TransactWriteItem::builder().put(data_put).build());
        items.push(TransactWriteItem::builder().put(index_put).build());
        // Cross-kind transition — sweep the orphan row of the old kind so
        // the index stays the single source of truth.
        if let Some(old_kind) = observed_kind {
            if old_kind != new_kind {
                let old_table = self.tables.for_kind(old_kind);
                let old_delete = Delete::builder()
                    .table_name(old_table)
                    .key(ATTR_PK, AttributeValue::S(redis_key.to_string()))
                    .build()
                    .map_err(|e| RustyAntError::S3(format!("dynamodb build delete (orphan): {e}")))?;
                items.push(TransactWriteItem::builder().delete(old_delete).build());
            }
        }

        match self.client.transact_write_items().set_transact_items(Some(items)).send().await {
            Ok(_) => Ok(()),
            Err(e) => {
                let svc = e.into_service_error();
                if matches!(svc, TransactWriteItemsError::TransactionCanceledException(_)) {
                    Err(RustyAntError::Contention)
                } else {
                    Err(RustyAntError::S3(format!("dynamodb transact_write_items (save): {svc}")))
                }
            }
        }
    }

    async fn delete(&self, redis_key: &str, cond: DeleteCondition) -> Result<(), RustyAntError> {
        match cond {
            DeleteCondition::Any => {
                // Read the index → transact (delete data, delete index).
                // No-op when the key isn't anywhere; matches Redis `DEL` on
                // a missing key.
                let Some(kind) = self.read_index_kind(redis_key).await? else {
                    return Ok(());
                };
                let table = self.tables.for_kind(kind);
                let data_delete = Delete::builder()
                    .table_name(table)
                    .key(ATTR_PK, AttributeValue::S(redis_key.to_string()))
                    .build()
                    .map_err(|e| RustyAntError::S3(format!("dynamodb build delete (data): {e}")))?;
                let index_delete = Delete::builder()
                    .table_name(&self.tables.index)
                    .key(ATTR_PK, AttributeValue::S(redis_key.to_string()))
                    .build()
                    .map_err(|e| RustyAntError::S3(format!("dynamodb build delete (index): {e}")))?;
                let items = vec![
                    TransactWriteItem::builder().delete(data_delete).build(),
                    TransactWriteItem::builder().delete(index_delete).build(),
                ];
                match self.client.transact_write_items().set_transact_items(Some(items)).send().await {
                    Ok(_) => Ok(()),
                    Err(e) => {
                        let svc = e.into_service_error();
                        // A racing writer flipped the kind between our read
                        // and the transact. The other writer's state is the
                        // current truth — a `DEL Any` against the value they
                        // just wrote isn't what the caller asked for, so we
                        // surface as Contention and let them decide.
                        if matches!(svc, TransactWriteItemsError::TransactionCanceledException(_)) {
                            Err(RustyAntError::Contention)
                        } else {
                            Err(RustyAntError::S3(format!("dynamodb transact_write_items (delete): {svc}")))
                        }
                    }
                }
            }
            DeleteCondition::IfMatch(token) => {
                let (token_kind, old_version) = parse_token(&token)?;
                let table = self.tables.for_kind(token_kind);
                let data_delete = Delete::builder()
                    .table_name(table)
                    .key(ATTR_PK, AttributeValue::S(redis_key.to_string()))
                    .condition_expression(format!("{ATTR_VERSION} = :old"))
                    .expression_attribute_values(":old", AttributeValue::S(old_version))
                    .build()
                    .map_err(|e| RustyAntError::S3(format!("dynamodb build delete (data): {e}")))?;
                let index_delete = Delete::builder()
                    .table_name(&self.tables.index)
                    .key(ATTR_PK, AttributeValue::S(redis_key.to_string()))
                    .condition_expression(format!("{ATTR_KIND} = :ok"))
                    .expression_attribute_values(":ok", AttributeValue::S(token_kind.as_str().to_string()))
                    .build()
                    .map_err(|e| RustyAntError::S3(format!("dynamodb build delete (index): {e}")))?;
                let items = vec![
                    TransactWriteItem::builder().delete(data_delete).build(),
                    TransactWriteItem::builder().delete(index_delete).build(),
                ];
                match self.client.transact_write_items().set_transact_items(Some(items)).send().await {
                    Ok(_) => Ok(()),
                    Err(e) => {
                        let svc = e.into_service_error();
                        if matches!(svc, TransactWriteItemsError::TransactionCanceledException(_)) {
                            Err(RustyAntError::Contention)
                        } else {
                            Err(RustyAntError::S3(format!("dynamodb transact_write_items (delete): {svc}")))
                        }
                    }
                }
            }
        }
    }

    async fn list_page(&self, cursor: Option<String>, max_keys: usize) -> Result<ListPage, RustyAntError> {
        let (keys, next_cursor) = self.scan_index(cursor, max_keys).await?;
        Ok(ListPage { keys, next_cursor })
    }
}

// ---------------------------------------------------------------------------
// Item / token codec
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
    let kind = parse_kind(kind_str)?;
    Ok((kind, version.to_string()))
}

fn parse_kind(s: &str) -> Result<ValueKind, RustyAntError> {
    match s {
        "string" => Ok(ValueKind::String),
        "hash" => Ok(ValueKind::Hash),
        "list" => Ok(ValueKind::List),
        "set" => Ok(ValueKind::Set),
        "zset" => Ok(ValueKind::ZSet),
        "stream" => Ok(ValueKind::Stream),
        other => Err(RustyAntError::Parse(format!("unknown kind: {other}"))),
    }
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
    fn table_names_with_prefix_yields_one_per_kind_plus_index() {
        let tn = TableNames::with_prefix("rustyant-");
        assert_eq!(tn.index, "rustyant-index");
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
    fn table_names_iter_walks_six_kind_tables_in_value_kind_order() {
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
    fn parse_kind_round_trip_for_all() {
        for kind in ValueKind::ALL {
            assert_eq!(parse_kind(kind.as_str()).unwrap(), kind);
        }
        assert!(parse_kind("nope").is_err());
    }
}
