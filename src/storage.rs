use std::collections::{BTreeMap, BTreeSet};
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::primitives::ByteStream;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::error::RustyAntError;
use crate::hll;
use crate::stream::{
    AddIdSpec, ConsumerGroup, GroupStartId, PendingEntry, RangeId, StreamEntry, StreamId, StreamValue, TrimBound,
    XAutoClaimResult, XClaimOpts, XClaimResult, XGroupOp, XInfoConsumer, XInfoGroup, XPendingDetailRow,
    XPendingSummary, XReadGroupId, resolve_add_id, trim_in_place,
};

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
    Stream(crate::stream::StreamValue),
}

#[derive(Debug)]
pub enum TtlResult {
    NoKey,
    NoExpire,
    Ms(i64),
}

/// TTL mutation requested by a `GETEX`.
///
/// `Leave` is the option-less call (pure GET), `SetExpireAtMs` is any of
/// `EX`/`PX`/`EXAT`/`PXAT` (pre-resolved to an absolute unix-time in ms by
/// the handler), and `Persist` clears any existing expiry.
#[derive(Debug, Copy, Clone)]
pub enum GetExOp {
    Leave,
    SetExpireAtMs(i64),
    Persist,
}

/// Keyspace summary for `INFO keyspace`.
///
/// `keys_with_expire` is reported as `0` by the default trait impl —
/// computing it exactly over S3 would require a GET per object, which the
/// keyspace page doesn't justify. A backend that can answer cheaply may
/// override [`Storage::keyspace_stats`].
#[derive(Debug, Clone, Copy)]
pub struct KeyspaceStats {
    pub total_keys: i64,
    pub keys_with_expire: i64,
}

/// Flags controlling a `ZADD`-family insert.
///
/// `INCR` is not a flag here because it changes the return type (new score
/// vs. count) — commands dispatch to `zadd_ext_incr` instead. Mutual
/// exclusions (`nx` with `xx` / `gt` / `lt`; `gt` with `lt`) are validated at
/// the command layer — the storage impl trusts the caller.
#[derive(Debug, Clone, Copy, Default)]
#[allow(clippy::struct_excessive_bools)] // mirrors Redis's ZADD flag layout 1:1
pub struct ZAddFlags {
    /// `NX`: only insert new members; leave existing scores alone.
    pub nx: bool,
    /// `XX`: only update existing members; refuse to add new ones.
    pub xx: bool,
    /// `GT`: only update if the new score is strictly greater than the old.
    /// New members (no old score) are still added.
    pub gt: bool,
    /// `LT`: only update if the new score is strictly less than the old.
    /// New members (no old score) are still added.
    pub lt: bool,
    /// `CH`: count (added + score-changed) rather than just newly added.
    pub ch: bool,
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

    /// True when `score` satisfies the minimum-side bound.
    pub fn ge_min(self, score: f64) -> bool {
        match self {
            Self::Inclusive(v) => score >= v,
            Self::Exclusive(v) => score > v,
            Self::MinusInf => true,
            Self::PlusInf => false,
        }
    }

    /// True when `score` satisfies the maximum-side bound.
    pub fn le_max(self, score: f64) -> bool {
        match self {
            Self::Inclusive(v) => score <= v,
            Self::Exclusive(v) => score < v,
            Self::MinusInf => false,
            Self::PlusInf => true,
        }
    }
}

/// Lex-bound for `ZRANGEBYLEX` / `ZLEXCOUNT`.
///
/// Matches Redis's syntax: `[member` is inclusive, `(member` is exclusive,
/// `-` / `+` are the extremes. Comparison is byte-wise (Redis's `memcmp`),
/// so it only makes sense when all members share the same score —
/// documented at both ends.
#[derive(Debug, Clone)]
pub enum LexBound {
    Inclusive(String),
    Exclusive(String),
    MinusInf,
    PlusInf,
}

impl LexBound {
    pub fn parse(s: &str) -> Result<Self, RustyAntError> {
        match s {
            "-" => Ok(Self::MinusInf),
            "+" => Ok(Self::PlusInf),
            other => other.strip_prefix('[').map(|rest| Self::Inclusive(rest.to_string())).map_or_else(
                || {
                    other.strip_prefix('(').map_or_else(
                        || Err(RustyAntError::Parse("min or max not valid string range item".into())),
                        |rest| Ok(Self::Exclusive(rest.to_string())),
                    )
                },
                Ok,
            ),
        }
    }

    /// True when `member` satisfies the minimum-side bound.
    pub fn ge_min(&self, member: &str) -> bool {
        match self {
            Self::Inclusive(v) => member >= v.as_str(),
            Self::Exclusive(v) => member > v.as_str(),
            Self::MinusInf => true,
            Self::PlusInf => false,
        }
    }

    /// True when `member` satisfies the maximum-side bound.
    pub fn le_max(&self, member: &str) -> bool {
        match self {
            Self::Inclusive(v) => member <= v.as_str(),
            Self::Exclusive(v) => member < v.as_str(),
            Self::MinusInf => false,
            Self::PlusInf => true,
        }
    }
}

pub fn now_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}

pub(crate) fn is_expired(v: &StoredValue) -> bool {
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

/// 0-based ascending rank of `member` in a zset: sort by (score asc, member
/// asc for tied scores) — the Redis canonical ordering — and return the
/// member's index, or `None` if absent.
fn asc_rank_of(map: &BTreeMap<String, f64>, member: &str) -> Option<i64> {
    if !map.contains_key(member) {
        return None;
    }
    let mut sorted: Vec<(&str, f64)> = map.iter().map(|(m, s)| (m.as_str(), *s)).collect();
    sorted.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal).then_with(|| a.0.cmp(b.0)));
    let pos = sorted.iter().position(|(m, _)| *m == member)?;
    Some(i64::try_from(pos).unwrap_or(i64::MAX))
}

fn wrong_type(key: &str) -> RustyAntError {
    RustyAntError::WrongType { key: key.to_string() }
}

/// Variant of `wrong_type` for use inside CAS closures where the `key`
/// borrow doesn't reach. Reports as a generic WRONGTYPE error.
const fn wrong_type_error() -> RustyAntError {
    RustyAntError::WrongType { key: String::new() }
}

/// Group does not exist on this stream — Redis's exact wording.
fn no_group_error(group: &str) -> RustyAntError {
    RustyAntError::Parse(format!("NOGROUP No such consumer group '{group}' for key name. The group does not exist."))
}

/// Apply an `XGroupOp` to the stream value in-place. Returns the integer
/// reply value the caller should propagate (Redis: `:1` / `:0` for most
/// subcommands; the count of pending entries owned by a deleted consumer
/// for `DELCONSUMER`).
fn apply_xgroup_op(stream: &mut StreamValue, op: &XGroupOp, existed: bool) -> Result<i64, RustyAntError> {
    match op {
        XGroupOp::Create { group, start_id, mkstream } => {
            if !existed && !*mkstream {
                return Err(RustyAntError::Parse(
                    "ERR The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically.".into(),
                ));
            }
            if stream.groups.contains_key(group) {
                return Err(RustyAntError::Parse("BUSYGROUP Consumer Group name already exists".into()));
            }
            let last_delivered_id = match start_id {
                GroupStartId::Concrete(id) => *id,
                GroupStartId::Latest => stream.last_generated_id,
            };
            stream.groups.insert(group.clone(), ConsumerGroup { last_delivered_id, ..ConsumerGroup::default() });
            Ok(1)
        }
        XGroupOp::SetId { group, start_id } => {
            let g = stream.groups.get_mut(group).ok_or_else(|| no_group_error(group))?;
            g.last_delivered_id = match start_id {
                GroupStartId::Concrete(id) => *id,
                GroupStartId::Latest => stream.last_generated_id,
            };
            Ok(1)
        }
        XGroupOp::Destroy { group } => Ok(i64::from(stream.groups.remove(group).is_some())),
        XGroupOp::CreateConsumer { group, consumer } => {
            let g = stream.groups.get_mut(group).ok_or_else(|| no_group_error(group))?;
            if g.consumers.contains_key(consumer) {
                return Ok(0);
            }
            g.consumers.insert(consumer.clone(), crate::stream::Consumer::default());
            Ok(1)
        }
        XGroupOp::DelConsumer { group, consumer } => {
            let g = stream.groups.get_mut(group).ok_or_else(|| no_group_error(group))?;
            g.consumers.remove(consumer);
            let pending_owned =
                i64::try_from(g.pel.values().filter(|p| p.consumer == *consumer).count()).unwrap_or(i64::MAX);
            g.pel.retain(|_, p| p.consumer != *consumer);
            Ok(pending_owned)
        }
    }
}

/// Redis `TYPE` reply tag for a stored value.
const fn value_kind(value: &Value) -> &'static str {
    ValueKind::of(value).as_str()
}

/// Discriminant for [`Value`] used to route per-partition backend reads.
///
/// Single-partition backends (S3) ignore the tag; partitioned backends (a
/// future `DynamoDB` impl with one table per kind) use it to go straight to
/// the right partition without probing the others.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ValueKind {
    String,
    Hash,
    List,
    Set,
    ZSet,
    Stream,
}

impl ValueKind {
    #[must_use]
    pub const fn of(value: &Value) -> Self {
        match value {
            Value::String(_) => Self::String,
            Value::Hash(_) => Self::Hash,
            Value::List(_) => Self::List,
            Value::Set(_) => Self::Set,
            Value::ZSet(_) => Self::ZSet,
            Value::Stream(_) => Self::Stream,
        }
    }

    /// Redis `TYPE` reply tag.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::String => "string",
            Self::Hash => "hash",
            Self::List => "list",
            Self::Set => "set",
            Self::ZSet => "zset",
            Self::Stream => "stream",
        }
    }

    /// Full ordered list, for probe-all backends.
    pub const ALL: [Self; 6] = [Self::String, Self::Hash, Self::List, Self::Set, Self::ZSet, Self::Stream];
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

/// Sort a `ZSet` by (score asc, member asc) — the canonical Redis ordering.
fn sorted_zset(map: BTreeMap<String, f64>) -> Vec<(String, f64)> {
    let mut sorted: Vec<(String, f64)> = map.into_iter().collect();
    sorted.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal).then_with(|| a.0.cmp(&b.0)));
    sorted
}

/// Pull the f64 numeric representation out of a string-typed entry,
/// preserving any existing TTL. Used by `INCRBYFLOAT`.
fn parse_string_as_f64(entry: Option<&StoredValue>, key: &str) -> Result<(f64, Option<i64>), RustyAntError> {
    match entry {
        Some(StoredValue { value: Value::String(data), expires_at_ms }) => {
            let s = std::str::from_utf8(data).map_err(|_| RustyAntError::Parse("value is not a float".into()))?;
            let n: f64 = s.parse().map_err(|_| RustyAntError::Parse("value is not a float".into()))?;
            Ok((n, *expires_at_ms))
        }
        Some(_) => Err(wrong_type(key)),
        None => Ok((0.0, None)),
    }
}

/// Render a finite float the way Redis renders `INCRBYFLOAT` output: integer
/// values come back without a decimal, others via shortest round-trip.
fn format_float(v: f64) -> String {
    if v.fract() == 0.0 && v.abs() < 9.007_199_254_740_992e15 {
        #[allow(clippy::cast_possible_truncation)] // fract==0 && range checked
        let as_int = v as i64;
        return as_int.to_string();
    }
    format!("{v}")
}

/// Redis `GETRANGE` substring: inclusive end, negative indices relative to
/// end, out-of-range collapses to empty. Operates on raw bytes, not UTF-8.
fn slice_string_range(data: &[u8], start: i64, end: i64) -> Vec<u8> {
    if data.is_empty() {
        return Vec::new();
    }
    let len = i64::try_from(data.len()).unwrap_or(i64::MAX);
    let s = if start < 0 { (len + start).max(0) } else { start };
    let e = if end < 0 { len + end } else { end.min(len - 1) };
    if s >= len || e < 0 || s > e {
        return Vec::new();
    }
    let su = usize::try_from(s).unwrap_or(0);
    let eu = usize::try_from(e).unwrap_or(0);
    data[su..=eu].to_vec()
}

/// Redis `SETRANGE` in-place overwrite: pad with zero bytes out to `offset`
/// if the existing string is shorter, then splat `value` starting at `offset`,
/// extending the buffer if the write runs past the end.
fn apply_setrange(data: &mut Vec<u8>, offset: usize, value: &[u8]) {
    if data.len() < offset {
        data.resize(offset, 0);
    }
    let end = offset + value.len();
    if data.len() < end {
        data.resize(end, 0);
    }
    data[offset..end].copy_from_slice(value);
}

/// Sort a `ZSet` by (score asc, member asc), then filter by the min/max
/// bounds.
fn filter_zset_by_score(map: BTreeMap<String, f64>, min: ScoreBound, max: ScoreBound) -> Vec<String> {
    sorted_zset(map).into_iter().filter(|(_, s)| min.ge_min(*s) && max.le_max(*s)).map(|(m, _)| m).collect()
}

/// Walk the `ZSet` in ascending member lex order and collect members whose
/// byte-wise key falls within the lex window. Callers apply any `LIMIT` on
/// the returned slice. Redis's lex-range commands only make sense when all
/// scores are equal; this does not validate that — same stance as Redis.
fn filter_zset_by_lex(map: &BTreeMap<String, f64>, min: &LexBound, max: &LexBound) -> Vec<String> {
    map.keys().filter(|m| min.ge_min(m.as_str()) && max.le_max(m.as_str())).cloned().collect()
}

/// Apply a `(offset, count)` limit to an already-ordered member list. `count`
/// <= 0 or `None` returns everything from `offset` onward. Offsets past the
/// end collapse to empty, matching Redis.
fn apply_lex_limit(mut members: Vec<String>, limit: Option<(i64, i64)>) -> Vec<String> {
    let Some((offset, count)) = limit else { return members };
    let offset_usize = usize::try_from(offset.max(0)).unwrap_or(0);
    if offset_usize >= members.len() {
        return Vec::new();
    }
    members.drain(..offset_usize);
    if count >= 0 {
        let cap = usize::try_from(count).unwrap_or(0);
        members.truncate(cap);
    }
    members
}

/// Number of members whose score falls within `[min, max]` (inclusive/
/// exclusive per `ScoreBound`).
fn count_zset_by_score(map: &BTreeMap<String, f64>, min: ScoreBound, max: ScoreBound) -> i64 {
    let n = map.values().filter(|s| min.ge_min(**s) && max.le_max(**s)).count();
    i64::try_from(n).unwrap_or(i64::MAX)
}

/// `ZREVRANGE` slice: sort ascending, reverse, then apply the rank window.
fn slice_zset_reversed(map: BTreeMap<String, f64>, start: i64, stop: i64) -> Vec<String> {
    let mut sorted = sorted_zset(map);
    sorted.reverse();
    let len = i64::try_from(sorted.len()).unwrap_or(i64::MAX);
    let Some((s, e)) = resolve_range(len, start, stop) else {
        return Vec::new();
    };
    sorted[s..=e].iter().map(|(m, _)| m.clone()).collect()
}

/// Split a zset into (kept, `removed_count`) based on an ascending rank range
/// — drives `ZREMRANGEBYRANK`.
fn partition_zset_by_rank(map: BTreeMap<String, f64>, start: i64, stop: i64) -> (BTreeMap<String, f64>, usize) {
    let sorted = sorted_zset(map);
    let len = i64::try_from(sorted.len()).unwrap_or(i64::MAX);
    let Some((s, e)) = resolve_range(len, start, stop) else {
        return (sorted.into_iter().collect(), 0);
    };
    let mut kept = BTreeMap::new();
    let mut removed = 0usize;
    for (i, (m, score)) in sorted.into_iter().enumerate() {
        if i >= s && i <= e {
            removed += 1;
        } else {
            kept.insert(m, score);
        }
    }
    (kept, removed)
}

/// Split a zset into (kept, `removed_count`) based on a score range — drives
/// `ZREMRANGEBYSCORE`.
fn partition_zset_by_score(
    map: BTreeMap<String, f64>,
    min: ScoreBound,
    max: ScoreBound,
) -> (BTreeMap<String, f64>, usize) {
    let mut kept = BTreeMap::new();
    let mut removed = 0usize;
    for (m, score) in map {
        if min.ge_min(score) && max.le_max(score) {
            removed += 1;
        } else {
            kept.insert(m, score);
        }
    }
    (kept, removed)
}

/// Take up to `count` members from one end of a zset. `from_max=true` pops
/// the highest-ranked (descending score, then lex desc within a tie);
/// otherwise pops from the bottom.
fn pop_zset_edge(
    map: BTreeMap<String, f64>,
    count: usize,
    from_max: bool,
) -> (BTreeMap<String, f64>, Vec<(String, f64)>) {
    if count == 0 || map.is_empty() {
        return (map, Vec::new());
    }
    let mut sorted = sorted_zset(map);
    if from_max {
        sorted.reverse();
    }
    let take = count.min(sorted.len());
    let mut popped: Vec<(String, f64)> = Vec::with_capacity(take);
    let mut kept = BTreeMap::new();
    for (i, (m, score)) in sorted.into_iter().enumerate() {
        if i < take {
            popped.push((m, score));
        } else {
            kept.insert(m, score);
        }
    }
    (kept, popped)
}

/// Time-seeded xorshift — good enough for Redis `SPOP` / `SRANDMEMBER`, which
/// only require "random-ish" sampling, not cryptographic randomness. Also
/// used by [`crate::dynamodb`] to mint per-write CAS version tokens.
pub(crate) fn pseudo_rand_u64() -> u64 {
    let nanos: u64 =
        SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX));
    let mut x = nanos ^ 0x2545_F491_4F6C_DD1D_u64;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    x
}

/// Translate `start` / `stop` into absolute indices for `LTRIM`. Returns
/// `None` when the resulting slice would be empty — Redis drops the key
/// entirely in that case, unlike `LRANGE` which only returns an empty reply.
/// The pair is `(keep_start, keep_stop_exclusive)`.
fn resolve_trim_range(len: usize, start: i64, stop: i64) -> Option<(usize, usize)> {
    if len == 0 {
        return None;
    }
    let len_i = i64::try_from(len).unwrap_or(i64::MAX);
    let s = if start < 0 { (len_i + start).max(0) } else { start };
    let e = if stop < 0 { len_i + stop } else { stop.min(len_i - 1) };
    if s >= len_i || e < 0 || s > e {
        return None;
    }
    Some((usize::try_from(s).unwrap_or(0), usize::try_from(e).unwrap_or(0) + 1))
}

/// Sequentially load every key as a `Set`. Missing keys contribute an empty
/// set (Redis semantics for `SINTER`/`SUNION`/`SDIFF`); any key that exists
/// with a non-set type bubbles up a `WRONGTYPE` error.
async fn load_sets_seq<S: Storage + ?Sized>(
    storage: &S,
    keys: &[String],
) -> Result<Vec<BTreeSet<String>>, RustyAntError> {
    let mut out = Vec::with_capacity(keys.len());
    for k in keys {
        let members = storage.smembers(k).await?;
        out.push(members.into_iter().collect());
    }
    Ok(out)
}

/// Intersection of `sets` — members present in every set. An empty input or
/// any empty set collapses the result to empty.
fn set_intersection(sets: Vec<BTreeSet<String>>) -> Vec<String> {
    let mut iter = sets.into_iter();
    let Some(first) = iter.next() else {
        return Vec::new();
    };
    iter.fold(first, |acc, s| acc.intersection(&s).cloned().collect()).into_iter().collect()
}

/// Union of all members across `sets`.
fn set_union(sets: Vec<BTreeSet<String>>) -> Vec<String> {
    let mut out = BTreeSet::new();
    for s in sets {
        out.extend(s);
    }
    out.into_iter().collect()
}

/// Members in the first set that are absent from every subsequent set.
fn set_difference(sets: Vec<BTreeSet<String>>) -> Vec<String> {
    let mut iter = sets.into_iter();
    let Some(first) = iter.next() else {
        return Vec::new();
    };
    let mut remaining = first;
    for s in iter {
        remaining = remaining.difference(&s).cloned().collect();
    }
    remaining.into_iter().collect()
}

/// Pick up to `count` unique members from `set` via pseudo-random rotation.
/// Used by `SPOP` (which mutates the caller's copy afterwards) and by the
/// positive-count branch of `SRANDMEMBER`.
fn pick_random_unique(set: &BTreeSet<String>, count: usize) -> Vec<String> {
    if set.is_empty() || count == 0 {
        return Vec::new();
    }
    let ordered: Vec<&String> = set.iter().collect();
    let take = count.min(ordered.len());
    let start = usize::try_from(pseudo_rand_u64() % (ordered.len() as u64)).unwrap_or(0);
    (0..take).map(|i| ordered[(start + i) % ordered.len()].clone()).collect()
}

/// `SRANDMEMBER` sampler: `count` is the absolute length wanted, and
/// `allow_duplicates` flips between Redis's positive-count (unique) and
/// negative-count (with repetition) semantics.
fn pick_random(set: &BTreeSet<String>, count: i64, allow_duplicates: bool) -> Vec<String> {
    if set.is_empty() || count <= 0 {
        return Vec::new();
    }
    let count_usize = usize::try_from(count).unwrap_or(0);
    let ordered: Vec<&String> = set.iter().collect();
    if !allow_duplicates {
        return pick_random_unique(set, count_usize);
    }
    let mut rng = pseudo_rand_u64();
    (0..count_usize)
        .map(|_| {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            let idx = usize::try_from(rng % (ordered.len() as u64)).unwrap_or(0);
            ordered[idx].clone()
        })
        .collect()
}

/// `HRANDFIELD` sampler: uniform over fields. Returns `(field, value)` pairs
/// so the handler can serialize with or without values. Shares the
/// positive/negative count semantics of `pick_random`.
fn pick_random_from_hash(
    map: &BTreeMap<String, Vec<u8>>,
    count: i64,
    allow_duplicates: bool,
) -> Vec<(String, Vec<u8>)> {
    if map.is_empty() || count == 0 {
        return Vec::new();
    }
    let ordered: Vec<(&String, &Vec<u8>)> = map.iter().collect();
    let len = ordered.len();
    if !allow_duplicates {
        let take = usize::try_from(count).unwrap_or(0).min(len);
        let start = usize::try_from(pseudo_rand_u64() % (len as u64)).unwrap_or(0);
        return (0..take)
            .map(|i| (ordered[(start + i) % len].0.clone(), ordered[(start + i) % len].1.clone()))
            .collect();
    }
    let take = usize::try_from(count.unsigned_abs()).unwrap_or(0);
    let mut rng = pseudo_rand_u64();
    (0..take)
        .map(|_| {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            let idx = usize::try_from(rng % (len as u64)).unwrap_or(0);
            (ordered[idx].0.clone(), ordered[idx].1.clone())
        })
        .collect()
}

/// `ZRANDMEMBER` sampler: uniform over members (Redis ignores scores for
/// weighting). Returns `(member, score)` pairs so the handler can format
/// with or without scores. Mirrors `pick_random`'s positive/negative count
/// semantics.
fn pick_random_from_zset(map: &BTreeMap<String, f64>, count: i64, allow_duplicates: bool) -> Vec<(String, f64)> {
    if map.is_empty() || count == 0 {
        return Vec::new();
    }
    let ordered: Vec<(&String, &f64)> = map.iter().collect();
    let len = ordered.len();
    if !allow_duplicates {
        // Positive count: up to `count` distinct members.
        let take = usize::try_from(count).unwrap_or(0).min(len);
        let start = usize::try_from(pseudo_rand_u64() % (len as u64)).unwrap_or(0);
        return (0..take).map(|i| (ordered[(start + i) % len].0.clone(), *ordered[(start + i) % len].1)).collect();
    }
    // Negative count: exactly |count| samples, duplicates allowed.
    let take = usize::try_from(count.unsigned_abs()).unwrap_or(0);
    let mut rng = pseudo_rand_u64();
    (0..take)
        .map(|_| {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            let idx = usize::try_from(rng % (len as u64)).unwrap_or(0);
            (ordered[idx].0.clone(), *ordered[idx].1)
        })
        .collect()
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

/// Conditional-write specification for [`KVBackend::save`].
///
/// `Any` writes unconditionally (overwriting any existing entry), `CreateOnly`
/// requires the key to be absent (maps to `If-None-Match: *` on S3), and
/// `IfMatch(etag)` requires the current entry to carry `etag` (maps to
/// `If-Match`).
#[derive(Debug, Clone)]
pub enum WriteCondition {
    Any,
    CreateOnly,
    IfMatch(String),
}

/// Conditional-delete specification for [`KVBackend::delete`].
///
/// `Any` deletes unconditionally (no-op if the key is missing);
/// `IfMatch(etag)` requires the current entry to carry `etag` — used to drop
/// collections that have been emptied by a CAS loop without stomping a
/// racing writer's new value.
#[derive(Debug, Clone)]
pub enum DeleteCondition {
    Any,
    IfMatch(String),
}

/// One page of keys emitted by [`KVBackend::list_page`]. `next_cursor` is
/// `None` on the last page; clients that want the full keyspace loop until
/// they see a `None`.
#[derive(Debug)]
pub struct ListPage {
    pub keys: Vec<String>,
    pub next_cursor: Option<String>,
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

// ---------------------------------------------------------------------------
// KVBackend — the raw key/value primitives every backend provides.
//
// This is the abstraction boundary between rustyant's command logic and its
// persistence substrate. All the Redis-shaped operations (`HGET`, `LPUSH`,
// `ZADD`, …) live in the blanket [`Storage`] impl on [`KVStorage`], expressed
// in terms of just four primitives:
//
//   load(key)       -> Option<(entry, version)>
//   save(key, cond) -> ()                         (CreateOnly / IfMatch / Any)
//   delete(key, c)  -> ()                         (IfMatch / Any)
//   list_page(cur)  -> (keys, next_cursor)
//
// Add a backend by implementing these four methods — every Redis command
// comes along for free via the shared [`KVStorage`] wrapper.
//
// S3 maps `load` to `GetObject` + `ETag` / `save` to `PutObject` with
// `If-Match` / `delete` to `DeleteObject` with `If-Match` / `list_page` to
// `ListObjectsV2`. A future DynamoDB backend maps the same four to
// `GetItem` / `PutItem` with `ConditionExpression` / `DeleteItem` /
// `Scan` respectively.
//
// Contention: 412 / ConditionalCheckFailed surface as
// [`RustyAntError::Contention`]. The shared CAS loop in `KVStorage::cas`
// handles the retry.
// ---------------------------------------------------------------------------

#[async_trait]
pub trait KVBackend: Send + Sync + std::fmt::Debug {
    /// Load the entry at `redis_key` and its backend-specific version token
    /// (an S3 `ETag`, a `DynamoDB` version attribute, etc). Returns `None` for
    /// missing or expired entries; backends should GC expired entries
    /// opportunistically on encounter.
    ///
    /// On per-partition backends this probes every partition and resolves
    /// cross-partition divergence by a deterministic fallback order — a key
    /// that ended up in multiple kind tables (because a concurrent race or a
    /// deliberate cross-kind skip-of-cleanup left both rows alive) resolves
    /// to whichever partition appears first in [`ValueKind::ALL`].
    async fn load(&self, redis_key: &str) -> Result<Option<(StoredValue, String)>, RustyAntError>;

    /// Write `entry` at `redis_key` under `cond`. A failed precondition
    /// surfaces as `Err(RustyAntError::Contention)`.
    async fn save(&self, redis_key: &str, entry: &StoredValue, cond: WriteCondition) -> Result<(), RustyAntError>;

    /// Delete `redis_key` under `cond`. `DeleteCondition::Any` is a no-op if
    /// the key is missing; `IfMatch(etag)` surfaces `Err(Contention)` on a
    /// version mismatch.
    async fn delete(&self, redis_key: &str, cond: DeleteCondition) -> Result<(), RustyAntError>;

    /// Paginate the full keyspace. Passing `cursor=None` starts a fresh walk;
    /// `max_keys` bounds the page size (advisory — backends may return fewer).
    /// The returned `next_cursor` is `None` when the walk is exhausted.
    async fn list_page(&self, cursor: Option<String>, max_keys: usize) -> Result<ListPage, RustyAntError>;

    /// Bulk-delete every key in the namespace. Default impl walks `list_page`
    /// and deletes one-by-one; backends with a native bulk-delete (S3's
    /// `DeleteObjects`, `DynamoDB`'s `BatchWriteItem`) should override for a
    /// ~1000x reduction in API calls on large keyspaces.
    async fn flush_all(&self) -> Result<(), RustyAntError> {
        let mut cursor = None;
        loop {
            let page = self.list_page(cursor, 1000).await?;
            for k in &page.keys {
                // Best-effort delete — swallow per-key errors so one missing
                // key doesn't abort the flush (matches the batched path).
                let _ = self.delete(k, DeleteCondition::Any).await;
            }
            match page.next_cursor {
                Some(c) => cursor = Some(c),
                None => break,
            }
        }
        Ok(())
    }

    /// Count live keys. Default impl walks `list_page`; same caveat as Redis
    /// `DBSIZE` — recently-expired-but-unGC'd keys still count.
    async fn count_keys(&self) -> Result<i64, RustyAntError> {
        let mut count: i64 = 0;
        let mut cursor = None;
        loop {
            let page = self.list_page(cursor, 1000).await?;
            count = count.saturating_add(i64::try_from(page.keys.len()).unwrap_or(i64::MAX));
            match page.next_cursor {
                Some(c) => cursor = Some(c),
                None => break,
            }
        }
        Ok(count)
    }
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
// Storage trait — the command-facing persistence API. `S3Storage` is the
// only implementor; tests run against it pointed at a floci emulator.
// ---------------------------------------------------------------------------

#[async_trait]
#[allow(clippy::too_many_arguments)] // xclaim / xreadgroup / xautoclaim mirror Redis's option surface
pub trait Storage: Send + Sync + std::fmt::Debug {
    async fn delete(&self, key: &str) -> Result<bool, RustyAntError>;
    async fn exists(&self, key: &str) -> Result<bool, RustyAntError>;
    async fn expire_at(&self, key: &str, expires_at_ms: i64) -> Result<bool, RustyAntError>;
    async fn ttl_ms(&self, key: &str) -> Result<TtlResult, RustyAntError>;
    /// Absolute expiry — like [`Self::ttl_ms`] but returns the stored epoch-ms
    /// instead of a delta from "now". Drives `EXPIRETIME` / `PEXPIRETIME`.
    async fn expire_time_ms(&self, key: &str) -> Result<TtlResult, RustyAntError>;
    /// Redis `TYPE` — returns the value-kind tag (`"string"`, `"hash"`,
    /// `"list"`, `"set"`, `"zset"`) or `None` when the key is missing/expired.
    async fn kind(&self, key: &str) -> Result<Option<&'static str>, RustyAntError>;

    /// Approximate byte size of the value for `MEMORY USAGE`. `None` for
    /// missing / expired keys. Returns the serialized-JSON byte length on
    /// the S3 backend (what the object actually occupies), which is the
    /// closest honest signal we have without an in-process allocator.
    async fn mem_usage(&self, key: &str) -> Result<Option<i64>, RustyAntError>;

    async fn get_string(&self, key: &str) -> Result<Option<Bytes>, RustyAntError>;
    /// `GETEX`: return the string value (`None` when missing / wrong type is an
    /// error) and atomically adjust its TTL per `op` in the same read-modify-
    /// write. Same CAS window as every other string mutation on the S3 backend.
    async fn get_string_with_ttl(&self, key: &str, op: GetExOp) -> Result<Option<Bytes>, RustyAntError>;
    async fn set_string(&self, key: &str, value: Bytes, expires_at_ms: Option<i64>) -> Result<(), RustyAntError>;
    async fn set_string_nx(&self, key: &str, value: Bytes, expires_at_ms: Option<i64>) -> Result<bool, RustyAntError>;
    async fn getset(&self, key: &str, value: Bytes) -> Result<Option<Bytes>, RustyAntError>;
    async fn get_and_delete(&self, key: &str) -> Result<Option<Bytes>, RustyAntError>;
    async fn strlen(&self, key: &str) -> Result<i64, RustyAntError>;
    async fn append(&self, key: &str, value: Bytes) -> Result<i64, RustyAntError>;
    /// Redis `PFADD`: add each element to the HLL at `key`. Returns `true`
    /// if any register was updated (i.e. at least one element hashed to a
    /// higher leading-zero run than the current bucket value). Creates an
    /// empty dense HLL on missing keys. Errors if `key` exists as a
    /// non-HLL string or a non-string kind.
    async fn pfadd(&self, key: &str, elements: &[Bytes]) -> Result<bool, RustyAntError>;
    /// Redis `PFCOUNT`: reads the HLL(s) at `keys`; for a single key
    /// returns its cardinality, for multiple the cardinality of the
    /// in-memory register-wise union. Missing keys contribute nothing.
    async fn pfcount(&self, keys: &[String]) -> Result<u64, RustyAntError>;
    /// Redis `PFMERGE`: unions `sources` into `dest` (creating `dest` if
    /// it did not exist). `dest`'s prior HLL participates in the union,
    /// matching Redis.
    async fn pfmerge(&self, dest: &str, sources: &[String]) -> Result<(), RustyAntError>;
    async fn incr_by(&self, key: &str, delta: i64) -> Result<i64, RustyAntError>;
    async fn incr_by_float(&self, key: &str, delta: f64) -> Result<f64, RustyAntError>;
    async fn getrange(&self, key: &str, start: i64, end: i64) -> Result<Bytes, RustyAntError>;
    async fn setrange(&self, key: &str, offset: usize, value: Bytes) -> Result<i64, RustyAntError>;
    async fn msetnx(&self, pairs: Vec<(String, Bytes)>) -> Result<bool, RustyAntError>;
    async fn persist(&self, key: &str) -> Result<bool, RustyAntError>;
    async fn rename(&self, from: &str, to: &str) -> Result<(), RustyAntError>;
    async fn renamenx(&self, from: &str, to: &str) -> Result<bool, RustyAntError>;

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
    async fn hsetnx(&self, key: &str, field: &str, value: Bytes) -> Result<bool, RustyAntError>;
    async fn hget(&self, key: &str, field: &str) -> Result<Option<Bytes>, RustyAntError>;
    async fn hdel(&self, key: &str, fields: &[String]) -> Result<i64, RustyAntError>;
    async fn hgetall(&self, key: &str) -> Result<Vec<(String, Bytes)>, RustyAntError>;
    async fn hlen(&self, key: &str) -> Result<i64, RustyAntError>;
    async fn hkeys(&self, key: &str) -> Result<Vec<String>, RustyAntError>;
    async fn hvals(&self, key: &str) -> Result<Vec<Bytes>, RustyAntError>;
    async fn hexists(&self, key: &str, field: &str) -> Result<bool, RustyAntError>;
    async fn hstrlen(&self, key: &str, field: &str) -> Result<i64, RustyAntError>;
    async fn hmget(&self, key: &str, fields: &[String]) -> Result<Vec<Option<Bytes>>, RustyAntError>;
    async fn hincr_by(&self, key: &str, field: &str, delta: i64) -> Result<i64, RustyAntError>;
    async fn hincr_by_float(&self, key: &str, field: &str, delta: f64) -> Result<f64, RustyAntError>;
    /// Redis `HRANDFIELD` sampler: uniform over fields. Positive `count` ⇒
    /// distinct fields (capped at hash size); negative ⇒ exactly `|count|`
    /// with duplicates. Returns `(field, value)` pairs; the handler decides
    /// whether to serialize values.
    async fn hrandfield(
        &self,
        key: &str,
        count: i64,
        allow_duplicates: bool,
    ) -> Result<Vec<(String, Vec<u8>)>, RustyAntError>;
    /// Incremental iteration over a hash's `(field, value)` pairs. `cursor=0`
    /// starts a fresh scan; a non-zero return cursor means more pages remain,
    /// `0` means the scan is exhausted. `count` bounds the batch size; `MATCH`
    /// filtering runs after the batch is sliced, matching Redis's "COUNT is
    /// advisory, MATCH can shrink a page" semantic. Both backends load the
    /// full hash per call — pagination is a client-side ergonomic, not a
    /// server-side cost saving, because the collection is one S3 object.
    async fn hscan(
        &self,
        key: &str,
        cursor: u64,
        pattern: Option<&str>,
        count: usize,
    ) -> Result<(u64, Vec<(String, Bytes)>), RustyAntError>;

    async fn list_push(&self, key: &str, values: Vec<Bytes>, left: bool) -> Result<i64, RustyAntError>;
    async fn list_pushx(&self, key: &str, values: Vec<Bytes>, left: bool) -> Result<i64, RustyAntError>;
    async fn list_pop(&self, key: &str, count: usize, left: bool) -> Result<Vec<Bytes>, RustyAntError>;
    async fn lrange(&self, key: &str, start: i64, stop: i64) -> Result<Vec<Bytes>, RustyAntError>;
    async fn llen(&self, key: &str) -> Result<i64, RustyAntError>;
    async fn lindex(&self, key: &str, index: i64) -> Result<Option<Bytes>, RustyAntError>;
    async fn lset(&self, key: &str, index: i64, value: Bytes) -> Result<(), RustyAntError>;
    async fn lrem(&self, key: &str, count: i64, value: Bytes) -> Result<i64, RustyAntError>;
    async fn linsert(&self, key: &str, before: bool, pivot: Bytes, value: Bytes) -> Result<i64, RustyAntError>;
    async fn ltrim(&self, key: &str, start: i64, stop: i64) -> Result<(), RustyAntError>;
    /// Atomically pop one element from `from` (head if `from_left`, else tail)
    /// and push it onto `to` (head if `to_left`, else tail). Returns the moved
    /// element, or `None` when `from` is missing / empty. Same-key moves run
    /// under a single CAS; cross-key moves are two-step on S3 (pop first, then
    /// push) — same best-effort guarantee as `RENAME` / `COPY`.
    async fn list_move(
        &self,
        from: &str,
        to: &str,
        from_left: bool,
        to_left: bool,
    ) -> Result<Option<Bytes>, RustyAntError>;
    /// Return zero-based positions of `element` inside the list at `key`.
    /// `rank` picks the k-th match (`1` = first forward, `-1` = first backward,
    /// never `0`); `count` = `None` means "return the first match only",
    /// `Some(0)` means "all matches", `Some(n)` means "up to n matches".
    /// `maxlen` bounds how many elements are compared (`0` = unlimited).
    async fn lpos(
        &self,
        key: &str,
        element: &[u8],
        rank: i64,
        count: Option<usize>,
        maxlen: usize,
    ) -> Result<Vec<i64>, RustyAntError>;

    async fn sadd(&self, key: &str, members: Vec<String>) -> Result<i64, RustyAntError>;
    async fn srem(&self, key: &str, members: &[String]) -> Result<i64, RustyAntError>;
    async fn smembers(&self, key: &str) -> Result<Vec<String>, RustyAntError>;
    async fn sismember(&self, key: &str, member: &str) -> Result<bool, RustyAntError>;
    async fn smismember(&self, key: &str, members: &[String]) -> Result<Vec<bool>, RustyAntError>;
    async fn scard(&self, key: &str) -> Result<i64, RustyAntError>;
    async fn sinter(&self, keys: &[String]) -> Result<Vec<String>, RustyAntError>;
    async fn sunion(&self, keys: &[String]) -> Result<Vec<String>, RustyAntError>;
    async fn sdiff(&self, keys: &[String]) -> Result<Vec<String>, RustyAntError>;
    async fn spop(&self, key: &str, count: usize) -> Result<Vec<String>, RustyAntError>;
    async fn srandmember(&self, key: &str, count: i64, allow_duplicates: bool) -> Result<Vec<String>, RustyAntError>;
    /// See [`Self::hscan`] for `cursor` / `count` / `pattern` semantics.
    async fn sscan(
        &self,
        key: &str,
        cursor: u64,
        pattern: Option<&str>,
        count: usize,
    ) -> Result<(u64, Vec<String>), RustyAntError>;

    async fn zadd(&self, key: &str, pairs: Vec<(f64, String)>) -> Result<i64, RustyAntError>;
    /// `ZADD`-with-flags variant. Honours `NX` / `XX` / `GT` / `LT` / `CH`;
    /// see `ZAddFlags`. Returns newly-added count by default, or
    /// `added + updated` when `flags.ch` is set. Used by `GEOADD` and by
    /// `ZADD` itself when the command carries any of these flags.
    async fn zadd_ext(&self, key: &str, pairs: Vec<(f64, String)>, flags: ZAddFlags) -> Result<i64, RustyAntError>;
    /// `ZADD INCR` variant: add `delta` to `member`'s score (creating the
    /// member with score = `delta` if absent), honouring `NX` / `XX` / `GT`
    /// / `LT`. Returns the new score, or `None` when a flag suppressed the
    /// update.
    async fn zadd_ext_incr(
        &self,
        key: &str,
        delta: f64,
        member: &str,
        flags: ZAddFlags,
    ) -> Result<Option<f64>, RustyAntError>;
    /// Return every `(member, score)` pair in the ZSET at `key`, unsorted.
    /// `GEOSEARCH` uses this to scan the whole collection in one pass; on S3
    /// the ZSET is a single object so one call materializes the full set
    /// regardless.
    async fn zitems(&self, key: &str) -> Result<Vec<(String, f64)>, RustyAntError>;
    async fn zrem(&self, key: &str, members: &[String]) -> Result<i64, RustyAntError>;
    async fn zincr_by(&self, key: &str, member: &str, delta: f64) -> Result<f64, RustyAntError>;
    async fn zrange(&self, key: &str, start: i64, stop: i64) -> Result<Vec<String>, RustyAntError>;
    async fn zrevrange(&self, key: &str, start: i64, stop: i64) -> Result<Vec<String>, RustyAntError>;
    async fn zrangebyscore(&self, key: &str, min: ScoreBound, max: ScoreBound) -> Result<Vec<String>, RustyAntError>;
    async fn zrevrangebyscore(&self, key: &str, max: ScoreBound, min: ScoreBound)
    -> Result<Vec<String>, RustyAntError>;
    async fn zremrangebyrank(&self, key: &str, start: i64, stop: i64) -> Result<i64, RustyAntError>;
    async fn zremrangebyscore(&self, key: &str, min: ScoreBound, max: ScoreBound) -> Result<i64, RustyAntError>;
    /// Lex-ordered range read. `limit` is `(offset, count)`; `count` <= 0 or
    /// `None` means no cap. Results are in ascending member order.
    async fn zrangebylex(
        &self,
        key: &str,
        min: LexBound,
        max: LexBound,
        limit: Option<(i64, i64)>,
    ) -> Result<Vec<String>, RustyAntError>;
    /// `ZREVRANGEBYLEX` — Redis takes `(key, max, min)` order to signal the
    /// reverse direction; the storage layer mirrors that.
    async fn zrevrangebylex(
        &self,
        key: &str,
        max: LexBound,
        min: LexBound,
        limit: Option<(i64, i64)>,
    ) -> Result<Vec<String>, RustyAntError>;
    async fn zlexcount(&self, key: &str, min: LexBound, max: LexBound) -> Result<i64, RustyAntError>;
    async fn zremrangebylex(&self, key: &str, min: LexBound, max: LexBound) -> Result<i64, RustyAntError>;
    async fn zpopmin(&self, key: &str, count: usize) -> Result<Vec<(String, f64)>, RustyAntError>;
    async fn zpopmax(&self, key: &str, count: usize) -> Result<Vec<(String, f64)>, RustyAntError>;
    async fn zscore(&self, key: &str, member: &str) -> Result<Option<f64>, RustyAntError>;
    async fn zcard(&self, key: &str) -> Result<i64, RustyAntError>;
    async fn zrank(&self, key: &str, member: &str) -> Result<Option<i64>, RustyAntError>;
    async fn zrevrank(&self, key: &str, member: &str) -> Result<Option<i64>, RustyAntError>;
    async fn zcount(&self, key: &str, min: ScoreBound, max: ScoreBound) -> Result<i64, RustyAntError>;
    async fn zmscore(&self, key: &str, members: &[String]) -> Result<Vec<Option<f64>>, RustyAntError>;
    /// Redis `ZRANDMEMBER` sampler: uniform over members (scores do not bias
    /// the pick, per Redis). Positive `count` returns up to `count` distinct
    /// members; negative `count` returns exactly `|count|` with duplicates
    /// allowed. Returns `(member, score)` pairs — the handler decides whether
    /// to serialize scores.
    async fn zrandmember(
        &self,
        key: &str,
        count: i64,
        allow_duplicates: bool,
    ) -> Result<Vec<(String, f64)>, RustyAntError>;
    /// See [`Self::hscan`] for pagination semantics. Iteration order follows
    /// member lex order under the backing `BTreeMap`; Redis doesn't pin an
    /// order, so callers that care must sort the accumulated result.
    async fn zscan(
        &self,
        key: &str,
        cursor: u64,
        pattern: Option<&str>,
        count: usize,
    ) -> Result<(u64, Vec<(String, f64)>), RustyAntError>;

    // ---- Streams -------------------------------------------------------
    /// Redis `XADD`. Resolves the caller's id spec against the stream's
    /// `last_generated_id`, appends the entry, and optionally trims the
    /// head to satisfy a `MAXLEN` / `MINID` bound. Returns the id that
    /// was actually written.
    ///
    /// `nomkstream` set to true: behave like Redis's NOMKSTREAM — if the
    /// key does not exist, return `None` without creating it.
    async fn xadd(
        &self,
        key: &str,
        id: AddIdSpec,
        fields: Vec<(String, Vec<u8>)>,
        nomkstream: bool,
        trim: Option<(TrimBound, Option<usize>)>,
    ) -> Result<Option<StreamId>, RustyAntError>;

    /// Redis `XLEN`: number of entries in the stream. 0 for missing key;
    /// WRONGTYPE if the key exists as a different kind.
    async fn xlen(&self, key: &str) -> Result<i64, RustyAntError>;

    /// Redis `XRANGE` / `XREVRANGE`: entries whose id falls within
    /// `[start, end]`. `reverse=true` walks the matching slice from
    /// newest to oldest. `count` caps the number of returned entries.
    async fn xrange(
        &self,
        key: &str,
        start: RangeId,
        end: RangeId,
        count: Option<usize>,
        reverse: bool,
    ) -> Result<Vec<StreamEntry>, RustyAntError>;

    /// Redis `XDEL`: remove entries by id; returns the number deleted.
    async fn xdel(&self, key: &str, ids: &[StreamId]) -> Result<i64, RustyAntError>;

    /// Redis `XTRIM`: apply `bound` to the stream, returning the count
    /// of entries removed. `limit` caps the work per call (Redis's
    /// `LIMIT n` argument).
    async fn xtrim(&self, key: &str, bound: TrimBound, limit: Option<usize>) -> Result<i64, RustyAntError>;

    /// Redis `XREAD`: for each `(key, after_id)` pair, return entries
    /// strictly after `after_id`. `count` caps per-stream. The caller is
    /// responsible for shaping the `XREAD` reply — this just returns the
    /// flat map from stream name to its matching entries.
    async fn xread(
        &self,
        keys: &[String],
        after_ids: &[StreamId],
        count: Option<usize>,
    ) -> Result<Vec<(String, Vec<StreamEntry>)>, RustyAntError>;

    /// Redis `XINFO STREAM key`: headline summary of the stream.
    async fn xinfo_stream(&self, key: &str) -> Result<Option<StreamValue>, RustyAntError>;

    /// Redis `XGROUP CREATE / SETID / DESTROY / CREATECONSUMER / DELCONSUMER`.
    /// `op` chooses which subcommand to apply; `mkstream` is honored only
    /// for `Create`. Returns a generic int payload (created? / destroyed?
    /// / consumer-deleted-pending-count / etc.) — the handler interprets it.
    async fn xgroup(&self, key: &str, op: XGroupOp) -> Result<i64, RustyAntError>;

    /// Redis `XREADGROUP GROUP <group> <consumer> [COUNT n] [NOACK] STREAMS keys ids`.
    /// `>` ids fetch new entries (advance the group's `last_delivered_id`
    /// and add to the PEL unless `noack`); explicit ids re-read from the
    /// PEL filtering by `consumer`.
    async fn xreadgroup(
        &self,
        group: &str,
        consumer: &str,
        keys: &[String],
        ids: &[XReadGroupId],
        count: Option<usize>,
        noack: bool,
        now_ms_override: i64,
    ) -> Result<Vec<(String, Vec<StreamEntry>)>, RustyAntError>;

    /// Redis `XACK key group id [id ...]`. Removes acknowledged entries
    /// from the group's PEL; returns the count actually removed.
    async fn xack(&self, key: &str, group: &str, ids: &[StreamId]) -> Result<i64, RustyAntError>;

    /// Redis `XCLAIM`. Reassigns ownership of pending entries to
    /// `consumer` if they have been idle for at least `min_idle_ms`.
    /// `force` adds entries to the PEL even when not previously pending.
    /// Returns the entries that were claimed (or just their ids when
    /// `just_id` is set).
    #[allow(clippy::too_many_arguments)]
    async fn xclaim(
        &self,
        key: &str,
        group: &str,
        consumer: &str,
        min_idle_ms: i64,
        ids: &[StreamId],
        opts: XClaimOpts,
    ) -> Result<Vec<XClaimResult>, RustyAntError>;

    /// Redis `XPENDING key group` — summary form. Returns `(count, min_id,
    /// max_id, per_consumer_counts)` for the group's PEL.
    async fn xpending_summary(&self, key: &str, group: &str) -> Result<Option<XPendingSummary>, RustyAntError>;

    /// Redis `XPENDING key group [IDLE ms] start end count [consumer]`.
    /// Detail form — returns matching PEL rows.
    async fn xpending_detail(
        &self,
        key: &str,
        group: &str,
        start: RangeId,
        end: RangeId,
        count: usize,
        consumer: Option<&str>,
        idle_ms: Option<i64>,
        now_ms_override: i64,
    ) -> Result<Vec<XPendingDetailRow>, RustyAntError>;

    /// Redis `XAUTOCLAIM`. Sweeps PEL entries idle ≥ `min_idle_ms` and
    /// reassigns up to `count` of them to `consumer`. Returns the cursor
    /// (id to resume from on the next call), the entries claimed (or
    /// just their ids if `just_id`), and any ids that were dropped
    /// because they no longer exist in the stream.
    #[allow(clippy::too_many_arguments)]
    async fn xautoclaim(
        &self,
        key: &str,
        group: &str,
        consumer: &str,
        min_idle_ms: i64,
        start: StreamId,
        count: usize,
        just_id: bool,
        now_ms_override: i64,
    ) -> Result<XAutoClaimResult, RustyAntError>;

    /// Redis `XINFO GROUPS key`: per-group summary.
    async fn xinfo_groups(&self, key: &str) -> Result<Vec<XInfoGroup>, RustyAntError>;

    /// Redis `XINFO CONSUMERS key group`: per-consumer summary.
    async fn xinfo_consumers(&self, key: &str, group: &str) -> Result<Vec<XInfoConsumer>, RustyAntError>;

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

    /// Total live key count. On S3 this is a full `ListObjectsV2` walk
    /// (proportional to keyspace size); recently-expired keys that have
    /// not yet been GC'd are still counted, matching Redis's lazy-expiry
    /// behavior.
    async fn dbsize(&self) -> Result<i64, RustyAntError>;

    /// Counts for `INFO keyspace`. Default routes through [`Self::dbsize`] and
    /// reports `keys_with_expire = 0` — the in-memory backend overrides this
    /// with an exact count, but S3 would need a GET per object, which the
    /// keyspace page doesn't justify.
    async fn keyspace_stats(&self) -> Result<KeyspaceStats, RustyAntError> {
        Ok(KeyspaceStats { total_keys: self.dbsize().await?, keys_with_expire: 0 })
    }

    /// Wipe every key in the namespace. Drives both `FLUSHDB` and `FLUSHALL`
    /// — rustyant has only one logical database, so the two commands collapse
    /// to the same operation.
    async fn flushall(&self) -> Result<(), RustyAntError>;

    /// Pick one key uniformly at random from the live keyspace, or `None`
    /// when the namespace is empty. Backs `RANDOMKEY`.
    async fn random_key(&self) -> Result<Option<String>, RustyAntError>;

    /// Copy `from`'s value (and TTL) to `to`. Returns `true` when the copy
    /// happened, `false` when it was refused — source missing, destination
    /// already present without `replace`, or `from == to`. On S3 the two
    /// objects aren't modified atomically, same caveat as `RENAME`.
    async fn copy(&self, from: &str, to: &str, replace: bool) -> Result<bool, RustyAntError>;

    /// Read a single bit (0 or 1) at the given offset. Bit 0 is the MSB of
    /// byte 0, matching Redis's bit ordering. Out-of-range or missing key
    /// returns 0. Default impl reads the whole string and indexes locally.
    async fn getbit(&self, key: &str, offset: u64) -> Result<i64, RustyAntError> {
        let Some(data) = self.get_string(key).await? else {
            return Ok(0);
        };
        Ok(i64::from(bit_at(&data, offset)))
    }

    /// Set a single bit at `offset` to `value`, zero-padding the underlying
    /// string if needed. Returns the previous bit value (0 or 1). Mutates
    /// under CAS on the S3 backend.
    async fn setbit(&self, key: &str, offset: u64, value: bool) -> Result<i64, RustyAntError>;

    /// Redis `DUMP`: return the key's value encoded as a DUMP binary payload.
    /// `None` for missing / expired keys. Errors on value kinds whose RDB
    /// encoding is not yet supported (currently: streams).
    async fn dump(&self, key: &str) -> Result<Option<Bytes>, RustyAntError>;

    /// Redis `RESTORE`: decode `payload` and store it at `key`.
    ///
    /// * `ttl_ms` — 0 means "no expiry"; otherwise the TTL to stamp on the
    ///   new entry. If `abs_ttl` is true the value is already absolute
    ///   epoch-ms; otherwise it's a delta from "now".
    /// * `replace` — false rejects the write if `key` already exists (Redis
    ///   returns `BUSYKEY Target key name already exists`). True overwrites.
    ///
    /// Returns `Err(Parse)` for malformed / wrong-version / bad-CRC payloads.
    /// `idletime` and `freq` are accepted by the handler and ignored here —
    /// rustyant has no LRU/LFU tracking on the S3 backend.
    async fn restore(
        &self,
        key: &str,
        payload: &[u8],
        ttl_ms: i64,
        replace: bool,
        abs_ttl: bool,
    ) -> Result<(), RustyAntError>;
}

/// Read bit `offset` (Redis bit numbering: bit 0 = MSB of byte 0) from `data`.
/// Returns 0 when the offset is beyond the string.
pub fn bit_at(data: &[u8], offset: u64) -> u8 {
    let byte_idx = offset / 8;
    let Ok(byte_idx) = usize::try_from(byte_idx) else {
        return 0;
    };
    if byte_idx >= data.len() {
        return 0;
    }
    let bit_in_byte = 7 - (offset % 8) as u8;
    (data[byte_idx] >> bit_in_byte) & 1
}

/// Paginate a deterministic collection for `HSCAN` / `SSCAN` / `ZSCAN`.
/// `cursor` is an integer offset into the caller's sorted iteration; `count`
/// bounds the batch before `MATCH` filtering is applied — Redis applies the
/// pattern after the batch is sliced, so a single page can return fewer
/// items than `count`.
fn apply_collection_scan<T, F>(
    items: Vec<T>,
    cursor: u64,
    count: usize,
    pattern: Option<&str>,
    extract_name: F,
) -> (u64, Vec<T>)
where
    F: Fn(&T) -> &str,
{
    let total = items.len();
    let offset = usize::try_from(cursor).unwrap_or(total);
    if offset >= total {
        return (0, Vec::new());
    }
    let end = offset.saturating_add(count).min(total);
    let next_cursor = if end >= total { 0 } else { u64::try_from(end).unwrap_or(0) };
    let batch = items.into_iter().skip(offset).take(end - offset);
    // `*` matches everything — skip the WildMatch compile + per-item check.
    let filtered: Vec<T> = match pattern {
        Some(p) if p != "*" => {
            let wm = wildmatch::WildMatch::new(p);
            batch.filter(|x| wm.matches(extract_name(x))).collect()
        }
        _ => batch.collect(),
    };
    (next_cursor, filtered)
}

/// Collect zero-based positions of `element` inside `list` for `LPOS`.
///
/// `rank` selects the k-th occurrence in the search direction (positive =
/// head→tail, negative = tail→head). `wanted` is the maximum number of
/// positions to return — the handler maps `LPOS`'s `COUNT 0` to `usize::MAX`
/// ("all matches") and `COUNT` absent to `1` ("single match"). `limit` is
/// `MAXLEN`; `0` means unlimited.
pub fn scan_list_positions(list: &[Vec<u8>], element: &[u8], rank: i64, wanted: usize, limit: usize) -> Vec<i64> {
    if rank == 0 || list.is_empty() || wanted == 0 {
        return Vec::new();
    }
    let forward = rank > 0;
    let skip = usize::try_from(rank.unsigned_abs()).unwrap_or(usize::MAX).saturating_sub(1);
    let cap = if limit == 0 { usize::MAX } else { limit };

    let mut hits: Vec<i64> = Vec::new();
    let mut seen = 0_usize;
    let indices: Box<dyn Iterator<Item = usize>> =
        if forward { Box::new(0..list.len()) } else { Box::new((0..list.len()).rev()) };

    for (compared, idx) in indices.enumerate() {
        if compared >= cap {
            break;
        }
        if list[idx].as_slice() == element {
            if seen < skip {
                seen += 1;
                continue;
            }
            hits.push(i64::try_from(idx).unwrap_or(i64::MAX));
            if hits.len() >= wanted {
                break;
            }
        }
    }
    hits
}

/// Mutate the bit at `offset` to `value` in `data`, zero-padding the buffer
/// if `offset` lands past the current end. Returns the previous bit value.
pub fn apply_setbit(data: &mut Vec<u8>, offset: u64, value: bool) -> u8 {
    let byte_idx = usize::try_from(offset / 8).unwrap_or(usize::MAX);
    if byte_idx >= data.len() {
        data.resize(byte_idx + 1, 0);
    }
    let bit_in_byte = 7 - (offset % 8) as u8;
    let mask: u8 = 1 << bit_in_byte;
    let prev = (data[byte_idx] & mask) >> bit_in_byte;
    if value {
        data[byte_idx] |= mask;
    } else {
        data[byte_idx] &= !mask;
    }
    prev
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
}

#[async_trait]
impl KVBackend for S3Storage {
    async fn load(&self, redis_key: &str) -> Result<Option<(StoredValue, String)>, RustyAntError> {
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
                    let _ = self.delete(redis_key, DeleteCondition::Any).await;
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

    async fn save(&self, redis_key: &str, entry: &StoredValue, cond: WriteCondition) -> Result<(), RustyAntError> {
        let body = serde_json::to_vec(entry)?;
        let mut req = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(self.key(redis_key))
            .body(ByteStream::from(body))
            .content_type("application/json");
        match cond {
            WriteCondition::Any => {}
            WriteCondition::CreateOnly => req = req.if_none_match("*"),
            WriteCondition::IfMatch(etag) => req = req.if_match(etag),
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

    async fn delete(&self, redis_key: &str, cond: DeleteCondition) -> Result<(), RustyAntError> {
        let mut req = self.client.delete_object().bucket(&self.bucket).key(self.key(redis_key));
        if let DeleteCondition::IfMatch(etag) = cond {
            req = req.if_match(etag);
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

    async fn list_page(&self, cursor: Option<String>, max_keys: usize) -> Result<ListPage, RustyAntError> {
        let mut req = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(&self.prefix)
            .max_keys(i32::try_from(max_keys).unwrap_or(i32::MAX));
        if let Some(c) = cursor {
            req = req.continuation_token(c);
        }
        let resp = req.send().await.map_err(|e| RustyAntError::S3(e.to_string()))?;
        let keys: Vec<String> = resp
            .contents()
            .iter()
            .filter_map(|o| o.key())
            .filter_map(|k| k.strip_prefix(self.prefix.as_str()).map(str::to_string))
            .collect();
        let next_cursor = resp.next_continuation_token().map(String::from);
        Ok(ListPage { keys, next_cursor })
    }

    /// Override the default `flush_all` with S3's batch-delete — up to 1000
    /// keys per `DeleteObjects` call, matching `ListObjectsV2`'s page size.
    async fn flush_all(&self) -> Result<(), RustyAntError> {
        let mut cursor: Option<String> = None;
        loop {
            let mut req = self.client.list_objects_v2().bucket(&self.bucket).prefix(&self.prefix);
            if let Some(c) = &cursor {
                req = req.continuation_token(c);
            }
            let resp = req.send().await.map_err(|e| RustyAntError::S3(e.to_string()))?;
            let ids: Vec<aws_sdk_s3::types::ObjectIdentifier> = resp
                .contents()
                .iter()
                .filter_map(|o| o.key())
                .map(|k| {
                    aws_sdk_s3::types::ObjectIdentifier::builder()
                        .key(k)
                        .build()
                        .map_err(|e| RustyAntError::S3(format!("object id: {e}")))
                })
                .collect::<Result<_, _>>()?;
            if !ids.is_empty() {
                let delete = aws_sdk_s3::types::Delete::builder()
                    .set_objects(Some(ids))
                    .quiet(true)
                    .build()
                    .map_err(|e| RustyAntError::S3(format!("delete payload: {e}")))?;
                self.client
                    .delete_objects()
                    .bucket(&self.bucket)
                    .delete(delete)
                    .send()
                    .await
                    .map_err(|e| RustyAntError::S3(e.to_string()))?;
            }
            match resp.next_continuation_token() {
                Some(next) => cursor = Some(next.to_string()),
                None => break,
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// KVStorage<K> — one shared [`Storage`] impl that turns a [`KVBackend`] into
// a full Redis-shaped surface. Every command's logic lives here exactly once
// and works against any backend that can answer the four [`KVBackend`]
// primitives.
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct KVStorage<K: KVBackend> {
    backend: K,
}

impl<K: KVBackend> KVStorage<K> {
    pub const fn new(backend: K) -> Self {
        Self { backend }
    }

    /// Load the entry and discard the version token — for read-only paths
    /// that don't need CAS.
    async fn load(&self, redis_key: &str) -> Result<Option<StoredValue>, RustyAntError> {
        Ok(self.backend.load(redis_key).await?.map(|(e, _)| e))
    }

    /// Read-modify-write helper: runs `modify` against the latest entry,
    /// writes the result back under version-based optimistic locking,
    /// retrying up to `MAX_CAS_RETRIES` times on contention. Backend-agnostic
    /// — S3 resolves the version as `ETag`, `DynamoDB` will resolve it as a
    /// version attribute; the loop itself is identical.
    async fn cas<F, R>(&self, redis_key: &str, mut modify: F) -> Result<R, RustyAntError>
    where
        F: FnMut(Option<&StoredValue>) -> Result<CasAction<R>, RustyAntError>,
    {
        for attempt in 0..MAX_CAS_RETRIES {
            cas_backoff(attempt).await;
            let loaded = self.backend.load(redis_key).await?;
            let (existing, etag) = match &loaded {
                Some((e, t)) => (Some(e), Some(t.clone())),
                None => (None, None),
            };
            match modify(existing)? {
                CasAction::NoOp(r) => return Ok(r),
                CasAction::Delete(r) => match etag {
                    Some(e) => match self.backend.delete(redis_key, DeleteCondition::IfMatch(e)).await {
                        Ok(()) => return Ok(r),
                        Err(RustyAntError::Contention) => (),
                        Err(err) => return Err(err),
                    },
                    None => return Ok(r),
                },
                CasAction::Write(new_entry, r) => {
                    let cond = etag.map_or(WriteCondition::CreateOnly, WriteCondition::IfMatch);
                    match self.backend.save(redis_key, &new_entry, cond).await {
                        Ok(()) => return Ok(r),
                        Err(RustyAntError::Contention) => {}
                        Err(e) => return Err(e),
                    }
                }
            }
        }
        Err(RustyAntError::Contention)
    }

    /// Shared `ZPOPMIN` / `ZPOPMAX` implementation. `from_max=true` pops the
    /// highest-ranked members; otherwise pops from the bottom.
    async fn zpop_impl(&self, key: &str, count: usize, from_max: bool) -> Result<Vec<(String, f64)>, RustyAntError> {
        if count == 0 {
            // Type-check even for a no-op count so a string key still errors.
            match self.load(key).await? {
                Some(StoredValue { value: Value::ZSet(_), .. }) | None => return Ok(Vec::new()),
                Some(_) => return Err(wrong_type(key)),
            }
        }
        self.cas(key, move |entry| {
            let (map, expires_at_ms) = match entry {
                Some(StoredValue { value: Value::ZSet(m), expires_at_ms }) => (m.clone(), *expires_at_ms),
                Some(_) => return Err(wrong_type(key)),
                None => return Ok(CasAction::NoOp(Vec::new())),
            };
            let (kept, popped) = pop_zset_edge(map, count, from_max);
            if kept.is_empty() {
                Ok(CasAction::Delete(popped))
            } else {
                let new_entry = StoredValue { expires_at_ms, value: Value::ZSet(kept) };
                Ok(CasAction::Write(new_entry, popped))
            }
        })
        .await
    }
}

#[async_trait]
impl<K: KVBackend> Storage for KVStorage<K> {
    async fn delete(&self, redis_key: &str) -> Result<bool, RustyAntError> {
        let existed = self.load(redis_key).await?.is_some();
        if existed {
            self.backend.delete(redis_key, DeleteCondition::Any).await?;
        }
        Ok(existed)
    }

    async fn exists(&self, key: &str) -> Result<bool, RustyAntError> {
        Ok(self.load(key).await?.is_some())
    }

    async fn kind(&self, key: &str) -> Result<Option<&'static str>, RustyAntError> {
        Ok(self.load(key).await?.map(|v| value_kind(&v.value)))
    }

    async fn mem_usage(&self, key: &str) -> Result<Option<i64>, RustyAntError> {
        let Some(entry) = self.load(key).await? else { return Ok(None) };
        // Serialize the value to JSON — the same shape written to S3 — and
        // report its byte length. Not a perfect analog of Redis's
        // `MEMORY USAGE` (which tallies in-process allocations) but an
        // honest "bytes this key occupies on the backend".
        let serialized = serde_json::to_vec(&entry.value)?;
        Ok(Some(i64::try_from(serialized.len()).unwrap_or(i64::MAX)))
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

    async fn expire_time_ms(&self, key: &str) -> Result<TtlResult, RustyAntError> {
        let Some(v) = self.load(key).await? else {
            return Ok(TtlResult::NoKey);
        };
        Ok(v.expires_at_ms.map_or(TtlResult::NoExpire, TtlResult::Ms))
    }

    async fn get_string(&self, key: &str) -> Result<Option<Bytes>, RustyAntError> {
        match self.load(key).await? {
            Some(StoredValue { value: Value::String(data), .. }) => Ok(Some(Bytes::from(data))),
            Some(_) => Err(wrong_type(key)),
            None => Ok(None),
        }
    }

    async fn get_string_with_ttl(&self, key: &str, op: GetExOp) -> Result<Option<Bytes>, RustyAntError> {
        if matches!(op, GetExOp::Leave) {
            return self.get_string(key).await;
        }
        self.cas(key, move |entry| {
            let (data, _old_expires_at) = match entry {
                Some(StoredValue { value: Value::String(d), expires_at_ms }) => (d.clone(), *expires_at_ms),
                Some(_) => return Err(wrong_type(key)),
                None => return Ok(CasAction::NoOp(None)),
            };
            let new_expires_at = match op {
                GetExOp::Leave => unreachable!("leave handled above"),
                GetExOp::SetExpireAtMs(t) => Some(t),
                GetExOp::Persist => None,
            };
            let new_entry = StoredValue { expires_at_ms: new_expires_at, value: Value::String(data.clone()) };
            Ok(CasAction::Write(new_entry, Some(Bytes::from(data))))
        })
        .await
    }

    async fn set_string(&self, key: &str, value: Bytes, expires_at_ms: Option<i64>) -> Result<(), RustyAntError> {
        self.backend
            .save(key, &StoredValue { expires_at_ms, value: Value::String(value.to_vec()) }, WriteCondition::Any)
            .await
    }

    async fn set_string_nx(&self, key: &str, value: Bytes, expires_at_ms: Option<i64>) -> Result<bool, RustyAntError> {
        // Surface any expired entry so `If-None-Match: *` doesn't reject
        // a legitimate create because the zombie object hasn't been swept yet.
        let _ = self.backend.load(key).await?;
        let entry = StoredValue { expires_at_ms, value: Value::String(value.to_vec()) };
        match self.backend.save(key, &entry, WriteCondition::CreateOnly).await {
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

    async fn hsetnx(&self, key: &str, field: &str, value: Bytes) -> Result<bool, RustyAntError> {
        let field = field.to_string();
        self.cas(key, move |entry| {
            let (mut map, expires_at_ms) = match entry {
                Some(StoredValue { value: Value::Hash(m), expires_at_ms }) => (m.clone(), *expires_at_ms),
                Some(_) => return Err(wrong_type(key)),
                None => (BTreeMap::new(), None),
            };
            if map.contains_key(&field) {
                return Ok(CasAction::NoOp(false));
            }
            map.insert(field.clone(), value.to_vec());
            let new_entry = StoredValue { expires_at_ms, value: Value::Hash(map) };
            Ok(CasAction::Write(new_entry, true))
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

    async fn hscan(
        &self,
        key: &str,
        cursor: u64,
        pattern: Option<&str>,
        count: usize,
    ) -> Result<(u64, Vec<(String, Bytes)>), RustyAntError> {
        match self.load(key).await? {
            Some(StoredValue { value: Value::Hash(m), .. }) => {
                let items: Vec<(String, Bytes)> = m.into_iter().map(|(k, v)| (k, Bytes::from(v))).collect();
                Ok(apply_collection_scan(items, cursor, count, pattern, |(f, _)| f.as_str()))
            }
            Some(_) => Err(wrong_type(key)),
            None => Ok((0, Vec::new())),
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

    async fn zadd_ext(&self, key: &str, pairs: Vec<(f64, String)>, flags: ZAddFlags) -> Result<i64, RustyAntError> {
        self.cas(key, move |entry| {
            let (mut map, expires_at_ms) = match entry {
                Some(StoredValue { value: Value::ZSet(m), expires_at_ms }) => (m.clone(), *expires_at_ms),
                Some(_) => return Err(wrong_type(key)),
                None => (BTreeMap::new(), None),
            };
            let mut added: i64 = 0;
            let mut updated: i64 = 0;
            for (score, member) in &pairs {
                if let Some(prev) = map.get(member).copied() {
                    if flags.nx {
                        continue;
                    }
                    if flags.gt && *score <= prev {
                        continue;
                    }
                    if flags.lt && *score >= prev {
                        continue;
                    }
                    // Redis counts a score-equality no-op as unchanged even
                    // under CH, so compare bit-for-bit before bumping.
                    if prev.to_bits() != score.to_bits() {
                        map.insert(member.clone(), *score);
                        updated += 1;
                    }
                } else {
                    if flags.xx {
                        continue;
                    }
                    // GT / LT don't block fresh inserts — there's no previous
                    // score to compare against, so Redis adds unconditionally.
                    map.insert(member.clone(), *score);
                    added += 1;
                }
            }
            let count = if flags.ch { added + updated } else { added };
            // Nothing touched the map — avoid gratuitously writing or, worse,
            // materializing an empty ZSet for a never-existed key under XX.
            if added == 0 && updated == 0 {
                return Ok(CasAction::NoOp(count));
            }
            let new_entry = StoredValue { expires_at_ms, value: Value::ZSet(map) };
            Ok(CasAction::Write(new_entry, count))
        })
        .await
    }

    async fn zadd_ext_incr(
        &self,
        key: &str,
        delta: f64,
        member: &str,
        flags: ZAddFlags,
    ) -> Result<Option<f64>, RustyAntError> {
        let member_owned = member.to_string();
        self.cas(key, move |entry| {
            let (mut map, expires_at_ms) = match entry {
                Some(StoredValue { value: Value::ZSet(m), expires_at_ms }) => (m.clone(), *expires_at_ms),
                Some(_) => return Err(wrong_type(key)),
                None => (BTreeMap::new(), None),
            };
            let new_score = if let Some(prev) = map.get(&member_owned).copied() {
                if flags.nx {
                    return Ok(CasAction::NoOp(None));
                }
                let candidate = prev + delta;
                if candidate.is_nan() {
                    return Err(RustyAntError::Parse("resulting score is not a number (NaN)".into()));
                }
                if flags.gt && candidate <= prev {
                    return Ok(CasAction::NoOp(None));
                }
                if flags.lt && candidate >= prev {
                    return Ok(CasAction::NoOp(None));
                }
                map.insert(member_owned.clone(), candidate);
                candidate
            } else {
                if flags.xx {
                    return Ok(CasAction::NoOp(None));
                }
                // Fresh insert; GT / LT don't block it (no prior score).
                map.insert(member_owned.clone(), delta);
                delta
            };
            let new_entry = StoredValue { expires_at_ms, value: Value::ZSet(map) };
            Ok(CasAction::Write(new_entry, Some(new_score)))
        })
        .await
    }

    async fn zitems(&self, key: &str) -> Result<Vec<(String, f64)>, RustyAntError> {
        match self.load(key).await? {
            Some(StoredValue { value: Value::ZSet(m), .. }) => Ok(m.into_iter().collect()),
            Some(_) => Err(wrong_type(key)),
            None => Ok(Vec::new()),
        }
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

    async fn hstrlen(&self, key: &str, field: &str) -> Result<i64, RustyAntError> {
        match self.load(key).await? {
            Some(StoredValue { value: Value::Hash(m), .. }) => {
                Ok(m.get(field).map_or(0, |v| i64::try_from(v.len()).unwrap_or(i64::MAX)))
            }
            Some(_) => Err(wrong_type(key)),
            None => Ok(0),
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

    async fn sscan(
        &self,
        key: &str,
        cursor: u64,
        pattern: Option<&str>,
        count: usize,
    ) -> Result<(u64, Vec<String>), RustyAntError> {
        match self.load(key).await? {
            Some(StoredValue { value: Value::Set(s), .. }) => {
                let items: Vec<String> = s.into_iter().collect();
                Ok(apply_collection_scan(items, cursor, count, pattern, String::as_str))
            }
            Some(_) => Err(wrong_type(key)),
            None => Ok((0, Vec::new())),
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

    async fn zrank(&self, key: &str, member: &str) -> Result<Option<i64>, RustyAntError> {
        match self.load(key).await? {
            Some(StoredValue { value: Value::ZSet(m), .. }) => Ok(asc_rank_of(&m, member)),
            Some(_) => Err(wrong_type(key)),
            None => Ok(None),
        }
    }

    async fn zrevrank(&self, key: &str, member: &str) -> Result<Option<i64>, RustyAntError> {
        match self.load(key).await? {
            Some(StoredValue { value: Value::ZSet(m), .. }) => {
                let len = i64::try_from(m.len()).unwrap_or(i64::MAX);
                Ok(asc_rank_of(&m, member).map(|r| len - 1 - r))
            }
            Some(_) => Err(wrong_type(key)),
            None => Ok(None),
        }
    }

    async fn zcount(&self, key: &str, min: ScoreBound, max: ScoreBound) -> Result<i64, RustyAntError> {
        match self.load(key).await? {
            Some(StoredValue { value: Value::ZSet(m), .. }) => Ok(count_zset_by_score(&m, min, max)),
            Some(_) => Err(wrong_type(key)),
            None => Ok(0),
        }
    }

    async fn zmscore(&self, key: &str, members: &[String]) -> Result<Vec<Option<f64>>, RustyAntError> {
        match self.load(key).await? {
            Some(StoredValue { value: Value::ZSet(m), .. }) => {
                Ok(members.iter().map(|mem| m.get(mem).copied()).collect())
            }
            Some(_) => Err(wrong_type(key)),
            None => Ok(members.iter().map(|_| None).collect()),
        }
    }

    async fn zrandmember(
        &self,
        key: &str,
        count: i64,
        allow_duplicates: bool,
    ) -> Result<Vec<(String, f64)>, RustyAntError> {
        match self.load(key).await? {
            Some(StoredValue { value: Value::ZSet(m), .. }) => Ok(pick_random_from_zset(&m, count, allow_duplicates)),
            Some(_) => Err(wrong_type(key)),
            None => Ok(Vec::new()),
        }
    }

    async fn zscan(
        &self,
        key: &str,
        cursor: u64,
        pattern: Option<&str>,
        count: usize,
    ) -> Result<(u64, Vec<(String, f64)>), RustyAntError> {
        match self.load(key).await? {
            Some(StoredValue { value: Value::ZSet(m), .. }) => {
                let items: Vec<(String, f64)> = m.into_iter().collect();
                Ok(apply_collection_scan(items, cursor, count, pattern, |(mem, _)| mem.as_str()))
            }
            Some(_) => Err(wrong_type(key)),
            None => Ok((0, Vec::new())),
        }
    }

    // ---- Streams -------------------------------------------------------

    async fn xadd(
        &self,
        key: &str,
        id_spec: AddIdSpec,
        fields: Vec<(String, Vec<u8>)>,
        nomkstream: bool,
        trim: Option<(TrimBound, Option<usize>)>,
    ) -> Result<Option<StreamId>, RustyAntError> {
        let now = u64::try_from(now_ms().max(0)).unwrap_or(0);
        self.cas(key, move |entry| {
            let (mut stream, expires_at_ms) = match entry {
                Some(StoredValue { value: Value::Stream(s), expires_at_ms }) => (s.clone(), *expires_at_ms),
                Some(_) => return Err(wrong_type(key)),
                None => {
                    if nomkstream {
                        return Ok(CasAction::NoOp(None));
                    }
                    (StreamValue::default(), None)
                }
            };
            let id = resolve_add_id(id_spec, stream.last_generated_id, now)?;
            stream.entries.push(StreamEntry { id, fields: fields.clone() });
            stream.last_generated_id = id;
            stream.entries_added = stream.entries_added.saturating_add(1);
            if let Some((bound, limit)) = trim {
                trim_in_place(&mut stream, bound, limit);
            }
            let new_entry = StoredValue { expires_at_ms, value: Value::Stream(stream) };
            Ok(CasAction::Write(new_entry, Some(id)))
        })
        .await
    }

    async fn xlen(&self, key: &str) -> Result<i64, RustyAntError> {
        match self.load(key).await? {
            Some(StoredValue { value: Value::Stream(s), .. }) => Ok(i64::try_from(s.entries.len()).unwrap_or(i64::MAX)),
            Some(_) => Err(wrong_type(key)),
            None => Ok(0),
        }
    }

    async fn xrange(
        &self,
        key: &str,
        start: RangeId,
        end: RangeId,
        count: Option<usize>,
        reverse: bool,
    ) -> Result<Vec<StreamEntry>, RustyAntError> {
        let stream = match self.load(key).await? {
            Some(StoredValue { value: Value::Stream(s), .. }) => s,
            Some(_) => return Err(wrong_type(key)),
            None => return Ok(Vec::new()),
        };
        let mut filtered: Vec<StreamEntry> =
            stream.entries.into_iter().filter(|e| start.ge_min(e.id) && end.le_max(e.id)).collect();
        if reverse {
            filtered.reverse();
        }
        if let Some(cap) = count {
            filtered.truncate(cap);
        }
        Ok(filtered)
    }

    async fn xdel(&self, key: &str, ids: &[StreamId]) -> Result<i64, RustyAntError> {
        let ids = ids.to_vec();
        self.cas(key, move |entry| {
            let (mut stream, expires_at_ms) = match entry {
                Some(StoredValue { value: Value::Stream(s), expires_at_ms }) => (s.clone(), *expires_at_ms),
                Some(_) => return Err(wrong_type(key)),
                None => return Ok(CasAction::NoOp(0)),
            };
            let before = stream.entries.len();
            stream.entries.retain(|e| {
                if ids.contains(&e.id) {
                    if e.id > stream.max_deleted_entry_id {
                        stream.max_deleted_entry_id = e.id;
                    }
                    false
                } else {
                    true
                }
            });
            let removed = i64::try_from(before - stream.entries.len()).unwrap_or(i64::MAX);
            if removed == 0 {
                return Ok(CasAction::NoOp(0));
            }
            let new_entry = StoredValue { expires_at_ms, value: Value::Stream(stream) };
            Ok(CasAction::Write(new_entry, removed))
        })
        .await
    }

    async fn xtrim(&self, key: &str, bound: TrimBound, limit: Option<usize>) -> Result<i64, RustyAntError> {
        self.cas(key, move |entry| {
            let (mut stream, expires_at_ms) = match entry {
                Some(StoredValue { value: Value::Stream(s), expires_at_ms }) => (s.clone(), *expires_at_ms),
                Some(_) => return Err(wrong_type(key)),
                None => return Ok(CasAction::NoOp(0)),
            };
            let removed = trim_in_place(&mut stream, bound, limit);
            if removed == 0 {
                return Ok(CasAction::NoOp(0));
            }
            let new_entry = StoredValue { expires_at_ms, value: Value::Stream(stream) };
            Ok(CasAction::Write(new_entry, i64::try_from(removed).unwrap_or(i64::MAX)))
        })
        .await
    }

    async fn xread(
        &self,
        keys: &[String],
        after_ids: &[StreamId],
        count: Option<usize>,
    ) -> Result<Vec<(String, Vec<StreamEntry>)>, RustyAntError> {
        let mut out: Vec<(String, Vec<StreamEntry>)> = Vec::new();
        for (key, after) in keys.iter().zip(after_ids.iter()) {
            let entries = match self.load(key).await? {
                Some(StoredValue { value: Value::Stream(s), .. }) => {
                    let mut matched: Vec<StreamEntry> = s.entries.into_iter().filter(|e| e.id > *after).collect();
                    if let Some(cap) = count {
                        matched.truncate(cap);
                    }
                    matched
                }
                Some(_) => return Err(wrong_type(key)),
                None => Vec::new(),
            };
            if !entries.is_empty() {
                out.push((key.clone(), entries));
            }
        }
        Ok(out)
    }

    async fn xinfo_stream(&self, key: &str) -> Result<Option<StreamValue>, RustyAntError> {
        match self.load(key).await? {
            Some(StoredValue { value: Value::Stream(s), .. }) => Ok(Some(s)),
            Some(_) => Err(wrong_type(key)),
            None => Ok(None),
        }
    }

    async fn xgroup(&self, key: &str, op: XGroupOp) -> Result<i64, RustyAntError> {
        self.cas(key, move |entry| {
            let (mut stream, expires_at_ms, existed) = match entry {
                Some(StoredValue { value: Value::Stream(s), expires_at_ms }) => (s.clone(), *expires_at_ms, true),
                Some(_) => return Err(wrong_type(key)),
                None => (StreamValue::default(), None, false),
            };
            let result = apply_xgroup_op(&mut stream, &op, existed)?;
            // CREATE / SETID / DESTROY / etc. all imply a write. CREATE
            // without MKSTREAM on a missing key is the one path that
            // bails out early via the helper.
            let new_entry = StoredValue { expires_at_ms, value: Value::Stream(stream) };
            Ok(CasAction::Write(new_entry, result))
        })
        .await
    }

    async fn xreadgroup(
        &self,
        group: &str,
        consumer: &str,
        keys: &[String],
        ids: &[XReadGroupId],
        count: Option<usize>,
        noack: bool,
        now_ms_override: i64,
    ) -> Result<Vec<(String, Vec<StreamEntry>)>, RustyAntError> {
        let mut out: Vec<(String, Vec<StreamEntry>)> = Vec::new();
        for (key, id_spec) in keys.iter().zip(ids.iter()) {
            let group = group.to_string();
            let consumer = consumer.to_string();
            let id_spec = *id_spec;
            let entries: Vec<StreamEntry> = self
                .cas(key, move |entry| {
                    let (mut stream, expires_at_ms) = match entry {
                        Some(StoredValue { value: Value::Stream(s), expires_at_ms }) => (s.clone(), *expires_at_ms),
                        Some(_) => return Err(wrong_type_error()),
                        None => return Err(no_group_error(&group)),
                    };
                    let g = stream.groups.get_mut(&group).ok_or_else(|| no_group_error(&group))?;
                    let mut delivered: Vec<StreamEntry> = match id_spec {
                        XReadGroupId::NewEntries => {
                            let last = g.last_delivered_id;
                            let mut newish: Vec<StreamEntry> =
                                stream.entries.iter().filter(|e| e.id > last).cloned().collect();
                            if let Some(cap) = count {
                                newish.truncate(cap);
                            }
                            // Replay through the PEL bookkeeping.
                            for entry in &newish {
                                if !noack {
                                    g.pel.insert(
                                        entry.id,
                                        PendingEntry {
                                            consumer: consumer.clone(),
                                            delivery_time_ms: now_ms_override,
                                            delivery_count: 1,
                                        },
                                    );
                                }
                                if entry.id > g.last_delivered_id {
                                    g.last_delivered_id = entry.id;
                                }
                            }
                            g.consumers.entry(consumer.clone()).or_default().seen_ms = now_ms_override;
                            newish
                        }
                        XReadGroupId::Pending(after) => {
                            // Re-read PEL rows owned by this consumer that are > after.
                            let mut rows: Vec<StreamId> = g
                                .pel
                                .iter()
                                .filter(|(id, p)| **id > after && p.consumer == consumer)
                                .map(|(id, _)| *id)
                                .collect();
                            if let Some(cap) = count {
                                rows.truncate(cap);
                            }
                            // Bump delivery_count for each re-delivery.
                            for id in &rows {
                                if let Some(p) = g.pel.get_mut(id) {
                                    p.delivery_count = p.delivery_count.saturating_add(1);
                                    p.delivery_time_ms = now_ms_override;
                                }
                            }
                            // Materialize the entries; drop ones that have
                            // since been XDELed from the stream.
                            rows.iter()
                                .filter_map(|id| stream.entries.iter().find(|e| e.id == *id).cloned())
                                .collect::<Vec<_>>()
                        }
                    };
                    delivered.sort_by_key(|e| e.id);
                    let new_entry = StoredValue { expires_at_ms, value: Value::Stream(stream) };
                    Ok(CasAction::Write(new_entry, delivered))
                })
                .await?;
            if !entries.is_empty() {
                out.push((key.clone(), entries));
            }
        }
        Ok(out)
    }

    async fn xack(&self, key: &str, group: &str, ids: &[StreamId]) -> Result<i64, RustyAntError> {
        let group = group.to_string();
        let ids = ids.to_vec();
        self.cas(key, move |entry| {
            let (mut stream, expires_at_ms) = match entry {
                Some(StoredValue { value: Value::Stream(s), expires_at_ms }) => (s.clone(), *expires_at_ms),
                Some(_) => return Err(wrong_type(key)),
                None => return Ok(CasAction::NoOp(0)),
            };
            let Some(g) = stream.groups.get_mut(&group) else {
                return Ok(CasAction::NoOp(0));
            };
            let mut acked: i64 = 0;
            for id in &ids {
                if g.pel.remove(id).is_some() {
                    acked += 1;
                }
            }
            if acked == 0 {
                return Ok(CasAction::NoOp(0));
            }
            let new_entry = StoredValue { expires_at_ms, value: Value::Stream(stream) };
            Ok(CasAction::Write(new_entry, acked))
        })
        .await
    }

    async fn xclaim(
        &self,
        key: &str,
        group: &str,
        consumer: &str,
        min_idle_ms: i64,
        ids: &[StreamId],
        opts: XClaimOpts,
    ) -> Result<Vec<XClaimResult>, RustyAntError> {
        let group = group.to_string();
        let consumer = consumer.to_string();
        let ids = ids.to_vec();
        self.cas(key, move |entry| {
            let (mut stream, expires_at_ms) = match entry {
                Some(StoredValue { value: Value::Stream(s), expires_at_ms }) => (s.clone(), *expires_at_ms),
                Some(_) => return Err(wrong_type(key)),
                None => return Err(no_group_error(&group)),
            };
            let g = stream.groups.get_mut(&group).ok_or_else(|| no_group_error(&group))?;
            let new_delivery_time = opts.time_ms.unwrap_or_else(|| opts.now_ms - opts.idle_ms.unwrap_or(0));
            let mut claimed: Vec<XClaimResult> = Vec::with_capacity(ids.len());
            for id in &ids {
                let existed_in_pel = g.pel.contains_key(id);
                if !existed_in_pel && !opts.force {
                    continue;
                }
                let entry_idle_ok =
                    g.pel.get(id).is_none_or(|p| (opts.now_ms - p.delivery_time_ms).max(0) >= min_idle_ms);
                if !entry_idle_ok {
                    continue;
                }
                let new_count = match (g.pel.get(id), opts.retry_count) {
                    (_, Some(rc)) => rc,
                    (Some(p), None) => p.delivery_count.saturating_add(1),
                    (None, None) => 1,
                };
                g.pel.insert(
                    *id,
                    PendingEntry {
                        consumer: consumer.clone(),
                        delivery_time_ms: new_delivery_time,
                        delivery_count: new_count,
                    },
                );
                g.consumers.entry(consumer.clone()).or_default().seen_ms = opts.now_ms;
                let body = if opts.just_id {
                    None
                } else {
                    stream.entries.iter().find(|e| e.id == *id).map(|e| e.fields.clone())
                };
                claimed.push(XClaimResult { id: *id, fields: body });
            }
            let new_entry = StoredValue { expires_at_ms, value: Value::Stream(stream) };
            Ok(CasAction::Write(new_entry, claimed))
        })
        .await
    }

    async fn xpending_summary(&self, key: &str, group: &str) -> Result<Option<XPendingSummary>, RustyAntError> {
        let stream = match self.load(key).await? {
            Some(StoredValue { value: Value::Stream(s), .. }) => s,
            Some(_) => return Err(wrong_type(key)),
            None => return Err(no_group_error(group)),
        };
        let g = stream.groups.get(group).ok_or_else(|| no_group_error(group))?;
        if g.pel.is_empty() {
            return Ok(None);
        }
        let mut per_consumer: BTreeMap<String, u64> = BTreeMap::new();
        for p in g.pel.values() {
            *per_consumer.entry(p.consumer.clone()).or_insert(0) += 1;
        }
        let min = *g.pel.keys().next().expect("non-empty");
        let max = *g.pel.keys().next_back().expect("non-empty");
        Ok(Some(XPendingSummary {
            count: g.pel.len() as u64,
            min,
            max,
            per_consumer: per_consumer.into_iter().collect(),
        }))
    }

    async fn xpending_detail(
        &self,
        key: &str,
        group: &str,
        start: RangeId,
        end: RangeId,
        count: usize,
        consumer: Option<&str>,
        idle_ms: Option<i64>,
        now_ms_override: i64,
    ) -> Result<Vec<XPendingDetailRow>, RustyAntError> {
        let stream = match self.load(key).await? {
            Some(StoredValue { value: Value::Stream(s), .. }) => s,
            Some(_) => return Err(wrong_type(key)),
            None => return Err(no_group_error(group)),
        };
        let g = stream.groups.get(group).ok_or_else(|| no_group_error(group))?;
        let mut rows: Vec<XPendingDetailRow> = Vec::new();
        for (id, p) in &g.pel {
            if !start.ge_min(*id) || !end.le_max(*id) {
                continue;
            }
            if let Some(c) = consumer {
                if p.consumer != c {
                    continue;
                }
            }
            let idle = (now_ms_override - p.delivery_time_ms).max(0);
            if let Some(min_idle) = idle_ms {
                if idle < min_idle {
                    continue;
                }
            }
            rows.push(XPendingDetailRow {
                id: *id,
                consumer: p.consumer.clone(),
                idle_ms: idle,
                delivery_count: p.delivery_count,
            });
            if rows.len() >= count {
                break;
            }
        }
        Ok(rows)
    }

    async fn xautoclaim(
        &self,
        key: &str,
        group: &str,
        consumer: &str,
        min_idle_ms: i64,
        start: StreamId,
        count: usize,
        just_id: bool,
        now_ms_override: i64,
    ) -> Result<XAutoClaimResult, RustyAntError> {
        let group = group.to_string();
        let consumer = consumer.to_string();
        self.cas(key, move |entry| {
            let (mut stream, expires_at_ms) = match entry {
                Some(StoredValue { value: Value::Stream(s), expires_at_ms }) => (s.clone(), *expires_at_ms),
                Some(_) => return Err(wrong_type(key)),
                None => return Err(no_group_error(&group)),
            };
            let g = stream.groups.get_mut(&group).ok_or_else(|| no_group_error(&group))?;
            let mut claimed: Vec<XClaimResult> = Vec::new();
            let mut deleted: Vec<StreamId> = Vec::new();
            let mut next_cursor = StreamId::MIN;
            let candidates: Vec<StreamId> = g
                .pel
                .range(start..)
                .filter(|(_, p)| (now_ms_override - p.delivery_time_ms).max(0) >= min_idle_ms)
                .map(|(id, _)| *id)
                .collect();
            for id in candidates {
                if claimed.len() >= count {
                    next_cursor = id;
                    break;
                }
                if !stream.entries.iter().any(|e| e.id == id) {
                    g.pel.remove(&id);
                    deleted.push(id);
                    continue;
                }
                if let Some(p) = g.pel.get_mut(&id) {
                    p.consumer.clone_from(&consumer);
                    p.delivery_time_ms = now_ms_override;
                    p.delivery_count = p.delivery_count.saturating_add(1);
                }
                let body =
                    if just_id { None } else { stream.entries.iter().find(|e| e.id == id).map(|e| e.fields.clone()) };
                claimed.push(XClaimResult { id, fields: body });
            }
            g.consumers.entry(consumer.clone()).or_default().seen_ms = now_ms_override;
            let new_entry = StoredValue { expires_at_ms, value: Value::Stream(stream) };
            Ok(CasAction::Write(new_entry, XAutoClaimResult { next_cursor, claimed, deleted_ids: deleted }))
        })
        .await
    }

    async fn xinfo_groups(&self, key: &str) -> Result<Vec<XInfoGroup>, RustyAntError> {
        match self.load(key).await? {
            Some(StoredValue { value: Value::Stream(s), .. }) => Ok(s
                .groups
                .into_iter()
                .map(|(name, g)| XInfoGroup {
                    name,
                    consumers: g.consumers.len() as u64,
                    pending: g.pel.len() as u64,
                    last_delivered_id: g.last_delivered_id,
                })
                .collect()),
            Some(_) => Err(wrong_type(key)),
            None => Ok(Vec::new()),
        }
    }

    async fn xinfo_consumers(&self, key: &str, group: &str) -> Result<Vec<XInfoConsumer>, RustyAntError> {
        let stream = match self.load(key).await? {
            Some(StoredValue { value: Value::Stream(s), .. }) => s,
            Some(_) => return Err(wrong_type(key)),
            None => return Err(no_group_error(group)),
        };
        let g = stream.groups.get(group).ok_or_else(|| no_group_error(group))?;
        let now = now_ms();
        Ok(g.consumers
            .iter()
            .map(|(name, c)| {
                let pending = g.pel.values().filter(|p| p.consumer == *name).count() as u64;
                XInfoConsumer { name: name.clone(), pending, idle_ms: (now - c.seen_ms).max(0) }
            })
            .collect())
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

    async fn get_and_delete(&self, key: &str) -> Result<Option<Bytes>, RustyAntError> {
        self.cas(key, move |entry| match entry {
            None => Ok(CasAction::NoOp(None)),
            Some(StoredValue { value: Value::String(data), .. }) => {
                Ok(CasAction::Delete(Some(Bytes::from(data.clone()))))
            }
            Some(_) => Err(wrong_type(key)),
        })
        .await
    }

    async fn strlen(&self, key: &str) -> Result<i64, RustyAntError> {
        match self.load(key).await? {
            Some(StoredValue { value: Value::String(data), .. }) => Ok(i64::try_from(data.len()).unwrap_or(i64::MAX)),
            Some(_) => Err(wrong_type(key)),
            None => Ok(0),
        }
    }

    async fn append(&self, key: &str, value: Bytes) -> Result<i64, RustyAntError> {
        self.cas(key, move |entry| {
            let (mut data, expires_at_ms) = match entry {
                Some(StoredValue { value: Value::String(d), expires_at_ms }) => (d.clone(), *expires_at_ms),
                Some(_) => return Err(wrong_type(key)),
                None => (Vec::new(), None),
            };
            data.extend_from_slice(&value);
            let len = i64::try_from(data.len()).unwrap_or(i64::MAX);
            let new_entry = StoredValue { expires_at_ms, value: Value::String(data) };
            Ok(CasAction::Write(new_entry, len))
        })
        .await
    }

    async fn pfadd(&self, key: &str, elements: &[Bytes]) -> Result<bool, RustyAntError> {
        let elements = elements.to_vec();
        self.cas(key, move |entry| {
            let (existed, mut buf, expires_at_ms) = match entry {
                Some(StoredValue { value: Value::String(d), expires_at_ms }) => {
                    if !hll::is_hll(d) {
                        return Err(RustyAntError::Parse(
                            "WRONGTYPE Key is not a valid HyperLogLog string value.".into(),
                        ));
                    }
                    (true, d.clone(), *expires_at_ms)
                }
                Some(_) => return Err(wrong_type(key)),
                None => (false, hll::empty_dense(), None),
            };
            let mut changed = false;
            for e in &elements {
                if hll::add(&mut buf, e)? {
                    changed = true;
                }
            }
            // Creating a new HLL counts as a change (matches Redis's
            // `PFADD newkey` with no elements → 1).
            if !existed {
                let new_entry = StoredValue { expires_at_ms, value: Value::String(buf) };
                return Ok(CasAction::Write(new_entry, true));
            }
            if !changed {
                return Ok(CasAction::NoOp(false));
            }
            let new_entry = StoredValue { expires_at_ms, value: Value::String(buf) };
            Ok(CasAction::Write(new_entry, true))
        })
        .await
    }

    async fn pfcount(&self, keys: &[String]) -> Result<u64, RustyAntError> {
        if keys.len() == 1 {
            return match self.load(&keys[0]).await? {
                Some(StoredValue { value: Value::String(d), .. }) => {
                    if !hll::is_hll(&d) {
                        return Err(RustyAntError::Parse(
                            "WRONGTYPE Key is not a valid HyperLogLog string value.".into(),
                        ));
                    }
                    hll::count(&d)
                }
                Some(_) => Err(wrong_type(&keys[0])),
                None => Ok(0),
            };
        }
        // Multi-key: build an in-memory union and count from that.
        let mut merged = hll::empty_dense();
        for k in keys {
            match self.load(k).await? {
                Some(StoredValue { value: Value::String(d), .. }) => {
                    if !hll::is_hll(&d) {
                        return Err(RustyAntError::Parse(
                            "WRONGTYPE Key is not a valid HyperLogLog string value.".into(),
                        ));
                    }
                    hll::merge_into(&mut merged, &d)?;
                }
                Some(_) => return Err(wrong_type(k)),
                None => {}
            }
        }
        hll::count(&merged)
    }

    async fn pfmerge(&self, dest: &str, sources: &[String]) -> Result<(), RustyAntError> {
        // Build the union in memory first — a multi-key PFCOUNT-style
        // traversal — then CAS it into `dest` (also including dest's
        // existing HLL if present).
        let mut merged = hll::empty_dense();
        for k in sources {
            match self.load(k).await? {
                Some(StoredValue { value: Value::String(d), .. }) => {
                    if !hll::is_hll(&d) {
                        return Err(RustyAntError::Parse(
                            "WRONGTYPE Key is not a valid HyperLogLog string value.".into(),
                        ));
                    }
                    hll::merge_into(&mut merged, &d)?;
                }
                Some(_) => return Err(wrong_type(k)),
                None => {}
            }
        }
        self.cas(dest, move |entry| {
            let (mut buf, expires_at_ms) = match entry {
                Some(StoredValue { value: Value::String(d), expires_at_ms }) => {
                    if !hll::is_hll(d) {
                        return Err(RustyAntError::Parse(
                            "WRONGTYPE Key is not a valid HyperLogLog string value.".into(),
                        ));
                    }
                    (d.clone(), *expires_at_ms)
                }
                Some(_) => return Err(wrong_type(dest)),
                None => (hll::empty_dense(), None),
            };
            hll::merge_into(&mut buf, &merged)?;
            let new_entry = StoredValue { expires_at_ms, value: Value::String(buf) };
            Ok(CasAction::Write(new_entry, ()))
        })
        .await
    }

    async fn incr_by_float(&self, key: &str, delta: f64) -> Result<f64, RustyAntError> {
        self.cas(key, move |entry| {
            let (current, expires_at_ms) = parse_string_as_f64(entry, key)?;
            let new_val = current + delta;
            if new_val.is_nan() || new_val.is_infinite() {
                return Err(RustyAntError::Parse("increment would produce NaN or infinity".into()));
            }
            let rendered = format_float(new_val).into_bytes();
            let new_entry = StoredValue { expires_at_ms, value: Value::String(rendered) };
            Ok(CasAction::Write(new_entry, new_val))
        })
        .await
    }

    async fn getrange(&self, key: &str, start: i64, end: i64) -> Result<Bytes, RustyAntError> {
        match self.load(key).await? {
            Some(StoredValue { value: Value::String(d), .. }) => Ok(Bytes::from(slice_string_range(&d, start, end))),
            Some(_) => Err(wrong_type(key)),
            None => Ok(Bytes::new()),
        }
    }

    async fn setrange(&self, key: &str, offset: usize, value: Bytes) -> Result<i64, RustyAntError> {
        self.cas(key, move |entry| {
            let (mut data, expires_at_ms) = match entry {
                Some(StoredValue { value: Value::String(d), expires_at_ms }) => (d.clone(), *expires_at_ms),
                Some(_) => return Err(wrong_type(key)),
                None => (Vec::new(), None),
            };
            // SETRANGE with empty value on missing key is a no-op — Redis
            // does not create the key in that corner case.
            if value.is_empty() && data.is_empty() {
                return Ok(CasAction::NoOp(0));
            }
            apply_setrange(&mut data, offset, &value);
            let len = i64::try_from(data.len()).unwrap_or(i64::MAX);
            let new_entry = StoredValue { expires_at_ms, value: Value::String(data) };
            Ok(CasAction::Write(new_entry, len))
        })
        .await
    }

    async fn msetnx(&self, pairs: Vec<(String, Bytes)>) -> Result<bool, RustyAntError> {
        // Best-effort atomicity — scan first, abort if any key exists, then
        // sequentially write. A concurrent setter landing between the scan
        // and the writes can leak past the NX guard; Redis's all-or-nothing
        // version is expensive to emulate over S3 and unused in practice.
        for (k, _) in &pairs {
            if self.load(k).await?.is_some() {
                return Ok(false);
            }
        }
        for (k, v) in pairs {
            self.set_string(&k, v, None).await?;
        }
        Ok(true)
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

    async fn linsert(&self, key: &str, before: bool, pivot: Bytes, value: Bytes) -> Result<i64, RustyAntError> {
        self.cas(key, move |entry| {
            let (mut list, expires_at_ms) = match entry {
                Some(StoredValue { value: Value::List(l), expires_at_ms }) => (l.clone(), *expires_at_ms),
                Some(_) => return Err(wrong_type(key)),
                None => return Ok(CasAction::NoOp(0)),
            };
            let Some(pos) = list.iter().position(|v| v.as_slice() == pivot.as_ref()) else {
                return Ok(CasAction::NoOp(-1));
            };
            let insert_at = if before { pos } else { pos + 1 };
            list.insert(insert_at, value.to_vec());
            let len = i64::try_from(list.len()).unwrap_or(i64::MAX);
            let new_entry = StoredValue { expires_at_ms, value: Value::List(list) };
            Ok(CasAction::Write(new_entry, len))
        })
        .await
    }

    async fn ltrim(&self, key: &str, start: i64, stop: i64) -> Result<(), RustyAntError> {
        self.cas(key, move |entry| {
            let (list, expires_at_ms) = match entry {
                Some(StoredValue { value: Value::List(l), expires_at_ms }) => (l.clone(), *expires_at_ms),
                Some(_) => return Err(wrong_type(key)),
                None => return Ok(CasAction::NoOp(())),
            };
            let Some((s, e_excl)) = resolve_trim_range(list.len(), start, stop) else {
                return Ok(CasAction::Delete(()));
            };
            let kept: Vec<Vec<u8>> = list[s..e_excl].to_vec();
            if kept.is_empty() {
                Ok(CasAction::Delete(()))
            } else {
                let new_entry = StoredValue { expires_at_ms, value: Value::List(kept) };
                Ok(CasAction::Write(new_entry, ()))
            }
        })
        .await
    }

    async fn list_pushx(&self, key: &str, values: Vec<Bytes>, left: bool) -> Result<i64, RustyAntError> {
        self.cas(key, move |entry| {
            let (mut list, expires_at_ms) = match entry {
                Some(StoredValue { value: Value::List(l), expires_at_ms }) => (l.clone(), *expires_at_ms),
                Some(_) => return Err(wrong_type(key)),
                None => return Ok(CasAction::NoOp(0)),
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

    async fn list_move(
        &self,
        from: &str,
        to: &str,
        from_left: bool,
        to_left: bool,
    ) -> Result<Option<Bytes>, RustyAntError> {
        if from == to {
            return self
                .cas(from, move |entry| {
                    let (mut list, expires_at_ms) = match entry {
                        Some(StoredValue { value: Value::List(l), expires_at_ms }) => (l.clone(), *expires_at_ms),
                        Some(_) => return Err(wrong_type(from)),
                        None => return Ok(CasAction::NoOp(None)),
                    };
                    if list.is_empty() {
                        return Ok(CasAction::NoOp(None));
                    }
                    let popped = if from_left { list.remove(0) } else { list.pop().expect("non-empty") };
                    if to_left {
                        list.insert(0, popped.clone());
                    } else {
                        list.push(popped.clone());
                    }
                    let new_entry = StoredValue { expires_at_ms, value: Value::List(list) };
                    Ok(CasAction::Write(new_entry, Some(Bytes::from(popped))))
                })
                .await;
        }
        // Pre-check dst type so we don't pop from src just to discover dst is
        // the wrong type. TOCTOU: a concurrent writer could still swap dst to
        // a non-list between here and the push — the push CAS closure will
        // surface that as wrong_type, after the pop has already landed. Same
        // best-effort guarantee as RENAME/COPY on S3.
        match self.load(to).await? {
            Some(StoredValue { value: Value::List(_), .. }) | None => {}
            Some(_) => return Err(wrong_type(to)),
        }
        let popped: Option<Vec<u8>> = self
            .cas(from, move |entry| {
                let (mut list, expires_at_ms) = match entry {
                    Some(StoredValue { value: Value::List(l), expires_at_ms }) => (l.clone(), *expires_at_ms),
                    Some(_) => return Err(wrong_type(from)),
                    None => return Ok(CasAction::NoOp(None)),
                };
                if list.is_empty() {
                    return Ok(CasAction::NoOp(None));
                }
                let popped = if from_left { list.remove(0) } else { list.pop().expect("non-empty") };
                if list.is_empty() {
                    Ok(CasAction::Delete(Some(popped)))
                } else {
                    let new_entry = StoredValue { expires_at_ms, value: Value::List(list) };
                    Ok(CasAction::Write(new_entry, Some(popped)))
                }
            })
            .await?;
        let Some(popped) = popped else {
            return Ok(None);
        };
        let out = popped.clone();
        self.cas(to, move |entry| {
            let (mut list, expires_at_ms) = match entry {
                Some(StoredValue { value: Value::List(l), expires_at_ms }) => (l.clone(), *expires_at_ms),
                Some(_) => return Err(wrong_type(to)),
                None => (Vec::new(), None),
            };
            if to_left {
                list.insert(0, popped.clone());
            } else {
                list.push(popped.clone());
            }
            let new_entry = StoredValue { expires_at_ms, value: Value::List(list) };
            Ok(CasAction::Write(new_entry, ()))
        })
        .await?;
        Ok(Some(Bytes::from(out)))
    }

    async fn lpos(
        &self,
        key: &str,
        element: &[u8],
        rank: i64,
        count: Option<usize>,
        maxlen: usize,
    ) -> Result<Vec<i64>, RustyAntError> {
        let list = match self.load(key).await? {
            Some(StoredValue { value: Value::List(l), .. }) => l,
            Some(_) => return Err(wrong_type(key)),
            None => return Ok(Vec::new()),
        };
        let wanted = count.map_or(1, |c| if c == 0 { usize::MAX } else { c });
        Ok(scan_list_positions(&list, element, rank, wanted, maxlen))
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
            let page = self.backend.list_page(cursor, 1000).await?;
            for k in page.keys {
                if wm.matches(&k) {
                    out.push(k);
                }
            }
            match page.next_cursor {
                Some(next) => cursor = Some(next),
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
        let page = self.backend.list_page(cursor.map(str::to_string), count).await?;
        let matched: Vec<String> = page.keys.into_iter().filter(|k| wm.as_ref().is_none_or(|w| w.matches(k))).collect();
        Ok((matched, page.next_cursor))
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

    async fn hincr_by_float(&self, key: &str, field: &str, delta: f64) -> Result<f64, RustyAntError> {
        let field = field.to_string();
        self.cas(key, move |entry| {
            let (mut map, expires_at_ms) = match entry {
                Some(StoredValue { value: Value::Hash(m), expires_at_ms }) => (m.clone(), *expires_at_ms),
                Some(_) => return Err(wrong_type(key)),
                None => (BTreeMap::new(), None),
            };
            let current: f64 = map
                .get(&field)
                .map(|v| {
                    let s =
                        std::str::from_utf8(v).map_err(|_| RustyAntError::Parse("hash value is not a float".into()))?;
                    s.parse::<f64>().map_err(|_| RustyAntError::Parse("hash value is not a float".into()))
                })
                .transpose()?
                .unwrap_or(0.0);
            let new_val = current + delta;
            if new_val.is_nan() || new_val.is_infinite() {
                return Err(RustyAntError::Parse("increment would produce NaN or infinity".into()));
            }
            map.insert(field.clone(), format_float(new_val).into_bytes());
            let new_entry = StoredValue { expires_at_ms, value: Value::Hash(map) };
            Ok(CasAction::Write(new_entry, new_val))
        })
        .await
    }

    async fn hrandfield(
        &self,
        key: &str,
        count: i64,
        allow_duplicates: bool,
    ) -> Result<Vec<(String, Vec<u8>)>, RustyAntError> {
        match self.load(key).await? {
            Some(StoredValue { value: Value::Hash(m), .. }) => Ok(pick_random_from_hash(&m, count, allow_duplicates)),
            Some(_) => Err(wrong_type(key)),
            None => Ok(Vec::new()),
        }
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

    async fn smismember(&self, key: &str, members: &[String]) -> Result<Vec<bool>, RustyAntError> {
        match self.load(key).await? {
            Some(StoredValue { value: Value::Set(s), .. }) => Ok(members.iter().map(|m| s.contains(m)).collect()),
            Some(_) => Err(wrong_type(key)),
            None => Ok(members.iter().map(|_| false).collect()),
        }
    }

    async fn sinter(&self, keys: &[String]) -> Result<Vec<String>, RustyAntError> {
        let sets = load_sets_seq(self, keys).await?;
        Ok(set_intersection(sets))
    }

    async fn sunion(&self, keys: &[String]) -> Result<Vec<String>, RustyAntError> {
        let sets = load_sets_seq(self, keys).await?;
        Ok(set_union(sets))
    }

    async fn sdiff(&self, keys: &[String]) -> Result<Vec<String>, RustyAntError> {
        let sets = load_sets_seq(self, keys).await?;
        Ok(set_difference(sets))
    }

    async fn spop(&self, key: &str, count: usize) -> Result<Vec<String>, RustyAntError> {
        if count == 0 {
            // Validate type even on no-op count so a string key still errors.
            match self.load(key).await? {
                Some(StoredValue { value: Value::Set(_), .. }) | None => return Ok(Vec::new()),
                Some(_) => return Err(wrong_type(key)),
            }
        }
        self.cas(key, move |entry| {
            let (mut set, expires_at_ms) = match entry {
                Some(StoredValue { value: Value::Set(s), expires_at_ms }) => (s.clone(), *expires_at_ms),
                Some(_) => return Err(wrong_type(key)),
                None => return Ok(CasAction::NoOp(Vec::new())),
            };
            let picked = pick_random_unique(&set, count);
            for m in &picked {
                set.remove(m);
            }
            if set.is_empty() {
                Ok(CasAction::Delete(picked))
            } else {
                let new_entry = StoredValue { expires_at_ms, value: Value::Set(set) };
                Ok(CasAction::Write(new_entry, picked))
            }
        })
        .await
    }

    async fn srandmember(&self, key: &str, count: i64, allow_duplicates: bool) -> Result<Vec<String>, RustyAntError> {
        let set = match self.load(key).await? {
            Some(StoredValue { value: Value::Set(s), .. }) => s,
            Some(_) => return Err(wrong_type(key)),
            None => return Ok(Vec::new()),
        };
        Ok(pick_random(&set, count, allow_duplicates))
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

    async fn zrevrange(&self, key: &str, start: i64, stop: i64) -> Result<Vec<String>, RustyAntError> {
        let map = match self.load(key).await? {
            Some(StoredValue { value: Value::ZSet(m), .. }) => m,
            Some(_) => return Err(wrong_type(key)),
            None => return Ok(Vec::new()),
        };
        Ok(slice_zset_reversed(map, start, stop))
    }

    async fn zrevrangebyscore(
        &self,
        key: &str,
        max: ScoreBound,
        min: ScoreBound,
    ) -> Result<Vec<String>, RustyAntError> {
        let map = match self.load(key).await? {
            Some(StoredValue { value: Value::ZSet(m), .. }) => m,
            Some(_) => return Err(wrong_type(key)),
            None => return Ok(Vec::new()),
        };
        let mut members = filter_zset_by_score(map, min, max);
        members.reverse();
        Ok(members)
    }

    async fn zremrangebyrank(&self, key: &str, start: i64, stop: i64) -> Result<i64, RustyAntError> {
        self.cas(key, move |entry| {
            let (map, expires_at_ms) = match entry {
                Some(StoredValue { value: Value::ZSet(m), expires_at_ms }) => (m.clone(), *expires_at_ms),
                Some(_) => return Err(wrong_type(key)),
                None => return Ok(CasAction::NoOp(0)),
            };
            let (kept, removed) = partition_zset_by_rank(map, start, stop);
            let removed_i = i64::try_from(removed).unwrap_or(i64::MAX);
            if kept.is_empty() {
                Ok(CasAction::Delete(removed_i))
            } else {
                let new_entry = StoredValue { expires_at_ms, value: Value::ZSet(kept) };
                Ok(CasAction::Write(new_entry, removed_i))
            }
        })
        .await
    }

    async fn zremrangebyscore(&self, key: &str, min: ScoreBound, max: ScoreBound) -> Result<i64, RustyAntError> {
        self.cas(key, move |entry| {
            let (map, expires_at_ms) = match entry {
                Some(StoredValue { value: Value::ZSet(m), expires_at_ms }) => (m.clone(), *expires_at_ms),
                Some(_) => return Err(wrong_type(key)),
                None => return Ok(CasAction::NoOp(0)),
            };
            let (kept, removed) = partition_zset_by_score(map, min, max);
            let removed_i = i64::try_from(removed).unwrap_or(i64::MAX);
            if kept.is_empty() {
                Ok(CasAction::Delete(removed_i))
            } else {
                let new_entry = StoredValue { expires_at_ms, value: Value::ZSet(kept) };
                Ok(CasAction::Write(new_entry, removed_i))
            }
        })
        .await
    }

    async fn zrangebylex(
        &self,
        key: &str,
        min: LexBound,
        max: LexBound,
        limit: Option<(i64, i64)>,
    ) -> Result<Vec<String>, RustyAntError> {
        match self.load(key).await? {
            Some(StoredValue { value: Value::ZSet(m), .. }) => {
                let filtered = filter_zset_by_lex(&m, &min, &max);
                Ok(apply_lex_limit(filtered, limit))
            }
            Some(_) => Err(wrong_type(key)),
            None => Ok(Vec::new()),
        }
    }

    async fn zrevrangebylex(
        &self,
        key: &str,
        max: LexBound,
        min: LexBound,
        limit: Option<(i64, i64)>,
    ) -> Result<Vec<String>, RustyAntError> {
        match self.load(key).await? {
            Some(StoredValue { value: Value::ZSet(m), .. }) => {
                let mut filtered = filter_zset_by_lex(&m, &min, &max);
                filtered.reverse();
                Ok(apply_lex_limit(filtered, limit))
            }
            Some(_) => Err(wrong_type(key)),
            None => Ok(Vec::new()),
        }
    }

    async fn zlexcount(&self, key: &str, min: LexBound, max: LexBound) -> Result<i64, RustyAntError> {
        match self.load(key).await? {
            Some(StoredValue { value: Value::ZSet(m), .. }) => {
                let n = m.keys().filter(|k| min.ge_min(k.as_str()) && max.le_max(k.as_str())).count();
                Ok(i64::try_from(n).unwrap_or(i64::MAX))
            }
            Some(_) => Err(wrong_type(key)),
            None => Ok(0),
        }
    }

    async fn zremrangebylex(&self, key: &str, min: LexBound, max: LexBound) -> Result<i64, RustyAntError> {
        self.cas(key, move |entry| {
            let (map, expires_at_ms) = match entry {
                Some(StoredValue { value: Value::ZSet(m), expires_at_ms }) => (m.clone(), *expires_at_ms),
                Some(_) => return Err(wrong_type(key)),
                None => return Ok(CasAction::NoOp(0)),
            };
            let mut kept = BTreeMap::new();
            let mut removed: i64 = 0;
            for (member, score) in map {
                if min.ge_min(member.as_str()) && max.le_max(member.as_str()) {
                    removed += 1;
                } else {
                    kept.insert(member, score);
                }
            }
            if kept.is_empty() {
                Ok(CasAction::Delete(removed))
            } else {
                let new_entry = StoredValue { expires_at_ms, value: Value::ZSet(kept) };
                Ok(CasAction::Write(new_entry, removed))
            }
        })
        .await
    }

    async fn zpopmin(&self, key: &str, count: usize) -> Result<Vec<(String, f64)>, RustyAntError> {
        self.zpop_impl(key, count, false).await
    }

    async fn zpopmax(&self, key: &str, count: usize) -> Result<Vec<(String, f64)>, RustyAntError> {
        self.zpop_impl(key, count, true).await
    }

    async fn rename(&self, from: &str, to: &str) -> Result<(), RustyAntError> {
        let Some(entry) = self.load(from).await? else {
            return Err(RustyAntError::Parse("no such key".into()));
        };
        if from == to {
            return Ok(());
        }
        // Match the destination's current etag (if present) to avoid stomping
        // on a concurrent writer; create-only otherwise. Two-object ops aren't
        // atomic over S3, but either half can still fail safely.
        let cond =
            self.backend.load(to).await?.map_or(WriteCondition::CreateOnly, |(_, etag)| WriteCondition::IfMatch(etag));
        self.backend.save(to, &entry, cond).await?;
        self.backend.delete(from, DeleteCondition::Any).await?;
        Ok(())
    }

    async fn renamenx(&self, from: &str, to: &str) -> Result<bool, RustyAntError> {
        let Some(entry) = self.load(from).await? else {
            return Err(RustyAntError::Parse("no such key".into()));
        };
        if from == to {
            return Ok(false);
        }
        if self.load(to).await?.is_some() {
            return Ok(false);
        }
        // CreateOnly guards against a racing writer populating the destination
        // between the existence check and this write.
        match self.backend.save(to, &entry, WriteCondition::CreateOnly).await {
            Ok(()) => {
                self.backend.delete(from, DeleteCondition::Any).await?;
                Ok(true)
            }
            Err(RustyAntError::Contention) => Ok(false),
            Err(e) => Err(e),
        }
    }

    async fn dbsize(&self) -> Result<i64, RustyAntError> {
        self.backend.count_keys().await
    }

    async fn flushall(&self) -> Result<(), RustyAntError> {
        self.backend.flush_all().await
    }

    async fn random_key(&self) -> Result<Option<String>, RustyAntError> {
        // Walk the full keyspace then pick — same O(n) caveat as Redis
        // without a native random-sampling primitive. Documented in README.
        let all = self.keys("*").await?;
        if all.is_empty() {
            return Ok(None);
        }
        let idx = usize::try_from(pseudo_rand_u64() % (all.len() as u64)).unwrap_or(0);
        Ok(Some(all[idx].clone()))
    }

    async fn copy(&self, from: &str, to: &str, replace: bool) -> Result<bool, RustyAntError> {
        if from == to {
            return Ok(false);
        }
        let Some(entry) = self.load(from).await? else {
            return Ok(false);
        };
        let dest = self.backend.load(to).await?;
        let cond = match (&dest, replace) {
            (Some(_), false) => return Ok(false),
            (Some((_, etag)), true) => WriteCondition::IfMatch(etag.clone()),
            (None, _) => WriteCondition::CreateOnly,
        };
        match self.backend.save(to, &entry, cond).await {
            Ok(()) => Ok(true),
            // CAS race: a concurrent writer beat us. Surface as "not copied"
            // rather than retrying — Redis COPY is a one-shot, not an RMW.
            Err(RustyAntError::Contention) => Ok(false),
            Err(e) => Err(e),
        }
    }

    async fn setbit(&self, key: &str, offset: u64, value: bool) -> Result<i64, RustyAntError> {
        self.cas(key, move |entry| {
            let (mut data, expires_at_ms) = match entry {
                Some(StoredValue { value: Value::String(d), expires_at_ms }) => (d.clone(), *expires_at_ms),
                Some(_) => return Err(wrong_type(key)),
                None => (Vec::new(), None),
            };
            let prev = apply_setbit(&mut data, offset, value);
            let new_entry = StoredValue { expires_at_ms, value: Value::String(data) };
            Ok(CasAction::Write(new_entry, i64::from(prev)))
        })
        .await
    }

    async fn dump(&self, key: &str) -> Result<Option<Bytes>, RustyAntError> {
        let Some(entry) = self.load(key).await? else {
            return Ok(None);
        };
        Ok(Some(crate::rdb::dump_value(&entry.value)?))
    }

    async fn restore(
        &self,
        key: &str,
        payload: &[u8],
        ttl_ms: i64,
        replace: bool,
        abs_ttl: bool,
    ) -> Result<(), RustyAntError> {
        let value = crate::rdb::restore_value(payload)?;
        let expires_at_ms = match ttl_ms {
            0 => None,
            n if abs_ttl => Some(n),
            n => Some(now_ms().saturating_add(n)),
        };
        let new_entry = StoredValue { expires_at_ms, value };
        if replace {
            // Overwrite unconditionally — REPLACE is explicit permission.
            self.backend.save(key, &new_entry, WriteCondition::Any).await?;
            return Ok(());
        }
        // Guard against existing-key: try create-only first; on Contention
        // (something already exists at the backend), translate to BUSYKEY.
        match self.backend.save(key, &new_entry, WriteCondition::CreateOnly).await {
            Ok(()) => Ok(()),
            Err(RustyAntError::Contention) => {
                Err(RustyAntError::Parse("BUSYKEY Target key name already exists.".into()))
            }
            Err(e) => Err(e),
        }
    }
}

#[allow(dead_code)]
const fn _assert_trait_object_safe() {
    const fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<std::sync::Arc<dyn Storage>>();
}
