//! Redis Streams — base primitives and shared parsing.
//!
//! Streams are an append-only log of `(id, field-value-map)` entries. The
//! id is `<ms>-<seq>`, monotonic within the key. Entries are kept sorted
//! ascending by id.
//!
//! This module owns the data types and the parsing helpers used by the
//! command handlers in `commands.rs`. Storage operations (append, range
//! scan, trim, delete) live on the `Storage` trait and are driven by the
//! CAS loop in `S3Storage`.

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Serialize};

use crate::error::RustyAntError;

/// Monotonic stream id. Redis formats ids as `"<ms>-<seq>"` on the wire;
/// we keep them as a pair for ordering and emit the formatted form when
/// serialising replies.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamId {
    pub ms: u64,
    pub seq: u64,
}

impl StreamId {
    pub const MIN: Self = Self { ms: 0, seq: 0 };
    pub const MAX: Self = Self { ms: u64::MAX, seq: u64::MAX };

    /// Parse a concrete id from the Redis wire form (`<ms>-<seq>` or just
    /// `<ms>`). Does NOT accept `*` / `-` / `+` — those only make sense in
    /// specific command contexts and are resolved by the caller.
    pub fn parse(s: &str) -> Result<Self, RustyAntError> {
        let (ms_part, seq_part) = s.split_once('-').map_or((s, None), |(a, b)| (a, Some(b)));
        let ms: u64 = ms_part
            .parse()
            .map_err(|_| RustyAntError::Parse("Invalid stream ID specified as stream command argument".into()))?;
        let seq: u64 = match seq_part {
            Some(b) => b
                .parse()
                .map_err(|_| RustyAntError::Parse("Invalid stream ID specified as stream command argument".into()))?,
            None => 0,
        };
        Ok(Self { ms, seq })
    }

    /// Increment by one seq (wrapping into the next ms on overflow). Used
    /// when a caller needs the "next" id after a known one — e.g. XREAD's
    /// exclusive "strictly after `id`" walk.
    #[must_use]
    pub const fn next(self) -> Self {
        if self.seq == u64::MAX {
            Self { ms: self.ms.wrapping_add(1), seq: 0 }
        } else {
            Self { ms: self.ms, seq: self.seq + 1 }
        }
    }
}

impl fmt::Display for StreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.ms, self.seq)
    }
}

impl PartialOrd for StreamId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for StreamId {
    fn cmp(&self, other: &Self) -> Ordering {
        self.ms.cmp(&other.ms).then_with(|| self.seq.cmp(&other.seq))
    }
}

/// An appended entry. Field ordering is preserved from the caller's
/// argument list, matching Redis's return order for `XRANGE` et al.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamEntry {
    pub id: StreamId,
    pub fields: Vec<(String, Vec<u8>)>,
}

/// The stream value-kind.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamValue {
    /// Entries in ascending id order.
    pub entries: Vec<StreamEntry>,
    /// Highest id ever generated for this stream — used to enforce
    /// monotonicity against auto-id (`*`) as well as explicit ids.
    #[serde(default)]
    pub last_generated_id: StreamId,
    /// Number of entries that have ever been deleted via `XDEL` or
    /// trimmed via `XADD MAXLEN` / `XTRIM`. Reported by `XINFO STREAM`
    /// and, in real Redis, used to compute consumer-group lag.
    #[serde(default)]
    pub max_deleted_entry_id: StreamId,
    /// Total entries ever added (including those since trimmed / deleted).
    #[serde(default)]
    pub entries_added: u64,
    /// Per-group state (consumers + pending entries list). Empty for
    /// streams with no groups.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub groups: BTreeMap<String, ConsumerGroup>,
}

/// Per-group state.
///
/// `last_delivered_id` advances on every `XREADGROUP > ...` call. The
/// `pel` (pending entries list) tracks entries that have been delivered
/// but not yet `XACK`ed; each consumer additionally records when it
/// last claimed an entry, used by `XCLAIM` / `XAUTOCLAIM` to find
/// stalled work.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsumerGroup {
    pub last_delivered_id: StreamId,
    /// Consumers known to this group, by name.
    #[serde(default)]
    pub consumers: BTreeMap<String, Consumer>,
    /// Pending entries: id → (consumer, `delivery_time_ms`, `delivery_count`).
    /// Serializes as a flat list of `[id, entry]` pairs so JSON can carry
    /// it (object keys must be strings, but `StreamId` is a struct).
    #[serde(default, with = "pel_serde")]
    pub pel: BTreeMap<StreamId, PendingEntry>,
}

mod pel_serde {
    use std::collections::BTreeMap;

    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    use super::{PendingEntry, StreamId};

    pub fn serialize<S: Serializer>(map: &BTreeMap<StreamId, PendingEntry>, s: S) -> Result<S::Ok, S::Error> {
        let pairs: Vec<(StreamId, PendingEntry)> = map.iter().map(|(k, v)| (*k, v.clone())).collect();
        pairs.serialize(s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<BTreeMap<StreamId, PendingEntry>, D::Error> {
        let pairs: Vec<(StreamId, PendingEntry)> = Vec::deserialize(d)?;
        Ok(pairs.into_iter().collect())
    }
}

/// A single consumer within a group.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Consumer {
    /// Wall-clock ms of the most recent delivery attributed to this consumer.
    pub seen_ms: i64,
}

/// One row of the pending-entries list.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingEntry {
    pub consumer: String,
    pub delivery_time_ms: i64,
    pub delivery_count: u64,
}

// ---------------------------------------------------------------------------
// XGROUP — subcommand router payload, plus per-op metadata.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub enum XGroupOp {
    /// Create a new group at `start_id` (Redis: `<id> | $`). `mkstream`
    /// auto-creates the key as an empty stream when missing. `entries_read`
    /// is a Redis 7+ optimization hint; rustyant accepts it but ignores.
    Create { group: String, start_id: GroupStartId, mkstream: bool },
    /// Reset the group's `last_delivered_id`.
    SetId { group: String, start_id: GroupStartId },
    /// Drop the group. Returns 1 if removed, 0 otherwise.
    Destroy { group: String },
    /// Force-add a consumer (returns 1 if created, 0 if it already existed).
    CreateConsumer { group: String, consumer: String },
    /// Remove a consumer; returns the number of pending entries that were
    /// owned by it (matching Redis's reply).
    DelConsumer { group: String, consumer: String },
}

/// `XGROUP CREATE` / `XGROUP SETID` accept either a concrete id or `$`,
/// which means "the current `last_generated_id` of the stream".
#[derive(Debug, Clone, Copy)]
pub enum GroupStartId {
    Concrete(StreamId),
    Latest,
}

impl GroupStartId {
    pub fn parse(s: &str) -> Result<Self, RustyAntError> {
        if s == "$" { Ok(Self::Latest) } else { Ok(Self::Concrete(StreamId::parse(s)?)) }
    }
}

/// Each `id` arg to `XREADGROUP` — `>` means "new entries", anything
/// else is a concrete id from the PEL.
#[derive(Debug, Clone, Copy)]
pub enum XReadGroupId {
    NewEntries,
    Pending(StreamId),
}

impl XReadGroupId {
    pub fn parse(s: &str) -> Result<Self, RustyAntError> {
        if s == ">" { Ok(Self::NewEntries) } else { Ok(Self::Pending(StreamId::parse(s)?)) }
    }
}

/// Options for `XCLAIM`.
///
/// `idle_ms` / `time_ms` override the default "set to current time"
/// behavior; `retry_count` overrides the default "increment by 1".
/// `force` adds the entry to the PEL even when not already pending.
/// `just_id` returns just the ids, not full entries.
#[derive(Debug, Clone, Copy, Default)]
pub struct XClaimOpts {
    pub idle_ms: Option<i64>,
    pub time_ms: Option<i64>,
    pub retry_count: Option<u64>,
    pub force: bool,
    pub just_id: bool,
    /// Wall-clock used when `idle_ms` and `time_ms` are unset — caller
    /// supplies this so the storage layer doesn't need to read a clock.
    pub now_ms: i64,
}

/// One entry returned by `XCLAIM`. Holds the full entry body unless
/// `just_id` was requested in the call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct XClaimResult {
    pub id: StreamId,
    pub fields: Option<Vec<(String, Vec<u8>)>>,
}

/// `XPENDING <key> <group>` summary form.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct XPendingSummary {
    pub count: u64,
    pub min: StreamId,
    pub max: StreamId,
    /// (consumer, count) pairs for every consumer with at least one
    /// pending entry.
    pub per_consumer: Vec<(String, u64)>,
}

/// One row of the `XPENDING <key> <group> [...] start end count` form.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct XPendingDetailRow {
    pub id: StreamId,
    pub consumer: String,
    pub idle_ms: i64,
    pub delivery_count: u64,
}

/// Result tuple for `XAUTOCLAIM`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct XAutoClaimResult {
    /// Next id to resume from (`0-0` when the sweep finished).
    pub next_cursor: StreamId,
    pub claimed: Vec<XClaimResult>,
    /// Ids that were in the PEL but no longer in the stream (Redis 7
    /// added this — XAUTOCLAIM "delete" ids).
    pub deleted_ids: Vec<StreamId>,
}

/// `XINFO GROUPS` row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct XInfoGroup {
    pub name: String,
    pub consumers: u64,
    pub pending: u64,
    pub last_delivered_id: StreamId,
}

/// `XINFO CONSUMERS` row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct XInfoConsumer {
    pub name: String,
    pub pending: u64,
    pub idle_ms: i64,
}

/// Parsed id argument for `XADD` — either auto (`*`), an auto-seq within
/// a specific ms (`<ms>-*`), or a concrete `<ms>-<seq>`.
#[derive(Debug, Clone, Copy)]
pub enum AddIdSpec {
    Auto,
    PartialMs(u64),
    Concrete(StreamId),
}

impl AddIdSpec {
    pub fn parse(s: &str) -> Result<Self, RustyAntError> {
        if s == "*" {
            return Ok(Self::Auto);
        }
        if let Some((ms_part, seq_part)) = s.split_once('-') {
            let ms: u64 = ms_part
                .parse()
                .map_err(|_| RustyAntError::Parse("Invalid stream ID specified as stream command argument".into()))?;
            if seq_part == "*" {
                return Ok(Self::PartialMs(ms));
            }
            let seq: u64 = seq_part
                .parse()
                .map_err(|_| RustyAntError::Parse("Invalid stream ID specified as stream command argument".into()))?;
            return Ok(Self::Concrete(StreamId { ms, seq }));
        }
        // Bare number — same as <ms>-0.
        let ms: u64 = s
            .parse()
            .map_err(|_| RustyAntError::Parse("Invalid stream ID specified as stream command argument".into()))?;
        Ok(Self::Concrete(StreamId { ms, seq: 0 }))
    }
}

/// Parsed start/end id for range queries.
///
/// `-` / `+` are the stream-wide extremes; an explicit id parses the
/// standard form. Redis also accepts a `(` prefix for exclusivity
/// (Redis 6.2+); this module supports both.
#[derive(Debug, Clone, Copy)]
pub enum RangeId {
    MinInf,
    MaxInf,
    Inclusive(StreamId),
    /// `(<ms>-<seq>` — exclusive, typically used for pagination.
    Exclusive(StreamId),
}

impl RangeId {
    pub fn parse(s: &str) -> Result<Self, RustyAntError> {
        match s {
            "-" => Ok(Self::MinInf),
            "+" => Ok(Self::MaxInf),
            other => {
                if let Some(rest) = other.strip_prefix('(') {
                    Ok(Self::Exclusive(StreamId::parse(rest)?))
                } else {
                    Ok(Self::Inclusive(StreamId::parse(other)?))
                }
            }
        }
    }

    /// Determine whether `id` is above the minimum bound this range
    /// represents.
    pub fn ge_min(self, id: StreamId) -> bool {
        match self {
            Self::MinInf => true,
            Self::MaxInf => false,
            Self::Inclusive(bound) => id >= bound,
            Self::Exclusive(bound) => id > bound,
        }
    }

    /// Determine whether `id` is below the maximum bound this range
    /// represents.
    pub fn le_max(self, id: StreamId) -> bool {
        match self {
            Self::MinInf => false,
            Self::MaxInf => true,
            Self::Inclusive(bound) => id <= bound,
            Self::Exclusive(bound) => id < bound,
        }
    }
}

/// Resolve an `AddIdSpec` against `last_generated_id` and the current
/// wall-clock `now_ms`. Returns the concrete id to store, or an error if
/// the caller-supplied id would go backwards.
pub fn resolve_add_id(spec: AddIdSpec, last: StreamId, now_ms: u64) -> Result<StreamId, RustyAntError> {
    let candidate = match spec {
        AddIdSpec::Auto => {
            if now_ms > last.ms {
                StreamId { ms: now_ms, seq: 0 }
            } else {
                // Same ms (or clock went backwards) — bump seq from last.
                last.next()
            }
        }
        AddIdSpec::PartialMs(ms) => {
            if ms < last.ms {
                return Err(RustyAntError::Parse(
                    "The ID specified in XADD is equal or smaller than the target stream top item".into(),
                ));
            }
            if ms == last.ms { last.next() } else { StreamId { ms, seq: 0 } }
        }
        AddIdSpec::Concrete(id) => {
            if id <= last {
                return Err(RustyAntError::Parse(
                    "The ID specified in XADD is equal or smaller than the target stream top item".into(),
                ));
            }
            id
        }
    };
    Ok(candidate)
}

/// `XTRIM` mode — MAXLEN bounds by entry count, MINID bounds by id.
#[derive(Debug, Clone, Copy)]
pub enum TrimBound {
    MaxLen(usize),
    MinId(StreamId),
}

/// Parse a `MAXLEN [~|=] n` / `MINID [~|=] id` pair plus optional
/// `LIMIT n` at the start of `tokens`. Returns the bound, optional
/// LIMIT, and the number of tokens consumed.
///
/// The `~` / `=` modifier is accepted and otherwise ignored: on S3 we
/// always apply exactly, and `~` is a Redis-internal optimisation hint.
/// LIMIT caps the number of entries trimmed per call; we honor it.
pub fn parse_trim(tokens: &[&str]) -> Result<(TrimBound, Option<usize>, usize), RustyAntError> {
    if tokens.is_empty() {
        return Err(RustyAntError::Parse("syntax error".into()));
    }
    let keyword = tokens[0].to_ascii_uppercase();
    let (bound_kind, mut consumed) = match keyword.as_str() {
        "MAXLEN" | "MINID" => (keyword, 1usize),
        _ => return Err(RustyAntError::Parse("syntax error".into())),
    };
    // Optional ~ / =.
    if tokens.get(consumed).is_some_and(|t| *t == "~" || *t == "=") {
        consumed += 1;
    }
    let threshold_tok = tokens.get(consumed).ok_or_else(|| RustyAntError::Parse("syntax error".into()))?;
    consumed += 1;
    let bound = match bound_kind.as_str() {
        "MAXLEN" => {
            let n: i64 = threshold_tok
                .parse()
                .map_err(|_| RustyAntError::Parse("value is not an integer or out of range".into()))?;
            if n < 0 {
                return Err(RustyAntError::Parse("MAXLEN must be non-negative".into()));
            }
            TrimBound::MaxLen(usize::try_from(n).unwrap_or(0))
        }
        _ => TrimBound::MinId(StreamId::parse(threshold_tok)?),
    };

    // Optional LIMIT n.
    let limit: Option<usize> = if tokens.get(consumed).is_some_and(|t| t.eq_ignore_ascii_case("LIMIT")) {
        consumed += 1;
        let n_tok = tokens.get(consumed).ok_or_else(|| RustyAntError::Parse("syntax error".into()))?;
        consumed += 1;
        let n: i64 =
            n_tok.parse().map_err(|_| RustyAntError::Parse("value is not an integer or out of range".into()))?;
        if n < 0 {
            return Err(RustyAntError::Parse("LIMIT must be non-negative".into()));
        }
        Some(usize::try_from(n).unwrap_or(0))
    } else {
        None
    };

    Ok((bound, limit, consumed))
}

/// Apply a `TrimBound` to the stream's entries in-place. Returns the
/// number of entries removed. `limit` caps the work per call (Redis
/// semantics — useful on very long streams; 0-cap removes nothing).
pub fn trim_in_place(stream: &mut StreamValue, bound: TrimBound, limit: Option<usize>) -> usize {
    let target_start_idx = match bound {
        TrimBound::MaxLen(max) => stream.entries.len().saturating_sub(max),
        TrimBound::MinId(min) => stream.entries.partition_point(|e| e.id < min),
    };
    let mut to_remove = target_start_idx;
    if let Some(cap) = limit {
        to_remove = to_remove.min(cap);
    }
    if to_remove == 0 {
        return 0;
    }
    let removed: Vec<StreamEntry> = stream.entries.drain(..to_remove).collect();
    if let Some(last) = removed.last() {
        if last.id > stream.max_deleted_entry_id {
            stream.max_deleted_entry_id = last.id;
        }
    }
    removed.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_id_roundtrip() {
        let id = StreamId::parse("1726934567890-5").expect("parse");
        assert_eq!(id.to_string(), "1726934567890-5");
        assert_eq!(StreamId::parse("42").expect("parse"), StreamId { ms: 42, seq: 0 });
    }

    #[test]
    fn stream_id_ordering_within_and_across_ms() {
        assert!(StreamId { ms: 1, seq: 2 } < StreamId { ms: 1, seq: 3 });
        assert!(StreamId { ms: 1, seq: 99 } < StreamId { ms: 2, seq: 0 });
    }

    #[test]
    fn auto_id_uses_now_when_ahead() {
        let last = StreamId { ms: 10, seq: 5 };
        let id = resolve_add_id(AddIdSpec::Auto, last, 50).expect("resolve");
        assert_eq!(id, StreamId { ms: 50, seq: 0 });
    }

    #[test]
    fn auto_id_bumps_seq_when_clock_not_advanced() {
        let last = StreamId { ms: 10, seq: 5 };
        let id = resolve_add_id(AddIdSpec::Auto, last, 10).expect("resolve");
        assert_eq!(id, StreamId { ms: 10, seq: 6 });
    }

    #[test]
    fn partial_ms_fills_seq() {
        let last = StreamId { ms: 5, seq: 9 };
        // ms ahead of last → seq starts at 0
        let id = resolve_add_id(AddIdSpec::PartialMs(6), last, 100).expect("resolve");
        assert_eq!(id, StreamId { ms: 6, seq: 0 });
        // same ms → seq bumps
        let id2 = resolve_add_id(AddIdSpec::PartialMs(5), last, 100).expect("resolve");
        assert_eq!(id2, StreamId { ms: 5, seq: 10 });
    }

    #[test]
    fn explicit_id_rejected_when_not_monotonic() {
        let last = StreamId { ms: 5, seq: 0 };
        assert!(resolve_add_id(AddIdSpec::Concrete(StreamId { ms: 5, seq: 0 }), last, 100).is_err());
        assert!(resolve_add_id(AddIdSpec::Concrete(StreamId { ms: 4, seq: 99 }), last, 100).is_err());
        assert!(resolve_add_id(AddIdSpec::Concrete(StreamId { ms: 5, seq: 1 }), last, 100).is_ok());
    }

    #[test]
    fn trim_maxlen_removes_head() {
        let mut s = StreamValue::default();
        for ms in 1..=5u64 {
            s.entries.push(StreamEntry { id: StreamId { ms, seq: 0 }, fields: vec![] });
        }
        assert_eq!(trim_in_place(&mut s, TrimBound::MaxLen(2), None), 3);
        assert_eq!(s.entries.len(), 2);
        assert_eq!(s.entries[0].id.ms, 4);
    }

    #[test]
    fn trim_minid_removes_below_threshold() {
        let mut s = StreamValue::default();
        for ms in 1..=5u64 {
            s.entries.push(StreamEntry { id: StreamId { ms, seq: 0 }, fields: vec![] });
        }
        assert_eq!(trim_in_place(&mut s, TrimBound::MinId(StreamId { ms: 3, seq: 0 }), None), 2);
        assert_eq!(s.entries[0].id.ms, 3);
    }

    #[test]
    fn trim_honors_limit() {
        let mut s = StreamValue::default();
        for ms in 1..=5u64 {
            s.entries.push(StreamEntry { id: StreamId { ms, seq: 0 }, fields: vec![] });
        }
        // Want to remove 3 (to keep 2), but LIMIT 1 caps at 1 removal.
        assert_eq!(trim_in_place(&mut s, TrimBound::MaxLen(2), Some(1)), 1);
        assert_eq!(s.entries.len(), 4);
    }

    #[test]
    fn range_id_bounds() {
        let r = RangeId::parse("-").expect("parse");
        assert!(r.ge_min(StreamId { ms: 0, seq: 0 }));
        let r = RangeId::parse("(5-0").expect("parse");
        assert!(!r.ge_min(StreamId { ms: 5, seq: 0 }));
        assert!(r.ge_min(StreamId { ms: 5, seq: 1 }));
    }
}
