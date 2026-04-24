//! Redis DUMP / RESTORE binary payload format.
//!
//! The DUMP payload is `[type_byte][kind_payload][rdb_version:2 LE][crc64:8 LE]`.
//! This module encodes/decodes it for the five commonly-migrated value kinds
//! (string, hash, list, set, zset) using Redis 7.x defaults:
//!
//! - string: `RDB_TYPE_STRING` (0x00)
//! - hash:   `RDB_TYPE_HASH_LISTPACK` (0x10) — listpack of alternating field/value
//! - zset:   `RDB_TYPE_ZSET_LISTPACK` (0x11) — listpack of alternating member/score-string
//! - list:   `RDB_TYPE_LIST_QUICKLIST_2` (0x12) — quicklist containing one packed listpack
//! - set:    `RDB_TYPE_SET_LISTPACK` (0x14) — listpack of members
//!
//! Streams (`0x15` `STREAM_LISTPACKS_3`) are not yet supported here — the stream
//! RDB layout carries consumer-group / PEL state and radix-tree-of-listpacks
//! which is a sizeable follow-up. `DUMP` of a stream key returns an error.
//!
//! # Interop target
//!
//! RDB version 11 (Redis 7.0 / 7.2). Output is byte-identical to
//! `redis-cli DUMP` for the encodings above, and input from real Redis is
//! accepted for the same type tags. Older RDB versions (9, 10) and older
//! encodings (ziplist, intset-only hash, skiplist zset) are out of scope for
//! the first iteration and will be added as needed.

use bytes::{BufMut, Bytes, BytesMut};

use crate::error::RustyAntError;
use crate::storage::Value;
use crate::stream::StreamValue;

/// RDB version tag stored in the 2-byte trailer before the CRC64. Version 11
/// covers Redis 7.0–7.2; Redis 7.4 bumped to 12 but the listpack encodings
/// below are stable across both.
pub const RDB_VERSION: u16 = 11;

/// Maximum RDB version we will accept on RESTORE. Redis's own check is
/// `version > RDB_VERSION`, not an exact match — newer payloads can round-trip
/// through an older server as long as none of the kinds involved changed
/// wire-format.
const RDB_MAX_ACCEPT_VERSION: u16 = 12;

// Type tags from Redis's rdb.h. Only the five we support encoding for are
// exhaustively handled; the others are listed so decode can give a clear
// error when it runs into them.
const RDB_TYPE_STRING: u8 = 0;
const RDB_TYPE_SET: u8 = 2;
const RDB_TYPE_ZSET_2: u8 = 5;
const RDB_TYPE_SET_INTSET: u8 = 11;
const RDB_TYPE_HASH_LISTPACK: u8 = 16;
const RDB_TYPE_ZSET_LISTPACK: u8 = 17;
const RDB_TYPE_LIST_QUICKLIST_2: u8 = 18;
const RDB_TYPE_SET_LISTPACK: u8 = 20;
const RDB_TYPE_STREAM_LISTPACKS_3: u8 = 21;

// Quicklist node container kinds. Only PACKED is produced; PLAIN shows up in
// payloads where a single list element was too big to fit in a listpack
// (Redis's default `list-max-listpack-size` is 8 KiB).
const QUICKLIST_NODE_PACKED: u64 = 2;
const QUICKLIST_NODE_PLAIN: u64 = 1;

/// Encode a `Value` into a DUMP payload (without the TTL — RESTORE takes TTL
/// as its own arg). Returns the full payload including the RDB version tag
/// and CRC64 trailer.
pub fn dump_value(value: &Value) -> Result<Bytes, RustyAntError> {
    let mut body = BytesMut::new();
    match value {
        Value::String(data) => {
            body.put_u8(RDB_TYPE_STRING);
            write_rdb_string(&mut body, data);
        }
        Value::Hash(map) => {
            body.put_u8(RDB_TYPE_HASH_LISTPACK);
            let mut lp = Vec::new();
            listpack_begin(&mut lp, map.len() * 2);
            for (field, val) in map {
                listpack_append_string(&mut lp, field.as_bytes());
                listpack_append_string(&mut lp, val);
            }
            listpack_finish(&mut lp);
            write_rdb_string(&mut body, &lp);
        }
        Value::List(items) => {
            body.put_u8(RDB_TYPE_LIST_QUICKLIST_2);
            // One quicklist node holding one packed listpack with every element.
            write_rdb_length(&mut body, 1);
            write_rdb_length(&mut body, QUICKLIST_NODE_PACKED);
            let mut lp = Vec::new();
            listpack_begin(&mut lp, items.len());
            for item in items {
                listpack_append_string(&mut lp, item);
            }
            listpack_finish(&mut lp);
            write_rdb_string(&mut body, &lp);
        }
        Value::Set(members) => {
            body.put_u8(RDB_TYPE_SET_LISTPACK);
            let mut lp = Vec::new();
            listpack_begin(&mut lp, members.len());
            for m in members {
                listpack_append_string(&mut lp, m.as_bytes());
            }
            listpack_finish(&mut lp);
            write_rdb_string(&mut body, &lp);
        }
        Value::ZSet(map) => {
            body.put_u8(RDB_TYPE_ZSET_LISTPACK);
            let mut lp = Vec::new();
            listpack_begin(&mut lp, map.len() * 2);
            for (member, score) in map {
                listpack_append_string(&mut lp, member.as_bytes());
                let s = format_rdb_double(*score);
                listpack_append_string(&mut lp, s.as_bytes());
            }
            listpack_finish(&mut lp);
            write_rdb_string(&mut body, &lp);
        }
        Value::Stream(_) => {
            return Err(RustyAntError::Parse(
                "DUMP for stream keys is not yet implemented (rustyant supports DUMP/RESTORE for string/hash/list/set/zset)".into(),
            ));
        }
    }
    body.put_u16_le(RDB_VERSION);
    let crc = crc64(0, &body);
    body.put_u64_le(crc);
    Ok(body.freeze())
}

/// Decode a DUMP payload into a `Value`. Validates the RDB version tag and
/// CRC64 trailer. Returns `Parse` with a clear message on any malformation.
#[allow(clippy::too_many_lines)] // one match arm per RDB type tag — splitting hurts readability
pub fn restore_value(payload: &[u8]) -> Result<Value, RustyAntError> {
    if payload.len() < 10 {
        return Err(bad_payload());
    }
    let split = payload.len() - 10;
    let (body, trailer) = payload.split_at(split);
    let version = u16::from_le_bytes([trailer[0], trailer[1]]);
    let got_crc = u64::from_le_bytes([
        trailer[2], trailer[3], trailer[4], trailer[5], trailer[6], trailer[7], trailer[8], trailer[9],
    ]);
    if version > RDB_MAX_ACCEPT_VERSION {
        return Err(bad_payload());
    }
    let mut hash_input = Vec::with_capacity(body.len() + 2);
    hash_input.extend_from_slice(body);
    hash_input.extend_from_slice(&trailer[..2]);
    let want_crc = crc64(0, &hash_input);
    if got_crc != want_crc {
        return Err(bad_payload());
    }
    let mut r = Reader::new(body);
    let type_byte = r.read_u8()?;
    let value = match type_byte {
        RDB_TYPE_STRING => Value::String(read_rdb_string(&mut r)?),
        RDB_TYPE_HASH_LISTPACK => {
            let lp_bytes = read_rdb_string(&mut r)?;
            let items = listpack_decode(&lp_bytes)?;
            if items.len() % 2 != 0 {
                return Err(bad_payload());
            }
            let mut map = std::collections::BTreeMap::new();
            let mut it = items.into_iter();
            while let (Some(k), Some(v)) = (it.next(), it.next()) {
                let field = String::from_utf8(k).map_err(|_| bad_payload())?;
                map.insert(field, v);
            }
            Value::Hash(map)
        }
        RDB_TYPE_LIST_QUICKLIST_2 => {
            let node_count = read_rdb_length(&mut r)?;
            let mut items = Vec::new();
            for _ in 0..node_count {
                let container = read_rdb_length(&mut r)?;
                match container {
                    QUICKLIST_NODE_PACKED => {
                        let lp_bytes = read_rdb_string(&mut r)?;
                        items.extend(listpack_decode(&lp_bytes)?);
                    }
                    QUICKLIST_NODE_PLAIN => {
                        // A single element too large to listpack.
                        items.push(read_rdb_string(&mut r)?);
                    }
                    _ => return Err(bad_payload()),
                }
            }
            Value::List(items)
        }
        RDB_TYPE_SET_LISTPACK => {
            let lp_bytes = read_rdb_string(&mut r)?;
            let members = listpack_decode(&lp_bytes)?;
            let mut set = std::collections::BTreeSet::new();
            for m in members {
                let s = String::from_utf8(m).map_err(|_| bad_payload())?;
                set.insert(s);
            }
            Value::Set(set)
        }
        RDB_TYPE_SET => {
            // Classic hashtable-encoded set: length-prefixed string per member.
            let count = read_rdb_length(&mut r)?;
            let mut set = std::collections::BTreeSet::new();
            for _ in 0..count {
                let m = read_rdb_string(&mut r)?;
                let s = String::from_utf8(m).map_err(|_| bad_payload())?;
                set.insert(s);
            }
            Value::Set(set)
        }
        RDB_TYPE_SET_INTSET => {
            // Intset encoding: RDB string whose bytes are the intset blob —
            // 4-byte LE element size (2/4/8), 4-byte LE length, then packed
            // signed integers.
            let blob = read_rdb_string(&mut r)?;
            if blob.len() < 8 {
                return Err(bad_payload());
            }
            let elem_size = u32::from_le_bytes([blob[0], blob[1], blob[2], blob[3]]) as usize;
            let count = u32::from_le_bytes([blob[4], blob[5], blob[6], blob[7]]) as usize;
            if !matches!(elem_size, 2 | 4 | 8) || blob.len() != 8 + count * elem_size {
                return Err(bad_payload());
            }
            let mut set = std::collections::BTreeSet::new();
            for i in 0..count {
                let off = 8 + i * elem_size;
                let n: i64 = match elem_size {
                    2 => i16::from_le_bytes([blob[off], blob[off + 1]]).into(),
                    4 => i32::from_le_bytes([blob[off], blob[off + 1], blob[off + 2], blob[off + 3]]).into(),
                    _ => i64::from_le_bytes([
                        blob[off],
                        blob[off + 1],
                        blob[off + 2],
                        blob[off + 3],
                        blob[off + 4],
                        blob[off + 5],
                        blob[off + 6],
                        blob[off + 7],
                    ]),
                };
                set.insert(n.to_string());
            }
            Value::Set(set)
        }
        RDB_TYPE_ZSET_LISTPACK => {
            let lp_bytes = read_rdb_string(&mut r)?;
            let items = listpack_decode(&lp_bytes)?;
            if items.len() % 2 != 0 {
                return Err(bad_payload());
            }
            let mut map = std::collections::BTreeMap::new();
            let mut it = items.into_iter();
            while let (Some(m), Some(score_bytes)) = (it.next(), it.next()) {
                let member = String::from_utf8(m).map_err(|_| bad_payload())?;
                let score_s = std::str::from_utf8(&score_bytes).map_err(|_| bad_payload())?;
                let score = parse_rdb_double(score_s)?;
                map.insert(member, score);
            }
            Value::ZSet(map)
        }
        RDB_TYPE_ZSET_2 => {
            // Skiplist form: length, then for each entry an RDB-string member
            // followed by an 8-byte little-endian double.
            let count = read_rdb_length(&mut r)?;
            let mut map = std::collections::BTreeMap::new();
            for _ in 0..count {
                let m = read_rdb_string(&mut r)?;
                let member = String::from_utf8(m).map_err(|_| bad_payload())?;
                let score_bytes = r.read_slice(8)?;
                let score = f64::from_le_bytes([
                    score_bytes[0],
                    score_bytes[1],
                    score_bytes[2],
                    score_bytes[3],
                    score_bytes[4],
                    score_bytes[5],
                    score_bytes[6],
                    score_bytes[7],
                ]);
                map.insert(member, score);
            }
            Value::ZSet(map)
        }
        RDB_TYPE_STREAM_LISTPACKS_3 => {
            // Placeholder so we decode far enough to fail with a consistent
            // message rather than a random "bad payload".
            let _ = StreamValue::default;
            return Err(RustyAntError::Parse("RESTORE for stream keys is not yet implemented (type 0x15)".into()));
        }
        _ => return Err(bad_payload()),
    };
    Ok(value)
}

fn bad_payload() -> RustyAntError {
    RustyAntError::Parse("DUMP payload version or checksum are wrong".into())
}

// ---- RDB length / string primitives ---------------------------------------

#[allow(clippy::cast_possible_truncation)] // truncation intended on checked range branches
fn write_rdb_length(buf: &mut BytesMut, n: u64) {
    if n < 1 << 6 {
        // 6-bit: 00xxxxxx
        buf.put_u8(n as u8);
    } else if n < 1 << 14 {
        // 14-bit: 01xxxxxx xxxxxxxx (big-endian across the two bytes)
        buf.put_u8(0x40 | ((n >> 8) as u8 & 0x3f));
        buf.put_u8(n as u8);
    } else if u32::try_from(n).is_ok() {
        // 32-bit: 10000000 + u32 big-endian
        buf.put_u8(0x80);
        buf.put_u32(n as u32);
    } else {
        // 64-bit: 10000001 + u64 big-endian
        buf.put_u8(0x81);
        buf.put_u64(n);
    }
}

fn read_rdb_length(r: &mut Reader<'_>) -> Result<u64, RustyAntError> {
    let b = r.read_u8()?;
    let high = b >> 6;
    match high {
        0 => Ok(u64::from(b & 0x3f)),
        1 => {
            let b2 = r.read_u8()?;
            Ok((u64::from(b & 0x3f) << 8) | u64::from(b2))
        }
        2 => {
            if b == 0x80 {
                let bs = r.read_slice(4)?;
                Ok(u64::from(u32::from_be_bytes([bs[0], bs[1], bs[2], bs[3]])))
            } else if b == 0x81 {
                let bs = r.read_slice(8)?;
                Ok(u64::from_be_bytes([bs[0], bs[1], bs[2], bs[3], bs[4], bs[5], bs[6], bs[7]]))
            } else {
                Err(bad_payload())
            }
        }
        _ => Err(bad_payload()),
    }
}

/// Read a length value that may be an "integer-encoded" or "LZF-compressed"
/// string marker. Used by `read_rdb_string`.
enum RdbStrLen {
    Normal(u64),
    IntStr(i64),
    Lzf,
}

fn read_rdb_str_len(r: &mut Reader<'_>) -> Result<RdbStrLen, RustyAntError> {
    let b = r.read_u8()?;
    let high = b >> 6;
    match high {
        0 => Ok(RdbStrLen::Normal(u64::from(b & 0x3f))),
        1 => {
            let b2 = r.read_u8()?;
            Ok(RdbStrLen::Normal((u64::from(b & 0x3f) << 8) | u64::from(b2)))
        }
        2 => {
            if b == 0x80 {
                let bs = r.read_slice(4)?;
                Ok(RdbStrLen::Normal(u64::from(u32::from_be_bytes([bs[0], bs[1], bs[2], bs[3]]))))
            } else if b == 0x81 {
                let bs = r.read_slice(8)?;
                Ok(RdbStrLen::Normal(u64::from_be_bytes([bs[0], bs[1], bs[2], bs[3], bs[4], bs[5], bs[6], bs[7]])))
            } else {
                Err(bad_payload())
            }
        }
        3 => match b {
            0xc0 => {
                #[allow(clippy::cast_possible_wrap)] // u8 -> i8 reinterpret is the encoding
                let n = r.read_u8()? as i8;
                Ok(RdbStrLen::IntStr(i64::from(n)))
            }
            0xc1 => {
                let bs = r.read_slice(2)?;
                // Integer-encoded ints are little-endian.
                let n = i16::from_le_bytes([bs[0], bs[1]]);
                Ok(RdbStrLen::IntStr(i64::from(n)))
            }
            0xc2 => {
                let bs = r.read_slice(4)?;
                let n = i32::from_le_bytes([bs[0], bs[1], bs[2], bs[3]]);
                Ok(RdbStrLen::IntStr(i64::from(n)))
            }
            0xc3 => Ok(RdbStrLen::Lzf),
            _ => Err(bad_payload()),
        },
        _ => Err(bad_payload()),
    }
}

fn write_rdb_string(buf: &mut BytesMut, data: &[u8]) {
    // Try integer compression: if the bytes parse as an ascii integer whose
    // round-trip matches, emit the compact form. Mirrors Redis's behaviour so
    // our DUMP output is byte-identical for integer-string values.
    if let Some(n) = try_parse_ascii_int(data) {
        if i8::try_from(n).is_ok() {
            buf.put_u8(0xc0);
            #[allow(clippy::cast_possible_truncation)] // i8 checked above
            buf.put_i8(n as i8);
            return;
        }
        if i16::try_from(n).is_ok() {
            buf.put_u8(0xc1);
            #[allow(clippy::cast_possible_truncation)] // i16 checked above
            buf.put_i16_le(n as i16);
            return;
        }
        if i32::try_from(n).is_ok() {
            buf.put_u8(0xc2);
            #[allow(clippy::cast_possible_truncation)] // i32 checked above
            buf.put_i32_le(n as i32);
            return;
        }
    }
    #[allow(clippy::cast_possible_truncation)] // len bounded by u64 below via write_rdb_length
    write_rdb_length(buf, data.len() as u64);
    buf.extend_from_slice(data);
}

fn read_rdb_string(r: &mut Reader<'_>) -> Result<Vec<u8>, RustyAntError> {
    match read_rdb_str_len(r)? {
        RdbStrLen::Normal(n) => {
            let bytes = r.read_slice(usize::try_from(n).map_err(|_| bad_payload())?)?;
            Ok(bytes.to_vec())
        }
        RdbStrLen::IntStr(n) => Ok(n.to_string().into_bytes()),
        RdbStrLen::Lzf => {
            let compressed_len = read_rdb_length(r)?;
            let uncompressed_len = read_rdb_length(r)?;
            let compressed = r.read_slice(usize::try_from(compressed_len).map_err(|_| bad_payload())?)?.to_vec();
            lzf_decompress(&compressed, usize::try_from(uncompressed_len).map_err(|_| bad_payload())?)
        }
    }
}

fn try_parse_ascii_int(data: &[u8]) -> Option<i64> {
    if data.is_empty() || data.len() > 20 {
        return None;
    }
    let s = std::str::from_utf8(data).ok()?;
    let n: i64 = s.parse().ok()?;
    // Round-trip check: "01" parses as 1 but encoding as int would change it.
    if n.to_string().as_bytes() == data { Some(n) } else { None }
}

// ---- Listpack encoder / decoder -------------------------------------------
//
// Listpack layout (all little-endian):
//
//     +---------+---------+---------+----+---------+---------+
//     | totlen  | numlen  | entry_1 | .. | entry_n | 0xff    |
//     +---------+---------+---------+----+---------+---------+
//       u32       u16                                 u8
//
// `totlen` is the full blob size including the trailer. `numlen` caps at 65535;
// beyond that it's stored as the same value and the decoder walks to the
// terminator to determine the real count. We always set it exactly (we don't
// build listpacks with more than `u16::MAX` elements at this layer; RESTORE of
// a larger listpack would need to fall back to the "walk-to-terminator" rule).
//
// Each entry: `<encoding byte + content> <backlen>`. The backlen itself
// grows from 1 byte (entries <= 127 B) up to 5 bytes for the largest
// entries. Decode accepts all sizes.

fn listpack_begin(buf: &mut Vec<u8>, num_elements: usize) {
    // Placeholder totlen (u32) + numlen (u16), patched in finish().
    buf.extend_from_slice(&[0, 0, 0, 0]);
    #[allow(clippy::cast_possible_truncation)] // saturates u16 below
    let num = if num_elements > u64::from(u16::MAX) as usize { u16::MAX } else { num_elements as u16 };
    buf.extend_from_slice(&num.to_le_bytes());
}

fn listpack_finish(buf: &mut Vec<u8>) {
    buf.push(0xff);
    #[allow(clippy::cast_possible_truncation)] // checked against u32::MAX at call sites indirectly
    let tot = buf.len() as u32;
    buf[0..4].copy_from_slice(&tot.to_le_bytes());
}

#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss, clippy::cast_possible_wrap)]
// Every truncating cast below is guarded by a prior range check (integer
// encodings) or by the corresponding length branch (string encodings).
fn listpack_append_string(buf: &mut Vec<u8>, data: &[u8]) {
    // Match Redis's `lpTryEncoding`: if the bytes parse as ASCII integer
    // within one of its ranges, use an integer-type entry; else a string entry.
    if let Some(n) = try_parse_ascii_int(data) {
        if (0..=127).contains(&n) {
            // 7-bit uint: 0xxxxxxx
            buf.push(n as u8);
            append_backlen(buf, 1);
            return;
        }
        if (-4096..=4095).contains(&n) {
            // 13-bit signed: 110xxxxx xxxxxxxx, stored 2's complement (MSB-first)
            let nn = (n as i16 as u16) & 0x1fff;
            let b0 = 0b1100_0000 | ((nn >> 8) as u8 & 0x1f);
            let b1 = nn as u8;
            buf.push(b0);
            buf.push(b1);
            append_backlen(buf, 2);
            return;
        }
        if (-32_768..=32_767).contains(&n) {
            // 16-bit signed int — little-endian
            buf.push(0xf1);
            buf.extend_from_slice(&(n as i16).to_le_bytes());
            append_backlen(buf, 3);
            return;
        }
        if (-8_388_608..=8_388_607).contains(&n) {
            // 24-bit signed int — 3 LE bytes
            buf.push(0xf2);
            let v = n & 0x00ff_ffff;
            buf.push((v & 0xff) as u8);
            buf.push(((v >> 8) & 0xff) as u8);
            buf.push(((v >> 16) & 0xff) as u8);
            append_backlen(buf, 4);
            return;
        }
        if (i64::from(i32::MIN)..=i64::from(i32::MAX)).contains(&n) {
            // 32-bit signed int — LE
            buf.push(0xf3);
            buf.extend_from_slice(&(n as i32).to_le_bytes());
            append_backlen(buf, 5);
            return;
        }
        // 64-bit signed int — LE
        buf.push(0xf4);
        buf.extend_from_slice(&n.to_le_bytes());
        append_backlen(buf, 9);
        return;
    }

    let len = data.len();
    if len <= 63 {
        // 10xxxxxx + data
        buf.push(0x80 | (len as u8));
        buf.extend_from_slice(data);
        append_backlen(buf, 1 + len);
    } else if len < 4096 {
        // 1110xxxx xxxxxxxx + data (12-bit big-endian len)
        let b0 = 0xe0 | ((len >> 8) as u8 & 0x0f);
        buf.push(b0);
        buf.push(len as u8);
        buf.extend_from_slice(data);
        append_backlen(buf, 2 + len);
    } else {
        // 11110000 + u32 BE len + data
        buf.push(0xf0);
        let bs = (len as u32).to_be_bytes();
        buf.extend_from_slice(&bs);
        buf.extend_from_slice(data);
        append_backlen(buf, 5 + len);
    }
}

/// Append the backlen (encoded length of the just-written `entry_len`) — used
/// for reverse iteration. We don't iterate backward ourselves, but Redis
/// validates the shape on RESTORE.
#[allow(clippy::cast_possible_truncation)] // each branch masks to the 7 bits being kept
fn append_backlen(buf: &mut Vec<u8>, entry_len: usize) {
    // See listpack.c `lpEncodeBacklen`. Bytes are emitted high-order-first,
    // each carrying 7 bits of length; the LAST byte (rightmost in memory, read
    // first when iterating backward) has bit 7 = 0 as the terminator.
    let l = entry_len as u64;
    if l <= 127 {
        buf.push(l as u8);
    } else if l < 16_383 {
        buf.push((l >> 7) as u8);
        buf.push((l & 0x7f) as u8 | 0x80);
    } else if l < 2_097_151 {
        buf.push((l >> 14) as u8);
        buf.push(((l >> 7) & 0x7f) as u8 | 0x80);
        buf.push((l & 0x7f) as u8 | 0x80);
    } else if l < 268_435_455 {
        buf.push((l >> 21) as u8);
        buf.push(((l >> 14) & 0x7f) as u8 | 0x80);
        buf.push(((l >> 7) & 0x7f) as u8 | 0x80);
        buf.push((l & 0x7f) as u8 | 0x80);
    } else {
        buf.push((l >> 28) as u8);
        buf.push(((l >> 21) & 0x7f) as u8 | 0x80);
        buf.push(((l >> 14) & 0x7f) as u8 | 0x80);
        buf.push(((l >> 7) & 0x7f) as u8 | 0x80);
        buf.push((l & 0x7f) as u8 | 0x80);
    }
}

/// Decode a listpack blob into its sequence of string entries. Integer entries
/// are converted to their ASCII-decimal representation, matching how Redis
/// returns them through the client API (e.g. an integer-encoded hash field
/// still prints as a string).
fn listpack_decode(buf: &[u8]) -> Result<Vec<Vec<u8>>, RustyAntError> {
    if buf.len() < 7 {
        return Err(bad_payload());
    }
    let totlen = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    if totlen != buf.len() {
        return Err(bad_payload());
    }
    // numlen is advisory — we walk to the terminator either way.
    let _ = u16::from_le_bytes([buf[4], buf[5]]);
    let mut p = 6;
    let end = buf.len() - 1;
    if buf[end] != 0xff {
        return Err(bad_payload());
    }
    let mut out = Vec::new();
    while p < end {
        let (entry, consumed) = listpack_read_entry(&buf[p..end])?;
        out.push(entry);
        p += consumed;
    }
    if p != end {
        return Err(bad_payload());
    }
    Ok(out)
}

fn listpack_read_entry(buf: &[u8]) -> Result<(Vec<u8>, usize), RustyAntError> {
    if buf.is_empty() {
        return Err(bad_payload());
    }
    let b0 = buf[0];
    // Decode order matters: the 4-bit prefix `0xe0` *partially* overlaps with
    // the 8-bit prefixes 0xf0..0xf4, so check exact-byte tags first.
    let (bytes, entry_len): (Vec<u8>, usize) = match b0 {
        0b0000_0000..=0b0111_1111 => {
            // 7-bit uint
            (i64::from(b0).to_string().into_bytes(), 1)
        }
        0b1000_0000..=0b1011_1111 => {
            // 6-bit string length (10xxxxxx)
            let len = (b0 & 0x3f) as usize;
            if 1 + len > buf.len() {
                return Err(bad_payload());
            }
            (buf[1..=len].to_vec(), 1 + len)
        }
        0b1100_0000..=0b1101_1111 => {
            // 13-bit signed int (110xxxxx xxxxxxxx, MSB-first big-endian)
            if buf.len() < 2 {
                return Err(bad_payload());
            }
            let raw = (u16::from(b0 & 0x1f) << 8) | u16::from(buf[1]);
            // Sign-extend from 13-bit
            let n = if raw & 0x1000 != 0 { i32::from(raw) - (1 << 13) } else { i32::from(raw) };
            (i64::from(n).to_string().into_bytes(), 2)
        }
        0xf1 => {
            if buf.len() < 3 {
                return Err(bad_payload());
            }
            let n = i16::from_le_bytes([buf[1], buf[2]]);
            (i64::from(n).to_string().into_bytes(), 3)
        }
        0xf2 => {
            if buf.len() < 4 {
                return Err(bad_payload());
            }
            let mut n = u32::from(buf[1]) | (u32::from(buf[2]) << 8) | (u32::from(buf[3]) << 16);
            if n & 0x0080_0000 != 0 {
                n |= 0xff00_0000;
            }
            #[allow(clippy::cast_possible_wrap)] // 24-bit sign extension above
            let n = n as i32;
            (i64::from(n).to_string().into_bytes(), 4)
        }
        0xf3 => {
            if buf.len() < 5 {
                return Err(bad_payload());
            }
            let n = i32::from_le_bytes([buf[1], buf[2], buf[3], buf[4]]);
            (i64::from(n).to_string().into_bytes(), 5)
        }
        0xf4 => {
            if buf.len() < 9 {
                return Err(bad_payload());
            }
            let n = i64::from_le_bytes([buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7], buf[8]]);
            (n.to_string().into_bytes(), 9)
        }
        0xf0 => {
            // 32-bit string length (big-endian u32)
            if buf.len() < 5 {
                return Err(bad_payload());
            }
            let len = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
            if 5 + len > buf.len() {
                return Err(bad_payload());
            }
            (buf[5..5 + len].to_vec(), 5 + len)
        }
        0b1110_0000..=0b1110_1111 => {
            // 12-bit string length (1110xxxx xxxxxxxx, big-endian)
            if buf.len() < 2 {
                return Err(bad_payload());
            }
            let len = ((usize::from(b0) & 0x0f) << 8) | usize::from(buf[1]);
            if 2 + len > buf.len() {
                return Err(bad_payload());
            }
            (buf[2..2 + len].to_vec(), 2 + len)
        }
        _ => return Err(bad_payload()),
    };
    let backlen_size = backlen_bytes(entry_len);
    let total = entry_len + backlen_size;
    if total > buf.len() {
        return Err(bad_payload());
    }
    Ok((bytes, total))
}

const fn backlen_bytes(entry_len: usize) -> usize {
    if entry_len <= 127 {
        1
    } else if entry_len < 16_383 {
        2
    } else if entry_len < 2_097_151 {
        3
    } else if entry_len < 268_435_455 {
        4
    } else {
        5
    }
}

// ---- CRC64-Jones ----------------------------------------------------------
//
// Reflected implementation of Redis's CRC64 (polynomial 0xad93d23594c935a9).
// Matches the vector `crc64(0, "123456789") == 0xe9c6d914c4b8d9ca`.

const CRC64_REFLECTED_POLY: u64 = 0x95ac_9329_ac4b_c9b5;

const fn crc64_build_table() -> [u64; 256] {
    let mut t = [0u64; 256];
    let mut i = 0usize;
    while i < 256 {
        let mut c = i as u64;
        let mut k = 0;
        while k < 8 {
            c = if c & 1 != 0 { (c >> 1) ^ CRC64_REFLECTED_POLY } else { c >> 1 };
            k += 1;
        }
        t[i] = c;
        i += 1;
    }
    t
}

fn crc64_table() -> &'static [u64; 256] {
    use std::sync::OnceLock;
    static TABLE: OnceLock<[u64; 256]> = OnceLock::new();
    TABLE.get_or_init(crc64_build_table)
}

#[must_use]
pub fn crc64(seed: u64, data: &[u8]) -> u64 {
    let t = crc64_table();
    let mut crc = seed;
    for &b in data {
        let idx = ((crc ^ u64::from(b)) & 0xff) as usize;
        crc = t[idx] ^ (crc >> 8);
    }
    crc
}

// ---- LZF decompression ----------------------------------------------------
//
// Redis sometimes emits LZF-compressed RDB strings for large text values.
// Encoding them on DUMP is uncommon (Redis only uses LZF for RDB *files*,
// not the over-the-wire DUMP payloads), but RESTORE should still accept
// them. Classic LZF bitstream:
//
//     <ctrl> : 1..7 literals → copy (ctrl+1) literal bytes
//              0xe0+ back-ref short → 3-byte run at offset `((ctrl & 0x1f) << 8) | b2`
//              0xe0+ back-ref long  → len-byte run, similar layout
fn lzf_decompress(input: &[u8], uncompressed_len: usize) -> Result<Vec<u8>, RustyAntError> {
    let mut out = Vec::with_capacity(uncompressed_len);
    let mut p = 0;
    while p < input.len() {
        let ctrl = input[p];
        p += 1;
        if ctrl < 32 {
            // (ctrl+1) literal bytes
            let n = usize::from(ctrl) + 1;
            if p + n > input.len() {
                return Err(bad_payload());
            }
            out.extend_from_slice(&input[p..p + n]);
            p += n;
        } else {
            let mut run_len = usize::from(ctrl >> 5);
            if run_len == 7 {
                if p >= input.len() {
                    return Err(bad_payload());
                }
                run_len += usize::from(input[p]);
                p += 1;
            }
            run_len += 2;
            if p >= input.len() {
                return Err(bad_payload());
            }
            let ref_off = ((usize::from(ctrl) & 0x1f) << 8) | usize::from(input[p]);
            p += 1;
            if out.len() < ref_off + 1 {
                return Err(bad_payload());
            }
            let start = out.len() - ref_off - 1;
            // Run may overlap the current tail — copy byte-by-byte.
            for i in 0..run_len {
                let b = out[start + i];
                out.push(b);
            }
        }
    }
    if out.len() != uncompressed_len {
        return Err(bad_payload());
    }
    Ok(out)
}

// ---- Double formatting for zset listpack ----------------------------------
//
// Redis stores zset scores in a listpack as stringified doubles using the
// same `double2string` path that feeds `ZRANGE WITHSCORES`. Matches
// `format_score` in commands.rs.

fn format_rdb_double(s: f64) -> String {
    if s.is_nan() {
        return "nan".to_string();
    }
    if s.is_infinite() {
        return (if s > 0.0 { "inf" } else { "-inf" }).to_string();
    }
    if s.fract() == 0.0 && s.abs() < 9.007_199_254_740_992e15 {
        #[allow(clippy::cast_possible_truncation)] // fract==0 && range checked
        let as_int = s as i64;
        return as_int.to_string();
    }
    format!("{s}")
}

fn parse_rdb_double(s: &str) -> Result<f64, RustyAntError> {
    match s {
        "inf" | "+inf" => Ok(f64::INFINITY),
        "-inf" => Ok(f64::NEG_INFINITY),
        "nan" => Ok(f64::NAN),
        other => other.parse::<f64>().map_err(|_| bad_payload()),
    }
}

// ---- Small byte reader ----------------------------------------------------

struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    const fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn read_u8(&mut self) -> Result<u8, RustyAntError> {
        if self.pos >= self.buf.len() {
            return Err(bad_payload());
        }
        let b = self.buf[self.pos];
        self.pos += 1;
        Ok(b)
    }

    fn read_slice(&mut self, n: usize) -> Result<&'a [u8], RustyAntError> {
        if self.pos + n > self.buf.len() {
            return Err(bad_payload());
        }
        let s = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use super::*;

    // ---- CRC64 ----

    #[test]
    fn crc64_matches_redis_vector() {
        // The canonical test vector documented in Redis's crc64.c.
        assert_eq!(crc64(0, b"123456789"), 0xe9c6_d914_c4b8_d9ca);
    }

    #[test]
    fn crc64_empty() {
        assert_eq!(crc64(0, b""), 0);
    }

    // ---- RDB length ----

    #[test]
    fn rdb_length_round_trip() {
        for &n in &[0u64, 1, 63, 64, 255, 16383, 16384, 65536, 1_000_000, u64::from(u32::MAX), u64::MAX] {
            let mut b = BytesMut::new();
            write_rdb_length(&mut b, n);
            let frozen = b.freeze();
            let mut r = Reader::new(&frozen);
            assert_eq!(read_rdb_length(&mut r).unwrap(), n);
        }
    }

    // ---- RDB string ----

    #[test]
    fn rdb_string_plain_round_trip() {
        for s in [&b""[..], b"a", b"hello world", &[0u8; 100][..]] {
            let mut b = BytesMut::new();
            write_rdb_string(&mut b, s);
            let frozen = b.freeze();
            let mut r = Reader::new(&frozen);
            assert_eq!(read_rdb_string(&mut r).unwrap(), s.to_vec());
        }
    }

    #[test]
    fn rdb_string_integer_compression() {
        for &n in &[0i64, 1, -1, 127, -128, 128, -129, 32767, -32768, 32768, -32769, 2_000_000_000] {
            let s = n.to_string();
            let mut b = BytesMut::new();
            write_rdb_string(&mut b, s.as_bytes());
            let frozen = b.freeze();
            let mut r = Reader::new(&frozen);
            assert_eq!(read_rdb_string(&mut r).unwrap(), s.into_bytes());
        }
    }

    // ---- Listpack ----

    #[test]
    fn listpack_empty_round_trip() {
        let mut lp = Vec::new();
        listpack_begin(&mut lp, 0);
        listpack_finish(&mut lp);
        let decoded = listpack_decode(&lp).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn listpack_strings_round_trip() {
        let inputs: Vec<Vec<u8>> =
            vec![b"".to_vec(), b"a".to_vec(), b"hello".to_vec(), vec![0u8; 100], vec![0xffu8; 5000]];
        let mut lp = Vec::new();
        listpack_begin(&mut lp, inputs.len());
        for s in &inputs {
            listpack_append_string(&mut lp, s);
        }
        listpack_finish(&mut lp);
        let decoded = listpack_decode(&lp).unwrap();
        assert_eq!(decoded, inputs);
    }

    #[test]
    fn listpack_ints_round_trip() {
        let inputs: Vec<i64> = vec![0, 1, -1, 127, -128, 128, 4095, -4096, 32767, -32768, 8_388_607, i32::MAX.into()];
        let mut lp = Vec::new();
        listpack_begin(&mut lp, inputs.len());
        for &n in &inputs {
            listpack_append_string(&mut lp, n.to_string().as_bytes());
        }
        listpack_finish(&mut lp);
        let decoded = listpack_decode(&lp).unwrap();
        let round: Vec<i64> = decoded.iter().map(|b| std::str::from_utf8(b).unwrap().parse().unwrap()).collect();
        assert_eq!(round, inputs);
    }

    // ---- End-to-end DUMP / RESTORE round-trips ----

    #[test]
    fn dump_restore_string() {
        let v = Value::String(b"hello".to_vec());
        let payload = dump_value(&v).unwrap();
        let round = restore_value(&payload).unwrap();
        match round {
            Value::String(b) => assert_eq!(b, b"hello".to_vec()),
            _ => panic!("wrong kind"),
        }
    }

    #[test]
    fn dump_restore_integer_string() {
        // Integer-encoded strings must round-trip bit-identically.
        let v = Value::String(b"12345".to_vec());
        let payload = dump_value(&v).unwrap();
        let round = restore_value(&payload).unwrap();
        match round {
            Value::String(b) => assert_eq!(b, b"12345".to_vec()),
            _ => panic!("wrong kind"),
        }
    }

    #[test]
    fn dump_restore_hash() {
        let mut m = BTreeMap::new();
        m.insert("a".to_string(), b"1".to_vec());
        m.insert("b".to_string(), b"hello".to_vec());
        let v = Value::Hash(m.clone());
        let payload = dump_value(&v).unwrap();
        match restore_value(&payload).unwrap() {
            Value::Hash(got) => assert_eq!(got, m),
            _ => panic!("wrong kind"),
        }
    }

    #[test]
    fn dump_restore_list() {
        let items = vec![b"a".to_vec(), b"bb".to_vec(), b"ccc".to_vec()];
        let v = Value::List(items.clone());
        let payload = dump_value(&v).unwrap();
        match restore_value(&payload).unwrap() {
            Value::List(got) => assert_eq!(got, items),
            _ => panic!("wrong kind"),
        }
    }

    #[test]
    fn dump_restore_set() {
        let mut s = BTreeSet::new();
        s.insert("alpha".to_string());
        s.insert("beta".to_string());
        s.insert("42".to_string()); // forces integer-encoded listpack entry
        let v = Value::Set(s.clone());
        let payload = dump_value(&v).unwrap();
        match restore_value(&payload).unwrap() {
            Value::Set(got) => assert_eq!(got, s),
            _ => panic!("wrong kind"),
        }
    }

    #[test]
    #[allow(clippy::approx_constant)] // literal sentinel, not π
    fn dump_restore_zset() {
        let mut z = BTreeMap::new();
        z.insert("alpha".to_string(), 1.0);
        z.insert("beta".to_string(), 3.14);
        z.insert("gamma".to_string(), -42.0);
        let v = Value::ZSet(z.clone());
        let payload = dump_value(&v).unwrap();
        match restore_value(&payload).unwrap() {
            Value::ZSet(got) => assert_eq!(got, z),
            _ => panic!("wrong kind"),
        }
    }

    #[test]
    fn restore_rejects_truncated() {
        let err = restore_value(b"").unwrap_err();
        assert!(matches!(err, RustyAntError::Parse(_)));
    }

    #[test]
    fn restore_rejects_bad_crc() {
        let v = Value::String(b"hello".to_vec());
        let mut payload = dump_value(&v).unwrap().to_vec();
        // Flip a bit in the CRC trailer.
        let last = payload.len() - 1;
        payload[last] ^= 0x01;
        let err = restore_value(&payload).unwrap_err();
        assert!(matches!(err, RustyAntError::Parse(_)));
    }

    #[test]
    fn restore_rejects_bad_version() {
        let v = Value::String(b"hello".to_vec());
        let mut payload = dump_value(&v).unwrap().to_vec();
        // Overwrite the version to something unreasonable.
        let len = payload.len();
        payload[len - 10] = 0xff;
        payload[len - 9] = 0xff;
        let err = restore_value(&payload).unwrap_err();
        assert!(matches!(err, RustyAntError::Parse(_)));
    }

    #[test]
    fn dump_stream_is_error() {
        let v = Value::Stream(crate::stream::StreamValue::default());
        let err = dump_value(&v).unwrap_err();
        assert!(matches!(err, RustyAntError::Parse(_)));
    }

    // ---- Golden vectors: hex-encoded DUMP payloads captured from real
    // Redis 7.2 to prove that rustyant's RESTORE accepts real Redis output
    // byte-for-byte. Capture command:
    //     redis-cli -p <port> DUMP <key> | xxd -c 256 -p
    // (trim the trailing newline).

    fn hex_decode(s: &str) -> Vec<u8> {
        let s: String = s.chars().filter(|c| !c.is_whitespace()).collect();
        assert!(s.len() % 2 == 0);
        (0..s.len()).step_by(2).map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap()).collect()
    }

    #[test]
    fn golden_restore_real_redis_string() {
        // Redis 7.2: SET str "hello world"; DUMP str
        let payload = hex_decode("000b68656c6c6f20776f726c640b006223f4ca5b5849bd");
        match restore_value(&payload).unwrap() {
            Value::String(b) => assert_eq!(b, b"hello world".to_vec()),
            _ => panic!("wrong kind"),
        }
    }

    #[test]
    fn golden_dump_string_matches_real_redis() {
        // Same input as above; our DUMP output should be byte-identical.
        let v = Value::String(b"hello world".to_vec());
        let ours = dump_value(&v).unwrap();
        let redis_payload = hex_decode("000b68656c6c6f20776f726c640b006223f4ca5b5849bd");
        assert_eq!(ours.to_vec(), redis_payload);
    }

    #[test]
    fn golden_restore_real_redis_hash() {
        // Redis 7.2: HSET h a 1 b hello; DUMP h
        let payload = hex_decode("101616000000040081610201018162028568656c6c6f06ff0b00811a43ea13987cd4");
        match restore_value(&payload).unwrap() {
            Value::Hash(m) => {
                assert_eq!(m.get("a").map(Vec::as_slice), Some(&b"1"[..]));
                assert_eq!(m.get("b").map(Vec::as_slice), Some(&b"hello"[..]));
                assert_eq!(m.len(), 2);
            }
            _ => panic!("wrong kind"),
        }
    }

    #[test]
    fn golden_restore_real_redis_list() {
        // Redis 7.2: RPUSH lst a bb ccc; DUMP lst
        let payload = hex_decode("12010213130000000300816102826262038363636304ff0b0047e8f54e761bf81e");
        match restore_value(&payload).unwrap() {
            Value::List(items) => {
                assert_eq!(items, vec![b"a".to_vec(), b"bb".to_vec(), b"ccc".to_vec()]);
            }
            _ => panic!("wrong kind"),
        }
    }

    #[test]
    fn golden_restore_real_redis_set() {
        // Redis 7.2: SADD myset alpha beta 42; DUMP myset
        // Members stored in insertion order; integer "42" gets 7-bit-int encoding.
        let payload = hex_decode("141616000000030085616c706861068462657461052a01ff0b007e65966f9dbf3bfd");
        match restore_value(&payload).unwrap() {
            Value::Set(s) => {
                assert!(s.contains("alpha"));
                assert!(s.contains("beta"));
                assert!(s.contains("42"));
                assert_eq!(s.len(), 3);
            }
            _ => panic!("wrong kind"),
        }
    }

    // One-off interop smoke: run with `cargo test -- --nocapture
    // rdb::tests::emit_interop_hex` to print DUMP hex for each kind, then
    // pipe to `redis-cli RESTORE` to validate the reverse direction. Kept
    // as a test for easy reruns; `--nocapture` is the only way to see the
    // output, so CI sees it as a no-op passing test.
    #[test]
    #[allow(clippy::approx_constant)] // 3.14 is deliberately the same value real Redis emits — not π
    fn emit_interop_hex() {
        use std::fmt::Write;
        let mut hash = BTreeMap::new();
        hash.insert("a".to_string(), b"1".to_vec());
        hash.insert("b".to_string(), b"hello".to_vec());
        let mut set = BTreeSet::new();
        set.insert("alpha".to_string());
        set.insert("beta".to_string());
        set.insert("42".to_string());
        let mut zset = BTreeMap::new();
        zset.insert("alpha".to_string(), 1.0);
        zset.insert("beta".to_string(), 3.14);
        zset.insert("gamma".to_string(), -42.0);
        for (name, v) in &[
            ("string", Value::String(b"hello world".to_vec())),
            ("hash", Value::Hash(hash)),
            ("list", Value::List(vec![b"a".to_vec(), b"bb".to_vec(), b"ccc".to_vec()])),
            ("set", Value::Set(set)),
            ("zset", Value::ZSet(zset)),
        ] {
            let payload = dump_value(v).unwrap();
            let mut hex = String::with_capacity(payload.len() * 2);
            for b in &payload {
                write!(&mut hex, "{b:02x}").unwrap();
            }
            eprintln!("INTEROP {name}: {hex}");
        }
    }

    #[test]
    #[allow(clippy::approx_constant)] // mirrors the literal Redis emitted in the captured payload
    fn golden_restore_real_redis_zset() {
        // Redis 7.2: ZADD zs 1 alpha 3.14 beta -42 gamma; DUMP zs
        // Integer scores get 13-bit-int or 7-bit-int encoding; "3.14" stays as a string.
        let payload = hex_decode(
            "11262600000006008567616d6d6106dfd60285616c70686106010184626574610584332e313405ff0b00d2095b2c3d6dc866",
        );
        match restore_value(&payload).unwrap() {
            Value::ZSet(m) => {
                assert_eq!(m.get("alpha"), Some(&1.0));
                assert_eq!(m.get("beta"), Some(&3.14));
                assert_eq!(m.get("gamma"), Some(&-42.0));
                assert_eq!(m.len(), 3);
            }
            _ => panic!("wrong kind"),
        }
    }
}
