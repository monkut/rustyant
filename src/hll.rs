//! `HyperLogLog` — Redis-compatible dense encoding.
//!
//! Uses 16384 6-bit registers (14-bit bucket index), matching Redis's
//! default precision. Values are stored as regular `string` keys with an
//! HLL-specific 16-byte header (`HYLL` magic), so plain `GET` / `STRLEN`
//! round-trip them transparently.
//!
//! The binary layout exactly matches `src/hyperloglog.c` in Redis — a
//! blob produced here can be restored into a real Redis server (and vice
//! versa). Only dense encoding is implemented; sparse is a later size
//! optimisation.
//!
//! Cardinality estimation follows the classic `HyperLogLog` paper (Flajolet
//! et al. 2007) with linear-counting correction for low-cardinality ranges
//! and the 2^32 saturation correction at the top. Redis ships a larger
//! HLL++ bias-correction table; on rustyant's scale (per-key analytics)
//! the classic estimator is well within the documented 0.81% error at
//! `m = 16384`.

use crate::error::RustyAntError;

/// Precision: 14 bits of hash → 16384 registers. Matches Redis.
pub const HLL_P: u32 = 14;
pub const HLL_REGISTERS: usize = 1 << HLL_P; // 16384
pub const HLL_REGISTER_BITS: u32 = 6;
const HLL_REGISTERS_BYTES: usize = (HLL_REGISTERS * HLL_REGISTER_BITS as usize).div_ceil(8);
pub const HLL_HEADER_BYTES: usize = 16;
pub const HLL_DENSE_BYTES: usize = HLL_HEADER_BYTES + HLL_REGISTERS_BYTES;

const HLL_MAGIC: &[u8; 4] = b"HYLL";
const HLL_DENSE: u8 = 0;

const MURMUR_SEED: u64 = 0xadc8_3b19;

/// True when `data` looks like a Redis HLL — checks magic + length.
pub fn is_hll(data: &[u8]) -> bool {
    data.len() == HLL_DENSE_BYTES && &data[0..4] == HLL_MAGIC && data[4] == HLL_DENSE
}

/// Construct an empty dense HLL blob (all registers zero, cached
/// cardinality invalid).
pub fn empty_dense() -> Vec<u8> {
    let mut buf = vec![0u8; HLL_DENSE_BYTES];
    buf[0..4].copy_from_slice(HLL_MAGIC);
    buf[4] = HLL_DENSE;
    // bytes 5..8 reserved (0). bytes 8..16: cached cardinality, all zero.
    buf
}

/// Hash an element (`MurmurHash2-64A`) and split it into a `(bucket, zeros+1)`
/// pair as the HLL algorithm expects: the low `HLL_P` bits are the bucket
/// index; the count of leading zeros of the remaining bits (+ 1, capped)
/// is the value to write into that register.
fn hash_element(element: &[u8]) -> (usize, u8) {
    let h = murmur64a(element, MURMUR_SEED);
    // `& (HLL_REGISTERS - 1)` keeps only the low 14 bits, which always fit
    // in usize on every platform we target.
    #[allow(clippy::cast_possible_truncation)]
    let bucket = (h as usize) & (HLL_REGISTERS - 1);
    // Shift out the bucket bits; the remaining 50 bits hold leading zeros.
    let rest = h >> HLL_P;
    // `count + 1`: the stored register value is position of the first 1-bit
    // in the remaining 50 bits (1-indexed). We cap at 50 (+1 for the one
    // bit itself) = max register value 63 (fits in 6 bits).
    #[allow(clippy::cast_possible_truncation)] // 64 - HLL_P = 50 fits in u8
    let max = (64 - HLL_P) as u8;
    #[allow(clippy::cast_possible_truncation)] // trailing_zeros ≤ 50 fits in u8
    let count = if rest == 0 { max + 1 } else { (rest.trailing_zeros() as u8) + 1 };
    (bucket, count.min(max + 1))
}

/// Read the 6-bit register at `idx` (0..16384) from the packed dense body.
fn read_register(registers: &[u8], idx: usize) -> u8 {
    let bit = idx * HLL_REGISTER_BITS as usize;
    let byte = bit / 8;
    let shift = bit % 8;
    // A 6-bit register straddles at most two bytes.
    let lo = u32::from(registers[byte]);
    let hi = u32::from(*registers.get(byte + 1).unwrap_or(&0));
    let combined = lo | (hi << 8);
    ((combined >> shift) & 0x3f) as u8
}

/// Write the 6-bit register at `idx` to `value` (must be 0..=63).
fn write_register(registers: &mut [u8], idx: usize, value: u8) {
    let bit = idx * HLL_REGISTER_BITS as usize;
    let byte = bit / 8;
    let shift = bit % 8;
    let mask = 0x3fu32 << shift;
    let val = u32::from(value & 0x3f) << shift;
    let mut combined = u32::from(registers[byte]) | (u32::from(registers.get(byte + 1).copied().unwrap_or(0)) << 8);
    combined = (combined & !mask) | val;
    registers[byte] = (combined & 0xff) as u8;
    if byte + 1 < registers.len() {
        registers[byte + 1] = ((combined >> 8) & 0xff) as u8;
    }
}

/// Invalidate the cached cardinality header by flipping its top bit, which
/// forces the next `PFCOUNT` to recompute.
fn invalidate_cache(buf: &mut [u8]) {
    buf[15] |= 0x80;
}

/// Add `element` to the HLL; returns `true` if a register was updated.
///
/// Callers are responsible for providing a valid dense-encoded buffer (the
/// wrapper in commands.rs validates before calling).
pub fn add(buf: &mut [u8], element: &[u8]) -> Result<bool, RustyAntError> {
    if !is_hll(buf) {
        return Err(RustyAntError::Parse("key is not a valid HyperLogLog string".into()));
    }
    let (bucket, value) = hash_element(element);
    let registers = &mut buf[HLL_HEADER_BYTES..];
    let current = read_register(registers, bucket);
    if value > current {
        write_register(registers, bucket, value);
        invalidate_cache(buf);
        Ok(true)
    } else {
        Ok(false)
    }
}

/// Return the cardinality estimate for a single HLL.
pub fn count(buf: &[u8]) -> Result<u64, RustyAntError> {
    if !is_hll(buf) {
        return Err(RustyAntError::Parse("key is not a valid HyperLogLog string".into()));
    }
    Ok(estimate_from_registers(&buf[HLL_HEADER_BYTES..]))
}

/// Merge `src` registers into `dest` in-place (register-wise max). Both
/// must already be dense-encoded HLL blobs.
pub fn merge_into(dest: &mut [u8], src: &[u8]) -> Result<(), RustyAntError> {
    if !is_hll(dest) || !is_hll(src) {
        return Err(RustyAntError::Parse("key is not a valid HyperLogLog string".into()));
    }
    let mut changed = false;
    for i in 0..HLL_REGISTERS {
        let s = read_register(&src[HLL_HEADER_BYTES..], i);
        let d = read_register(&dest[HLL_HEADER_BYTES..], i);
        if s > d {
            let registers = &mut dest[HLL_HEADER_BYTES..];
            write_register(registers, i, s);
            changed = true;
        }
    }
    if changed {
        invalidate_cache(dest);
    }
    Ok(())
}

/// Classic `HyperLogLog` estimator with linear-counting correction for the
/// low-cardinality regime and 2^32 saturation correction at the top.
#[allow(clippy::cast_precision_loss)] // HLL_REGISTERS (16384) and zeros (≤ 16384) fit in f64 mantissa
fn estimate_from_registers(registers: &[u8]) -> u64 {
    let m = HLL_REGISTERS as f64;
    // α_m constant for m = 16384 (from the HLL paper).
    let alpha = 0.7213 / (1.0 + 1.079 / m);

    let mut sum = 0.0_f64;
    let mut zeros: u64 = 0;
    for i in 0..HLL_REGISTERS {
        let r = read_register(registers, i);
        if r == 0 {
            zeros += 1;
        }
        // 2^{-r}
        sum += 2.0_f64.powi(-i32::from(r));
    }

    let raw = alpha * m * m / sum;
    let two32 = f64::from(u32::MAX) + 1.0; // 2^32

    // Low-cardinality regime: linear counting based on the proportion of
    // zero registers (more accurate than raw HLL when many buckets are
    // still unset).
    let e = if raw <= 2.5 * m && zeros > 0 {
        let v = zeros as f64;
        m * (m / v).ln()
    } else if raw > two32 / 30.0 {
        // Saturation regime — unlikely to hit since Redis-scale counts
        // rarely exceed 10^9, but matches the canonical formula.
        -two32 * (1.0 - raw / two32).ln()
    } else {
        raw
    };

    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let n = e.round() as u64;
    n
}

// ---------------------------------------------------------------------------
// MurmurHash64A — Redis uses this for HLL with a fixed seed.
// ---------------------------------------------------------------------------

fn murmur64a(data: &[u8], seed: u64) -> u64 {
    const M: u64 = 0xc6a4_a793_5bd1_e995;
    const R: u32 = 47;

    #[allow(clippy::cast_possible_truncation)]
    let mut h = seed ^ ((data.len() as u64).wrapping_mul(M));

    let chunks = data.chunks_exact(8);
    let tail = chunks.remainder();
    for chunk in chunks {
        let mut k =
            u64::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3], chunk[4], chunk[5], chunk[6], chunk[7]]);
        k = k.wrapping_mul(M);
        k ^= k >> R;
        k = k.wrapping_mul(M);
        h ^= k;
        h = h.wrapping_mul(M);
    }

    // Tail: up to 7 straggler bytes, mixed most-significant first.
    for (i, &b) in tail.iter().enumerate().rev() {
        h ^= u64::from(b) << (i * 8);
        if i == 0 {
            h = h.wrapping_mul(M);
        }
    }

    h ^= h >> R;
    h = h.wrapping_mul(M);
    h ^= h >> R;
    h
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_is_valid_hll() {
        let buf = empty_dense();
        assert_eq!(buf.len(), HLL_DENSE_BYTES);
        assert!(is_hll(&buf));
        assert_eq!(count(&buf).expect("count"), 0);
    }

    #[test]
    fn single_add_updates_register() {
        let mut buf = empty_dense();
        let changed = add(&mut buf, b"hello").expect("add");
        assert!(changed);
        // Counting with only one element should report 1 (linear counting
        // regime — exact for tiny cardinalities).
        assert_eq!(count(&buf).expect("count"), 1);
    }

    #[test]
    fn duplicate_add_is_stable() {
        let mut buf = empty_dense();
        add(&mut buf, b"hello").expect("add");
        let changed_again = add(&mut buf, b"hello").expect("add");
        assert!(!changed_again, "repeat add should not change any register");
        assert_eq!(count(&buf).expect("count"), 1);
    }

    #[test]
    #[allow(clippy::cast_precision_loss)]
    fn cardinality_estimates_within_two_percent() {
        let mut buf = empty_dense();
        for i in 0..10_000u32 {
            add(&mut buf, format!("elem-{i}").as_bytes()).expect("add");
        }
        let est = count(&buf).expect("count") as f64;
        let err = (est - 10_000.0).abs() / 10_000.0;
        assert!(err < 0.02, "HLL estimate off by {err:.4} (got {est})");
    }

    #[test]
    #[allow(clippy::cast_precision_loss)]
    fn merge_takes_register_max() {
        let mut a = empty_dense();
        let mut b = empty_dense();
        for i in 0..500u32 {
            add(&mut a, format!("a-{i}").as_bytes()).expect("add");
            add(&mut b, format!("b-{i}").as_bytes()).expect("add");
        }
        // Shared prefix — same element lands in both, so union cardinality
        // is close to 500 + 500 = 1000.
        let mut merged = a.clone();
        merge_into(&mut merged, &b).expect("merge");
        let est = count(&merged).expect("count") as f64;
        let err = (est - 1000.0).abs() / 1000.0;
        assert!(err < 0.05, "merged HLL estimate off by {err:.4} (got {est})");
    }

    #[test]
    fn is_hll_rejects_arbitrary_strings() {
        assert!(!is_hll(b"HYLL short"));
        assert!(!is_hll(b""));
        let mut buf = empty_dense();
        buf[0] = b'X';
        assert!(!is_hll(&buf));
    }
}
