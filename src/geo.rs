//! Geohash encoding / decoding and geographic distance math.
//!
//! The on-disk representation of a geo member is a ZSET entry whose score is
//! a 52-bit interleaved geohash integer stored as an `f64` (the mantissa has
//! 52 bits, so 52-bit integers roundtrip exactly). Longitude is encoded over
//! [-180, 180] and latitude over Redis's Mercator-clamped [-85.05112878,
//! 85.05112878], 26 bits each — matching Redis so external tooling that
//! speaks the standard geohash wire format can interoperate.
//!
//! The `GEOHASH` command reply uses the standard geohash alphabet and the
//! standard latitude range [-90, 90], so `geohash_string` re-encodes with
//! standard bounds before base32ing.
//!
//! Haversine distance uses the same Earth radius Redis does
//! (`6_372_797.560856` metres).

use crate::error::RustyAntError;

pub const LON_MIN: f64 = -180.0;
pub const LON_MAX: f64 = 180.0;
// Redis clamps latitude to a Mercator-safe band rather than the full
// [-90, 90] — matches `GEO_LAT_MIN` / `GEO_LAT_MAX` in Redis's geohash.h.
pub const LAT_MIN: f64 = -85.051_128_78;
pub const LAT_MAX: f64 = 85.051_128_78;

const STD_LAT_MIN: f64 = -90.0;
const STD_LAT_MAX: f64 = 90.0;

const GEO_BITS: u32 = 26;
const EARTH_RADIUS_M: f64 = 6_372_797.560_856;

const BASE32_ALPHABET: &[u8; 32] = b"0123456789bcdefghjkmnpqrstuvwxyz";

/// Units accepted by `GEODIST` / `GEOSEARCH`.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum GeoUnit {
    Meters,
    Kilometers,
    Miles,
    Feet,
}

impl GeoUnit {
    pub fn parse(s: &str) -> Result<Self, RustyAntError> {
        match s.to_ascii_lowercase().as_str() {
            "m" => Ok(Self::Meters),
            "km" => Ok(Self::Kilometers),
            "mi" => Ok(Self::Miles),
            "ft" => Ok(Self::Feet),
            other => Err(RustyAntError::Parse(format!("unsupported unit of distance: {other}"))),
        }
    }

    /// Metres per one unit.
    pub const fn to_meters(self) -> f64 {
        match self {
            Self::Meters => 1.0,
            Self::Kilometers => 1000.0,
            Self::Miles => 1609.344,
            Self::Feet => 0.3048,
        }
    }
}

/// Validate a `(longitude, latitude)` pair against Redis's geo bounds.
pub fn validate_lon_lat(lon: f64, lat: f64) -> Result<(), RustyAntError> {
    if !(LON_MIN..=LON_MAX).contains(&lon) || !(LAT_MIN..=LAT_MAX).contains(&lat) {
        return Err(RustyAntError::Parse(format!("invalid longitude,latitude pair {lon:.6},{lat:.6}")));
    }
    Ok(())
}

fn spread_bits(v: u32) -> u64 {
    let mut x = u64::from(v) & 0x0000_0000_03ff_ffff;
    x = (x | (x << 16)) & 0x0000_ffff_0000_ffff;
    x = (x | (x << 8)) & 0x00ff_00ff_00ff_00ff;
    x = (x | (x << 4)) & 0x0f0f_0f0f_0f0f_0f0f;
    x = (x | (x << 2)) & 0x3333_3333_3333_3333;
    x = (x | (x << 1)) & 0x5555_5555_5555_5555;
    x
}

fn gather_bits(v: u64) -> u32 {
    let mut x = v & 0x5555_5555_5555_5555;
    x = (x | (x >> 1)) & 0x3333_3333_3333_3333;
    x = (x | (x >> 2)) & 0x0f0f_0f0f_0f0f_0f0f;
    x = (x | (x >> 4)) & 0x00ff_00ff_00ff_00ff;
    x = (x | (x >> 8)) & 0x0000_ffff_0000_ffff;
    x = (x | (x >> 16)) & 0x0000_0000_ffff_ffff;
    u32::try_from(x & 0xffff_ffff).unwrap_or(0)
}

fn quantize(value: f64, min: f64, max: f64, bits: u32) -> u32 {
    // Fraction in [0, 1); cell count = 2^bits. Clamp to the last cell on
    // the upper boundary so the `max` input doesn't land on `2^bits`.
    let frac = (value - min) / (max - min);
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let cell = (frac * f64::from(1u32 << bits)) as u32;
    cell.min((1u32 << bits) - 1)
}

fn dequantize(cell: u32, min: f64, max: f64, bits: u32) -> f64 {
    // Return the centre of the cell — matches Redis's decode behaviour,
    // which averages the cell bounds.
    let span = max - min;
    let step = span / f64::from(1u32 << bits);
    (f64::from(cell) + 0.5).mul_add(step, min)
}

fn encode_interleaved(lon: f64, lat: f64, lon_min: f64, lon_max: f64, lat_min: f64, lat_max: f64) -> u64 {
    let lon_cell = quantize(lon, lon_min, lon_max, GEO_BITS);
    let lat_cell = quantize(lat, lat_min, lat_max, GEO_BITS);
    // Latitude on even bit positions (0, 2, 4, ...), longitude on odd —
    // so the most-significant bit of the 52-bit score is longitude's MSB,
    // matching Redis's wire format so external geo tooling can interop.
    spread_bits(lat_cell) | (spread_bits(lon_cell) << 1)
}

fn decode_interleaved(hash: u64, lon_min: f64, lon_max: f64, lat_min: f64, lat_max: f64) -> (f64, f64) {
    let lat_cell = gather_bits(hash);
    let lon_cell = gather_bits(hash >> 1);
    let lon = dequantize(lon_cell, lon_min, lon_max, GEO_BITS);
    let lat = dequantize(lat_cell, lat_min, lat_max, GEO_BITS);
    (lon, lat)
}

/// Encode `(lon, lat)` as the 52-bit internal geohash used as the ZSET score.
pub fn encode_score(lon: f64, lat: f64) -> u64 {
    encode_interleaved(lon, lat, LON_MIN, LON_MAX, LAT_MIN, LAT_MAX)
}

/// Decode a 52-bit internal score back to `(lon, lat)`.
pub fn decode_score(score: u64) -> (f64, f64) {
    decode_interleaved(score, LON_MIN, LON_MAX, LAT_MIN, LAT_MAX)
}

/// Convert a score stored as `f64` back to the 52-bit integer form.
///
/// ZSET scores are `f64`, but GEO-encoded values are always integer-valued
/// within `[0, 2^52)` so the conversion is lossless.
#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
pub const fn score_to_u64(score: f64) -> u64 {
    score as u64
}

/// Produce the 11-character base32 geohash string the `GEOHASH` command
/// returns.
///
/// Matches Redis's behaviour: decode the internal (Mercator) hash, re-encode
/// using the standard latitude range, take the top 50 bits in 5-bit groups,
/// and pin the 11th character to `'0'` (Redis emits 55 bits but only has 52
/// of them, so the low 3 are zero by construction).
pub fn geohash_string(score: f64) -> String {
    let (lon, lat) = decode_score(score_to_u64(score));
    let standard = encode_interleaved(lon, lat, LON_MIN, LON_MAX, STD_LAT_MIN, STD_LAT_MAX);
    let mut out = String::with_capacity(11);
    for i in 0..10 {
        let shift = 52 - (i + 1) * 5;
        let idx = ((standard >> shift) & 0x1f) as usize;
        out.push(BASE32_ALPHABET[idx] as char);
    }
    // Redis pads the 11th character to the alphabet's zero-index — '0'.
    out.push(BASE32_ALPHABET[0] as char);
    out
}

/// Great-circle distance (metres) between two lon/lat pairs, Haversine
/// formula with Redis's Earth radius constant.
pub fn haversine_meters(lon1: f64, lat1: f64, lon2: f64, lat2: f64) -> f64 {
    let lat1r = lat1.to_radians();
    let lat2r = lat2.to_radians();
    let dlat = (lat2 - lat1).to_radians() / 2.0;
    let dlon = (lon2 - lon1).to_radians() / 2.0;
    let lat_term = dlat.sin().powi(2);
    let lon_term = dlon.sin().powi(2);
    let a = lat1r.cos().mul_add(lat2r.cos() * lon_term, lat_term);
    let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());
    EARTH_RADIUS_M * c
}

/// Redis-compatible "is the point inside this lat-aligned box" check.
///
/// Returns the Haversine distance from centre to point in metres when the
/// point is inside the box, `None` otherwise. Redis projects axis distances
/// independently: it measures lat-axis distance (`|Δlat|`) and lon-axis
/// distance (`|Δlon|` at the centre's latitude) via Haversine and compares
/// each against half the box dimension. This avoids flat-Earth distortion
/// that a naïve degree-based rectangle would introduce at high latitudes.
pub fn point_in_box(
    centre_lon: f64,
    centre_lat: f64,
    width_m: f64,
    height_m: f64,
    point_lon: f64,
    point_lat: f64,
) -> Option<f64> {
    let lat_distance = haversine_meters(0.0, centre_lat, 0.0, point_lat);
    let lon_distance = haversine_meters(centre_lon, centre_lat, point_lon, centre_lat);
    if lat_distance > height_m / 2.0 || lon_distance > width_m / 2.0 {
        return None;
    }
    Some(haversine_meters(centre_lon, centre_lat, point_lon, point_lat))
}

#[cfg(test)]
#[allow(clippy::unreadable_literal, clippy::inconsistent_digit_grouping, clippy::cast_precision_loss)]
mod tests {
    use super::*;

    // Redis docs' canonical test points.
    const PALERMO: (f64, f64) = (13.361389, 38.115556);
    const CATANIA: (f64, f64) = (15.087269, 37.502669);

    #[test]
    fn encode_decode_roundtrip_recovers_coordinates_within_cell() {
        let (lon, lat) = PALERMO;
        let score = encode_score(lon, lat);
        let (rlon, rlat) = decode_score(score);
        // 26 bits of precision over 360° → ~5.4e-6° longitude, ~2.5e-6° latitude.
        // We just need to be well within a cell — a 1e-3 tolerance is plenty.
        assert!((lon - rlon).abs() < 1e-3, "lon drift: {lon} vs {rlon}");
        assert!((lat - rlat).abs() < 1e-3, "lat drift: {lat} vs {rlat}");
    }

    #[test]
    fn geohash_string_matches_redis_palermo_example() {
        let (lon, lat) = PALERMO;
        let score = encode_score(lon, lat) as f64;
        let h = geohash_string(score);
        // Redis docs: GEOHASH Sicily Palermo → "sqc8b49rny0"
        assert_eq!(h, "sqc8b49rny0", "palermo geohash mismatch");
    }

    #[test]
    fn geohash_string_matches_redis_catania_example() {
        let (lon, lat) = CATANIA;
        let score = encode_score(lon, lat) as f64;
        let h = geohash_string(score);
        // Redis docs: GEOHASH Sicily Catania → "sqdtr74hyu0"
        assert_eq!(h, "sqdtr74hyu0", "catania geohash mismatch");
    }

    #[test]
    fn haversine_palermo_to_catania_matches_redis_example() {
        let (lon1, lat1) = PALERMO;
        let (lon2, lat2) = CATANIA;
        let d = haversine_meters(lon1, lat1, lon2, lat2);
        // Redis docs: GEODIST Sicily Palermo Catania → "166274.1516" m.
        // Our constants and formula match Redis's exactly, so the result is
        // stable — allow a tiny tolerance for floating-point jitter.
        assert!((d - 166_274.1516).abs() < 1.0, "haversine drift: {d}");
    }

    #[test]
    fn validate_lon_lat_accepts_bounds() {
        assert!(validate_lon_lat(-180.0, -85.05112878).is_ok());
        assert!(validate_lon_lat(180.0, 85.05112878).is_ok());
        assert!(validate_lon_lat(0.0, 0.0).is_ok());
    }

    #[test]
    fn validate_lon_lat_rejects_out_of_range() {
        assert!(validate_lon_lat(-181.0, 0.0).is_err());
        assert!(validate_lon_lat(181.0, 0.0).is_err());
        assert!(validate_lon_lat(0.0, -90.0).is_err());
        assert!(validate_lon_lat(0.0, 90.0).is_err());
    }

    #[test]
    fn geo_unit_parses_case_insensitively() {
        assert_eq!(GeoUnit::parse("m").unwrap(), GeoUnit::Meters);
        assert_eq!(GeoUnit::parse("KM").unwrap(), GeoUnit::Kilometers);
        assert_eq!(GeoUnit::parse("Mi").unwrap(), GeoUnit::Miles);
        assert_eq!(GeoUnit::parse("ft").unwrap(), GeoUnit::Feet);
        assert!(GeoUnit::parse("yd").is_err());
    }

    #[test]
    fn geo_unit_to_meters_conversions() {
        assert!((GeoUnit::Kilometers.to_meters() - 1000.0).abs() < 1e-9);
        assert!((GeoUnit::Miles.to_meters() - 1609.344).abs() < 1e-9);
        assert!((GeoUnit::Feet.to_meters() - 0.3048).abs() < 1e-9);
    }

    #[test]
    fn point_in_box_accepts_palermo_inside_wide_box_around_catania() {
        let (clon, clat) = CATANIA;
        let (plon, plat) = PALERMO;
        // 400 km wide, 200 km tall — Palermo (~166 km west, ~60 km north)
        // sits comfortably inside.
        let d = point_in_box(clon, clat, 400_000.0, 200_000.0, plon, plat);
        assert!(d.is_some(), "palermo should fall inside a 400×200 km box around catania");
    }

    #[test]
    fn point_in_box_rejects_palermo_outside_narrow_box_around_catania() {
        let (clon, clat) = CATANIA;
        let (plon, plat) = PALERMO;
        // 10 km wide × 10 km tall — Palermo is ~166 km away and should miss.
        let d = point_in_box(clon, clat, 10_000.0, 10_000.0, plon, plat);
        assert!(d.is_none(), "palermo should NOT fall inside a 10×10 km box around catania");
    }
}
