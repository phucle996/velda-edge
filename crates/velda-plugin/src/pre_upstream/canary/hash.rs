//! Division-Free SWAR Multi-Header Hashing & Multiplicative Permille Ratio (`canary::hash`).
//!
//! ```text
//!  for (idx, hdr) in hash_headers:
//!    HeaderWorkspace::get_all(hdr) -> trimmed non-empty &[u8]
//!      ├─ None (any configured header missing) ──► return None (stay on baseline, 0 fallback)
//!      └─ Some(bytes) ─► fold(idx, len, bytes) via SWAR as_chunks::<8>() + SplitMix64
//!                                │
//!                                ▼  ((hash64 as u128 * 1000) >> 64) as u32  [1x mulxq, 0 DIV]
//!                          Some(bucket in 0..=999)
//! ```

use http::HeaderName;
use velda_core::L7Request;

const GOLDEN_RATIO_64: u64 = 0x9e37_79b9_7f4a_7c15;
const MIX_C1: u64 = 0xbf58_476d_1ce4_e5b9;
const MIX_C2: u64 = 0x94d0_49bb_1331_11eb;

#[inline(always)]
fn splitmix64(mut x: u64) -> u64 {
    x = (x ^ (x >> 30)).wrapping_mul(MIX_C1);
    x = (x ^ (x >> 27)).wrapping_mul(MIX_C2);
    x ^ (x >> 31)
}

/// Hashes header value bytes in 8-byte SWAR chunks + SplitMix64 (zero division, stateless).
#[inline(always)]
pub(crate) fn swar_hash64(seed: u64, bytes: &[u8]) -> u64 {
    let mut acc = seed ^ (bytes.len() as u64).wrapping_mul(GOLDEN_RATIO_64);
    let (chunks, tail) = bytes.as_chunks::<8>();
    for chunk in chunks {
        let word = u64::from_le_bytes(*chunk);
        acc = splitmix64(acc ^ word);
    }
    if !tail.is_empty() {
        let mut last = 0u64;
        for (i, &b) in tail.iter().enumerate() {
            last |= (b as u64) << (i * 8);
        }
        acc = splitmix64(acc ^ last);
    }
    acc ^ (acc >> 33)
}

/// Maps any 64-bit value uniformly into `0..=999` via multiplicative inverse (`* 1000 >> 64`),
/// using zero division and zero user counters so user cardinality cannot overflow.
#[inline(always)]
pub const fn hash_to_permille(hash: u64) -> u32 {
    (((hash as u128).wrapping_mul(1000)) >> 64) as u32
}

/// Reads trimmed non-empty header value bytes directly from `HeaderWorkspace`.
#[inline]
fn extract_header_bytes<'a>(req: &'a L7Request, hdr: &HeaderName) -> Option<&'a [u8]> {
    for val in req.headers.get_all(hdr) {
        let trimmed = val.as_bytes().trim_ascii();
        if !trimmed.is_empty() {
            return Some(trimmed);
        }
    }
    None
}

/// Evaluates stateless permille bucket (`0..=999`) solely from `HeaderWorkspace` headers.
///
/// Strict multi-header semantics with zero fallback: all configured `hash_headers` must be present
/// and non-empty; missing any configured header returns `None` so the caller remains on baseline.
#[inline]
pub(crate) fn evaluate_permille(req: &L7Request, hash_headers: &[HeaderName]) -> Option<u32> {
    if hash_headers.is_empty() {
        return None;
    }

    let mut acc = GOLDEN_RATIO_64;
    for (idx, hdr) in hash_headers.iter().enumerate() {
        let bytes = extract_header_bytes(req, hdr)?;
        acc = splitmix64(acc ^ ((idx as u64) << 32) ^ (bytes.len() as u64).wrapping_add(1));
        acc = swar_hash64(acc, bytes);
    }
    Some(hash_to_permille(acc))
}
