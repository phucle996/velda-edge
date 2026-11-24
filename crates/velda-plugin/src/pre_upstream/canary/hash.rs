//! Division-Free SWAR Header Hashing & Multiplicative Permille Ratio (`canary::hash`).
//!
//! ```text
//!  HeaderWorkspace::get_all(hdr) -> trimmed &[u8]
//!        │
//!        ▼  SWAR as_chunks::<8>() + SplitMix64 (stateless, 0 DIV, 0 user counters)
//!     hash64: u64
//!        │
//!        ▼  ((hash64 as u128 * 1000) >> 64) as u32   [1x mulq inverse scaling, 0 DIV]
//!     bucket in 0..=999
//!        │
//!        └──► (bucket < ratio)   [ratio in 0..=1000]
//! ```

use std::cell::Cell;

use http::HeaderName;
use velda_core::L7Request;

use super::config::HashStrategy;

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
pub fn hash_to_permille(hash: u64) -> u32 {
    (((hash as u128).wrapping_mul(1000)) >> 64) as u32
}

/// Fast thread-local 64-bit PRNG (Xorshift64*, zero cross-core cache contention).
#[inline]
pub fn fast_prng_u64() -> u64 {
    thread_local! {
        static RNG_STATE: Cell<u64> = Cell::new({
            let ptr = &RNG_STATE as *const _ as usize as u64;
            let time = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0x517c_c1b7_2722_0a95);
            ptr ^ time ^ GOLDEN_RATIO_64
        });
    }

    RNG_STATE.with(|cell| {
        let mut x = cell.get();
        if x == 0 {
            x = 0x853c_49e6_748f_ea9b;
        }
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        cell.set(x);
        x.wrapping_mul(0x2545_f491_4f6c_dd1d)
    })
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
#[inline]
pub(crate) fn evaluate_permille(
    req: &L7Request,
    strategy: HashStrategy,
    hash_headers: &[HeaderName],
) -> u32 {
    match strategy {
        HashStrategy::Random => hash_to_permille(fast_prng_u64()),
        HashStrategy::Priority => {
            for hdr in hash_headers {
                if let Some(bytes) = extract_header_bytes(req, hdr) {
                    return hash_to_permille(swar_hash64(GOLDEN_RATIO_64, bytes));
                }
            }
            hash_to_permille(fast_prng_u64())
        }
        HashStrategy::Composite => {
            let mut acc = GOLDEN_RATIO_64;
            let mut matched_any = false;
            for (idx, hdr) in hash_headers.iter().enumerate() {
                if let Some(bytes) = extract_header_bytes(req, hdr) {
                    acc = splitmix64(
                        acc ^ ((idx as u64) << 32) ^ (bytes.len() as u64).wrapping_add(1),
                    );
                    acc = swar_hash64(acc, bytes);
                    matched_any = true;
                } else {
                    acc = splitmix64(acc ^ ((idx as u64) << 32));
                }
            }
            if matched_any {
                hash_to_permille(acc)
            } else {
                hash_to_permille(fast_prng_u64())
            }
        }
    }
}
