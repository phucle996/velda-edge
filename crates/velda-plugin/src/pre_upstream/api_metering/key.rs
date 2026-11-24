//! Division-Free SWAR Key Mixer & Zero-Hash Tier Resolution (`api_metering::key`).
//!
//! ```text
//! L7Request.headers
//!   ├──► account_headers[0..3] (no length cap, zero division):
//!   │      └── swar_mix_64(seed ^ level, trimmed) ─► (h63 << 1) | has_child  [bit 0: 0=leaf, 1=child]
//!   │      └── all missing/empty ────────────────► None ──► #[cold] 400 Bad Request
//!   └──► tier_header ──► len == tier.len && eq_ignore_ascii_case (0 hash, fallback: default_tier)
//! ```

use http::HeaderName;
use velda_core::L7Request;

use super::arena::MAX_HIERARCHY_LEVELS;
use super::config::CompiledTier;

const SWAR_K1: u64 = 0x517c_c1b7_2722_0a95;
const SWAR_K2: u64 = 0x9e37_79b9_7f4a_7c15;

/// Packed multi-level key chain where bit `0` of each `u64` signals child presence (`1`) vs leaf (`0`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AccountKeyChain {
    /// Tagged keys `[L0, L1, L2]`: bits `1..64` = 63-bit hash (`>= 1`), bit `0` = `has_child`.
    pub tagged_keys: [u64; MAX_HIERARCHY_LEVELS],
    /// Raw 64-bit hashes for node-global sharding (independent of `has_child` flag).
    pub raw_hashes: [u64; MAX_HIERARCHY_LEVELS],
    /// Active hierarchy depth (`1..=MAX_HIERARCHY_LEVELS`).
    pub depth: usize,
}

/// Packs a 63-bit hash (`>= 1`) and `has_child` bit into a tagged key (`>= 2`).
#[inline(always)]
pub const fn pack_tagged_key(raw_hash: u64, has_child: bool) -> u64 {
    let m = raw_hash & 0x7FFF_FFFF_FFFF_FFFF;
    let h63 = if m == 0 { 1 } else { m };
    (h63 << 1) | (has_child as u64)
}

/// Mixes a byte slice in 8-byte SWAR words without division or arbitrary length caps.
#[inline(always)]
fn swar_mix_bytes(mut acc: u64, level_idx: u8, bytes: &[u8]) -> u64 {
    acc ^= ((level_idx as u64) << 56) | (bytes.len() as u64);
    acc = acc.wrapping_mul(SWAR_K2);

    let (chunks, rem) = bytes.as_chunks::<8>();
    for chunk in chunks {
        let word = u64::from_le_bytes(*chunk);
        acc = (acc.rotate_left(5) ^ word).wrapping_mul(SWAR_K1);
    }

    if !rem.is_empty() {
        let mut tail = [0u8; 8];
        tail[..rem.len()].copy_from_slice(rem);
        let word = u64::from_le_bytes(tail);
        acc = (acc.rotate_left(5) ^ word).wrapping_mul(SWAR_K1);
    }

    // SplitMix64 avalanche (zero division) to distribute evenly across 64 shards & buckets.
    let mut z = acc ^ (acc >> 30);
    z = z.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z ^= z >> 27;
    z = z.wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

/// Resolves hierarchical tagged account keys (`Level 0 -> Level 1 -> Level 2`) with zero heap alloc.
///
/// Returns `None` if all configured `account_headers` are absent or whitespace/empty.
#[inline]
pub fn resolve_account_keys(
    req: &L7Request,
    account_headers: &[HeaderName],
) -> Option<AccountKeyChain> {
    let mut raw_hashes = [0u64; MAX_HIERARCHY_LEVELS];
    let mut tagged_keys = [0u64; MAX_HIERARCHY_LEVELS];
    let mut depth = 0usize;
    let mut chained_acc = SWAR_K2;

    for (idx, name) in account_headers.iter().enumerate() {
        if let Some(val) = req.headers.get(name) {
            let trimmed = val.as_bytes().trim_ascii();
            if !trimmed.is_empty() {
                chained_acc = swar_mix_bytes(chained_acc, idx as u8, trimmed);
                raw_hashes[depth] = chained_acc.max(2);
                depth += 1;
                if depth == MAX_HIERARCHY_LEVELS {
                    break;
                }
            }
        }
    }

    if depth == 0 {
        return None;
    }

    for i in 0..depth {
        let has_child = i + 1 < depth;
        tagged_keys[i] = pack_tagged_key(raw_hashes[i], has_child);
    }

    Some(AccountKeyChain {
        tagged_keys,
        raw_hashes,
        depth,
    })
}

/// Resolves the precompiled tier descriptor directly via length + ASCII case-insensitive match (no hash).
#[inline]
pub fn resolve_tier<'a>(
    req: &L7Request,
    tier_header: &HeaderName,
    tiers: &'a [CompiledTier],
    default_tier: &'a CompiledTier,
) -> &'a CompiledTier {
    let Some(val) = req.headers.get(tier_header) else {
        return default_tier;
    };
    let trimmed = val.as_bytes().trim_ascii();
    if trimmed.is_empty() {
        return default_tier;
    }

    for tier in tiers {
        let name_bytes = tier.name.as_bytes();
        if name_bytes.len() == trimmed.len() && name_bytes.eq_ignore_ascii_case(trimmed) {
            return tier;
        }
    }
    default_tier
}
