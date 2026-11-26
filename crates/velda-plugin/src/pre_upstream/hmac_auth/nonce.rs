//! Thread-Safe Sharded Nonce Replay Cache (`hmac_auth::nonce`).
//!
//! Tracks unique request nonces using 64-byte cache-line aligned shards (`16..=512` power-of-two).
//! Invoked strictly once per valid signed request (post-signature verification) to minimize lock
//! contention and block unauthenticated nonce flood / lock poisoning.
//!
//! ```text
//!   nonce_bytes ─► nonce_fingerprint_128 ─► shard_idx = fp & (shards.len() - 1)
//!                                               │
//!                                               ▼ (single Mutex lock, post-HMAC verify)
//!                                      expired sweep + bounded insert (cap 8192/shard)
//! ```

use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use rustc_hash::FxHashMap;

pub const MIN_SHARDS: usize = 16;
pub const MAX_SHARDS: usize = 512;
pub const DEFAULT_SHARD_CAPACITY: usize = 512;
pub const MAX_ENTRIES_PER_SHARD: usize = 8192;

/// Computes power-of-two shard count scaled with CPU cores (`clamp(16, 512)`).
#[inline]
pub fn compute_optimal_shards(cores: usize) -> usize {
    (cores.max(1) * 16)
        .next_power_of_two()
        .clamp(MIN_SHARDS, MAX_SHARDS)
}

/// Computes a 128-bit fingerprint from nonce bytes without heap allocations.
#[inline(always)]
pub fn nonce_fingerprint_128(bytes: &[u8]) -> u128 {
    let mut h1 = 0xCBF29CE484222325u64 ^ (bytes.len() as u64);
    let mut h2 = 0x94D049BB133111EBu64 ^ (bytes.len() as u64).wrapping_mul(0x9E3779B97F4A7C15);
    let (chunks, remainder) = bytes.as_chunks::<8>();
    for chunk in chunks {
        let val = u64::from_ne_bytes(*chunk);
        h1 = (h1 ^ val).wrapping_mul(0x9E3779B97F4A7C15);
        h2 = (h2 ^ val).wrapping_mul(0xBF58476D1CE4E5B9);
    }
    for &b in remainder {
        h1 = (h1 ^ (b as u64)).wrapping_mul(0x100000001B3u64);
        h2 = (h2 ^ (b as u64)).wrapping_mul(0x9E3779B97F4A7C15);
    }
    // Final avalanche mix
    h1 ^= h1 >> 30;
    h1 = h1.wrapping_mul(0xBF58476D1CE4E5B9);
    h1 ^= h1 >> 27;
    h2 ^= h2 >> 31;
    h2 = h2.wrapping_mul(0x94D049BB133111EB);
    h2 ^= h2 >> 29;
    ((h1 as u128) << 64) | (h2 as u128)
}

#[repr(align(64))]
struct NonceShard {
    entries: Mutex<FxHashMap<u128, u64>>,
    last_sweep: AtomicU64,
}

impl NonceShard {
    fn with_capacity(cap: usize) -> Self {
        Self {
            entries: Mutex::new(FxHashMap::with_capacity_and_hasher(cap, Default::default())),
            last_sweep: AtomicU64::new(0),
        }
    }
}

/// High-throughput hardware-sharded set for tracking unique request nonces.
pub struct NonceCache {
    shards: Box<[NonceShard]>,
    shard_mask: usize,
    last_cleanup: AtomicU64,
}

impl Default for NonceCache {
    fn default() -> Self {
        Self::new()
    }
}

impl NonceCache {
    /// Constructs a new `NonceCache` sized to host CPU parallelism.
    pub fn new() -> Self {
        let cores = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        let num_shards = compute_optimal_shards(cores);
        let shard_mask = num_shards - 1;

        let mut shards = Vec::with_capacity(num_shards);
        for _ in 0..num_shards {
            shards.push(NonceShard::with_capacity(DEFAULT_SHARD_CAPACITY));
        }

        Self {
            shards: shards.into_boxed_slice(),
            shard_mask,
            last_cleanup: AtomicU64::new(0),
        }
    }

    #[inline(always)]
    fn shard_idx(&self, fp: u128) -> usize {
        (fp as usize) & self.shard_mask
    }

    /// Single-lock check-and-insert: rejects if `fp` exists with `exp > now` or if shard exceeds cap after sweep.
    #[inline]
    pub fn check_and_record_fp(&self, fp: u128, expiry_epoch_secs: u64, now: u64) -> bool {
        let idx = self.shard_idx(fp);
        let shard_ref = &self.shards[idx];

        let mut shard = match shard_ref.entries.lock() {
            Ok(s) => s,
            Err(poisoned) => poisoned.into_inner(),
        };

        let last = shard_ref.last_sweep.load(Ordering::Relaxed);
        if (shard.len() >= 2048 && now > last) || shard.len() >= MAX_ENTRIES_PER_SHARD {
            shard_ref.last_sweep.store(now, Ordering::Relaxed);
            shard.retain(|_, &mut exp| exp > now);
        }

        if let Some(&existing_exp) = shard.get(&fp)
            && existing_exp > now
        {
            return false;
        }

        if shard.len() >= MAX_ENTRIES_PER_SHARD {
            return false;
        }

        shard.insert(fp, expiry_epoch_secs);

        let last_c = self.last_cleanup.load(Ordering::Relaxed);
        if now.saturating_sub(last_c) >= 30
            && self
                .last_cleanup
                .compare_exchange(last_c, now, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
        {
            drop(shard);
            self.purge_expired(now);
        }

        true
    }

    /// Checks and records a raw nonce string directly.
    #[inline]
    pub fn check_and_record(&self, nonce: &str, expiry_epoch_secs: u64, now: u64) -> bool {
        let fp = nonce_fingerprint_128(nonce.as_bytes());
        self.check_and_record_fp(fp, expiry_epoch_secs, now)
    }

    fn purge_expired(&self, now: u64) {
        for shard_wrapper in &self.shards {
            if let Ok(mut shard) = shard_wrapper.entries.try_lock() {
                shard.retain(|_, &mut exp| exp > now);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_optimal_shards_bounds() {
        assert_eq!(compute_optimal_shards(1), 16);
        assert_eq!(compute_optimal_shards(4), 64);
        assert_eq!(compute_optimal_shards(16), 256);
        assert_eq!(compute_optimal_shards(64), 512);
        assert_eq!(compute_optimal_shards(512), 512);
    }

    #[test]
    fn test_nonce_fresh_and_replay() {
        let cache = NonceCache::new();
        let now = 1000;
        let exp = now + 60;

        assert!(cache.check_and_record("nonce-12345", exp, now));
        assert!(!cache.check_and_record("nonce-12345", exp, now + 5));
        assert!(cache.check_and_record("nonce-67890", exp, now + 5));
        assert!(cache.check_and_record("nonce-12345", now + 120, now + 70));
    }
}
