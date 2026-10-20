//! Sharded, Lock-Striped Dual Cache for JWT Verification (`positive` and `negative` caching).
//!
//! # Architecture & Threat Mitigation
//! - **Positive Cache**: Stores pre-verified tokens along with pre-built [`HeaderValue`] instances.
//!   On cache hit, claims injection takes ~20ns without re-verifying cryptography or re-parsing JSON.
//! - **Negative Cache**: Caches invalid or malformed tokens for a bounded TTL (e.g. 10s).
//!   Protects against Crypto-Exhaustion Denial-of-Service attacks by failing fast before running
//!   expensive asymmetric crypto algorithms (RSA/ECDSA).
//! - **Lock-Striped Sharding**: Uses 64 independent mutex-guarded shards and [`rustc_hash::FxHasher`]
//!   to eliminate lock contention across worker threads.
//! - **Bounded Capacity & Safe Eviction**: Each shard enforces a strict capacity limit, preventing
//!   unbounded memory growth under sustained token churn.

use std::hash::Hasher;
use std::sync::Mutex;

use http::header::{HeaderName, HeaderValue};
use rustc_hash::{FxHashMap, FxHasher};

/// Positive cache entry storing expiration epoch and pre-built headers for instant injection.
#[derive(Clone, Debug)]
pub struct ValidTokenEntry {
    pub exp_epoch_secs: u64,
    pub injected_headers: Vec<(HeaderName, HeaderValue)>,
}

/// Negative cache entry recording failure expiry to defeat cryptographic exhaustion attacks.
#[derive(Clone, Copy, Debug)]
pub struct InvalidTokenEntry {
    pub expire_epoch_secs: u64,
}

/// Cache-line aligned shard container preventing false sharing across CPU cores during JWT verification.
#[repr(align(64))]
struct CacheShard {
    positive: FxHashMap<u64, ValidTokenEntry>,
    negative: FxHashMap<u64, InvalidTokenEntry>,
    max_entries: usize,
}

impl CacheShard {
    fn new(max_entries: usize) -> Self {
        Self {
            positive: FxHashMap::default(),
            negative: FxHashMap::default(),
            max_entries,
        }
    }
}

/// Thread-safe, lock-striped dual cache for JWT verification.
pub struct JwtCache {
    // Optimization: Sharded into cache-line aligned (64-byte) cells to eliminate MESI cache bouncing between cores.
    shards: Box<[Mutex<CacheShard>]>,
    mask: usize,
    default_ttl_secs: u64,
    negative_ttl_secs: u64,
}

impl JwtCache {
    /// Creates a new `JwtCache` with the specified shard count and capacities.
    ///
    /// Shard count is automatically rounded up to the nearest power of 2 for fast bitwise masking.
    pub fn new(
        shard_count: usize,
        max_entries_per_shard: usize,
        default_ttl_secs: u64,
        negative_ttl_secs: u64,
    ) -> Self {
        let count = shard_count.max(1).next_power_of_two();
        let mask = count - 1;
        let mut shards = Vec::with_capacity(count);
        for _ in 0..count {
            shards.push(Mutex::new(CacheShard::new(max_entries_per_shard)));
        }

        Self {
            shards: shards.into_boxed_slice(),
            mask,
            default_ttl_secs,
            negative_ttl_secs,
        }
    }

    /// Computes a fast 64-bit hash of a raw token slice using [`FxHasher`].
    #[inline]
    pub fn hash_token(token: &str) -> u64 {
        let mut hasher = FxHasher::default();
        hasher.write(token.as_bytes());
        hasher.finish()
    }

    /// Checks the positive cache. If valid and not expired, returns a clone of pre-built headers.
    pub fn get_valid(
        &self,
        token_hash: u64,
        now_epoch: u64,
    ) -> Option<Vec<(HeaderName, HeaderValue)>> {
        let shard_idx = (token_hash as usize) & self.mask;
        if let Ok(shard) = self.shards[shard_idx].lock()
            && let Some(entry) = shard.positive.get(&token_hash)
            && entry.exp_epoch_secs > now_epoch
        {
            return Some(entry.injected_headers.clone());
        }
        None
    }

    /// Checks the negative cache. Returns `true` if the token is currently known to be invalid.
    pub fn is_negative(&self, token_hash: u64, now_epoch: u64) -> bool {
        let shard_idx = (token_hash as usize) & self.mask;
        if let Ok(shard) = self.shards[shard_idx].lock()
            && let Some(entry) = shard.negative.get(&token_hash)
            && entry.expire_epoch_secs > now_epoch
        {
            return true;
        }
        false
    }

    /// Stores a verified token and its pre-built headers into the positive cache.
    pub fn insert_valid(
        &self,
        token_hash: u64,
        exp_epoch_secs: u64,
        headers: Vec<(HeaderName, HeaderValue)>,
        now_epoch: u64,
    ) {
        let shard_idx = (token_hash as usize) & self.mask;
        if let Ok(mut shard) = self.shards[shard_idx].lock() {
            shard.negative.remove(&token_hash);

            // Bounded eviction if full
            if shard.positive.len() >= shard.max_entries {
                shard.positive.retain(|_, v| v.exp_epoch_secs > now_epoch);
                if shard.positive.len() >= shard.max_entries
                    && let Some(&oldest_key) = shard.positive.keys().next()
                {
                    shard.positive.remove(&oldest_key);
                }
            }

            let effective_exp = exp_epoch_secs.min(now_epoch + self.default_ttl_secs);
            shard.positive.insert(
                token_hash,
                ValidTokenEntry {
                    exp_epoch_secs: effective_exp,
                    injected_headers: headers,
                },
            );
        }
    }

    /// Stores an invalid or unverified token into the negative cache for bounded TTL.
    pub fn insert_invalid(&self, token_hash: u64, now_epoch: u64) {
        let shard_idx = (token_hash as usize) & self.mask;
        if let Ok(mut shard) = self.shards[shard_idx].lock() {
            shard.positive.remove(&token_hash);

            // Bounded eviction if full
            if shard.negative.len() >= shard.max_entries {
                shard
                    .negative
                    .retain(|_, v| v.expire_epoch_secs > now_epoch);
                if shard.negative.len() >= shard.max_entries
                    && let Some(&oldest_key) = shard.negative.keys().next()
                {
                    shard.negative.remove(&oldest_key);
                }
            }

            shard.negative.insert(
                token_hash,
                InvalidTokenEntry {
                    expire_epoch_secs: now_epoch + self.negative_ttl_secs,
                },
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_positive_cache_hit_and_expiration() {
        let cache = JwtCache::new(4, 100, 300, 10);
        let token = "valid.jwt.token";
        let token_hash = JwtCache::hash_token(token);
        let now = 1_000_000;

        let headers = vec![(
            HeaderName::from_static("x-user-id"),
            HeaderValue::from_static("usr_42"),
        )];

        cache.insert_valid(token_hash, now + 100, headers, now);

        let res = cache.get_valid(token_hash, now + 50);
        assert!(res.is_some());
        let headers = res.unwrap();
        assert_eq!(headers[0].1, "usr_42");

        // Expired entry
        assert!(cache.get_valid(token_hash, now + 101).is_none());
    }

    #[test]
    fn test_negative_cache_hit_and_expiration() {
        let cache = JwtCache::new(4, 100, 300, 10);
        let token = "bad.jwt.token";
        let token_hash = JwtCache::hash_token(token);
        let now = 1_000_000;

        cache.insert_invalid(token_hash, now);

        assert!(cache.is_negative(token_hash, now + 5));
        assert!(!cache.is_negative(token_hash, now + 11));
    }

    #[test]
    fn test_cache_capacity_eviction() {
        let cache = JwtCache::new(1, 2, 300, 10);
        let now = 1_000_000;

        let h1 = vec![(
            HeaderName::from_static("x-id"),
            HeaderValue::from_static("1"),
        )];
        let h2 = vec![(
            HeaderName::from_static("x-id"),
            HeaderValue::from_static("2"),
        )];
        let h3 = vec![(
            HeaderName::from_static("x-id"),
            HeaderValue::from_static("3"),
        )];

        cache.insert_valid(1, now + 100, h1, now);
        cache.insert_valid(2, now + 100, h2, now);
        // Exceeds capacity 2, should evict one
        cache.insert_valid(3, now + 100, h3, now);

        // At least entry 3 must be present
        assert!(cache.get_valid(3, now + 10).is_some());
    }
}
