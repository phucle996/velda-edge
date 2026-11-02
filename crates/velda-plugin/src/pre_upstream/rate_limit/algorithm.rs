//! Rate Limiting Algorithms and In-Memory Sharded State.
//!
//! Provides lock-striped concurrent state partitions and rate-limiting algorithms:
//! - **Token Bucket**: Fractional refill with burst capacity.
//! - **Sliding Window**: Smooth transition between adjacent seconds.
//! - **Fixed Window**: 1-second atomic intervals for ultra-high throughput.
//!
//! # Performance Contract
//! - 64 lock-striped shards eliminating thread contention across worker cores.
//! - Direct 64-bit integer keys (`u64` hash) for zero-allocation lookups.
//! - Periodic 60s lazy cleanup to bound memory footprint without dedicated GC threads.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use rustc_hash::FxHashMap;

/// Supported rate limiting algorithms.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RateLimitAlgorithm {
    /// Token Bucket algorithm with burst capacity and continuous fractional refill (Default).
    #[default]
    TokenBucket,
    /// Sliding Window counter blending current and previous second windows.
    SlidingWindow,
    /// Fixed Window 1-second atomic counter for ultra-lightweight high-throughput pacing.
    FixedWindow,
}

impl RateLimitAlgorithm {
    /// Parses an algorithm identifier from configuration string.
    pub fn parse(s: &str) -> Self {
        match s.trim().to_ascii_lowercase().as_str() {
            "token_bucket" | "tokenbucket" => Self::TokenBucket,
            "sliding_window" | "slidingwindow" => Self::SlidingWindow,
            "fixed_window" | "fixedwindow" => Self::FixedWindow,
            _ => Self::default(),
        }
    }
}

/// Fixed-point scaling factor for integer Token Bucket accounting (`1 token = 1_000_000_000 nano-tokens`).
/// Eliminates all floating-point (`f64`/FPU) conversions and IEEE-754 precision drift on the hot path.
pub const NANO_TOKENS_PER_UNIT: u64 = 1_000_000_000;

/// Per-client state stored in in-memory shards.
#[derive(Debug, Clone)]
pub struct KeyState {
    pub last_access_sec: u64,
    // Token Bucket fields (pure integer fixed-point nano-tokens)
    pub nano_tokens: u64,
    pub last_refill: Instant,
    // Sliding Window fields
    pub sw_current_sec: u64,
    pub sw_current_count: u64,
    pub sw_prev_count: u64,
    // Fixed Window fields
    pub fw_window_sec: u64,
    pub fw_count: u64,
}

impl KeyState {
    pub fn new(burst_capacity: u64, now_sec: u64, now_instant: Instant) -> Self {
        Self {
            last_access_sec: now_sec,
            nano_tokens: burst_capacity.saturating_mul(NANO_TOKENS_PER_UNIT),
            last_refill: now_instant,
            sw_current_sec: now_sec,
            sw_current_count: 0,
            sw_prev_count: 0,
            fw_window_sec: now_sec,
            fw_count: 0,
        }
    }
}

/// Shard count for in-memory concurrent state striping (power of 2).
const NUM_SHARDS: usize = 64;

/// Maximum active client state entries allowed per shard to prevent cardinality bombs and memory exhaustion.
/// With 64 shards, 32,768 entries/shard permits up to 2,097,152 concurrently tracked clients (~160MB max).
pub const MAX_ENTRIES_PER_SHARD: usize = 32_768;

/// Cache-line aligned shard container preventing false sharing across CPU cores.
#[repr(align(64))]
struct CacheAlignedShard {
    lock: std::sync::Mutex<FxHashMap<u64, KeyState>>,
    last_cleanup: AtomicU64,
}

/// Sharded in-memory table eliminating lock contention across worker threads.
pub struct ShardedState {
    shards: [CacheAlignedShard; NUM_SHARDS],
}

impl Default for ShardedState {
    fn default() -> Self {
        Self::new()
    }
}

impl ShardedState {
    /// Instantiates a new lock-striped state container with 64 independent, cache-aligned shards.
    pub fn new() -> Self {
        Self {
            shards: std::array::from_fn(|_| CacheAlignedShard {
                lock: std::sync::Mutex::new(FxHashMap::default()),
                last_cleanup: AtomicU64::new(0),
            }),
        }
    }

    /// Evaluates rate limiting for a hashed client identifier key.
    ///
    /// Returns `(allowed: bool, retry_after_seconds: u64)`.
    pub fn evaluate(
        &self,
        key_hash: u64,
        algorithm: RateLimitAlgorithm,
        rate_per_second: u64,
        burst_capacity: u64,
    ) -> (bool, u64) {
        let shard_idx = (key_hash as usize) & (NUM_SHARDS - 1);
        let shard_entry = &self.shards[shard_idx];
        let mut shard = shard_entry.lock.lock().unwrap();

        let now_instant = Instant::now();
        let now_dur = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        let now_sec = now_dur.as_secs();

        // Perform periodic cleanup of stale keys (>60s inactivity) every 60s
        let last_clean = shard_entry.last_cleanup.load(Ordering::Relaxed);
        if now_sec.saturating_sub(last_clean) > 60 {
            shard.retain(|_, v| now_sec.saturating_sub(v.last_access_sec) < 60);
            shard_entry.last_cleanup.store(now_sec, Ordering::Relaxed);
        }

        let entry = match shard.get_mut(&key_hash) {
            Some(existing) => existing,
            None => {
                // Anti-Cardinality-Bomb Guard: If shard reaches capacity, eagerly prune inactive keys (>10s)
                if shard.len() >= MAX_ENTRIES_PER_SHARD {
                    shard.retain(|_, v| now_sec.saturating_sub(v.last_access_sec) < 10);
                    shard_entry.last_cleanup.store(now_sec, Ordering::Relaxed);
                }
                if shard.len() >= MAX_ENTRIES_PER_SHARD {
                    // Shard saturated under sustained attack: reject new key to prevent memory exhaustion
                    return (false, 1);
                }
                shard
                    .entry(key_hash)
                    .or_insert_with(|| KeyState::new(burst_capacity, now_sec, now_instant))
            }
        };
        entry.last_access_sec = now_sec;

        match algorithm {
            RateLimitAlgorithm::TokenBucket => {
                let elapsed_nanos = now_instant.duration_since(entry.last_refill).as_nanos() as u64;
                entry.last_refill = now_instant;

                // Pure integer fixed-point refill: 1 token = 1_000_000_000 nano-tokens.
                // Zero FPU (f64) instructions, zero IEEE-754 precision drift.
                let added_nano = (elapsed_nanos as u128)
                    .saturating_mul(rate_per_second as u128)
                    .min(u64::MAX as u128) as u64;
                let max_nano = burst_capacity.saturating_mul(NANO_TOKENS_PER_UNIT);
                entry.nano_tokens = entry.nano_tokens.saturating_add(added_nano).min(max_nano);

                if entry.nano_tokens >= NANO_TOKENS_PER_UNIT {
                    entry.nano_tokens -= NANO_TOKENS_PER_UNIT;
                    (true, 0)
                } else if rate_per_second == 0 {
                    (false, 1)
                } else {
                    let deficit_nano = NANO_TOKENS_PER_UNIT - entry.nano_tokens;
                    let denom = rate_per_second.saturating_mul(NANO_TOKENS_PER_UNIT).max(1);
                    let wait_sec = deficit_nano.saturating_add(denom - 1) / denom;
                    (false, wait_sec.max(1))
                }
            }
            RateLimitAlgorithm::SlidingWindow => {
                if now_sec == entry.sw_current_sec {
                    // Same 1-second window
                } else if now_sec == entry.sw_current_sec + 1 {
                    entry.sw_prev_count = entry.sw_current_count;
                    entry.sw_current_count = 0;
                    entry.sw_current_sec = now_sec;
                } else {
                    entry.sw_prev_count = 0;
                    entry.sw_current_count = 0;
                    entry.sw_current_sec = now_sec;
                }

                // Pure integer cross-multiplication (scaled by 1000 ms, zero FPU division):
                // effective_milli = sw_prev_count * (1000 - subsec_ms) + sw_current_count * 1000
                let subsec_ms = now_dur.subsec_millis() as u64;
                let weighted_prev = entry
                    .sw_prev_count
                    .saturating_mul(1000u64.saturating_sub(subsec_ms));
                let weighted_curr = entry.sw_current_count.saturating_mul(1000);
                let effective_milli = weighted_prev.saturating_add(weighted_curr);
                let limit_milli = rate_per_second.saturating_mul(1000);

                if effective_milli < limit_milli {
                    entry.sw_current_count += 1;
                    (true, 0)
                } else {
                    (false, 1)
                }
            }
            RateLimitAlgorithm::FixedWindow => {
                if now_sec != entry.fw_window_sec {
                    entry.fw_window_sec = now_sec;
                    entry.fw_count = 1;
                    (true, 0)
                } else if entry.fw_count < rate_per_second {
                    entry.fw_count += 1;
                    (true, 0)
                } else {
                    (false, 1)
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_algorithm_parsing() {
        assert_eq!(
            RateLimitAlgorithm::parse("token_bucket"),
            RateLimitAlgorithm::TokenBucket
        );
        assert_eq!(
            RateLimitAlgorithm::parse("tokenbucket"),
            RateLimitAlgorithm::TokenBucket
        );
        assert_eq!(
            RateLimitAlgorithm::parse("sliding_window"),
            RateLimitAlgorithm::SlidingWindow
        );
        assert_eq!(
            RateLimitAlgorithm::parse("fixed_window"),
            RateLimitAlgorithm::FixedWindow
        );
        assert_eq!(
            RateLimitAlgorithm::parse("unknown_mode"),
            RateLimitAlgorithm::TokenBucket
        );
    }

    #[test]
    fn test_token_bucket_exhaustion() {
        let state = ShardedState::new();
        let key_hash = 99999u64;

        // Rate 2, burst 2
        let (ok1, _) = state.evaluate(key_hash, RateLimitAlgorithm::TokenBucket, 2, 2);
        assert!(ok1);

        let (ok2, _) = state.evaluate(key_hash, RateLimitAlgorithm::TokenBucket, 2, 2);
        assert!(ok2);

        // 3rd hit must be rejected
        let (ok3, retry) = state.evaluate(key_hash, RateLimitAlgorithm::TokenBucket, 2, 2);
        assert!(!ok3);
        assert!(retry >= 1);
    }

    #[test]
    fn test_fixed_window_evaluation() {
        let state = ShardedState::new();
        let key_hash = 11111u64;

        let (ok1, _) = state.evaluate(key_hash, RateLimitAlgorithm::FixedWindow, 1, 1);
        assert!(ok1);

        let (ok2, retry) = state.evaluate(key_hash, RateLimitAlgorithm::FixedWindow, 1, 1);
        assert!(!ok2);
        assert_eq!(retry, 1);
    }

    #[test]
    fn test_cardinality_bomb_protection() {
        let state = ShardedState::new();
        let shard_idx = 0usize;
        {
            let mut shard = state.shards[shard_idx].lock.lock().unwrap();
            let now_sec = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0);
            let now_instant = Instant::now();
            // Fill shard to capacity with keys mapping to shard 0
            for i in 0..MAX_ENTRIES_PER_SHARD {
                let key = (i as u64) * 64; // key % 64 == 0
                shard.insert(key, KeyState::new(10, now_sec, now_instant));
            }
        }

        // New key mapping to shard 0 should be rejected due to capacity limit
        let new_key = (MAX_ENTRIES_PER_SHARD as u64 + 100) * 64;
        let (allowed, retry) = state.evaluate(new_key, RateLimitAlgorithm::TokenBucket, 10, 10);
        assert!(!allowed);
        assert_eq!(retry, 1);
    }
}
