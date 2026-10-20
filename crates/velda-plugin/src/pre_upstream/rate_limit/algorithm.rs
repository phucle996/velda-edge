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

/// Per-client state stored in in-memory shards.
#[derive(Debug, Clone)]
pub struct KeyState {
    pub last_access_sec: u64,
    // Token Bucket fields
    pub tokens: f64,
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
            tokens: burst_capacity as f64,
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
        let now_sec = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        // Perform periodic cleanup of stale keys (>60s inactivity) every 60s
        let last_clean = shard_entry.last_cleanup.load(Ordering::Relaxed);
        if now_sec.saturating_sub(last_clean) > 60 {
            shard.retain(|_, v| now_sec.saturating_sub(v.last_access_sec) < 60);
            shard_entry.last_cleanup.store(now_sec, Ordering::Relaxed);
        }

        let entry = shard
            .entry(key_hash)
            .or_insert_with(|| KeyState::new(burst_capacity, now_sec, now_instant));
        entry.last_access_sec = now_sec;

        match algorithm {
            RateLimitAlgorithm::TokenBucket => {
                let elapsed_sec = now_instant.duration_since(entry.last_refill).as_secs_f64();
                entry.last_refill = now_instant;

                // Refill tokens
                let new_tokens = entry.tokens + (elapsed_sec * rate_per_second as f64);
                entry.tokens = new_tokens.min(burst_capacity as f64);

                if entry.tokens >= 1.0 {
                    entry.tokens -= 1.0;
                    (true, 0)
                } else {
                    let deficit = 1.0 - entry.tokens;
                    let wait_sec = (deficit / rate_per_second as f64).ceil() as u64;
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

                let now_millis = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_millis())
                    .unwrap_or(0);
                let elapsed_fraction = (now_millis % 1000) as f64 / 1000.0;
                let effective_count = (entry.sw_prev_count as f64 * (1.0 - elapsed_fraction))
                    + entry.sw_current_count as f64;

                if effective_count < rate_per_second as f64 {
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
}
