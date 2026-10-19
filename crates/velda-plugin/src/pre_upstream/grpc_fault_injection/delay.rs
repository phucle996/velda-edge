//! gRPC Delay Injection Configuration and Zero-Allocation PRNG Evaluation.
//!
//! # Architecture & Performance Invariants
//! - **Unified Range Model**: Modeled exclusively as `[min_ms, max_ms]`. When `min_ms == max_ms`,
//!   it natively represents fixed duration delay.
//! - **Zero-Syscall PRNG**: Uses a thread-local XorShift64* PRNG running in ~1 nanosecond with
//!   zero locks, zero allocations, and zero syscalls.
//! - **Pre-computed Threshold**: Percentage ($0.0\% - 100.0\%$) is converted at instantiation
//!   into a single `u64` threshold (`fast_random_u64() < threshold`).

use std::cell::Cell;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

static GLOBAL_SEED: AtomicU64 = AtomicU64::new(0xa0761d6478bd642f);

thread_local! {
    static RNG_STATE: Cell<u64> = const { Cell::new(0) };
}

/// Fast thread-local XorShift64* PRNG running in ~1 nanosecond with zero dependencies.
#[inline(always)]
fn fast_random_u64() -> u64 {
    RNG_STATE.with(|cell| {
        let mut x = cell.get();
        if x == 0 {
            let seed = GLOBAL_SEED.fetch_add(0x9e3779b97f4a7c15, Ordering::Relaxed);
            let mut z = seed ^ 0xbf58476d1ce4e5b9;
            z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
            x = z ^ (z >> 31);
            if x == 0 {
                x = 0xa0761d6478bd642f;
            }
        }
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        cell.set(x);
        x.wrapping_mul(0x2545f4914f6cdd1d)
    })
}

/// Daniel Lemire's fast alternative to modulo reduction for uniform bounded random integers.
#[inline(always)]
fn fast_reduce(val: u64, n: usize) -> usize {
    ((val as u128 * n as u128) >> 64) as usize
}

/// Configuration for gRPC Delay Injection.
#[derive(Debug, Clone, PartialEq)]
pub struct GrpcDelayConfig {
    /// Configured percentage of matching requests that receive delay (0.0 to 100.0).
    pub percentage: f64,
    /// Pre-computed integer threshold in `0..=u64::MAX`.
    threshold: u64,
    /// Minimum delay in milliseconds (inclusive).
    pub min_ms: u64,
    /// Maximum delay in milliseconds (inclusive).
    pub max_ms: u64,
}

impl GrpcDelayConfig {
    /// Creates a new delay configuration with validated bounds.
    pub fn new(percentage: f64, min_ms: u64, max_ms: u64) -> Result<Self, String> {
        if !(0.0..=100.0).contains(&percentage) {
            return Err(format!(
                "Delay percentage must be between 0.0 and 100.0, got {percentage}"
            ));
        }
        if min_ms > max_ms {
            return Err(format!(
                "min_delay_ms ({min_ms}) cannot be greater than max_delay_ms ({max_ms})"
            ));
        }

        let threshold = if percentage <= 0.0 {
            0
        } else if percentage >= 100.0 {
            u64::MAX
        } else {
            ((percentage / 100.0) * (u64::MAX as f64)) as u64
        };

        Ok(Self {
            percentage,
            threshold,
            min_ms,
            max_ms,
        })
    }

    /// Evaluates whether this request should be delayed, returning the duration.
    #[inline]
    pub fn calculate_delay(&self, override_ms: Option<u64>) -> Option<Duration> {
        if let Some(ms) = override_ms {
            return Some(Duration::from_millis(ms));
        }

        if self.threshold == 0 {
            return None;
        }

        if self.threshold < u64::MAX && fast_random_u64() >= self.threshold {
            return None;
        }

        let delay_ms = if self.min_ms == self.max_ms {
            self.min_ms
        } else {
            let delta = (self.max_ms - self.min_ms) as usize;
            let r = fast_random_u64();
            self.min_ms + (fast_reduce(r, delta + 1) as u64)
        };

        Some(Duration::from_millis(delay_ms))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_grpc_delay_config() {
        let config = GrpcDelayConfig::new(100.0, 150, 150).unwrap();
        let dur = config.calculate_delay(None).unwrap();
        assert_eq!(dur, Duration::from_millis(150));

        let override_dur = config.calculate_delay(Some(500)).unwrap();
        assert_eq!(override_dur, Duration::from_millis(500));
    }
}
