//! Delay Injection Configuration and Zero-Allocation PRNG Evaluation.
//!
//! # Architecture & Performance Invariants
//! - **Unified Range Model**: Delay is modeled exclusively as `[min_ms, max_ms]`. When
//!   `min_ms == max_ms`, it natively represents fixed duration delay.
//! - **Zero-Syscall PRNG**: Uses a thread-local XorShift64* pseudo-random number generator
//!   running in ~1 nanosecond with zero locks, zero memory allocations, and zero syscalls.
//! - **Pre-computed Threshold**: Percentage ($0.0\% - 100.0\%$) is converted at instantiation
//!   into a single `u64` threshold. Evaluating the hit condition takes exactly 1 CPU clock cycle
//!   via `fast_random_u64() < threshold`.

use std::cell::Cell;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

static GLOBAL_SEED: AtomicU64 = AtomicU64::new(0x853c49e6748fea9b);

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
                x = 0x853c49e6748fea9b;
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

/// Configuration for HTTP Delay Injection.
#[derive(Debug, Clone, PartialEq)]
pub struct HttpDelayConfig {
    /// Configured percentage of matching requests that receive delay (0.0 to 100.0).
    pub percentage: f64,
    /// Pre-computed integer threshold in `0..=u64::MAX`.
    threshold: u64,
    /// Minimum delay in milliseconds (inclusive).
    pub min_ms: u64,
    /// Maximum delay in milliseconds (inclusive).
    pub max_ms: u64,
}

impl HttpDelayConfig {
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
    ///
    /// # Precedence Rule
    /// If `override_ms` is supplied (from `x-fault-delay-ms`), it takes immediate precedence
    /// without evaluating random percentage rolls.
    #[inline]
    pub fn calculate_delay(&self, override_ms: Option<u64>) -> Option<Duration> {
        if let Some(ms) = override_ms {
            return Some(Duration::from_millis(ms));
        }

        if self.threshold == 0 {
            return None;
        }

        // 1 CPU cycle integer comparison
        if self.threshold < u64::MAX && fast_random_u64() >= self.threshold {
            return None;
        }

        // Hit! Calculate delay within [min_ms, max_ms]
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
    fn test_delay_config_validation() {
        assert!(HttpDelayConfig::new(101.0, 10, 20).is_err());
        assert!(HttpDelayConfig::new(-1.0, 10, 20).is_err());
        assert!(HttpDelayConfig::new(50.0, 30, 20).is_err()); // min > max
        assert!(HttpDelayConfig::new(10.0, 200, 200).is_ok()); // fixed range
    }

    #[test]
    fn test_delay_fixed_range_100_percent() {
        let config = HttpDelayConfig::new(100.0, 250, 250).unwrap();
        for _ in 0..100 {
            let dur = config.calculate_delay(None).unwrap();
            assert_eq!(dur, Duration::from_millis(250));
        }
    }

    #[test]
    fn test_delay_zero_percent() {
        let config = HttpDelayConfig::new(0.0, 100, 500).unwrap();
        for _ in 0..100 {
            assert!(config.calculate_delay(None).is_none());
        }
    }

    #[test]
    fn test_delay_override_takes_precedence() {
        let config = HttpDelayConfig::new(0.0, 100, 500).unwrap(); // 0% configured
        let dur = config.calculate_delay(Some(750)).unwrap(); // Override supplied!
        assert_eq!(dur, Duration::from_millis(750));
    }

    #[test]
    fn test_delay_range_bounded() {
        let config = HttpDelayConfig::new(100.0, 100, 300).unwrap();
        for _ in 0..100 {
            let dur = config.calculate_delay(None).unwrap();
            let ms = dur.as_millis() as u64;
            assert!((100..=300).contains(&ms));
        }
    }
}
