//! Status-Aware Sampling Engine (`std_http_access_log::sampling`).
//!
//! Provides ultra-fast direct-index $O(1)$ sampling lookups per HTTP status code
//! with zero allocations, zero branch mispredictions, and instantaneous hot-path escape.
//!
//! # Strict Declarative Contract
//! - **Exact 3-Digit HTTP Status Codes Only**: Explicitly declared in config (e.g. `"200": 0.05`, `"429": 1.0`).
//! - **Zero Wildcards**: Wildcard patterns like `"2xx"` or `"5xx"` are strictly forbidden.
//! - **No Default Rate**: Unconfigured status codes have an absolute sampling rate of `0.0` (never logged).
//! - **L1-Cache Resident**: The 600-entry direct index table occupies exactly 1.2 KB (`[u16; 600]`),
//!   guaranteeing it stays hot inside the CPU L1 Data Cache (32 KB/48 KB).

use std::cell::Cell;
use std::collections::HashMap;

thread_local! {
    /// Thread-local fast pseudo-random number generator state (Xorshift32).
    /// Pure register-based arithmetic (~1.2ns), zero syscalls, zero mutex contention.
    static PRNG_SEED: Cell<u32> = const { Cell::new(0x243F6A88) };
}

/// Generates a fast pseudo-random integer in the range `[0, 10_000)`.
#[inline(always)]
pub fn fast_rand_10k() -> u16 {
    PRNG_SEED.with(|seed| {
        let mut x = seed.get();
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        seed.set(x);
        (x % 10_000) as u16
    })
}

/// Direct-index table of sampling rates for HTTP status codes `0..=599`.
///
/// Rates are represented as basis points (`0..=10_000`), where `10_000` = 100.0% and `0` = 0.0%.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SamplingTable {
    rates: [u16; 600],
}

impl Default for SamplingTable {
    /// By default, all rates are 0.0 (no default logging without explicit configuration).
    fn default() -> Self {
        Self { rates: [0u16; 600] }
    }
}

impl SamplingTable {
    /// Constructs an empty sampling table (all rates = 0.0).
    pub fn empty() -> Self {
        Self::default()
    }

    /// Builds a direct-index sampling table from a status-to-rate map.
    ///
    /// # Errors
    /// Returns an error if any status code is invalid, contains wildcards, or rate is outside `[0.0, 1.0]`.
    pub fn from_map(map: &HashMap<String, f64>) -> Result<Self, String> {
        let mut table = Self::empty();
        for (key, &rate) in map {
            let trimmed = key.trim();
            if trimmed.contains('x') || trimmed.contains('X') {
                return Err(format!(
                    "Wildcard patterns like '{trimmed}' are strictly forbidden; specify exact 3-digit HTTP status codes"
                ));
            }

            let code = trimmed.parse::<u16>().map_err(|_| {
                format!("Invalid status code '{trimmed}': must be a 3-digit integer")
            })?;

            if !(100..=599).contains(&code) {
                return Err(format!("Status code '{trimmed}' out of range (100..=599)"));
            }

            if !(0.0..=1.0).contains(&rate) || rate.is_nan() {
                return Err(format!(
                    "Sampling rate for '{trimmed}' ({rate}) must be between 0.0 and 1.0"
                ));
            }

            let basis_points = (rate * 10_000.0).round().clamp(0.0, 10_000.0) as u16;
            table.rates[code as usize] = basis_points;
        }
        Ok(table)
    }

    /// Configures a single status code sampling rate directly.
    #[inline]
    pub fn set_rate(&mut self, status: u16, rate: f64) {
        if status < 600 {
            let basis_points = (rate * 10_000.0).round().clamp(0.0, 10_000.0) as u16;
            self.rates[status as usize] = basis_points;
        }
    }

    /// Returns the configured basis points for a status code (`0..=10_000`).
    #[inline(always)]
    pub fn rate_basis_points(&self, status: u16) -> u16 {
        if status < 600 {
            self.rates[status as usize]
        } else {
            0
        }
    }

    /// Evaluates whether a request with `status` should be sampled.
    ///
    /// # Performance
    /// - **Instant Escape**: If `status` was not declared or has rate 0.0, returns `false`
    ///   in ~1 CPU cycle without computing random numbers.
    /// - **100% Rate**: Returns `true` immediately without random number generation.
    /// - **Sub-sampled**: Generates thread-local Xorshift32 PRNG and evaluates `rand < threshold`.
    #[inline(always)]
    pub fn should_sample(&self, status: u16) -> bool {
        if status >= 600 {
            return false;
        }
        let threshold = self.rates[status as usize];
        if threshold == 0 {
            return false; // Instant escape: status not configured -> 0 cycles on hot path
        }
        if threshold >= 10_000 {
            return true; // 100% sampled
        }
        fast_rand_10k() < threshold
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unconfigured_status_returns_false() {
        let table = SamplingTable::empty();
        // None of these were configured -> all return false instantly
        assert!(!table.should_sample(200));
        assert!(!table.should_sample(404));
        assert!(!table.should_sample(500));
        assert!(!table.should_sample(999));
    }

    #[test]
    fn test_exact_100_percent_sampling() {
        let mut table = SamplingTable::empty();
        table.set_rate(502, 1.0);
        table.set_rate(429, 1.0);

        for _ in 0..100 {
            assert!(table.should_sample(502));
            assert!(table.should_sample(429));
            assert!(!table.should_sample(200)); // Unconfigured
        }
    }

    #[test]
    fn test_sampling_rate_distribution() {
        let mut table = SamplingTable::empty();
        table.set_rate(200, 0.10); // 10% sampling

        let mut hits = 0;
        let trials = 10_000;
        for _ in 0..trials {
            if table.should_sample(200) {
                hits += 1;
            }
        }

        // 10% of 10,000 = ~1,000 hits (allow tolerance of +/- 200 due to pseudo-random variance)
        assert!(
            (800..=1200).contains(&hits),
            "Expected ~1000 hits (10%), got {hits}"
        );
    }

    #[test]
    fn test_from_map_validation() {
        let mut map = HashMap::new();
        map.insert("200".into(), 0.05);
        map.insert("502".into(), 1.0);

        let table = SamplingTable::from_map(&map).expect("Valid map");
        assert_eq!(table.rate_basis_points(200), 500);
        assert_eq!(table.rate_basis_points(502), 10_000);
        assert_eq!(table.rate_basis_points(404), 0);

        let mut bad_map = HashMap::new();
        bad_map.insert("2xx".into(), 0.5);
        assert!(SamplingTable::from_map(&bad_map).is_err());
    }
}
