//! Direct-Index Status-Aware Sampling Engine (`opentelemetry_tracer::sampling`).
//!
//! Provides ultra-fast direct-index $O(1)$ sampling lookups per HTTP status code
//! with zero allocations, zero branch mispredictions, and instantaneous hot-path escape.

use std::cell::Cell;
use std::collections::HashMap;

thread_local! {
    static PRNG_SEED: Cell<u32> = const { Cell::new(0x243F6A88) };
}

#[inline(always)]
fn fast_rand_10k() -> u16 {
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SamplingTable {
    rates: [u16; 600],
}

impl Default for SamplingTable {
    fn default() -> Self {
        let mut rates = [0u16; 600];
        rates[100..=599].fill(10_000);
        Self { rates }
    }
}

impl SamplingTable {
    pub fn empty() -> Self {
        Self { rates: [0u16; 600] }
    }

    pub fn all(rate: f64) -> Self {
        let mut rates = [0u16; 600];
        let bp = (rate * 10_000.0).round().clamp(0.0, 10_000.0) as u16;
        rates[100..=599].fill(bp);
        Self { rates }
    }

    pub fn from_map(map: &HashMap<String, f64>) -> Result<Self, String> {
        let mut table = Self::default();

        if let Some(&default_rate) = map.get("default") {
            if !(0.0..=1.0).contains(&default_rate) || default_rate.is_nan() {
                return Err(format!(
                    "Default sampling rate ({default_rate}) must be between 0.0 and 1.0"
                ));
            }
            let bp = (default_rate * 10_000.0).round().clamp(0.0, 10_000.0) as u16;
            table.rates[100..=599].fill(bp);
        }

        for (key, &rate) in map {
            let trimmed = key.trim();
            if trimmed == "default" {
                continue;
            }
            if trimmed.contains('x') || trimmed.contains('X') {
                return Err(format!(
                    "Wildcard patterns like '{trimmed}' are strictly forbidden; specify exact 3-digit HTTP status codes"
                ));
            }

            let code = trimmed.parse::<u16>().map_err(|_| {
                format!("Invalid status code '{trimmed}': must be a 3-digit integer or 'default'")
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

    #[inline]
    pub fn set_rate(&mut self, status: u16, rate: f64) {
        if status < 600 {
            let basis_points = (rate * 10_000.0).round().clamp(0.0, 10_000.0) as u16;
            self.rates[status as usize] = basis_points;
        }
    }

    #[inline(always)]
    pub fn should_sample(&self, status: u16) -> bool {
        if status >= 600 {
            return false;
        }
        let threshold = self.rates[status as usize];
        if threshold == 0 {
            return false;
        }
        if threshold >= 10_000 {
            return true;
        }
        fast_rand_10k() < threshold
    }
}
