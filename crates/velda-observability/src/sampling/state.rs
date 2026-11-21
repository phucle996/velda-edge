//! Thread-local Weyl sequence sampler state for zero-hash hot-path decisions.

use crate::worker_tls::WORKER_TLS;

/// 64-bit golden ratio fractional constant (`floor(2^64 / phi)`), odd so the Weyl sequence
/// has full period `2^64` and is equidistributed across every arithmetic subsequence.
pub const GOLDEN: u64 = 0x9E37_79B9_7F4A_7C15;

/// Fixed-point threshold representing probability `1.0` (always keep, since `(c >> 1) < 2^63`).
pub const THRESHOLD_ALWAYS: u64 = 1u64 << 63;

/// Seeds the calling worker thread's sampler state once at initialization so parallel
/// cores sample out of lockstep phase.
#[inline]
pub fn seed_worker(lane: usize) {
    WORKER_TLS.with(|tls| tls.weyl.set((lane as u64).wrapping_mul(GOLDEN)));
}

/// Evaluates whether the current event should be sampled against a pre-baked fixed-point
/// threshold in `[0, THRESHOLD_ALWAYS]` using 3 ALU instructions (`add`, `shr`, `cmp`).
#[inline(always)]
pub fn keep_threshold(threshold: u64) -> bool {
    WORKER_TLS.with(|tls| {
        let c = tls.weyl.get().wrapping_add(GOLDEN);
        tls.weyl.set(c);
        (c >> 1) < threshold
    })
}

/// Converts a validated sampling probability in `0.0..=1.0` to a fixed-point `u64` threshold.
///
/// Non-finite (`NaN`) or `>= 1.0` values clamp fail-open to [`THRESHOLD_ALWAYS`]; `<= 0.0` clamps to `0`.
#[inline]
pub fn rate_to_threshold(rate: f64) -> u64 {
    if rate.is_nan() || rate >= 1.0 {
        return THRESHOLD_ALWAYS;
    }
    if rate <= 0.0 {
        return 0;
    }
    ((rate * (THRESHOLD_ALWAYS as f64)).round() as u64).min(THRESHOLD_ALWAYS)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rate_boundaries_and_clamping() {
        assert_eq!(rate_to_threshold(0.0), 0);
        assert_eq!(rate_to_threshold(-0.5), 0);
        assert_eq!(rate_to_threshold(1.0), THRESHOLD_ALWAYS);
        assert_eq!(rate_to_threshold(1.5), THRESHOLD_ALWAYS);
        assert_eq!(rate_to_threshold(f64::NAN), THRESHOLD_ALWAYS);
        assert_eq!(rate_to_threshold(f64::INFINITY), THRESHOLD_ALWAYS);
        assert_eq!(rate_to_threshold(f64::NEG_INFINITY), 0);
    }

    #[test]
    fn exact_never_and_always() {
        seed_worker(0);
        for _ in 0..100_000 {
            assert!(!keep_threshold(0));
        }
        for _ in 0..100_000 {
            assert!(keep_threshold(THRESHOLD_ALWAYS));
        }
    }

    #[test]
    fn empirical_rates_and_alternating_listeners() {
        seed_worker(3);
        let total = 2_000_000u64;
        for &p in &[0.5_f64, 0.333333333, 0.1, 0.01, 0.001] {
            let thr = rate_to_threshold(p);
            let mut kept_a = 0u64;
            let mut kept_b = 0u64;
            for i in 0..total {
                let k = keep_threshold(thr) as u64;
                if i & 1 == 0 {
                    kept_a += k;
                } else {
                    kept_b += k;
                }
            }
            let rate_a = kept_a as f64 / (total / 2) as f64;
            let rate_b = kept_b as f64 / (total / 2) as f64;
            let tol = (p * 0.05).max(1e-4);
            assert!(
                (rate_a - p).abs() <= tol,
                "listener A drifted for p={p}: got {rate_a}"
            );
            assert!(
                (rate_b - p).abs() <= tol,
                "listener B drifted for p={p}: got {rate_b}"
            );
        }
    }
}
