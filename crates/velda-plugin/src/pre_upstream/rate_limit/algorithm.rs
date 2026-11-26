//! Fixed-point rate-limiting accounting kernels (`TokenBucket`, `SlidingWindow`, `FixedWindow`)
//! with monotonic nanosecond/millisecond progression and bounded soft overdraft grace.
//!
//! ```text
//!   BucketState + Request Consume(now_ms, count)
//!         │
//!         ├─► Hard limit  : burst_capacity
//!         ├─► Grace limit : burst_capacity + (burst_capacity * overdraft_percent / 100)
//!         │
//!         ├─► total_consumed <= burst_capacity   ──► Allow { remaining = burst - total, in_grace: false }
//!         ├─► total_consumed <= grace_limit      ──► Allow { remaining = 0,             in_grace: true  }
//!         └─► total_consumed >  grace_limit      ──► Reject { retry_after_secs }
//! ```

use crate::pre_upstream::rate_limit::validation::RateLimitAlgorithm;

/// Millitoken scale (`1 token = 1000`) for sub-second integer refill accuracy without `f64` drift.
pub const TOKEN_SCALE: u64 = 1000;

/// Decision returned by deterministic local state evaluation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RateDecision {
    pub allowed: bool,
    pub remaining: u64,
    pub in_overdraft: bool,
    pub retry_after_secs: u64,
}

/// Computes the inclusive maximum capacity including configured bounded overdraft grace.
#[inline]
pub const fn effective_capacity_with_grace(burst_capacity: u64, overdraft_percent: u8) -> u64 {
    let grace = burst_capacity.saturating_mul(overdraft_percent as u64) / 100;
    burst_capacity.saturating_add(grace)
}

/// Evaluates and mutates a single bucket's local+cluster consumption state.
///
/// Arguments:
/// - `algo`: enforcement model (`TokenBucket`, `SlidingWindow`, `FixedWindow`).
/// - `rate_per_second`, `burst_capacity`, `overdraft_percent`: configured limits.
/// - `now_ms`: monotonically non-decreasing milliseconds since process-local epoch.
/// - `requested`: tokens/requests requested in this batch (typically `1`, or up to `lease_batch`).
/// - `bucket`: mutable bucket counters (`state_val`, `prev_val`, `last_ms`, `epoch_sec`, `cluster_other_used`).
#[inline]
#[allow(clippy::too_many_arguments)]
pub fn evaluate_bucket_consume(
    algo: RateLimitAlgorithm,
    rate_per_second: u64,
    burst_capacity: u64,
    overdraft_percent: u8,
    now_ms: u64,
    requested: u64,
    bucket: &mut BucketAccountingState,
) -> (RateDecision, u64) {
    let hard_cap = burst_capacity.max(1);
    let eff_cap = effective_capacity_with_grace(hard_cap, overdraft_percent);

    match algo {
        RateLimitAlgorithm::TokenBucket => {
            let rate = rate_per_second.max(1);
            let elapsed_ms = now_ms.saturating_sub(bucket.last_ms);
            if elapsed_ms > 0 {
                let refill_milli = elapsed_ms.saturating_mul(rate);
                let max_milli = hard_cap.saturating_mul(TOKEN_SCALE);
                // Saturate back towards zero debt; unused tokens accumulate up to max_milli,
                // represented cleanly as `used_milli` starting from 0 when untouched.
                bucket.used_milli = bucket.used_milli.saturating_sub(refill_milli);
                let _ = max_milli;
                bucket.last_ms = now_ms;
            }

            let effective_used_tokens = bucket
                .used_milli
                .div_ceil(TOKEN_SCALE)
                .saturating_add(bucket.cluster_other_used);
            let avail_grace_tokens = eff_cap.saturating_sub(effective_used_tokens);
            if avail_grace_tokens == 0 {
                return (
                    RateDecision {
                        allowed: false,
                        remaining: 0,
                        in_overdraft: true,
                        retry_after_secs: 1,
                    },
                    0,
                );
            }

            let granted = requested.min(avail_grace_tokens).max(1);
            bucket.used_milli = bucket
                .used_milli
                .saturating_add(granted.saturating_mul(TOKEN_SCALE));

            let after_used = bucket
                .used_milli
                .div_ceil(TOKEN_SCALE)
                .saturating_add(bucket.cluster_other_used);
            let remaining = hard_cap.saturating_sub(after_used);
            let in_overdraft = after_used > hard_cap;

            (
                RateDecision {
                    allowed: true,
                    remaining,
                    in_overdraft,
                    retry_after_secs: 0,
                },
                granted,
            )
        }
        RateLimitAlgorithm::FixedWindow => {
            let epoch = now_ms / 1000;
            if bucket.epoch_sec != epoch {
                bucket.epoch_sec = epoch;
                bucket.used_milli = 0;
                bucket.prev_window_count = 0;
                bucket.cluster_other_used = 0;
                bucket.last_ms = now_ms;
            }
            let current_used =
                (bucket.used_milli / TOKEN_SCALE).saturating_add(bucket.cluster_other_used);
            let avail = eff_cap.saturating_sub(current_used);
            if avail == 0 {
                return (
                    RateDecision {
                        allowed: false,
                        remaining: 0,
                        in_overdraft: true,
                        retry_after_secs: 1,
                    },
                    0,
                );
            }

            let granted = requested.min(avail).max(1);
            bucket.used_milli = bucket
                .used_milli
                .saturating_add(granted.saturating_mul(TOKEN_SCALE));

            let after_used =
                (bucket.used_milli / TOKEN_SCALE).saturating_add(bucket.cluster_other_used);
            let remaining = hard_cap.saturating_sub(after_used);
            let in_overdraft = after_used > hard_cap;

            (
                RateDecision {
                    allowed: true,
                    remaining,
                    in_overdraft,
                    retry_after_secs: 0,
                },
                granted,
            )
        }
        RateLimitAlgorithm::SlidingWindow => {
            let epoch = now_ms / 1000;
            let offset_ms = now_ms % 1000;
            if bucket.epoch_sec < epoch {
                if epoch - bucket.epoch_sec == 1 {
                    bucket.prev_window_count = bucket.used_milli / TOKEN_SCALE;
                } else {
                    bucket.prev_window_count = 0;
                }
                bucket.used_milli = 0;
                bucket.cluster_other_used = 0;
                bucket.epoch_sec = epoch;
                bucket.last_ms = now_ms;
            }

            let prev_weight = 1000u64.saturating_sub(offset_ms);
            let weighted_prev = bucket.prev_window_count.saturating_mul(prev_weight) / 1000;
            let current_used = (bucket.used_milli / TOKEN_SCALE)
                .saturating_add(weighted_prev)
                .saturating_add(bucket.cluster_other_used);

            let avail = eff_cap.saturating_sub(current_used);
            if avail == 0 {
                return (
                    RateDecision {
                        allowed: false,
                        remaining: 0,
                        in_overdraft: true,
                        retry_after_secs: 1,
                    },
                    0,
                );
            }

            let granted = requested.min(avail).max(1);
            bucket.used_milli = bucket
                .used_milli
                .saturating_add(granted.saturating_mul(TOKEN_SCALE));

            let after_used = (bucket.used_milli / TOKEN_SCALE)
                .saturating_add(weighted_prev)
                .saturating_add(bucket.cluster_other_used);
            let remaining = hard_cap.saturating_sub(after_used);
            let in_overdraft = after_used > hard_cap;

            (
                RateDecision {
                    allowed: true,
                    remaining,
                    in_overdraft,
                    retry_after_secs: 0,
                },
                granted,
            )
        }
    }
}

/// Compact per-key accounting registers updated under shard isolation.
#[derive(Debug, Clone, Copy, Default)]
pub struct BucketAccountingState {
    pub used_milli: u64,
    pub prev_window_count: u64,
    pub last_ms: u64,
    pub epoch_sec: u64,
    pub cluster_other_used: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_token_bucket_overdraft_grace_allows_small_spike_then_blocks() {
        let mut st = BucketAccountingState {
            last_ms: 10_000,
            ..Default::default()
        };
        // burst=10, overdraft=20% => hard=10, grace=2, effective=12
        for i in 0..10 {
            let (dec, granted) = evaluate_bucket_consume(
                RateLimitAlgorithm::TokenBucket,
                10,
                10,
                20,
                10_000,
                1,
                &mut st,
            );
            assert!(dec.allowed, "request {i} should pass");
            assert!(!dec.in_overdraft);
            assert_eq!(granted, 1);
            assert_eq!(dec.remaining, (9 - i) as u64);
        }
        // Requests 11 & 12 hit bounded overdraft grace
        for _ in 0..2 {
            let (dec, granted) = evaluate_bucket_consume(
                RateLimitAlgorithm::TokenBucket,
                10,
                10,
                20,
                10_000,
                1,
                &mut st,
            );
            assert!(dec.allowed);
            assert!(dec.in_overdraft);
            assert_eq!(dec.remaining, 0);
            assert_eq!(granted, 1);
        }
        // Request 13 is blocked
        let (dec, granted) = evaluate_bucket_consume(
            RateLimitAlgorithm::TokenBucket,
            10,
            10,
            20,
            10_000,
            1,
            &mut st,
        );
        assert!(!dec.allowed);
        assert_eq!(granted, 0);
        assert_eq!(dec.retry_after_secs, 1);
    }

    #[test]
    fn test_sliding_window_rolls_previous_weight_monotonically() {
        let mut st = BucketAccountingState {
            last_ms: 5_000,
            epoch_sec: 5,
            ..Default::default()
        };
        for _ in 0..10 {
            let (d, _) = evaluate_bucket_consume(
                RateLimitAlgorithm::SlidingWindow,
                10,
                10,
                0,
                5_100,
                1,
                &mut st,
            );
            assert!(d.allowed);
        }
        // At t = 6_500 (50% into next second): prev weight = 5 (50% of 10), so only 5 left
        for _ in 0..5 {
            let (d, _) = evaluate_bucket_consume(
                RateLimitAlgorithm::SlidingWindow,
                10,
                10,
                0,
                6_500,
                1,
                &mut st,
            );
            assert!(d.allowed);
        }
        let (blocked, _) = evaluate_bucket_consume(
            RateLimitAlgorithm::SlidingWindow,
            10,
            10,
            0,
            6_500,
            1,
            &mut st,
        );
        assert!(!blocked.allowed);
    }
}
