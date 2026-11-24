//! Pure-Integer Quota Window Arithmetic (`api_metering::window`).
//!
//! ```text
//! InlineWindowSlot (24B POD: a, b, c)
//!   ├── FixedWindow   : epoch = now_sec / window_secs  | a=epoch, b=count
//!   ├── SlidingWindow : bp = (elapsed * 10000) / secs  | a=epoch, b=curr, c=prev
//!   └── TokenBucket   : refill = (dt_ns * limit) / sec | a=last_ns, b=nano_tokens
//! ```

use super::arena::{InlineWindowSlot, MAX_WINDOWS_PER_TIER};
use super::config::{CompiledWindow, QuotaAlgorithm, QuotaWindowConfig};

/// Fixed-point token scaling (1 request token = 1,000,000,000 nano-tokens).
pub(crate) const NANO_TOKENS_PER_UNIT: u64 = 1_000_000_000;

#[inline(always)]
pub(crate) fn init_inline_windows(
    slots: &mut [InlineWindowSlot; MAX_WINDOWS_PER_TIER],
    windows: &[CompiledWindow],
    now_sec: u64,
    elapsed_ns: u64,
) {
    for (slot, w) in slots.iter_mut().zip(windows.iter()) {
        let cfg = &w.cfg;
        *slot = match cfg.algorithm {
            QuotaAlgorithm::SlidingWindow | QuotaAlgorithm::FixedWindow => {
                let epoch = now_sec.checked_div(cfg.window_seconds).unwrap_or(now_sec);
                InlineWindowSlot {
                    a: epoch,
                    b: 0,
                    c: 0,
                }
            }
            QuotaAlgorithm::TokenBucket => InlineWindowSlot {
                a: elapsed_ns,
                b: cfg.limit.saturating_mul(NANO_TOKENS_PER_UNIT),
                c: 0,
            },
        };
    }
}

#[inline(always)]
fn sliding_current_weight_bp(elapsed_in_window: u64, window_seconds: u64) -> u64 {
    if elapsed_in_window <= u64::MAX / 10_000 {
        (elapsed_in_window * 10_000) / window_seconds
    } else {
        ((elapsed_in_window as u128).saturating_mul(10_000) / (window_seconds as u128)) as u64
    }
}

pub(crate) fn peek_node_window_capacity(
    slot: &mut InlineWindowSlot,
    cfg: &QuotaWindowConfig,
    now_sec: u64,
    elapsed_ns: u64,
    grace_rem: &mut u32,
    grace_limit: u64,
) -> (bool, u64, u64) {
    if cfg.window_seconds == 0 {
        return (true, cfg.limit, 0);
    }

    match cfg.algorithm {
        QuotaAlgorithm::SlidingWindow => {
            let actual_epoch = now_sec / cfg.window_seconds;
            if actual_epoch > slot.a {
                let steps = actual_epoch - slot.a;
                slot.c = if steps == 1 { slot.b } else { 0 };
                slot.b = 0;
                slot.a = actual_epoch;
                *grace_rem = grace_limit.min(u32::MAX as u64) as u32;
            }
            let elapsed_in_window = now_sec % cfg.window_seconds;
            let current_weight_bp =
                sliding_current_weight_bp(elapsed_in_window, cfg.window_seconds);
            let prev_weight_bp = 10_000u64.saturating_sub(current_weight_bp);
            let estimated_prev =
                ((slot.c as u128).saturating_mul(prev_weight_bp as u128) / 10_000) as u64;
            let total_estimated = slot.b.saturating_add(estimated_prev);
            let reset_seconds = cfg.window_seconds.saturating_sub(elapsed_in_window).max(1);

            if total_estimated < cfg.limit {
                (
                    true,
                    cfg.limit.saturating_sub(total_estimated.saturating_add(1)),
                    reset_seconds,
                )
            } else if *grace_rem > 0 {
                (true, 0, reset_seconds)
            } else {
                (false, 0, reset_seconds)
            }
        }
        QuotaAlgorithm::FixedWindow => {
            let actual_epoch = now_sec / cfg.window_seconds;
            if actual_epoch > slot.a {
                slot.b = 0;
                slot.a = actual_epoch;
                *grace_rem = grace_limit.min(u32::MAX as u64) as u32;
            }
            let elapsed_in_window = now_sec % cfg.window_seconds;
            let reset_seconds = cfg.window_seconds.saturating_sub(elapsed_in_window).max(1);

            if slot.b < cfg.limit {
                (
                    true,
                    cfg.limit.saturating_sub(slot.b.saturating_add(1)),
                    reset_seconds,
                )
            } else if *grace_rem > 0 {
                (true, 0, reset_seconds)
            } else {
                (false, 0, reset_seconds)
            }
        }
        QuotaAlgorithm::TokenBucket => {
            let delta_ns = elapsed_ns.saturating_sub(slot.a) as u128;
            let window_secs = cfg.window_seconds.max(1) as u128;
            let max_nano = cfg.limit.saturating_mul(NANO_TOKENS_PER_UNIT);
            let refill_nano_u128 = delta_ns.saturating_mul(cfg.limit as u128) / window_secs;
            if refill_nano_u128 > 0 {
                let refill_nano = refill_nano_u128.min(u64::MAX as u128) as u64;
                slot.b = slot.b.saturating_add(refill_nano).min(max_nano);
                slot.a = elapsed_ns;
            }
            if slot.b >= NANO_TOKENS_PER_UNIT {
                (
                    true,
                    (slot.b - NANO_TOKENS_PER_UNIT) / NANO_TOKENS_PER_UNIT,
                    0,
                )
            } else if *grace_rem > 0 {
                (true, 0, 0)
            } else {
                let needed_nano = NANO_TOKENS_PER_UNIT.saturating_sub(slot.b);
                let wait_secs = if cfg.limit > 0 {
                    let denom = (cfg.limit as u128).saturating_mul(NANO_TOKENS_PER_UNIT as u128);
                    ((needed_nano as u128)
                        .saturating_mul(window_secs)
                        .div_ceil(denom)
                        .min(u64::MAX as u128) as u64)
                        .max(1)
                } else {
                    cfg.window_seconds
                };
                (false, 0, wait_secs)
            }
        }
    }
}

pub(crate) fn apply_node_window_lease(
    slot: &mut InlineWindowSlot,
    cfg: &QuotaWindowConfig,
    now_sec: u64,
    _elapsed_ns: u64,
    count: u64,
    grace_rem: &mut u32,
) -> (bool, u64) {
    if cfg.window_seconds == 0 {
        return (true, cfg.limit);
    }

    match cfg.algorithm {
        QuotaAlgorithm::SlidingWindow => {
            let elapsed_in_window = now_sec % cfg.window_seconds;
            let current_weight_bp =
                sliding_current_weight_bp(elapsed_in_window, cfg.window_seconds);
            let prev_weight_bp = 10_000u64.saturating_sub(current_weight_bp);
            let estimated_prev =
                ((slot.c as u128).saturating_mul(prev_weight_bp as u128) / 10_000) as u64;
            let before = slot.b.saturating_add(estimated_prev);
            if before >= cfg.limit && *grace_rem > 0 {
                *grace_rem = grace_rem.saturating_sub(1);
            }
            slot.b = slot.b.saturating_add(count);
            let after = slot.b.saturating_add(estimated_prev);
            (true, cfg.limit.saturating_sub(after))
        }
        QuotaAlgorithm::FixedWindow => {
            if slot.b >= cfg.limit && *grace_rem > 0 {
                *grace_rem = grace_rem.saturating_sub(1);
            }
            slot.b = slot.b.saturating_add(count);
            (true, cfg.limit.saturating_sub(slot.b))
        }
        QuotaAlgorithm::TokenBucket => {
            let cost_nano = count.saturating_mul(NANO_TOKENS_PER_UNIT);
            if slot.b < cost_nano && *grace_rem > 0 {
                *grace_rem = grace_rem.saturating_sub(1);
            }
            slot.b = slot.b.saturating_sub(cost_nano);
            (true, slot.b / NANO_TOKENS_PER_UNIT)
        }
    }
}
