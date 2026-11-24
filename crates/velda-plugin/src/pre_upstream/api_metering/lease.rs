//! Per-Core Lease Escrow & Node-Global Refill (`api_metering::lease`).
//!
//! ```text
//! LocalAccountRecord (L1/L2) ──► local_lease_covers_request()?
//!   ├── Hit  ─► compute_local_hit_preview() -> commit_local_deduction() (0 Node locks)
//!   └── Miss ─► #[cold] refill_from_node_shard()
//!                 ├─ flush local_credits_spent_unflushed -> Node AccountState
//!                 ├─ wallet < cost? -> try grace_credits -> else Passthrough | Exhausted
//!                 └─ windows OK?    -> adaptive batch (1 if <256, else avail/128 <=32)
//! ```

use super::arena::{AccountState, LocalAccountRecord, ShardArena};
use super::config::{CompiledTier, CompiledWindow, ExhaustedAction, QuotaAlgorithm};
use super::window::{apply_node_window_lease, init_inline_windows, peek_node_window_capacity};

pub(crate) enum LevelLeaseOutcome {
    Ready {
        in_overage: bool,
        min_remaining: u64,
        min_window_idx: Option<usize>,
        balance_after: i64,
    },
    Exhausted {
        action: ExhaustedAction,
        window_idx: Option<usize>,
        reset_seconds: u64,
    },
}

#[derive(Clone, Copy)]
pub(crate) struct PendingLevelLease {
    pub(crate) record_idx: usize,
    pub(crate) min_remaining: u64,
    pub(crate) min_window_idx: Option<usize>,
    pub(crate) balance_after: i64,
    pub(crate) in_overage: bool,
}

pub(crate) struct RefillContext<'a> {
    pub(crate) tier: &'a CompiledTier,
    pub(crate) windows: &'a [CompiledWindow],
    pub(crate) initial_balance_micro: i64,
    pub(crate) now_sec: u64,
    pub(crate) elapsed_ns: u64,
    pub(crate) global_reconcile_epoch: u32,
    pub(crate) inactivity_ttl_sec: u64,
}

#[inline(always)]
pub(crate) fn local_lease_covers_request(
    rec: &LocalAccountRecord,
    windows: &[CompiledWindow],
    cost: i64,
    now_sec: u64,
) -> bool {
    if cost > 0 && rec.local_credits_lease < cost {
        return false;
    }
    for (i, w) in windows.iter().enumerate() {
        if matches!(w.cfg.algorithm, QuotaAlgorithm::TokenBucket) && rec.local_window_lease[i] == 0
        {
            return false;
        }
        let expected_epoch = now_sec.checked_div(w.cfg.window_seconds).unwrap_or(0);
        if rec.local_window_epoch[i] != expected_epoch || rec.local_window_lease[i] == 0 {
            return false;
        }
    }
    true
}

#[inline(always)]
pub(crate) fn compute_local_hit_preview(
    rec: &LocalAccountRecord,
    windows: &[CompiledWindow],
    cost: i64,
) -> LevelLeaseOutcome {
    let mut min_rem = u64::MAX;
    let mut min_idx = None;
    for (i, _) in windows.iter().enumerate() {
        let rem = (rec.local_window_rem_hint[i] as u64).saturating_sub(1);
        if min_idx.is_none() || rem < min_rem {
            min_rem = rem;
            min_idx = Some(i);
        }
    }
    LevelLeaseOutcome::Ready {
        in_overage: false,
        min_remaining: if min_idx.is_some() { min_rem } else { 0 },
        min_window_idx: min_idx,
        balance_after: rec.local_balance_snapshot.saturating_sub(cost),
    }
}

#[inline(always)]
pub(crate) fn commit_local_deduction(
    rec: &mut LocalAccountRecord,
    windows: &[CompiledWindow],
    cost: i64,
) {
    if cost > 0 {
        rec.local_credits_lease = rec.local_credits_lease.saturating_sub(cost);
        rec.local_balance_snapshot = rec.local_balance_snapshot.saturating_sub(cost);
        rec.local_credits_spent_unflushed = rec.local_credits_spent_unflushed.saturating_add(cost);
    }
    for (i, _) in windows.iter().enumerate() {
        rec.local_window_lease[i] = rec.local_window_lease[i].saturating_sub(1);
        rec.local_window_rem_hint[i] = rec.local_window_rem_hint[i].saturating_sub(1);
    }
}

#[cold]
#[inline(never)]
pub(crate) fn refill_from_node_shard(
    shard: &mut ShardArena,
    rec: &mut LocalAccountRecord,
    is_new_local: bool,
    ctx: &RefillContext<'_>,
) -> LevelLeaseOutcome {
    let tier = ctx.tier;
    let windows = ctx.windows;
    let now_sec = ctx.now_sec;
    let elapsed_ns = ctx.elapsed_ns;
    let global_reconcile_epoch = ctx.global_reconcile_epoch;

    let (node, is_new_node) =
        shard.get_or_insert_slot(rec.raw_hash, ctx.inactivity_ttl_sec, now_sec);

    if is_new_node || node.tier_fingerprint != tier.tier_fingerprint {
        if is_new_node {
            node.balance_micro_credits = ctx.initial_balance_micro;
            node.delta_micro_credits_deducted = 0;
            node.in_flight_delta = 0;
            node.reconcile_epoch = global_reconcile_epoch;
        }
        node.tier_fingerprint = tier.tier_fingerprint;
        node.window_count = windows.len() as u8;
        node.grace_credits_remaining = if tier.node_grace_requests > 0 && tier.cost_i64 > 0 {
            (tier.node_grace_requests as i64).saturating_mul(tier.cost_i64)
        } else {
            0
        };
        init_inline_windows(&mut node.windows, windows, now_sec, elapsed_ns);
        for (g, w) in node.grace_window_remaining.iter_mut().zip(windows.iter()) {
            *g = w.node_grace_limit.min(u32::MAX as u64) as u32;
        }
    }
    node.last_access_sec = now_sec;

    if !is_new_local && rec.local_credits_spent_unflushed != 0 {
        let spent = rec.local_credits_spent_unflushed;
        node.balance_micro_credits = node.balance_micro_credits.saturating_sub(spent);
        node.delta_micro_credits_deducted = node.delta_micro_credits_deducted.saturating_add(spent);
        rec.local_credits_spent_unflushed = 0;
    }
    if is_new_local || rec.reconcile_epoch != global_reconcile_epoch {
        rec.local_credits_lease = 0;
        rec.local_credits_spent_unflushed = 0;
        rec.local_balance_snapshot = node.balance_micro_credits;
        rec.reconcile_epoch = global_reconcile_epoch;
    }
    rec.tier_fingerprint = tier.tier_fingerprint;
    rec.window_count = windows.len() as u8;

    // 1. Wallet check (with bounded node grace when enabled)
    let cost = tier.cost_i64;
    if cost > 0 && node.balance_micro_credits < cost {
        if node.grace_credits_remaining >= cost {
            node.grace_credits_remaining = node.grace_credits_remaining.saturating_sub(cost);
            if let Err((win_idx, reset)) =
                ensure_node_windows_for_single_req(node, windows, now_sec, elapsed_ns, true)
            {
                if matches!(tier.exhausted_action, ExhaustedAction::Passthrough) {
                    return grant_overage_single(rec, node, windows, cost, now_sec, elapsed_ns);
                }
                return LevelLeaseOutcome::Exhausted {
                    action: tier.exhausted_action,
                    window_idx: Some(win_idx),
                    reset_seconds: reset,
                };
            }
            rec.local_credits_lease = cost;
            rec.local_balance_snapshot = node.balance_micro_credits;
            sync_local_single_window_grant(rec, windows, now_sec);
            return LevelLeaseOutcome::Ready {
                in_overage: false,
                min_remaining: 0,
                min_window_idx: if windows.is_empty() { None } else { Some(0) },
                balance_after: node.balance_micro_credits.saturating_sub(cost),
            };
        }
        if matches!(tier.exhausted_action, ExhaustedAction::Passthrough) {
            return grant_overage_single(rec, node, windows, cost, now_sec, elapsed_ns);
        }
        return LevelLeaseOutcome::Exhausted {
            action: tier.exhausted_action,
            window_idx: None,
            reset_seconds: 0,
        };
    }

    // 2. Window capacity check & adaptive micro-lease grant
    let mut min_win_avail = u64::MAX;
    let mut min_win_idx = 0usize;
    let mut hit_window_shortfall = None;

    for (i, ((slot, w), grace_rem)) in node
        .windows
        .iter_mut()
        .zip(windows.iter())
        .zip(node.grace_window_remaining.iter_mut())
        .enumerate()
    {
        let (allowed, rem_after, reset_secs) = peek_node_window_capacity(
            slot,
            &w.cfg,
            now_sec,
            elapsed_ns,
            grace_rem,
            w.node_grace_limit,
        );
        if !allowed {
            hit_window_shortfall = Some((i, reset_secs));
            break;
        }
        let avail_including_current = rem_after.saturating_add(1);
        if avail_including_current < min_win_avail {
            min_win_avail = avail_including_current;
            min_win_idx = i;
        }
    }

    if let Some((widx, reset_secs)) = hit_window_shortfall {
        if matches!(tier.exhausted_action, ExhaustedAction::Passthrough) {
            return grant_overage_single(rec, node, windows, cost, now_sec, elapsed_ns);
        }
        return LevelLeaseOutcome::Exhausted {
            action: tier.exhausted_action,
            window_idx: Some(widx),
            reset_seconds: reset_secs,
        };
    }

    let bal_reqs = if cost > 0 {
        (node.balance_micro_credits / cost).max(0) as u64
    } else {
        u64::MAX
    };
    let avail_reqs = min_win_avail.min(bal_reqs);
    let lease_reqs = if tier.has_token_bucket || avail_reqs < 256 {
        1
    } else {
        (avail_reqs / 128).clamp(1, 32)
    };

    let mut min_rem_after_first = u64::MAX;
    for (((((slot, w), grace_rem), local_lease), local_epoch), local_hint) in node
        .windows
        .iter_mut()
        .zip(windows.iter())
        .zip(node.grace_window_remaining.iter_mut())
        .zip(rec.local_window_lease.iter_mut())
        .zip(rec.local_window_epoch.iter_mut())
        .zip(rec.local_window_rem_hint.iter_mut())
    {
        let (_, rem_after_batch) =
            apply_node_window_lease(slot, &w.cfg, now_sec, elapsed_ns, lease_reqs, grace_rem);
        let epoch = now_sec.checked_div(w.cfg.window_seconds).unwrap_or(0);
        let rem_before_commit = rem_after_batch.saturating_add(lease_reqs);
        *local_lease = lease_reqs.min(u32::MAX as u64) as u32;
        *local_epoch = epoch;
        *local_hint = rem_before_commit.min(u32::MAX as u64) as u32;
        let after_first = rem_before_commit.saturating_sub(1);
        if after_first < min_rem_after_first {
            min_rem_after_first = after_first;
        }
    }

    rec.local_credits_lease = if cost > 0 {
        cost.saturating_mul(lease_reqs as i64)
    } else {
        0
    };
    rec.local_balance_snapshot = node.balance_micro_credits;

    LevelLeaseOutcome::Ready {
        in_overage: false,
        min_remaining: if windows.is_empty() {
            0
        } else {
            min_rem_after_first
        },
        min_window_idx: if windows.is_empty() {
            None
        } else {
            Some(min_win_idx)
        },
        balance_after: node.balance_micro_credits.saturating_sub(cost),
    }
}

#[cold]
#[inline(never)]
fn grant_overage_single(
    rec: &mut LocalAccountRecord,
    node: &mut AccountState,
    windows: &[CompiledWindow],
    cost: i64,
    now_sec: u64,
    elapsed_ns: u64,
) -> LevelLeaseOutcome {
    for ((slot, w), grace_rem) in node
        .windows
        .iter_mut()
        .zip(windows.iter())
        .zip(node.grace_window_remaining.iter_mut())
    {
        apply_node_window_lease(slot, &w.cfg, now_sec, elapsed_ns, 1, grace_rem);
    }
    rec.local_credits_lease = cost;
    rec.local_balance_snapshot = node.balance_micro_credits;
    sync_local_single_window_grant(rec, windows, now_sec);
    LevelLeaseOutcome::Ready {
        in_overage: true,
        min_remaining: 0,
        min_window_idx: if windows.is_empty() { None } else { Some(0) },
        balance_after: node.balance_micro_credits.saturating_sub(cost),
    }
}

#[inline(always)]
fn sync_local_single_window_grant(
    rec: &mut LocalAccountRecord,
    windows: &[CompiledWindow],
    now_sec: u64,
) {
    for (i, w) in windows.iter().enumerate() {
        rec.local_window_lease[i] = 1;
        rec.local_window_epoch[i] = now_sec.checked_div(w.cfg.window_seconds).unwrap_or(0);
        rec.local_window_rem_hint[i] = 1;
    }
}

fn ensure_node_windows_for_single_req(
    node: &mut AccountState,
    windows: &[CompiledWindow],
    now_sec: u64,
    elapsed_ns: u64,
    consume: bool,
) -> Result<(), (usize, u64)> {
    for (i, (slot, w)) in node.windows.iter_mut().zip(windows.iter()).enumerate() {
        let (allowed, _, reset) = peek_node_window_capacity(
            slot,
            &w.cfg,
            now_sec,
            elapsed_ns,
            &mut node.grace_window_remaining[i],
            w.node_grace_limit,
        );
        if !allowed {
            return Err((i, reset));
        }
    }
    if consume {
        for (i, (slot, w)) in node.windows.iter_mut().zip(windows.iter()).enumerate() {
            apply_node_window_lease(
                slot,
                &w.cfg,
                now_sec,
                elapsed_ns,
                1,
                &mut node.grace_window_remaining[i],
            );
        }
    }
    Ok(())
}
