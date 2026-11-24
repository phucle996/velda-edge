//! Hierarchical Metering Orchestrator (`api_metering::algorithm`).
//!
//! ```text
//! evaluate_chain_compiled(chain: [L0, L1, L2], tier)
//!   ├─► Phase 1 (Check/Lease): Lane[core].levels[lvl] ─► hit? ─(yes)─► next level (if bit0==1)
//!   │                                                    └─(no)─► lease::refill_from_node_shard()
//!   └─► Phase 2 (Commit)     : commit_local_deduction() across verified levels [0..active]
//! ```

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;

use super::arena::{LaneArena, MAX_HIERARCHY_LEVELS, MAX_WINDOWS_PER_TIER, ShardArena};
use super::config::{CompiledTier, ExhaustedAction, TierConfig};
use super::key::{AccountKeyChain, pack_tagged_key};
use super::lease::{
    LevelLeaseOutcome, PendingLevelLease, RefillContext, commit_local_deduction,
    compute_local_hit_preview, local_lease_covers_request, refill_from_node_shard,
};

/// Scaling factor for micro-credits (1 credit = 1,000,000 micro-credits).
pub const MICRO_CREDITS_PER_UNIT: u64 = 1_000_000;

/// Number of independent cache-aligned Node-Global shards (power of 2).
const NUM_SHARDS: usize = 64;

/// Number of per-core lane arenas (power of 2).
const MAX_LANES: usize = 64;

/// Inactivity threshold for pruning stale account states (30 days in seconds).
const ACCOUNT_INACTIVITY_TTL_SEC: u64 = 30 * 86_400;

static NEXT_CORE_LANE: AtomicUsize = AtomicUsize::new(0);
thread_local! {
    static LOCAL_CORE_LANE: usize = NEXT_CORE_LANE.fetch_add(1, Ordering::Relaxed);
}

#[inline(always)]
fn current_core_lane() -> usize {
    LOCAL_CORE_LANE.with(|&id| id & (MAX_LANES - 1))
}

/// Zero-allocation hot-path decision returned by [`ShardedMeteringState::evaluate_compiled`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompiledDecision {
    Allowed {
        min_remaining: u64,
        min_window_idx: Option<usize>,
        balance_micro_credits: i64,
    },
    Exhausted {
        action: ExhaustedAction,
        window_idx: Option<usize>,
        reset_seconds: u64,
    },
    OverageAllowed {
        negative_balance: i64,
    },
}

/// Decision output resulting from evaluating an account against its tier policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MeteringDecision {
    /// Request permitted.
    Allowed {
        min_remaining: u64,
        window_name: String,
        balance_micro_credits: i64,
    },
    /// Request rejected due to quota or credit exhaustion.
    Exhausted {
        action: ExhaustedAction,
        window_name: String,
        reset_seconds: u64,
    },
    /// Request permitted under overage / debt recording policy.
    OverageAllowed { negative_balance: i64 },
}

#[repr(align(64))]
struct CacheAlignedLane {
    arena: std::sync::Mutex<LaneArena>,
}

#[repr(align(64))]
struct CacheAlignedShard {
    arena: std::sync::Mutex<Option<ShardArena>>,
}

/// Thread-safe hierarchical metering state combining Per-Core Lane Arenas + Node-Global Shards.
pub struct ShardedMeteringState {
    start_instant: Instant,
    reconcile_gen: AtomicU64,
    lanes: Box<[CacheAlignedLane; MAX_LANES]>,
    shards: Box<[CacheAlignedShard; NUM_SHARDS]>,
}

impl Default for ShardedMeteringState {
    fn default() -> Self {
        Self::new()
    }
}

impl ShardedMeteringState {
    /// Creates a new hierarchical metering container (lazy per-lane & per-shard slab allocation).
    pub fn new() -> Self {
        let lanes: Vec<CacheAlignedLane> = (0..MAX_LANES)
            .map(|_| CacheAlignedLane {
                arena: std::sync::Mutex::new(LaneArena::new()),
            })
            .collect();
        let shards: Vec<CacheAlignedShard> = (0..NUM_SHARDS)
            .map(|_| CacheAlignedShard {
                arena: std::sync::Mutex::new(None),
            })
            .collect();

        Self {
            start_instant: Instant::now(),
            reconcile_gen: AtomicU64::new(0),
            lanes: lanes
                .into_boxed_slice()
                .try_into()
                .unwrap_or_else(|_| unreachable!()),
            shards: shards
                .into_boxed_slice()
                .try_into()
                .unwrap_or_else(|_| unreachable!()),
        }
    }

    #[inline(always)]
    pub fn start_instant(&self) -> Instant {
        self.start_instant
    }

    /// Evaluates a single-level key on the calling core's local lane.
    #[inline]
    pub fn evaluate_compiled(
        &self,
        account_hash: u64,
        tier: &CompiledTier,
        initial_balance_micro: i64,
        now_sec: u64,
        now_instant: Instant,
    ) -> CompiledDecision {
        let raw = account_hash.max(2);
        let chain = AccountKeyChain {
            tagged_keys: [pack_tagged_key(raw, false), 0, 0],
            raw_hashes: [raw, 0, 0],
            depth: 1,
        };
        self.evaluate_chain_compiled(&chain, tier, initial_balance_micro, now_sec, now_instant)
    }

    /// 2-Phase Check-Then-Commit across `chain.depth` hierarchy levels (`bit 0 = has_child`).
    #[inline]
    pub fn evaluate_chain_compiled(
        &self,
        chain: &AccountKeyChain,
        tier: &CompiledTier,
        initial_balance_micro: i64,
        now_sec: u64,
        now_instant: Instant,
    ) -> CompiledDecision {
        let windows_slice = tier
            .windows
            .get(..MAX_WINDOWS_PER_TIER)
            .unwrap_or(&tier.windows);
        let elapsed_ns = if tier.has_token_bucket {
            now_instant
                .saturating_duration_since(self.start_instant)
                .as_nanos()
                .min(u64::MAX as u128) as u64
        } else {
            0
        };
        let global_reconcile_epoch = self.reconcile_gen.load(Ordering::Acquire) as u32;
        let lane_idx = current_core_lane();
        let mut lane = self.lanes[lane_idx].arena.lock().unwrap();

        let depth = chain.depth.min(MAX_HIERARCHY_LEVELS);
        let mut pending = [PendingLevelLease {
            record_idx: 0,
            min_remaining: 0,
            min_window_idx: None,
            balance_after: 0,
            in_overage: false,
        }; MAX_HIERARCHY_LEVELS];
        let mut active_levels = 0usize;

        // Phase 1: Check & ensure local leases across Level 0 -> ... -> leaf (0 commits until all pass).
        for (lvl, slot_pending) in pending.iter_mut().enumerate().take(depth) {
            let tagged = chain.tagged_keys[lvl];
            let raw_hash = chain.raw_hashes[lvl];
            let level_arena = lane.level_mut(lvl);
            let (record_idx, rec, is_new) = level_arena.get_or_insert_local(tagged, raw_hash);

            let outcome = if !is_new
                && rec.tier_fingerprint == tier.tier_fingerprint
                && rec.reconcile_epoch == global_reconcile_epoch
                && local_lease_covers_request(rec, windows_slice, tier.cost_i64, now_sec)
            {
                compute_local_hit_preview(rec, windows_slice, tier.cost_i64)
            } else {
                let ctx = RefillContext {
                    tier,
                    windows: windows_slice,
                    initial_balance_micro,
                    now_sec,
                    elapsed_ns,
                    global_reconcile_epoch,
                    inactivity_ttl_sec: ACCOUNT_INACTIVITY_TTL_SEC,
                };
                let shard_idx = (rec.raw_hash as usize) & (NUM_SHARDS - 1);
                let mut shard_guard = self.shards[shard_idx].arena.lock().unwrap();
                let shard = shard_guard.get_or_insert_with(ShardArena::new);
                refill_from_node_shard(shard, rec, is_new, &ctx)
            };

            match outcome {
                LevelLeaseOutcome::Ready {
                    in_overage,
                    min_remaining,
                    min_window_idx,
                    balance_after,
                } => {
                    *slot_pending = PendingLevelLease {
                        record_idx,
                        min_remaining,
                        min_window_idx,
                        balance_after,
                        in_overage,
                    };
                    active_levels = lvl + 1;
                    // Follow bit 0 (`has_child`): stop immediately if leaf (`0`).
                    if (tagged & 1) == 0 {
                        break;
                    }
                }
                LevelLeaseOutcome::Exhausted {
                    action,
                    window_idx,
                    reset_seconds,
                } => {
                    return CompiledDecision::Exhausted {
                        action,
                        window_idx,
                        reset_seconds,
                    };
                }
            }
        }

        // Phase 2: Commit deduction across all verified levels atomically on local lane.
        let mut leaf_min_rem = u64::MAX;
        let mut leaf_min_win_idx = None;
        let mut leaf_balance = 0i64;
        let mut any_overage = false;

        for (lvl, p) in pending.iter().enumerate().take(active_levels) {
            if let Some(rec) = lane.level_mut(lvl).record_mut_by_idx(p.record_idx) {
                commit_local_deduction(rec, windows_slice, tier.cost_i64);
            }

            leaf_balance = p.balance_after;
            if p.in_overage {
                any_overage = true;
            }
            if let Some(widx) = p.min_window_idx
                && (leaf_min_win_idx.is_none() || p.min_remaining < leaf_min_rem)
            {
                leaf_min_rem = p.min_remaining;
                leaf_min_win_idx = Some(widx);
            }
        }

        if any_overage {
            CompiledDecision::OverageAllowed {
                negative_balance: leaf_balance,
            }
        } else {
            CompiledDecision::Allowed {
                min_remaining: if leaf_min_win_idx.is_some() {
                    leaf_min_rem
                } else {
                    0
                },
                min_window_idx: leaf_min_win_idx,
                balance_micro_credits: leaf_balance,
            }
        }
    }

    /// Evaluates account quotas and deducts cost atomically from an uncompiled [`TierConfig`].
    pub fn evaluate(
        &self,
        account_hash: u64,
        tier_cfg: &TierConfig,
        initial_balance_micro: i64,
        now_sec: u64,
        now_instant: Instant,
    ) -> MeteringDecision {
        let compiled = CompiledTier::compile(tier_cfg);
        match self.evaluate_compiled(
            account_hash,
            &compiled,
            initial_balance_micro,
            now_sec,
            now_instant,
        ) {
            CompiledDecision::Allowed {
                min_remaining,
                min_window_idx,
                balance_micro_credits,
            } => {
                let window_name = min_window_idx
                    .and_then(|idx| compiled.windows.get(idx))
                    .map(|w| w.cfg.name.clone())
                    .unwrap_or_default();
                MeteringDecision::Allowed {
                    min_remaining,
                    window_name,
                    balance_micro_credits,
                }
            }
            CompiledDecision::Exhausted {
                action,
                window_idx,
                reset_seconds,
            } => {
                let window_name = window_idx
                    .and_then(|idx| compiled.windows.get(idx))
                    .map(|w| w.cfg.name.clone())
                    .unwrap_or_else(|| "credit_balance".to_string());
                MeteringDecision::Exhausted {
                    action,
                    window_name,
                    reset_seconds,
                }
            }
            CompiledDecision::OverageAllowed { negative_balance } => {
                MeteringDecision::OverageAllowed { negative_balance }
            }
        }
    }

    /// Flushes uncommitted local lane deductions into NodeGlobal, then leases deltas for Redis sync.
    pub fn collect_deltas(&self) -> Vec<(u64, i64)> {
        for lane_cell in self.lanes.iter() {
            let mut lane = lane_cell.arena.lock().unwrap();
            for lvl_opt in lane.levels.iter_mut() {
                let Some(lvl) = lvl_opt.as_mut() else {
                    continue;
                };
                for rec in lvl.records_mut() {
                    if rec.tagged_key >= 2 && rec.local_credits_spent_unflushed != 0 {
                        let spent = rec.local_credits_spent_unflushed;
                        rec.local_credits_spent_unflushed = 0;
                        let shard_idx = (rec.raw_hash as usize) & (NUM_SHARDS - 1);
                        let mut shard_guard = self.shards[shard_idx].arena.lock().unwrap();
                        if let Some(shard) = shard_guard.as_mut()
                            && let Some(node) = shard.get_mut(rec.raw_hash)
                        {
                            node.balance_micro_credits =
                                node.balance_micro_credits.saturating_sub(spent);
                            node.delta_micro_credits_deducted =
                                node.delta_micro_credits_deducted.saturating_add(spent);
                        }
                    }
                }
            }
        }

        let mut batch = Vec::new();
        for shard_cell in self.shards.iter() {
            let mut shard_guard = shard_cell.arena.lock().unwrap();
            let Some(arena) = shard_guard.as_mut() else {
                continue;
            };
            for state in arena.records_mut() {
                if state.account_hash >= 2 && state.delta_micro_credits_deducted > 0 {
                    let delta = state.delta_micro_credits_deducted;
                    state.in_flight_delta = state.in_flight_delta.saturating_add(delta);
                    state.delta_micro_credits_deducted = 0;
                    batch.push((state.account_hash, delta));
                }
            }
        }
        batch
    }

    /// Restores `in_flight_delta` back to `delta_micro_credits_deducted` when a Redis sync fails.
    pub fn revert_in_flight_deltas(&self, failed: &[(u64, i64)]) {
        for &(acc_hash, delta) in failed {
            let key = acc_hash.max(2);
            let shard_idx = (key as usize) & (NUM_SHARDS - 1);
            let mut shard_guard = self.shards[shard_idx].arena.lock().unwrap();
            if let Some(arena) = shard_guard.as_mut()
                && let Some(state) = arena.get_mut(key)
            {
                let restore = state.in_flight_delta.min(delta);
                state.in_flight_delta = state.in_flight_delta.saturating_sub(restore);
                state.delta_micro_credits_deducted =
                    state.delta_micro_credits_deducted.saturating_add(restore);
            }
        }
    }

    /// Reconciles updated global balances returned from Redis while preserving uncommitted local deltas.
    pub fn reconcile_balances(&self, updates: &[(u64, i64)]) {
        let next_epoch = self
            .reconcile_gen
            .fetch_add(1, Ordering::Release)
            .wrapping_add(1) as u32;
        for &(acc_hash, global_balance) in updates {
            let key = acc_hash.max(2);
            let shard_idx = (key as usize) & (NUM_SHARDS - 1);
            let mut shard_guard = self.shards[shard_idx].arena.lock().unwrap();
            if let Some(arena) = shard_guard.as_mut()
                && let Some(state) = arena.get_mut(key)
            {
                state.balance_micro_credits =
                    global_balance.saturating_sub(state.delta_micro_credits_deducted);
                state.in_flight_delta = 0;
                state.reconcile_epoch = next_epoch;
            }
        }
    }
}
