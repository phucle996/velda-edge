//! Per-Core L1 Micro-Lease Cache (`CoreLane`) + Node-Global Set-Associative `CLOCK`
//! Slab Arena (`ShardArena`) with Monotonic Timing & Shadow SoT Reconciliation.
//!
//! ```text
//!   Hot-Path Request (`evaluate_key`)
//!         │
//!         ├─► Fast Path (when lease_batch > 1):
//!         │     CAS on CoreLane[lane_id][hash & 63] packed AtomicU64:
//!         │       [epoch12 | key_tag40 | rem_batch12]
//!         │     Hit (rem > 0 && same epoch/key) ──► dec rem in-place (< 5 ns, 0 locks!)
//!         │
//!         └─► Refill / Direct Path (`ShardArena` 64 Shards × 16-way CLOCK Sets):
//!               1. Select shard = key_hash & 63, set_idx = (key_hash >> 6) % num_sets
//!               2. Probe 16 contiguous SlabSlots -> hit sets ref_bit = true
//!               3. Miss -> O(1) CLOCK sweep over those 16 slots -> instant evict if busy
//!               4. `evaluate_bucket_consume(..., requested = lease_batch)`
//!               5. dirty_delta += granted; install remaining batch into CoreLane if batch > 1
//!
//!   Shadow Logistics Path (`pbww > 0`):
//!   ┌────────────────────────────────────────────────────────────────────────────┐
//!   │ collect_dirty_deltas(max) ──► drain non-zero dirty_delta across shards     │
//!   │ SoT Provider client.rs    ──► atomic upsert delta, read cluster_total      │
//!   │ reconcile_cluster_totals  ──► update cluster_other_used = total - own_sync │
//!   └────────────────────────────────────────────────────────────────────────────┘
//! ```

use std::sync::Mutex;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::time::Instant;

use crate::pre_upstream::rate_limit::algorithm::{
    BucketAccountingState, RateDecision, effective_capacity_with_grace, evaluate_bucket_consume,
};
use crate::pre_upstream::rate_limit::validation::{RateLimitAlgorithm, RateLimitConfig};

const NUM_SHARDS: usize = 64;
const SET_WAYS: usize = 16;
const LANE_SLOTS: usize = 64;
const NUM_LANES: usize = 16;

/// Degraded cluster health flag observed by Shadow Worker SoT sync and read lock-free on hot path.
/// `0` = Healthy, `1` = Degraded (Fail-Open / Allow or Fallback Memory), `2` = Degraded (Fail-Closed / Deny).
pub const SOT_HEALTH_OK: u8 = 0;
pub const SOT_HEALTH_FAIL_OPEN: u8 = 1;
pub const SOT_HEALTH_FAIL_CLOSED: u8 = 2;

/// Delta drained from `ShardArena` by the Shadow Worker to push into SoT storage.
#[derive(Debug, Clone, Copy)]
pub struct DirtyKeyDelta {
    pub key_hash: u64,
    pub tier: u8,
    pub delta: u64,
    pub window_epoch: u64,
}

/// Authoritative response returned by a SoT adapter for one pushed key delta.
#[derive(Debug, Clone, Copy)]
pub struct ReconciledClusterTotal {
    pub key_hash: u64,
    pub tier: u8,
    pub pushed_delta: u64,
    pub window_epoch: u64,
    pub cluster_total: u64,
}

#[repr(C, align(64))]
struct CoreLane {
    /// Packed `AtomicU64`:
    /// - bits 52..63 (12 bits): `epoch_sec & 0x0FFF`
    /// - bits 12..51 (40 bits): `(key_hash >> 24) & 0x000F_FFFF_FFFF`
    /// - bits 0..11  (12 bits): `remaining_batch`
    slots: [AtomicU64; LANE_SLOTS],
}

impl CoreLane {
    fn new() -> Self {
        Self {
            slots: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }

    #[inline]
    fn try_consume(&self, key_hash: u64, epoch_sec: u64, hard_cap: u64) -> Option<RateDecision> {
        let slot_idx = (key_hash as usize) & (LANE_SLOTS - 1);
        let expect_epoch = epoch_sec & 0x0FFF;
        let expect_tag = (key_hash >> 24) & 0x000F_FFFF_FFFF;
        let expect_prefix = (expect_epoch << 52) | (expect_tag << 12);

        let mut prev = self.slots[slot_idx].load(Ordering::Relaxed);
        loop {
            if (prev & !0x0FFF) != expect_prefix {
                return None;
            }
            let rem = prev & 0x0FFF;
            if rem == 0 {
                return None;
            }
            let next = expect_prefix | (rem - 1);
            match self.slots[slot_idx].compare_exchange_weak(
                prev,
                next,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    // Approximate remaining conservatively at remaining batch vs hard cap.
                    let clamped_rem = (rem - 1).min(hard_cap);
                    return Some(RateDecision {
                        allowed: true,
                        remaining: clamped_rem,
                        in_overdraft: false,
                        retry_after_secs: 0,
                    });
                }
                Err(actual) => prev = actual,
            }
        }
    }

    #[inline]
    fn install(&self, key_hash: u64, epoch_sec: u64, remaining_batch: u64) {
        if remaining_batch == 0 {
            return;
        }
        let slot_idx = (key_hash as usize) & (LANE_SLOTS - 1);
        let packed = ((epoch_sec & 0x0FFF) << 52)
            | (((key_hash >> 24) & 0x000F_FFFF_FFFF) << 12)
            | (remaining_batch & 0x0FFF);
        self.slots[slot_idx].store(packed, Ordering::Release);
    }
}

#[derive(Clone, Copy, Default)]
struct SlabSlot {
    key_hash: u64,
    tier: u8,
    ref_bit: bool,
    dirty_delta: u64,
    node_synced_total: u64,
    synced_epoch: u64,
    state: BucketAccountingState,
}

#[repr(C, align(64))]
struct ShardSlab {
    entries: Box<[SlabSlot]>,
    set_clocks: Box<[u8]>,
    num_sets: usize,
}

impl ShardSlab {
    fn new(min_slots: usize) -> Self {
        let num_sets = min_slots.div_ceil(SET_WAYS).max(1);
        let total = num_sets * SET_WAYS;
        Self {
            entries: vec![SlabSlot::default(); total].into_boxed_slice(),
            set_clocks: vec![0u8; num_sets].into_boxed_slice(),
            num_sets,
        }
    }

    #[inline]
    fn find_or_evict_slot(&mut self, key_hash: u64, tier: u8, now_ms: u64) -> &mut SlabSlot {
        let set_idx = ((key_hash >> 6) as usize) % self.num_sets;
        let base = set_idx * SET_WAYS;
        let set_slice = &mut self.entries[base..base + SET_WAYS];

        let mut first_empty: Option<usize> = None;
        for (i, slot) in set_slice.iter().enumerate() {
            if slot.key_hash == key_hash {
                let s = &mut self.entries[base + i];
                s.ref_bit = true;
                return s;
            }
            if slot.key_hash == 0 && first_empty.is_none() {
                first_empty = Some(i);
            }
        }

        let target_rel = if let Some(empty_idx) = first_empty {
            empty_idx
        } else {
            // Bounded O(SET_WAYS + 1) CLOCK hand eviction (never blocks legitimate IPs).
            let mut hand = self.set_clocks[set_idx] as usize;
            let mut chosen = hand;
            for _ in 0..=SET_WAYS {
                if !self.entries[base + hand].ref_bit {
                    chosen = hand;
                    hand = (hand + 1) % SET_WAYS;
                    break;
                }
                self.entries[base + hand].ref_bit = false;
                hand = (hand + 1) % SET_WAYS;
            }
            self.set_clocks[set_idx] = hand as u8;
            chosen
        };

        let slot = &mut self.entries[base + target_rel];
        *slot = SlabSlot {
            key_hash,
            tier,
            ref_bit: true,
            dirty_delta: 0,
            node_synced_total: 0,
            synced_epoch: now_ms / 1000,
            state: BucketAccountingState {
                used_milli: 0,
                prev_window_count: 0,
                last_ms: now_ms,
                epoch_sec: now_ms / 1000,
                cluster_other_used: 0,
            },
        };
        slot
    }
}

/// Complete lock-free L1 `CoreLane` + 64-shard `ShardArena` rate-limit engine.
pub struct RateLimitArena {
    anchor: Instant,
    lanes: Box<[CoreLane]>,
    shards: Box<[Mutex<ShardSlab>; NUM_SHARDS]>,
    algorithm: RateLimitAlgorithm,
    rate_per_second: u64,
    burst_capacity: u64,
    overdraft_percent: u8,
    lease_batch: u64,
    sot_health: AtomicU8,
}

impl std::fmt::Debug for RateLimitArena {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RateLimitArena")
            .field("algorithm", &self.algorithm)
            .field("rate_per_second", &self.rate_per_second)
            .field("burst_capacity", &self.burst_capacity)
            .field("overdraft_percent", &self.overdraft_percent)
            .field("lease_batch", &self.lease_batch)
            .field("sot_health", &self.sot_health.load(Ordering::Relaxed))
            .finish()
    }
}

impl RateLimitArena {
    /// Allocates bounded L1 `CoreLane`s and 64 fixed `ShardSlab` containers up front.
    pub fn new(config: &RateLimitConfig) -> Self {
        let per_shard = config.max_entries.div_ceil(NUM_SHARDS).max(SET_WAYS);
        let shards = std::array::from_fn(|_| Mutex::new(ShardSlab::new(per_shard)));
        let lanes = (0..NUM_LANES)
            .map(|_| CoreLane::new())
            .collect::<Vec<_>>()
            .into_boxed_slice();

        // Only enable per-core batch buffering when burst_capacity is large enough so tight
        // small-quota limits (e.g. RPS=1 or burst=10) remain exact down to a single token.
        let lease_batch = if config.burst_capacity >= (NUM_LANES as u64) * 4 {
            (config.burst_capacity / (NUM_LANES as u64 * 4)).clamp(2, 16)
        } else {
            1
        };

        Self {
            anchor: Instant::now(),
            lanes,
            shards: Box::new(shards),
            algorithm: config.algorithm,
            rate_per_second: config.rate_per_second,
            burst_capacity: config.burst_capacity,
            overdraft_percent: config.overdraft_percent,
            lease_batch,
            sot_health: AtomicU8::new(SOT_HEALTH_OK),
        }
    }

    #[inline]
    pub fn now_ms(&self) -> u64 {
        // Offset by 1000ms so monotonic epoch_sec starts cleanly at 1 rather than 0.
        self.anchor.elapsed().as_millis() as u64 + 1000
    }

    #[inline]
    pub fn sot_health(&self) -> u8 {
        self.sot_health.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn set_sot_health(&self, state: u8) {
        self.sot_health.store(state, Ordering::Release);
    }

    /// Evaluates one key digest against Per-Core `CoreLane` and Node-Global `ShardArena`.
    #[inline]
    pub fn evaluate_key(&self, key_hash: u64, tier: u8) -> RateDecision {
        let now_ms = self.now_ms();
        let epoch_sec = now_ms / 1000;

        if self.lease_batch > 1 {
            let lane_idx = current_lane_idx();
            if let Some(dec) =
                self.lanes[lane_idx].try_consume(key_hash, epoch_sec, self.burst_capacity)
            {
                return dec;
            }
        }

        let shard_idx = (key_hash as usize) & (NUM_SHARDS - 1);
        let (dec, granted) = {
            let mut guard = self.shards[shard_idx]
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            let slot = guard.find_or_evict_slot(key_hash, tier, now_ms);
            if slot.synced_epoch != epoch_sec {
                slot.synced_epoch = epoch_sec;
                slot.node_synced_total = 0;
            }

            let (decision, granted) = evaluate_bucket_consume(
                self.algorithm,
                self.rate_per_second,
                self.burst_capacity,
                self.overdraft_percent,
                now_ms,
                self.lease_batch,
                &mut slot.state,
            );
            slot.dirty_delta = slot.dirty_delta.saturating_add(granted);
            (decision, granted)
        };

        if granted > 1 && self.lease_batch > 1 {
            let lane_idx = current_lane_idx();
            self.lanes[lane_idx].install(key_hash, epoch_sec, granted - 1);
        }

        dec
    }

    /// Drains non-zero `dirty_delta` counters across shards for Shadow SoT push.
    pub fn collect_dirty_deltas(&self, max_items: usize) -> Vec<DirtyKeyDelta> {
        let mut batch = Vec::with_capacity(max_items.min(256));
        for shard_mutex in self.shards.iter() {
            if batch.len() >= max_items {
                break;
            }
            let mut shard = shard_mutex.lock().unwrap_or_else(|p| p.into_inner());
            for slot in shard.entries.iter_mut() {
                if slot.key_hash != 0 && slot.dirty_delta > 0 {
                    let delta = slot.dirty_delta;
                    slot.dirty_delta = 0;
                    batch.push(DirtyKeyDelta {
                        key_hash: slot.key_hash,
                        tier: slot.tier,
                        delta,
                        window_epoch: slot.synced_epoch,
                    });
                    if batch.len() >= max_items {
                        break;
                    }
                }
            }
        }
        batch
    }

    /// Re-queues unsent deltas back into `ShardArena` if SoT push failed temporarily.
    pub fn requeue_failed_deltas(&self, deltas: &[DirtyKeyDelta]) {
        let now_ms = self.now_ms();
        for d in deltas {
            let shard_idx = (d.key_hash as usize) & (NUM_SHARDS - 1);
            let mut shard = self.shards[shard_idx]
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            let slot = shard.find_or_evict_slot(d.key_hash, d.tier, now_ms);
            if slot.synced_epoch == d.window_epoch {
                slot.dirty_delta = slot.dirty_delta.saturating_add(d.delta);
            }
        }
    }

    /// Reconciles cluster-wide SoT counts back into `ShardArena`, updating `cluster_other_used`
    /// so subsequent local checks reflect peer-node traffic without blocking hot-path requests.
    pub fn reconcile_cluster_totals(&self, results: &[ReconciledClusterTotal]) {
        let eff_cap = effective_capacity_with_grace(self.burst_capacity, self.overdraft_percent);
        let now_ms = self.now_ms();
        for res in results {
            let shard_idx = (res.key_hash as usize) & (NUM_SHARDS - 1);
            let mut shard = self.shards[shard_idx]
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            let slot = shard.find_or_evict_slot(res.key_hash, res.tier, now_ms);
            if slot.synced_epoch == res.window_epoch {
                slot.node_synced_total = slot.node_synced_total.saturating_add(res.pushed_delta);
                let peer_used = res.cluster_total.saturating_sub(slot.node_synced_total);
                slot.state.cluster_other_used = peer_used.min(eff_cap.saturating_mul(2));
            }
        }
    }
}

#[inline]
fn current_lane_idx() -> usize {
    // Derive from thread stack address bits for zero-syscall distribution across threads/cores.
    let stack_var: u8 = 0;
    let addr = (&stack_var as *const u8) as usize;
    ((addr >> 14) ^ (addr >> 7)) & (NUM_LANES - 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_arena_clock_eviction_under_ip_flood_never_stalls_new_keys() {
        let cfg = RateLimitConfig {
            algorithm: RateLimitAlgorithm::FixedWindow,
            rate_per_second: 5,
            burst_capacity: 5,
            overdraft_percent: 0,
            max_entries: 64 * 16,
            ..RateLimitConfig::default()
        };
        let arena = RateLimitArena::new(&cfg);

        // Flood 5000 distinct keys across 64 shards (> capacity)
        for i in 1..=5000u64 {
            let hash = i.wrapping_mul(0x9e37_79b9_7f4a_7c15).max(1);
            let dec = arena.evaluate_key(hash, 0);
            assert!(
                dec.allowed,
                "fresh key {i} must never be locked out by flood"
            );
        }

        // Dirty deltas drain and reconcile cleanly
        let deltas = arena.collect_dirty_deltas(100);
        assert!(!deltas.is_empty());
        let first = deltas[0];
        arena.reconcile_cluster_totals(&[ReconciledClusterTotal {
            key_hash: first.key_hash,
            tier: first.tier,
            pushed_delta: first.delta,
            window_epoch: first.window_epoch,
            cluster_total: 100,
        }]);
        let after = arena.evaluate_key(first.key_hash, 0);
        assert!(
            !after.allowed,
            "reconciled cluster_total=100 must block key with limit=5"
        );
    }
}
