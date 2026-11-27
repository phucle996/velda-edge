//! Per-Core L1 Micro-Lease & Exhausted Fast-Reject Cache (`CoreLane`) + Node-Global
//! Set-Associative `CLOCK` Slab Arena (`ShardArena`) with `dirty_shard_mask` & Shadow Sync.
//!
//! ```text
//!   Hot-Path Request (`evaluate_key`)
//!         │
//!         ├─► Fast Path (`CoreLane[LOCAL_CORE_LANE & lane_mask][hash & 63]` AtomicU64):
//!         │     [epoch10 | key_tag24 | exhausted1 | payload29]
//!         │       ├─ exhausted1 == 1 ──► Instant 429 fast-reject (full epoch for FixedWindow,
//!         │       │                        sub-ms flood guard for TokenBucket/SlidingWindow)
//!         │       └─ exhausted1 == 0 & batch_rem > 0
//!         │                          ──► CAS dec (bucket_rem, batch_rem) in-place (< 5 ns!)
//!         │
//!         └─► Refill / Direct Path (`ShardArena` 64 Shards × 16-way CLOCK Sets):
//!               1. Select shard = key_hash & 63, set_idx = (key_hash >> 6) % num_sets
//!               2. Probe 16 contiguous SlabSlots -> hit sets ref_bit = true
//!               3. Miss -> O(1) CLOCK sweep over those 16 slots -> instant evict if busy
//!               4. `evaluate_bucket_consume(..., requested = lease_batch)`
//!               5. dirty_delta += granted; set bit `shard_idx` in `dirty_shard_mask`
//!               6. Install remaining lease (if granted > 1) or exhausted marker into `CoreLane`
//!
//!   Shadow Logistics Path (`pbww > 0`):
//!   ┌────────────────────────────────────────────────────────────────────────────┐
//!   │ collect_dirty_deltas(max) ──► fast-check `dirty_shard_mask` (`0` = skip!), │
//!   │                               round-robin from `sync_shard_cursor`         │
//!   │ SoT Provider client.rs    ──► atomic upsert delta, read cluster_total      │
//!   │ reconcile_cluster_totals  ──► update cluster_other_used & poison overflow  │
//!   └────────────────────────────────────────────────────────────────────────────┘
//! ```

use std::sync::Mutex;
use std::sync::atomic::{AtomicU8, AtomicU64, AtomicUsize, Ordering};
#[cfg(not(target_os = "linux"))]
use std::time::Instant;

use crate::pre_upstream::rate_limit::algorithm::{
    BucketAccountingState, RateDecision, effective_capacity_with_grace, evaluate_bucket_consume,
};
use crate::pre_upstream::rate_limit::validation::{
    MAX_ALLOWED_ENTRIES, RateLimitAlgorithm, RateLimitConfig,
};

const NUM_SHARDS: usize = 64;
const SET_WAYS: usize = 16;
const LANE_SLOTS: usize = 64;

const PREFIX_MASK: u64 = !((1u64 << 30) - 1);
const EXHAUSTED_BIT: u64 = 1u64 << 29;
const BLOCKED_MS_MASK: u64 = (1u64 << 29) - 1;
const BUCKET_REM_MASK: u64 = 0x00FF_FFFF;
const BATCH_MASK: u64 = 0x1F;

static NEXT_CORE_LANE: AtomicUsize = AtomicUsize::new(0);
thread_local! {
    static LOCAL_CORE_LANE: usize = NEXT_CORE_LANE.fetch_add(1, Ordering::Relaxed);
}

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
    /// - bits 54..63 (10 bits): `epoch_sec & 0x03FF`
    /// - bits 30..53 (24 bits): `(key_hash >> 24) & 0x00FF_FFFF`
    /// - bit  29     (1 bit) : `EXHAUSTED_BIT` (`0` = positive micro-lease, `1` = blocked fast-reject)
    /// - When `EXHAUSTED_BIT == 0`:
    ///   - bits 5..28 (24 bits): `bucket_rem` clamped to `BUCKET_REM_MASK` (`16,777,215`)
    ///   - bits 0..4  (5 bits) : `batch_rem` (`1..=16`)
    /// - When `EXHAUSTED_BIT == 1`:
    ///   - bits 0..28 (29 bits): `now_ms & BLOCKED_MS_MASK`
    slots: [AtomicU64; LANE_SLOTS],
}

impl CoreLane {
    fn new() -> Self {
        Self {
            slots: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }

    #[inline(always)]
    fn prefix(key_hash: u64, epoch_sec: u64) -> u64 {
        ((epoch_sec & 0x03FF) << 54) | (((key_hash >> 24) & 0x00FF_FFFF) << 30)
    }

    #[inline]
    fn try_consume(
        &self,
        key_hash: u64,
        algo: RateLimitAlgorithm,
        now_ms: u64,
        hard_cap: u64,
    ) -> Option<RateDecision> {
        let slot_idx = (key_hash as usize) & (LANE_SLOTS - 1);
        let epoch_sec = now_ms / 1000;
        let expect_prefix = Self::prefix(key_hash, epoch_sec);

        let mut prev = self.slots[slot_idx].load(Ordering::Relaxed);
        loop {
            if (prev & PREFIX_MASK) != expect_prefix {
                return None;
            }
            if (prev & EXHAUSTED_BIT) != 0 {
                let is_blocked = matches!(algo, RateLimitAlgorithm::FixedWindow)
                    || (prev & BLOCKED_MS_MASK) == (now_ms & BLOCKED_MS_MASK);
                if is_blocked {
                    return Some(RateDecision {
                        allowed: false,
                        remaining: 0,
                        in_overdraft: true,
                        retry_after_secs: 1,
                    });
                }
                return None;
            }

            let batch_rem = prev & BATCH_MASK;
            if batch_rem == 0 {
                return None;
            }
            let prev_bucket_rem = (prev >> 5) & BUCKET_REM_MASK;
            let next_bucket_rem = prev_bucket_rem.saturating_sub(1);
            let next_batch = batch_rem - 1;
            let next = expect_prefix | (next_bucket_rem << 5) | next_batch;
            match self.slots[slot_idx].compare_exchange_weak(
                prev,
                next,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    return Some(RateDecision {
                        allowed: true,
                        remaining: next_bucket_rem.min(hard_cap),
                        in_overdraft: prev_bucket_rem == 0,
                        retry_after_secs: 0,
                    });
                }
                Err(actual) => prev = actual,
            }
        }
    }

    #[inline]
    fn install_lease(
        &self,
        key_hash: u64,
        epoch_sec: u64,
        bucket_rem_after_first: u64,
        remaining_batch: u64,
    ) {
        if remaining_batch == 0 || bucket_rem_after_first > BUCKET_REM_MASK {
            return;
        }
        let slot_idx = (key_hash as usize) & (LANE_SLOTS - 1);
        let packed = Self::prefix(key_hash, epoch_sec)
            | ((bucket_rem_after_first & BUCKET_REM_MASK) << 5)
            | (remaining_batch & BATCH_MASK);
        self.slots[slot_idx].store(packed, Ordering::Release);
    }

    #[inline]
    fn install_exhausted(&self, key_hash: u64, now_ms: u64) {
        let slot_idx = (key_hash as usize) & (LANE_SLOTS - 1);
        let epoch_sec = now_ms / 1000;
        let packed = Self::prefix(key_hash, epoch_sec) | EXHAUSTED_BIT | (now_ms & BLOCKED_MS_MASK);
        self.slots[slot_idx].store(packed, Ordering::Release);
    }

    #[inline]
    fn invalidate_key(&self, key_hash: u64, epoch_sec: u64) {
        let slot_idx = (key_hash as usize) & (LANE_SLOTS - 1);
        let expect_prefix = Self::prefix(key_hash, epoch_sec);
        let cur = self.slots[slot_idx].load(Ordering::Relaxed);
        if (cur & PREFIX_MASK) == expect_prefix && (cur & EXHAUSTED_BIT) == 0 {
            let _ =
                self.slots[slot_idx].compare_exchange(cur, 0, Ordering::Release, Ordering::Relaxed);
        }
    }
}

#[repr(C, align(64))]
#[derive(Clone, Copy, Default)]
struct SlotBody {
    state: BucketAccountingState, // 40B
    dirty_delta: u64,             // 8B
    node_synced_total: u64,       // 8B
    tier: u8,                     // 1B (+7B alignment padding -> exact 64B cache line)
}

const _: () = assert!(std::mem::size_of::<SlotBody>() == 64);

#[repr(C, align(64))]
#[derive(Clone, Copy)]
struct SlabSet {
    /// Contiguous 16 × 64-bit hashes (`128 bytes` = 2 cache lines for AVX2/portable probe).
    hashes: [u64; SET_WAYS],
    /// Dedicated cache-line-aligned per-way accounting blocks (`16 × 64B`).
    slots: [SlotBody; SET_WAYS],
    /// CLOCK reference bitmask (`bit i` = way `i` recently hit).
    ref_mask: u16,
    /// Pending dirty delta bitmask (`bit i` = `slots[i].dirty_delta > 0`).
    dirty_mask: u16,
    /// Bounded CLOCK sweep hand (`0..SET_WAYS`).
    clock_hand: u8,
}

impl Default for SlabSet {
    fn default() -> Self {
        Self {
            hashes: [0u64; SET_WAYS],
            slots: [SlotBody::default(); SET_WAYS],
            ref_mask: 0,
            dirty_mask: 0,
            clock_hand: 0,
        }
    }
}

#[inline]
fn find_hash_in_set16(hashes: &[u64; SET_WAYS], key_hash: u64) -> Option<usize> {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    // SAFETY: `avx2` is enabled at compile time; 256-bit unaligned loads stay within `[u64; 16]`.
    unsafe {
        use core::arch::x86_64::*;
        let target = _mm256_set1_epi64x(key_hash as i64);
        for chunk in 0..4 {
            let ptr = hashes.as_ptr().add(chunk * 4) as *const __m256i;
            let vec = _mm256_loadu_si256(ptr);
            let cmp = _mm256_cmpeq_epi64(vec, target);
            let mask = _mm256_movemask_pd(_mm256_castsi256_pd(cmp));
            if mask != 0 {
                return Some(chunk * 4 + mask.trailing_zeros() as usize);
            }
        }
        None
    }

    #[cfg(not(all(target_arch = "x86_64", target_feature = "avx2")))]
    {
        for (i, &h) in hashes.iter().enumerate() {
            if h == key_hash {
                return Some(i);
            }
        }
        None
    }
}

#[inline]
fn is_slot_expired(
    slot: &SlotBody,
    algo: RateLimitAlgorithm,
    current_epoch: u64,
    now_ms: u64,
    tb_idle_ms: u64,
) -> bool {
    match algo {
        RateLimitAlgorithm::FixedWindow => slot.state.epoch_sec < current_epoch,
        RateLimitAlgorithm::SlidingWindow => slot.state.epoch_sec.saturating_add(1) < current_epoch,
        RateLimitAlgorithm::TokenBucket => now_ms.saturating_sub(slot.state.last_ms) >= tb_idle_ms,
    }
}

#[repr(C, align(64))]
struct ShardSlab {
    sets: Box<[SlabSet]>,
    dirty_set_words: Box<[u64]>,
    set_mask: usize,
}

impl ShardSlab {
    fn new(min_slots: usize) -> Self {
        let num_sets = min_slots.div_ceil(SET_WAYS).max(1).next_power_of_two();
        let num_words = num_sets.div_ceil(64);
        Self {
            sets: vec![SlabSet::default(); num_sets].into_boxed_slice(),
            dirty_set_words: vec![0u64; num_words].into_boxed_slice(),
            set_mask: num_sets - 1,
        }
    }

    #[inline(always)]
    fn mark_dirty(&mut self, set_idx: usize, way: usize) {
        self.sets[set_idx].dirty_mask |= 1u16 << way;
        self.dirty_set_words[set_idx >> 6] |= 1u64 << (set_idx & 63);
    }

    #[inline(always)]
    fn clear_dirty_way(&mut self, set_idx: usize, way: usize) {
        self.sets[set_idx].dirty_mask &= !(1u16 << way);
        if self.sets[set_idx].dirty_mask == 0 {
            self.dirty_set_words[set_idx >> 6] &= !(1u64 << (set_idx & 63));
        }
    }

    #[inline]
    fn find_or_evict_way(
        &mut self,
        key_hash: u64,
        tier: u8,
        algo: RateLimitAlgorithm,
        now_ms: u64,
        tb_idle_ms: u64,
    ) -> (usize, usize) {
        let set_idx = ((key_hash >> 6) as usize) & self.set_mask;
        let current_epoch = now_ms / 1000;
        let set = &mut self.sets[set_idx];

        if let Some(way) = find_hash_in_set16(&set.hashes, key_hash)
            && set.slots[way].tier == tier
        {
            set.ref_mask |= 1u16 << way;
            return (set_idx, way);
        }

        // Handle ultra-rare same-set hash match with different `tier` without silent aliasing.
        let mut first_empty: Option<usize> = None;
        let mut first_expired_clean: Option<usize> = None;
        for way in 0..SET_WAYS {
            let h = set.hashes[way];
            if h == key_hash && set.slots[way].tier == tier {
                set.ref_mask |= 1u16 << way;
                return (set_idx, way);
            }
            if h == 0 && first_empty.is_none() {
                first_empty = Some(way);
            } else if h != 0
                && first_expired_clean.is_none()
                && (set.dirty_mask & (1u16 << way)) == 0
                && is_slot_expired(&set.slots[way], algo, current_epoch, now_ms, tb_idle_ms)
            {
                first_expired_clean = Some(way);
            }
        }

        let target_way = if let Some(empty_way) = first_empty {
            empty_way
        } else if let Some(exp_way) = first_expired_clean {
            exp_way
        } else {
            // Bounded CLOCK sweep: prefer clean (`dirty == 0`), else spare tier-0 if possible.
            let mut hand = set.clock_hand as usize;
            let mut chosen = None;
            if set.dirty_mask != u16::MAX {
                for _ in 0..=SET_WAYS {
                    let bit = 1u16 << hand;
                    if (set.dirty_mask & bit) == 0 && (set.ref_mask & bit) == 0 {
                        chosen = Some(hand);
                        hand = (hand + 1) & (SET_WAYS - 1);
                        break;
                    }
                    set.ref_mask &= !bit;
                    hand = (hand + 1) & (SET_WAYS - 1);
                }
            }
            let way = chosen.unwrap_or_else(|| {
                let mut fb = None;
                for _ in 0..=SET_WAYS {
                    let bit = 1u16 << hand;
                    if set.slots[hand].tier > 0 && (set.ref_mask & bit) == 0 {
                        fb = Some(hand);
                        hand = (hand + 1) & (SET_WAYS - 1);
                        break;
                    }
                    set.ref_mask &= !bit;
                    hand = (hand + 1) & (SET_WAYS - 1);
                }
                fb.unwrap_or_else(|| {
                    let w = hand;
                    hand = (hand + 1) & (SET_WAYS - 1);
                    w
                })
            });
            set.clock_hand = hand as u8;
            way
        };

        set.hashes[target_way] = key_hash;
        set.slots[target_way] = SlotBody {
            tier,
            dirty_delta: 0,
            node_synced_total: 0,
            state: BucketAccountingState {
                used_milli: 0,
                prev_window_count: 0,
                last_ms: now_ms,
                epoch_sec: current_epoch,
                cluster_other_used: 0,
            },
        };
        set.ref_mask |= 1u16 << target_way;
        self.clear_dirty_way(set_idx, target_way);
        (set_idx, target_way)
    }
}

/// Complete lock-free L1 `CoreLane` + 64-shard `ShardArena` rate-limit engine.
pub struct RateLimitArena {
    anchor_unix_ms: u64,
    #[cfg(target_os = "linux")]
    anchor_coarse_mono_ms: u64,
    #[cfg(not(target_os = "linux"))]
    anchor_instant: Instant,
    lanes: Box<[CoreLane]>,
    lane_mask: usize,
    shards: Box<[Mutex<ShardSlab>; NUM_SHARDS]>,
    dirty_shard_mask: AtomicU64,
    sync_shard_cursor: AtomicUsize,
    algorithm: RateLimitAlgorithm,
    rate_per_second: u64,
    burst_capacity: u64,
    overdraft_percent: u8,
    lease_batch: u64,
    tb_idle_ms: u64,
    sot_health: AtomicU8,
}

#[cfg(target_os = "linux")]
#[inline]
fn coarse_mono_ms() -> u64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: `ts` is a valid stack `timespec` initialized before calling `clock_gettime`.
    let rc = unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC_COARSE, &mut ts) };
    if rc == 0 {
        (ts.tv_sec as u64)
            .saturating_mul(1000)
            .saturating_add((ts.tv_nsec as u64) / 1_000_000)
    } else {
        velda_core::clock::now_unix_nanos() / 1_000_000
    }
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
    /// Allocates bounded hardware-sized L1 `CoreLane`s and 64 fixed `ShardSlab` containers up front.
    pub fn new(config: &RateLimitConfig) -> Self {
        let bounded_entries = config.max_entries.clamp(64, MAX_ALLOWED_ENTRIES);
        let per_shard = bounded_entries.div_ceil(NUM_SHARDS).max(SET_WAYS);
        let shards = std::array::from_fn(|_| Mutex::new(ShardSlab::new(per_shard)));

        let hw_cores = velda_core::hardware::probe_cpu().available_cores;
        let affine_cores = velda_core::worker::get_allowed_cores().len().max(1);
        let num_lanes = hw_cores.min(affine_cores).max(1).next_power_of_two();
        let lanes = (0..num_lanes)
            .map(|_| CoreLane::new())
            .collect::<Vec<_>>()
            .into_boxed_slice();

        // Scale micro-lease batch threshold with actual hardware lanes while keeping a minimum
        // floor of 64 so tight small-burst quotas remain exact down to a single token.
        let min_lease_burst = (num_lanes as u64 * 4).max(64);
        let lease_batch = if config.burst_capacity >= min_lease_burst {
            (config.burst_capacity / min_lease_burst).clamp(2, 16)
        } else {
            1
        };

        let tb_idle_ms =
            (config.burst_capacity.saturating_mul(1000) / config.rate_per_second.max(1)).max(2000);
        let anchor_unix_ms = (velda_core::clock::now_unix_nanos() / 1_000_000).max(1000);

        Self {
            anchor_unix_ms,
            #[cfg(target_os = "linux")]
            anchor_coarse_mono_ms: coarse_mono_ms(),
            #[cfg(not(target_os = "linux"))]
            anchor_instant: Instant::now(),
            lanes,
            lane_mask: num_lanes - 1,
            shards: Box::new(shards),
            dirty_shard_mask: AtomicU64::new(0),
            sync_shard_cursor: AtomicUsize::new(0),
            algorithm: config.algorithm,
            rate_per_second: config.rate_per_second,
            burst_capacity: config.burst_capacity,
            overdraft_percent: config.overdraft_percent,
            lease_batch,
            tb_idle_ms,
            sot_health: AtomicU8::new(SOT_HEALTH_OK),
        }
    }

    /// Returns monotonic coarse milliseconds on Unix-ms scale (`>= 1000`) for background sync
    /// and fallback when request `ingress_time_nanos` is unset (`0`).
    #[inline]
    pub fn now_ms(&self) -> u64 {
        #[cfg(target_os = "linux")]
        {
            let now = coarse_mono_ms();
            self.anchor_unix_ms
                .saturating_add(now.saturating_sub(self.anchor_coarse_mono_ms))
                .max(1000)
        }
        #[cfg(not(target_os = "linux"))]
        {
            self.anchor_unix_ms
                .saturating_add(self.anchor_instant.elapsed().as_millis() as u64)
                .max(1000)
        }
    }

    /// Derives request `now_ms` from pre-captured `L7Request::ingress_time_nanos` without
    /// calling any clock syscall on the hot path, falling back to [`Self::now_ms`] when `0`.
    #[inline(always)]
    pub fn now_ms_from_ingress(&self, ingress_time_nanos: u64) -> u64 {
        if ingress_time_nanos != 0 {
            (ingress_time_nanos / 1_000_000).max(1000)
        } else {
            self.now_ms()
        }
    }

    #[inline(always)]
    fn current_lane_idx(&self) -> usize {
        LOCAL_CORE_LANE.with(|&id| id & self.lane_mask)
    }

    #[inline]
    pub fn sot_health(&self) -> u8 {
        self.sot_health.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn set_sot_health(&self, state: u8) {
        self.sot_health.store(state, Ordering::Release);
    }

    /// Evaluates one key digest against Per-Core `CoreLane` and Node-Global `ShardArena`
    /// using caller-provided `now_ms` (captured once per request).
    #[inline]
    pub fn evaluate_key(&self, key_hash: u64, tier: u8, now_ms: u64) -> RateDecision {
        // Reserve `0` exclusively as empty-slot sentinel in `SlabSet.hashes`.
        let key_hash = key_hash.max(1);
        let now_ms = now_ms.max(1000);
        let epoch_sec = now_ms / 1000;
        let lane_idx = self.current_lane_idx();

        if let Some(dec) =
            self.lanes[lane_idx].try_consume(key_hash, self.algorithm, now_ms, self.burst_capacity)
        {
            return dec;
        }

        let shard_idx = (key_hash as usize) & (NUM_SHARDS - 1);
        let (mut dec, granted) = {
            let mut guard = self.shards[shard_idx]
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            let (set_idx, way) =
                guard.find_or_evict_way(key_hash, tier, self.algorithm, now_ms, self.tb_idle_ms);
            let slot = &mut guard.sets[set_idx].slots[way];
            let prev_epoch = slot.state.epoch_sec;

            let (decision, granted) = evaluate_bucket_consume(
                self.algorithm,
                self.rate_per_second,
                self.burst_capacity,
                self.overdraft_percent,
                now_ms,
                self.lease_batch,
                &mut slot.state,
            );
            if prev_epoch < epoch_sec {
                slot.node_synced_total = 0;
            }
            if granted > 0 {
                let was_clean = slot.dirty_delta == 0;
                slot.dirty_delta = slot.dirty_delta.saturating_add(granted);
                if was_clean {
                    guard.mark_dirty(set_idx, way);
                    let bit = 1u64 << shard_idx;
                    if (self.dirty_shard_mask.load(Ordering::Relaxed) & bit) == 0 {
                        self.dirty_shard_mask.fetch_or(bit, Ordering::Release);
                    }
                }
            }
            (decision, granted)
        };

        if !dec.allowed {
            self.lanes[lane_idx].install_exhausted(key_hash, now_ms);
        } else if granted > 1 && self.lease_batch > 1 {
            let first_rem = if dec.in_overdraft {
                0
            } else {
                dec.remaining
                    .saturating_add(granted - 1)
                    .min(self.burst_capacity)
            };
            dec.remaining = first_rem;
            self.lanes[lane_idx].install_lease(key_hash, epoch_sec, first_rem, granted - 1);
        }

        dec
    }

    /// Drains non-zero `dirty_delta` counters O(dirty_sets) across dirty shards only (`dirty_shard_mask`).
    pub fn collect_dirty_deltas(&self, max_items: usize) -> Vec<DirtyKeyDelta> {
        let mask = self.dirty_shard_mask.load(Ordering::Acquire);
        if mask == 0 || max_items == 0 {
            return Vec::new();
        }

        let mut batch = Vec::with_capacity(max_items.min(2048));
        let start = self.sync_shard_cursor.load(Ordering::Relaxed) & (NUM_SHARDS - 1);
        let mut next_cursor = start;

        for offset in 0..NUM_SHARDS {
            if batch.len() >= max_items {
                break;
            }
            let shard_idx = (start + offset) & (NUM_SHARDS - 1);
            let bit = 1u64 << shard_idx;
            if (mask & bit) == 0 {
                continue;
            }

            let mut shard = self.shards[shard_idx]
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            let mut exhausted_shard = true;
            'words: for word_idx in 0..shard.dirty_set_words.len() {
                let mut word = shard.dirty_set_words[word_idx];
                while word != 0 {
                    let s_bit = word.trailing_zeros() as usize;
                    let set_idx = (word_idx << 6) + s_bit;
                    let set = &mut shard.sets[set_idx];
                    let mut dm = set.dirty_mask;
                    while dm != 0 {
                        if batch.len() >= max_items {
                            set.dirty_mask = dm;
                            shard.dirty_set_words[word_idx] = word;
                            exhausted_shard = false;
                            break 'words;
                        }
                        let way = dm.trailing_zeros() as usize;
                        dm &= dm - 1;
                        let key_hash = set.hashes[way];
                        let slot = &mut set.slots[way];
                        if key_hash != 0 && slot.dirty_delta > 0 {
                            let delta = slot.dirty_delta;
                            slot.dirty_delta = 0;
                            batch.push(DirtyKeyDelta {
                                key_hash,
                                tier: slot.tier,
                                delta,
                                window_epoch: slot.state.epoch_sec,
                            });
                        }
                    }
                    set.dirty_mask = 0;
                    word &= word - 1;
                    shard.dirty_set_words[word_idx] = word;
                }
            }

            if exhausted_shard {
                // Clear under the shard mutex so concurrent hot-path `fetch_or` cannot be lost.
                self.dirty_shard_mask.fetch_and(!bit, Ordering::Release);
                next_cursor = (shard_idx + 1) & (NUM_SHARDS - 1);
            } else {
                next_cursor = shard_idx;
                break;
            }
        }

        self.sync_shard_cursor.store(next_cursor, Ordering::Relaxed);
        batch
    }

    /// Re-queues unsent deltas back into `ShardArena` if SoT push failed temporarily.
    pub fn requeue_failed_deltas(&self, deltas: &[DirtyKeyDelta]) {
        let now_ms = self.now_ms();
        for d in deltas {
            let key_hash = d.key_hash.max(1);
            let shard_idx = (key_hash as usize) & (NUM_SHARDS - 1);
            let mut shard = self.shards[shard_idx]
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            let (set_idx, way) =
                shard.find_or_evict_way(key_hash, d.tier, self.algorithm, now_ms, self.tb_idle_ms);
            let slot = &mut shard.sets[set_idx].slots[way];
            if slot.state.epoch_sec == d.window_epoch && d.delta > 0 {
                slot.dirty_delta = slot.dirty_delta.saturating_add(d.delta);
                shard.mark_dirty(set_idx, way);
                let bit = 1u64 << shard_idx;
                if (self.dirty_shard_mask.load(Ordering::Relaxed) & bit) == 0 {
                    self.dirty_shard_mask.fetch_or(bit, Ordering::Release);
                }
            }
        }
    }

    /// Reconciles cluster-wide SoT counts back into `ShardArena`, updating `cluster_other_used`
    /// so subsequent local checks reflect peer-node traffic without blocking hot-path requests.
    pub fn reconcile_cluster_totals(&self, results: &[ReconciledClusterTotal]) {
        let eff_cap = effective_capacity_with_grace(self.burst_capacity, self.overdraft_percent);
        let now_ms = self.now_ms();
        for res in results {
            let key_hash = res.key_hash.max(1);
            let shard_idx = (key_hash as usize) & (NUM_SHARDS - 1);
            let mut shard = self.shards[shard_idx]
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            let (set_idx, way) = shard.find_or_evict_way(
                key_hash,
                res.tier,
                self.algorithm,
                now_ms,
                self.tb_idle_ms,
            );
            let slot = &mut shard.sets[set_idx].slots[way];
            if slot.state.epoch_sec == res.window_epoch {
                slot.node_synced_total = slot.node_synced_total.saturating_add(res.pushed_delta);
                let peer_used = res.cluster_total.saturating_sub(slot.node_synced_total);
                slot.state.cluster_other_used = peer_used.min(eff_cap.saturating_mul(2));
                if res.cluster_total >= eff_cap && self.lease_batch > 1 {
                    for lane in self.lanes.iter() {
                        lane.invalidate_key(key_hash, res.window_epoch);
                    }
                }
            }
        }
    }
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
        let now_ms = arena.now_ms();
        for i in 1..=5000u64 {
            let hash = i.wrapping_mul(0x9e37_79b9_7f4a_7c15).max(1);
            let dec = arena.evaluate_key(hash, 0, now_ms);
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
        let after = arena.evaluate_key(first.key_hash, 0, now_ms);
        assert!(
            !after.allowed,
            "reconciled cluster_total=100 must block key with limit=5"
        );
    }

    #[test]
    fn test_core_lane_reports_exact_remaining_and_fast_rejects_exhausted_keys() {
        let cfg = RateLimitConfig {
            algorithm: RateLimitAlgorithm::FixedWindow,
            rate_per_second: 1000,
            burst_capacity: 1000,
            overdraft_percent: 0,
            ..RateLimitConfig::default()
        };
        let arena = RateLimitArena::new(&cfg);
        assert!(arena.lease_batch > 1);

        let now_ms = arena.now_ms();
        let hot_key = 0x1234_5678_9abc_def1u64;
        for step in 0..30u64 {
            let dec = arena.evaluate_key(hot_key, 0, now_ms);
            assert!(dec.allowed);
            assert_eq!(dec.remaining, 1000 - 1 - step);
        }

        // Small quota (`lease_batch = 1`): exhaustion installs lock-free fast-reject marker
        let small_cfg = RateLimitConfig {
            algorithm: RateLimitAlgorithm::FixedWindow,
            rate_per_second: 3,
            burst_capacity: 3,
            overdraft_percent: 0,
            ..RateLimitConfig::default()
        };
        let small_arena = RateLimitArena::new(&small_cfg);
        assert_eq!(small_arena.lease_batch, 1);
        for _ in 0..3 {
            assert!(small_arena.evaluate_key(hot_key, 0, now_ms).allowed);
        }
        assert!(!small_arena.evaluate_key(hot_key, 0, now_ms).allowed);
        // Subsequent flood hit returns blocked directly from CoreLane in same epoch
        let lane_idx = small_arena.current_lane_idx();
        assert!(
            small_arena.lanes[lane_idx]
                .try_consume(hot_key, RateLimitAlgorithm::FixedWindow, now_ms, 3)
                .is_some_and(|d| !d.allowed)
        );
        // Next epoch clears cleanly
        assert!(small_arena.evaluate_key(hot_key, 0, now_ms + 1000).allowed);
    }

    #[test]
    fn test_collect_dirty_deltas_round_robin_fairness_and_mask_clearing() {
        let cfg = RateLimitConfig {
            algorithm: RateLimitAlgorithm::FixedWindow,
            rate_per_second: 10,
            burst_capacity: 10,
            overdraft_percent: 0,
            ..RateLimitConfig::default()
        };
        let arena = RateLimitArena::new(&cfg);
        let now_ms = arena.now_ms();

        // Populate 1 key in every single one of the 64 shards
        for s in 0..64u64 {
            let key_hash = ((s + 1) << 16) | s;
            assert!(arena.evaluate_key(key_hash, 0, now_ms).allowed);
        }
        assert_eq!(arena.dirty_shard_mask.load(Ordering::Acquire), u64::MAX);

        // Drain 20 per tick -> 4 calls must visit shards 0..63 fairly and clear mask to 0
        let mut total_drained = 0usize;
        for _ in 0..4 {
            total_drained += arena.collect_dirty_deltas(20).len();
        }
        assert_eq!(total_drained, 64);
        assert_eq!(arena.dirty_shard_mask.load(Ordering::Acquire), 0);
        assert!(arena.collect_dirty_deltas(20).is_empty());
    }

    #[test]
    fn test_expired_first_and_tier_aware_clock_preserves_active_dirty_tier0() {
        let cfg = RateLimitConfig {
            algorithm: RateLimitAlgorithm::FixedWindow,
            rate_per_second: 10,
            burst_capacity: 10,
            overdraft_percent: 0,
            // 1 set per shard (64 * 16 = 1024 entries), so every key colliding into shard 0 lands in set 0
            max_entries: 64 * 16,
            ..RateLimitConfig::default()
        };
        let arena = RateLimitArena::new(&cfg);
        let t0 = 10_000u64;

        // Fill 15 ways in shard 0 with keys at epoch=10, then drain dirty flags so they are expired-clean by t1
        for i in 1..=15u64 {
            let k = i << 16; // shard 0, set 0
            assert!(arena.evaluate_key(k, 1, t0).allowed);
        }
        assert_eq!(arena.collect_dirty_deltas(64).len(), 15);

        // Advance to epoch=11: add active dirty tier-0 tenant key AND 15 fresh active keys into same set!
        let t1 = t0 + 1000;
        let active_tier0 = 999u64 << 16;
        assert!(arena.evaluate_key(active_tier0, 0, t1).allowed);

        // Insert 15 brand new keys at t1: all 15 MUST reclaim the 15 expired-clean ways from epoch=10,
        // keeping `active_tier0` intact with its `dirty_delta = 1`!
        for i in 100..115u64 {
            let k = i << 16;
            assert!(arena.evaluate_key(k, 1, t1).allowed);
        }

        // Verify `active_tier0` is still present with remaining=8 after another consume (`used` preserved as 1->2)
        let second_t0 = arena.evaluate_key(active_tier0, 0, t1);
        assert!(second_t0.allowed);
        assert_eq!(second_t0.remaining, 8);
    }
}
