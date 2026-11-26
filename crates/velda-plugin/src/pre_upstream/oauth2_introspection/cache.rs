//! Two-Tier L1D Control + Share-Nothing Per-Core L3 SIMD Bucket Cache (`oauth2_introspection::cache`).
//!
//! ```text
//!                     OAUTH2 TWO-TIER HARDWARE RESIDENCY PIPELINE
//!
//!   [L2 Warm] HeaderWorkspace / Cookie / URI Query (current worker core)
//!        │
//!        ▼
//!   [L1D Cache Line 0: 64B `IntrospectionCache` Control Header (`#[repr(C, align(64))]`)]
//!   +---------------------------------------------------------------------------------------+
//!   | 0x00..0x20  boot_salt: [u8; 32]          (CSPRNG salt for SHA-256 -> u128)            |
//!   | 0x20..0x30  lanes: Option<Box<[CoreCacheLane]>> (`None` when all TTLs == 0)           |
//!   | 0x30..0x38  cache_ttl_secs (u32) | negative_ttl_secs (u32)                            |
//!   | 0x38..0x40  lane_mask (u16) | hot_group_mask (u16) | cold_group_mask | stale/flags    |
//!   +---------------------------------------------------------------------------------------+
//!        │
//!        │ 1. Select core-local lane: `&lanes[current_worker_lane() & lane_mask]`
//!        │ 2. SHA-256(boot_salt || token) -> `fp: u128`
//!        ▼
//!   [Tier 1 — Hot L3 Cap (NUMA-Local, <= 4 MiB Total Across All Cores)]
//!        ├── Hit          ──► Serve headers (or Stale if within 3..=10s window + dispatch shadow)
//!        ├── Evict live   ──► Demote valid (`Allow`/`Forbidden`) victim to Cold DRAM;
//!        │                    never demote `Unauthorized` (prevents 401 spray pollution)
//!        ▼
//!   [Tier 2 — Cold DRAM Spillover (Lazy, <= 2% RAM, max 256 MiB Total)]
//!        └── Unexpired hit ──► Swap/promote entry back into Hot L3 (`cold -> hot`)
//! ```

use std::cell::UnsafeCell;
use std::sync::Arc;
use std::sync::atomic::{AtomicPtr, AtomicU8, Ordering};

use http::header::HeaderValue;
use ring::digest::{Context as DigestContext, SHA256};
use ring::rand::{SecureRandom, SystemRandom};
use velda_core::HeaderKey;

use crate::hook::current_worker_lane;

/// Number of cache slots scanned simultaneously in one 64-byte TagLine SIMD load.
pub const GROUP_WIDTH: usize = 64;

/// Hard upper bound on incoming opaque/JWT token length before SHA-256 hashing (16 KiB).
pub const MAX_OAUTH2_TOKEN_BYTES: usize = 16 * 1024;

/// Maximum aggregate Hot L3 cache budget across all worker cores (`4 MiB`).
pub const MAX_HOT_L3_TOTAL_BYTES: usize = 4 * 1024 * 1024;

/// Maximum aggregate Cold DRAM spillover budget across all cores (`256 MiB`, capped at 2% RAM).
pub const MAX_COLD_DRAM_TOTAL_BYTES: usize = 256 * 1024 * 1024;

/// Percentage ceiling (`2%`) of host physical/cgroup RAM allowed for Cold DRAM spillover.
pub const COLD_DRAM_MEMORY_PERCENT: usize = 2;

const TAG_HASH_MASK: u8 = 0x7F;
const TAG_REJECT_BIT: u8 = 0x80;

const FLAG_VALID_PASS: u8 = 0x01;
const FLAG_FORBIDDEN_SCOPE: u8 = 0x02;
const FLAG_UNAUTHORIZED: u8 = 0x04;
const FLAG_REVALIDATING: u8 = 0x80;

pub type SharedProjectedHeaders = Arc<[(HeaderKey, HeaderValue)]>;

/// Computes the adaptive Stale-While-Revalidate pre-expiry window:
/// `10%` of `cache_ttl_seconds` clamped cleanly into `[3s ..= 10s]` (`0` when TTL <= 3s).
#[inline]
pub const fn compute_stale_refresh_secs(cache_ttl_seconds: u64) -> u64 {
    if cache_ttl_seconds <= 3 {
        0
    } else {
        let ten_pct = cache_ttl_seconds / 10;
        if ten_pct < 3 {
            3
        } else if ten_pct > 10 {
            10
        } else {
            ten_pct
        }
    }
}

/// Computes maximum Cold DRAM spillover bytes (`min(2% RAM, 256 MiB)`).
#[inline]
pub const fn max_oauth2_cold_dram_bytes_for(total_memory_bytes: usize) -> usize {
    let pct = (total_memory_bytes / 100).saturating_mul(COLD_DRAM_MEMORY_PERCENT);
    if pct > MAX_COLD_DRAM_TOTAL_BYTES {
        MAX_COLD_DRAM_TOTAL_BYTES
    } else {
        pct
    }
}

/// Outcome returned by [`IntrospectionCache::lookup`].
#[derive(Clone, Debug)]
pub enum CacheLookup {
    Miss,
    Hit(Option<SharedProjectedHeaders>),
    Stale(Option<SharedProjectedHeaders>),
    Forbidden,
    Unauthorized,
}

/// Outcome produced either by IdP/Vault introspection or a single-flight follower.
#[derive(Clone, Debug)]
pub enum ColdOutcome {
    Allow(Option<SharedProjectedHeaders>),
    Forbidden,
    Unauthorized,
    Unavailable,
}

#[repr(C, align(64))]
#[derive(Clone, Copy)]
pub(crate) struct TagLine([u8; GROUP_WIDTH]);

/// Contiguous 64-way SIMD set-associative bucket residing in L3 or Cold DRAM.
#[repr(C, align(64))]
struct BucketGroup {
    tags: TagLine,
    flags: [u8; GROUP_WIDTH],
    keys: [u128; GROUP_WIDTH],
    expiries: [u64; GROUP_WIDTH],
    reval_at: [u64; GROUP_WIDTH],
    headers: [Option<SharedProjectedHeaders>; GROUP_WIDTH],
    clock_hand: u8,
}

impl BucketGroup {
    fn new() -> Self {
        Self {
            tags: TagLine([0u8; GROUP_WIDTH]),
            flags: [0u8; GROUP_WIDTH],
            keys: [0u128; GROUP_WIDTH],
            expiries: [0u64; GROUP_WIDTH],
            reval_at: [0u64; GROUP_WIDTH],
            headers: std::array::from_fn(|_| None),
            clock_hand: 0,
        }
    }

    /// Selects a slot in O(1): existing matching `fp`, SIMD `0x00` empty, expired, or clock victim.
    #[inline]
    fn select_slot(&mut self, fp: u128, tag7: u8, now_epoch: u64) -> usize {
        let mut hit_mask = match_tag7_mask64(&self.tags, tag7);
        while hit_mask != 0 {
            let idx = hit_mask.trailing_zeros() as usize;
            if self.keys[idx] == fp {
                return idx;
            }
            hit_mask &= hit_mask - 1;
        }

        let empty_mask = empty_slot_mask64(&self.tags);
        if empty_mask != 0 {
            return empty_mask.trailing_zeros() as usize;
        }

        let start = self.clock_hand as usize;
        for offset in 0..GROUP_WIDTH {
            let idx = (start + offset) & (GROUP_WIDTH - 1);
            if self.expiries[idx] <= now_epoch {
                self.clock_hand = ((idx + 1) & (GROUP_WIDTH - 1)) as u8;
                return idx;
            }
        }

        let victim = start & (GROUP_WIDTH - 1);
        self.clock_hand = ((victim + 1) & (GROUP_WIDTH - 1)) as u8;
        victim
    }
}

struct LaneBuckets {
    hot: Option<Box<[BucketGroup]>>,
    cold: Option<Box<[BucketGroup]>>,
}

/// Cache-line isolated per-core Two-Tier bucket container (`align(64)`).
#[repr(C, align(64))]
struct CoreCacheLane {
    lock: AtomicU8,
    hot_groups_per_core: u16,
    cold_groups_per_core: u16,
    hot_base: AtomicPtr<BucketGroup>,
    buckets: UnsafeCell<LaneBuckets>,
}

// Safety: Exclusive mutable access to `buckets` is enforced by acquiring `lock` (`0 -> 1`).
unsafe impl Send for CoreCacheLane {}
unsafe impl Sync for CoreCacheLane {}

struct LaneGuard<'a> {
    lock: &'a AtomicU8,
    buckets: &'a mut LaneBuckets,
    cold_groups_per_core: u16,
}

impl Drop for LaneGuard<'_> {
    #[inline(always)]
    fn drop(&mut self) {
        self.lock.store(0, Ordering::Release);
    }
}

impl CoreCacheLane {
    fn new(hot_groups_per_core: u16, cold_groups_per_core: u16) -> Self {
        Self {
            lock: AtomicU8::new(0),
            hot_groups_per_core,
            cold_groups_per_core,
            hot_base: AtomicPtr::new(std::ptr::null_mut()),
            buckets: UnsafeCell::new(LaneBuckets {
                hot: None,
                cold: None,
            }),
        }
    }

    #[inline(always)]
    fn acquire_and_init_hot(&self) -> LaneGuard<'_> {
        while self
            .lock
            .compare_exchange_weak(0, 1, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            std::hint::spin_loop();
        }
        // Safety: `lock == 1` enforces exclusive access to `self.buckets`.
        let buckets = unsafe { &mut *self.buckets.get() };
        if buckets.hot.is_none() {
            let n = self.hot_groups_per_core as usize;
            let mut vec = Vec::with_capacity(n);
            for _ in 0..n {
                vec.push(BucketGroup::new());
            }
            let boxed = vec.into_boxed_slice();
            self.hot_base
                .store(boxed.as_ptr() as *mut BucketGroup, Ordering::Release);
            buckets.hot = Some(boxed);
        }
        LaneGuard {
            lock: &self.lock,
            buckets,
            cold_groups_per_core: self.cold_groups_per_core,
        }
    }

    #[inline(always)]
    fn acquire_if_init(&self) -> Option<LaneGuard<'_>> {
        if self.hot_base.load(Ordering::Relaxed).is_null() {
            return None;
        }
        while self
            .lock
            .compare_exchange_weak(0, 1, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            std::hint::spin_loop();
        }
        // Safety: `lock == 1` enforces exclusive access to `self.buckets`.
        let buckets = unsafe { &mut *self.buckets.get() };
        if buckets.hot.is_some() {
            Some(LaneGuard {
                lock: &self.lock,
                buckets,
                cold_groups_per_core: self.cold_groups_per_core,
            })
        } else {
            self.lock.store(0, Ordering::Release);
            None
        }
    }
}

struct DisplacedSlot {
    tag: u8,
    flag: u8,
    fp: u128,
    exp: u64,
    reval_at: u64,
    headers: Option<SharedProjectedHeaders>,
}

#[cold]
fn ensure_cold_groups(
    cold_opt: &mut Option<Box<[BucketGroup]>>,
    cold_groups_per_core: u16,
) -> Option<&mut Box<[BucketGroup]>> {
    if cold_groups_per_core == 0 {
        return None;
    }
    Some(cold_opt.get_or_insert_with(|| {
        let n = cold_groups_per_core as usize;
        let mut vec = Vec::with_capacity(n);
        for _ in 0..n {
            vec.push(BucketGroup::new());
        }
        vec.into_boxed_slice()
    }))
}

#[inline]
fn demote_victim_if_valid(
    cold_opt: &mut Option<Box<[BucketGroup]>>,
    cold_groups_per_core: u16,
    cold_group_mask: u16,
    victim: DisplacedSlot,
    now_epoch: u64,
) -> Option<SharedProjectedHeaders> {
    // Only demote unexpired positive/forbidden entries so 401 unauthorized sprays
    // never flush legitimate sessions out of Cold DRAM.
    let eligible = victim.tag != 0
        && victim.exp > now_epoch
        && (victim.flag & FLAG_UNAUTHORIZED) == 0
        && (victim.flag & (FLAG_VALID_PASS | FLAG_FORBIDDEN_SCOPE)) != 0;
    if !eligible {
        return victim.headers;
    }
    let Some(cold_groups) = ensure_cold_groups(cold_opt, cold_groups_per_core) else {
        return victim.headers;
    };
    let cidx = ((victim.fp >> 20) as usize) & (cold_group_mask as usize);
    let cgroup = &mut cold_groups[cidx];
    let cslot = cgroup.select_slot(victim.fp, victim.tag & TAG_HASH_MASK, now_epoch);
    cgroup.tags.0[cslot] = victim.tag;
    cgroup.flags[cslot] = victim.flag & !FLAG_REVALIDATING;
    cgroup.keys[cslot] = victim.fp;
    cgroup.expiries[cslot] = victim.exp;
    cgroup.reval_at[cslot] = victim.reval_at;
    std::mem::replace(&mut cgroup.headers[cslot], victim.headers)
}

#[cfg(target_os = "linux")]
fn probe_per_core_l3_bytes() -> usize {
    let raw_size = std::fs::read_to_string("/sys/devices/system/cpu/cpu0/cache/index3/size")
        .ok()
        .and_then(|s| parse_sysfs_cache_bytes(&s));
    let shared_cpus =
        std::fs::read_to_string("/sys/devices/system/cpu/cpu0/cache/index3/shared_cpu_list")
            .ok()
            .map(|s| count_cpu_list(&s))
            .filter(|&c| c > 0);
    if let (Some(bytes), Some(cpus)) = (raw_size, shared_cpus) {
        return (bytes / cpus).max(128 * 1024);
    }
    1024 * 1024
}

#[cfg(not(target_os = "linux"))]
fn probe_per_core_l3_bytes() -> usize {
    1024 * 1024
}

#[cfg(target_os = "linux")]
fn parse_sysfs_cache_bytes(s: &str) -> Option<usize> {
    let t = s.trim();
    if let Some(k) = t.strip_suffix('K').or_else(|| t.strip_suffix('k')) {
        return k.trim().parse::<usize>().ok().map(|v| v * 1024);
    }
    if let Some(m) = t.strip_suffix('M').or_else(|| t.strip_suffix('m')) {
        return m.trim().parse::<usize>().ok().map(|v| v * 1024 * 1024);
    }
    t.parse::<usize>().ok()
}

#[cfg(target_os = "linux")]
fn count_cpu_list(s: &str) -> usize {
    let mut total = 0usize;
    for part in s.trim().split(',') {
        let p = part.trim();
        if p.is_empty() {
            continue;
        }
        if let Some((a, b)) = p.split_once('-')
            && let (Ok(lo), Ok(hi)) = (a.trim().parse::<usize>(), b.trim().parse::<usize>())
            && hi >= lo
        {
            total += hi - lo + 1;
        } else if p.parse::<usize>().is_ok() {
            total += 1;
        }
    }
    total
}

/// Single-cacheline (64-byte) L1D Control Header orchestrating capped per-core Hot L3 + Cold DRAM buckets.
#[repr(C, align(64))]
pub struct IntrospectionCache {
    boot_salt: [u8; 32],
    lanes: Option<Box<[CoreCacheLane]>>,
    cache_ttl_secs: u32,
    negative_ttl_secs: u32,
    lane_mask: u16,
    hot_group_mask: u16,
    cold_group_mask: u16,
    stale_refresh_secs: u8,
    enabled: u8,
}

const _: () = assert!(std::mem::size_of::<IntrospectionCache>() == 64);
const _: () = assert!(std::mem::align_of::<IntrospectionCache>() == 64);
const _: () = assert!(std::mem::align_of::<BucketGroup>() == 64);
const _: () = assert!(std::mem::offset_of!(BucketGroup, tags) == 0);
const _: () = assert!(std::mem::align_of::<CoreCacheLane>() == 64);
const _: () = assert!(std::mem::size_of::<CoreCacheLane>().is_multiple_of(64));

impl IntrospectionCache {
    /// Constructs a hardware-sized Per-Core Two-Tier cache bounded by L3 LLC (`<= 4 MiB`)
    /// and Cold DRAM (`<= 2% RAM`, max `256 MiB`).
    pub fn new(cache_ttl_secs: u64, negative_ttl_secs: u64) -> Self {
        let cores = velda_core::worker::get_allowed_cores().len().max(1);
        let mem = velda_core::hardware::global_hardware_topology().memory_bytes();
        Self::hardware_sized(cores, mem, cache_ttl_secs, negative_ttl_secs)
    }

    /// Computes exact Per-Core Hot L3 & Cold DRAM sizes from core count and system memory bytes.
    pub fn hardware_sized(
        lane_count: usize,
        total_memory_bytes: usize,
        cache_ttl_secs: u64,
        negative_ttl_secs: u64,
    ) -> Self {
        let num_lanes = lane_count.clamp(1, 512).next_power_of_two();
        let per_core_l3 = probe_per_core_l3_bytes();
        let group_bytes = std::mem::size_of::<BucketGroup>().max(1);

        let global_hot_cap = (MAX_HOT_L3_TOTAL_BYTES / num_lanes).max(group_bytes);
        let hot_budget_bytes = (per_core_l3 / 16)
            .clamp(16 * 1024, 256 * 1024)
            .min(global_hot_cap);
        let raw_hot = (hot_budget_bytes / group_bytes).clamp(1, 64);
        let hot_groups = 1usize << raw_hot.ilog2();

        let dram_ceiling = max_oauth2_cold_dram_bytes_for(total_memory_bytes).max(group_bytes);
        let per_core_cold_bytes = (dram_ceiling / num_lanes).max(group_bytes);
        let raw_cold = (per_core_cold_bytes / group_bytes).clamp(1, 1024);
        let cold_groups = 1usize << raw_cold.ilog2();

        Self::with_tier_sizes(
            num_lanes,
            hot_groups,
            cold_groups,
            cache_ttl_secs,
            negative_ttl_secs,
        )
    }

    /// Constructs a cache with explicit Hot L3 and Cold DRAM group counts per core.
    ///
    /// When `cold_groups_per_core == 0`, Cold DRAM spillover is disabled cleanly.
    pub fn with_tier_sizes(
        lane_count: usize,
        hot_groups_per_core: usize,
        cold_groups_per_core: usize,
        cache_ttl_secs: u64,
        negative_ttl_secs: u64,
    ) -> Self {
        let mut boot_salt = [0u8; 32];
        if SystemRandom::new().fill(&mut boot_salt).is_err() {
            let seed = velda_core::clock::now_unix_nanos();
            for (i, b) in boot_salt.iter_mut().enumerate() {
                *b = ((seed >> ((i & 7) * 8)) as u8) ^ ((i as u8).wrapping_mul(0x9E));
            }
        }

        let pos_ttl = cache_ttl_secs.min(u32::MAX as u64) as u32;
        let neg_ttl = negative_ttl_secs.min(u32::MAX as u64) as u32;
        let stale_secs = compute_stale_refresh_secs(pos_ttl as u64) as u8;

        if pos_ttl == 0 && neg_ttl == 0 {
            return Self {
                boot_salt,
                lanes: None,
                cache_ttl_secs: 0,
                negative_ttl_secs: 0,
                lane_mask: 0,
                hot_group_mask: 0,
                cold_group_mask: 0,
                stale_refresh_secs: 0,
                enabled: 0,
            };
        }

        let num_lanes = lane_count.clamp(1, 512).next_power_of_two();
        let hot_groups = hot_groups_per_core.clamp(1, 256).next_power_of_two();
        let cold_groups = if cold_groups_per_core == 0 {
            0usize
        } else {
            cold_groups_per_core.clamp(1, 1024).next_power_of_two()
        };

        let mut lanes = Vec::with_capacity(num_lanes);
        for _ in 0..num_lanes {
            lanes.push(CoreCacheLane::new(hot_groups as u16, cold_groups as u16));
        }

        Self {
            boot_salt,
            lanes: Some(lanes.into_boxed_slice()),
            cache_ttl_secs: pos_ttl,
            negative_ttl_secs: neg_ttl,
            lane_mask: (num_lanes - 1) as u16,
            hot_group_mask: (hot_groups - 1) as u16,
            cold_group_mask: if cold_groups > 0 {
                (cold_groups - 1) as u16
            } else {
                0
            },
            stale_refresh_secs: stale_secs,
            enabled: 1,
        }
    }

    #[inline(always)]
    pub fn is_enabled(&self) -> bool {
        self.enabled != 0
    }

    #[inline(always)]
    pub fn stale_refresh_secs(&self) -> u64 {
        self.stale_refresh_secs as u64
    }

    #[inline]
    pub fn hot_groups_per_core(&self) -> usize {
        self.lanes
            .as_deref()
            .and_then(|l| l.first())
            .map_or(0, |l| l.hot_groups_per_core as usize)
    }

    #[inline]
    pub fn cold_groups_per_core(&self) -> usize {
        self.lanes
            .as_deref()
            .and_then(|l| l.first())
            .map_or(0, |l| l.cold_groups_per_core as usize)
    }

    #[inline(always)]
    pub fn current_lane_idx(&self) -> usize {
        current_worker_lane() & (self.lane_mask as usize)
    }

    /// Computes a hardware-accelerated 128-bit fingerprint (`SHA-256(boot_salt || 0x00 || token)`).
    #[inline]
    pub fn token_to_u128(&self, token: &str) -> u128 {
        let mut ctx = DigestContext::new(&SHA256);
        ctx.update(&self.boot_salt);
        ctx.update(&[0u8]);
        ctx.update(token.as_bytes());
        let d = ctx.finish();
        let bytes = d.as_ref();
        u128::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15],
        ])
    }

    /// Issues an L1D hardware prefetch (`_MM_HINT_T0` / `pldl1keep`) on the `IntrospectionCache` header.
    #[inline(always)]
    pub fn prefetch_control_l1(&self) {
        let ptr = (self as *const Self) as *const u8;
        #[cfg(target_arch = "x86_64")]
        unsafe {
            core::arch::x86_64::_mm_prefetch(ptr as *const i8, core::arch::x86_64::_MM_HINT_T0);
        }
        #[cfg(target_arch = "aarch64")]
        unsafe {
            core::arch::asm!(
                "prfm pldl1keep, [{0}]",
                in(reg) ptr,
                options(nostack, preserves_flags, readonly)
            );
        }
        #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
        let _ = ptr;
    }

    #[inline(always)]
    fn tag7_from_fp(fp: u128) -> u8 {
        let raw = ((fp >> 120) as u8) & TAG_HASH_MASK;
        if raw == 0 { 1 } else { raw }
    }

    #[inline(always)]
    fn hot_group_index(&self, fp: u128) -> usize {
        ((fp >> 4) as usize) & (self.hot_group_mask as usize)
    }

    /// Performs a lock-free-contended core-local Tier-1 Hot L3 lookup with Cold DRAM promotion.
    #[inline]
    pub fn lookup(&self, fp: u128, now_epoch: u64) -> CacheLookup {
        let Some(lanes) = self.lanes.as_deref() else {
            return CacheLookup::Miss;
        };

        let lane_idx = self.current_lane_idx();
        let lane = &lanes[lane_idx];
        let hot_idx = self.hot_group_index(fp);
        let tag7 = Self::tag7_from_fp(fp);

        let base = lane.hot_base.load(Ordering::Acquire);
        if !base.is_null() {
            // Safety: `hot` slice is fixed size `hot_groups_per_core` once initialized for `IntrospectionCache` lifetime.
            let target_ptr = unsafe { base.add(hot_idx) } as *const u8;
            prefetch_bucket_l3(target_ptr);
        }

        let Some(guard) = lane.acquire_if_init() else {
            return CacheLookup::Miss;
        };
        let Some(hot_groups) = guard.buckets.hot.as_mut() else {
            return CacheLookup::Miss;
        };

        let group = &mut hot_groups[hot_idx];
        let mut mask = match_tag7_mask64(&group.tags, tag7);
        while mask != 0 {
            let idx = mask.trailing_zeros() as usize;
            if group.keys[idx] == fp {
                let exp = group.expiries[idx];
                if exp <= now_epoch {
                    let old_hdrs = group.headers[idx].take();
                    group.tags.0[idx] = 0;
                    group.flags[idx] = 0;
                    group.keys[idx] = 0;
                    group.expiries[idx] = 0;
                    group.reval_at[idx] = 0;
                    drop(guard);
                    drop(old_hdrs);
                    return CacheLookup::Miss;
                }

                let flag = group.flags[idx];
                if (flag & FLAG_UNAUTHORIZED) != 0 {
                    return CacheLookup::Unauthorized;
                }
                if (flag & FLAG_FORBIDDEN_SCOPE) != 0 {
                    return CacheLookup::Forbidden;
                }
                if (flag & FLAG_VALID_PASS) != 0 {
                    let hdrs = group.headers[idx].clone();
                    let stale_window = self.stale_refresh_secs as u64;
                    if stale_window > 0 && exp.saturating_sub(now_epoch) <= stale_window {
                        let reval_in_flight = (flag & FLAG_REVALIDATING) != 0
                            && now_epoch.saturating_sub(group.reval_at[idx]) <= stale_window;
                        if !reval_in_flight {
                            return CacheLookup::Stale(hdrs);
                        }
                    }
                    return CacheLookup::Hit(hdrs);
                }
                return CacheLookup::Miss;
            }
            mask &= mask - 1;
        }

        // Tier 1 miss -> probe lazy Tier 2 Cold DRAM on this lane and promote back into Hot L3.
        let cold_groups_cap = guard.cold_groups_per_core;
        if cold_groups_cap > 0
            && let Some(cold_groups) = guard.buckets.cold.as_mut()
        {
            let cidx = ((fp >> 20) as usize) & (self.cold_group_mask as usize);
            let cgroup = &mut cold_groups[cidx];
            let mut cmask = match_tag7_mask64(&cgroup.tags, tag7);
            while cmask != 0 {
                let cslot = cmask.trailing_zeros() as usize;
                if cgroup.keys[cslot] == fp {
                    let cexp = cgroup.expiries[cslot];
                    let ctag = cgroup.tags.0[cslot];
                    let cflag = cgroup.flags[cslot];
                    let chdrs = cgroup.headers[cslot].take();
                    cgroup.tags.0[cslot] = 0;
                    cgroup.flags[cslot] = 0;
                    cgroup.keys[cslot] = 0;
                    cgroup.expiries[cslot] = 0;
                    cgroup.reval_at[cslot] = 0;

                    if cexp <= now_epoch {
                        drop(guard);
                        drop(chdrs);
                        return CacheLookup::Miss;
                    }

                    // Promote back into Hot L3 while demoting any displaced live victim back to Cold.
                    let hgroup = &mut hot_groups[hot_idx];
                    let hslot = hgroup.select_slot(fp, tag7, now_epoch);
                    let victim = DisplacedSlot {
                        tag: hgroup.tags.0[hslot],
                        flag: hgroup.flags[hslot],
                        fp: hgroup.keys[hslot],
                        exp: hgroup.expiries[hslot],
                        reval_at: hgroup.reval_at[hslot],
                        headers: hgroup.headers[hslot].take(),
                    };
                    hgroup.tags.0[hslot] = ctag;
                    hgroup.flags[hslot] = cflag & !FLAG_REVALIDATING;
                    hgroup.keys[hslot] = fp;
                    hgroup.expiries[hslot] = cexp;
                    hgroup.reval_at[hslot] = 0;
                    hgroup.headers[hslot] = chdrs.clone();

                    let stale_window = self.stale_refresh_secs as u64;
                    let res = if (cflag & FLAG_FORBIDDEN_SCOPE) != 0 {
                        CacheLookup::Forbidden
                    } else if (cflag & FLAG_VALID_PASS) != 0 {
                        if stale_window > 0 && cexp.saturating_sub(now_epoch) <= stale_window {
                            CacheLookup::Stale(chdrs)
                        } else {
                            CacheLookup::Hit(chdrs)
                        }
                    } else {
                        CacheLookup::Unauthorized
                    };

                    let dropped = if victim.fp != fp {
                        demote_victim_if_valid(
                            &mut guard.buckets.cold,
                            cold_groups_cap,
                            self.cold_group_mask,
                            victim,
                            now_epoch,
                        )
                    } else {
                        victim.headers
                    };
                    drop(guard);
                    drop(dropped);
                    return res;
                }
                cmask &= cmask - 1;
            }
        }

        CacheLookup::Miss
    }

    /// Atomically claims the stale-refresh lease on the calling core lane (`false` if another
    /// request on this core already initiated revalidation within `stale_refresh_secs`).
    #[inline]
    pub fn mark_revalidating(&self, fp: u128, now_epoch: u64) -> bool {
        let Some(lanes) = self.lanes.as_deref() else {
            return false;
        };
        let Some(guard) = lanes[self.current_lane_idx()].acquire_if_init() else {
            return false;
        };
        let Some(hot_groups) = guard.buckets.hot.as_mut() else {
            return false;
        };
        let hot_idx = self.hot_group_index(fp);
        let tag7 = Self::tag7_from_fp(fp);
        let group = &mut hot_groups[hot_idx];
        let stale_window = self.stale_refresh_secs as u64;

        let mut mask = match_tag7_mask64(&group.tags, tag7);
        while mask != 0 {
            let idx = mask.trailing_zeros() as usize;
            if group.keys[idx] == fp {
                let flag = group.flags[idx];
                if (flag & FLAG_REVALIDATING) != 0
                    && now_epoch.saturating_sub(group.reval_at[idx]) <= stale_window
                {
                    return false;
                }
                group.flags[idx] = flag | FLAG_REVALIDATING;
                group.reval_at[idx] = now_epoch;
                return true;
            }
            mask &= mask - 1;
        }
        false
    }

    /// Clears the in-flight revalidation bit on the calling core lane.
    #[inline]
    pub fn finish_revalidation(&self, fp: u128) {
        self.finish_revalidation_on_lane(self.current_lane_idx(), fp);
    }

    /// Clears the in-flight revalidation bit on a specific originating core lane.
    pub fn finish_revalidation_on_lane(&self, lane_idx: usize, fp: u128) {
        let Some(lanes) = self.lanes.as_deref() else {
            return;
        };
        let Some(guard) = lanes[lane_idx & (self.lane_mask as usize)].acquire_if_init() else {
            return;
        };
        let Some(hot_groups) = guard.buckets.hot.as_mut() else {
            return;
        };
        let hot_idx = self.hot_group_index(fp);
        let tag7 = Self::tag7_from_fp(fp);
        let group = &mut hot_groups[hot_idx];

        let mut mask = match_tag7_mask64(&group.tags, tag7);
        while mask != 0 {
            let idx = mask.trailing_zeros() as usize;
            if group.keys[idx] == fp {
                group.flags[idx] &= !FLAG_REVALIDATING;
                return;
            }
            mask &= mask - 1;
        }
    }

    /// Inserts or refreshes a verified positive (`Allow` or `Forbidden`) decision on the calling lane.
    #[inline]
    pub fn store_positive(
        &self,
        fp: u128,
        has_required_scopes: bool,
        headers: Option<SharedProjectedHeaders>,
        token_exp_epoch: Option<u64>,
        now_epoch: u64,
    ) {
        self.store_positive_on_lane(
            self.current_lane_idx(),
            fp,
            has_required_scopes,
            headers,
            token_exp_epoch,
            now_epoch,
        );
    }

    /// Inserts or refreshes a verified positive decision on a specific originating core lane.
    pub fn store_positive_on_lane(
        &self,
        lane_idx: usize,
        fp: u128,
        has_required_scopes: bool,
        headers: Option<SharedProjectedHeaders>,
        token_exp_epoch: Option<u64>,
        now_epoch: u64,
    ) {
        if self.cache_ttl_secs == 0 {
            return;
        }
        let mut effective_exp = now_epoch.saturating_add(self.cache_ttl_secs as u64);
        if let Some(exp) = token_exp_epoch {
            effective_exp = effective_exp.min(exp);
        }
        if effective_exp <= now_epoch {
            return;
        }

        let tag_bit = if has_required_scopes {
            0u8
        } else {
            TAG_REJECT_BIT
        };
        let flag_byte = if has_required_scopes {
            FLAG_VALID_PASS
        } else {
            FLAG_FORBIDDEN_SCOPE
        };
        self.write_slot_on_lane(
            lane_idx,
            fp,
            tag_bit,
            flag_byte,
            effective_exp,
            headers,
            now_epoch,
        );
    }

    /// Inserts an invalid/revoked token decision (`401`) on the calling core lane.
    #[inline]
    pub fn store_negative(&self, fp: u128, now_epoch: u64) {
        self.store_negative_on_lane(self.current_lane_idx(), fp, now_epoch);
    }

    /// Inserts an invalid/revoked token decision (`401`) on a specific originating core lane.
    pub fn store_negative_on_lane(&self, lane_idx: usize, fp: u128, now_epoch: u64) {
        if self.negative_ttl_secs == 0 {
            return;
        }
        let expire = now_epoch.saturating_add(self.negative_ttl_secs as u64);
        if expire <= now_epoch {
            return;
        }
        self.write_slot_on_lane(
            lane_idx,
            fp,
            TAG_REJECT_BIT,
            FLAG_UNAUTHORIZED,
            expire,
            None,
            now_epoch,
        );
    }

    #[allow(clippy::too_many_arguments)]
    fn write_slot_on_lane(
        &self,
        lane_idx: usize,
        fp: u128,
        tag_polarity_bit: u8,
        flag_state: u8,
        expires_at: u64,
        headers: Option<SharedProjectedHeaders>,
        now_epoch: u64,
    ) {
        let Some(lanes) = self.lanes.as_deref() else {
            return;
        };
        let lane = &lanes[lane_idx & (self.lane_mask as usize)];
        let hot_idx = self.hot_group_index(fp);
        let tag7 = Self::tag7_from_fp(fp);

        let guard = lane.acquire_and_init_hot();
        let cold_groups_cap = guard.cold_groups_per_core;
        let Some(hot_groups) = guard.buckets.hot.as_mut() else {
            return;
        };

        let group = &mut hot_groups[hot_idx];
        let slot = group.select_slot(fp, tag7, now_epoch);
        let victim = DisplacedSlot {
            tag: group.tags.0[slot],
            flag: group.flags[slot],
            fp: group.keys[slot],
            exp: group.expiries[slot],
            reval_at: group.reval_at[slot],
            headers: group.headers[slot].take(),
        };

        group.tags.0[slot] = tag_polarity_bit | tag7;
        group.flags[slot] = flag_state;
        group.keys[slot] = fp;
        group.expiries[slot] = expires_at;
        group.reval_at[slot] = 0;
        group.headers[slot] = headers;

        let dropped = if victim.fp != fp {
            demote_victim_if_valid(
                &mut guard.buckets.cold,
                cold_groups_cap,
                self.cold_group_mask,
                victim,
                now_epoch,
            )
        } else {
            victim.headers
        };
        drop(guard);
        drop(dropped);
    }

    #[cfg(test)]
    fn is_cold_allocated_on_lane(&self, lane_idx: usize) -> bool {
        let Some(lanes) = self.lanes.as_deref() else {
            return false;
        };
        lanes[lane_idx & (self.lane_mask as usize)]
            .acquire_if_init()
            .is_some_and(|g| g.buckets.cold.is_some())
    }
}

#[inline(always)]
fn prefetch_bucket_l3(ptr: *const u8) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        core::arch::x86_64::_mm_prefetch(ptr as *const i8, core::arch::x86_64::_MM_HINT_T2);
    }
    #[cfg(target_arch = "aarch64")]
    unsafe {
        core::arch::asm!(
            "prfm pldl3keep, [{0}]",
            in(reg) ptr,
            options(nostack, preserves_flags, readonly)
        );
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    let _ = ptr;
}

#[inline(always)]
fn match_tag7_mask64(tags: &TagLine, target7: u8) -> u64 {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    unsafe {
        use core::arch::x86_64::{
            __m256i, _mm256_and_si256, _mm256_cmpeq_epi8, _mm256_load_si256, _mm256_movemask_epi8,
            _mm256_set1_epi8,
        };
        let ptr = tags.0.as_ptr();
        let lo = _mm256_load_si256(ptr as *const __m256i);
        let hi = _mm256_load_si256(ptr.add(32) as *const __m256i);
        let mask7 = _mm256_set1_epi8(0x7F_u8 as i8);
        let needle = _mm256_set1_epi8((target7 & 0x7F) as i8);
        let m_lo = _mm256_movemask_epi8(_mm256_cmpeq_epi8(_mm256_and_si256(lo, mask7), needle))
            as u32 as u64;
        let m_hi = _mm256_movemask_epi8(_mm256_cmpeq_epi8(_mm256_and_si256(hi, mask7), needle))
            as u32 as u64;
        m_lo | (m_hi << 32)
    }

    #[cfg(all(
        target_arch = "x86_64",
        not(target_feature = "avx2"),
        target_feature = "sse2"
    ))]
    unsafe {
        use core::arch::x86_64::{
            __m128i, _mm_and_si128, _mm_cmpeq_epi8, _mm_load_si128, _mm_movemask_epi8,
            _mm_set1_epi8,
        };
        let ptr = tags.0.as_ptr();
        let mask7 = _mm_set1_epi8(0x7F_u8 as i8);
        let needle = _mm_set1_epi8((target7 & 0x7F) as i8);
        let mut out = 0u64;
        let mut chunk = 0usize;
        while chunk < 4 {
            let v = _mm_load_si128(ptr.add(chunk * 16) as *const __m128i);
            let m =
                _mm_movemask_epi8(_mm_cmpeq_epi8(_mm_and_si128(v, mask7), needle)) as u32 as u64;
            out |= m << (chunk * 16);
            chunk += 1;
        }
        out
    }

    #[cfg(target_arch = "aarch64")]
    unsafe {
        use core::arch::aarch64::{vandq_u8, vceqq_u8, vdupq_n_u8, vld1q_u8};
        let ptr = tags.0.as_ptr();
        let mask7 = vdupq_n_u8(0x7F);
        let needle = vdupq_n_u8(target7 & 0x7F);
        let mut out = 0u64;
        let mut chunk = 0usize;
        while chunk < 4 {
            let v = vandq_u8(vld1q_u8(ptr.add(chunk * 16)), mask7);
            let cmp = vceqq_u8(v, needle);
            out |= neon_movemask_u8(cmp) << (chunk * 16);
            chunk += 1;
        }
        out
    }

    #[cfg(not(any(
        all(
            target_arch = "x86_64",
            any(target_feature = "avx2", target_feature = "sse2")
        ),
        target_arch = "aarch64"
    )))]
    {
        match_tag7_mask64_swar(&tags.0, target7)
    }
}

#[inline(always)]
fn empty_slot_mask64(tags: &TagLine) -> u64 {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    unsafe {
        use core::arch::x86_64::{
            __m256i, _mm256_cmpeq_epi8, _mm256_load_si256, _mm256_movemask_epi8,
            _mm256_setzero_si256,
        };
        let ptr = tags.0.as_ptr();
        let zero = _mm256_setzero_si256();
        let lo = _mm256_load_si256(ptr as *const __m256i);
        let hi = _mm256_load_si256(ptr.add(32) as *const __m256i);
        let m_lo = _mm256_movemask_epi8(_mm256_cmpeq_epi8(lo, zero)) as u32 as u64;
        let m_hi = _mm256_movemask_epi8(_mm256_cmpeq_epi8(hi, zero)) as u32 as u64;
        m_lo | (m_hi << 32)
    }

    #[cfg(all(
        target_arch = "x86_64",
        not(target_feature = "avx2"),
        target_feature = "sse2"
    ))]
    unsafe {
        use core::arch::x86_64::{
            __m128i, _mm_cmpeq_epi8, _mm_load_si128, _mm_movemask_epi8, _mm_setzero_si128,
        };
        let ptr = tags.0.as_ptr();
        let zero = _mm_setzero_si128();
        let mut out = 0u64;
        let mut chunk = 0usize;
        while chunk < 4 {
            let v = _mm_load_si128(ptr.add(chunk * 16) as *const __m128i);
            let m = _mm_movemask_epi8(_mm_cmpeq_epi8(v, zero)) as u32 as u64;
            out |= m << (chunk * 16);
            chunk += 1;
        }
        out
    }

    #[cfg(target_arch = "aarch64")]
    unsafe {
        use core::arch::aarch64::{vceqzq_u8, vld1q_u8};
        let ptr = tags.0.as_ptr();
        let mut out = 0u64;
        let mut chunk = 0usize;
        while chunk < 4 {
            let cmp = vceqzq_u8(vld1q_u8(ptr.add(chunk * 16)));
            out |= neon_movemask_u8(cmp) << (chunk * 16);
            chunk += 1;
        }
        out
    }

    #[cfg(not(any(
        all(
            target_arch = "x86_64",
            any(target_feature = "avx2", target_feature = "sse2")
        ),
        target_arch = "aarch64"
    )))]
    {
        empty_slot_mask64_swar(&tags.0)
    }
}

#[cfg(target_arch = "aarch64")]
#[inline(always)]
unsafe fn neon_movemask_u8(cmp: core::arch::aarch64::uint8x16_t) -> u64 {
    unsafe {
        use core::arch::aarch64::{vaddv_u8, vandq_u8, vextq_u8, vreinterpretq_u8_u64};
        const BIT_WEIGHTS: u64 = 0x8040_2010_0804_0201;
        let weights = vreinterpretq_u8_u64(core::arch::aarch64::vdupq_n_u64(BIT_WEIGHTS));
        let masked = vandq_u8(cmp, weights);
        let lo_half = vandq_u8(
            masked,
            vreinterpretq_u8_u64(core::arch::aarch64::vcombine_u64(
                core::arch::aarch64::vcreate_u64(u64::MAX),
                core::arch::aarch64::vcreate_u64(0),
            )),
        );
        let hi_shifted = vextq_u8(masked, masked, 8);
        let hi_half = vandq_u8(
            hi_shifted,
            vreinterpretq_u8_u64(core::arch::aarch64::vcombine_u64(
                core::arch::aarch64::vcreate_u64(u64::MAX),
                core::arch::aarch64::vcreate_u64(0),
            )),
        );
        (vaddv_u8(lo_half) as u64) | ((vaddv_u8(hi_half) as u64) << 8)
    }
}

#[cfg(any(
    test,
    not(any(
        all(
            target_arch = "x86_64",
            any(target_feature = "avx2", target_feature = "sse2")
        ),
        target_arch = "aarch64"
    ))
))]
#[inline(always)]
pub(crate) fn match_tag7_mask64_swar(tags: &[u8; GROUP_WIDTH], target7: u8) -> u64 {
    const LO7: u64 = 0x7F7F_7F7F_7F7F_7F7F;
    const ONES: u64 = 0x0101_0101_0101_0101;
    const MSBS: u64 = 0x8080_8080_8080_8080;

    let needle = ONES.wrapping_mul((target7 & 0x7F) as u64);
    let mut out = 0u64;
    let mut chunk = 0usize;
    while chunk < 8 {
        let base = chunk * 8;
        let word = u64::from_le_bytes([
            tags[base],
            tags[base + 1],
            tags[base + 2],
            tags[base + 3],
            tags[base + 4],
            tags[base + 5],
            tags[base + 6],
            tags[base + 7],
        ]);
        let x = (word & LO7) ^ needle;
        let zero_msbs = !((x | MSBS).wrapping_sub(ONES)) & MSBS;
        out |= pack_msbs_u64(zero_msbs) << base;
        chunk += 1;
    }
    out
}

#[cfg(any(
    test,
    not(any(
        all(
            target_arch = "x86_64",
            any(target_feature = "avx2", target_feature = "sse2")
        ),
        target_arch = "aarch64"
    ))
))]
#[inline(always)]
pub(crate) fn empty_slot_mask64_swar(tags: &[u8; GROUP_WIDTH]) -> u64 {
    const LO7: u64 = 0x7F7F_7F7F_7F7F_7F7F;
    const ONES: u64 = 0x0101_0101_0101_0101;
    const MSBS: u64 = 0x8080_8080_8080_8080;

    let mut out = 0u64;
    let mut chunk = 0usize;
    while chunk < 8 {
        let base = chunk * 8;
        let word = u64::from_le_bytes([
            tags[base],
            tags[base + 1],
            tags[base + 2],
            tags[base + 3],
            tags[base + 4],
            tags[base + 5],
            tags[base + 6],
            tags[base + 7],
        ]);
        let low_zero = !(((word & LO7) | MSBS).wrapping_sub(ONES)) & MSBS;
        let exact_zero = low_zero & !word;
        out |= pack_msbs_u64(exact_zero) << base;
        chunk += 1;
    }
    out
}

#[cfg(any(
    test,
    not(any(
        all(
            target_arch = "x86_64",
            any(target_feature = "avx2", target_feature = "sse2")
        ),
        target_arch = "aarch64"
    ))
))]
#[inline(always)]
const fn pack_msbs_u64(msbs: u64) -> u64 {
    ((msbs >> 7).wrapping_mul(0x0102_0408_1020_4080) >> 56) & 0xFF
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hook::set_current_worker_lane;

    #[test]
    fn test_stale_refresh_window_clamps_to_10_percent_and_3_to_10_secs() {
        assert_eq!(compute_stale_refresh_secs(0), 0);
        assert_eq!(compute_stale_refresh_secs(2), 0);
        assert_eq!(compute_stale_refresh_secs(3), 0);
        assert_eq!(compute_stale_refresh_secs(4), 3);
        assert_eq!(compute_stale_refresh_secs(10), 3);
        assert_eq!(compute_stale_refresh_secs(30), 3);
        assert_eq!(compute_stale_refresh_secs(50), 5);
        assert_eq!(compute_stale_refresh_secs(100), 10);
        assert_eq!(compute_stale_refresh_secs(300), 10);
    }

    #[test]
    fn test_hardware_sizing_caps_hot_l3_and_cold_dram_on_many_cores() {
        let cache = IntrospectionCache::hardware_sized(64, 16 * 1024 * 1024 * 1024, 300, 10);
        let group_bytes = std::mem::size_of::<BucketGroup>();
        assert!(cache.hot_groups_per_core() * 64 * group_bytes <= MAX_HOT_L3_TOTAL_BYTES);
        assert!(cache.cold_groups_per_core() * 64 * group_bytes <= MAX_COLD_DRAM_TOTAL_BYTES);
        assert!(cache.cold_groups_per_core() >= cache.hot_groups_per_core());
    }

    #[test]
    fn test_hot_to_cold_demotion_promotion_and_401_non_demotion() {
        set_current_worker_lane(0);
        let cache = IntrospectionCache::with_tier_sizes(1, 1, 2, 300, 30);
        let now = 1_000_000;

        // Cold DRAM starts unallocated until Hot L3 evicts a valid victim
        assert!(!cache.is_cold_allocated_on_lane(0));

        let valid_fp = cache.token_to_u128("valid_token_0");
        cache.store_positive(valid_fp, true, None, Some(now + 600), now);
        assert!(!cache.is_cold_allocated_on_lane(0));

        // 70 negative (401) tokens fill the 1x64 Hot L3 group and overflow:
        // `valid_fp` demotes cleanly into Cold DRAM, while 401 entries never stay in Cold DRAM!
        for i in 0..70u64 {
            let neg_fp = cache.token_to_u128(&format!("spray_401_{i}"));
            cache.store_negative(neg_fp, now);
        }
        assert!(cache.is_cold_allocated_on_lane(0));

        // Looking up `valid_fp` hits Cold DRAM and promotes it back to Hot L3
        assert!(matches!(
            cache.lookup(valid_fp, now + 5),
            CacheLookup::Hit(_)
        ));

        // Spraying tokens must not evict valid Cold DRAM once overflow stabilizes, whereas 401 tokens
        // evicted from Hot L3 are discarded cleanly and return Miss later.
        let sample_401 = cache.token_to_u128("spray_401_0");
        for i in 0..90u64 {
            let more_fp = cache.token_to_u128(&format!("more_tok_{i}"));
            cache.store_positive(more_fp, true, None, Some(now + 600), now);
        }
        assert!(matches!(
            cache.lookup(sample_401, now + 5),
            CacheLookup::Miss
        ));
    }

    #[test]
    fn test_per_core_isolation_across_lanes() {
        let cache = IntrospectionCache::with_tier_sizes(2, 1, 2, 300, 10);
        let now = 1_000_000;
        let fp = cache.token_to_u128("core0_session");

        set_current_worker_lane(0);
        cache.store_positive(fp, true, None, Some(now + 600), now);
        assert!(matches!(cache.lookup(fp, now + 5), CacheLookup::Hit(_)));

        set_current_worker_lane(1);
        assert!(matches!(cache.lookup(fp, now + 5), CacheLookup::Miss));

        set_current_worker_lane(0);
    }

    #[test]
    fn test_unified_simd_lookup_stale_revalidation_and_negative_flip() {
        set_current_worker_lane(0);
        let cache = IntrospectionCache::with_tier_sizes(1, 2, 4, 100, 10);
        assert_eq!(cache.stale_refresh_secs(), 10);
        let now = 1_000_000;
        let fp = cache.token_to_u128("opaque_tok_1");

        cache.store_positive(fp, true, None, Some(now + 200), now);
        assert!(matches!(cache.lookup(fp, now + 50), CacheLookup::Hit(_)));

        assert!(matches!(cache.lookup(fp, now + 95), CacheLookup::Stale(_)));
        assert!(cache.mark_revalidating(fp, now + 95));
        assert!(!cache.mark_revalidating(fp, now + 96));
        assert!(matches!(cache.lookup(fp, now + 96), CacheLookup::Hit(_)));

        cache.store_negative(fp, now + 96);
        assert!(matches!(
            cache.lookup(fp, now + 97),
            CacheLookup::Unauthorized
        ));
    }

    #[test]
    fn test_swar_and_simd_parity() {
        let mut tags = TagLine([0u8; GROUP_WIDTH]);
        tags.0[0] = 0x22;
        tags.0[1] = 0x23;
        tags.0[63] = 0x80 | 0x22;
        assert_eq!(
            match_tag7_mask64(&tags, 0x22),
            match_tag7_mask64_swar(&tags.0, 0x22)
        );
        assert_eq!(empty_slot_mask64(&tags), empty_slot_mask64_swar(&tags.0));
    }
}
