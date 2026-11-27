//! Two-Tier L1D Control + Share-Nothing Per-Core L3 SIMD Bucket Cache (`jwt_auth::cache`).
//!
//! ```text
//!                     JWT CACHE 3-TIER HARDWARE RESIDENCY PIPELINE
//!
//!   [L2 Warm] HeaderWorkspace / URI (decoded at ingress on current worker core)
//!        │
//!        │ 1. Structural 2-dot guard (`is_plausible_jwt_shape`, ~3 ns, rejects junk early)
//!        ▼
//!   [L1D Cache Line 0: 64B `JwtCache` Control Header (`#[repr(C, align(64))]`, `_MM_HINT_T0`)]
//!   +---------------------------------------------------------------------------------------+
//!   | 0x00..0x20  boot_salt: [u8; 32]           (CSPRNG key for SHA-256 -> u128)            |
//!   | 0x20..0x30  lanes: Option<Box<[CoreCacheLane]>> (None = 0B when cache_ttl_secs == 0)  |
//!   | 0x30..0x38  default_ttl_secs (u32) | negative_ttl_secs (u32)                          |
//!   | 0x38..0x40  lane_mask (u16) | hot_group_mask (u16) | cold_group_mask (u16) | flags   |
//!   +---------------------------------------------------------------------------------------+
//!        │
//!        │ 2. Select core-local lane: `lane = &lanes[current_worker_lane() & lane_mask]`
//!        │    (Dedicated to `velda-worker-{i}` -> 0 Mutex, 0 futex, 0 cross-core Arc/Bytes RFO)
//!        │ 3. SHA-256(boot_salt || jwt_bytes) -> `fp: u128`, `hot_idx = fp & hot_group_mask`
//!        │ 4. Issue `_MM_HINT_T2` (`pldl3keep`) on `&lane.hot_base[hot_idx].tags[0]`
//!        ▼
//!   [Tier 1 — Hot L3 Cap (NUMA-Local, <= 4 MiB Total Across All Cores)] ──► Demotes valid victims
//!        │                                                                      ▼
//!   [Tier 2 — Cold DRAM Spillover (Lazy, <= 2% RAM, max 256 MiB Total)] ◄───────┘
//!     • Cold DRAM hits swap/promote back into Hot L3; 401 sprays never demote into Cold DRAM.
//!        ▼
//!   [L3 Per-Core Slice (First-Touch NUMA-Local): `BucketGroup` (`#[repr(C, align(64))]`, 64-Way)]
//!   +---------------------------------------------------------------------------------------+
//!   | Cache Line 0 (0x00..0x40, 64B): `tags: [u8; 64]`  <-- CONTIGUOUS SIMD INDEX MAP       |
//!   |   • 0x00       : Empty slot                                                           |
//!   |   • 0x01..0x7F : Positive Valid Entry (`tag7`)                                        |
//!   |   • 0x81..0xFF : Negative / Forbidden Entry (`0x80 | tag7`)                           |
//!   +---------------------------------------------------------------------------------------+
//!   | Contiguous Parallel Arrays (indexed ONLY at matched `idx = mask.trailing_zeros()`):   |
//!   |   • keys:      [u128; 64]          (`keys[idx] == fp`, 128-bit integer verify)        |
//!   |   • expiries:  [u64; 64]           (`expiries[idx] > now_epoch`)                      |
//!   |   • key_tags:  [u64; 64]           (`key_table.is_cache_tag_valid(key_tags[idx])`)    |
//!   |   • decisions: [CachedDecision; 64] (isolated per core; old drops happen outside lock)|
//!   +---------------------------------------------------------------------------------------+
//! ```

use std::cell::UnsafeCell;
use std::sync::Arc;
use std::sync::atomic::{AtomicPtr, AtomicU8, Ordering};

use http::header::HeaderValue;
use ring::digest::{Context as DigestContext, SHA256};
use ring::rand::{SecureRandom, SystemRandom};
use velda_core::HeaderKey;

pub use super::simd::GROUP_WIDTH;
use super::simd::{TagLine, empty_slot_mask64, match_tag7_mask64, prefetch_bucket_l3};
use super::store::JwksKeyTable;
use crate::hook::current_worker_lane;

/// Maximum aggregate Hot L3 cache budget across all worker cores (`4 MiB`).
pub const MAX_HOT_L3_TOTAL_BYTES: usize = 4 * 1024 * 1024;

/// Maximum aggregate Cold DRAM spillover budget across all cores (`256 MiB`, capped at 2% RAM).
pub const MAX_COLD_DRAM_TOTAL_BYTES: usize = 256 * 1024 * 1024;

/// Fallback Hot L3 groups per core when using explicit `JwtCache::new`.
pub const DEFAULT_GROUPS_PER_CORE: usize = 16;

/// Probes the physical L3 LLC byte budget available per core on Linux (`shared_cpu_list`),
/// falling back to a conservative `1 MiB` on non-Linux hosts or missing sysfs entries.
fn probe_per_core_l3_bytes() -> usize {
    #[cfg(target_os = "linux")]
    {
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
    }
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

/// Hard upper bound on accepted compact JWT length (16 KiB) before hashing or decoding.
pub const MAX_JWT_TOKEN_BYTES: usize = 16 * 1024;

const TAG_HASH_MASK: u8 = 0x7F;
const TAG_REJECT_BIT: u8 = 0x80;

/// Fast O(1)-amortized structural check rejecting non-JWT junk before hashing or touching cache.
#[inline(always)]
pub fn is_plausible_jwt_shape(token: &str) -> bool {
    let bytes = token.as_bytes();
    if bytes.len() < 5 || bytes.len() > MAX_JWT_TOKEN_BYTES {
        return false;
    }
    let Some(first_dot) = bytes.iter().position(|&b| b == b'.') else {
        return false;
    };
    if first_dot == 0 {
        return false;
    }
    let rest = &bytes[first_dot + 1..];
    let Some(second_rel) = rest.iter().position(|&b| b == b'.') else {
        return false;
    };
    if second_rel == 0 {
        return false;
    }
    let sig = &rest[second_rel + 1..];
    !sig.is_empty() && !sig.contains(&b'.')
}

/// Cached authentication decision stored per slot inside [`BucketGroup`].
#[derive(Clone, Debug)]
pub enum CachedDecision {
    Empty,
    Unauthorized,
    Forbidden,
    /// `None` when `claims_to_headers` produced zero headers (avoids heap allocation & pointer chase).
    Valid(Option<Arc<[(HeaderKey, HeaderValue)]>>),
}

/// Outcome tag returned by lock-free [`JwtCache::lookup_and_apply`] without cloning `Arc`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CacheHitKind {
    Miss,
    Positive,
    Unauthorized,
    Forbidden,
}

/// Outcome returned by test-only [`JwtCache::lookup`] helper.
#[cfg(test)]
#[derive(Clone, Debug)]
pub enum CacheLookup {
    Miss,
    Positive(Option<Arc<[(HeaderKey, HeaderValue)]>>),
    Unauthorized,
    Forbidden,
}

/// Contiguous 64-way SIMD set-associative bucket residing in L3 Shared LLC.
///
/// Cache Line 0 (`0x00..0x40`) holds all 64 1-byte tags so a cache miss inspects
/// exclusively one 64-byte cache line without touching `keys`, `expiries`, or heap pointers.
#[repr(C, align(64))]
struct BucketGroup {
    tags: TagLine,
    keys: [u128; GROUP_WIDTH],
    expiries: [u64; GROUP_WIDTH],
    key_tags: [u64; GROUP_WIDTH],
    decisions: [CachedDecision; GROUP_WIDTH],
    clock_hand: u8,
}

impl BucketGroup {
    fn new() -> Self {
        Self {
            tags: TagLine([0u8; GROUP_WIDTH]),
            keys: [0u128; GROUP_WIDTH],
            expiries: [0u64; GROUP_WIDTH],
            key_tags: [0u64; GROUP_WIDTH],
            decisions: std::array::from_fn(|_| CachedDecision::Empty),
            clock_hand: 0,
        }
    }

    /// Selects a target slot in O(1) via:
    /// 1) Existing matching `fp` overwrite,
    /// 2) SIMD scan for `0x00` empty tag,
    /// 3) Expired / retired-kid slot reclaim,
    /// 4) Bounded clock-hand round-robin replacement.
    #[inline]
    fn select_slot(
        &mut self,
        fp: u128,
        tag7: u8,
        now_epoch: u64,
        key_table: &JwksKeyTable,
    ) -> usize {
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
            if !key_table.is_cache_tag_valid(self.key_tags[idx]) || self.expiries[idx] <= now_epoch
            {
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

/// Cache-line isolated per-core Two-Tier bucket container (`align(64)`, zero false sharing).
///
/// - **Tier 1 (`hot`)**: Strictly capped L3-resident working set (`hot_groups_per_core`).
/// - **Tier 2 (`cold`)**: Lazily allocated NUMA-local Cold DRAM spillover ring (`cold_groups_per_core`),
///   receiving valid unexpired victims evicted from Hot L3 without thrashing physical L3 cache lines.
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
        // Safety: `lock == 1` grants exclusive access to `self.buckets`.
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
        // Safety: `lock == 1` grants exclusive access to `self.buckets`.
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
    fp: u128,
    exp: u64,
    ktag: u64,
    dec: CachedDecision,
}

/// Allocates the Cold DRAM group slice lazily on first valid eviction from Hot L3.
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
    key_table: &JwksKeyTable,
) -> CachedDecision {
    // Only demote unexpired, non-Unauthorized entries (`Positive` or `Forbidden`) so 401 token
    // sprays never flush legitimate cached sessions out of Cold DRAM.
    let eligible = victim.tag != 0
        && victim.exp > now_epoch
        && !matches!(
            victim.dec,
            CachedDecision::Empty | CachedDecision::Unauthorized
        )
        && key_table.is_cache_tag_valid(victim.ktag);
    if !eligible {
        return victim.dec;
    }
    let Some(cold_groups) = ensure_cold_groups(cold_opt, cold_groups_per_core) else {
        return victim.dec;
    };
    let cidx = ((victim.fp >> 16) as usize) & (cold_group_mask as usize);
    let cgroup = &mut cold_groups[cidx];
    let cslot = cgroup.select_slot(victim.fp, victim.tag & TAG_HASH_MASK, now_epoch, key_table);
    cgroup.tags.0[cslot] = victim.tag;
    cgroup.keys[cslot] = victim.fp;
    cgroup.expiries[cslot] = victim.exp;
    cgroup.key_tags[cslot] = victim.ktag;
    std::mem::replace(&mut cgroup.decisions[cslot], victim.dec)
}

/// Single-cacheline (64-byte) L1D Control Header orchestrating capped per-core Hot L3 + Cold DRAM buckets.
#[repr(C, align(64))]
pub struct JwtCache {
    boot_salt: [u8; 32],
    lanes: Option<Box<[CoreCacheLane]>>,
    default_ttl_secs: u32,
    negative_ttl_secs: u32,
    lane_mask: u16,
    hot_group_mask: u16,
    cold_group_mask: u16,
    enabled: u8,
    has_cold: u8,
}

const _: () = assert!(std::mem::size_of::<JwtCache>() == 64);
const _: () = assert!(std::mem::align_of::<JwtCache>() == 64);
const _: () = assert!(std::mem::align_of::<BucketGroup>() == 64);
const _: () = assert!(std::mem::offset_of!(BucketGroup, tags) == 0);
const _: () = assert!(std::mem::align_of::<CoreCacheLane>() == 64);
const _: () = assert!(std::mem::size_of::<CoreCacheLane>().is_multiple_of(64));

impl JwtCache {
    /// Sizes per-core Hot L3 (`<= MAX_HOT_L3_TOTAL_BYTES` shared LLC ceiling) and Cold DRAM
    /// (`<= 2% RAM` capped at `MAX_COLD_DRAM_TOTAL_BYTES`) dynamically from host topology.
    pub fn hardware_sized(
        lane_count: usize,
        default_ttl_secs: u64,
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

        let total_mem = velda_core::hardware::global_hardware_topology().memory_bytes;
        let dram_ceiling = (total_mem / 50).clamp(4 * 1024 * 1024, MAX_COLD_DRAM_TOTAL_BYTES);
        let per_core_cold_bytes = (dram_ceiling / num_lanes).clamp(64 * 1024, 4 * 1024 * 1024);
        let raw_cold = (per_core_cold_bytes / group_bytes).clamp(4, 1024);
        let cold_groups = 1usize << raw_cold.ilog2();

        Self::with_tier_sizes(
            num_lanes,
            hot_groups,
            cold_groups,
            default_ttl_secs,
            negative_ttl_secs,
        )
    }

    /// Constructs a per-core `JwtCache` with explicit Hot L3 groups and proportional (`4x`) Cold DRAM spillover.
    pub fn new(
        lane_count: usize,
        groups_per_core: usize,
        default_ttl_secs: u64,
        negative_ttl_secs: u64,
    ) -> Self {
        let hot = groups_per_core.clamp(1, 256).next_power_of_two();
        let cold = (hot * 4).clamp(4, 1024).next_power_of_two();
        Self::with_tier_sizes(lane_count, hot, cold, default_ttl_secs, negative_ttl_secs)
    }

    /// Constructs a per-core `JwtCache` with explicit Hot L3 and Cold DRAM group counts per core.
    ///
    /// When `cold_groups_per_core == 0`, disables Cold DRAM spillover cleanly.
    pub fn with_tier_sizes(
        lane_count: usize,
        hot_groups_per_core: usize,
        cold_groups_per_core: usize,
        default_ttl_secs: u64,
        negative_ttl_secs: u64,
    ) -> Self {
        let mut boot_salt = [0u8; 32];
        if SystemRandom::new().fill(&mut boot_salt).is_err() {
            let seed = velda_core::clock::now_unix_nanos();
            for (i, b) in boot_salt.iter_mut().enumerate() {
                *b = ((seed >> ((i & 7) * 8)) as u8) ^ ((i as u8).wrapping_mul(0x9E));
            }
        }

        let pos_ttl = default_ttl_secs.min(u32::MAX as u64) as u32;
        let neg_ttl = negative_ttl_secs.min(u32::MAX as u64) as u32;
        if pos_ttl == 0 && neg_ttl == 0 {
            return Self {
                boot_salt,
                lanes: None,
                default_ttl_secs: 0,
                negative_ttl_secs: 0,
                lane_mask: 0,
                hot_group_mask: 0,
                cold_group_mask: 0,
                enabled: 0,
                has_cold: 0,
            };
        }

        let num_lanes = lane_count.clamp(1, 512).next_power_of_two();
        let hot_groups = hot_groups_per_core.clamp(1, 256).next_power_of_two();
        let (cold_groups, has_cold) = if cold_groups_per_core == 0 {
            (0usize, 0u8)
        } else {
            (cold_groups_per_core.clamp(1, 1024).next_power_of_two(), 1u8)
        };
        let lane_mask = (num_lanes - 1) as u16;
        let hot_group_mask = (hot_groups - 1) as u16;
        let cold_group_mask = if cold_groups > 0 {
            (cold_groups - 1) as u16
        } else {
            0
        };

        let mut lanes = Vec::with_capacity(num_lanes);
        for _ in 0..num_lanes {
            lanes.push(CoreCacheLane::new(hot_groups as u16, cold_groups as u16));
        }

        Self {
            boot_salt,
            lanes: Some(lanes.into_boxed_slice()),
            default_ttl_secs: pos_ttl,
            negative_ttl_secs: neg_ttl,
            lane_mask,
            hot_group_mask,
            cold_group_mask,
            enabled: 1,
            has_cold,
        }
    }

    /// Returns `true` when positive or negative caching is enabled.
    #[inline(always)]
    pub fn is_enabled(&self) -> bool {
        self.enabled != 0
    }

    /// Returns the per-boot CSPRNG salt residing inside L1D Cache Line 0.
    #[inline(always)]
    pub fn boot_salt(&self) -> &[u8; 32] {
        &self.boot_salt
    }

    /// Prefetches the 64-byte `JwtCache` control line into L1D (`_MM_HINT_T0` / `pldl1keep`).
    #[inline(always)]
    pub fn prefetch_control_l1(&self) {
        let ptr = self as *const Self as *const u8;
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

    /// Converts an inbound JWT slice (warm in L2) into a 128-bit keyed cryptographic integer
    /// (`Trunc128(SHA256(boot_salt || token))`) on stack with zero heap allocations.
    #[inline]
    pub fn token_to_u128(&self, token: &str) -> u128 {
        let mut ctx = DigestContext::new(&SHA256);
        ctx.update(&self.boot_salt);
        ctx.update(token.as_bytes());
        let digest = ctx.finish();
        let bytes: [u8; 16] = digest.as_ref()[..16]
            .try_into()
            .expect("SHA-256 digest is 32 bytes");
        u128::from_le_bytes(bytes)
    }

    #[inline(always)]
    fn split_fp(&self, fp: u128) -> (usize, usize, usize, u8) {
        let lane_idx = current_worker_lane() & (self.lane_mask as usize);
        let hot_idx = (fp as usize) & (self.hot_group_mask as usize);
        let cold_idx = ((fp >> 16) as usize) & (self.cold_group_mask as usize);
        let tag7 = (((fp >> 56) as u8) & TAG_HASH_MASK).max(1);
        (lane_idx, hot_idx, cold_idx, tag7)
    }

    /// Two-tier per-core lookup applying positive headers inline without cloning `Arc`:
    /// 1. Hot L3 tier (`_MM_HINT_T2` prefetch + 64B SIMD tag scan).
    /// 2. On Hot miss, checks Cold DRAM spillover (`cold_groups`); valid hits apply headers inline,
    ///    promote back to Hot L3, and demote displaced Hot L3 victims down to Cold DRAM cleanly.
    #[inline]
    pub fn lookup_and_apply<F>(
        &self,
        fp: u128,
        now_epoch: u64,
        key_table: &JwksKeyTable,
        mut apply_positive: F,
    ) -> CacheHitKind
    where
        F: FnMut(&[(HeaderKey, HeaderValue)]),
    {
        let Some(lanes) = self.lanes.as_deref() else {
            return CacheHitKind::Miss;
        };

        let (lane_idx, hot_idx, cold_idx, tag7) = self.split_fp(fp);
        let lane = &lanes[lane_idx];

        let base = lane.hot_base.load(Ordering::Relaxed);
        if base.is_null() {
            return CacheHitKind::Miss;
        }
        prefetch_bucket_l3(unsafe { base.add(hot_idx) as *const u8 });

        let Some(guard) = lane.acquire_if_init() else {
            return CacheHitKind::Miss;
        };

        let mut discarded = CachedDecision::Empty;
        let outcome = 'probe: {
            if let Some(hot_groups) = guard.buckets.hot.as_mut() {
                let hgroup = &mut hot_groups[hot_idx];
                let mut mask = match_tag7_mask64(&hgroup.tags, tag7);
                while mask != 0 {
                    let idx = mask.trailing_zeros() as usize;
                    if hgroup.keys[idx] == fp {
                        if !key_table.is_cache_tag_valid(hgroup.key_tags[idx])
                            || hgroup.expiries[idx] <= now_epoch
                        {
                            hgroup.tags.0[idx] = 0;
                            discarded = std::mem::replace(
                                &mut hgroup.decisions[idx],
                                CachedDecision::Empty,
                            );
                            break 'probe CacheHitKind::Miss;
                        }
                        break 'probe match &hgroup.decisions[idx] {
                            CachedDecision::Valid(headers) => {
                                if let Some(pairs) = headers.as_deref() {
                                    apply_positive(pairs);
                                }
                                CacheHitKind::Positive
                            }
                            CachedDecision::Unauthorized => CacheHitKind::Unauthorized,
                            CachedDecision::Forbidden => CacheHitKind::Forbidden,
                            CachedDecision::Empty => CacheHitKind::Miss,
                        };
                    }
                    mask &= mask - 1;
                }
            }

            if self.has_cold != 0
                && let Some(cold_groups) = guard.buckets.cold.as_mut()
            {
                let cgroup = &mut cold_groups[cold_idx];
                let mut cmask = match_tag7_mask64(&cgroup.tags, tag7);
                while cmask != 0 {
                    let cidx = cmask.trailing_zeros() as usize;
                    if cgroup.keys[cidx] == fp {
                        if !key_table.is_cache_tag_valid(cgroup.key_tags[cidx])
                            || cgroup.expiries[cidx] <= now_epoch
                        {
                            cgroup.tags.0[cidx] = 0;
                            discarded = std::mem::replace(
                                &mut cgroup.decisions[cidx],
                                CachedDecision::Empty,
                            );
                            break 'probe CacheHitKind::Miss;
                        }

                        let hit = match &cgroup.decisions[cidx] {
                            CachedDecision::Valid(headers) => {
                                if let Some(pairs) = headers.as_deref() {
                                    apply_positive(pairs);
                                }
                                CacheHitKind::Positive
                            }
                            CachedDecision::Unauthorized => CacheHitKind::Unauthorized,
                            CachedDecision::Forbidden => CacheHitKind::Forbidden,
                            CachedDecision::Empty => CacheHitKind::Miss,
                        };
                        if hit == CacheHitKind::Miss {
                            break 'probe CacheHitKind::Miss;
                        }

                        // Extract Cold DRAM slot before swapping into Hot L3 tier
                        let c_tag = cgroup.tags.0[cidx];
                        let c_exp = cgroup.expiries[cidx];
                        let c_ktag = cgroup.key_tags[cidx];
                        let c_dec =
                            std::mem::replace(&mut cgroup.decisions[cidx], CachedDecision::Empty);
                        cgroup.tags.0[cidx] = 0;

                        if let Some(hot_groups) = guard.buckets.hot.as_mut() {
                            let hgroup = &mut hot_groups[hot_idx];
                            let hslot = hgroup.select_slot(fp, tag7, now_epoch, key_table);
                            let v_tag = hgroup.tags.0[hslot];
                            let v_fp = hgroup.keys[hslot];
                            let v_exp = hgroup.expiries[hslot];
                            let v_ktag = hgroup.key_tags[hslot];
                            let v_dec = std::mem::replace(&mut hgroup.decisions[hslot], c_dec);
                            hgroup.tags.0[hslot] = c_tag;
                            hgroup.keys[hslot] = fp;
                            hgroup.expiries[hslot] = c_exp;
                            hgroup.key_tags[hslot] = c_ktag;

                            // Demote valid displaced Hot victim back into Cold DRAM slot `cidx`
                            let eligible_demote = v_tag != 0
                                && v_exp > now_epoch
                                && !matches!(
                                    v_dec,
                                    CachedDecision::Empty | CachedDecision::Unauthorized
                                )
                                && key_table.is_cache_tag_valid(v_ktag);
                            if eligible_demote {
                                cgroup.tags.0[cidx] = v_tag;
                                cgroup.keys[cidx] = v_fp;
                                cgroup.expiries[cidx] = v_exp;
                                cgroup.key_tags[cidx] = v_ktag;
                                cgroup.decisions[cidx] = v_dec;
                            } else {
                                discarded = v_dec;
                            }
                        } else {
                            discarded = c_dec;
                        }

                        break 'probe hit;
                    }
                    cmask &= cmask - 1;
                }
            }

            CacheHitKind::Miss
        };

        drop(guard);
        drop(discarded);
        outcome
    }

    /// Test-only helper returning cloned cached headers directly.
    #[cfg(test)]
    pub fn lookup(&self, fp: u128, now_epoch: u64, key_table: &JwksKeyTable) -> CacheLookup {
        let mut cloned: Option<Arc<[(HeaderKey, HeaderValue)]>> = None;
        match self.lookup_and_apply(fp, now_epoch, key_table, |pairs| {
            cloned = Some(Arc::from(pairs));
        }) {
            CacheHitKind::Miss => CacheLookup::Miss,
            CacheHitKind::Positive => CacheLookup::Positive(cloned),
            CacheHitKind::Unauthorized => CacheLookup::Unauthorized,
            CacheHitKind::Forbidden => CacheLookup::Forbidden,
        }
    }

    /// Inserts a verified token decision into Hot L3, demoting any valid unexpired occupant down
    /// to Cold DRAM spillover and dropping expired/rejected waste outside the lane lock.
    #[inline]
    pub fn insert_valid(
        &self,
        fp: u128,
        exp_epoch_secs: u64,
        headers: Option<Arc<[(HeaderKey, HeaderValue)]>>,
        now_epoch: u64,
        key_tag: u64,
        key_table: &JwksKeyTable,
    ) {
        if self.default_ttl_secs == 0 {
            return;
        }
        let Some(lanes) = self.lanes.as_deref() else {
            return;
        };
        let effective_exp =
            exp_epoch_secs.min(now_epoch.saturating_add(self.default_ttl_secs as u64));
        if effective_exp <= now_epoch {
            return;
        }

        let (lane_idx, hot_idx, _, tag7) = self.split_fp(fp);
        let lane = &lanes[lane_idx];
        let base = lane.hot_base.load(Ordering::Relaxed);
        if !base.is_null() {
            prefetch_bucket_l3(unsafe { base.add(hot_idx) as *const u8 });
        }

        let evicted = {
            let guard = lane.acquire_and_init_hot();
            let LaneBuckets { hot, cold } = guard.buckets;
            let hgroup = &mut hot.as_mut().unwrap()[hot_idx];
            let slot = hgroup.select_slot(fp, tag7, now_epoch, key_table);
            let old_tag = hgroup.tags.0[slot];
            let old_fp = hgroup.keys[slot];
            let old_exp = hgroup.expiries[slot];
            let old_ktag = hgroup.key_tags[slot];
            let old_dec =
                std::mem::replace(&mut hgroup.decisions[slot], CachedDecision::Valid(headers));
            hgroup.tags.0[slot] = tag7;
            hgroup.keys[slot] = fp;
            hgroup.expiries[slot] = effective_exp;
            hgroup.key_tags[slot] = key_tag;

            if old_fp != fp {
                demote_victim_if_valid(
                    cold,
                    guard.cold_groups_per_core,
                    self.cold_group_mask,
                    DisplacedSlot {
                        tag: old_tag,
                        fp: old_fp,
                        exp: old_exp,
                        ktag: old_ktag,
                        dec: old_dec,
                    },
                    now_epoch,
                    key_table,
                )
            } else {
                old_dec
            }
        };
        drop(evicted);
    }

    /// Inserts an invalid (`401`) or claim-rejected (`403`) token into Hot L3.
    /// Displaced valid occupants safely demote to Cold DRAM; `401` spray victims drop directly.
    #[inline]
    pub fn insert_reject(
        &self,
        fp: u128,
        is_forbidden: bool,
        now_epoch: u64,
        key_tag: u64,
        key_table: &JwksKeyTable,
    ) {
        if self.negative_ttl_secs == 0 {
            return;
        }
        let Some(lanes) = self.lanes.as_deref() else {
            return;
        };
        let expire = now_epoch.saturating_add(self.negative_ttl_secs as u64);

        let (lane_idx, hot_idx, _, tag7) = self.split_fp(fp);
        let lane = &lanes[lane_idx];
        let base = lane.hot_base.load(Ordering::Relaxed);
        if !base.is_null() {
            prefetch_bucket_l3(unsafe { base.add(hot_idx) as *const u8 });
        }

        let new_decision = if is_forbidden {
            CachedDecision::Forbidden
        } else {
            CachedDecision::Unauthorized
        };
        let evicted = {
            let guard = lane.acquire_and_init_hot();
            let LaneBuckets { hot, cold } = guard.buckets;
            let hgroup = &mut hot.as_mut().unwrap()[hot_idx];
            let slot = hgroup.select_slot(fp, tag7, now_epoch, key_table);
            let old_tag = hgroup.tags.0[slot];
            let old_fp = hgroup.keys[slot];
            let old_exp = hgroup.expiries[slot];
            let old_ktag = hgroup.key_tags[slot];
            let old_dec = std::mem::replace(&mut hgroup.decisions[slot], new_decision);
            hgroup.tags.0[slot] = TAG_REJECT_BIT | tag7;
            hgroup.keys[slot] = fp;
            hgroup.expiries[slot] = expire;
            hgroup.key_tags[slot] = key_tag;

            if old_fp != fp {
                demote_victim_if_valid(
                    cold,
                    guard.cold_groups_per_core,
                    self.cold_group_mask,
                    DisplacedSlot {
                        tag: old_tag,
                        fp: old_fp,
                        exp: old_exp,
                        ktag: old_ktag,
                        dec: old_dec,
                    },
                    now_epoch,
                    key_table,
                )
            } else {
                old_dec
            }
        };
        drop(evicted);
    }
}

#[cfg(test)]
mod tests {
    use super::super::simd::{
        empty_slot_mask64, empty_slot_mask64_swar, match_tag7_mask64, match_tag7_mask64_swar,
    };
    use super::*;
    use crate::hook::set_current_worker_lane;
    use http::header::HeaderName;

    #[test]
    fn test_control_header_and_bucket_cacheline_geometry() {
        assert_eq!(std::mem::size_of::<JwtCache>(), 64);
        assert_eq!(std::mem::align_of::<JwtCache>(), 64);
        assert_eq!(std::mem::align_of::<BucketGroup>(), 64);
        assert_eq!(std::mem::offset_of!(BucketGroup, tags), 0);
        assert_eq!(std::mem::align_of::<CoreCacheLane>(), 64);
        assert_eq!(std::mem::size_of::<CoreCacheLane>() % 64, 0);

        let cache = JwtCache::new(8, 4, 300, 10);
        assert_eq!(cache.hot_group_mask, 3);
        let lanes = cache.lanes.as_deref().unwrap();
        for lane in lanes {
            let lane_addr = (lane as *const CoreCacheLane) as usize;
            assert_eq!(lane_addr % 64, 0);
            let guard = lane.acquire_and_init_hot();
            let group_addr = guard.buckets.hot.as_ref().unwrap().as_ptr() as usize;
            assert_eq!(group_addr % 64, 0);
        }
    }

    #[test]
    fn test_hardware_sized_respects_l3_cap_on_64_cores() {
        let cache = JwtCache::hardware_sized(64, 300, 10);
        let lanes = cache.lanes.as_deref().unwrap();
        assert_eq!(lanes.len(), 64);
        let total_hot_bytes = lanes.len()
            * ((cache.hot_group_mask as usize) + 1)
            * std::mem::size_of::<BucketGroup>();
        assert!(total_hot_bytes <= MAX_HOT_L3_TOTAL_BYTES);
        assert!(cache.cold_group_mask >= 15);
    }

    #[test]
    fn test_hot_l3_eviction_demotes_to_cold_dram_and_promotes_on_hit() {
        use super::super::crypto::{JwtAlgorithm, VerifierKey};

        // 1 Hot group (64 slots) + 4 Cold DRAM groups (256 slots)
        let cache = JwtCache::with_tier_sizes(1, 1, 4, 300, 10);
        set_current_worker_lane(0);
        let now = 1_000_000;
        let k = VerifierKey::new(JwtAlgorithm::HS256, Some("sec"), None).unwrap();
        let table = JwksKeyTable::with_static_key(k, JwtAlgorithm::HS256, cache.boot_salt());
        let tag = table.key_tags.0[0];

        // Overflow Hot L3 (70 valid entries into 64-slot Hot tier) -> oldest 6 must demote to Cold DRAM
        for i in 0..70u128 {
            let fp = (i << 64) | (i.wrapping_mul(0x9E37_79B9_7F4A_7C15));
            cache.insert_valid(fp, now + 100, None, now, tag, &table);
        }

        // Token 0 was evicted from Hot L3 but MUST hit in Cold DRAM (and promote back to Hot L3)
        let fp0 = 0u128;
        assert!(matches!(
            cache.lookup(fp0, now + 1, &table),
            CacheLookup::Positive(None)
        ));

        // 401 spray victims MUST NEVER demote into Cold DRAM
        let fp_bad = (999u128 << 64) | 0xCAFE;
        cache.insert_reject(fp_bad, false, now, table.dir.table_version_tag, &table);
        for i in 100..170u128 {
            let fp = (i << 64) | (i.wrapping_mul(0x9E37_79B9_7F4A_7C15));
            cache.insert_valid(fp, now + 100, None, now, tag, &table);
        }
        assert!(matches!(
            cache.lookup(fp_bad, now + 1, &table),
            CacheLookup::Miss
        ));
    }

    #[test]
    fn test_disabled_cache_allocates_zero_lanes() {
        let cache = JwtCache::new(8, 16, 0, 0);
        assert!(!cache.is_enabled());
        assert!(cache.lanes.is_none());
        let fp = cache.token_to_u128("a.b.c");
        let table = JwksKeyTable::empty();
        assert!(matches!(cache.lookup(fp, 100, &table), CacheLookup::Miss));
    }

    #[test]
    fn test_per_core_lane_isolation_no_cross_lane_leakage() {
        use super::super::crypto::{JwtAlgorithm, VerifierKey};

        let cache = JwtCache::new(4, 4, 300, 10);
        let now = 1_000_000;
        let k = VerifierKey::new(JwtAlgorithm::HS256, Some("sec"), None).unwrap();
        let table = JwksKeyTable::with_static_key(k, JwtAlgorithm::HS256, cache.boot_salt());
        let tag = table.key_tags.0[0];
        let fp = cache.token_to_u128("hdr.pay.sig");

        set_current_worker_lane(0);
        cache.insert_valid(fp, now + 100, None, now, tag, &table);
        assert!(matches!(
            cache.lookup(fp, now + 1, &table),
            CacheLookup::Positive(None)
        ));

        // Core 1 must have its own completely isolated L3 slice (first touch returns Miss)
        set_current_worker_lane(1);
        assert!(matches!(
            cache.lookup(fp, now + 1, &table),
            CacheLookup::Miss
        ));
        cache.insert_valid(fp, now + 100, None, now, tag, &table);
        assert!(matches!(
            cache.lookup(fp, now + 1, &table),
            CacheLookup::Positive(None)
        ));

        set_current_worker_lane(0);
    }

    #[test]
    fn test_positive_and_negative_unified_simd_lookup_and_kid_tag_invalidation() {
        use super::super::crypto::{JwtAlgorithm, VerifierKey};
        use super::super::store::KeyEntrySpec;

        let cache = JwtCache::new(8, 4, 300, 10);
        let now = 1_000_000;
        let k1 = VerifierKey::new(JwtAlgorithm::HS256, Some("sec-1"), None).unwrap();
        let k2 = VerifierKey::new(JwtAlgorithm::HS512, Some("sec-2-longer"), None).unwrap();

        let table1 = JwksKeyTable::build_from_entries(
            &[KeyEntrySpec {
                raw_kid: Some("kid-1".into()),
                alg: JwtAlgorithm::HS256,
                key: k1.clone(),
            }],
            0,
            false,
            true,
            cache.boot_salt(),
        )
        .unwrap();
        let k1_tag = table1.key_tags.0[0];
        let tbl_ver = table1.dir.table_version_tag;

        let tok_ok = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ1MSJ9.c2lnMQ";
        let tok_bad = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ1MiJ9.YmFkc2ln";
        let tok_forbid = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ1MyJ9.Zm9yYmlk";

        let fp_ok = cache.token_to_u128(tok_ok);
        let fp_bad = cache.token_to_u128(tok_bad);
        let fp_forbid = cache.token_to_u128(tok_forbid);

        let headers: Arc<[(HeaderKey, HeaderValue)]> = Arc::from([(
            HeaderKey::from_name(HeaderName::from_static("x-user-id")),
            HeaderValue::from_static("usr_42"),
        )]);

        cache.insert_valid(fp_ok, now + 100, Some(headers), now, k1_tag, &table1);
        cache.insert_reject(fp_bad, false, now, tbl_ver, &table1);
        cache.insert_reject(fp_forbid, true, now, k1_tag, &table1);

        match cache.lookup(fp_ok, now + 50, &table1) {
            CacheLookup::Positive(Some(hdrs)) => assert_eq!(hdrs[0].1, "usr_42"),
            other => panic!("expected Positive hit, got {other:?}"),
        }
        assert!(matches!(
            cache.lookup(fp_bad, now + 5, &table1),
            CacheLookup::Unauthorized
        ));
        assert!(matches!(
            cache.lookup(fp_forbid, now + 5, &table1),
            CacheLookup::Forbidden
        ));

        // Expiration enforced & eagerly reclaimed
        assert!(matches!(
            cache.lookup(fp_ok, now + 101, &table1),
            CacheLookup::Miss
        ));
        assert!(matches!(
            cache.lookup(fp_bad, now + 11, &table1),
            CacheLookup::Miss
        ));

        // Re-populate active entries at current time before testing rolling overlap
        let headers2: Arc<[(HeaderKey, HeaderValue)]> = Arc::from([(
            HeaderKey::from_name(HeaderName::from_static("x-user-id")),
            HeaderValue::from_static("usr_42"),
        )]);
        cache.insert_valid(fp_ok, now + 100, Some(headers2), now, k1_tag, &table1);
        cache.insert_reject(fp_bad, false, now, tbl_ver, &table1);

        // Rolling overlap adding k2 preserves k1 cache hit!
        let table2 = JwksKeyTable::build_from_entries(
            &[
                KeyEntrySpec {
                    raw_kid: Some("kid-2".into()),
                    alg: JwtAlgorithm::HS512,
                    key: k2.clone(),
                },
                KeyEntrySpec {
                    raw_kid: Some("kid-1".into()),
                    alg: JwtAlgorithm::HS256,
                    key: k1,
                },
            ],
            0,
            false,
            true,
            cache.boot_salt(),
        )
        .unwrap();
        assert!(matches!(
            cache.lookup(fp_ok, now + 10, &table2),
            CacheLookup::Positive(_)
        ));
        // Unbound reject misses on rotation so newly introduced keys take effect
        assert!(matches!(
            cache.lookup(fp_bad, now + 5, &table2),
            CacheLookup::Miss
        ));

        // Retiring k1 invalidates k1's cached entries immediately
        let table3 = JwksKeyTable::build_from_entries(
            &[KeyEntrySpec {
                raw_kid: Some("kid-2".into()),
                alg: JwtAlgorithm::HS512,
                key: k2,
            }],
            0,
            false,
            true,
            cache.boot_salt(),
        )
        .unwrap();
        assert!(matches!(
            cache.lookup(fp_ok, now + 10, &table3),
            CacheLookup::Miss
        ));
    }

    #[test]
    fn test_64_way_bucket_saturation_and_clock_eviction() {
        use super::super::crypto::{JwtAlgorithm, VerifierKey};

        let cache = JwtCache::new(1, 1, 300, 10);
        let now = 1_000_000;
        let k = VerifierKey::new(JwtAlgorithm::HS256, Some("sec"), None).unwrap();
        let table = JwksKeyTable::with_static_key(k, JwtAlgorithm::HS256, cache.boot_salt());
        let tag = table.key_tags.0[0];

        for i in 0..80u128 {
            let fp = (i << 64) | (i * 0x9E37_79B9_7F4A_7C15);
            cache.insert_valid(fp, now + 100, None, now, tag, &table);
        }

        let last_fp = (79u128 << 64) | (79u128 * 0x9E37_79B9_7F4A_7C15);
        assert!(matches!(
            cache.lookup(last_fp, now + 10, &table),
            CacheLookup::Positive(None)
        ));
    }

    #[test]
    fn test_swar_parity_against_simd_and_adjacent_borrow_edges() {
        let mut tags = TagLine([0u8; GROUP_WIDTH]);
        // Adjacent bytes `[target, target ^ 1, 0x80 | target, 0x00, 0x80, 0xFF]` test borrow safety
        tags.0[0] = 0x2A;
        tags.0[1] = 0x2B;
        tags.0[2] = 0x80 | 0x2A;
        tags.0[3] = 0x01;
        tags.0[15] = 0x2A;
        tags.0[31] = 0x80 | 0x2A;
        tags.0[32] = 0x2B;
        tags.0[63] = 0x2A;

        let expected_match = (1u64 << 0) | (1u64 << 2) | (1u64 << 15) | (1u64 << 31) | (1u64 << 63);
        assert_eq!(match_tag7_mask64(&tags, 0x2A), expected_match);
        assert_eq!(match_tag7_mask64_swar(&tags.0, 0x2A), expected_match);

        tags.0[4] = 0x80; // MSB set, low 7 bits zero -> NOT empty!
        let simd_empty = empty_slot_mask64(&tags);
        let swar_empty = empty_slot_mask64_swar(&tags.0);
        assert_eq!(simd_empty, swar_empty);
        assert_eq!(simd_empty & (1u64 << 4), 0);
        assert_ne!(simd_empty & (1u64 << 5), 0);

        // All 64 lanes matching simultaneously verifies zero carry into MSB byte
        let all_match = TagLine([0x2A; GROUP_WIDTH]);
        assert_eq!(match_tag7_mask64(&all_match, 0x2A), u64::MAX);
        assert_eq!(match_tag7_mask64_swar(&all_match.0, 0x2A), u64::MAX);
        let all_zero = TagLine([0u8; GROUP_WIDTH]);
        assert_eq!(empty_slot_mask64(&all_zero), u64::MAX);
        assert_eq!(empty_slot_mask64_swar(&all_zero.0), u64::MAX);
    }

    #[test]
    fn test_plausible_jwt_shape_guard() {
        assert!(is_plausible_jwt_shape("a.b.c"));
        assert!(!is_plausible_jwt_shape(""));
        assert!(!is_plausible_jwt_shape("no-dots"));
        assert!(!is_plausible_jwt_shape("only.one"));
        assert!(!is_plausible_jwt_shape(".b.c"));
        assert!(!is_plausible_jwt_shape("a..c"));
        assert!(!is_plausible_jwt_shape("a.b."));
        assert!(!is_plausible_jwt_shape("a.b.c.d"));
    }
}
