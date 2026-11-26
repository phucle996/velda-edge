//! Two-Tier L1D Control + Dynamic 10%-RAM-Capped L3 Contiguous SIMD Introspection Cache (`oauth2_introspection::cache`).
//!
//! ```text
//!               OAUTH2 INTROSPECTION L1D + L3 SIMD BUCKET CACHE
//!
//!   [L2 Warm] Request Token (&str)
//!        │
//!        ▼
//!   [L1D Control Line 0: 64B `IntrospectionCache` (`#[repr(C, align(64))]`, `_MM_HINT_T0`)]
//!   +---------------------------------------------------------------------------------------+
//!   | 0x00..0x20  boot_salt: [u8; 32]        (CSPRNG key for SHA-256 -> u128)               |
//!   | 0x20..0x30  shards: Option<Box<[Shard]>> (`None` = 0B RAM when TTLs are 0)            |
//!   | 0x30..0x38  cache_ttl_secs (u32) | negative_ttl_secs (u32)                            |
//!   | 0x38..0x40  stale_refresh_secs | enabled | shard_mask | max_groups_log2 | shard_shift |
//!   +---------------------------------------------------------------------------------------+
//!        │
//!        │ 1. SHA-256(boot_salt || token) -> `fp: u128`
//!        │ 2. `stale_refresh_secs = (ttl / 10).clamp(3, 10)` when `ttl > 3`, else `0`
//!        │ 3. Dynamically scales shard groups up to 10% host RAM (`velda-core` topology),
//!        │    engaging O(1) clock-hand eviction once the 10% RAM ceiling is reached
//!        ▼
//!   [L3 Shared LLC: Contiguous 64-Way SIMD `BucketGroup` (`#[repr(C, align(64))]`)]
//!   +---------------------------------------------------------------------------------------+
//!   | Cache Line 0 (`0x00..0x40`): `tags: TagLine([u8; 64])` (_MM_HINT_T2 prefetched)       |
//!   |   • `0x00`       : Empty                                                              |
//!   |   • `0x01..0x7F` : Positive ValidPass entry (`tag7`)                                  |
//!   |   • `0x81..0xFF` : Reject entry (`0x80 | tag7`: InsufficientScope 403 / Invalid 401)  |
//!   +---------------------------------------------------------------------------------------+
//!   | Contiguous Parallel Arrays:                                                           |
//!   |   • flags: [u8; 64] | keys: [u128; 64] | expiries: [u64; 64] | reval_at: [u64; 64]    |
//!   |   • headers: [Option<Arc<[(HeaderKey, HeaderValue)]>>; 64]                            |
//!   +---------------------------------------------------------------------------------------+
//! ```

use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use http::header::HeaderValue;
use ring::digest::{Context as DigestContext, SHA256};
use ring::rand::{SecureRandom, SystemRandom};
use velda_core::HeaderKey;

/// Shared heap-projected `(HeaderKey, HeaderValue)` slice cached across L3 slots.
pub type SharedProjectedHeaders = Arc<[(HeaderKey, HeaderValue)]>;

/// Number of slots per contiguous SIMD bucket group (exact 64B cache line of `u8` tags).
pub const GROUP_WIDTH: usize = 64;

/// Maximum percentage of system/cgroup RAM (`velda-core`) permitted for OAuth2 introspection cache.
pub const OAUTH2_CACHE_MEMORY_CEILING_PERCENT: usize = 10;

/// Initial groups allocated per shard (`64 * 4 * 64 = 16,384` slots ≈ ~835 KiB upfront).
const INITIAL_GROUPS_PER_SHARD: usize = 4;

/// Worst-case per-group memory budget (flat `BucketGroup` struct + average projected header `Arc` slices).
const ESTIMATED_GROUP_FOOTPRINT_BYTES: usize = std::mem::size_of::<BucketGroup>() + (64 * 64);

const TAG_HASH_MASK: u8 = 0x7F;
const TAG_REJECT_BIT: u8 = 0x80;

const FLAG_VALID_PASS: u8 = 1;
const FLAG_FORBIDDEN_SCOPE: u8 = 2;
const FLAG_UNAUTHORIZED: u8 = 3;
const FLAG_STATE_MASK: u8 = 0x0F;
const FLAG_REVALIDATING: u8 = 0x80;

/// Computes the stale-while-revalidate refresh lead window (`10%` of TTL clamped to `[3s, 10s]`, or `0` for `ttl <= 3`).
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

/// Computes the maximum RAM byte ceiling (`10%` of host/cgroup physical RAM from `velda-core`).
#[inline]
pub const fn max_oauth2_cache_bytes_for(total_memory_bytes: usize) -> usize {
    (total_memory_bytes / 100).saturating_mul(OAUTH2_CACHE_MEMORY_CEILING_PERCENT)
}

/// Returns `(shard_count, initial_groups_per_shard, max_groups_per_shard)` sized cleanly to hardware
/// cores while strictly bounding total memory within `10%` of host RAM.
pub fn compute_cache_geometry(total_memory_bytes: usize, cores: usize) -> (usize, usize, usize) {
    let budget = max_oauth2_cache_bytes_for(total_memory_bytes);
    let mut num_shards = (cores.max(1) * 4).next_power_of_two().clamp(16, 64);

    // Ensure tiny memory containers still fit at least 1 group per shard within 10% RAM
    while num_shards > 8 && num_shards * ESTIMATED_GROUP_FOOTPRINT_BYTES > budget {
        num_shards /= 2;
    }

    let raw_max_groups = (budget / (num_shards * ESTIMATED_GROUP_FOOTPRINT_BYTES)).max(1);
    let max_groups_log2 = (usize::BITS - 1 - raw_max_groups.leading_zeros()).min(15);
    let max_groups_per_shard = 1usize << max_groups_log2;
    let init_groups = INITIAL_GROUPS_PER_SHARD.min(max_groups_per_shard);

    (num_shards, init_groups, max_groups_per_shard)
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

/// Outcome produced either by IdP introspection or a single-flight follower.
#[derive(Clone, Debug)]
pub enum ColdOutcome {
    Allow(Option<SharedProjectedHeaders>),
    Forbidden,
    Unauthorized,
    Unavailable,
}

#[repr(C, align(64))]
#[derive(Clone, Copy)]
struct TagLine([u8; GROUP_WIDTH]);

/// Contiguous 64-way SIMD set-associative bucket residing in L3 Shared LLC.
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

    /// Attempts to place or overwrite a slot without eviction:
    /// 1) Existing matching `fp` slot, 2) SIMD `0x00` empty slot, or 3) expired slot.
    ///
    /// Returns `None` if all 64 slots are live and unexpired (signaling shard growth headroom check).
    #[inline]
    fn try_find_free_or_existing(&mut self, fp: u128, tag7: u8, now_epoch: u64) -> Option<usize> {
        let mut hit_mask = match_tag7_mask64(&self.tags, tag7);
        while hit_mask != 0 {
            let idx = hit_mask.trailing_zeros() as usize;
            if self.keys[idx] == fp {
                return Some(idx);
            }
            hit_mask &= hit_mask - 1;
        }

        let empty_mask = empty_slot_mask64(&self.tags);
        if empty_mask != 0 {
            return Some(empty_mask.trailing_zeros() as usize);
        }

        let start = self.clock_hand as usize;
        for offset in 0..GROUP_WIDTH {
            let idx = (start + offset) & (GROUP_WIDTH - 1);
            if self.expiries[idx] <= now_epoch {
                self.clock_hand = ((idx + 1) & (GROUP_WIDTH - 1)) as u8;
                return Some(idx);
            }
        }

        None
    }

    /// Advances the bounded O(1) clock hand to evict one slot when the 10% RAM ceiling is reached.
    #[inline]
    fn evict_clock(&mut self) -> usize {
        let victim = (self.clock_hand as usize) & (GROUP_WIDTH - 1);
        self.clock_hand = ((victim + 1) & (GROUP_WIDTH - 1)) as u8;
        victim
    }
}

struct ShardInner {
    groups: Box<[BucketGroup]>,
}

/// Cache-line isolated shard container (`size % 64 == 0`, zero false sharing across CPU cores).
#[repr(C, align(64))]
struct CacheShard {
    groups_base: AtomicPtr<BucketGroup>,
    group_mask: AtomicUsize,
    inner: RwLock<ShardInner>,
}

unsafe impl Send for CacheShard {}
unsafe impl Sync for CacheShard {}

impl CacheShard {
    fn new(init_groups: usize) -> Self {
        let mut vec = Vec::with_capacity(init_groups);
        for _ in 0..init_groups {
            vec.push(BucketGroup::new());
        }
        let mut boxed = vec.into_boxed_slice();
        let base_ptr = boxed.as_mut_ptr();
        let mask = init_groups - 1;
        Self {
            groups_base: AtomicPtr::new(base_ptr),
            group_mask: AtomicUsize::new(mask),
            inner: RwLock::new(ShardInner { groups: boxed }),
        }
    }
}

/// Single-cacheline (64-byte) L1D Control Header orchestrating bit-shifted L3 SIMD bucket lookups.
#[repr(C, align(64))]
pub struct IntrospectionCache {
    boot_salt: [u8; 32],
    shards: Option<Box<[CacheShard]>>,
    cache_ttl_secs: u32,
    negative_ttl_secs: u32,
    stale_refresh_secs: u8,
    enabled: u8,
    shard_mask: u16,
    max_groups_log2: u8,
    shard_shift: u8,
    _pad: [u8; 2],
}

const _: () = assert!(std::mem::size_of::<IntrospectionCache>() == 64);
const _: () = assert!(std::mem::align_of::<IntrospectionCache>() == 64);
const _: () = assert!(std::mem::align_of::<BucketGroup>() == 64);
const _: () = assert!(std::mem::offset_of!(BucketGroup, tags) == 0);
const _: () = assert!(std::mem::align_of::<CacheShard>() == 64);
const _: () = assert!(std::mem::size_of::<CacheShard>().is_multiple_of(64));

impl IntrospectionCache {
    /// Constructs a new cache bounded strictly within 10% of system/cgroup RAM from `velda-core`.
    pub fn new(cache_ttl_secs: u64, negative_ttl_secs: u64) -> Self {
        let topo = velda_core::hardware::global_hardware_topology();
        Self::with_hardware_budget(
            cache_ttl_secs,
            negative_ttl_secs,
            topo.memory_bytes(),
            topo.available_cores,
        )
    }

    /// Constructs a cache for explicit hardware memory/core bounds (deterministic testing & sizing).
    pub fn with_hardware_budget(
        cache_ttl_secs: u64,
        negative_ttl_secs: u64,
        total_memory_bytes: usize,
        cores: usize,
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
                shards: None,
                cache_ttl_secs: 0,
                negative_ttl_secs: 0,
                stale_refresh_secs: 0,
                enabled: 0,
                shard_mask: 0,
                max_groups_log2: 0,
                shard_shift: 0,
                _pad: [0; 2],
            };
        }

        let (num_shards, init_groups, max_groups) =
            compute_cache_geometry(total_memory_bytes, cores);
        let shard_mask = (num_shards - 1) as u16;
        let shard_shift = num_shards.trailing_zeros() as u8;
        let max_groups_log2 = max_groups.trailing_zeros() as u8;

        let mut shards = Vec::with_capacity(num_shards);
        for _ in 0..num_shards {
            shards.push(CacheShard::new(init_groups));
        }

        Self {
            boot_salt,
            shards: Some(shards.into_boxed_slice()),
            cache_ttl_secs: pos_ttl,
            negative_ttl_secs: neg_ttl,
            stale_refresh_secs: stale_secs,
            enabled: 1,
            shard_mask,
            max_groups_log2,
            shard_shift,
            _pad: [0; 2],
        }
    }

    /// Returns `true` when positive or negative caching is enabled.
    #[inline(always)]
    pub fn is_enabled(&self) -> bool {
        self.enabled != 0
    }

    /// Returns `true` when positive token caching (`cache_ttl_secs > 0`) is enabled.
    #[inline(always)]
    pub fn is_positive_enabled(&self) -> bool {
        self.cache_ttl_secs > 0 && self.enabled != 0
    }

    /// Returns the configured stale-while-revalidate lead threshold in seconds.
    #[inline(always)]
    pub fn stale_refresh_secs(&self) -> u64 {
        self.stale_refresh_secs as u64
    }

    /// Returns the shard group memory ceiling per shard (`<= 10%` aggregate host RAM).
    #[inline]
    pub fn max_groups_per_shard(&self) -> usize {
        1usize << (self.max_groups_log2 as usize)
    }

    /// Returns total currently allocated bucket groups across all shards.
    pub fn total_allocated_groups(&self) -> usize {
        let Some(shards) = self.shards.as_deref() else {
            return 0;
        };
        shards
            .iter()
            .map(|s| s.group_mask.load(Ordering::Relaxed) + 1)
            .sum()
    }

    /// Prefetches the 64-byte L1D `IntrospectionCache` control line (`_MM_HINT_T0` / `pldl1keep`).
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

    /// Compresses an arbitrary-length opaque token (warm in L2) into a 128-bit keyed
    /// cryptographic integer (`Trunc128(SHA256(boot_salt || token))`) without heap allocation.
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
    fn split_fp(&self, fp: u128, group_mask: usize) -> (usize, usize, u8) {
        let shard_idx = (fp as usize) & (self.shard_mask as usize);
        let group_idx = ((fp >> self.shard_shift) as usize) & group_mask;
        let tag7 = (((fp >> 56) as u8) & TAG_HASH_MASK).max(1);
        (shard_idx, group_idx, tag7)
    }

    /// Single-pass lookup across positive, stale, insufficient-scope (403), and negative (401) entries.
    #[inline]
    pub fn lookup(&self, fp: u128, now_epoch: u64) -> CacheLookup {
        let Some(shards) = self.shards.as_deref() else {
            return CacheLookup::Miss;
        };

        let shard_idx = (fp as usize) & (self.shard_mask as usize);
        let shard = &shards[shard_idx];

        let hint_mask = shard.group_mask.load(Ordering::Acquire);
        let hint_group_idx = ((fp >> self.shard_shift) as usize) & hint_mask;
        let hint_base = shard.groups_base.load(Ordering::Acquire);
        prefetch_bucket_l3(unsafe { hint_base.add(hint_group_idx) as *const u8 });

        let guard = match shard.inner.read() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        let group_idx = ((fp >> self.shard_shift) as usize) & (guard.groups.len() - 1);
        let tag7 = (((fp >> 56) as u8) & TAG_HASH_MASK).max(1);
        let group = &guard.groups[group_idx];

        let mut mask = match_tag7_mask64(&group.tags, tag7);
        while mask != 0 {
            let idx = mask.trailing_zeros() as usize;
            if group.keys[idx] == fp {
                let exp = group.expiries[idx];
                if now_epoch >= exp {
                    return CacheLookup::Miss;
                }

                let flag = group.flags[idx];
                let state = flag & FLAG_STATE_MASK;
                let stale_window = self.stale_refresh_secs as u64;

                return match state {
                    FLAG_VALID_PASS | FLAG_FORBIDDEN_SCOPE
                        if stale_window > 0 && exp.saturating_sub(now_epoch) <= stale_window =>
                    {
                        let reval_active = (flag & FLAG_REVALIDATING) != 0
                            && now_epoch.saturating_sub(group.reval_at[idx]) <= stale_window;
                        if state == FLAG_FORBIDDEN_SCOPE {
                            CacheLookup::Forbidden
                        } else if reval_active {
                            CacheLookup::Hit(group.headers[idx].clone())
                        } else {
                            CacheLookup::Stale(group.headers[idx].clone())
                        }
                    }
                    FLAG_VALID_PASS => CacheLookup::Hit(group.headers[idx].clone()),
                    FLAG_FORBIDDEN_SCOPE => CacheLookup::Forbidden,
                    FLAG_UNAUTHORIZED => CacheLookup::Unauthorized,
                    _ => CacheLookup::Miss,
                };
            }
            mask &= mask - 1;
        }

        CacheLookup::Miss
    }

    /// Atomically claims the single-flight shadow revalidation lease for a stale token.
    pub fn mark_revalidating(&self, fp: u128, now_epoch: u64) -> bool {
        let Some(shards) = self.shards.as_deref() else {
            return false;
        };
        let shard_idx = (fp as usize) & (self.shard_mask as usize);
        let shard = &shards[shard_idx];
        let mut guard = match shard.inner.write() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        let (_, group_idx, tag7) = self.split_fp(fp, guard.groups.len() - 1);
        let group = &mut guard.groups[group_idx];
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

    /// Clears the in-flight revalidation bit if a background shadow refresh failed.
    pub fn finish_revalidation(&self, fp: u128) {
        let Some(shards) = self.shards.as_deref() else {
            return;
        };
        let shard_idx = (fp as usize) & (self.shard_mask as usize);
        let shard = &shards[shard_idx];
        let mut guard = match shard.inner.write() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        let (_, group_idx, tag7) = self.split_fp(fp, guard.groups.len() - 1);
        let group = &mut guard.groups[group_idx];

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

    /// Inserts or refreshes a verified positive (`ValidPass` or `ForbiddenScope`) entry.
    pub fn store_positive(
        &self,
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

        let tag_byte = if has_required_scopes {
            0u8
        } else {
            TAG_REJECT_BIT
        };
        let flag_byte = if has_required_scopes {
            FLAG_VALID_PASS
        } else {
            FLAG_FORBIDDEN_SCOPE
        };

        self.write_slot(fp, tag_byte, flag_byte, effective_exp, headers, now_epoch);
    }

    /// Inserts an invalid, revoked, or expired token decision into the unified L3 negative bit.
    pub fn store_negative(&self, fp: u128, now_epoch: u64) {
        if self.negative_ttl_secs == 0 {
            return;
        }
        let expire = now_epoch.saturating_add(self.negative_ttl_secs as u64);
        if expire <= now_epoch {
            return;
        }
        self.write_slot(
            fp,
            TAG_REJECT_BIT,
            FLAG_UNAUTHORIZED,
            expire,
            None,
            now_epoch,
        );
    }

    fn write_slot(
        &self,
        fp: u128,
        tag_polarity_bit: u8,
        flag_state: u8,
        expires_at: u64,
        headers: Option<SharedProjectedHeaders>,
        now_epoch: u64,
    ) {
        let Some(shards) = self.shards.as_deref() else {
            return;
        };
        let shard_idx = (fp as usize) & (self.shard_mask as usize);
        let shard = &shards[shard_idx];

        let mut guard = match shard.inner.write() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };

        let (_, mut group_idx, tag7) = self.split_fp(fp, guard.groups.len() - 1);
        let slot = match guard.groups[group_idx].try_find_free_or_existing(fp, tag7, now_epoch) {
            Some(idx) => idx,
            None => {
                // Grow shard dynamically while total memory remains under 10% RAM ceiling
                let max_groups = self.max_groups_per_shard();
                if guard.groups.len() < max_groups {
                    self.grow_shard_doubling(shard, &mut guard, now_epoch);
                    group_idx = ((fp >> self.shard_shift) as usize) & (guard.groups.len() - 1);
                    guard.groups[group_idx]
                        .try_find_free_or_existing(fp, tag7, now_epoch)
                        .unwrap_or_else(|| guard.groups[group_idx].evict_clock())
                } else {
                    guard.groups[group_idx].evict_clock()
                }
            }
        };

        let group = &mut guard.groups[group_idx];
        group.tags.0[slot] = tag_polarity_bit | tag7;
        group.flags[slot] = flag_state;
        group.keys[slot] = fp;
        group.expiries[slot] = expires_at;
        group.reval_at[slot] = 0;
        group.headers[slot] = headers;
    }

    #[cold]
    fn grow_shard_doubling(&self, shard: &CacheShard, guard: &mut ShardInner, now_epoch: u64) {
        let old_len = guard.groups.len();
        let new_len = old_len * 2;
        let new_mask = new_len - 1;

        let mut next_vec = Vec::with_capacity(new_len);
        for _ in 0..new_len {
            next_vec.push(BucketGroup::new());
        }
        let mut next_groups = next_vec.into_boxed_slice();

        for old_group in guard.groups.iter_mut() {
            for slot in 0..GROUP_WIDTH {
                let tag = old_group.tags.0[slot];
                if tag == 0 {
                    continue;
                }
                if old_group.expiries[slot] <= now_epoch {
                    old_group.headers[slot] = None;
                    continue;
                }
                let fp = old_group.keys[slot];
                let dest_group = ((fp >> self.shard_shift) as usize) & new_mask;
                let tag7 = (tag & TAG_HASH_MASK).max(1);
                let target = next_groups[dest_group]
                    .try_find_free_or_existing(fp, tag7, now_epoch)
                    .unwrap_or_else(|| next_groups[dest_group].evict_clock());

                next_groups[dest_group].tags.0[target] = tag;
                next_groups[dest_group].flags[target] = old_group.flags[slot];
                next_groups[dest_group].keys[target] = fp;
                next_groups[dest_group].expiries[target] = old_group.expiries[slot];
                next_groups[dest_group].reval_at[target] = old_group.reval_at[slot];
                next_groups[dest_group].headers[target] = old_group.headers[slot].take();
            }
        }

        let new_base = next_groups.as_mut_ptr();
        guard.groups = next_groups;
        shard.groups_base.store(new_base, Ordering::Release);
        shard.group_mask.store(new_mask, Ordering::Release);
    }
}

/// Prefetches Cache Line 0 (`tags`) of the target L3 `BucketGroup` (`_MM_HINT_T2` / `pldl3keep`).
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

// ============================================================================
// Multi-Target Compile-Time SIMD / SWAR 64-Byte Tag Scanners
// ============================================================================

#[inline(always)]
fn match_tag7_mask64(tags: &TagLine, target7: u8) -> u64 {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    unsafe {
        use core::arch::x86_64::{
            __m256i, _mm256_and_si256, _mm256_cmpeq_epi8, _mm256_load_si256, _mm256_movemask_epi8,
            _mm256_set1_epi8,
        };
        let ptr = tags.0.as_ptr();
        let mask7 = _mm256_set1_epi8(0x7F);
        let needle = _mm256_set1_epi8(target7 as i8);
        let lo = _mm256_and_si256(_mm256_load_si256(ptr as *const __m256i), mask7);
        let hi = _mm256_and_si256(_mm256_load_si256(ptr.add(32) as *const __m256i), mask7);
        let m_lo = _mm256_movemask_epi8(_mm256_cmpeq_epi8(lo, needle)) as u32 as u64;
        let m_hi = _mm256_movemask_epi8(_mm256_cmpeq_epi8(hi, needle)) as u32 as u64;
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
        let mask7 = _mm_set1_epi8(0x7F);
        let needle = _mm_set1_epi8(target7 as i8);
        let mut out = 0u64;
        let mut chunk = 0usize;
        while chunk < 4 {
            let v = _mm_and_si128(_mm_load_si128(ptr.add(chunk * 16) as *const __m128i), mask7);
            let m = _mm_movemask_epi8(_mm_cmpeq_epi8(v, needle)) as u32 as u64;
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
        let needle = vdupq_n_u8(target7);
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
    fn test_10_percent_ram_ceiling_geometry_and_bounded_eviction() {
        // 32 MiB tiny container -> 10% RAM = 3.2 MiB budget
        let tiny = IntrospectionCache::with_hardware_budget(300, 10, 32 * 1024 * 1024, 4);
        let budget = max_oauth2_cache_bytes_for(32 * 1024 * 1024);
        assert_eq!(budget, 3_355_440);
        assert!(
            tiny.total_allocated_groups() * ESTIMATED_GROUP_FOOTPRINT_BYTES <= budget,
            "Initial groups must fit under 10% RAM ceiling"
        );

        // Insert 5000 tokens: shard grows smoothly within 10% RAM and evicts O(1) without OOM
        let now = 1_000_000;
        for i in 0..5_000u64 {
            let fp = tiny.token_to_u128(&format!("tok_user_{i}"));
            tiny.store_positive(fp, true, None, Some(now + 600), now);
        }
        assert!(
            tiny.total_allocated_groups()
                <= (tiny.shard_mask as usize + 1) * tiny.max_groups_per_shard()
        );
    }

    #[test]
    fn test_unified_simd_lookup_stale_revalidation_and_negative_flip() {
        let cache = IntrospectionCache::with_hardware_budget(100, 10, 512 * 1024 * 1024, 4);
        assert_eq!(cache.stale_refresh_secs(), 10);
        let now = 1_000_000;
        let fp = cache.token_to_u128("opaque_tok_1");

        cache.store_positive(fp, true, None, Some(now + 200), now);
        assert!(matches!(cache.lookup(fp, now + 50), CacheLookup::Hit(_)));

        // Within final 10s stale window (remaining = 5s) -> Stale once, Hit after lease claimed
        assert!(matches!(cache.lookup(fp, now + 95), CacheLookup::Stale(_)));
        assert!(cache.mark_revalidating(fp, now + 95));
        assert!(!cache.mark_revalidating(fp, now + 96));
        assert!(matches!(cache.lookup(fp, now + 96), CacheLookup::Hit(_)));

        // User logs out -> Shadow flips slot to negative immediately
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
