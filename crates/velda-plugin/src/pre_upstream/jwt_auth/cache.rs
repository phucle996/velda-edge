//! Two-Tier L1D Control + L3 Contiguous SIMD Bucket Cache (`jwt_auth::cache`).
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
//!   | 0x20..0x30  shards: Option<Box<[Shard]>>  (None = 0B RAM when cache_ttl_secs == 0)    |
//!   | 0x30..0x38  default_ttl_secs (u32) | negative_ttl_secs (u32)                          |
//!   | 0x38..0x40  shard_mask (u16) | group_mask (u16) | group_shift (u8) | enabled (u8)     |
//!   +---------------------------------------------------------------------------------------+
//!        │
//!        │ 2. SHA-256(boot_salt || jwt_bytes) -> `fp: u128` (16B integer in %rax:%rdx)
//!        │ 3. Bit-shift index:
//!        │      shard_idx = fp & shard_mask
//!        │      group_idx = (fp >> group_shift) & group_mask
//!        │      tag7      = ((fp >> 56) & 0x7F).max(1)   [0x00 strictly reserved for Empty]
//!        │ 4. Issue `_MM_HINT_T2` (`pldl3keep`) on `&shard.groups[group_idx].tags[0]`
//!        │    (OoOE overlaps ~14 ns L3 transfer with `key_epoch` load & shard lock)
//!        ▼
//!   [L3 Shared LLC: Contiguous `BucketGroup` (`#[repr(C, align(64))]`, 64-Way Set)]
//!   +---------------------------------------------------------------------------------------+
//!   | Cache Line 0 (0x00..0x40, 64B): `tags: [u8; 64]`  <-- CONTIGUOUS SIMD INDEX MAP       |
//!   |   • 0x00       : Empty slot                                                           |
//!   |   • 0x01..0x7F : Positive Valid Entry (`tag7`)                                        |
//!   |   • 0x81..0xFF : Negative / Forbidden Entry (`0x80 | tag7`)                           |
//!   +---------------------------------------------------------------------------------------+
//!   | Contiguous Parallel Arrays (indexed ONLY at matched `idx = mask.trailing_zeros()`):   |
//!   |   • keys:      [u128; 64]          (`keys[idx] == fp`, 128-bit integer verify)        |
//!   |   • expiries:  [u64; 64]           (`expiries[idx] > now_epoch`)                      |
//!   |   • epochs:    [u64; 64]           (`epochs[idx] == key_epoch`, rotation-safe)        |
//!   |   • decisions: [CachedDecision; 64] (jumps heap `Arc<[(HeaderKey, HeaderValue)]>`     |
//!   |                                      ONLY on Positive Hit with non-empty claims)      |
//!   +---------------------------------------------------------------------------------------+
//! ```

use std::sync::{Arc, Mutex};

use http::header::HeaderValue;
use ring::digest::{Context as DigestContext, SHA256};
use ring::rand::{SecureRandom, SystemRandom};
use velda_core::HeaderKey;

/// Number of slots per contiguous SIMD bucket group (exact 64-byte L1/L3 cache line of `u8` tags).
pub const GROUP_WIDTH: usize = 64;

/// Default number of lock-striped shards across the gateway.
pub const DEFAULT_SHARD_COUNT: usize = 64;

/// Default number of 64-way SIMD groups per shard (`64 * 16 * 64 = 65,536` concurrent tokens).
pub const DEFAULT_GROUPS_PER_SHARD: usize = 16;

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

/// Outcome returned by a single-pass [`JwtCache::lookup`] operation.
#[derive(Clone, Debug)]
pub enum CacheLookup {
    Miss,
    Positive(Option<Arc<[(HeaderKey, HeaderValue)]>>),
    Unauthorized,
    Forbidden,
}

/// 64-byte cache-line aligned SIMD tag directory (`0x00..0x40`).
#[repr(C, align(64))]
#[derive(Clone, Copy)]
struct TagLine([u8; GROUP_WIDTH]);

/// Contiguous 64-way SIMD set-associative bucket residing in L3 Shared LLC.
///
/// Cache Line 0 (`0x00..0x40`) holds all 64 1-byte tags so a cache miss inspects
/// exclusively one 64-byte cache line without touching `keys`, `expiries`, or heap pointers.
#[repr(C, align(64))]
struct BucketGroup {
    tags: TagLine,
    keys: [u128; GROUP_WIDTH],
    expiries: [u64; GROUP_WIDTH],
    epochs: [u64; GROUP_WIDTH],
    decisions: [CachedDecision; GROUP_WIDTH],
    clock_hand: u8,
}

impl BucketGroup {
    fn new() -> Self {
        Self {
            tags: TagLine([0u8; GROUP_WIDTH]),
            keys: [0u128; GROUP_WIDTH],
            expiries: [0u64; GROUP_WIDTH],
            epochs: [0u64; GROUP_WIDTH],
            decisions: std::array::from_fn(|_| CachedDecision::Empty),
            clock_hand: 0,
        }
    }

    /// Selects a target slot in O(1) via:
    /// 1) Existing matching `fp` overwrite,
    /// 2) SIMD scan for `0x00` empty tag,
    /// 3) Expired / stale-epoch slot reclaim,
    /// 4) Bounded clock-hand round-robin replacement.
    #[inline]
    fn select_slot(&mut self, fp: u128, tag7: u8, now_epoch: u64, key_epoch: u64) -> usize {
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
            if self.epochs[idx] != key_epoch || self.expiries[idx] <= now_epoch {
                self.clock_hand = ((idx + 1) & (GROUP_WIDTH - 1)) as u8;
                return idx;
            }
        }

        let victim = start & (GROUP_WIDTH - 1);
        self.clock_hand = ((victim + 1) & (GROUP_WIDTH - 1)) as u8;
        victim
    }
}

/// Cache-line isolated shard container (`size % 64 == 0`, zero false sharing across adjacent shards).
#[repr(C, align(64))]
struct CacheShard {
    /// Stable base pointer to `groups[0]` (allocated once at init, never reallocated),
    /// allowing lock-free `_MM_HINT_T2` L3 prefetch hints prior to mutex acquisition.
    groups_base: *const BucketGroup,
    groups: Mutex<Box<[BucketGroup]>>,
}

// Safety: `groups_base` is used solely for non-dereferencing hardware cache prefetch instructions
// (`_mm_prefetch` / `prfm`), while all reads and writes go through `groups: Mutex<Box<[BucketGroup]>>`.
unsafe impl Send for CacheShard {}
unsafe impl Sync for CacheShard {}

impl CacheShard {
    fn new(groups_per_shard: usize) -> Self {
        let mut vec = Vec::with_capacity(groups_per_shard);
        for _ in 0..groups_per_shard {
            vec.push(BucketGroup::new());
        }
        let boxed = vec.into_boxed_slice();
        let groups_base = boxed.as_ptr();
        Self {
            groups_base,
            groups: Mutex::new(boxed),
        }
    }
}

/// Single-cacheline (64-byte) L1D Control Header orchestrating bit-shifted L3 SIMD bucket lookups.
#[repr(C, align(64))]
pub struct JwtCache {
    boot_salt: [u8; 32],
    shards: Option<Box<[CacheShard]>>,
    default_ttl_secs: u32,
    negative_ttl_secs: u32,
    shard_mask: u16,
    group_mask: u16,
    group_shift: u8,
    enabled: u8,
    _pad: [u8; 2],
}

const _: () = assert!(std::mem::size_of::<JwtCache>() == 64);
const _: () = assert!(std::mem::align_of::<JwtCache>() == 64);
const _: () = assert!(std::mem::align_of::<BucketGroup>() == 64);
const _: () = assert!(std::mem::offset_of!(BucketGroup, tags) == 0);
const _: () = assert!(std::mem::align_of::<CacheShard>() == 64);
const _: () = assert!(std::mem::size_of::<CacheShard>().is_multiple_of(64));

impl JwtCache {
    /// Constructs a new `JwtCache`.
    ///
    /// When `default_ttl_secs == 0` and `negative_ttl_secs == 0`, allocates zero heap memory (`shards = None`).
    pub fn new(
        shard_count: usize,
        groups_per_shard: usize,
        default_ttl_secs: u64,
        negative_ttl_secs: u64,
    ) -> Self {
        let mut boot_salt = [0u8; 32];
        if SystemRandom::new().fill(&mut boot_salt).is_err() {
            // Deterministic mix fallback only if OS CSPRNG fails
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
                shards: None,
                default_ttl_secs: 0,
                negative_ttl_secs: 0,
                shard_mask: 0,
                group_mask: 0,
                group_shift: 0,
                enabled: 0,
                _pad: [0; 2],
            };
        }

        let num_shards = shard_count.clamp(1, 1024).next_power_of_two();
        let num_groups = groups_per_shard.clamp(1, 1024).next_power_of_two();
        let shard_mask = (num_shards - 1) as u16;
        let group_mask = (num_groups - 1) as u16;
        let group_shift = num_shards.trailing_zeros() as u8;

        let mut shards = Vec::with_capacity(num_shards);
        for _ in 0..num_shards {
            shards.push(CacheShard::new(num_groups));
        }

        Self {
            boot_salt,
            shards: Some(shards.into_boxed_slice()),
            default_ttl_secs: pos_ttl,
            negative_ttl_secs: neg_ttl,
            shard_mask,
            group_mask,
            group_shift,
            enabled: 1,
            _pad: [0; 2],
        }
    }

    /// Returns `true` when positive or negative caching is enabled.
    #[inline(always)]
    pub fn is_enabled(&self) -> bool {
        self.enabled != 0
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
    fn split_fp(&self, fp: u128) -> (usize, usize, u8) {
        let shard_idx = (fp as usize) & (self.shard_mask as usize);
        let group_idx = ((fp >> self.group_shift) as usize) & (self.group_mask as usize);
        let tag7 = (((fp >> 56) as u8) & TAG_HASH_MASK).max(1);
        (shard_idx, group_idx, tag7)
    }

    /// Single-pass lookup across both positive and negative entries via L3 prefetch + 64B SIMD scan.
    #[inline]
    pub fn lookup(&self, fp: u128, now_epoch: u64, key_epoch: u64) -> CacheLookup {
        let Some(shards) = self.shards.as_deref() else {
            return CacheLookup::Miss;
        };

        let (shard_idx, group_idx, tag7) = self.split_fp(fp);
        let shard = &shards[shard_idx];

        // Issue Tier-3 (`_MM_HINT_T2` / `pldl3keep`) prefetch on Cache Line 0 (`tags: [u8; 64]`)
        // of the target bucket so L3 transfer overlaps with mutex lock acquisition via OoOE.
        prefetch_bucket_l3(unsafe { shard.groups_base.add(group_idx) as *const u8 });

        let guard = match shard.groups.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        let group = &guard[group_idx];

        let mut mask = match_tag7_mask64(&group.tags, tag7);
        while mask != 0 {
            let idx = mask.trailing_zeros() as usize;
            if group.keys[idx] == fp {
                if group.epochs[idx] != key_epoch || group.expiries[idx] <= now_epoch {
                    return CacheLookup::Miss;
                }
                return match &group.decisions[idx] {
                    CachedDecision::Valid(headers) => CacheLookup::Positive(headers.clone()),
                    CachedDecision::Unauthorized => CacheLookup::Unauthorized,
                    CachedDecision::Forbidden => CacheLookup::Forbidden,
                    CachedDecision::Empty => CacheLookup::Miss,
                };
            }
            mask &= mask - 1;
        }

        CacheLookup::Miss
    }

    /// Inserts a verified token decision into its bit-shifted 64-way L3 bucket.
    #[inline]
    pub fn insert_valid(
        &self,
        fp: u128,
        exp_epoch_secs: u64,
        headers: Option<Arc<[(HeaderKey, HeaderValue)]>>,
        now_epoch: u64,
        key_epoch: u64,
    ) {
        if self.default_ttl_secs == 0 {
            return;
        }
        let Some(shards) = self.shards.as_deref() else {
            return;
        };
        let effective_exp =
            exp_epoch_secs.min(now_epoch.saturating_add(self.default_ttl_secs as u64));
        if effective_exp <= now_epoch {
            return;
        }

        let (shard_idx, group_idx, tag7) = self.split_fp(fp);
        let shard = &shards[shard_idx];
        prefetch_bucket_l3(unsafe { shard.groups_base.add(group_idx) as *const u8 });

        let mut guard = match shard.groups.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        let group = &mut guard[group_idx];
        let slot = group.select_slot(fp, tag7, now_epoch, key_epoch);
        group.tags.0[slot] = tag7;
        group.keys[slot] = fp;
        group.expiries[slot] = effective_exp;
        group.epochs[slot] = key_epoch;
        group.decisions[slot] = CachedDecision::Valid(headers);
    }

    /// Inserts an invalid (`401`) or claim-rejected (`403`) token into its bit-shifted 64-way L3 bucket.
    #[inline]
    pub fn insert_reject(&self, fp: u128, is_forbidden: bool, now_epoch: u64, key_epoch: u64) {
        if self.negative_ttl_secs == 0 {
            return;
        }
        let Some(shards) = self.shards.as_deref() else {
            return;
        };
        let expire = now_epoch.saturating_add(self.negative_ttl_secs as u64);

        let (shard_idx, group_idx, tag7) = self.split_fp(fp);
        let shard = &shards[shard_idx];
        prefetch_bucket_l3(unsafe { shard.groups_base.add(group_idx) as *const u8 });

        let mut guard = match shard.groups.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        let group = &mut guard[group_idx];
        let slot = group.select_slot(fp, tag7, now_epoch, key_epoch);
        group.tags.0[slot] = TAG_REJECT_BIT | tag7;
        group.keys[slot] = fp;
        group.expiries[slot] = expire;
        group.epochs[slot] = key_epoch;
        group.decisions[slot] = if is_forbidden {
            CachedDecision::Forbidden
        } else {
            CachedDecision::Unauthorized
        };
    }
}

/// Prefetches Cache Line 0 (`tags: [u8; 64]`) of the target `BucketGroup` into L3 LLC (`_MM_HINT_T2` / `pldl3keep`).
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

/// Scans a 64-byte aligned `tags` cache line for `(tag & 0x7F) == target7`, returning a 64-bit lane mask.
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

/// Scans a 64-byte aligned `tags` cache line for `tag == 0x00` (empty slots), returning a 64-bit lane mask.
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
        use core::arch::aarch64::{
            vaddv_u8, vandq_u8, vextq_u8, vgetq_lane_u64, vreinterpretq_u8_u64,
        };
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
        let _ = vgetq_lane_u64;
        (vaddv_u8(lo_half) as u64) | ((vaddv_u8(hi_half) as u64) << 8)
    }
}

/// Zero-borrow 64-bit SWAR scanner for `(tag & 0x7F) == target7` across 64 bytes.
///
/// Setting MSB (`| 0x8080...`) on 7-bit XOR residuals guarantees every byte is `>= 0x80`,
/// preventing borrow propagation across byte boundaries when subtracting `0x0101...`.
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

/// Zero-borrow 64-bit SWAR scanner for `tag == 0x00` across 64 bytes.
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

/// Compresses the 8 MSB bits (`0x80` of each byte in `msbs`) into an 8-bit integer (`0x00..=0xFF`).
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
    use http::header::HeaderName;

    #[test]
    fn test_control_header_and_bucket_cacheline_geometry() {
        assert_eq!(std::mem::size_of::<JwtCache>(), 64);
        assert_eq!(std::mem::align_of::<JwtCache>(), 64);
        assert_eq!(std::mem::align_of::<BucketGroup>(), 64);
        assert_eq!(std::mem::offset_of!(BucketGroup, tags), 0);
        assert_eq!(std::mem::align_of::<CacheShard>(), 64);
        assert_eq!(std::mem::size_of::<CacheShard>() % 64, 0);

        let cache = JwtCache::new(64, 4, 300, 10);
        let shards = cache.shards.as_deref().unwrap();
        for shard in shards {
            let shard_addr = (shard as *const CacheShard) as usize;
            assert_eq!(shard_addr % 64, 0);
            let group_addr = shard.groups_base as usize;
            assert_eq!(group_addr % 64, 0);
        }
    }

    #[test]
    fn test_disabled_cache_allocates_zero_shards() {
        let cache = JwtCache::new(64, 16, 0, 0);
        assert!(!cache.is_enabled());
        assert!(cache.shards.is_none());
        let fp = cache.token_to_u128("a.b.c");
        assert!(matches!(cache.lookup(fp, 100, 1), CacheLookup::Miss));
    }

    #[test]
    fn test_positive_and_negative_unified_simd_lookup_and_epoch_invalidation() {
        let cache = JwtCache::new(8, 4, 300, 10);
        let now = 1_000_000;

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

        cache.insert_valid(fp_ok, now + 100, Some(headers), now, 1);
        cache.insert_reject(fp_bad, false, now, 1);
        cache.insert_reject(fp_forbid, true, now, 1);

        match cache.lookup(fp_ok, now + 50, 1) {
            CacheLookup::Positive(Some(hdrs)) => assert_eq!(hdrs[0].1, "usr_42"),
            other => panic!("expected Positive hit, got {other:?}"),
        }
        assert!(matches!(
            cache.lookup(fp_bad, now + 5, 1),
            CacheLookup::Unauthorized
        ));
        assert!(matches!(
            cache.lookup(fp_forbid, now + 5, 1),
            CacheLookup::Forbidden
        ));

        // Expiration enforced
        assert!(matches!(
            cache.lookup(fp_ok, now + 101, 1),
            CacheLookup::Miss
        ));
        assert!(matches!(
            cache.lookup(fp_bad, now + 11, 1),
            CacheLookup::Miss
        ));

        // JWKS key rotation epoch bump invalidates cached entries immediately
        assert!(matches!(
            cache.lookup(fp_ok, now + 10, 2),
            CacheLookup::Miss
        ));
    }

    #[test]
    fn test_64_way_bucket_saturation_and_clock_eviction() {
        let cache = JwtCache::new(1, 1, 300, 10);
        let now = 1_000_000;

        for i in 0..80u128 {
            let fp = (i << 64) | (i * 0x9E37_79B9_7F4A_7C15);
            cache.insert_valid(fp, now + 100, None, now, 1);
        }

        let last_fp = (79u128 << 64) | (79u128 * 0x9E37_79B9_7F4A_7C15);
        assert!(matches!(
            cache.lookup(last_fp, now + 10, 1),
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
