//! Pre-warmed User-Space Payload Slab Allocator.
//!
//! # Architecture and Invariants
//! 1. **Continuous Mathematical Sizing & Anti-OOM Cap**:
//!    - Baseline and maximum burst capacities are mathematically bounded via
//!      [`crate::buffer::adaptive::AdaptiveBufferDimensions`].
//!    - Prevents Out-Of-Memory (OOM) crashes by strictly enforcing `max_burst_cap`
//!      (Dual-Constraint formula: $\le 3\times$ baseline and $\le 20\%$ headroom RAM).
//! 2. **Tier 1 (Permanent Contiguous Baseline Floor)**:
//!    - Slabs across all four canonical classes (`Small4K`, `Medium16K`, `Standard32K`, `Large64K`)
//!      in Tier 1 are carved out of **ONE single contiguous memory arena** per pool
//!      ([`ContiguousArena`]).
//!    - These baseline slabs never get evicted or freed throughout the lifetime of the gateway process.
//!    - Memory slices are offset-based, page-aligned (4096 bytes), and cache-line aligned.
//! 3. **Eager Physical Page Commit (Anti-Page-Fault)**:
//!    - During cold-start initialization and proactive shadow replenishment, every 4096-byte memory
//!      page in the slabs is touched with volatile writes to force the Linux Kernel MMU to establish
//!      page table entries (PTEs) upfront, eliminating runtime minor page faults.
//! 4. **Tier 2 (Transient Burst Standby Queue with 15s TTL)**:
//!    - Dynamic heap allocations (`Box<[u8]>`) created dynamically during traffic spikes exceeding
//!      the baseline floor.
//!    - Excess slabs are stored with a return timestamp from [`crate::coarse_now_sec`] and reused
//!      across multi-wave micro-bursts, eliminating allocator heap churn (`malloc`/`free` thrashing).
//!    - When burst traffic subsides, `sweep_expired()` drops these excess slabs so jemalloc and the
//!      operating system can reclaim dirty pages across the process RSS decay window.
//! 5. **Strict Core-Jailing & Shadow Maintenance**:
//!    - Slabs are partitioned per CPU core into [`crate::buffer::CoreBufferPool`].
//!    - Traffic Workers exclusively acquire from and recycle into their own core's pool (zero cross-core contention).
//!    - Companion Shadow Logistics Worker (`velda-shadow-{id}`) maintains its core's pool inline
//!      under `SCHED_IDLE` using non-blocking `try_lock()`:
//!      - **Low-Watermark Proactive Replenishment**: When available pooled slabs drop $\le 25\%$ of baseline,
//!        the shadow worker proactively commits a batch of physical slabs into the burst queue (15s TTL).
//!      - **15s TTL Eviction**: Unused burst slabs idle $> 15s$ are dropped back to `jemalloc` RSS.
//! 6. **Four Canonical Slab Classes with Contiguous Slicing**:
//!    - `Small4K` (4 KB): small JSON payloads, auth tokens, small RPC.
//!    - `Medium16K` (16 KB): standard REST payloads, chunks.
//!    - `Standard32K` (32 KB): medium payloads (e.g. 18 KB - 32 KB), bridging the gap to 64 KB
//!      while preserving a single contiguous memory slice `&[u8]` required for zero-copy SIMD WAF / JSON parsing.
//!    - `Large64K` (64 KB): large uploads and full-frame buffering.
//! 7. **Deliberate 4-Tier Hardware Cache Hierarchy Invariant (`T0` / `T1` / `T2` / `NTA`)**:
//!    - **L1D (`_MM_HINT_T0` / `pldl1keep`)**: Strictly reserved for `velda-router` hot lookup tables
//!      (`SlashBlockNode`, `HotCandidate`) so route matching executes in ~1-3 ns with zero L1D thrashing.
//!    - **L2 (`_MM_HINT_T1` / `pldl2keep` + LIFO `pop_back`)**: Houses **Pool Management Metadata**
//!      (`SlabBucket` vectors of 24-byte `PayloadSlab` descriptors) and [`crate::HeaderWorkspace`].
//!      Because pool management runs on every request, pinning the lightweight 24-byte slab descriptors
//!      and LIFO burst queue in L2 yields ~3-4 ns acquisition without touching Core L1D.
//!    - **L3 LLC (`_MM_HINT_T2` / `pldl3keep`) & Physical DRAM (`_MM_HINT_NTA` / WC Streaming)**:
//!      The multi-megabyte backing payload buffers (4 KB - 64 KB) reside in L3/DRAM. Bulk payload writes
//!      (`>= 256` bytes) stream through CPU Write-Combining (WC) buffers (`COPY_NON_TEMPORAL_FN` /
//!      `ZERO_NON_TEMPORAL_FN`), bypassing Read-For-Ownership (RFO) and never evicting L1D or L2.

use std::alloc::Layout;
use std::collections::VecDeque;
use std::ops::{Deref, DerefMut};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

/// Physical page alignment boundary for edge buffer allocations.
const PAGE_SIZE: usize = 4096;

/// Canonical slab size classes for payload processing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SlabClass {
    /// 4 KB slab for small REST JSON payloads, auth tokens, and small RPC messages.
    Small4K = 4096,
    /// 16 KB slab for standard API payloads and buffered chunk processing.
    Medium16K = 16384,
    /// 32 KB slab for medium payloads (16 KB - 32 KB), bridging the gap to 64 KB
    /// while preserving a single contiguous memory slice `&[u8]` for zero-copy SIMD WAF.
    Standard32K = 32768,
    /// 64 KB slab for large uploads, media streaming, and full-frame buffering.
    Large64K = 65536,
}

impl SlabClass {
    /// Returns the exact size in bytes for this slab class.
    #[inline(always)]
    pub const fn byte_size(self) -> usize {
        self as usize
    }

    /// Selects the smallest pre-warmed slab class that fits `len` bytes, or `None` if `len > 64KB`.
    #[inline(always)]
    pub const fn for_size(len: usize) -> Option<Self> {
        if len <= Self::Small4K.byte_size() {
            Some(Self::Small4K)
        } else if len <= Self::Medium16K.byte_size() {
            Some(Self::Medium16K)
        } else if len <= Self::Standard32K.byte_size() {
            Some(Self::Standard32K)
        } else if len <= Self::Large64K.byte_size() {
            Some(Self::Large64K)
        } else {
            None
        }
    }
}

/// A single contiguous memory arena holding the Tier 1 permanent baseline floor for a pool.
///
/// Allocated once during [`PayloadSlabPool`] initialization with 4096-byte page alignment.
/// All physical pages are eagerly committed via volatile memory writes during initialization
/// to force the Linux kernel MMU to map physical frames upfront, eliminating runtime minor page faults.
pub struct ContiguousArena {
    ptr: NonNull<u8>,
    layout: Option<Layout>,
    total_bytes: usize,
}

unsafe impl Send for ContiguousArena {}
unsafe impl Sync for ContiguousArena {}

impl ContiguousArena {
    /// Allocates and eagerly warms up a contiguous memory arena of `total_bytes` length.
    ///
    /// Every 4096-byte page is eagerly touched via volatile writes to commit physical memory
    /// in the OS page tables upfront.
    pub fn new(total_bytes: usize) -> Self {
        if total_bytes == 0 {
            return Self {
                ptr: NonNull::dangling(),
                layout: None,
                total_bytes: 0,
            };
        }

        let layout = Layout::from_size_align(total_bytes, PAGE_SIZE)
            .expect("invalid memory layout for contiguous baseline arena");
        let raw_ptr = unsafe { std::alloc::alloc_zeroed(layout) };
        let ptr = match NonNull::new(raw_ptr) {
            Some(p) => p,
            None => std::alloc::handle_alloc_error(layout),
        };

        // Eagerly commit physical pages: touch every 4096-byte page during arena initialization
        // to force the Linux kernel MMU to map physical frames and eliminate runtime minor page faults.
        // Volatile writes guarantee that LLVM does not optimize out writes to zero-initialized pages.
        let mut page_offset = 0;
        while page_offset < total_bytes {
            unsafe {
                std::ptr::write_volatile(raw_ptr.add(page_offset), 0);
            }
            page_offset += PAGE_SIZE;
        }

        Self {
            ptr,
            layout: Some(layout),
            total_bytes,
        }
    }

    /// Returns the total size in bytes of the contiguous arena.
    #[inline(always)]
    pub const fn total_bytes(&self) -> usize {
        self.total_bytes
    }

    /// Returns a raw immutable pointer to the base of the arena memory.
    #[inline(always)]
    pub fn as_ptr(&self) -> *const u8 {
        self.ptr.as_ptr()
    }

    /// Returns a raw mutable pointer to the base of the arena memory.
    #[inline(always)]
    pub fn as_mut_ptr(&self) -> *mut u8 {
        self.ptr.as_ptr()
    }
}

impl Drop for ContiguousArena {
    fn drop(&mut self) {
        if let Some(layout) = self.layout {
            unsafe {
                std::alloc::dealloc(self.ptr.as_ptr(), layout);
            }
        }
    }
}

/// Internal backing memory for a [`PayloadSlab`].
enum SlabStorage {
    /// Tier 1 permanent baseline slab carved from the contiguous arena.
    Arena {
        ptr: NonNull<u8>,
        _arena: Arc<ContiguousArena>,
    },
    /// Tier 2 transient dynamic burst slab allocated on the heap with 15s TTL.
    Dynamic(Box<[u8]>),
}

/// A contiguous pre-allocated byte buffer slab.
pub struct PayloadSlab {
    storage: SlabStorage,
    class: SlabClass,
}

unsafe impl Send for PayloadSlab {}
unsafe impl Sync for PayloadSlab {}

impl std::fmt::Debug for PayloadSlab {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PayloadSlab")
            .field("class", &self.class)
            .field("is_arena", &self.is_arena())
            .field("len", &self.len())
            .finish()
    }
}

impl PayloadSlab {
    /// Allocates and eagerly warms up a dynamic payload slab, touching every 4KB page.
    pub fn new(class: SlabClass) -> Self {
        let size = class.byte_size();
        let mut buf = vec![0u8; size].into_boxed_slice();

        // Eagerly commit physical pages: write 1 byte per 4096-byte Linux memory page
        // to force kernel page allocation and eliminate Minor Page Faults during traffic spikes.
        let page_size = PAGE_SIZE;
        let mut offset = 0;
        let raw_ptr = buf.as_mut_ptr();
        while offset < size {
            unsafe {
                std::ptr::write_volatile(raw_ptr.add(offset), 0);
            }
            offset += page_size;
        }

        Self {
            storage: SlabStorage::Dynamic(buf),
            class,
        }
    }

    /// Creates a slab backed by a contiguous slice of the Tier 1 baseline arena.
    ///
    /// # Safety
    /// The caller must ensure that `ptr` points to a valid, exclusive memory region of at least
    /// `class.byte_size()` bytes within `arena`, and that no other slab shares this address range.
    pub(crate) unsafe fn from_arena(
        arena: Arc<ContiguousArena>,
        ptr: NonNull<u8>,
        class: SlabClass,
    ) -> Self {
        Self {
            storage: SlabStorage::Arena { ptr, _arena: arena },
            class,
        }
    }

    /// Borrows the slab as an immutable byte slice.
    #[inline(always)]
    pub fn as_slice(&self) -> &[u8] {
        match &self.storage {
            SlabStorage::Arena { ptr, .. } => unsafe {
                std::slice::from_raw_parts(ptr.as_ptr(), self.class.byte_size())
            },
            SlabStorage::Dynamic(buf) => buf.as_ref(),
        }
    }

    /// Borrows the slab as a mutable byte slice.
    #[inline(always)]
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        match &mut self.storage {
            SlabStorage::Arena { ptr, .. } => unsafe {
                std::slice::from_raw_parts_mut(ptr.as_ptr(), self.class.byte_size())
            },
            SlabStorage::Dynamic(buf) => buf.as_mut(),
        }
    }

    /// Returns the slab class.
    #[inline(always)]
    pub const fn class(&self) -> SlabClass {
        self.class
    }

    /// Returns the size of this slab in bytes.
    #[inline(always)]
    pub const fn len(&self) -> usize {
        self.class.byte_size()
    }

    /// Returns `true` if empty (always false for pre-warmed slabs).
    #[inline(always)]
    pub const fn is_empty(&self) -> bool {
        false
    }

    /// Returns `true` if this slab is carved from the Tier 1 permanent contiguous arena.
    #[inline(always)]
    pub fn is_arena(&self) -> bool {
        matches!(self.storage, SlabStorage::Arena { .. })
    }

    /// Returns `true` if this slab is a Tier 2 transient dynamic heap allocation.
    #[inline(always)]
    pub fn is_dynamic(&self) -> bool {
        matches!(self.storage, SlabStorage::Dynamic(_))
    }

    /// Returns a raw immutable pointer to the underlying buffer memory.
    #[inline(always)]
    pub fn as_ptr(&self) -> *const u8 {
        match &self.storage {
            SlabStorage::Arena { ptr, .. } => ptr.as_ptr(),
            SlabStorage::Dynamic(buf) => buf.as_ptr(),
        }
    }

    /// Returns a raw mutable pointer to the underlying buffer memory.
    #[inline(always)]
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        match &mut self.storage {
            SlabStorage::Arena { ptr, .. } => ptr.as_ptr(),
            SlabStorage::Dynamic(buf) => buf.as_mut_ptr(),
        }
    }

    /// Copies data from `src` into the beginning of this slab using Register-Adaptive Non-Temporal streaming stores.
    ///
    /// # Cache Invariant
    /// Non-temporal streaming stores (`vmovntdq` / `movntdq` / `movnti`) bypass the CPU L1D cache,
    /// writing directly to the processor's Write-Combining (WC) line buffers and flushing directly to RAM.
    /// This prevents large payload transfers (4 KB - 64 KB) from triggering Write-Allocate and evicting
    /// the hot `velda-router` lookup tables or L1D cache lines.
    #[inline(always)]
    pub fn copy_from_non_temporal(&mut self, src: &[u8]) {
        self.write_non_temporal(0, src);
    }

    /// Writes data from `src` into this slab starting at `offset` using Register-Adaptive Non-Temporal streaming stores.
    ///
    /// Copies up to `self.len().saturating_sub(offset)` bytes. Safely handles unaligned leading bytes
    /// and trailing bytes across all 4 register width tiers down to 32-bit hardware.
    #[inline]
    pub fn write_non_temporal(&mut self, offset: usize, src: &[u8]) {
        let total_len = self.len();
        if offset >= total_len {
            return;
        }
        let available = total_len - offset;
        let to_copy = src.len().min(available);
        if to_copy == 0 {
            return;
        }
        let dst_slice = &mut self.as_mut_slice()[offset..offset + to_copy];
        copy_non_temporal(&src[..to_copy], dst_slice);
    }

    /// Issues hardware Non-Temporal Read hints (`_MM_HINT_NTA` / `pldl1strm`) across the slab's memory.
    ///
    /// Signals the CPU memory subsystem that the payload data is streaming/transient and should not
    /// pollute or displace the hot `velda-router` routing tables from the L1D cache.
    #[inline(always)]
    pub fn prefetch_nta(&self, offset: usize, len: usize) {
        let total_len = self.len();
        if offset >= total_len {
            return;
        }
        let available = total_len - offset;
        let to_prefetch = len.min(available);
        if to_prefetch == 0 {
            return;
        }
        let base_ptr = unsafe { self.as_ptr().add(offset) };
        prefetch_nta_range(base_ptr, to_prefetch);
    }

    /// Issues hardware Tier-3 (`T2` / L3 LLC Cache) Prefetch hints (`_MM_HINT_T2` / `pldl3keep`)
    /// across the slab's memory, aligning with PCIe DMA / Intel DDIO residency without touching L1D/L2.
    #[inline(always)]
    pub fn prefetch_l3(&self, offset: usize, len: usize) {
        let total_len = self.len();
        if offset >= total_len {
            return;
        }
        let available = total_len - offset;
        let to_prefetch = len.min(available);
        if to_prefetch == 0 {
            return;
        }
        let base_ptr = unsafe { self.as_ptr().add(offset) };
        prefetch_l3_range(base_ptr, to_prefetch);
    }

    /// Zeroes `len` bytes starting at `offset` using Register-Adaptive Non-Temporal streaming stores,
    /// bypassing L1D/L2 cache allocation so CWE-226 sanitization never evicts hot routing tables.
    #[inline]
    pub fn zero_non_temporal(&mut self, offset: usize, len: usize) {
        let total_len = self.len();
        if offset >= total_len {
            return;
        }
        let available = total_len - offset;
        let to_zero = len.min(available);
        if to_zero == 0 {
            return;
        }
        let dst_slice = &mut self.as_mut_slice()[offset..offset + to_zero];
        zero_non_temporal(dst_slice);
    }
}

/// Prefetches up to 4 leading cache lines (`256` bytes) of `[ptr .. ptr + len)` into L2 Cache (`_MM_HINT_T1` / `pldl2keep`),
/// preserving Core L1D Cache exclusively for the hot `velda-router` lookup tables without overflowing CPU Load Fill Buffers (LFB).
#[inline(always)]
pub fn prefetch_l2_range(ptr: *const u8, len: usize) {
    if ptr.is_null() || len == 0 {
        return;
    }
    let limit = len.min(256);
    #[cfg(target_arch = "x86_64")]
    {
        use std::arch::x86_64::{_MM_HINT_T1, _mm_prefetch};
        let mut i = 0;
        while i < limit {
            unsafe {
                _mm_prefetch(ptr.wrapping_add(i) as *const i8, _MM_HINT_T1);
            }
            i += 64;
        }
    }
    #[cfg(target_arch = "aarch64")]
    {
        let mut i = 0;
        while i < limit {
            unsafe {
                std::arch::asm!(
                    "prfm pldl2keep, [{0}]",
                    in(reg) ptr.wrapping_add(i),
                    options(nostack, preserves_flags, readonly)
                );
            }
            i += 64;
        }
    }
}

/// Prefetches up to 4 leading cache lines (`256` bytes) of `[ptr .. ptr + len)` into L3 Last-Level Cache (`_MM_HINT_T2` / `pldl3keep`),
/// matching PCIe NIC/rNIC DMA (Intel DDIO / AMD SDCI) residency while leaving L1D and L2 untouched.
#[inline(always)]
pub fn prefetch_l3_range(ptr: *const u8, len: usize) {
    if ptr.is_null() || len == 0 {
        return;
    }
    let limit = len.min(256);
    #[cfg(target_arch = "x86_64")]
    {
        use std::arch::x86_64::{_MM_HINT_T2, _mm_prefetch};
        let mut i = 0;
        while i < limit {
            unsafe {
                _mm_prefetch(ptr.wrapping_add(i) as *const i8, _MM_HINT_T2);
            }
            i += 64;
        }
    }
    #[cfg(target_arch = "aarch64")]
    {
        let mut i = 0;
        while i < limit {
            unsafe {
                std::arch::asm!(
                    "prfm pldl3keep, [{0}]",
                    in(reg) ptr.wrapping_add(i),
                    options(nostack, preserves_flags, readonly)
                );
            }
            i += 64;
        }
    }
}

/// Prefetches up to 4 leading cache lines (`256` bytes) of `[ptr .. ptr + len)` with Non-Temporal Streaming hint (`_MM_HINT_NTA` / `pldl1strm`),
/// signaling transient single-pass access that must not displace temporal cache lines.
#[inline(always)]
pub fn prefetch_nta_range(ptr: *const u8, len: usize) {
    if ptr.is_null() || len == 0 {
        return;
    }
    let limit = len.min(256);
    #[cfg(target_arch = "x86_64")]
    {
        use std::arch::x86_64::{_MM_HINT_NTA, _mm_prefetch};
        let mut i = 0;
        while i < limit {
            unsafe {
                _mm_prefetch(ptr.wrapping_add(i) as *const i8, _MM_HINT_NTA);
            }
            i += 64;
        }
    }
    #[cfg(target_arch = "aarch64")]
    {
        let mut i = 0;
        while i < limit {
            unsafe {
                std::arch::asm!(
                    "prfm pldl1strm, [{0}]",
                    in(reg) ptr.wrapping_add(i),
                    options(nostack, preserves_flags, readonly)
                );
            }
            i += 64;
        }
    }
}

/// Minimum byte length (`256` bytes = 4 cache lines) before engaging Non-Temporal Write-Combining
/// streaming stores + `_mm_sfence()`. Below 256 bytes, direct register copy avoids `sfence` pipeline stalls.
const NON_TEMPORAL_THRESHOLD: usize = 256;

/// Signature for non-temporal streaming copy functions.
pub type CopyNonTemporalFn = fn(src: &[u8], dst: &mut [u8]);

/// Signature for non-temporal streaming zeroing functions (CWE-226 safe).
pub type ZeroNonTemporalFn = fn(dst: &mut [u8]);

/// Copies payload using streaming non-temporal stores with tiered hardware vector acceleration (AVX2 -> SSE2 -> 64-bit).
///
/// Small payloads (`< 256` bytes) copy directly to avoid `sfence` pipeline stalls;
/// larger payloads stream through Write-Combining (WC) buffers bypassing L1D/L2 caches.
#[inline]
pub fn copy_non_temporal(src: &[u8], dst: &mut [u8]) {
    let len = src.len().min(dst.len());
    if len < NON_TEMPORAL_THRESHOLD {
        dst[..len].copy_from_slice(&src[..len]);
        return;
    }
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") {
            #[target_feature(enable = "avx2")]
            #[allow(clippy::cast_ptr_alignment)]
            unsafe fn stream_avx2(src: &[u8], dst: &mut [u8], len: usize) {
                use std::arch::x86_64::*;
                let dst_addr = dst.as_mut_ptr() as usize;
                let lead = (32usize.wrapping_sub(dst_addr & 31)) & 31;
                if lead > 0 {
                    dst[..lead].copy_from_slice(&src[..lead]);
                }
                let stream_bytes = ((len - lead) / 32) * 32;
                let end_stream = lead + stream_bytes;
                let mut offset = lead;
                while offset < end_stream {
                    unsafe {
                        let v = _mm256_loadu_si256(src.as_ptr().add(offset) as *const __m256i);
                        _mm256_stream_si256(dst.as_mut_ptr().add(offset) as *mut __m256i, v);
                    }
                    offset += 32;
                }
                if end_stream < len {
                    dst[end_stream..len].copy_from_slice(&src[end_stream..len]);
                }
                _mm_sfence();
            }
            unsafe {
                stream_avx2(src, dst, len);
            }
            return;
        }

        #[target_feature(enable = "sse2")]
        #[allow(clippy::cast_ptr_alignment)]
        unsafe fn stream_sse2(src: &[u8], dst: &mut [u8], len: usize) {
            use std::arch::x86_64::*;
            let dst_addr = dst.as_mut_ptr() as usize;
            let lead = (16usize.wrapping_sub(dst_addr & 15)) & 15;
            if lead > 0 {
                dst[..lead].copy_from_slice(&src[..lead]);
            }
            let stream_bytes = ((len - lead) / 16) * 16;
            let end_stream = lead + stream_bytes;
            let mut offset = lead;
            while offset < end_stream {
                unsafe {
                    let v = _mm_loadu_si128(src.as_ptr().add(offset) as *const __m128i);
                    _mm_stream_si128(dst.as_mut_ptr().add(offset) as *mut __m128i, v);
                }
                offset += 16;
            }
            if end_stream < len {
                dst[end_stream..len].copy_from_slice(&src[end_stream..len]);
            }
            _mm_sfence();
        }
        unsafe {
            stream_sse2(src, dst, len);
        }
    }

    #[cfg(not(target_arch = "x86_64"))]
    {
        let mut i = 0;
        while i + 16 <= len {
            unsafe {
                let w0 = std::ptr::read_unaligned(src.as_ptr().add(i) as *const u64);
                let w1 = std::ptr::read_unaligned(src.as_ptr().add(i + 8) as *const u64);
                std::ptr::write_unaligned(dst.as_mut_ptr().add(i) as *mut u64, w0);
                std::ptr::write_unaligned(dst.as_mut_ptr().add(i + 8) as *mut u64, w1);
            }
            i += 16;
        }
        if i < len {
            dst[i..len].copy_from_slice(&src[i..len]);
        }
    }
}

/// Pre-resolved static function pointer for non-temporal streaming payload transfers.
pub static COPY_NON_TEMPORAL_FN: std::sync::LazyLock<CopyNonTemporalFn> =
    std::sync::LazyLock::new(|| copy_non_temporal);

/// Zeroes memory using streaming non-temporal stores with tiered hardware vector acceleration (AVX2 -> SSE2 -> 64-bit).
///
/// Clears sensitive memory (CWE-226) directly through Write-Combining (WC) buffers to L3/RAM
/// without triggering Read-For-Ownership (RFO) or polluting L1D/L2 caches.
#[inline]
pub fn zero_non_temporal(dst: &mut [u8]) {
    let len = dst.len();
    if len < NON_TEMPORAL_THRESHOLD {
        dst.fill(0);
        return;
    }
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") {
            #[target_feature(enable = "avx2")]
            #[allow(clippy::cast_ptr_alignment)]
            unsafe fn zero_avx2(dst: &mut [u8], len: usize) {
                use std::arch::x86_64::*;
                let dst_addr = dst.as_mut_ptr() as usize;
                let lead = (32usize.wrapping_sub(dst_addr & 31)) & 31;
                if lead > 0 {
                    dst[..lead].fill(0);
                }
                let stream_bytes = ((len - lead) / 32) * 32;
                let end_stream = lead + stream_bytes;
                unsafe {
                    let zero = _mm256_setzero_si256();
                    let mut offset = lead;
                    while offset < end_stream {
                        _mm256_stream_si256(dst.as_mut_ptr().add(offset) as *mut __m256i, zero);
                        offset += 32;
                    }
                }
                if end_stream < len {
                    dst[end_stream..len].fill(0);
                }
                _mm_sfence();
            }
            unsafe {
                zero_avx2(dst, len);
            }
            return;
        }

        #[target_feature(enable = "sse2")]
        #[allow(clippy::cast_ptr_alignment)]
        unsafe fn zero_sse2(dst: &mut [u8], len: usize) {
            use std::arch::x86_64::*;
            let dst_addr = dst.as_mut_ptr() as usize;
            let lead = (16usize.wrapping_sub(dst_addr & 15)) & 15;
            if lead > 0 {
                dst[..lead].fill(0);
            }
            let stream_bytes = ((len - lead) / 16) * 16;
            let end_stream = lead + stream_bytes;
            unsafe {
                let zero = _mm_setzero_si128();
                let mut offset = lead;
                while offset < end_stream {
                    _mm_stream_si128(dst.as_mut_ptr().add(offset) as *mut __m128i, zero);
                    offset += 16;
                }
            }
            if end_stream < len {
                dst[end_stream..len].fill(0);
            }
            _mm_sfence();
        }
        unsafe {
            zero_sse2(dst, len);
        }
    }

    #[cfg(not(target_arch = "x86_64"))]
    {
        let mut i = 0;
        while i + 16 <= len {
            unsafe {
                std::ptr::write_unaligned(dst.as_mut_ptr().add(i) as *mut u64, 0);
                std::ptr::write_unaligned(dst.as_mut_ptr().add(i + 8) as *mut u64, 0);
            }
            i += 16;
        }
        if i < len {
            dst[i..len].fill(0);
        }
    }
}

/// Pre-resolved static function pointer for non-temporal streaming memory zeroing (CWE-226 safe).
pub static ZERO_NON_TEMPORAL_FN: std::sync::LazyLock<ZeroNonTemporalFn> =
    std::sync::LazyLock::new(|| zero_non_temporal);

/// Internal bucket segregating permanent baseline slabs from transient burst slabs.
struct SlabBucket {
    /// Baseline pre-warmed physical slabs carved from the contiguous arena. Permanent, never evicted.
    baseline: Vec<PayloadSlab>,
    /// Burst standby slabs paired with their return timestamp in coarse seconds.
    /// Ordered chronologically: oldest at front, newest at back.
    burst: VecDeque<(u64, PayloadSlab)>,
    /// Maximum number of permanent baseline slabs.
    baseline_limit: usize,
    /// Absolute ceiling of active + pooled slabs (anti-OOM protection).
    max_burst_limit: usize,
}

/// Pre-warmed two-tier adaptive slab pool managing Small (4KB), Medium (16KB), Standard (32KB), and Large (64KB) slabs.
pub struct PayloadSlabPool {
    arena: Arc<ContiguousArena>,
    small_bucket: Mutex<SlabBucket>,
    medium_bucket: Mutex<SlabBucket>,
    standard_bucket: Mutex<SlabBucket>,
    large_bucket: Mutex<SlabBucket>,
    active_small: AtomicUsize,
    active_medium: AtomicUsize,
    active_standard: AtomicUsize,
    active_large: AtomicUsize,
    idle_ttl_secs: u64,
}

impl std::fmt::Debug for PayloadSlabPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PayloadSlabPool")
            .field("arena_total_bytes", &self.arena.total_bytes())
            .field("active_small", &self.active_small.load(Ordering::Relaxed))
            .field("active_medium", &self.active_medium.load(Ordering::Relaxed))
            .field(
                "active_standard",
                &self.active_standard.load(Ordering::Relaxed),
            )
            .field("active_large", &self.active_large.load(Ordering::Relaxed))
            .finish()
    }
}

/// Configuration limits for an adaptive payload slab pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdaptiveSlabLimits {
    pub small_baseline: usize,
    pub small_max_cap: usize,
    pub medium_baseline: usize,
    pub medium_max_cap: usize,
    pub standard_baseline: usize,
    pub standard_max_cap: usize,
    pub large_baseline: usize,
    pub large_max_cap: usize,
    pub idle_ttl_secs: u64,
}

impl PayloadSlabPool {
    /// Creates and eagerly warms up all slab classes with default 3x burst ceilings and 15s TTL.
    pub fn new(
        small_baseline: usize,
        medium_baseline: usize,
        standard_baseline: usize,
        large_baseline: usize,
    ) -> Arc<Self> {
        Self::with_adaptive_limits(AdaptiveSlabLimits {
            small_baseline,
            small_max_cap: small_baseline.saturating_mul(3).max(small_baseline + 16),
            medium_baseline,
            medium_max_cap: medium_baseline.saturating_mul(3).max(medium_baseline + 8),
            standard_baseline,
            standard_max_cap: standard_baseline
                .saturating_mul(3)
                .max(standard_baseline + 8),
            large_baseline,
            large_max_cap: large_baseline.saturating_mul(3).max(large_baseline + 4),
            idle_ttl_secs: 15,
        })
    }

    /// Creates an adaptive slab pool with explicit baselines, max burst ceilings, and idle TTL in seconds.
    pub fn with_adaptive_limits(limits: AdaptiveSlabLimits) -> Arc<Self> {
        let small_size = SlabClass::Small4K.byte_size();
        let medium_size = SlabClass::Medium16K.byte_size();
        let standard_size = SlabClass::Standard32K.byte_size();
        let large_size = SlabClass::Large64K.byte_size();

        let small_bytes = limits.small_baseline.saturating_mul(small_size);
        let medium_bytes = limits.medium_baseline.saturating_mul(medium_size);
        let standard_bytes = limits.standard_baseline.saturating_mul(standard_size);
        let large_bytes = limits.large_baseline.saturating_mul(large_size);

        let total_arena_bytes = small_bytes
            .saturating_add(medium_bytes)
            .saturating_add(standard_bytes)
            .saturating_add(large_bytes);

        // Allocate ONE single contiguous memory arena for the entire Tier 1 baseline per pool.
        // Touch every 4096-byte page upfront to commit physical frames and eliminate minor page faults.
        let arena = Arc::new(ContiguousArena::new(total_arena_bytes));
        let base_ptr = arena.as_mut_ptr();

        let mut current_offset = 0;

        // Carve Small4K baseline slabs
        let mut small_vec = Vec::with_capacity(limits.small_baseline);
        for _ in 0..limits.small_baseline {
            let slab_ptr = unsafe { NonNull::new_unchecked(base_ptr.add(current_offset)) };
            small_vec.push(unsafe {
                PayloadSlab::from_arena(Arc::clone(&arena), slab_ptr, SlabClass::Small4K)
            });
            current_offset += small_size;
        }

        // Carve Medium16K baseline slabs
        let mut medium_vec = Vec::with_capacity(limits.medium_baseline);
        for _ in 0..limits.medium_baseline {
            let slab_ptr = unsafe { NonNull::new_unchecked(base_ptr.add(current_offset)) };
            medium_vec.push(unsafe {
                PayloadSlab::from_arena(Arc::clone(&arena), slab_ptr, SlabClass::Medium16K)
            });
            current_offset += medium_size;
        }

        // Carve Standard32K baseline slabs
        let mut standard_vec = Vec::with_capacity(limits.standard_baseline);
        for _ in 0..limits.standard_baseline {
            let slab_ptr = unsafe { NonNull::new_unchecked(base_ptr.add(current_offset)) };
            standard_vec.push(unsafe {
                PayloadSlab::from_arena(Arc::clone(&arena), slab_ptr, SlabClass::Standard32K)
            });
            current_offset += standard_size;
        }

        // Carve Large64K baseline slabs
        let mut large_vec = Vec::with_capacity(limits.large_baseline);
        for _ in 0..limits.large_baseline {
            let slab_ptr = unsafe { NonNull::new_unchecked(base_ptr.add(current_offset)) };
            large_vec.push(unsafe {
                PayloadSlab::from_arena(Arc::clone(&arena), slab_ptr, SlabClass::Large64K)
            });
            current_offset += large_size;
        }

        debug_assert_eq!(current_offset, total_arena_bytes);

        Arc::new(Self {
            arena,
            small_bucket: Mutex::new(SlabBucket {
                baseline: small_vec,
                burst: VecDeque::new(),
                baseline_limit: limits.small_baseline,
                max_burst_limit: limits.small_max_cap.max(limits.small_baseline),
            }),
            medium_bucket: Mutex::new(SlabBucket {
                baseline: medium_vec,
                burst: VecDeque::new(),
                baseline_limit: limits.medium_baseline,
                max_burst_limit: limits.medium_max_cap.max(limits.medium_baseline),
            }),
            standard_bucket: Mutex::new(SlabBucket {
                baseline: standard_vec,
                burst: VecDeque::new(),
                baseline_limit: limits.standard_baseline,
                max_burst_limit: limits.standard_max_cap.max(limits.standard_baseline),
            }),
            large_bucket: Mutex::new(SlabBucket {
                baseline: large_vec,
                burst: VecDeque::new(),
                baseline_limit: limits.large_baseline,
                max_burst_limit: limits.large_max_cap.max(limits.large_baseline),
            }),
            active_small: AtomicUsize::new(0),
            active_medium: AtomicUsize::new(0),
            active_standard: AtomicUsize::new(0),
            active_large: AtomicUsize::new(0),
            idle_ttl_secs: limits.idle_ttl_secs,
        })
    }

    /// Returns the total bytes allocated for the Tier 1 contiguous baseline arena.
    #[inline(always)]
    pub fn arena_total_bytes(&self) -> usize {
        self.arena.total_bytes()
    }

    /// Returns the raw pointer to the base of the Tier 1 contiguous baseline arena.
    #[inline(always)]
    pub fn arena_ptr(&self) -> *const u8 {
        self.arena.as_ptr()
    }

    /// Returns references to the bucket mutex and active counter for a class.
    #[inline(always)]
    fn bucket_and_counter(&self, class: SlabClass) -> (&Mutex<SlabBucket>, &AtomicUsize) {
        match class {
            SlabClass::Small4K => (&self.small_bucket, &self.active_small),
            SlabClass::Medium16K => (&self.medium_bucket, &self.active_medium),
            SlabClass::Standard32K => (&self.standard_bucket, &self.active_standard),
            SlabClass::Large64K => (&self.large_bucket, &self.active_large),
        }
    }

    /// Borrows a slab of the requested class wrapped in an RAII lease.
    ///
    /// Resolution Order:
    /// 1. **Tier 1 (Hot Baseline)**: Pops from pre-warmed contiguous arena slabs (0 alloc, 0 page fault).
    /// 2. **Tier 2 (Burst Standby Queue)**: Pops from warm user-space burst queue (0 alloc).
    /// 3. **Spillover Dynamic Allocation**: Dynamically allocates if below `max_burst_cap`.
    /// 4. **Anti-OOM Protection**: Returns `None` if `max_burst_cap` has been reached.
    pub fn acquire(self: &Arc<Self>, class: SlabClass) -> Option<PayloadSlabLease> {
        let (bucket_mutex, active_counter) = self.bucket_and_counter(class);
        let mut guard = bucket_mutex
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        // 1. Try baseline (fastest path, hot pre-warmed contiguous arena)
        let slab = if let Some(slab) = guard.baseline.pop() {
            if let Some(next_desc) = guard.baseline.last() {
                prefetch_l2_range(
                    next_desc as *const PayloadSlab as *const u8,
                    std::mem::size_of::<PayloadSlab>(),
                );
            }
            slab
        } else if let Some((_, slab)) = guard.burst.pop_back() {
            // 2. Try burst standby (warm in user space within 15s TTL) - LIFO order.
            // Popping from the back ensures frontline traffic workers reuse the hottest
            // 1-2 recycled slabs, keeping pool descriptors in L2 (_MM_HINT_T1).
            if let Some((_, next_desc)) = guard.burst.back() {
                prefetch_l2_range(
                    next_desc as *const PayloadSlab as *const u8,
                    std::mem::size_of::<PayloadSlab>(),
                );
            }
            slab
        } else {
            // 3. Both pools empty: check anti-OOM ceiling before allocating on heap
            let current_active = active_counter.load(Ordering::Relaxed);
            if current_active >= guard.max_burst_limit {
                return None;
            }
            PayloadSlab::new(class)
        };

        active_counter.fetch_add(1, Ordering::Relaxed);

        Some(PayloadSlabLease {
            slab: Some(slab),
            pool: Arc::clone(self),
        })
    }

    /// Borrows the smallest pre-warmed slab that can hold `size` bytes, wrapped in an RAII lease.
    /// Returns `None` if `size > 64KB` or if pool burst ceiling is reached.
    #[inline(always)]
    pub fn acquire_for_size(self: &Arc<Self>, size: usize) -> Option<PayloadSlabLease> {
        let class = SlabClass::for_size(size)?;
        self.acquire(class)
    }

    /// Returns the number of currently checked-out active leases for a class.
    #[inline(always)]
    pub fn active_leases(&self, class: SlabClass) -> usize {
        let (_, counter) = self.bucket_and_counter(class);
        counter.load(Ordering::Relaxed)
    }

    /// Returns the permanent baseline capacity for a class.
    #[inline(always)]
    pub fn baseline_slots(&self, class: SlabClass) -> usize {
        let (bucket, _) = self.bucket_and_counter(class);
        bucket
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .baseline_limit
    }

    /// Returns the absolute max burst ceiling for a class.
    #[inline(always)]
    pub fn max_burst_cap(&self, class: SlabClass) -> usize {
        let (bucket, _) = self.bucket_and_counter(class);
        bucket
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .max_burst_limit
    }

    /// Returns the count of burst slabs currently held in standby for a class.
    #[inline(always)]
    pub fn burst_standby_count(&self, class: SlabClass) -> usize {
        let (bucket, _) = self.bucket_and_counter(class);
        bucket
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .burst
            .len()
    }

    /// Returns the total number of slabs currently sitting in the pool for a class (baseline + burst).
    #[inline(always)]
    pub fn total_pooled(&self, class: SlabClass) -> usize {
        let (bucket, _) = self.bucket_and_counter(class);
        let guard = bucket
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.baseline.len() + guard.burst.len()
    }

    /// Performs non-blocking maintenance on this core's slab pool using [`crate::coarse_now_sec`].
    ///
    /// # Strict Core-Jailing & Coarse Clock Invariant
    /// Executed exclusively by `velda-shadow-{core_id}` companion worker pinned to the same CPU core.
    /// Uses non-blocking `try_lock()`: if the Traffic Worker on this core is currently acquiring
    /// or recycling a slab, this cycle is immediately skipped with zero lock contention.
    ///
    /// # Two Responsibilities
    /// 1. **15s TTL Standby Eviction**:
    ///    Burst slabs idle $> \text{idle\_ttl\_secs}$ (default 15s) are evicted back to `jemalloc`.
    ///    Crucially, jemalloc retains dirty pages in process Resident Set Size (RSS) across its
    ///    decay window (10s), ensuring subsequent micro-bursts reacquire physical memory without
    ///    incurring kernel minor page faults.
    /// 2. **Low-Watermark Proactive Replenishment**:
    ///    When available pooled slabs drop $\le 25\%$ of baseline capacity (due to traffic spike),
    ///    the shadow worker proactively pre-warms a batch of dynamic slabs, eagerly touching each 4KB page
    ///    (physical commit) and placing them into the burst standby queue with a 15s expiration timestamp.
    ///    This guarantees that the frontline traffic worker never has to perform cold page allocations
    ///    on the hot path.
    ///
    /// Returns `(slabs_evicted, slabs_replenished)`.
    #[inline(always)]
    pub fn maintain(&self) -> (usize, usize) {
        let now_sec = crate::coarse_now_sec();
        let mut evicted = 0;
        let mut replenished = 0;

        for class in [
            SlabClass::Small4K,
            SlabClass::Medium16K,
            SlabClass::Standard32K,
            SlabClass::Large64K,
        ] {
            let (bucket_mutex, active_counter) = self.bucket_and_counter(class);
            if let Ok(mut guard) = bucket_mutex.try_lock() {
                // 1. Evict expired burst standby slabs (> idle_ttl_secs)
                while let Some((returned_at_sec, _)) = guard.burst.front() {
                    if now_sec.saturating_sub(*returned_at_sec) >= self.idle_ttl_secs {
                        if let Some((_, slab)) = guard.burst.pop_front() {
                            drop(slab); // Evicted back to jemalloc RSS arena
                            evicted += 1;
                        }
                    } else {
                        break;
                    }
                }

                // 2. Low-watermark proactive replenishment
                let available = guard.baseline.len() + guard.burst.len();
                let active = active_counter.load(Ordering::Relaxed);
                let low_watermark = (guard.baseline_limit / 4).max(1);

                if available <= low_watermark {
                    let batch_size = (guard.baseline_limit / 2).max(2);
                    let current_total = available + active;
                    let allowed = guard.max_burst_limit.saturating_sub(current_total);
                    let to_replenish = batch_size.min(allowed);
                    for _ in 0..to_replenish {
                        let slab = PayloadSlab::new(class);
                        guard.burst.push_back((now_sec, slab));
                        replenished += 1;
                    }
                }
            }
        }

        (evicted, replenished)
    }

    /// Evicts burst slabs across all classes that have been idle longer than `idle_ttl_secs`.
    #[inline(always)]
    pub fn sweep_expired(&self) -> usize {
        let now_sec = crate::coarse_now_sec();
        let mut evicted = 0;
        for class in [
            SlabClass::Small4K,
            SlabClass::Medium16K,
            SlabClass::Standard32K,
            SlabClass::Large64K,
        ] {
            let (bucket_mutex, _) = self.bucket_and_counter(class);
            if let Ok(mut guard) = bucket_mutex.try_lock() {
                while let Some((returned_at_sec, _)) = guard.burst.front() {
                    if now_sec.saturating_sub(*returned_at_sec) >= self.idle_ttl_secs {
                        if let Some((_, slab)) = guard.burst.pop_front() {
                            drop(slab); // Evicted back to allocator arena (retained in process RSS)
                            evicted += 1;
                        }
                    } else {
                        // Slabs were pushed chronologically; front is oldest
                        break;
                    }
                }
            }
        }
        evicted
    }

    /// Recycles a returned slab back into its respective class pool.
    fn recycle(&self, slab: PayloadSlab) {
        let (bucket_mutex, active_counter) = self.bucket_and_counter(slab.class);
        active_counter.fetch_sub(1, Ordering::Relaxed);

        let mut guard = bucket_mutex
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if slab.is_arena() {
            // Priority 1: Permanent Tier 1 baseline floor.
            // Slabs carved from the contiguous arena never get evicted or freed throughout process lifetime.
            guard.baseline.push(slab);
            // Invariant defense: If returning an arena slab pushes total pooled beyond max_burst_limit
            // (e.g. Due to dynamic slabs held in burst standby), trim the oldest burst standby slab.
            while guard.baseline.len() + guard.burst.len() > guard.max_burst_limit {
                if guard.burst.pop_front().is_none() {
                    break;
                }
            }
        } else if guard.baseline.len() + guard.burst.len() < guard.max_burst_limit {
            // Priority 2: Retain dynamic slab in Tier 2 burst standby queue.
            // Stamped with coarse integer clock (relaxed atomic load without syscall overhead).
            let now_sec = crate::coarse_now_sec();
            guard.burst.push_back((now_sec, slab));
        }
        // Priority 3: Above max_burst_limit -> slab drops and frees its Box<[u8]> back to jemalloc RSS arena
    }
}

/// RAII Lease for a [`PayloadSlab`]. Automatically returns the slab to the pool on drop.
///
/// # DEFENSIVE MEMORY INVARIANT: Anti-Use-After-Free, Anti-Double-Free & Sub-Slice Safety
/// 1. **Zero Use-After-Free via Compiler Borrow Checker**:
///    Deref to `[u8]` produces slice references `&'a [u8]` strictly bounded to the lifetime of
///    this lease. The moment this lease is dropped, any outstanding slice references are
///    invalidated statically by `rustc`.
/// 2. **Prohibition of "Faux-Zero-Copy" Pointer Aliasing**:
///    Data plane code MUST NEVER use `unsafe` raw pointer casting to convert a lease slice
///    into an unmanaged `bytes::Bytes` or detached pointer. Slices must remain bounded to this
///    lease, or explicitly copied when sent across uncoordinated task boundaries.
/// 3. **Anti-Data-Leakage via Explicit Sub-Slicing**:
///    When reading network I/O into this slab, request handlers MUST ONLY expose the sub-slice
///    of bytes actually read (`&lease[..bytes_read]`). Never expose the full 16KB/64KB slab buffer,
///    preventing cross-request dirty data contamination.
/// 4. **Zero Double-Free via `Option::take()`**:
///    During `drop()`, `self.slab.take()` extracts the slab out of `self`. The slab is returned
///    to the pool exactly once, eliminating double-recycle hazards.
/// 5. **Async Cancellation Resilience**:
///    If a client drops a TCP connection or Tokio cancels an in-flight stream future, stack
///    unwinding drops this lease and returns the slab cleanly without resource leaks.
pub struct PayloadSlabLease {
    slab: Option<PayloadSlab>,
    pool: Arc<PayloadSlabPool>,
}

impl std::fmt::Debug for PayloadSlabLease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PayloadSlabLease")
            .field("class", &self.class())
            .field("is_arena", &self.is_arena())
            .field("len", &self.len())
            .finish()
    }
}

impl PayloadSlabLease {
    /// Returns the slab class of this lease.
    #[inline(always)]
    pub fn class(&self) -> SlabClass {
        self.slab
            .as_ref()
            .expect("PayloadSlabLease held invalid state")
            .class()
    }

    /// Returns `true` if this lease holds a Tier 1 contiguous arena slab.
    #[inline(always)]
    pub fn is_arena(&self) -> bool {
        self.slab.as_ref().is_some_and(|s| s.is_arena())
    }

    /// Returns `true` if this lease holds a Tier 2 dynamic burst slab.
    #[inline(always)]
    pub fn is_dynamic(&self) -> bool {
        self.slab.as_ref().is_some_and(|s| s.is_dynamic())
    }

    /// Borrows the slab as an immutable byte slice.
    #[inline(always)]
    pub fn as_slice(&self) -> &[u8] {
        self.deref()
    }

    /// Borrows the slab as a mutable byte slice.
    #[inline(always)]
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        self.deref_mut()
    }

    /// Copies data from `src` into the beginning of this leased slab using Register-Adaptive Non-Temporal streaming stores.
    #[inline(always)]
    pub fn copy_from_non_temporal(&mut self, src: &[u8]) {
        self.slab
            .as_mut()
            .expect("PayloadSlabLease held invalid state")
            .copy_from_non_temporal(src);
    }

    /// Writes data from `src` into this leased slab starting at `offset` using Register-Adaptive Non-Temporal streaming stores.
    #[inline(always)]
    pub fn write_non_temporal(&mut self, offset: usize, src: &[u8]) {
        self.slab
            .as_mut()
            .expect("PayloadSlabLease held invalid state")
            .write_non_temporal(offset, src);
    }

    /// Zeroes `len` bytes starting at `offset` using Register-Adaptive Non-Temporal streaming stores.
    #[inline(always)]
    pub fn zero_non_temporal(&mut self, offset: usize, len: usize) {
        self.slab
            .as_mut()
            .expect("PayloadSlabLease held invalid state")
            .zero_non_temporal(offset, len);
    }

    /// Issues hardware Non-Temporal Read hints (`_MM_HINT_NTA` / `pldl1strm`) across the leased slab's memory.
    #[inline(always)]
    pub fn prefetch_nta(&self, offset: usize, len: usize) {
        self.slab
            .as_ref()
            .expect("PayloadSlabLease held invalid state")
            .prefetch_nta(offset, len);
    }

    /// Issues hardware Tier-3 (`T2` / L3 LLC Cache) Prefetch hints (`_MM_HINT_T2` / `pldl3keep`) across the leased slab's memory.
    #[inline(always)]
    pub fn prefetch_l3(&self, offset: usize, len: usize) {
        self.slab
            .as_ref()
            .expect("PayloadSlabLease held invalid state")
            .prefetch_l3(offset, len);
    }
}

impl Deref for PayloadSlabLease {
    type Target = [u8];

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        self.slab
            .as_ref()
            .expect("PayloadSlabLease held invalid state")
            .as_slice()
    }
}

impl DerefMut for PayloadSlabLease {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.slab
            .as_mut()
            .expect("PayloadSlabLease held invalid state")
            .as_mut_slice()
    }
}

impl Drop for PayloadSlabLease {
    fn drop(&mut self) {
        if let Some(slab) = self.slab.take() {
            self.pool.recycle(slab);
        }
    }
}

const _: () = {
    const fn assert_send_sync<T: Send + Sync>() {}
    let _ = assert_send_sync::<PayloadSlab>;
    let _ = assert_send_sync::<PayloadSlabLease>;
    let _ = assert_send_sync::<PayloadSlabPool>;
    let _ = assert_send_sync::<ContiguousArena>;
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_payload_slab_warmup_and_sizes() {
        let small = PayloadSlab::new(SlabClass::Small4K);
        assert_eq!(small.len(), 4096);
        assert_eq!(small.class(), SlabClass::Small4K);
        assert!(small.is_dynamic());
        assert!(!small.is_arena());

        let med = PayloadSlab::new(SlabClass::Medium16K);
        assert_eq!(med.len(), 16384);
        assert_eq!(med.class(), SlabClass::Medium16K);

        let standard = PayloadSlab::new(SlabClass::Standard32K);
        assert_eq!(standard.len(), 32768);
        assert_eq!(standard.class(), SlabClass::Standard32K);

        let large = PayloadSlab::new(SlabClass::Large64K);
        assert_eq!(large.len(), 65536);
        assert_eq!(large.class(), SlabClass::Large64K);
    }

    #[test]
    fn test_slab_class_selection_contiguous_slice() {
        assert_eq!(SlabClass::for_size(100), Some(SlabClass::Small4K));
        assert_eq!(SlabClass::for_size(4096), Some(SlabClass::Small4K));
        assert_eq!(SlabClass::for_size(4097), Some(SlabClass::Medium16K));
        assert_eq!(SlabClass::for_size(16384), Some(SlabClass::Medium16K));
        // 18KB payload fits in 32KB standard slab (contiguous single slice &[u8])
        assert_eq!(SlabClass::for_size(18 * 1024), Some(SlabClass::Standard32K));
        assert_eq!(SlabClass::for_size(32768), Some(SlabClass::Standard32K));
        assert_eq!(SlabClass::for_size(32769), Some(SlabClass::Large64K));
        assert_eq!(SlabClass::for_size(65536), Some(SlabClass::Large64K));
        assert_eq!(SlabClass::for_size(65537), None);
    }

    #[test]
    fn test_payload_slab_pool_acquire_and_recycle() {
        let pool = PayloadSlabPool::new(2, 2, 2, 2);
        assert_eq!(pool.baseline_slots(SlabClass::Small4K), 2);
        assert_eq!(pool.active_leases(SlabClass::Small4K), 0);

        {
            let mut lease = pool
                .acquire(SlabClass::Small4K)
                .expect("acquire small slab");
            assert_eq!(pool.active_leases(SlabClass::Small4K), 1);
            assert_eq!(lease.len(), 4096);
            assert!(lease.is_arena());
            assert!(!lease.is_dynamic());

            lease[0] = 0xAA;
            lease[4095] = 0xBB;
            assert_eq!(lease[0], 0xAA);
            assert_eq!(lease[4095], 0xBB);
        }

        // Lease dropped -> active reset to 0
        assert_eq!(pool.active_leases(SlabClass::Small4K), 0);

        // Next acquire reuses the slot
        let lease2 = pool
            .acquire(SlabClass::Small4K)
            .expect("acquire small slab 2");
        assert_eq!(pool.active_leases(SlabClass::Small4K), 1);
        assert_eq!(lease2.len(), 4096);
        assert!(lease2.is_arena());
    }

    #[test]
    fn test_payload_slab_pool_adaptive_burst_and_decay() {
        // Pool with baseline=2, max_burst_cap=4, idle_ttl_secs=15
        let pool = PayloadSlabPool::with_adaptive_limits(AdaptiveSlabLimits {
            small_baseline: 2,
            small_max_cap: 4,
            medium_baseline: 2,
            medium_max_cap: 4,
            standard_baseline: 2,
            standard_max_cap: 4,
            large_baseline: 2,
            large_max_cap: 4,
            idle_ttl_secs: 15,
        });
        assert_eq!(pool.baseline_slots(SlabClass::Medium16K), 2);
        assert_eq!(pool.max_burst_cap(SlabClass::Medium16K), 4);

        // 1. Acquire baseline (2 slots)
        let l1 = pool
            .acquire(SlabClass::Medium16K)
            .expect("lease 1 (baseline)");
        let l2 = pool
            .acquire(SlabClass::Medium16K)
            .expect("lease 2 (baseline)");
        assert_eq!(pool.active_leases(SlabClass::Medium16K), 2);
        assert!(l1.is_arena());
        assert!(l2.is_arena());

        // 2. Acquire burst (2 slots spillover)
        let l3 = pool.acquire(SlabClass::Medium16K).expect("lease 3 (burst)");
        let l4 = pool.acquire(SlabClass::Medium16K).expect("lease 4 (burst)");
        assert_eq!(pool.active_leases(SlabClass::Medium16K), 4);
        assert!(l3.is_dynamic());
        assert!(l4.is_dynamic());

        // 3. Attempt 5th acquire -> Anti-OOM cap reached -> returns None!
        assert!(pool.acquire(SlabClass::Medium16K).is_none());

        // 4. Drop all leases
        drop(l1);
        drop(l2);
        drop(l3);
        drop(l4);
        assert_eq!(pool.active_leases(SlabClass::Medium16K), 0);

        // 2 refilled baseline, 2 in burst standby queue
        assert_eq!(pool.total_pooled(SlabClass::Medium16K), 4);
        assert_eq!(pool.burst_standby_count(SlabClass::Medium16K), 2);

        // Immediate sweep should evict 0 because 15s TTL hasn't elapsed
        let evicted = pool.sweep_expired();
        assert_eq!(evicted, 0);
        assert_eq!(pool.total_pooled(SlabClass::Medium16K), 4);

        // Simulate time passage (> 15s) by backdating timestamps on burst slabs
        {
            let mut guard = pool.medium_bucket.lock().unwrap();
            for (ts, _) in guard.burst.iter_mut() {
                *ts = ts.saturating_sub(20);
            }
        }
        let evicted = pool.sweep_expired();
        assert_eq!(evicted, 2);

        // Pool cleanly decayed back to baseline floor (2)
        assert_eq!(pool.total_pooled(SlabClass::Medium16K), 2);
        assert_eq!(pool.burst_standby_count(SlabClass::Medium16K), 0);
    }

    #[test]
    fn test_shadow_low_watermark_replenishment_and_rss_decay() {
        // Pool with baseline=4, max_burst_cap=12, idle_ttl_secs=15
        let pool = PayloadSlabPool::with_adaptive_limits(AdaptiveSlabLimits {
            small_baseline: 4,
            small_max_cap: 12,
            medium_baseline: 4,
            medium_max_cap: 12,
            standard_baseline: 4,
            standard_max_cap: 12,
            large_baseline: 4,
            large_max_cap: 12,
            idle_ttl_secs: 15,
        });
        assert_eq!(pool.baseline_slots(SlabClass::Standard32K), 4);

        // Traffic worker checks out 4 baseline slabs (pool available drops to 0 <= low_watermark 1)
        let l1 = pool.acquire(SlabClass::Standard32K).unwrap();
        let l2 = pool.acquire(SlabClass::Standard32K).unwrap();
        let l3 = pool.acquire(SlabClass::Standard32K).unwrap();
        let l4 = pool.acquire(SlabClass::Standard32K).unwrap();
        assert_eq!(pool.active_leases(SlabClass::Standard32K), 4);
        assert_eq!(pool.total_pooled(SlabClass::Standard32K), 0);

        // Companion Shadow Logistics Worker runs inline maintenance on the same core:
        // Automatically reads coarse_now_sec(), detects low watermark (0 <= 1), pre-warms a batch of slabs
        let (evicted, replenished) = pool.maintain();
        assert_eq!(evicted, 0);
        assert!(replenished > 0);
        assert!(pool.burst_standby_count(SlabClass::Standard32K) >= 2);

        // Frontline traffic worker acquires next slab instantly from warm burst queue (0 alloc)!
        let l5 = pool.acquire(SlabClass::Standard32K).unwrap();
        assert_eq!(l5.len(), 32768);
        assert!(l5.is_dynamic());

        drop(l1);
        drop(l2);
        drop(l3);
        drop(l4);
        drop(l5);

        // After traffic burst subsides, simulate time passage (> 15s) by backdating burst timestamps
        {
            let mut guard = pool.standard_bucket.lock().unwrap();
            for (ts, _) in guard.burst.iter_mut() {
                *ts = ts.saturating_sub(20);
            }
        }
        let (evicted_after, _) = pool.maintain();
        assert!(evicted_after > 0);
        // Pool cleanly returns to baseline floor without resource leak
        assert_eq!(pool.burst_standby_count(SlabClass::Standard32K), 0);
    }

    #[test]
    fn test_contiguous_baseline_arena_slicing_and_validity() {
        let limits = AdaptiveSlabLimits {
            small_baseline: 4,
            small_max_cap: 8,
            medium_baseline: 2,
            medium_max_cap: 4,
            standard_baseline: 2,
            standard_max_cap: 4,
            large_baseline: 1,
            large_max_cap: 2,
            idle_ttl_secs: 15,
        };

        // Expected arena size: 4 * 4KB + 2 * 16KB + 2 * 32KB + 1 * 64KB = 16K + 32K + 64K + 64K = 176 KB = 180224 bytes
        let expected_total_bytes = 4 * 4096 + 2 * 16384 + 2 * 32768 + 65536;
        assert_eq!(expected_total_bytes, 180224);

        let pool = PayloadSlabPool::with_adaptive_limits(limits);
        assert_eq!(pool.arena_total_bytes(), expected_total_bytes);

        let arena_start = pool.arena_ptr() as usize;
        let arena_end = arena_start + expected_total_bytes;

        // Verify base pointer is 4096-byte page-aligned
        assert_eq!(
            arena_start % PAGE_SIZE,
            0,
            "Arena base pointer must be 4096-byte page-aligned"
        );

        // Acquire all baseline slabs
        let mut leases = Vec::new();
        for _ in 0..4 {
            leases.push(pool.acquire(SlabClass::Small4K).expect("small slab"));
        }
        for _ in 0..2 {
            leases.push(pool.acquire(SlabClass::Medium16K).expect("medium slab"));
        }
        for _ in 0..2 {
            leases.push(pool.acquire(SlabClass::Standard32K).expect("standard slab"));
        }
        leases.push(pool.acquire(SlabClass::Large64K).expect("large slab"));

        assert_eq!(leases.len(), 9);

        // Collect slab ranges (start, end, len)
        let mut ranges: Vec<(usize, usize, usize)> = leases
            .iter()
            .map(|l| {
                let start = l.as_ptr() as usize;
                let len = l.len();
                let end = start + len;
                (start, end, len)
            })
            .collect();

        // Sort by start address
        ranges.sort_by_key(|r| r.0);

        // Verify that the first slab begins at the arena start
        assert_eq!(ranges[0].0, arena_start);

        // Verify each slab is page-aligned (multiple of 4096)
        for (i, &(start, _, _)) in ranges.iter().enumerate() {
            assert_eq!(
                start % PAGE_SIZE,
                0,
                "Slab {} at address {:#x} must be 4096-byte page-aligned",
                i,
                start
            );
        }

        // Verify strict contiguity: slab[i].end == slab[i+1].start with 0 wasted gaps
        for i in 0..ranges.len() - 1 {
            assert_eq!(
                ranges[i].1,
                ranges[i + 1].0,
                "Slab {} end ({:#x}) must match slab {} start ({:#x}) without gaps",
                i,
                ranges[i].1,
                i + 1,
                ranges[i + 1].0
            );
        }

        // Verify that the last slab ends exactly at arena_end
        assert_eq!(ranges.last().unwrap().1, arena_end);

        // Verify memory independence: write unique pattern to each slab and assert no corruption
        for (idx, lease) in leases.iter_mut().enumerate() {
            assert!(lease.is_arena());
            assert!(!lease.is_dynamic());
            let fill_byte = (idx as u8) + 1;
            lease[0] = fill_byte;
            let len = lease.len();
            lease[len - 1] = 0xAA ^ fill_byte;
        }

        // Read back and verify all patterns are intact (disjoint memory ranges)
        for (idx, lease) in leases.iter().enumerate() {
            let fill_byte = (idx as u8) + 1;
            assert_eq!(lease[0], fill_byte);
            let len = lease.len();
            assert_eq!(lease[len - 1], 0xAA ^ fill_byte);
        }

        // Drop all leases
        drop(leases);

        // Verify all baseline pools are refilled and 0 active leases remain
        assert_eq!(pool.active_leases(SlabClass::Small4K), 0);
        assert_eq!(pool.active_leases(SlabClass::Medium16K), 0);
        assert_eq!(pool.active_leases(SlabClass::Standard32K), 0);
        assert_eq!(pool.active_leases(SlabClass::Large64K), 0);
        assert_eq!(pool.total_pooled(SlabClass::Small4K), 4);
        assert_eq!(pool.total_pooled(SlabClass::Medium16K), 2);
        assert_eq!(pool.total_pooled(SlabClass::Standard32K), 2);
        assert_eq!(pool.total_pooled(SlabClass::Large64K), 1);
    }

    #[test]
    fn test_burst_spillover_and_ttl_expiration() {
        let pool = PayloadSlabPool::with_adaptive_limits(AdaptiveSlabLimits {
            small_baseline: 2,
            small_max_cap: 5,
            medium_baseline: 1,
            medium_max_cap: 3,
            standard_baseline: 1,
            standard_max_cap: 3,
            large_baseline: 1,
            large_max_cap: 3,
            idle_ttl_secs: 15,
        });

        // 1. Acquire 2 baseline slabs
        let l1 = pool.acquire(SlabClass::Small4K).expect("baseline 1");
        let l2 = pool.acquire(SlabClass::Small4K).expect("baseline 2");
        assert!(l1.is_arena());
        assert!(l2.is_arena());
        assert_eq!(pool.active_leases(SlabClass::Small4K), 2);

        // 2. Spill over into dynamic burst allocations
        let l3 = pool.acquire(SlabClass::Small4K).expect("burst 1");
        let l4 = pool.acquire(SlabClass::Small4K).expect("burst 2");
        let l5 = pool.acquire(SlabClass::Small4K).expect("burst 3");
        assert!(l3.is_dynamic());
        assert!(l4.is_dynamic());
        assert!(l5.is_dynamic());
        assert_eq!(pool.active_leases(SlabClass::Small4K), 5);

        // 3. Exceeding max_cap returns None (Anti-OOM protection)
        assert!(pool.acquire(SlabClass::Small4K).is_none());

        // 4. Return dynamic slabs first
        drop(l3);
        drop(l4);
        drop(l5);
        assert_eq!(pool.active_leases(SlabClass::Small4K), 2);
        assert_eq!(pool.burst_standby_count(SlabClass::Small4K), 3);

        // Immediate sweep should evict 0 because TTL hasn't expired
        let evicted = pool.sweep_expired();
        assert_eq!(evicted, 0);

        // Simulate TTL passage by backdating timestamps
        {
            let mut guard = pool.small_bucket.lock().unwrap();
            for (ts, _) in guard.burst.iter_mut() {
                *ts = ts.saturating_sub(20);
            }
        }

        // Sweep expired drops all 3 transient burst slabs
        let evicted = pool.sweep_expired();
        assert_eq!(evicted, 3);
        assert_eq!(pool.burst_standby_count(SlabClass::Small4K), 0);

        // 5. Return baseline arena slabs
        drop(l1);
        drop(l2);
        assert_eq!(pool.active_leases(SlabClass::Small4K), 0);
        assert_eq!(pool.total_pooled(SlabClass::Small4K), 2);

        // Subsequent acquire retrieves the permanent arena slab
        let reacquired = pool.acquire(SlabClass::Small4K).expect("reacquired");
        assert!(reacquired.is_arena());
    }

    #[test]
    fn test_raii_lease_mutation_and_lifecycle() {
        let pool = PayloadSlabPool::new(1, 1, 1, 1);

        for class in [
            SlabClass::Small4K,
            SlabClass::Medium16K,
            SlabClass::Standard32K,
            SlabClass::Large64K,
        ] {
            let mut lease = pool.acquire(class).expect("acquire lease");
            assert_eq!(lease.class(), class);
            assert_eq!(lease.len(), class.byte_size());
            assert!(!lease.is_empty());

            // Write through DerefMut
            lease[0] = 0x12;
            let last_idx = lease.len() - 1;
            lease[last_idx] = 0x34;

            // Read through Deref
            assert_eq!(lease[0], 0x12);
            assert_eq!(lease[last_idx], 0x34);

            // Sub-slice borrowing
            let slice = &lease[..4];
            assert_eq!(slice[0], 0x12);

            // Mut slice method
            lease.as_mut_slice()[1] = 0x56;
            assert_eq!(lease.as_slice()[1], 0x56);

            drop(lease);
            assert_eq!(pool.active_leases(class), 0);
        }
    }

    #[test]
    fn test_multithreaded_slab_concurrency() {
        let pool = PayloadSlabPool::with_adaptive_limits(AdaptiveSlabLimits {
            small_baseline: 8,
            small_max_cap: 32,
            medium_baseline: 8,
            medium_max_cap: 32,
            standard_baseline: 4,
            standard_max_cap: 16,
            large_baseline: 4,
            large_max_cap: 16,
            idle_ttl_secs: 15,
        });

        let mut handles = Vec::new();
        let num_threads = 8;
        let iterations_per_thread = 100;

        for thread_id in 0..num_threads {
            let pool_clone = Arc::clone(&pool);
            handles.push(std::thread::spawn(move || {
                for i in 0..iterations_per_thread {
                    let class = match (thread_id + i) % 4 {
                        0 => SlabClass::Small4K,
                        1 => SlabClass::Medium16K,
                        2 => SlabClass::Standard32K,
                        _ => SlabClass::Large64K,
                    };

                    let mut lease = pool_clone.acquire(class).expect("thread lease acquisition");
                    let expected_val = ((thread_id * 17 + i) % 251 + 1) as u8;

                    // Mutate buffer
                    lease[0] = expected_val;
                    let last = lease.len() - 1;
                    lease[last] = expected_val ^ 0xFF;

                    // Verify consistency
                    assert_eq!(lease[0], expected_val);
                    assert_eq!(lease[last], expected_val ^ 0xFF);

                    // Drop lease
                    drop(lease);
                }
            }));
        }

        for handle in handles {
            handle.join().expect("thread completed successfully");
        }

        // Verify that after all threads join, all active leases return to 0
        assert_eq!(pool.active_leases(SlabClass::Small4K), 0);
        assert_eq!(pool.active_leases(SlabClass::Medium16K), 0);
        assert_eq!(pool.active_leases(SlabClass::Standard32K), 0);
        assert_eq!(pool.active_leases(SlabClass::Large64K), 0);
    }

    #[test]
    fn test_zero_baseline_pool() {
        let pool = PayloadSlabPool::with_adaptive_limits(AdaptiveSlabLimits {
            small_baseline: 0,
            small_max_cap: 4,
            medium_baseline: 0,
            medium_max_cap: 4,
            standard_baseline: 0,
            standard_max_cap: 4,
            large_baseline: 0,
            large_max_cap: 4,
            idle_ttl_secs: 15,
        });

        assert_eq!(pool.arena_total_bytes(), 0);

        let lease = pool
            .acquire(SlabClass::Small4K)
            .expect("dynamic acquire with 0 baseline");
        assert!(lease.is_dynamic());
        assert!(!lease.is_arena());
        drop(lease);

        assert_eq!(pool.burst_standby_count(SlabClass::Small4K), 1);
    }

    #[test]
    fn test_non_temporal_copy_and_zero_alignment() {
        let mut src = vec![0u8; 4137];
        for (i, b) in src.iter_mut().enumerate() {
            *b = ((i * 31 + 17) & 0xFF) as u8;
        }

        // Test non-temporal streaming copy and zeroing across aligned and unaligned offsets (0, 1, 3, 7, 15, 31)
        for &offset in &[0usize, 1, 3, 7, 15, 31] {
            let mut dst = vec![0xEEu8; src.len() + 64];
            copy_non_temporal(&src, &mut dst[offset..offset + src.len()]);
            assert_eq!(&dst[offset..offset + src.len()], &src[..]);
            assert!(dst[..offset].iter().all(|&b| b == 0xEE));
            assert!(dst[offset + src.len()..].iter().all(|&b| b == 0xEE));

            zero_non_temporal(&mut dst[offset..offset + src.len()]);
            assert!(dst[offset..offset + src.len()].iter().all(|&b| b == 0));
            assert!(dst[..offset].iter().all(|&b| b == 0xEE));
            assert!(dst[offset + src.len()..].iter().all(|&b| b == 0xEE));
        }

        // Also test via PayloadSlabLease API
        let pool = PayloadSlabPool::new(1, 1, 1, 1);
        let mut lease = pool.acquire(SlabClass::Small4K).unwrap();
        lease.copy_from_non_temporal(&src[..4096]);
        lease.prefetch_nta(0, 4096);
        lease.prefetch_l3(0, 4096);
        assert_eq!(&lease[..4096], &src[..4096]);
        lease.zero_non_temporal(0, 4096);
        assert!(lease.iter().all(|&b| b == 0));
    }

    #[test]
    fn test_lifo_burst_reuse_and_fifo_ttl_eviction() {
        let pool = PayloadSlabPool::with_adaptive_limits(AdaptiveSlabLimits {
            small_baseline: 1,
            small_max_cap: 4,
            medium_baseline: 1,
            medium_max_cap: 4,
            standard_baseline: 1,
            standard_max_cap: 4,
            large_baseline: 1,
            large_max_cap: 4,
            idle_ttl_secs: 15,
        });

        let base = pool.acquire(SlabClass::Small4K).unwrap();
        let b1 = pool.acquire(SlabClass::Small4K).unwrap();
        let b2 = pool.acquire(SlabClass::Small4K).unwrap();
        let b1_ptr = b1.as_ptr() as usize;
        let b2_ptr = b2.as_ptr() as usize;

        // Drop b1 first (pushed to burst back -> now at front), then b2 (pushed to burst back)
        drop(b1);
        drop(b2);
        assert_eq!(pool.burst_standby_count(SlabClass::Small4K), 2);

        // LIFO acquire must pop_back -> returns b2 (the warmest, most recently returned slab!)
        let reacquired = pool.acquire(SlabClass::Small4K).unwrap();
        assert_eq!(reacquired.as_ptr() as usize, b2_ptr);
        assert_ne!(reacquired.as_ptr() as usize, b1_ptr);
        drop(reacquired);
        drop(base);
    }
}
