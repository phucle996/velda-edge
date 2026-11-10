//! # Zero-Copy RDMA Slab Memory Arena
//!
//! ## Architectural Mechanism & Operating Principles
//!
//! Conventional network socket forwarding incurs substantial CPU and memory overhead due to repeated
//! data copying (`memcpy`) across user-space buffers and kernel socket buffers (`sk_buff`). Even in
//! standard RDMA pipelines, dynamically pinning and registering memory via `ibv_reg_mr` on the request
//! hot path introduces severe kernel transition penalties and thrashing of the Host Channel Adapter's
//! (HCA) onboard Memory Translation Table (MTT) cache.
//!
//! `RdmaSlabArena` solves this by pre-allocating a contiguous, page-aligned block of host memory
//! (baseline 8MB) and registering it **exactly once** with the RDMA Protection Domain upfront.
//!
//! ### Key Architectural Invariants
//! 1. **Single Registration & MTT Resilience**:
//!    The entire 8MB slab memory block is registered with a single `ibv_reg_mr` call, generating a single
//!    `lkey` and occupying exactly 1 contiguous MTT entry in the NIC. This completely prevents MTT cache
//!    eviction loops during multi-gigabit request bursts.
//! 2. **Canonical Slot Sizing & Alignment**:
//!    - **`Standard2K` (2048 bytes)**: Sized for standard Ethernet MTU (1500 bytes) + framing headers
//!      and metadata. Exactly 2 standard slots fit into a single 4096-byte Linux page with zero fragmentation.
//!    - **`Large64K` (65536 bytes = 32 x 2048 bytes)**: Sized for Linux UDP Generic Receive Offload (GRO)
//!      coalesced packets and bulk TCP stream chunks. Because each 64KB slot occupies a contiguous memory span,
//!      outbound Work Requests are submitted with a single Scatter-Gather Entry (`num_sge = 1`). This eliminates
//!      PCIe descriptor pressure on the HCA and allows the rNIC DMA engine to execute full-line burst reads.
//! 3. **Lock-Free Atomic Bitmask Allocation**:
//!    Slot availability is tracked via arrays of `AtomicU64` words. Slot leasing and recycling operate
//!    via atomic bit manipulation (`compare_exchange_weak` and `fetch_and`), guaranteeing zero mutex
//!    contention on frontline worker hot paths.
//! 4. **RAII Slot Lifecycle (`RdmaSlabLease`)**:
//!    Buffers are leased as RAII guards implementing `Deref` and `DerefMut` to `[u8]`. Upon drop, the lease
//!    atomically returns its slot index to the arena's bitmask with a single atomic instruction, without
//!    deallocating or deregistering memory.
//! 5. **Dual Operation Mode (Hardware & Mock)**:
//!    - **Hardware Mode**: Registers memory directly with Mellanox/InfiniBand hardware drivers via `ibv_reg_mr`.
//!    - **Mock Mode**: Operates on aligned host memory without Verbs syscalls, ensuring tests and non-RDMA
//!      environments execute reliably.
//! 6. **Deliberate Hardware Cache Partitioning (Pool Metadata in L2 vs DMA Payload in L3/DRAM)**:
//!    - **Pool Management Bitmasks (`standard_masks`, `large_masks`) -> L2 Cache (`_MM_HINT_T1`)**:
//!      Slot allocation bitmasks are tiny (64-256 bytes) and accessed on every request, so they are
//!      prefetched into L2 (`prefetch_l2_range`) while leaving Core L1D (`_MM_HINT_T0`) exclusively
//!      for `velda-router`.
//!    - **RDMA Payload Slots (`Standard2K`, `Large64K`) -> L3 LLC (`_MM_HINT_T2`) & Non-Temporal WC (`NTA`)**:
//!      PCIe rNIC DMA controllers interact directly with L3 Last-Level Cache (Intel DDIO / AMD SDCI) and DRAM.
//!      Leased payload slots are prefetched into L3 (`_MM_HINT_T2`)—never L1D/L2—to prevent 64KB buffers
//!      from evicting routing tables and to avoid PCIe snoop-invalidation stalls on core private caches.
//!    - **Non-Temporal CWE-226 Zeroing on Drop (`ZERO_NON_TEMPORAL_FN`)**:
//!      Sanitizing a 64KB slot upon `Drop` uses Non-Temporal Write-Combining streaming stores (`vmovntdq` /
//!      `movntdq` / `movnti`), flushing zeros directly to L3/DRAM with zero Read-For-Ownership (RFO) traffic
//!      and zero L1D/L2 cache pollution.

use std::alloc::{Layout, alloc_zeroed, dealloc};
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use crate::config::RdmaConfig;
use crate::error::RdmaError;
use crate::ffi::IBV_ACCESS_LOCAL_WRITE;
use crate::verbs::device::RdmaDevice;
use crate::verbs::mr::MemoryRegion;
use crate::verbs::pd::ProtectionDomain;

/// Canonical slot size classes for zero-copy RDMA buffer allocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RdmaSlabClass {
    /// 2048 bytes (2KB): Sized for standard Ethernet MTU 1500 + framing metadata.
    /// Aligns cleanly into 4KB OS pages (2 slots per 4KB page) with zero internal fragmentation.
    Standard2K = 2048,
    /// 65536 bytes (64KB = 32 x 2048 bytes): Sized for coalesced Linux UDP GRO datagrams
    /// and bulk TCP stream chunks. Dispatched as a single contiguous SGE (`num_sge = 1`)
    /// to eliminate NIC PCIe descriptor pressure.
    Large64K = 65536,
}

/// Standard 2048-byte (2KB) slot size for Ethernet MTU frames.
pub const STANDARD_SLOT_SIZE_2K: usize = 2048;
/// Large 65536-byte (64KB) slot size for coalesced UDP GRO datagrams and bulk TCP stream chunks.
pub const LARGE_SLOT_SIZE_64K: usize = 65536;
/// Page alignment boundary (4096 bytes) for RDMA physical memory pinning.
pub const RDMA_ARENA_PAGE_SIZE: usize = 4096;
/// Number of bits per AtomicU64 allocation word.
const BITS_PER_ATOMIC_WORD: usize = 64;
/// Bitmask for aligning offsets to the 64KB boundary.
const LARGE_SLOT_ALIGNMENT_MASK: usize = LARGE_SLOT_SIZE_64K - 1;

impl RdmaSlabClass {
    pub const STANDARD_2K_SIZE: usize = STANDARD_SLOT_SIZE_2K;
    pub const LARGE_64K_SIZE: usize = LARGE_SLOT_SIZE_64K;

    /// Returns the exact size in bytes for this slab class.
    #[inline(always)]
    pub const fn byte_size(self) -> usize {
        self as usize
    }

    /// Returns the static label string for this slab class.
    #[inline(always)]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Standard2K => "Standard2K",
            Self::Large64K => "Large64K",
        }
    }

    /// Selects the smallest pre-warmed slab class that fits `len` bytes, or `None` if `len > 64KB`.
    #[inline(always)]
    pub const fn for_size(len: usize) -> Option<Self> {
        if len <= Self::STANDARD_2K_SIZE {
            Some(Self::Standard2K)
        } else if len <= Self::LARGE_64K_SIZE {
            Some(Self::Large64K)
        } else {
            None
        }
    }
}

/// Default number of 2048-byte standard slots (4MB total capacity).
pub const DEFAULT_STANDARD_SLOT_COUNT: usize = 2048;
/// Default number of 65536-byte large slots (4MB total capacity).
pub const DEFAULT_LARGE_SLOT_COUNT: usize = 64;

/// Configuration parameters for pre-registered RDMA slab memory arenas.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RdmaSlabConfig {
    /// Number of 2048-byte (2KB) standard slots. Defaults to 2048 slots (4MB).
    pub standard_count: usize,
    /// Number of 65536-byte (64KB) large slots. Defaults to 64 slots (4MB).
    pub large_count: usize,
}

impl Default for RdmaSlabConfig {
    fn default() -> Self {
        Self {
            standard_count: DEFAULT_STANDARD_SLOT_COUNT,
            large_count: DEFAULT_LARGE_SLOT_COUNT,
        }
    }
}

impl RdmaSlabConfig {
    /// Computes the exact contiguous byte capacity required for this arena configuration.
    #[inline]
    pub fn total_bytes(&self) -> usize {
        let standard_bytes = self.standard_count * STANDARD_SLOT_SIZE_2K;
        let large_offset =
            (standard_bytes + LARGE_SLOT_ALIGNMENT_MASK) & !LARGE_SLOT_ALIGNMENT_MASK;
        large_offset + self.large_count * LARGE_SLOT_SIZE_64K
    }

    /// Computes the aligned offset within the arena memory where large 64KB slots begin.
    #[inline]
    pub fn large_offset(&self) -> usize {
        let standard_bytes = self.standard_count * STANDARD_SLOT_SIZE_2K;
        (standard_bytes + LARGE_SLOT_ALIGNMENT_MASK) & !LARGE_SLOT_ALIGNMENT_MASK
    }
}

/// Parallel bitmap scanning function signature for pre-registered RDMA memory arenas.
///
/// Dispatched dynamically via cached function pointer resolved at initialization
/// based on detected CPU capability (AVX2 256-bit, SSE2 128-bit, or SWAR 64-bit integer registers).
pub type BitmapScanFn = fn(words: &[AtomicU64], start: usize, word_mask: usize) -> Option<usize>;

/// Internal shared state of the RDMA slab arena.
struct RdmaSlabArenaInner {
    base_ptr: *mut u8,
    layout: Layout,
    mr: Option<MemoryRegion>,
    lkey: u32,
    config: RdmaSlabConfig,
    standard_masks: Box<[AtomicU64]>,
    large_masks: Box<[AtomicU64]>,
    standard_word_mask: usize,
    large_word_mask: usize,
    standard_hint: AtomicUsize,
    large_hint: AtomicUsize,
    standard_used: AtomicUsize,
    large_used: AtomicUsize,
}

unsafe impl Send for RdmaSlabArenaInner {}
unsafe impl Sync for RdmaSlabArenaInner {}

impl Drop for RdmaSlabArenaInner {
    fn drop(&mut self) {
        // Step 1: Explicitly drop MemoryRegion first to deregister from HCA before freeing memory
        self.mr.take();

        // Step 2: Return contiguous page-aligned virtual memory block to OS allocator
        if !self.base_ptr.is_null() {
            unsafe {
                dealloc(self.base_ptr, self.layout);
            }
        }
    }
}

// ============================================================================
// Universe 1: AVX2 256-bit Vector Lane Scanning (4x 64-bit words = 256 slots)
// ============================================================================
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn scan_avx2_256_inner(
    words: &[AtomicU64],
    start: usize,
    _word_mask: usize,
) -> Option<usize> {
    use core::arch::x86_64::*;
    unsafe {
        let num_chunks = words.len() / 4;
        let start_chunk = (start / 4) & (num_chunks - 1);
        let all_ones = _mm256_set1_epi64x(-1);

        for chunk in 0..num_chunks {
            let chunk_idx = (start_chunk + chunk) & (num_chunks - 1);
            let base_word = chunk_idx * 4;
            let ptr = words.as_ptr().add(base_word) as *const __m256i;
            let ymm = _mm256_loadu_si256(ptr);
            let cmp = _mm256_cmpeq_epi64(ymm, all_ones);
            let mask = _mm256_movemask_pd(_mm256_castsi256_pd(cmp));
            if mask != 0x0F {
                let free_lane = (!mask & 0x0F).trailing_zeros() as usize;
                return Some(base_word + free_lane);
            }
        }
    }
    None
}

#[cfg(target_arch = "x86_64")]
fn scan_avx2_256(words: &[AtomicU64], start: usize, word_mask: usize) -> Option<usize> {
    if words.len() >= 4 {
        unsafe { scan_avx2_256_inner(words, start, word_mask) }
    } else {
        scan_swar_64(words, start, word_mask)
    }
}

// ============================================================================
// Universe 2: SSE2 128-bit Vector Lane Scanning (2x 64-bit words = 128 slots)
// Baseline on 100% of x86_64 CPUs since 2003 (Zero downclocking risk)
// ============================================================================
#[cfg(target_arch = "x86_64")]
fn scan_sse2_128(words: &[AtomicU64], start: usize, word_mask: usize) -> Option<usize> {
    use core::arch::x86_64::*;
    if words.len() < 2 {
        return scan_swar_64(words, start, word_mask);
    }
    let num_pairs = words.len() / 2;
    let start_pair = (start / 2) & (num_pairs - 1);
    let all_ones = unsafe { _mm_set1_epi32(-1) };

    for pair in 0..num_pairs {
        let pair_idx = (start_pair + pair) & (num_pairs - 1);
        let base_word = pair_idx * 2;
        let ptr = unsafe { words.as_ptr().add(base_word) as *const __m128i };
        let xmm = unsafe { _mm_loadu_si128(ptr) };
        let cmp = unsafe { _mm_cmpeq_epi32(xmm, all_ones) };
        let mask = unsafe { _mm_movemask_ps(_mm_castsi128_ps(cmp)) };
        if mask != 0x0F {
            if (mask & 0x03) != 0x03 {
                return Some(base_word);
            } else {
                return Some(base_word + 1);
            }
        }
    }
    None
}

// ============================================================================
// Universe 3: Pure 64-bit SWAR (SIMD-Within-A-Register) Parallel Reduction
// 100% universal across all CPU architectures (x86_64, aarch64, riscv64)
// Evaluates 4x 64-bit words (256 slots) in 3 single-cycle ALU bitwise operations
// ============================================================================
fn scan_swar_64(words: &[AtomicU64], start: usize, word_mask: usize) -> Option<usize> {
    let len = words.len();
    if len >= 4 && (len & 3) == 0 {
        let num_quads = len / 4;
        let start_quad = (start / 4) & (num_quads - 1);

        for quad in 0..num_quads {
            let quad_idx = (start_quad + quad) & (num_quads - 1);
            let b = quad_idx * 4;
            let w0 = words[b].load(Ordering::Relaxed);
            let w1 = words[b + 1].load(Ordering::Relaxed);
            let w2 = words[b + 2].load(Ordering::Relaxed);
            let w3 = words[b + 3].load(Ordering::Relaxed);

            // SWAR parallel reduction across 4 general-purpose registers
            let full_01 = w0 & w1;
            let full_23 = w2 & w3;
            let all_full = full_01 & full_23;

            if all_full != !0u64 {
                if full_01 != !0u64 {
                    if w0 != !0u64 {
                        return Some(b);
                    } else {
                        return Some(b + 1);
                    }
                } else if w2 != !0u64 {
                    return Some(b + 2);
                } else {
                    return Some(b + 3);
                }
            }
        }
        None
    } else {
        for i in 0..len {
            let idx = (start + i) & word_mask;
            if words[idx].load(Ordering::Relaxed) != !0u64 {
                return Some(idx);
            }
        }
        None
    }
}

/// Dispatches bitmap scanning directly through tiered vector/SWAR acceleration (AVX2 -> SSE2 -> SWAR).
#[inline]
pub fn scan_bitmap(words: &[AtomicU64], start: usize, word_mask: usize) -> Option<usize> {
    #[cfg(target_arch = "x86_64")]
    {
        if std::is_x86_feature_detected!("avx2") {
            return scan_avx2_256(words, start, word_mask);
        }
        scan_sse2_128(words, start, word_mask)
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        scan_swar_64(words, start, word_mask)
    }
}

impl RdmaSlabArenaInner {
    /// Allocates an available slot index from the atomic bitmask for the requested class.
    ///
    /// Dispatches directly through inlined tiered vector/SWAR acceleration on the hot path.
    fn alloc_slot(&self, class: RdmaSlabClass) -> Result<u32, RdmaError> {
        let (count, words, word_mask, hint_atom, used_atom) = match class {
            RdmaSlabClass::Standard2K => (
                self.config.standard_count,
                &self.standard_masks,
                self.standard_word_mask,
                &self.standard_hint,
                &self.standard_used,
            ),
            RdmaSlabClass::Large64K => (
                self.config.large_count,
                &self.large_masks,
                self.large_word_mask,
                &self.large_hint,
                &self.large_used,
            ),
        };

        let start = hint_atom.load(Ordering::Relaxed) & word_mask;

        // Keep pool management bitmap metadata warm in L2 (_MM_HINT_T1) while payload stays in L3/DRAM.
        velda_core::prefetch_l2_range(
            unsafe { words.as_ptr().add(start) as *const u8 },
            std::mem::size_of_val(words).min(256),
        );

        // Dispatches directly through inlined tiered vector/SWAR acceleration
        let candidate_start = scan_bitmap(words, start, word_mask);

        let search_start = match candidate_start {
            Some(w) => w,
            None => {
                return Err(RdmaError::ArenaExhausted {
                    class: class.as_str(),
                });
            }
        };

        // Atomic CAS allocation loop starting at the candidate word
        for i in 0..words.len() {
            let word_idx = (search_start + i) & word_mask;
            let atomic_word = &words[word_idx];
            let mut current = atomic_word.load(Ordering::Acquire);

            while current != !0u64 {
                let bit = (!current).trailing_zeros();
                if bit >= BITS_PER_ATOMIC_WORD as u32 {
                    break;
                }

                let slot_idx = (word_idx * BITS_PER_ATOMIC_WORD + bit as usize) as u32;
                if slot_idx as usize >= count {
                    // Slot index exceeds valid capacity
                    break;
                }

                let new_val = current | (1u64 << bit);
                match atomic_word.compare_exchange_weak(
                    current,
                    new_val,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => {
                        used_atom.fetch_add(1, Ordering::Relaxed);
                        hint_atom.store(word_idx, Ordering::Relaxed);
                        return Ok(slot_idx);
                    }
                    Err(actual) => current = actual,
                }
            }
        }

        Err(RdmaError::ArenaExhausted {
            class: class.as_str(),
        })
    }

    /// Atomically marks a slot as free in the corresponding bitmask.
    fn free_slot(&self, class: RdmaSlabClass, slot_idx: u32) {
        match class {
            RdmaSlabClass::Standard2K => {
                let word_idx = (slot_idx as usize) / BITS_PER_ATOMIC_WORD;
                let bit = (slot_idx as usize) % BITS_PER_ATOMIC_WORD;
                if word_idx < self.standard_masks.len() {
                    self.standard_masks[word_idx].fetch_and(!(1u64 << bit), Ordering::Release);
                    self.standard_used.fetch_sub(1, Ordering::Relaxed);
                }
            }
            RdmaSlabClass::Large64K => {
                let word_idx = (slot_idx as usize) / BITS_PER_ATOMIC_WORD;
                let bit = (slot_idx as usize) % BITS_PER_ATOMIC_WORD;
                if word_idx < self.large_masks.len() {
                    self.large_masks[word_idx].fetch_and(!(1u64 << bit), Ordering::Release);
                    self.large_used.fetch_sub(1, Ordering::Relaxed);
                }
            }
        }
    }

    /// Returns the underlying Protection Domain associated with this arena, if hardware-backed.
    pub fn pd(&self) -> Option<Arc<ProtectionDomain>> {
        self.mr.as_ref().map(|m| m.pd().clone())
    }
}

/// Pre-registered zero-copy memory slab arena.
///
/// Manages a single contiguous block of physical RAM pinned and registered with the RDMA HCA upfront.
#[derive(Clone)]
pub struct RdmaSlabArena {
    inner: Arc<RdmaSlabArenaInner>,
}

impl Default for RdmaSlabArena {
    fn default() -> Self {
        Self::new_mock(RdmaSlabConfig::default())
            .expect("Failed to initialize default mock RDMA slab arena")
    }
}

impl RdmaSlabArena {
    /// Creates a pure-Rust software mock arena without Verbs syscalls.
    ///
    /// Ideal for pure-Rust unit tests, CI pipelines, and non-RDMA runtime environments.
    pub fn new_mock(config: RdmaSlabConfig) -> Result<Self, RdmaError> {
        Self::create_internal(None, 1, config)
    }

    /// Creates a hardware-backed arena, registering the contiguous memory block with the HCA.
    pub fn new_hardware(
        pd: Arc<ProtectionDomain>,
        config: RdmaSlabConfig,
    ) -> Result<Self, RdmaError> {
        let total_bytes = config.total_bytes();
        let layout = Layout::from_size_align(total_bytes, RDMA_ARENA_PAGE_SIZE).map_err(|e| {
            RdmaError::Library(format!("Invalid memory layout for RDMA arena: {e}"))
        })?;

        // Step 1: Allocate contiguous page-aligned host memory
        let ptr = unsafe { alloc_zeroed(layout) };
        if ptr.is_null() {
            return Err(RdmaError::Library(format!(
                "Failed allocating {total_bytes} bytes for RDMA arena"
            )));
        }

        // Step 2: Eagerly touch every page to establish MMU mappings upfront
        for offset in (0..total_bytes).step_by(RDMA_ARENA_PAGE_SIZE) {
            unsafe {
                ptr.add(offset).write_volatile(0);
            }
        }

        // Step 3: Register the entire contiguous block once with the HCA protection domain.
        // Grant exclusively LOCAL_WRITE. Remote read/write operations are forbidden to
        // eliminate unauthorized cross-connection RDMA tampering of slab buffers.
        let access_flags = IBV_ACCESS_LOCAL_WRITE;
        let mr = match MemoryRegion::register(pd, ptr, total_bytes, access_flags) {
            Ok(region) => region,
            Err(e) => {
                unsafe {
                    dealloc(ptr, layout);
                }
                return Err(e);
            }
        };

        let lkey = mr.lkey();
        Self::create_with_ptr(ptr, layout, Some(mr), lkey, config)
    }

    /// Automatically discovers RDMA hardware.
    ///
    /// In production builds (`cfg(not(test))`), missing hardware immediately fails fast with an error.
    /// In test mode (`cfg(test)`), falls back to mock arena to allow hermetic local testing.
    pub fn new_auto(config: &RdmaConfig, slab_config: RdmaSlabConfig) -> Result<Self, RdmaError> {
        if config.force_mock {
            return Self::new_mock(slab_config);
        }

        match RdmaDevice::list_all() {
            Ok(devs) if !devs.is_empty() => {
                let dev = match &config.device_name {
                    Some(name) => devs.into_iter().find(|d| d.name() == name),
                    None => devs.into_iter().next(),
                };

                if let Some(dev) = dev
                    && let Ok(ctx) = dev.open()
                    && let Ok(pd) = ProtectionDomain::new(ctx)
                    && let Ok(arena) = Self::new_hardware(pd, slab_config)
                {
                    return Ok(arena);
                }

                #[cfg(test)]
                return Self::new_mock(slab_config);

                #[cfg(not(test))]
                Err(RdmaError::DeviceNotFound(
                    "No active RDMA devices found or failed to initialize hardware arena".into(),
                ))
            }
            _ => {
                #[cfg(test)]
                return Self::new_mock(slab_config);

                #[cfg(not(test))]
                Err(RdmaError::DeviceNotFound(
                    "No active RDMA hardware devices detected on host for slab arena".into(),
                ))
            }
        }
    }

    /// Internal constructor allocating host memory and building bitmasks.
    fn create_internal(
        mr: Option<MemoryRegion>,
        lkey: u32,
        config: RdmaSlabConfig,
    ) -> Result<Self, RdmaError> {
        let total_bytes = config.total_bytes();
        let layout = Layout::from_size_align(total_bytes, RDMA_ARENA_PAGE_SIZE).map_err(|e| {
            RdmaError::Library(format!("Invalid memory layout for RDMA arena: {e}"))
        })?;

        let ptr = unsafe { alloc_zeroed(layout) };
        if ptr.is_null() {
            return Err(RdmaError::Library(format!(
                "Failed allocating {total_bytes} bytes for RDMA arena"
            )));
        }

        // Eager physical page commitment
        for offset in (0..total_bytes).step_by(RDMA_ARENA_PAGE_SIZE) {
            unsafe {
                ptr.add(offset).write_volatile(0);
            }
        }

        Self::create_with_ptr(ptr, layout, mr, lkey, config)
    }

    /// Internal helper to assemble the arena structure around an initialized memory pointer.
    fn create_with_ptr(
        ptr: *mut u8,
        layout: Layout,
        mr: Option<MemoryRegion>,
        lkey: u32,
        config: RdmaSlabConfig,
    ) -> Result<Self, RdmaError> {
        let std_words = config.standard_count.div_ceil(BITS_PER_ATOMIC_WORD).max(1);
        let std_capacity = std_words.max(4).next_power_of_two();
        let mut std_vec = Vec::with_capacity(std_capacity);
        for i in 0..std_capacity {
            let mut init_mask = 0u64;
            // Mark out-of-range trailing bits as in-use so they are never allocated
            let start_slot = i * BITS_PER_ATOMIC_WORD;
            for bit in 0..BITS_PER_ATOMIC_WORD {
                if start_slot + bit >= config.standard_count {
                    init_mask |= 1u64 << bit;
                }
            }
            std_vec.push(AtomicU64::new(init_mask));
        }

        let lrg_words = config.large_count.div_ceil(BITS_PER_ATOMIC_WORD).max(1);
        let lrg_capacity = lrg_words.max(4).next_power_of_two();
        let mut lrg_vec = Vec::with_capacity(lrg_capacity);
        for i in 0..lrg_capacity {
            let mut init_mask = 0u64;
            let start_slot = i * BITS_PER_ATOMIC_WORD;
            for bit in 0..BITS_PER_ATOMIC_WORD {
                if start_slot + bit >= config.large_count {
                    init_mask |= 1u64 << bit;
                }
            }
            lrg_vec.push(AtomicU64::new(init_mask));
        }

        let standard_word_mask = std_capacity - 1;
        let large_word_mask = lrg_capacity - 1;

        let inner = RdmaSlabArenaInner {
            base_ptr: ptr,
            layout,
            mr,
            lkey,
            config,
            standard_masks: std_vec.into_boxed_slice(),
            large_masks: lrg_vec.into_boxed_slice(),
            standard_word_mask,
            large_word_mask,
            standard_hint: AtomicUsize::new(0),
            large_hint: AtomicUsize::new(0),
            standard_used: AtomicUsize::new(0),
            large_used: AtomicUsize::new(0),
        };

        Ok(Self {
            inner: Arc::new(inner),
        })
    }

    /// Acquires a zero-copy slot lease for the requested slab class.
    pub fn acquire(&self, class: RdmaSlabClass) -> Result<RdmaSlabLease, RdmaError> {
        let slot_idx = self.inner.alloc_slot(class)?;
        let (offset, len) = match class {
            RdmaSlabClass::Standard2K => (
                (slot_idx as usize) * RdmaSlabClass::STANDARD_2K_SIZE,
                RdmaSlabClass::STANDARD_2K_SIZE,
            ),
            RdmaSlabClass::Large64K => (
                self.inner.config.large_offset()
                    + (slot_idx as usize) * RdmaSlabClass::LARGE_64K_SIZE,
                RdmaSlabClass::LARGE_64K_SIZE,
            ),
        };

        let ptr = unsafe { self.inner.base_ptr.add(offset) };

        // Prefetch into L3 Last-Level Cache (_MM_HINT_T2 / pldl3keep) where PCIe rNIC DMA
        // (Intel DDIO / AMD SDCI) operates, strictly avoiding L1D/L2 cache pollution.
        velda_core::prefetch_l3_range(ptr, 64);

        let addr = ptr as u64;

        Ok(RdmaSlabLease {
            inner: Arc::clone(&self.inner),
            ptr,
            len,
            addr,
            lkey: self.inner.lkey,
            class,
            slot_idx,
        })
    }

    /// Selects the appropriate slab class and acquires a zero-copy slot that fits `size` bytes.
    pub fn acquire_for_size(&self, size: usize) -> Result<RdmaSlabLease, RdmaError> {
        let class = RdmaSlabClass::for_size(size).ok_or(RdmaError::BufferTooSmall {
            capacity: RdmaSlabClass::LARGE_64K_SIZE,
            needed: size,
        })?;
        self.acquire(class)
    }

    /// Returns the underlying Protection Domain associated with this hardware arena, if present.
    #[inline(always)]
    pub fn pd(&self) -> Option<Arc<ProtectionDomain>> {
        self.inner.pd()
    }

    /// Returns the number of available (unallocated) slots for the given class.
    #[inline]
    pub fn available_slots(&self, class: RdmaSlabClass) -> usize {
        match class {
            RdmaSlabClass::Standard2K => {
                let used = self.inner.standard_used.load(Ordering::Relaxed);
                self.inner.config.standard_count.saturating_sub(used)
            }
            RdmaSlabClass::Large64K => {
                let used = self.inner.large_used.load(Ordering::Relaxed);
                self.inner.config.large_count.saturating_sub(used)
            }
        }
    }

    /// Returns the total configured slot capacity for the given class.
    #[inline(always)]
    pub fn total_slots(&self, class: RdmaSlabClass) -> usize {
        match class {
            RdmaSlabClass::Standard2K => self.inner.config.standard_count,
            RdmaSlabClass::Large64K => self.inner.config.large_count,
        }
    }

    /// Hardware local key (`lkey`) authorized to access this memory region.
    #[inline(always)]
    pub fn lkey(&self) -> u32 {
        self.inner.lkey
    }

    /// Virtual memory address of the arena's base buffer.
    #[inline(always)]
    pub fn base_addr(&self) -> u64 {
        self.inner.base_ptr as u64
    }

    /// Total byte capacity of the memory arena.
    #[inline(always)]
    pub fn total_bytes(&self) -> usize {
        self.inner.config.total_bytes()
    }

    /// Returns true if this arena is backed by real RDMA verbs hardware.
    #[inline(always)]
    pub fn is_hardware(&self) -> bool {
        self.inner.mr.is_some()
    }
}

/// RAII guard representing an acquired slot lease from the RDMA slab arena.
///
/// Automatically returns the slot to the arena bitmask on drop without memory deallocation.
pub struct RdmaSlabLease {
    inner: Arc<RdmaSlabArenaInner>,
    /// Pointer to the allocated slot memory within the arena.
    pub ptr: *mut u8,
    /// Byte capacity of this slot.
    pub len: usize,
    /// Virtual address of this slot buffer, referenced directly in RDMA Work Requests.
    pub addr: u64,
    /// Hardware local key (`lkey`) authorized to access this memory region.
    pub lkey: u32,
    /// The slab class (`Standard2K` or `Large64K`) of this lease.
    pub class: RdmaSlabClass,
    /// The slot index within its slab class partition.
    pub slot_idx: u32,
}

unsafe impl Send for RdmaSlabLease {}
unsafe impl Sync for RdmaSlabLease {}

impl Deref for RdmaSlabLease {
    type Target = [u8];

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl DerefMut for RdmaSlabLease {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut_slice()
    }
}

impl Drop for RdmaSlabLease {
    fn drop(&mut self) {
        // Zero memory before releasing slot to prevent cross-tenant memory bleed (CWE-226).
        // Uses Non-Temporal Streaming Zero (`vmovntdq` / `movntdq` / `movnti`) to flush zeros
        // directly through Write-Combining (WC) buffers to L3/RAM without triggering
        // Read-For-Ownership (RFO) or evicting the hot `velda-router` table from L1D/L2.
        velda_core::zero_non_temporal(self.as_mut_slice());
        self.inner.free_slot(self.class, self.slot_idx);
    }
}

impl RdmaSlabLease {
    /// Returns the immutable byte slice of the leased slot.
    #[inline(always)]
    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }

    /// Returns the mutable byte slice of the leased slot.
    #[inline(always)]
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) }
    }

    /// Copies `src` into the beginning of this RDMA slot using Register-Adaptive Non-Temporal
    /// streaming stores (`COPY_NON_TEMPORAL_FN`), bypassing L1D/L2 cache allocation and eliminating
    /// PCIe snoop-invalidation stalls when the HCA reads the buffer over DMA.
    #[inline(always)]
    pub fn copy_from_non_temporal(&mut self, src: &[u8]) {
        self.write_non_temporal(0, src);
    }

    /// Writes `src` at `offset` into this RDMA slot using Register-Adaptive Non-Temporal streaming stores.
    #[inline]
    pub fn write_non_temporal(&mut self, offset: usize, src: &[u8]) {
        if offset >= self.len {
            return;
        }
        let to_copy = src.len().min(self.len - offset);
        if to_copy == 0 {
            return;
        }
        let dst = &mut self.as_mut_slice()[offset..offset + to_copy];
        velda_core::copy_non_temporal(&src[..to_copy], dst);
    }

    /// Prefetches `[offset .. offset + len)` of this RDMA slot into L3 Cache (`_MM_HINT_T2` / `pldl3keep`),
    /// matching Intel DDIO / AMD SDCI PCIe DMA residency without touching Core L1D/L2 caches.
    #[inline(always)]
    pub fn prefetch_l3(&self, offset: usize, len: usize) {
        if offset >= self.len {
            return;
        }
        let to_prefetch = len.min(self.len - offset);
        if to_prefetch > 0 {
            velda_core::prefetch_l3_range(unsafe { self.ptr.add(offset) }, to_prefetch);
        }
    }

    /// Prefetches `[offset .. offset + len)` of this RDMA slot with Non-Temporal hint (`_MM_HINT_NTA` / `pldl1strm`).
    #[inline(always)]
    pub fn prefetch_nta(&self, offset: usize, len: usize) {
        if offset >= self.len {
            return;
        }
        let to_prefetch = len.min(self.len - offset);
        if to_prefetch > 0 {
            velda_core::prefetch_nta_range(unsafe { self.ptr.add(offset) }, to_prefetch);
        }
    }

    /// Virtual address of this slot buffer, referenced directly in RDMA Work Requests.
    #[inline(always)]
    pub fn addr(&self) -> u64 {
        self.addr
    }

    /// Hardware local key (`lkey`) authorized to access this memory region.
    #[inline(always)]
    pub fn lkey(&self) -> u32 {
        self.lkey
    }

    /// The slab class (`Standard2K` or `Large64K`) of this lease.
    #[inline(always)]
    pub fn slot_class(&self) -> RdmaSlabClass {
        self.class
    }

    /// The slot index within its slab class partition.
    #[inline(always)]
    pub fn slot_index(&self) -> u32 {
        self.slot_idx
    }

    /// Total byte capacity of this slot.
    #[inline(always)]
    pub fn capacity(&self) -> usize {
        self.len
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_slab_class_selection() {
        assert_eq!(
            RdmaSlabClass::for_size(1500),
            Some(RdmaSlabClass::Standard2K)
        );
        assert_eq!(
            RdmaSlabClass::for_size(2048),
            Some(RdmaSlabClass::Standard2K)
        );
        assert_eq!(RdmaSlabClass::for_size(2049), Some(RdmaSlabClass::Large64K));
        assert_eq!(
            RdmaSlabClass::for_size(65536),
            Some(RdmaSlabClass::Large64K)
        );
        assert_eq!(RdmaSlabClass::for_size(65537), None);
    }

    #[test]
    fn test_arena_allocation_and_deref() {
        let arena = RdmaSlabArena::new_mock(RdmaSlabConfig {
            standard_count: 64,
            large_count: 4,
        })
        .expect("Arena creation should succeed");

        let mut lease = arena
            .acquire(RdmaSlabClass::Standard2K)
            .expect("Should acquire standard slot");
        assert_eq!(lease.len(), 2048);
        assert_eq!(lease.slot_class(), RdmaSlabClass::Standard2K);
        assert_eq!(lease.slot_index(), 0);
        assert_ne!(lease.addr(), 0);
        assert_eq!(lease.lkey(), 1);

        // Write and read via Deref/DerefMut
        lease[0] = 0xAA;
        lease[2047] = 0xBB;
        assert_eq!(lease.as_slice()[0], 0xAA);
        assert_eq!(lease.as_slice()[2047], 0xBB);
    }

    #[test]
    fn test_arena_drop_and_recycle() {
        let arena = RdmaSlabArena::new_mock(RdmaSlabConfig {
            standard_count: 2,
            large_count: 1,
        })
        .expect("Arena creation should succeed");

        assert_eq!(arena.available_slots(RdmaSlabClass::Standard2K), 2);

        let lease1 = arena
            .acquire(RdmaSlabClass::Standard2K)
            .expect("Acquire slot 0");
        assert_eq!(lease1.slot_index(), 0);
        assert_eq!(arena.available_slots(RdmaSlabClass::Standard2K), 1);

        let lease2 = arena
            .acquire(RdmaSlabClass::Standard2K)
            .expect("Acquire slot 1");
        assert_eq!(lease2.slot_index(), 1);
        assert_eq!(arena.available_slots(RdmaSlabClass::Standard2K), 0);

        // Third acquisition should fail
        let err = arena.acquire(RdmaSlabClass::Standard2K);
        assert!(matches!(err, Err(RdmaError::ArenaExhausted { .. })));

        // Drop lease1: slot 0 should be recycled immediately
        drop(lease1);
        assert_eq!(arena.available_slots(RdmaSlabClass::Standard2K), 1);

        let lease3 = arena
            .acquire(RdmaSlabClass::Standard2K)
            .expect("Acquire recycled slot");
        assert_eq!(lease3.slot_index(), 0);

        drop(lease2);
        drop(lease3);
        assert_eq!(arena.available_slots(RdmaSlabClass::Standard2K), 2);
    }

    #[test]
    fn test_arena_large_slot_contiguous_span() {
        let arena = RdmaSlabArena::new_mock(RdmaSlabConfig {
            standard_count: 64,
            large_count: 2,
        })
        .expect("Arena creation should succeed");

        let mut large_lease = arena
            .acquire(RdmaSlabClass::Large64K)
            .expect("Acquire large slot");
        assert_eq!(large_lease.len(), 65536);
        assert_eq!(large_lease.capacity(), 65536);
        assert_eq!(large_lease.slot_class(), RdmaSlabClass::Large64K);

        // Verify contiguous span read/write
        large_lease.as_mut_slice()[0] = 0x11;
        large_lease.as_mut_slice()[65535] = 0x99;
        assert_eq!(large_lease[0], 0x11);
        assert_eq!(large_lease[65535], 0x99);
    }

    #[test]
    fn test_arena_acquire_for_size() {
        let arena = RdmaSlabArena::default();

        let l1 = arena
            .acquire_for_size(1400)
            .expect("Should fit in Standard2K");
        assert_eq!(l1.slot_class(), RdmaSlabClass::Standard2K);

        let l2 = arena
            .acquire_for_size(32768)
            .expect("Should fit in Large64K");
        assert_eq!(l2.slot_class(), RdmaSlabClass::Large64K);

        let l3 = arena.acquire_for_size(70000);
        assert!(matches!(l3, Err(RdmaError::BufferTooSmall { .. })));
    }

    #[test]
    fn test_arena_multithreaded_concurrency() {
        let arena = Arc::new(
            RdmaSlabArena::new_mock(RdmaSlabConfig {
                standard_count: 128,
                large_count: 16,
            })
            .expect("Arena creation should succeed"),
        );

        let mut handles = Vec::new();
        for _ in 0..8 {
            let arena_clone = Arc::clone(&arena);
            handles.push(std::thread::spawn(move || {
                for _ in 0..1000 {
                    let mut lease = arena_clone
                        .acquire(RdmaSlabClass::Standard2K)
                        .expect("Acquire should succeed under load");
                    lease[0] = 0x55;
                    assert_eq!(lease[0], 0x55);
                    // Automatic drop recycles slot
                }
            }));
        }

        for h in handles {
            h.join().expect("Worker thread panicked");
        }

        assert_eq!(arena.available_slots(RdmaSlabClass::Standard2K), 128);
    }

    #[test]
    fn test_avx2_bitmap_scan_and_high_occupancy_saturation() {
        let arena = RdmaSlabArena::new_mock(RdmaSlabConfig {
            standard_count: 512,
            large_count: 64,
        })
        .expect("Arena creation should succeed");

        // 1. Acquire first 256 slots (fills exactly 4x64-bit words, i.e. 1 full AVX2 256-bit chunk)
        let mut leases = Vec::with_capacity(512);
        for i in 0..256 {
            let lease = arena
                .acquire(RdmaSlabClass::Standard2K)
                .expect("Acquiring initial 256 slots");
            assert_eq!(lease.slot_index(), i as u32);
            leases.push(lease);
        }

        // 2. The next slot MUST come from chunk 1 (slot index 256)
        let lease_256 = arena
            .acquire(RdmaSlabClass::Standard2K)
            .expect("Acquiring 257th slot across 256-bit boundary");
        assert_eq!(lease_256.slot_index(), 256);
        leases.push(lease_256);

        // 3. Acquire remaining slots up to 512
        for i in 257..512 {
            let lease = arena
                .acquire(RdmaSlabClass::Standard2K)
                .expect("Acquiring remaining slots");
            assert_eq!(lease.slot_index(), i as u32);
            leases.push(lease);
        }

        // 4. Verify fail-fast exhaustion
        let err = arena.acquire(RdmaSlabClass::Standard2K);
        assert!(matches!(err, Err(RdmaError::ArenaExhausted { .. })));

        // 5. Drop slot 100 in chunk 0; verify AVX2 vector scan wraps and re-leases slot 100
        leases.remove(100);
        let recycled = arena
            .acquire(RdmaSlabClass::Standard2K)
            .expect("Acquiring recycled slot in chunk 0");
        assert_eq!(recycled.slot_index(), 100);
    }

    #[test]
    fn test_parallel_scan_universes_avx2_sse2_swar_equivalence() {
        use std::sync::atomic::AtomicU64;

        // 8 words = 512 slots
        let words = [
            AtomicU64::new(!0u64),         // word 0: full
            AtomicU64::new(!0u64),         // word 1: full
            AtomicU64::new(!0u64),         // word 2: full
            AtomicU64::new(!0u64),         // word 3: full (slots 0..255 all full)
            AtomicU64::new(!(1u64 << 12)), // word 4: has bit 12 free!
            AtomicU64::new(!0u64),         // word 5: full
            AtomicU64::new(!0u64),         // word 6: full
            AtomicU64::new(!0u64),         // word 7: full
        ];

        // 1. SWAR (pure 64-bit GPR)
        let res_swar = scan_swar_64(&words, 0, 7);
        assert_eq!(res_swar, Some(4), "SWAR must locate word 4");

        // 2. SSE2 (128-bit XMM)
        #[cfg(target_arch = "x86_64")]
        {
            let res_sse2 = scan_sse2_128(&words, 0, 7);
            assert_eq!(res_sse2, Some(4), "SSE2 must locate word 4");
        }

        // 3. AVX2 (256-bit YMM)
        #[cfg(target_arch = "x86_64")]
        {
            if std::is_x86_feature_detected!("avx2") {
                let res_avx2 = scan_avx2_256(&words, 0, 7);
                assert_eq!(res_avx2, Some(4), "AVX2 must locate word 4");
            }
        }

        // Fill word 4 so all 512 slots are full
        words[4].store(!0u64, Ordering::Relaxed);

        assert_eq!(scan_swar_64(&words, 0, 7), None);
        #[cfg(target_arch = "x86_64")]
        {
            assert_eq!(scan_sse2_128(&words, 0, 7), None);
            if std::is_x86_feature_detected!("avx2") {
                assert_eq!(scan_avx2_256(&words, 0, 7), None);
            }
        }
    }
}
