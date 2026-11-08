//! # Zero-Copy UMEM Slab Arena & Header Workspace
//!
//! ## Memory Architecture & Hardware Alignment
//!
//! Hot-path ingress packet ingestion bypasses the kernel network stack by delivering wire
//! packets directly into pre-allocated user-space memory (UMEM) slabs.
//!
//! ### Frame Sizing & MMU Page Boundaries
//! Sizing each frame to exactly 4096 bytes aligns memory allocations with standard x86_64 and ARM64
//! MMU page boundaries. Frames spanning page boundaries force NIC DMA engines and IOMMU translation
//! tables into multi-page scatter-gather transactions that saturate PCIe bus bandwidth. A 1:1
//! frame-to-page mapping guarantees contiguous DMA access.
//!
//! ### Functional Frame Partitioning: [256B Headroom | 2048B Payload | 1792B Workspace]
//! ```text
//! ┌──────────────────────────────────────────────────────────────────────────┐
//! │                       1 XdpSlab Frame (4096 Bytes)                       │
//! ├──────────────────────┬───────────────────────────┬───────────────────────┤
//! │   XDP Headroom       │     Packet DMA Payload    │   Header Workspace    │
//! │    (256 Bytes)       │       (2048 Bytes)        │     (1792 Bytes)      │
//! │ Reserved for TX      │ NIC DMA controller writes │ L7 parser scratchpad: │
//! │ prepend and tunnel   │ incoming wire packet here │ stores header tokens  │
//! │ headers (no memmove) │ (fits Ethernet MTU)       │ with zero heap alloc  │
//! └──────────────────────┴───────────────────────────┴───────────────────────┘
//! ```
//! - **256B Headroom**: Supports packet reflection (`XDP_TX`) and tunnel encapsulation (VLAN, Geneve, GRE)
//!   by shifting pointers backward and prepending headers in-place, eliminating buffer reallocations and `memmove`.
//! - **2048B Payload**: Accommodates standard Ethernet MTU frames (1500B) and encapsulated payloads up to 2KB.
//! - **1792B Header Workspace**: Exploits CPU data cache locality. Because the physical 4KB page is already
//!   cached in L1/L2 lines following payload inspection, protocol parsers (HTTP/1, HTTP/2, gRPC) write header
//!   slices, token offsets, and status codes directly into this scratchpad without heap allocations (`malloc`).
//!
//! ### Lock-Free Bitmask Allocation & Deliberate Hardware Cache Partitioning
//! Frontline traffic workers execute on dedicated CPU cores and avoid mutex synchronization to eliminate
//! OS `futex` sleep delays (1-2µs latency penalty). Slot availability is tracked via `AtomicU64` bitmask words.
//! Hardware bit-scan instructions (`trailing_zeros` / `TZCNT`, executing in a single CPU cycle) locate and
//! reserve slots in nanoseconds without thread contention.
//!
//! To prevent high-rate ingress packets from evicting `velda-router` lookup tables out of Core L1D (`_MM_HINT_T0`),
//! `XdpSlabArena` enforces strict hardware cache tiering across the 4KB frame:
//! - **Pool Bitmask Metadata (`masks`) & 1792B `header_workspace` -> L2 Cache (`_MM_HINT_T1` / `pldl2keep`)**:
//!   Frequently mutated pool bitmasks and L7 header parser scratchpads reside in L2 (~3-4 ns access).
//! - **2048B Packet DMA `payload` -> L3 LLC (`_MM_HINT_T2` / `pldl3keep`) & Non-Temporal WC (`NTA`)**:
//!   NIC PCIe DMA (Intel DDIO / AMD SDCI) injects incoming wire packets directly into L3 LLC / DRAM.
//!   Keeping payload buffers in L3/DRAM avoids L1D/L2 cache thrashing and PCIe snoop stalls.

use std::alloc::{Layout, alloc_zeroed, dealloc};
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use crate::config::XdpConfig;
use crate::error::{Result, XdpError};

const BITS_PER_ATOMIC_WORD: usize = 64;

/// Internal shared state of the UMEM slab arena.
#[derive(Debug)]
struct XdpSlabArenaInner {
    base_ptr: *mut u8,
    layout: Layout,
    frame_size: usize,
    frame_count: u32,
    headroom: usize,
    header_workspace: usize,
    masks: Box<[AtomicU64]>,
    hint: AtomicUsize,
    used: AtomicUsize,
}

unsafe impl Send for XdpSlabArenaInner {}
unsafe impl Sync for XdpSlabArenaInner {}

impl Drop for XdpSlabArenaInner {
    fn drop(&mut self) {
        if !self.base_ptr.is_null() {
            unsafe {
                dealloc(self.base_ptr, self.layout);
            }
        }
    }
}

/// Pre-allocated zero-copy UMEM Slab Memory Arena registered with the NIC for direct DMA.
#[derive(Clone, Debug)]
pub struct XdpSlabArena {
    inner: Arc<XdpSlabArenaInner>,
}

// ============================================================================
// 4-Tier Hardware Bitmask Scanner for UMEM Arena
// ============================================================================

/// Function pointer type for hardware-accelerated bitmask word scanning.
pub type ScanFn = fn(&[AtomicU64], usize) -> Option<usize>;

// ----------------------------------------------------------------------------
// Tier 1: 256-bit AVX2 Vector Scanner (4x 64-bit words = 256 UMEM frames)
// ----------------------------------------------------------------------------
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn scan_avx2_256_inner(words: &[AtomicU64], start: usize) -> Option<usize> {
    use core::arch::x86_64::*;
    let len = words.len();
    let num_quads = len / 4;
    let start_quad = (start / 4) % num_quads;
    let all_ones = _mm256_set1_epi64x(-1);

    for quad in 0..num_quads {
        let quad_idx = (start_quad + quad) % num_quads;
        let base_word = quad_idx * 4;
        unsafe {
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
    ((num_quads * 4)..len).find(|&i| words[i].load(Ordering::Relaxed) != !0u64)
}

/// Tier 1 (256-bit AVX2): Evaluates 4 `u64` words (256 UMEM frames) in one register.
#[cfg(target_arch = "x86_64")]
pub fn scan_avx2_256(words: &[AtomicU64], start: usize) -> Option<usize> {
    if words.len() >= 4 {
        unsafe { scan_avx2_256_inner(words, start) }
    } else {
        scan_swar_64(words, start)
    }
}

// ----------------------------------------------------------------------------
// Tier 2: 128-bit SSE2 Vector Scanner (2x 64-bit words = 128 UMEM frames)
// ----------------------------------------------------------------------------
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse2")]
unsafe fn scan_sse2_128_inner(words: &[AtomicU64], start: usize) -> Option<usize> {
    use core::arch::x86_64::*;
    let len = words.len();
    let num_pairs = len / 2;
    let start_pair = (start / 2) % num_pairs;
    let all_ones = _mm_set1_epi32(-1);

    for pair in 0..num_pairs {
        let pair_idx = (start_pair + pair) % num_pairs;
        let base_word = pair_idx * 2;
        unsafe {
            let ptr = words.as_ptr().add(base_word) as *const __m128i;
            let xmm = _mm_loadu_si128(ptr);
            let cmp = _mm_cmpeq_epi32(xmm, all_ones);
            let mask = _mm_movemask_ps(_mm_castsi128_ps(cmp));
            if mask != 0x0F {
                if (mask & 0x03) != 0x03 {
                    return Some(base_word);
                } else {
                    return Some(base_word + 1);
                }
            }
        }
    }
    ((num_pairs * 2)..len).find(|&i| words[i].load(Ordering::Relaxed) != !0u64)
}

/// Tier 2 (128-bit SSE2): Evaluates 2 `u64` words (128 UMEM frames) in one register.
#[cfg(target_arch = "x86_64")]
pub fn scan_sse2_128(words: &[AtomicU64], start: usize) -> Option<usize> {
    if words.len() >= 2 {
        unsafe { scan_sse2_128_inner(words, start) }
    } else {
        scan_swar_64(words, start)
    }
}

// ----------------------------------------------------------------------------
// Tier 3: 64-bit Dual-SWAR Parallel Reduction Scanner
// ----------------------------------------------------------------------------
/// Tier 3 (64-bit Dual-SWAR): Evaluates 4x 64-bit words in parallel ALU bitwise reduction.
pub fn scan_swar_64(words: &[AtomicU64], start: usize) -> Option<usize> {
    let len = words.len();
    if len >= 4 {
        let num_quads = len / 4;
        let start_quad = (start / 4) % num_quads;

        for quad in 0..num_quads {
            let quad_idx = (start_quad + quad) % num_quads;
            let b = quad_idx * 4;
            let w0 = words[b].load(Ordering::Relaxed);
            let w1 = words[b + 1].load(Ordering::Relaxed);
            let w2 = words[b + 2].load(Ordering::Relaxed);
            let w3 = words[b + 3].load(Ordering::Relaxed);

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
        ((num_quads * 4)..len).find(|&i| words[i].load(Ordering::Relaxed) != !0u64)
    } else {
        for i in 0..len {
            let idx = (start + i) % len;
            if words[idx].load(Ordering::Relaxed) != !0u64 {
                return Some(idx);
            }
        }
        None
    }
}

// ----------------------------------------------------------------------------
// Tier 4: 32-bit Quad-SWAR Sub-Word Scanner
// ----------------------------------------------------------------------------
/// Tier 4 (32-bit Quad-SWAR): Evaluates 64-bit atomic words via 32-bit sub-word halves.
pub fn scan_swar_32(words: &[AtomicU64], start: usize) -> Option<usize> {
    let len = words.len();
    for i in 0..len {
        let idx = (start + i) % len;
        let w = words[idx].load(Ordering::Relaxed);
        let lo = w as u32;
        let hi = (w >> 32) as u32;
        if (lo & hi) != !0u32 {
            return Some(idx);
        }
    }
    None
}

/// Pre-resolved static function pointer for 4-tier bitmask scanning.
///
/// Bound once at bootstrap via `velda_core::global_hardware_topology().cpu.register_capabilities`.
pub static SCAN_AVAILABLE_WORD_FN: std::sync::LazyLock<ScanFn> = std::sync::LazyLock::new(|| {
    use velda_core::hardware::RegisterWidth;
    let caps = velda_core::global_hardware_topology()
        .cpu
        .register_capabilities;
    match caps.select_for_workload(RegisterWidth::Bits256) {
        RegisterWidth::Bits256 => {
            #[cfg(target_arch = "x86_64")]
            {
                if std::is_x86_feature_detected!("avx2") {
                    return scan_avx2_256;
                }
                scan_sse2_128
            }
            #[cfg(not(target_arch = "x86_64"))]
            {
                scan_swar_64
            }
        }
        RegisterWidth::Bits128 => {
            #[cfg(target_arch = "x86_64")]
            {
                scan_sse2_128
            }
            #[cfg(not(target_arch = "x86_64"))]
            {
                scan_swar_64
            }
        }
        RegisterWidth::Bits64 => scan_swar_64,
        RegisterWidth::Bits32 => scan_swar_32,
    }
});

/// Dispatches bitmask word scanning through the hardware-resolved function pointer.
#[inline]
pub fn scan_available_word(words: &[AtomicU64], start: usize) -> Option<usize> {
    (*SCAN_AVAILABLE_WORD_FN)(words, start)
}

impl XdpSlabArena {
    /// Allocates and page-aligns a contiguous host memory block for UMEM registration.
    pub fn new(config: &XdpConfig) -> Result<Self> {
        let frame_size = config.frame_size;
        let frame_count = config.frame_count;
        let total_bytes = frame_size * (frame_count as usize);

        // Enforce power-of-two sizing so ring buffer calculations use bitwise masking
        // (`index & mask`) rather than integer modulo division on the hot path.
        if total_bytes == 0 || !total_bytes.is_power_of_two() {
            return Err(XdpError::UmemAllocation(format!(
                "Total UMEM size ({total_bytes} bytes) must be a non-zero power of two for bitwise ring masking"
            )));
        }

        // Align allocation to 4096-byte page boundary for kernel MMU and IOMMU translation
        let layout = Layout::from_size_align(total_bytes, 4096)
            .map_err(|e| XdpError::UmemAllocation(e.to_string()))?;

        let base_ptr = unsafe { alloc_zeroed(layout) };
        if base_ptr.is_null() {
            return Err(XdpError::UmemAllocation(format!(
                "Failed to allocate {total_bytes} bytes of contiguous page-aligned memory for UMEM"
            )));
        }

        let num_words = (frame_count as usize).div_ceil(BITS_PER_ATOMIC_WORD);
        let mut masks = Vec::with_capacity(num_words);
        for _ in 0..num_words {
            masks.push(AtomicU64::new(0));
        }

        Ok(Self {
            inner: Arc::new(XdpSlabArenaInner {
                base_ptr,
                layout,
                frame_size,
                frame_count,
                headroom: config.headroom,
                header_workspace: config.header_workspace,
                masks: masks.into_boxed_slice(),
                hint: AtomicUsize::new(0),
                used: AtomicUsize::new(0),
            }),
        })
    }

    /// Leases an available frame from the arena using lock-free bitmask allocation.
    ///
    /// The search begins at `hint` (the last allocated word index) to distribute
    /// allocation scans and prevent cache contention on word 0.
    pub fn lease(&self) -> Result<XdpSlabLease> {
        let words = &self.inner.masks;
        let num_words = words.len();
        if num_words == 0 {
            return Err(XdpError::RingFull {
                ring_name: "UMEM Arena",
                capacity: self.inner.frame_count,
            });
        }

        let mut scan_start = self.inner.hint.load(Ordering::Relaxed) % num_words;
        // Keep UMEM pool management bitmask metadata warm in L2 (_MM_HINT_T1).
        velda_core::prefetch_l2_range(
            unsafe { words.as_ptr().add(scan_start) as *const u8 },
            std::mem::size_of_val(words).min(256),
        );

        let mut attempts = 0;
        while attempts < num_words {
            let word_idx = match scan_available_word(words, scan_start) {
                Some(idx) => idx,
                None => break,
            };

            let atomic_word = &words[word_idx];
            let mut current = atomic_word.load(Ordering::Acquire);

            while current != !0u64 {
                // Hardware TZCNT finds the first free bit index in a single CPU cycle
                let bit = (!current).trailing_zeros();
                if bit >= BITS_PER_ATOMIC_WORD as u32 {
                    break;
                }

                let frame_idx = (word_idx * BITS_PER_ATOMIC_WORD + bit as usize) as u32;
                if frame_idx >= self.inner.frame_count {
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
                        self.inner.used.fetch_add(1, Ordering::Relaxed);
                        self.inner.hint.store(word_idx, Ordering::Relaxed);
                        if self.inner.header_workspace > 0 {
                            let ws_offset = (frame_idx as usize) * self.inner.frame_size
                                + self.inner.frame_size
                                - self.inner.header_workspace;
                            velda_core::prefetch_l2_range(
                                unsafe { self.inner.base_ptr.add(ws_offset) },
                                self.inner.header_workspace,
                            );
                        }
                        return Ok(XdpSlabLease::new(self.inner.clone(), frame_idx, 0));
                    }
                    Err(actual) => current = actual,
                }
            }

            scan_start = (word_idx + 1) % num_words;
            attempts += 1;
        }

        Err(XdpError::RingFull {
            ring_name: "UMEM Arena",
            capacity: self.inner.frame_count,
        })
    }

    /// Leases a specific frame by index when the AF_XDP Rx ring reports packet arrival.
    pub fn lease_by_index(&self, frame_idx: u32, payload_len: usize) -> Result<XdpSlabLease> {
        if frame_idx >= self.inner.frame_count {
            return Err(XdpError::UmemAllocation(format!(
                "Frame index {frame_idx} exceeds UMEM arena capacity ({})",
                self.inner.frame_count
            )));
        }

        let word_idx = (frame_idx as usize) / BITS_PER_ATOMIC_WORD;
        let bit = (frame_idx as usize) % BITS_PER_ATOMIC_WORD;
        let atomic_word = &self.inner.masks[word_idx];

        let mut current = atomic_word.load(Ordering::Acquire);
        loop {
            if current & (1u64 << bit) != 0 {
                // Already marked as leased
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
                    self.inner.used.fetch_add(1, Ordering::Relaxed);
                    break;
                }
                Err(actual) => current = actual,
            }
        }

        // Hardware Cache Partitioning on RX Packet Arrival:
        // 1. DMA payload stays in L3 LLC (_MM_HINT_T2 / Intel DDIO) — never pulled into L1D/L2.
        // 2. 1792B Header Workspace is prefetched into L2 (_MM_HINT_T1) for L7 header parsing.
        let frame_base = (frame_idx as usize) * self.inner.frame_size;
        if payload_len > 0 {
            velda_core::prefetch_l3_range(
                unsafe { self.inner.base_ptr.add(frame_base + self.inner.headroom) },
                payload_len,
            );
        }
        if self.inner.header_workspace > 0 {
            velda_core::prefetch_l2_range(
                unsafe {
                    self.inner
                        .base_ptr
                        .add(frame_base + self.inner.frame_size - self.inner.header_workspace)
                },
                self.inner.header_workspace,
            );
        }

        Ok(XdpSlabLease::new(
            self.inner.clone(),
            frame_idx,
            payload_len,
        ))
    }

    /// Collects base UMEM offsets for all frames to initialize the Fill Ring during startup.
    pub fn fill_all_addresses(&self) -> Vec<u64> {
        let frame_size = self.inner.frame_size as u64;
        (0..self.inner.frame_count as u64)
            .map(|idx| idx * frame_size)
            .collect()
    }

    /// Raw base pointer to the contiguous UMEM memory block in user-space.
    #[inline]
    pub fn base_ptr(&self) -> *mut u8 {
        self.inner.base_ptr
    }

    /// Total memory capacity in bytes.
    #[inline]
    pub fn total_bytes(&self) -> usize {
        self.inner.frame_size * (self.inner.frame_count as usize)
    }

    /// Slot frame size in bytes (4096 bytes).
    #[inline]
    pub fn frame_size(&self) -> usize {
        self.inner.frame_size
    }

    /// Total slot count in the arena.
    #[inline]
    pub fn frame_count(&self) -> u32 {
        self.inner.frame_count
    }

    /// Number of frames currently leased by active requests.
    #[inline]
    pub fn leased_count(&self) -> usize {
        self.inner.used.load(Ordering::Relaxed)
    }

    /// Number of available frames ready to receive traffic.
    #[inline]
    pub fn available_count(&self) -> usize {
        (self.inner.frame_count as usize).saturating_sub(self.leased_count())
    }
}

/// RAII Guard managing the lifecycle of an allocated 4KB UMEM slab frame.
///
/// Upon drop, the slot index is returned to the bitmask via atomic `fetch_and`,
/// recycling the slot for Fill Ring replenishment without OS memory deallocation.
pub struct XdpSlabLease {
    inner: Arc<XdpSlabArenaInner>,
    frame_idx: u32,
    payload_len: usize,
}

impl XdpSlabLease {
    #[inline]
    fn new(inner: Arc<XdpSlabArenaInner>, frame_idx: u32, payload_len: usize) -> Self {
        Self {
            inner,
            frame_idx,
            payload_len,
        }
    }

    /// Zero-based index of this frame within the arena.
    #[inline]
    pub fn frame_idx(&self) -> u32 {
        self.frame_idx
    }

    /// Absolute byte offset from the start of UMEM: `frame_idx * frame_size`.
    #[inline]
    pub fn umem_offset(&self) -> u64 {
        (self.frame_idx as u64) * (self.inner.frame_size as u64)
    }

    /// Physical UMEM address of the payload section: `umem_offset + headroom`.
    #[inline]
    pub fn payload_umem_addr(&self) -> u64 {
        self.umem_offset() + (self.inner.headroom as u64)
    }

    /// Active payload byte length.
    #[inline]
    pub fn payload_len(&self) -> usize {
        self.payload_len
    }

    /// Sets the active payload length following header parsing or NIC reception.
    #[inline]
    pub fn set_payload_len(&mut self, len: usize) {
        let max_payload = self
            .inner
            .frame_size
            .saturating_sub(self.inner.headroom + self.inner.header_workspace);
        self.payload_len = len.min(max_payload);
    }

    /// Accesses the reserved Headroom segment for protocol header prepending.
    #[inline]
    pub fn headroom(&self) -> &[u8] {
        let offset = self.umem_offset() as usize;
        unsafe {
            let ptr = self.inner.base_ptr.add(offset);
            std::slice::from_raw_parts(ptr, self.inner.headroom)
        }
    }

    /// Mutably accesses the Headroom segment.
    #[inline]
    pub fn headroom_mut(&mut self) -> &mut [u8] {
        let offset = self.umem_offset() as usize;
        unsafe {
            let ptr = self.inner.base_ptr.add(offset);
            std::slice::from_raw_parts_mut(ptr, self.inner.headroom)
        }
    }

    /// Accesses the packet payload written by the NIC DMA controller.
    #[inline]
    pub fn payload(&self) -> &[u8] {
        let offset = (self.umem_offset() as usize) + self.inner.headroom;
        unsafe {
            let ptr = self.inner.base_ptr.add(offset);
            std::slice::from_raw_parts(ptr, self.payload_len)
        }
    }

    /// Mutably accesses the packet payload for in-place decryption and parsing.
    #[inline]
    pub fn payload_mut(&mut self) -> &mut [u8] {
        let offset = (self.umem_offset() as usize) + self.inner.headroom;
        unsafe {
            let ptr = self.inner.base_ptr.add(offset);
            std::slice::from_raw_parts_mut(ptr, self.payload_len)
        }
    }

    /// Accesses the Header Workspace segment at the tail of the 4KB page.
    #[inline]
    pub fn header_workspace(&self) -> &[u8] {
        let offset =
            (self.umem_offset() as usize) + self.inner.frame_size - self.inner.header_workspace;
        unsafe {
            let ptr = self.inner.base_ptr.add(offset);
            std::slice::from_raw_parts(ptr, self.inner.header_workspace)
        }
    }

    /// Mutably accesses the Header Workspace segment for in-place token storage.
    #[inline]
    pub fn header_workspace_mut(&mut self) -> &mut [u8] {
        let offset =
            (self.umem_offset() as usize) + self.inner.frame_size - self.inner.header_workspace;
        unsafe {
            let ptr = self.inner.base_ptr.add(offset);
            std::slice::from_raw_parts_mut(ptr, self.inner.header_workspace)
        }
    }

    /// Copies `src` into the UMEM DMA payload region using Register-Adaptive Non-Temporal streaming stores
    /// (`COPY_NON_TEMPORAL_FN`), bypassing L1D/L2 cache allocation and avoiding PCIe TX DMA snoop stalls.
    #[inline]
    pub fn copy_payload_non_temporal(&mut self, src: &[u8]) {
        self.set_payload_len(src.len());
        let len = self.payload_len;
        if len == 0 {
            return;
        }
        let dst = self.payload_mut();
        (*velda_core::COPY_NON_TEMPORAL_FN)(&src[..len], dst);
    }
}

impl Deref for XdpSlabLease {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.payload()
    }
}

impl DerefMut for XdpSlabLease {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.payload_mut()
    }
}

impl Drop for XdpSlabLease {
    fn drop(&mut self) {
        let word_idx = (self.frame_idx as usize) / BITS_PER_ATOMIC_WORD;
        let bit = (self.frame_idx as usize) % BITS_PER_ATOMIC_WORD;
        let atomic_word = &self.inner.masks[word_idx];

        atomic_word.fetch_and(!(1u64 << bit), Ordering::Release);
        self.inner.used.fetch_sub(1, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{DEFAULT_XDP_HEADER_WORKSPACE, DEFAULT_XDP_HEADROOM};

    #[test]
    fn test_slab_arena_lifecycle_and_offsets() {
        let config = XdpConfig::new("lo", 0).with_frame_count(64);
        let arena = XdpSlabArena::new(&config).expect("Arena creation should succeed");

        assert_eq!(arena.frame_count(), 64);
        assert_eq!(arena.available_count(), 64);
        assert_eq!(arena.leased_count(), 0);

        {
            let mut lease = arena.lease().expect("Lease should succeed");
            assert_eq!(lease.frame_idx(), 0);
            assert_eq!(lease.umem_offset(), 0);
            assert_eq!(lease.payload_umem_addr(), DEFAULT_XDP_HEADROOM as u64);
            assert_eq!(arena.leased_count(), 1);
            assert_eq!(arena.available_count(), 63);

            // Write into payload
            lease.set_payload_len(100);
            lease[0] = 0xAA;
            lease[99] = 0xBB;
            assert_eq!(lease.payload()[0], 0xAA);
            assert_eq!(lease.payload()[99], 0xBB);

            // Write into header workspace
            let ws = lease.header_workspace_mut();
            assert_eq!(ws.len(), DEFAULT_XDP_HEADER_WORKSPACE);
            ws[0] = 0x42;
            assert_eq!(lease.header_workspace()[0], 0x42);
        }

        // Slab released on drop
        assert_eq!(arena.leased_count(), 0);
        assert_eq!(arena.available_count(), 64);
    }

    #[test]
    fn test_scan_available_word_all_tiers_consistency() {
        let words: Vec<AtomicU64> = (0..8).map(|_| AtomicU64::new(!0u64)).collect();
        // All words are completely full
        assert_eq!(scan_swar_32(&words, 0), None);
        assert_eq!(scan_swar_64(&words, 0), None);
        #[cfg(target_arch = "x86_64")]
        {
            assert_eq!(scan_sse2_128(&words, 0), None);
            assert_eq!(scan_avx2_256(&words, 0), None);
        }
        assert_eq!(scan_available_word(&words, 0), None);

        // Make word 5 have one free bit
        words[5].store(!(1u64 << 17), Ordering::Release);
        assert_eq!(scan_swar_32(&words, 0), Some(5));
        assert_eq!(scan_swar_64(&words, 0), Some(5));
        #[cfg(target_arch = "x86_64")]
        {
            assert_eq!(scan_sse2_128(&words, 0), Some(5));
            assert_eq!(scan_avx2_256(&words, 0), Some(5));
        }
        assert_eq!(scan_available_word(&words, 0), Some(5));

        // Start scanning after word 5 with wrap-around
        words[5].store(!0u64, Ordering::Release);
        words[2].store(!(1u64 << 3), Ordering::Release);
        assert_eq!(scan_swar_32(&words, 6), Some(2));
        assert_eq!(scan_swar_64(&words, 6), Some(2));
        #[cfg(target_arch = "x86_64")]
        {
            assert_eq!(scan_sse2_128(&words, 6), Some(2));
            assert_eq!(scan_avx2_256(&words, 6), Some(2));
        }
        assert_eq!(scan_available_word(&words, 6), Some(2));
    }

    #[test]
    fn test_slab_arena_exhaustion_and_recovery() {
        let config = XdpConfig::new("lo", 0).with_frame_count(64);
        let arena = XdpSlabArena::new(&config).expect("Arena creation ok");

        let mut leases = Vec::new();
        for _ in 0..64 {
            leases.push(arena.lease().expect("Should lease successfully"));
        }
        assert_eq!(arena.leased_count(), 64);
        assert_eq!(arena.available_count(), 0);

        // 65th lease should fail with RingFull
        assert!(arena.lease().is_err());

        // Drop one lease
        leases.pop();
        assert_eq!(arena.leased_count(), 63);
        assert_eq!(arena.available_count(), 1);

        // Lease again should succeed
        let recovered = arena.lease();
        assert!(recovered.is_ok());
    }
}
