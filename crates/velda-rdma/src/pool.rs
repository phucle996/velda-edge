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

use std::alloc::{Layout, alloc_zeroed, dealloc};
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use crate::config::RdmaConfig;
use crate::error::RdmaError;
use crate::ffi::{IBV_ACCESS_LOCAL_WRITE, IBV_ACCESS_REMOTE_READ, IBV_ACCESS_REMOTE_WRITE};
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

/// Internal shared state of the RDMA slab arena.
struct RdmaSlabArenaInner {
    base_ptr: *mut u8,
    layout: Layout,
    mr: Option<MemoryRegion>,
    lkey: u32,
    config: RdmaSlabConfig,
    standard_masks: Box<[AtomicU64]>,
    large_masks: Box<[AtomicU64]>,
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

impl RdmaSlabArenaInner {
    /// Allocates an available slot index from the atomic bitmask for the requested class.
    fn alloc_slot(&self, class: RdmaSlabClass) -> Result<u32, RdmaError> {
        match class {
            RdmaSlabClass::Standard2K => {
                let count = self.config.standard_count;
                let words = &self.standard_masks;
                let num_words = words.len();
                if num_words == 0 {
                    return Err(RdmaError::ArenaExhausted {
                        class: class.as_str(),
                    });
                }

                let start = self.standard_hint.load(Ordering::Relaxed) % num_words;
                for i in 0..num_words {
                    let word_idx = (start + i) % num_words;
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
                                self.standard_used.fetch_add(1, Ordering::Relaxed);
                                self.standard_hint.store(word_idx, Ordering::Relaxed);
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
            RdmaSlabClass::Large64K => {
                let count = self.config.large_count;
                let words = &self.large_masks;
                let num_words = words.len();
                if num_words == 0 {
                    return Err(RdmaError::ArenaExhausted {
                        class: class.as_str(),
                    });
                }

                let start = self.large_hint.load(Ordering::Relaxed) % num_words;
                for i in 0..num_words {
                    let word_idx = (start + i) % num_words;
                    let atomic_word = &words[word_idx];
                    let mut current = atomic_word.load(Ordering::Acquire);

                    while current != !0u64 {
                        let bit = (!current).trailing_zeros();
                        if bit >= BITS_PER_ATOMIC_WORD as u32 {
                            break;
                        }

                        let slot_idx = (word_idx * BITS_PER_ATOMIC_WORD + bit as usize) as u32;
                        if slot_idx as usize >= count {
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
                                self.large_used.fetch_add(1, Ordering::Relaxed);
                                self.large_hint.store(word_idx, Ordering::Relaxed);
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
        }
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

        // Step 3: Register the entire contiguous block once with the HCA protection domain
        let access_flags =
            IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ;
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

    /// Automatically discovers RDMA hardware, falling back to mock mode if unavailable.
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
                Self::new_mock(slab_config)
            }
            _ => Self::new_mock(slab_config),
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
        let std_words = config.standard_count.div_ceil(BITS_PER_ATOMIC_WORD);
        let mut std_vec = Vec::with_capacity(std_words);
        for i in 0..std_words {
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

        let lrg_words = config.large_count.div_ceil(BITS_PER_ATOMIC_WORD);
        let mut lrg_vec = Vec::with_capacity(lrg_words);
        for i in 0..lrg_words {
            let mut init_mask = 0u64;
            let start_slot = i * BITS_PER_ATOMIC_WORD;
            for bit in 0..BITS_PER_ATOMIC_WORD {
                if start_slot + bit >= config.large_count {
                    init_mask |= 1u64 << bit;
                }
            }
            lrg_vec.push(AtomicU64::new(init_mask));
        }

        let inner = RdmaSlabArenaInner {
            base_ptr: ptr,
            layout,
            mr,
            lkey,
            config,
            standard_masks: std_vec.into_boxed_slice(),
            large_masks: lrg_vec.into_boxed_slice(),
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
}
