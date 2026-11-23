//! Hardware-Aligned Page Bitmap Arena (`EtpPageArena<C>`) with L1D/L2 Cache Prefetching
//! and `(required_pages + 1) * 4096` Burst-Spillover Virtual Memory Allocation.
//!
//! ```text
//!   Plugin calls acquire() / try_acquire_ready_slot()
//!         │
//!         ▼
//!   [1] prefetch_l1_control(base_ptr)  ──► Read Cache Line 0 (ArenaControlLine) in L1D
//!         │
//!         ├─► ready_bitmap != 0 (Warm Hit):
//!         │     1. idx = ready_bitmap.trailing_zeros()          [1-cycle hardware TZCNT]
//!         │     2. Atomic bit clear ready[idx]=0, set busy[idx]=1 (LOCK BTR + LOCK OR)
//!         │     3. AGU computes slot_ptr = base + (idx << 6) + 64
//!         │     4. prefetch_l1_slot(slot_ptr) pulls ONLY Cache Line (idx + 1) from L2 -> L1D
//!         │     5. Return ReadyAcquire::Hit(idx) -> borrow &mut Slot[idx].conn in-place
//!         │
//!         └─► ready_bitmap == 0 (Miss / Burst Spillover):
//!               1. claim_empty_slot(allow_spare = true) via TZCNT(!(init | busy | ready))
//!               2. Connects in-place at Slot[idx] (spills into +4096B Spare Page when idx >= baseline)
//!               3. Marks init[idx]=1, busy[idx]=1 and borrows &mut Slot[idx].conn in-place
//!
//!   Plugin drops Lease (or calls reconnect() / recreate_in_background())
//!         │
//!         ├─► reconnect().await          ──► Drops dead socket & reconnects in-place at SAME Slot[idx]
//!         ├─► Healthy & idx < baseline   ──► prefetch_l2_slot(Slot[idx]), busy[idx]=0, ready[idx]=1
//!         ├─► Healthy & idx >= baseline  ──► Auto-decay burst slot: drop conn, init[idx]=0, busy[idx]=0
//!         └─► Broken (I/O error)         ──► Drop conn, init[idx]=0, busy[idx]=0, trigger_replenish()
//!
//!   Memory & Cache-Line Layout ((required_pages + 1) * 4096 Bytes, align = 4096):
//!   ┌───────────────────────────────────────────────────────────────────────────────────┐
//!   │ Cache Line 0   (offset = 0, 64B = 512 bits, L1D-Pinned via _MM_HINT_T0)           │
//!   │   ready_bitmap: [AtomicU64; 2] | busy_bitmap: [AtomicU64; 2] | init_bitmap: [2]   │
//!   │   baseline_slots: u16 | total_slots: u16 | page_count: u16 | replenishing: u8     │
//!   ├───────────────────────────────────────────────────────────────────────────────────┤
//!   │ Cache Line 1..=baseline (offset = (idx << 6) + 64, 64B/slot, L2-Resident)         │
//!   │   Slot[idx] (64B): [ conn: EtpStream (24B) / UdpSocket (16B) | last_used_sec: 8B] │
//!   ├───────────────────────────────────────────────────────────────────────────────────┤
//!   │ Spare Page (+4096 Bytes contiguous tail): Slot[baseline .. total_slots]           │
//!   │   Absorbs burst connections without heap fragmentation; auto-decays on Drop       │
//!   └───────────────────────────────────────────────────────────────────────────────────┘
//! ```

use std::alloc::{Layout, alloc_zeroed, dealloc, handle_alloc_error};
use std::cell::UnsafeCell;
use std::fmt;
use std::mem::{MaybeUninit, align_of, size_of};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// Standard OS virtual memory page size (4 KiB).
pub const PAGE_SIZE: usize = 4096;

/// Maximum number of connection slots tracked by the 2 x 64-bit bitmaps inside Cache Line 0.
pub const MAX_ARENA_SLOTS: usize = 128;

/// Maximum configured baseline pool size, reserving at least 1 addressable slot bit in
/// `[AtomicU64; 2]` for the `+4096` byte burst-spillover spare page.
pub const MAX_BASELINE_SLOTS: usize = MAX_ARENA_SLOTS - 1;

const CONTROL_LINE_BYTES: usize = 64;

/// Cache Line 0 (64 bytes = 512 bits) pinned at byte offset 0 of the page-aligned arena.
#[repr(C, align(64))]
pub struct ArenaControlLine {
    /// Bit `idx == 1` indicates `Slot[idx]` holds a warm, healthy, idle connection ready to borrow.
    pub ready_bitmap: [AtomicU64; 2],
    /// Bit `idx == 1` indicates `Slot[idx]` is exclusively owned (leased, connecting, or dropping).
    pub busy_bitmap: [AtomicU64; 2],
    /// Bit `idx == 1` indicates `Slot[idx].conn` contains an initialized value `C`.
    pub init_bitmap: [AtomicU64; 2],
    /// Configured baseline pool capacity (`pool_size`).
    pub baseline_slots: u16,
    /// Total slot capacity including burst overflow slots in the `+ 4096` byte spare page.
    pub total_slots: u16,
    /// Total allocated 4 KiB pages (`required_pages + 1`).
    pub page_count: u16,
    /// Lock-free background replenishment guard flag.
    pub replenishing: AtomicBool,
    _pad: [u8; 9],
}

const _: () = assert!(size_of::<ArenaControlLine>() == 64);
const _: () = assert!(align_of::<ArenaControlLine>() == 64);

/// Cache-line-aligned in-place connection storage slot inside [`EtpPageArena`].
#[repr(C, align(64))]
pub struct ArenaSlot<C> {
    conn: UnsafeCell<MaybeUninit<C>>,
    last_used_sec: UnsafeCell<u64>,
}

const _: () = assert!(size_of::<ArenaSlot<crate::connect::EtpStream>>() == 64);
const _: () = assert!(size_of::<ArenaSlot<tokio::net::UdpSocket>>() == 64);

/// Outcome of attempting to borrow a warm, non-expired connection slot in-place.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadyAcquire {
    /// Claimed warm `Slot[slot_idx]` in-place (now `busy = 1, ready = 0, init = 1`).
    Hit(usize),
    /// No valid warm connection was available; `discarded` indicates expired slots were pruned.
    Empty { discarded: bool },
}

// ============================================================================
// Multi-Target Hardware Cache Prefetch Primitives (L1D vs L2)
// ============================================================================

/// Prefetches Cache Line 0 (`ArenaControlLine`) into **L1D Cache** (`_MM_HINT_T0` / `pldl1keep`).
#[inline(always)]
pub fn prefetch_l1_control(ptr: *const u8) {
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
    {
        let _ = ptr;
    }
}

/// Prefetches an idle `ArenaSlot<C>` into **L2 Cache** (`_MM_HINT_T1` / `pldl2keep`) so idle
/// connection structs remain warm in L2 without evicting Cache Line 0 from L1D.
#[inline(always)]
pub fn prefetch_l2_slot(ptr: *const u8, size: usize) {
    #[cfg(target_arch = "x86_64")]
    {
        let mut offset = 0usize;
        while offset < size {
            unsafe {
                core::arch::x86_64::_mm_prefetch(
                    ptr.wrapping_add(offset) as *const i8,
                    core::arch::x86_64::_MM_HINT_T1,
                );
            }
            offset += 64;
        }
    }
    #[cfg(target_arch = "aarch64")]
    {
        let mut offset = 0usize;
        while offset < size {
            unsafe {
                let target = ptr.wrapping_add(offset);
                core::arch::asm!(
                    "prfm pldl2keep, [{0}]",
                    in(reg) target,
                    options(nostack, preserves_flags, readonly)
                );
            }
            offset += 64;
        }
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        let _ = (ptr, size);
    }
}

/// Prefetches only the selected `ArenaSlot<C>` into **L1D Cache** (`_MM_HINT_T0` / `pldl1keep`)
/// immediately after `TZCNT` resolves `slot_idx`.
#[inline(always)]
pub fn prefetch_l1_slot(ptr: *const u8, size: usize) {
    #[cfg(target_arch = "x86_64")]
    {
        let mut offset = 0usize;
        while offset < size {
            unsafe {
                core::arch::x86_64::_mm_prefetch(
                    ptr.wrapping_add(offset) as *const i8,
                    core::arch::x86_64::_MM_HINT_T0,
                );
            }
            offset += 64;
        }
    }
    #[cfg(target_arch = "aarch64")]
    {
        let mut offset = 0usize;
        while offset < size {
            unsafe {
                let target = ptr.wrapping_add(offset);
                core::arch::asm!(
                    "prfm pldl1keep, [{0}]",
                    in(reg) target,
                    options(nostack, preserves_flags, readonly)
                );
            }
            offset += 64;
        }
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        let _ = (ptr, size);
    }
}

// ============================================================================
// Page-Aligned In-Place Bitmap Arena
// ============================================================================

/// Contiguous, 4096-byte page-aligned virtual memory arena storing Cache Line 0 followed
/// by `total_slots` cache-line-aligned [`ArenaSlot<C>`] entries.
pub struct EtpPageArena<C> {
    pub base: NonNull<u8>,
    pub layout: Layout,
    pub baseline_slots: usize,
    pub total_slots: usize,
    pub page_count: usize,
    replenish_pending: AtomicBool,
    _marker: std::marker::PhantomData<ArenaSlot<C>>,
}

unsafe impl<C: Send> Send for EtpPageArena<C> {}
unsafe impl<C: Send> Sync for EtpPageArena<C> {}

impl<C> fmt::Debug for EtpPageArena<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EtpPageArena")
            .field(
                "arena_ptr",
                &format_args!("0x{:x}", self.base.as_ptr() as usize),
            )
            .field("page_count", &self.page_count)
            .field("byte_len", &self.layout.size())
            .field("baseline_slots", &self.baseline_slots)
            .field("total_slots", &self.total_slots)
            .finish()
    }
}

impl<C> EtpPageArena<C> {
    const SLOTS_OFFSET: usize = if align_of::<ArenaSlot<C>>() > CONTROL_LINE_BYTES {
        align_of::<ArenaSlot<C>>()
    } else {
        CONTROL_LINE_BYTES
    };

    /// Allocates a `(required_pages + 1) * 4096` byte, `4096`-aligned memory arena
    /// sized for `pool_size` baseline connections plus burst spillover slots in the spare page.
    pub fn new(pool_size: usize) -> Self {
        velda_core::clock::init_clock();
        let baseline_slots = pool_size.clamp(1, MAX_BASELINE_SLOTS);
        let slot_size = size_of::<ArenaSlot<C>>();

        let min_bytes = Self::SLOTS_OFFSET + baseline_slots * slot_size;
        let required_pages = min_bytes.div_ceil(PAGE_SIZE).max(1);
        let total_pages = required_pages + 1;
        let byte_len = total_pages * PAGE_SIZE;

        let raw_slots = (byte_len - Self::SLOTS_OFFSET) / slot_size;
        let total_slots = raw_slots.clamp(baseline_slots + 1, MAX_ARENA_SLOTS);
        debug_assert!(total_slots > baseline_slots);

        let layout =
            Layout::from_size_align(byte_len, PAGE_SIZE).expect("valid 4096-aligned arena layout");
        let raw_ptr = unsafe { alloc_zeroed(layout) };
        let Some(base) = NonNull::new(raw_ptr) else {
            handle_alloc_error(layout);
        };

        let ctrl_ptr = base.as_ptr() as *mut ArenaControlLine;
        unsafe {
            ctrl_ptr.write(ArenaControlLine {
                ready_bitmap: [AtomicU64::new(0), AtomicU64::new(0)],
                busy_bitmap: [AtomicU64::new(0), AtomicU64::new(0)],
                init_bitmap: [AtomicU64::new(0), AtomicU64::new(0)],
                baseline_slots: baseline_slots as u16,
                total_slots: total_slots as u16,
                page_count: total_pages as u16,
                replenishing: AtomicBool::new(false),
                _pad: [0u8; 9],
            });
        }

        Self {
            base,
            layout,
            baseline_slots,
            total_slots,
            page_count: total_pages,
            replenish_pending: AtomicBool::new(false),
            _marker: std::marker::PhantomData,
        }
    }

    #[inline(always)]
    pub fn control(&self) -> &ArenaControlLine {
        unsafe { &*(self.base.as_ptr() as *const ArenaControlLine) }
    }

    /// Computes the exact cache line address of `Slot[idx]`.
    ///
    /// Writing `.add(idx << 6).add(64)` instead of `.add((idx + 1) << 6)` eliminates
    /// the serial `idx + 1` ALU dependency before the shift, folding `+ 64` (Cache Line 0)
    /// and `+ base` directly into the CPU's AGU (Address Generation Unit) `[base + (idx << 6) + 64]`.
    #[inline(always)]
    unsafe fn slot_ptr(&self, idx: usize) -> *mut ArenaSlot<C> {
        debug_assert!(idx < self.total_slots);
        unsafe {
            if size_of::<ArenaSlot<C>>() == 64 && Self::SLOTS_OFFSET == 64 {
                self.base.as_ptr().add(idx << 6).add(64) as *mut ArenaSlot<C>
            } else {
                let slots_base = self.base.as_ptr().add(Self::SLOTS_OFFSET) as *mut ArenaSlot<C>;
                slots_base.add(idx)
            }
        }
    }

    #[inline(always)]
    fn word_mask(limit: usize, word: usize) -> u64 {
        let start = word * 64;
        if limit <= start {
            0
        } else {
            let bits = (limit - start).min(64);
            if bits == 64 {
                u64::MAX
            } else {
                (1u64 << bits) - 1
            }
        }
    }

    /// Returns the count of currently ready (warm, idle) connections in `ready_bitmap`.
    #[inline]
    pub fn ready_count(&self) -> usize {
        let ctrl = self.control();
        let w0 = ctrl.ready_bitmap[0].load(Ordering::Relaxed).count_ones() as usize;
        let w1 = ctrl.ready_bitmap[1].load(Ordering::Relaxed).count_ones() as usize;
        w0 + w1
    }

    /// Returns the count of baseline slots (`0..baseline_slots`) that are currently
    /// ready, busy, or initialized.
    #[inline]
    pub fn baseline_active_count(&self) -> usize {
        let ctrl = self.control();
        let mut total = 0usize;
        for w in 0..2 {
            let mask = Self::word_mask(self.baseline_slots, w);
            if mask == 0 {
                continue;
            }
            let active = (ctrl.ready_bitmap[w].load(Ordering::Acquire)
                | ctrl.busy_bitmap[w].load(Ordering::Acquire)
                | ctrl.init_bitmap[w].load(Ordering::Acquire))
                & mask;
            total += active.count_ones() as usize;
        }
        total
    }

    /// Attempts to claim an already-warm, non-expired connection slot in-place using
    /// hardware `TZCNT` (`trailing_zeros()`) on `ready_bitmap`.
    ///
    /// Expired idle slots encountered along the way are dropped in-place and cleared.
    pub fn try_acquire_ready_slot(&self, idle_timeout_secs: u64) -> ReadyAcquire {
        prefetch_l1_control(self.base.as_ptr());
        let ctrl = self.control();
        let mut discarded = false;

        'scan: loop {
            let mut saw_any = false;
            for w in 0..2 {
                let valid_mask = Self::word_mask(self.total_slots, w);
                if valid_mask == 0 {
                    continue;
                }
                let mut bits = ctrl.ready_bitmap[w].load(Ordering::Acquire) & valid_mask;
                while bits != 0 {
                    saw_any = true;
                    let tz = bits.trailing_zeros() as usize;
                    let bit = 1u64 << tz;
                    bits &= !bit;

                    let prev = ctrl.ready_bitmap[w].fetch_and(!bit, Ordering::Acquire);
                    if (prev & bit) == 0 {
                        // Lost race on this bit; try next ready bit.
                        continue;
                    }

                    // Claimed exclusive ownership of ready Slot[idx].
                    ctrl.busy_bitmap[w].fetch_or(bit, Ordering::Relaxed);
                    let idx = w * 64 + tz;
                    let slot = unsafe { &*self.slot_ptr(idx) };
                    prefetch_l1_slot(
                        slot as *const ArenaSlot<C> as *const u8,
                        size_of::<ArenaSlot<C>>(),
                    );

                    let last_used = unsafe { *slot.last_used_sec.get() };
                    let now = velda_core::clock::coarse_sec();
                    if now.saturating_sub(last_used) > idle_timeout_secs {
                        // Expired in-place: drop conn, clear init & busy, keep scanning.
                        unsafe {
                            (*slot.conn.get()).assume_init_drop();
                        }
                        ctrl.init_bitmap[w].fetch_and(!bit, Ordering::Release);
                        ctrl.busy_bitmap[w].fetch_and(!bit, Ordering::Release);
                        discarded = true;
                        continue 'scan;
                    }

                    return ReadyAcquire::Hit(idx);
                }
            }
            if !saw_any {
                break;
            }
        }

        ReadyAcquire::Empty { discarded }
    }

    /// Claims an empty slot (`!init && !busy && !ready`) via `TZCNT` and returns an
    /// async-cancellation-safe [`ClaimedSlotGuard`].
    ///
    /// If the future holding this guard is dropped or cancelled across an `.await` before
    /// `commit_leased()` or `commit_warm()` is invoked, [`ClaimedSlotGuard::drop`]
    /// automatically clears `busy_bitmap[idx]` back to `0`.
    ///
    /// - `allow_spare == false`: restricted to `0..baseline_slots` (for `warm()` and replenish).
    /// - `allow_spare == true`: scans `0..total_slots`, allowing spillover into the `+4096B`
    ///   spare page when all baseline slots are busy.
    pub fn claim_empty_slot(&self, allow_spare: bool) -> Option<ClaimedSlotGuard<'_, C>> {
        prefetch_l1_control(self.base.as_ptr());
        let ctrl = self.control();
        let limit = if allow_spare {
            self.total_slots
        } else {
            self.baseline_slots
        };

        for w in 0..2 {
            let limit_mask = Self::word_mask(limit, w);
            if limit_mask == 0 {
                continue;
            }
            loop {
                let occupied = ctrl.init_bitmap[w].load(Ordering::Acquire)
                    | ctrl.busy_bitmap[w].load(Ordering::Acquire)
                    | ctrl.ready_bitmap[w].load(Ordering::Acquire);
                let mut free = (!occupied) & limit_mask;
                if free == 0 {
                    break;
                }
                while free != 0 {
                    let tz = free.trailing_zeros() as usize;
                    let bit = 1u64 << tz;
                    free &= !bit;

                    let prev_busy = ctrl.busy_bitmap[w].fetch_or(bit, Ordering::Acquire);
                    if (prev_busy & bit) != 0 {
                        continue;
                    }

                    // Re-verify slot did not become initialized or ready while we set busy.
                    let still_occupied = (ctrl.init_bitmap[w].load(Ordering::Acquire)
                        | ctrl.ready_bitmap[w].load(Ordering::Acquire))
                        & bit;
                    if still_occupied != 0 {
                        ctrl.busy_bitmap[w].fetch_and(!bit, Ordering::Release);
                        continue;
                    }

                    return Some(ClaimedSlotGuard {
                        arena: self,
                        slot_idx: w * 64 + tz,
                    });
                }
            }
        }

        None
    }

    /// Writes a newly connected `C` directly into an exclusively claimed empty `Slot[idx]`
    /// and keeps it busy (`busy = 1, init = 1, ready = 0`) for immediate lease usage.
    pub fn write_leased_slot(&self, idx: usize, conn: C) {
        let w = (idx >> 6) & 1;
        let bit = 1u64 << (idx & 63);
        let ctrl = self.control();
        let slot = unsafe { &*self.slot_ptr(idx) };
        unsafe {
            (*slot.conn.get()).write(conn);
            *slot.last_used_sec.get() = velda_core::clock::coarse_sec();
        }
        ctrl.init_bitmap[w].fetch_or(bit, Ordering::Release);
        prefetch_l1_slot(
            slot as *const ArenaSlot<C> as *const u8,
            size_of::<ArenaSlot<C>>(),
        );
    }

    /// Writes a newly connected `C` directly into an exclusively claimed empty `Slot[idx]`,
    /// prefetches it into L2 cache, clears `busy`, and publishes it in `ready_bitmap`.
    pub fn finish_warm_slot(&self, idx: usize, conn: C) {
        let w = (idx >> 6) & 1;
        let bit = 1u64 << (idx & 63);
        let ctrl = self.control();
        let slot = unsafe { &*self.slot_ptr(idx) };
        unsafe {
            (*slot.conn.get()).write(conn);
            *slot.last_used_sec.get() = velda_core::clock::coarse_sec();
        }
        ctrl.init_bitmap[w].fetch_or(bit, Ordering::Release);
        prefetch_l2_slot(
            slot as *const ArenaSlot<C> as *const u8,
            size_of::<ArenaSlot<C>>(),
        );
        ctrl.busy_bitmap[w].fetch_and(!bit, Ordering::Release);
        ctrl.ready_bitmap[w].fetch_or(bit, Ordering::Release);
    }

    /// Releases an empty claimed slot (`init == 0`) back to unoccupied on connect failure.
    #[inline]
    pub fn release_empty_slot(&self, idx: usize) {
        let w = (idx >> 6) & 1;
        let bit = 1u64 << (idx & 63);
        self.control().busy_bitmap[w].fetch_and(!bit, Ordering::Release);
    }

    /// Returns a healthy leased slot back to the arena on `Drop`.
    ///
    /// - If `idx < baseline_slots`: updates `last_used_sec`, prefetches `Slot[idx]` to L2,
    ///   clears `busy_bitmap`, and sets `ready_bitmap` bit `idx` (`Release`).
    /// - If `idx >= baseline_slots` (burst overflow slot in `+4096B` spare page): drops `C`
    ///   in-place and clears `init_bitmap` + `busy_bitmap` so the burst socket auto-decays.
    pub fn release_healthy_slot(&self, idx: usize) {
        let w = (idx >> 6) & 1;
        let bit = 1u64 << (idx & 63);
        let ctrl = self.control();
        let slot = unsafe { &*self.slot_ptr(idx) };

        if idx < self.baseline_slots {
            unsafe {
                *slot.last_used_sec.get() = velda_core::clock::coarse_sec();
            }
            prefetch_l2_slot(
                slot as *const ArenaSlot<C> as *const u8,
                size_of::<ArenaSlot<C>>(),
            );
            ctrl.busy_bitmap[w].fetch_and(!bit, Ordering::Release);
            ctrl.ready_bitmap[w].fetch_or(bit, Ordering::Release);
        } else {
            // Burst slot in the +4096B spare page: auto-decay on Drop.
            unsafe {
                (*slot.conn.get()).assume_init_drop();
            }
            ctrl.init_bitmap[w].fetch_and(!bit, Ordering::Release);
            ctrl.busy_bitmap[w].fetch_and(!bit, Ordering::Release);
        }
    }

    /// Discards a broken leased slot on `Drop`, dropping `C` in-place and
    /// clearing `init_bitmap` + `busy_bitmap`.
    pub fn discard_broken_slot(&self, idx: usize) {
        let w = (idx >> 6) & 1;
        let bit = 1u64 << (idx & 63);
        let ctrl = self.control();
        let slot = unsafe { &*self.slot_ptr(idx) };
        unsafe {
            (*slot.conn.get()).assume_init_drop();
        }
        ctrl.init_bitmap[w].fetch_and(!bit, Ordering::Release);
        ctrl.busy_bitmap[w].fetch_and(!bit, Ordering::Release);
    }

    /// Replaces an already-initialized leased `Slot[idx]` in-place (dropping the old `C`
    /// and storing `fresh` + updating `last_used_sec`) so `Slot[idx]` is NEVER uninitialized
    /// while a Lease exists.
    #[inline]
    pub fn replace_leased_slot(&self, idx: usize, fresh: C) {
        let slot = unsafe { &*self.slot_ptr(idx) };
        unsafe {
            *(*slot.conn.get()).assume_init_mut() = fresh;
            *slot.last_used_sec.get() = velda_core::clock::coarse_sec();
        }
        prefetch_l1_slot(
            slot as *const ArenaSlot<C> as *const u8,
            size_of::<ArenaSlot<C>>(),
        );
    }

    /// Returns a shared reference to the initialized connection at `Slot[idx]`.
    ///
    /// # Safety
    /// Caller must hold the exclusive lease for `idx` (`busy_bitmap` bit `idx == 1` and
    /// `init_bitmap` bit `idx == 1`).
    #[inline(always)]
    pub unsafe fn slot_ref(&self, idx: usize) -> &C {
        let slot = unsafe { &*self.slot_ptr(idx) };
        unsafe { (*slot.conn.get()).assume_init_ref() }
    }

    /// Returns a mutable reference to the initialized connection at `Slot[idx]`.
    ///
    /// # Safety
    /// Caller must hold the exclusive lease for `idx` (`busy_bitmap` bit `idx == 1` and
    /// `init_bitmap` bit `idx == 1`) with `&mut` exclusivity.
    #[inline(always)]
    #[allow(clippy::mut_from_ref)]
    pub unsafe fn slot_mut(&self, idx: usize) -> &mut C {
        let slot = unsafe { &*self.slot_ptr(idx) };
        unsafe { (*slot.conn.get()).assume_init_mut() }
    }

    /// Attempts to claim the lock-free `AtomicBool` replenishment flag in Cache Line 0.
    #[inline]
    pub fn try_begin_replenish(&self) -> bool {
        self.replenish_pending.store(true, Ordering::Release);
        self.control()
            .replenishing
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    /// Clears the pending flag at the start of a replenish pass.
    #[inline]
    pub fn clear_replenish_pending(&self) {
        self.replenish_pending.store(false, Ordering::Release);
    }

    /// Finishes the replenish loop safely without losing concurrent wakeup signals.
    #[inline]
    pub fn try_finish_replenish(&self) -> bool {
        if self.replenish_pending.load(Ordering::Acquire) {
            return false;
        }
        self.control().replenishing.store(false, Ordering::Release);
        if self.replenish_pending.swap(false, Ordering::AcqRel)
            && self
                .control()
                .replenishing
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        {
            return false;
        }
        true
    }

    /// Forces the replenishing flag to `false` on error exit.
    #[inline]
    pub fn abort_replenish(&self) {
        self.control().replenishing.store(false, Ordering::Release);
    }
}

impl<C> Drop for EtpPageArena<C> {
    fn drop(&mut self) {
        let ctrl = self.control();
        for idx in 0..self.total_slots {
            let w = (idx >> 6) & 1;
            let bit = 1u64 << (idx & 63);
            if (ctrl.init_bitmap[w].load(Ordering::Acquire) & bit) != 0 {
                let slot = unsafe { &*self.slot_ptr(idx) };
                unsafe {
                    (*slot.conn.get()).assume_init_drop();
                }
                ctrl.init_bitmap[w].fetch_and(!bit, Ordering::Release);
            }
        }
        unsafe {
            dealloc(self.base.as_ptr(), self.layout);
        }
    }
}

/// Async-cancellation-safe RAII guard holding an exclusively claimed empty `Slot[idx]`
/// across `connect().await`.
///
/// Dropping this guard without calling [`ClaimedSlotGuard::commit_leased`] or
/// [`ClaimedSlotGuard::commit_warm`] automatically clears `busy_bitmap[idx]` back to `0`.
pub struct ClaimedSlotGuard<'a, C> {
    arena: &'a EtpPageArena<C>,
    pub slot_idx: usize,
}

impl<C> ClaimedSlotGuard<'_, C> {
    /// Commits a connected `C` as an active leased slot (`busy = 1, init = 1, ready = 0`)
    /// and returns `slot_idx`.
    #[inline]
    pub fn commit_leased(self, conn: C) -> usize {
        let idx = self.slot_idx;
        let arena = self.arena;
        std::mem::forget(self);
        arena.write_leased_slot(idx, conn);
        idx
    }

    /// Commits a connected `C` as a warm ready slot (`init = 1, busy = 0, ready = 1`).
    #[inline]
    pub fn commit_warm(self, conn: C) {
        let idx = self.slot_idx;
        let arena = self.arena;
        std::mem::forget(self);
        arena.finish_warm_slot(idx, conn);
    }
}

impl<C> Drop for ClaimedSlotGuard<'_, C> {
    #[inline]
    fn drop(&mut self) {
        self.arena.release_empty_slot(self.slot_idx);
    }
}
