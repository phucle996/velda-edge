//! Per-Core Multi-Level Arena & Node-Global 2-Tier Arena (`api_metering::arena`).
//!
//! ```text
//!  Per-Core Lane[core_id] (0 cross-core bounce, lazy-init on first touch)
//!  ├── Level 0 (tenant): Index(16B, (k+1)*4096B) ─► LocalRecord(128B) ─ bit0=1 ─┐
//!  ├── Level 1 (team)  : Index(16B, (k+1)*4096B) ─► LocalRecord(128B) ─ bit0=1 ─┼─► leaf
//!  └── Level 2 (user)  : Index(16B, (k+1)*4096B) ─► LocalRecord(128B) ─ bit0=0 ─┘
//!
//!  Refill / Sync on lease depletion -> Node-Global Shards[raw_hash & 63]
//!  └── Index(16B, 4/cacheline) ─► AccountState(192B, wallet + windows + grace_budget)
//! ```

use std::alloc::{Layout, alloc_zeroed, dealloc, handle_alloc_error};
use std::marker::PhantomData;
use std::mem::{align_of, size_of};
use std::ptr::NonNull;

/// Standard 4 KiB page-alignment quantum for contiguous user-space jemalloc extents.
pub const PAGE_SIZE: usize = 4096;

/// Maximum inline metering windows per tier (`192B` slot fits 4 windows inline without heap alloc).
pub const MAX_WINDOWS_PER_TIER: usize = 4;

/// Maximum hierarchical account levels (`Level 0 = tenant`, `Level 1 = team`, `Level 2 = user`).
pub const MAX_HIERARCHY_LEVELS: usize = 3;

/// Maximum tracked accounts per shard (power of 2; ~2M accounts across 64 shards).
pub const MAX_ACCOUNTS_PER_SHARD: usize = 32_768;

/// Maximum accounts tracked per core per level before CLOCK eviction.
const INITIAL_RECORD_SLOTS: usize = 64;
const INITIAL_LANE_RECORD_SLOTS: usize = 64;

/// Sentinel `key` indicating an unused table slot.
pub const SLOT_EMPTY: u64 = 0;

/// Sentinel `key` indicating a lazily reclaimed tombstone slot.
pub const SLOT_TOMBSTONE: u64 = 1;

/// Bounded CLOCK probe step limit when a shard hits `MAX_ACCOUNTS_PER_SHARD`.
const MAX_CLOCK_EVICTION_PROBES: usize = 64;

/// 16-byte probe slot (4 slots fit in one 64-byte L1 cache line).
#[repr(C)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AccountIndexSlot {
    pub key: u64,
    pub record_idx: u32,
    pub clock_ref: u8,
    pub _pad: [u8; 3],
}

const _: () = assert!(size_of::<AccountIndexSlot>() == 16);

/// Compact inline state for a single quota window (24 bytes exact, POD, `alloc_zeroed`-safe).
#[repr(C)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct InlineWindowSlot {
    pub a: u64,
    pub b: u64,
    pub c: u64,
}

const _: () = assert!(size_of::<InlineWindowSlot>() == 24);

/// Node-global authoritative account state (`192B` = 3 cache lines, `#[repr(C, align(64))]`).
#[repr(C, align(64))]
#[derive(Debug, Clone, Copy)]
pub struct AccountState {
    pub account_hash: u64,
    pub tier_fingerprint: u64,
    pub last_access_sec: u64,
    pub balance_micro_credits: i64,
    pub delta_micro_credits_deducted: i64,
    pub in_flight_delta: i64,
    pub windows: [InlineWindowSlot; MAX_WINDOWS_PER_TIER],
    pub grace_credits_remaining: i64,
    pub grace_window_remaining: [u32; MAX_WINDOWS_PER_TIER],
    pub reconcile_epoch: u32,
    pub window_count: u8,
}

const _: () = assert!(size_of::<AccountState>() == 192);
const _: () = assert!(align_of::<AccountState>() == 64);

/// Per-core local lease record (`128B` = 2 cache lines, zero Mutex / zero cross-core MESI).
#[repr(C, align(64))]
#[derive(Debug, Clone, Copy)]
pub struct LocalAccountRecord {
    pub tagged_key: u64,
    pub raw_hash: u64,
    pub tier_fingerprint: u64,
    pub reconcile_epoch: u32,
    pub window_count: u8,
    pub _pad0: [u8; 3],
    pub local_credits_lease: i64,
    pub local_balance_snapshot: i64,
    pub local_credits_spent_unflushed: i64,
    pub local_window_lease: [u32; MAX_WINDOWS_PER_TIER],
    pub local_window_epoch: [u64; MAX_WINDOWS_PER_TIER],
    pub local_window_rem_hint: [u32; MAX_WINDOWS_PER_TIER],
}

const _: () = assert!(size_of::<LocalAccountRecord>() == 128);
const _: () = assert!(align_of::<LocalAccountRecord>() == 64);

/// Contiguous `(k + 1) * 4096`-byte allocation aligned to `4096` bytes (`+1` slack page avoids early lazy alloc).
pub(crate) struct PageSlab<T> {
    ptr: NonNull<u8>,
    byte_size: usize,
    cap: usize,
    _marker: PhantomData<T>,
}

unsafe impl<T: Send> Send for PageSlab<T> {}
unsafe impl<T: Sync> Sync for PageSlab<T> {}

impl<T> PageSlab<T> {
    pub(crate) fn with_slack_page(min_elems: usize) -> Self {
        assert!(size_of::<T>() > 0);
        assert!(align_of::<T>() <= PAGE_SIZE);

        let min_bytes = min_elems
            .max(1)
            .checked_mul(size_of::<T>())
            .expect("PageSlab byte size overflow");
        // Allocate (k + 1) * 4096 bytes so slack capacity avoids immediate lazy realloc.
        let pages = min_bytes.div_ceil(PAGE_SIZE).saturating_add(1);
        let byte_size = pages
            .checked_mul(PAGE_SIZE)
            .expect("PageSlab page overflow");
        let layout =
            Layout::from_size_align(byte_size, PAGE_SIZE).expect("Invalid PageSlab layout");

        let raw = unsafe { alloc_zeroed(layout) };
        let ptr = NonNull::new(raw).unwrap_or_else(|| handle_alloc_error(layout));

        Self {
            ptr,
            byte_size,
            cap: byte_size / size_of::<T>(),
            _marker: PhantomData,
        }
    }

    #[inline(always)]
    pub(crate) fn as_slice(&self) -> &[T] {
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr() as *const T, self.cap) }
    }

    #[inline(always)]
    pub(crate) fn as_mut_slice(&mut self) -> &mut [T] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr() as *mut T, self.cap) }
    }
}

impl<T> Drop for PageSlab<T> {
    fn drop(&mut self) {
        let layout = Layout::from_size_align(self.byte_size, PAGE_SIZE)
            .expect("Invalid PageSlab drop layout");
        unsafe {
            dealloc(self.ptr.as_ptr(), layout);
        }
    }
}

/// 2-tier Node-Global shard container (`16B` index + dense `192B` `AccountState`).
pub(crate) struct ShardArena {
    index: PageSlab<AccountIndexSlot>,
    records: PageSlab<AccountState>,
    index_cap: usize,
    index_mask: usize,
    record_cap: usize,
    next_record_idx: usize,
    live_len: usize,
    occupied_len: usize,
    clock_hand: usize,
}

impl ShardArena {
    pub(crate) fn new() -> Self {
        let record_cap = INITIAL_RECORD_SLOTS;
        let index_cap = record_cap * 2;
        debug_assert!(index_cap.is_power_of_two());
        Self {
            index: PageSlab::with_slack_page(index_cap),
            records: PageSlab::with_slack_page(record_cap),
            index_cap,
            index_mask: index_cap - 1,
            record_cap,
            next_record_idx: 0,
            live_len: 0,
            occupied_len: 0,
            clock_hand: 0,
        }
    }

    #[inline(always)]
    fn start_slot(&self, account_hash: u64) -> usize {
        ((account_hash >> 6) as usize) & self.index_mask
    }

    #[inline(always)]
    pub(crate) fn get_mut(&mut self, account_hash: u64) -> Option<&mut AccountState> {
        debug_assert!(account_hash >= 2);
        let mut idx = self.start_slot(account_hash);
        let index_slots = self.index.as_slice();
        for _ in 0..self.index_cap {
            let slot = unsafe { index_slots.get_unchecked(idx) };
            if slot.key == account_hash {
                let ridx = slot.record_idx as usize;
                return Some(unsafe { self.records.as_mut_slice().get_unchecked_mut(ridx) });
            }
            if slot.key == SLOT_EMPTY {
                return None;
            }
            idx = (idx + 1) & self.index_mask;
        }
        None
    }

    #[inline(always)]
    pub(crate) fn get_or_insert_slot(
        &mut self,
        account_hash: u64,
        inactivity_ttl_sec: u64,
        now_sec: u64,
    ) -> (&mut AccountState, bool) {
        debug_assert!(account_hash >= 2);
        match self.find_index_slot(account_hash) {
            Ok((idx_pos, record_idx)) => unsafe {
                self.index
                    .as_mut_slice()
                    .get_unchecked_mut(idx_pos)
                    .clock_ref = 1;
                (
                    self.records.as_mut_slice().get_unchecked_mut(record_idx),
                    false,
                )
            },
            Err(insert_idx) => {
                self.insert_cold_or_rehash(account_hash, insert_idx, inactivity_ttl_sec, now_sec)
            }
        }
    }

    #[inline(always)]
    fn find_index_slot(&self, account_hash: u64) -> Result<(usize, usize), Option<usize>> {
        let mut idx = self.start_slot(account_hash);
        let mut first_tombstone: Option<usize> = None;
        let slots = self.index.as_slice();

        for _ in 0..self.index_cap {
            let slot = unsafe { slots.get_unchecked(idx) };
            if slot.key == account_hash {
                return Ok((idx, slot.record_idx as usize));
            }
            if slot.key == SLOT_EMPTY {
                return Err(Some(first_tombstone.unwrap_or(idx)));
            }
            if slot.key == SLOT_TOMBSTONE && first_tombstone.is_none() {
                first_tombstone = Some(idx);
            }
            idx = (idx + 1) & self.index_mask;
        }
        Err(first_tombstone)
    }

    #[cold]
    #[inline(never)]
    fn insert_cold_or_rehash(
        &mut self,
        account_hash: u64,
        mut insert_idx: Option<usize>,
        inactivity_ttl_sec: u64,
        now_sec: u64,
    ) -> (&mut AccountState, bool) {
        if self.next_record_idx >= self.record_cap
            || insert_idx.is_none()
            || self.occupied_len * 4 >= self.index_cap * 3
        {
            if self.record_cap < MAX_ACCOUNTS_PER_SHARD {
                self.grow_and_compact(inactivity_ttl_sec, now_sec);
                insert_idx = self.find_index_slot(account_hash).err().flatten();
            } else {
                let (evicted_idx, ridx) = self.evict_one_clock_slot(inactivity_ttl_sec, now_sec);
                let target_idx = insert_idx.unwrap_or(evicted_idx);
                if target_idx != evicted_idx {
                    unsafe {
                        self.index.as_mut_slice().get_unchecked_mut(evicted_idx).key =
                            SLOT_TOMBSTONE;
                    }
                }
                let slot = unsafe { self.index.as_mut_slice().get_unchecked_mut(target_idx) };
                if slot.key == SLOT_EMPTY {
                    self.occupied_len += 1;
                }
                slot.key = account_hash;
                slot.record_idx = ridx as u32;
                slot.clock_ref = 1;
                self.live_len += 1;

                let rec = unsafe { self.records.as_mut_slice().get_unchecked_mut(ridx) };
                rec.account_hash = account_hash;
                return (rec, true);
            }
        }

        let target_idx = match insert_idx {
            Some(i) => i,
            None => {
                self.grow_and_compact(inactivity_ttl_sec, now_sec);
                self.find_index_slot(account_hash)
                    .err()
                    .flatten()
                    .unwrap_or(0)
            }
        };

        let ridx = if self.next_record_idx < self.record_cap {
            let r = self.next_record_idx;
            self.next_record_idx += 1;
            r
        } else {
            let (_, r) = self.evict_one_clock_slot(inactivity_ttl_sec, now_sec);
            r
        };

        let slot = unsafe { self.index.as_mut_slice().get_unchecked_mut(target_idx) };
        if slot.key == SLOT_EMPTY {
            self.occupied_len += 1;
        }
        slot.key = account_hash;
        slot.record_idx = ridx as u32;
        slot.clock_ref = 1;
        self.live_len += 1;

        let rec = unsafe { self.records.as_mut_slice().get_unchecked_mut(ridx) };
        rec.account_hash = account_hash;
        (rec, true)
    }

    #[cold]
    #[inline(never)]
    fn grow_and_compact(&mut self, inactivity_ttl_sec: u64, now_sec: u64) {
        let mut active_live = 0usize;
        let idx_slice = self.index.as_mut_slice();
        let rec_slice = self.records.as_mut_slice();

        for i in 0..self.index_cap {
            let slot = unsafe { idx_slice.get_unchecked_mut(i) };
            if slot.key >= 2 {
                let rec = unsafe { rec_slice.get_unchecked_mut(slot.record_idx as usize) };
                if now_sec.saturating_sub(rec.last_access_sec) >= inactivity_ttl_sec {
                    slot.key = SLOT_TOMBSTONE;
                    rec.account_hash = SLOT_TOMBSTONE;
                } else {
                    active_live += 1;
                }
            }
        }

        let next_record_cap =
            if active_live * 4 >= self.record_cap * 3 && self.record_cap < MAX_ACCOUNTS_PER_SHARD {
                (self.record_cap * 2).min(MAX_ACCOUNTS_PER_SHARD)
            } else {
                self.record_cap
            };
        let next_index_cap = next_record_cap * 2;
        let next_mask = next_index_cap - 1;

        let mut new_index = PageSlab::<AccountIndexSlot>::with_slack_page(next_index_cap);
        let mut new_records = PageSlab::<AccountState>::with_slack_page(next_record_cap);
        let new_idx_slice = new_index.as_mut_slice();
        let new_rec_slice = new_records.as_mut_slice();
        let mut new_live = 0usize;

        for i in 0..self.index_cap {
            let old_idx = unsafe { idx_slice.get_unchecked(i) };
            if old_idx.key >= 2 && new_live < next_record_cap {
                let old_rec = unsafe { rec_slice.get_unchecked(old_idx.record_idx as usize) };
                unsafe {
                    *new_rec_slice.get_unchecked_mut(new_live) = *old_rec;
                }

                let mut pos = ((old_idx.key >> 6) as usize) & next_mask;
                for _ in 0..next_index_cap {
                    let dst = unsafe { new_idx_slice.get_unchecked_mut(pos) };
                    if dst.key == SLOT_EMPTY {
                        dst.key = old_idx.key;
                        dst.record_idx = new_live as u32;
                        dst.clock_ref = old_idx.clock_ref;
                        break;
                    }
                    pos = (pos + 1) & next_mask;
                }
                new_live += 1;
            }
        }

        self.index = new_index;
        self.records = new_records;
        self.index_cap = next_index_cap;
        self.index_mask = next_mask;
        self.record_cap = next_record_cap;
        self.next_record_idx = new_live;
        self.live_len = new_live;
        self.occupied_len = new_live;
        self.clock_hand = 0;
    }

    #[cold]
    #[inline(never)]
    fn evict_one_clock_slot(&mut self, inactivity_ttl_sec: u64, now_sec: u64) -> (usize, usize) {
        let cap = self.index_cap;
        let idx_slots = self.index.as_mut_slice();
        let rec_slots = self.records.as_mut_slice();
        let mut fallback_idx = self.clock_hand & self.index_mask;
        let mut fallback_rec = 0usize;

        for _ in 0..MAX_CLOCK_EVICTION_PROBES.min(cap) {
            let pos = self.clock_hand & self.index_mask;
            self.clock_hand = (self.clock_hand + 1) & self.index_mask;
            let slot = unsafe { idx_slots.get_unchecked_mut(pos) };

            if slot.key >= 2 {
                let ridx = slot.record_idx as usize;
                let rec = unsafe { rec_slots.get_unchecked_mut(ridx) };
                if now_sec.saturating_sub(rec.last_access_sec) >= inactivity_ttl_sec
                    || slot.clock_ref == 0
                {
                    slot.key = SLOT_TOMBSTONE;
                    rec.account_hash = SLOT_TOMBSTONE;
                    self.live_len = self.live_len.saturating_sub(1);
                    return (pos, ridx);
                }
                slot.clock_ref = 0;
                fallback_idx = pos;
                fallback_rec = ridx;
            }
        }

        let slot = unsafe { idx_slots.get_unchecked_mut(fallback_idx) };
        if slot.key >= 2 {
            let ridx = slot.record_idx as usize;
            slot.key = SLOT_TOMBSTONE;
            unsafe {
                rec_slots.get_unchecked_mut(ridx).account_hash = SLOT_TOMBSTONE;
            }
            self.live_len = self.live_len.saturating_sub(1);
            return (fallback_idx, ridx);
        }

        for i in 0..cap {
            let pos = (self.clock_hand + i) & self.index_mask;
            let s = unsafe { idx_slots.get_unchecked_mut(pos) };
            if s.key >= 2 {
                let ridx = s.record_idx as usize;
                s.key = SLOT_TOMBSTONE;
                unsafe {
                    rec_slots.get_unchecked_mut(ridx).account_hash = SLOT_TOMBSTONE;
                }
                self.live_len = self.live_len.saturating_sub(1);
                return (pos, ridx);
            }
        }

        (fallback_idx, fallback_rec)
    }

    #[inline(always)]
    pub(crate) fn records_mut(&mut self) -> &mut [AccountState] {
        unsafe {
            self.records
                .as_mut_slice()
                .get_unchecked_mut(..self.next_record_idx)
        }
    }
}

/// Independent `(k + 1) * 4096 B` arena for one hierarchy level inside a single core lane.
pub(crate) struct LevelArena {
    index: PageSlab<AccountIndexSlot>,
    records: PageSlab<LocalAccountRecord>,
    index_cap: usize,
    index_mask: usize,
    record_cap: usize,
    next_record_idx: usize,
    occupied_len: usize,
    clock_hand: usize,
}

impl LevelArena {
    pub(crate) fn new() -> Self {
        let record_cap = INITIAL_LANE_RECORD_SLOTS;
        let index_cap = record_cap * 2;
        Self {
            index: PageSlab::with_slack_page(index_cap),
            records: PageSlab::with_slack_page(record_cap),
            index_cap,
            index_mask: index_cap - 1,
            record_cap,
            next_record_idx: 0,
            occupied_len: 0,
            clock_hand: 0,
        }
    }

    #[inline(always)]
    fn start_slot(&self, tagged_key: u64) -> usize {
        ((tagged_key >> 1) as usize) & self.index_mask
    }

    /// Direct O(1) mutable access to a `LocalAccountRecord` by its slab index from Phase 1.
    #[inline(always)]
    pub(crate) fn record_mut_by_idx(
        &mut self,
        record_idx: usize,
    ) -> Option<&mut LocalAccountRecord> {
        self.records.as_mut_slice().get_mut(record_idx)
    }

    /// Probes 16B index (matching `tagged_key`), then jumps directly to `LocalAccountRecord`.
    #[inline(always)]
    pub(crate) fn get_or_insert_local(
        &mut self,
        tagged_key: u64,
        raw_hash: u64,
    ) -> (usize, &mut LocalAccountRecord, bool) {
        debug_assert!(tagged_key >= 2);
        let mut idx = self.start_slot(tagged_key);
        let mut first_tombstone: Option<usize> = None;
        let slots = self.index.as_slice();

        for _ in 0..self.index_cap {
            let slot = unsafe { slots.get_unchecked(idx) };
            if slot.key == tagged_key {
                let ridx = slot.record_idx as usize;
                unsafe {
                    self.index.as_mut_slice().get_unchecked_mut(idx).clock_ref = 1;
                    return (
                        ridx,
                        self.records.as_mut_slice().get_unchecked_mut(ridx),
                        false,
                    );
                }
            }
            if slot.key == SLOT_EMPTY {
                let insert_at = first_tombstone.unwrap_or(idx);
                return self.insert_local_cold(tagged_key, raw_hash, Some(insert_at));
            }
            if slot.key == SLOT_TOMBSTONE && first_tombstone.is_none() {
                first_tombstone = Some(idx);
            }
            idx = (idx + 1) & self.index_mask;
        }

        self.insert_local_cold(tagged_key, raw_hash, first_tombstone)
    }

    #[cold]
    #[inline(never)]
    fn insert_local_cold(
        &mut self,
        tagged_key: u64,
        raw_hash: u64,
        mut insert_idx: Option<usize>,
    ) -> (usize, &mut LocalAccountRecord, bool) {
        if self.next_record_idx >= self.record_cap
            || insert_idx.is_none()
            || self.occupied_len * 4 >= self.index_cap * 3
        {
            if self.record_cap < MAX_ACCOUNTS_PER_SHARD {
                self.grow_local();
                insert_idx = self.find_local_insert_slot(tagged_key);
            } else {
                let (ev_idx, ridx) = self.evict_local_clock();
                let target_idx = insert_idx.unwrap_or(ev_idx);
                let slot = unsafe { self.index.as_mut_slice().get_unchecked_mut(target_idx) };
                if slot.key == SLOT_EMPTY {
                    self.occupied_len += 1;
                }
                slot.key = tagged_key;
                slot.record_idx = ridx as u32;
                slot.clock_ref = 1;
                let rec = unsafe { self.records.as_mut_slice().get_unchecked_mut(ridx) };
                *rec = zero_local_record(tagged_key, raw_hash);
                return (ridx, rec, true);
            }
        }

        let target_idx = insert_idx.unwrap_or(0);
        let ridx = if self.next_record_idx < self.record_cap {
            let r = self.next_record_idx;
            self.next_record_idx += 1;
            r
        } else {
            let (_, r) = self.evict_local_clock();
            r
        };

        let slot = unsafe { self.index.as_mut_slice().get_unchecked_mut(target_idx) };
        if slot.key == SLOT_EMPTY {
            self.occupied_len += 1;
        }
        slot.key = tagged_key;
        slot.record_idx = ridx as u32;
        slot.clock_ref = 1;
        let rec = unsafe { self.records.as_mut_slice().get_unchecked_mut(ridx) };
        *rec = zero_local_record(tagged_key, raw_hash);
        (ridx, rec, true)
    }

    fn find_local_insert_slot(&self, tagged_key: u64) -> Option<usize> {
        let mut idx = self.start_slot(tagged_key);
        let slots = self.index.as_slice();
        for _ in 0..self.index_cap {
            let slot = unsafe { slots.get_unchecked(idx) };
            if slot.key < 2 {
                return Some(idx);
            }
            idx = (idx + 1) & self.index_mask;
        }
        None
    }

    #[cold]
    #[inline(never)]
    fn grow_local(&mut self) {
        let next_record_cap = (self.record_cap * 2).min(MAX_ACCOUNTS_PER_SHARD);
        let next_index_cap = next_record_cap * 2;
        let next_mask = next_index_cap - 1;

        let mut new_index = PageSlab::<AccountIndexSlot>::with_slack_page(next_index_cap);
        let mut new_records = PageSlab::<LocalAccountRecord>::with_slack_page(next_record_cap);
        let idx_slice = self.index.as_slice();
        let rec_slice = self.records.as_slice();
        let new_idx_slice = new_index.as_mut_slice();
        let new_rec_slice = new_records.as_mut_slice();
        let mut new_live = 0usize;

        for i in 0..self.index_cap {
            let old_idx = unsafe { idx_slice.get_unchecked(i) };
            if old_idx.key >= 2 && new_live < next_record_cap {
                let old_rec = unsafe { rec_slice.get_unchecked(old_idx.record_idx as usize) };
                unsafe {
                    *new_rec_slice.get_unchecked_mut(new_live) = *old_rec;
                }
                let mut pos = ((old_idx.key >> 1) as usize) & next_mask;
                for _ in 0..next_index_cap {
                    let dst = unsafe { new_idx_slice.get_unchecked_mut(pos) };
                    if dst.key == SLOT_EMPTY {
                        dst.key = old_idx.key;
                        dst.record_idx = new_live as u32;
                        dst.clock_ref = old_idx.clock_ref;
                        break;
                    }
                    pos = (pos + 1) & next_mask;
                }
                new_live += 1;
            }
        }

        self.index = new_index;
        self.records = new_records;
        self.index_cap = next_index_cap;
        self.index_mask = next_mask;
        self.record_cap = next_record_cap;
        self.next_record_idx = new_live;
        self.occupied_len = new_live;
        self.clock_hand = 0;
    }

    #[cold]
    #[inline(never)]
    fn evict_local_clock(&mut self) -> (usize, usize) {
        let cap = self.index_cap;
        let idx_slots = self.index.as_mut_slice();
        let mut fallback_idx = self.clock_hand & self.index_mask;
        let mut fallback_rec = 0usize;

        for _ in 0..MAX_CLOCK_EVICTION_PROBES.min(cap) {
            let pos = self.clock_hand & self.index_mask;
            self.clock_hand = (self.clock_hand + 1) & self.index_mask;
            let slot = unsafe { idx_slots.get_unchecked_mut(pos) };
            if slot.key >= 2 {
                let ridx = slot.record_idx as usize;
                if slot.clock_ref == 0 {
                    slot.key = SLOT_TOMBSTONE;
                    return (pos, ridx);
                }
                slot.clock_ref = 0;
                fallback_idx = pos;
                fallback_rec = ridx;
            }
        }

        let s = unsafe { idx_slots.get_unchecked_mut(fallback_idx) };
        if s.key >= 2 {
            let ridx = s.record_idx as usize;
            s.key = SLOT_TOMBSTONE;
            return (fallback_idx, ridx);
        }
        (fallback_idx, fallback_rec)
    }

    #[inline(always)]
    pub(crate) fn records_mut(&mut self) -> &mut [LocalAccountRecord] {
        unsafe {
            self.records
                .as_mut_slice()
                .get_unchecked_mut(..self.next_record_idx)
        }
    }
}

#[inline(always)]
fn zero_local_record(tagged_key: u64, raw_hash: u64) -> LocalAccountRecord {
    LocalAccountRecord {
        tagged_key,
        raw_hash,
        tier_fingerprint: 0,
        reconcile_epoch: 0,
        window_count: 0,
        _pad0: [0; 3],
        local_credits_lease: 0,
        local_balance_snapshot: 0,
        local_credits_spent_unflushed: 0,
        local_window_lease: [0; MAX_WINDOWS_PER_TIER],
        local_window_epoch: [0; MAX_WINDOWS_PER_TIER],
        local_window_rem_hint: [0; MAX_WINDOWS_PER_TIER],
    }
}

/// Per-core lane holding up to `MAX_HIERARCHY_LEVELS` non-contiguous `(k+1)*4096B` regions.
pub(crate) struct LaneArena {
    pub(crate) levels: [Option<LevelArena>; MAX_HIERARCHY_LEVELS],
}

impl LaneArena {
    pub(crate) const fn new() -> Self {
        Self {
            levels: [None, None, None],
        }
    }

    #[inline(always)]
    pub(crate) fn level_mut(&mut self, level: usize) -> &mut LevelArena {
        self.levels[level].get_or_insert_with(LevelArena::new)
    }
}
