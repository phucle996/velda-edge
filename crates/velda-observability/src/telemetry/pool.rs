//! Pre-Allocated Sharded SubPool Contiguous Batch Pool (`velda_observability::telemetry::pool`).
//!
//! ```text
//!  ┌────────────────────────────────────────────────────────────────────────────────────────┐
//!  │             SHARDED PER-CORE SUBPOOL + 1-CACHE-LINE BITMAP ROTATION                    │
//!  │                                                                                        │
//!  │  TelemetryBatchPool (1 SubPool per CPU core, clamped by RAM budget)                    │
//!  │   ├── SubPool[0] (Core 0)                                                              │
//!  │   │    ├─ CacheLine 0: bitmap: AtomicU64 (1 = batch full/leased, 0 = batch free)       │
//!  │   │    └─ slots[0..63]: [TelemetryBatchSlot; 64]  (each 64B align(64) -> 1 cache line) │
//!  │   │         └─ Vec<TelemetryRecord> buffer (512 × 256B = 128 KB heap, L3/RAM backed)   │
//!  │   ├── SubPool[1] (Core 1) ...                                                          │
//!  │   └── SubPool[N-1]                                                                     │
//!  │                                                                                        │
//!  │  [ STRICT CORE-LOCAL ACQUIRE / DROP LIFECYCLE (ZERO CROSS-CORE STEALING) ]             │
//!  │   1. Shadow Worker c: TZCNT(!bitmap[c]) -> CAS 0->1 exclusively on SubPool[c]          │
//!  │      └─ If bitmap[c] == u64::MAX: heap Arc fallback on c (never starves sibling cores) │
//!  │   2. ring.drain_batch_into() streams into RAM + stamps BatchSummary into 64B header    │
//!  │   3. leases.store(K, Release) -> Fan-out immutable batch to K subscribers              │
//!  │   4. Drop on exporter core: leases.fetch_sub(1, AcqRel) == 1                           │
//!  │      └─ records.clear(); bitmap[home_subpool].fetch_and(!(1 << slot_bit), Release)     │
//!  └────────────────────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Structural Geometry
//! - **Strict Core-Local SubPool Sharding**: Each core lane maps 1:1 to a dedicated `SubPool`
//!   of `64` slots governed by one 64-byte-aligned `AtomicU64` occupancy bitmap (`1` = full,
//!   `0` = free). One core never steals slots from sibling cores, preventing core-skew saturation
//!   from cascading across the node and avoiding cross-core bitmap cache invalidations on acquire.
//! - **Exact 64-Byte Slot Header (`TelemetryBatchSlot`)**: Compresses metadata (`Vec` handle,
//!   `AtomicU32` lease count, `home_subpool`, `slot_bit`, `seq_epoch`, timestamps, `error_count`,
//!   and `payload_bytes` / `url_bytes_sum` byte hints) cleanly into 1 cache line with zero
//!   cross-slot false sharing.

use std::cell::UnsafeCell;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use super::record::{TelemetryBatchSummary, TelemetryRecord};

/// Number of batch slots managed per [`SubPool`] (matches 64 bits of 1 [`AtomicU64`] bitmap).
pub const SUBPOOL_BATCHES: usize = 64;

/// Minimum number of subpools (`1 × 64 = 64` batch slots on 1-core hosts).
pub const MIN_SUBPOOLS: usize = 1;

/// Upper bound on auto-sized subpools (`64 × 64 = 4,096` batch slots).
pub const MAX_SUBPOOLS: usize = 64;

/// Default capacity per batch for multi-core hosts (512 records × 256B = 128 KB).
pub const DEFAULT_BATCH_CAPACITY: usize = 512;

/// Single-core floor batch capacity so 1-core hosts still hold 64 × 1024 = 65,536 records (16 MiB).
pub const SINGLE_CORE_FLOOR_BATCH_CAPACITY: usize = 1024;

/// Minimum batch record capacity under strict cgroup RAM constraints (256 records × 256B = 64 KB).
pub const MIN_BATCH_CAPACITY: usize = 256;

/// Target upper RAM fraction allowed for the rotating batch pool (6.25% of host/cgroup RAM).
const POOL_RAM_FRACTION_DIVISOR: usize = 16;
const MIN_RAM_BUDGET_BYTES: usize = 16 * 1024 * 1024;
const RECORD_BYTES: usize = std::mem::size_of::<TelemetryRecord>();

/// Dedicated cache-line wrapper for each [`SubPool`]'s 64-bit occupancy bitmap.
///
/// Bit `b == 1`: slot `b` is filled or actively leased by subscribers.
/// Bit `b == 0`: slot `b` is clear and ready for a Shadow Worker to acquire.
#[repr(C, align(64))]
struct AlignedBitmap(AtomicU64);

/// A pre-allocated, 64-byte cacheline-aligned batch header residing in a [`SubPool`].
///
/// Exactly 64 bytes (`0..64` B) with zero hidden compiler padding:
/// - `0..24`:  `records: UnsafeCell<Vec<TelemetryRecord>>` (pointer to contiguous batch records)
/// - `24..32`: `first_ts_ns: UnsafeCell<u64>` (Unix nanos of earliest record in batch)
/// - `32..40`: `last_ts_ns: UnsafeCell<u64>` (Unix nanos of latest record in batch)
/// - `40..44`: `leases: AtomicU32` (remaining active subscriber leases)
/// - `44..48`: `seq_epoch: AtomicU32` (monotonic batch acquisition counter)
/// - `48..52`: `error_count: UnsafeCell<u32>` (count of non-`Completed` records in batch)
/// - `52..56`: `payload_bytes: UnsafeCell<u32>` (sum of `bytes_rx + bytes_tx` across batch)
/// - `56..60`: `url_bytes_sum: UnsafeCell<u32>` (sum of `url_len` bytes across batch)
/// - `60..62`: `home_subpool: u16` (owning [`SubPool`] index for bit return on `Drop`)
/// - `62..63`: `slot_bit: u8` (`0..63` bit position within `home_subpool` bitmap)
/// - `63..64`: `flags: u8` (`BATCH_FLAG_*` summary bits)
#[repr(C, align(64))]
pub struct TelemetryBatchSlot {
    pub records: UnsafeCell<Vec<TelemetryRecord>>,
    pub first_ts_ns: UnsafeCell<u64>,
    pub last_ts_ns: UnsafeCell<u64>,
    pub leases: AtomicU32,
    pub seq_epoch: AtomicU32,
    pub error_count: UnsafeCell<u32>,
    pub payload_bytes: UnsafeCell<u32>,
    pub url_bytes_sum: UnsafeCell<u32>,
    pub home_subpool: u16,
    pub slot_bit: u8,
    pub flags: UnsafeCell<u8>,
}

const _: () = assert!(std::mem::size_of::<TelemetryBatchSlot>() == 64);
const _: () = assert!(std::mem::align_of::<TelemetryBatchSlot>() == 64);

unsafe impl Send for TelemetryBatchSlot {}
unsafe impl Sync for TelemetryBatchSlot {}

impl TelemetryBatchSlot {
    fn new(home_subpool: u16, slot_bit: u8, batch_capacity: usize) -> Self {
        Self {
            records: UnsafeCell::new(Vec::with_capacity(batch_capacity)),
            first_ts_ns: UnsafeCell::new(0),
            last_ts_ns: UnsafeCell::new(0),
            leases: AtomicU32::new(0),
            seq_epoch: AtomicU32::new(0),
            error_count: UnsafeCell::new(0),
            payload_bytes: UnsafeCell::new(0),
            url_bytes_sum: UnsafeCell::new(0),
            home_subpool,
            slot_bit,
            flags: UnsafeCell::new(0),
        }
    }

    /// Stamps summary fields after filling `records` and before publishing leases.
    ///
    /// # Safety
    /// Caller must hold exclusive write ownership via the `0 -> 1` bitmap transition on this slot
    /// prior to releasing any [`PooledTelemetryBatch`] handles to subscribers.
    #[inline]
    pub unsafe fn write_summary(&self, summary: TelemetryBatchSummary) {
        unsafe {
            *self.first_ts_ns.get() = summary.first_ts_ns;
            *self.last_ts_ns.get() = summary.last_ts_ns;
            *self.error_count.get() = summary.error_count;
            *self.payload_bytes.get() = summary.payload_bytes;
            *self.url_bytes_sum.get() = summary.url_bytes_sum;
            *self.flags.get() = summary.flags;
        }
    }

    /// Reads immutable summary metadata for a published slot.
    ///
    /// # Safety
    /// Slot must be in published read-only state (an active [`PooledTelemetryBatch`] lease exists).
    #[inline]
    pub unsafe fn read_summary(&self) -> TelemetryBatchSummary {
        unsafe {
            TelemetryBatchSummary {
                first_ts_ns: *self.first_ts_ns.get(),
                last_ts_ns: *self.last_ts_ns.get(),
                error_count: *self.error_count.get(),
                payload_bytes: *self.payload_bytes.get(),
                url_bytes_sum: *self.url_bytes_sum.get(),
                flags: *self.flags.get(),
            }
        }
    }
}

/// Core-local subpool holding 64 slots indexed by a single cache-line occupancy bitmap.
pub struct SubPool {
    bitmap: AlignedBitmap,
    slots: Box<[TelemetryBatchSlot]>,
}

unsafe impl Send for SubPool {}
unsafe impl Sync for SubPool {}

impl SubPool {
    fn new(subpool_idx: u16, batch_capacity: usize) -> Self {
        let mut slots = Vec::with_capacity(SUBPOOL_BATCHES);
        for bit in 0..SUBPOOL_BATCHES {
            slots.push(TelemetryBatchSlot::new(
                subpool_idx,
                bit as u8,
                batch_capacity,
            ));
        }
        Self {
            bitmap: AlignedBitmap(AtomicU64::new(0)),
            slots: slots.into_boxed_slice(),
        }
    }

    /// Attempts wait-free bit acquisition (`0 -> 1` CAS) on this subpool's 64-bit bitmap.
    #[inline]
    fn try_acquire(&self, subpool_idx: u16) -> Option<AcquiredSlot> {
        let mut cur = self.bitmap.0.load(Ordering::Relaxed);
        loop {
            let free_mask = !cur;
            if free_mask == 0 {
                return None;
            }
            let bit = free_mask.trailing_zeros() as usize;
            let next = cur | (1u64 << bit);
            match self.bitmap.0.compare_exchange_weak(
                cur,
                next,
                Ordering::Acquire,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    let slot = &self.slots[bit];
                    let epoch = slot
                        .seq_epoch
                        .fetch_add(1, Ordering::Relaxed)
                        .wrapping_add(1);
                    return Some(AcquiredSlot {
                        subpool_idx,
                        slot_bit: bit as u8,
                        epoch,
                    });
                }
                Err(observed) => cur = observed,
            }
        }
    }

    /// Returns a slot bit (`1 -> 0`) once all leases drop.
    #[inline]
    fn release_bit(&self, slot_bit: u8) {
        self.bitmap
            .0
            .fetch_and(!(1u64 << slot_bit), Ordering::Release);
    }

    #[inline]
    fn available_count(&self) -> usize {
        (!self.bitmap.0.load(Ordering::Relaxed)).count_ones() as usize
    }

    #[inline]
    fn raw_bitmap(&self) -> u64 {
        self.bitmap.0.load(Ordering::Relaxed)
    }
}

/// Token identifying a slot claimed via [`TelemetryBatchPool::acquire_slot_for_lane`].
#[derive(Debug, Clone, Copy)]
pub struct AcquiredSlot {
    pub subpool_idx: u16,
    pub slot_bit: u8,
    pub epoch: u32,
}

/// Computes `(subpool_count, batch_capacity)` from CPU core count and physical/cgroup RAM bytes.
///
/// Maps strictly 1 [`SubPool`] per core (`cores.clamp(MIN_SUBPOOLS, MAX_SUBPOOLS)`), applying a
/// `SINGLE_CORE_FLOOR_BATCH_CAPACITY` (`1024` records = `16 MiB` total) on 1-core hosts while
/// capping pool RAM usage within `memory_bytes / 16` (`MIN_RAM_BUDGET_BYTES = 16 MiB` floor).
pub const fn compute_telemetry_pool_geometry(cores: usize, memory_bytes: usize) -> (usize, usize) {
    let target_subpools = if cores < MIN_SUBPOOLS {
        MIN_SUBPOOLS
    } else if cores > MAX_SUBPOOLS {
        MAX_SUBPOOLS
    } else {
        cores
    };

    let nominal_batch_cap = if target_subpools == 1 {
        SINGLE_CORE_FLOOR_BATCH_CAPACITY
    } else {
        DEFAULT_BATCH_CAPACITY
    };

    if memory_bytes == 0 {
        return (target_subpools, nominal_batch_cap);
    }

    let raw_budget = memory_bytes / POOL_RAM_FRACTION_DIVISOR;
    let budget = if raw_budget < MIN_RAM_BUDGET_BYTES {
        MIN_RAM_BUDGET_BYTES
    } else {
        raw_budget
    };

    let bytes_per_subpool_nominal = SUBPOOL_BATCHES * nominal_batch_cap * RECORD_BYTES;
    if target_subpools * bytes_per_subpool_nominal <= budget {
        return (target_subpools, nominal_batch_cap);
    }

    let bytes_per_subpool_default = SUBPOOL_BATCHES * DEFAULT_BATCH_CAPACITY * RECORD_BYTES;
    if target_subpools * bytes_per_subpool_default <= budget {
        return (target_subpools, DEFAULT_BATCH_CAPACITY);
    }

    let bytes_per_subpool_min = SUBPOOL_BATCHES * MIN_BATCH_CAPACITY * RECORD_BYTES;
    if target_subpools * bytes_per_subpool_min <= budget {
        return (target_subpools, MIN_BATCH_CAPACITY);
    }

    let max_subpools = budget / bytes_per_subpool_min;
    let clamped = if max_subpools < MIN_SUBPOOLS {
        MIN_SUBPOOLS
    } else if max_subpools > target_subpools {
        target_subpools
    } else {
        max_subpools
    };
    (clamped, MIN_BATCH_CAPACITY)
}

/// Central sharded batch pool organized into per-core 64-slot [`SubPool`]s.
pub struct TelemetryBatchPool {
    sub_pools: Box<[SubPool]>,
    pub batch_capacity: usize,
}

unsafe impl Send for TelemetryBatchPool {}
unsafe impl Sync for TelemetryBatchPool {}

impl TelemetryBatchPool {
    /// Allocates a sharded pool sized for `(cores, memory_bytes)` hardware limits.
    pub fn for_hardware(cores: usize, memory_bytes: usize) -> Arc<Self> {
        let (subpool_count, batch_capacity) = compute_telemetry_pool_geometry(cores, memory_bytes);
        Self::with_subpools(subpool_count, batch_capacity)
    }

    /// Allocates an explicit number of 64-slot [`SubPool`]s with fixed per-batch capacity.
    pub fn with_subpools(subpool_count: usize, batch_capacity: usize) -> Arc<Self> {
        let count = subpool_count.max(1);
        let cap = batch_capacity.max(64);
        let mut sub_pools = Vec::with_capacity(count);
        for idx in 0..count {
            sub_pools.push(SubPool::new(idx as u16, cap));
        }
        Arc::new(Self {
            sub_pools: sub_pools.into_boxed_slice(),
            batch_capacity: cap,
        })
    }

    /// Allocates at least `slot_count` total batch slots rounded up to full 64-slot [`SubPool`]s.
    pub fn new(slot_count: usize, batch_capacity: usize) -> Arc<Self> {
        let subpool_count = slot_count.div_ceil(SUBPOOL_BATCHES).max(1);
        Self::with_subpools(subpool_count, batch_capacity)
    }

    /// Total number of slots across all [`SubPool`]s (`subpool_count() * 64`).
    #[inline]
    pub fn total_slots(&self) -> usize {
        self.sub_pools.len() * SUBPOOL_BATCHES
    }

    /// Number of [`SubPool`] shards backing this pool.
    #[inline]
    pub fn subpool_count(&self) -> usize {
        self.sub_pools.len()
    }

    /// Claims a free slot strictly from `lane`'s owning [`SubPool`] without cross-core stealing.
    #[inline]
    pub fn acquire_slot_for_lane(&self, lane: usize) -> Option<AcquiredSlot> {
        let idx = lane % self.sub_pools.len();
        self.sub_pools[idx].try_acquire(idx as u16)
    }

    /// Returns a reference to the [`TelemetryBatchSlot`] identified by `(subpool_idx, slot_bit)`.
    #[inline(always)]
    pub fn slot(&self, subpool_idx: u16, slot_bit: u8) -> &TelemetryBatchSlot {
        &self.sub_pools[subpool_idx as usize].slots[slot_bit as usize]
    }

    /// Releases a claimed slot bit back to its owning [`SubPool`] (`1 -> 0`).
    #[inline]
    pub fn release_slot(&self, subpool_idx: u16, slot_bit: u8) {
        self.sub_pools[subpool_idx as usize].release_bit(slot_bit);
    }

    /// Total number of currently free slots summed lock-free across all [`SubPool`] bitmaps.
    pub fn available_count(&self) -> usize {
        self.sub_pools.iter().map(SubPool::available_count).sum()
    }

    /// Reads the raw 64-bit occupancy bitmap for a given [`SubPool`] (`1` = full, `0` = free).
    pub fn subpool_bitmap(&self, subpool_idx: usize) -> Option<u64> {
        self.sub_pools.get(subpool_idx).map(SubPool::raw_bitmap)
    }
}

/// An active lease pointing to a pooled contiguous memory batch inside a [`SubPool`].
pub struct PooledTelemetryBatch {
    subpool_idx: u16,
    slot_bit: u8,
    epoch: u32,
    ptr: *const [TelemetryRecord],
    pool: Arc<TelemetryBatchPool>,
}

unsafe impl Send for PooledTelemetryBatch {}
unsafe impl Sync for PooledTelemetryBatch {}

impl PooledTelemetryBatch {
    /// Current number of active leases sharing this pooled slot.
    #[inline]
    pub fn lease_count(&self) -> usize {
        self.pool
            .slot(self.subpool_idx, self.slot_bit)
            .leases
            .load(Ordering::Acquire) as usize
    }

    /// Returns the batch summary metadata stamped into the slot header during drain.
    #[inline]
    pub fn summary(&self) -> TelemetryBatchSummary {
        unsafe {
            self.pool
                .slot(self.subpool_idx, self.slot_bit)
                .read_summary()
        }
    }

    /// Returns `(home_subpool, slot_bit, seq_epoch)` identifying where this batch resides.
    #[inline]
    pub fn location(&self) -> (u16, u8, u32) {
        (self.subpool_idx, self.slot_bit, self.epoch)
    }
}

impl Clone for PooledTelemetryBatch {
    fn clone(&self) -> Self {
        self.pool
            .slot(self.subpool_idx, self.slot_bit)
            .leases
            .fetch_add(1, Ordering::Relaxed);
        Self {
            subpool_idx: self.subpool_idx,
            slot_bit: self.slot_bit,
            epoch: self.epoch,
            ptr: self.ptr,
            pool: Arc::clone(&self.pool),
        }
    }
}

impl Drop for PooledTelemetryBatch {
    fn drop(&mut self) {
        let slot = self.pool.slot(self.subpool_idx, self.slot_bit);
        if slot.leases.fetch_sub(1, Ordering::AcqRel) == 1 {
            // Last consumer finished reading: reset length without freeing backing buffer,
            // then flip the owning SubPool bitmap bit back from 1 -> 0.
            let records = unsafe { &mut *slot.records.get() };
            records.clear();
            self.pool.release_slot(self.subpool_idx, self.slot_bit);
        }
    }
}

/// Canonical batched telemetry representation shared across log shippers and trace exporters.
pub enum TelemetryBatch {
    Pooled(PooledTelemetryBatch),
    Arc(Arc<[TelemetryRecord]>, TelemetryBatchSummary),
}

impl Clone for TelemetryBatch {
    fn clone(&self) -> Self {
        match self {
            Self::Pooled(p) => Self::Pooled(p.clone()),
            Self::Arc(a, s) => Self::Arc(Arc::clone(a), *s),
        }
    }
}

impl TelemetryBatch {
    /// Creates a pooled batch handle from an acquired subpool slot and filled record slice.
    #[inline]
    pub fn from_pooled(
        token: AcquiredSlot,
        slice: &[TelemetryRecord],
        pool: Arc<TelemetryBatchPool>,
    ) -> Self {
        Self::Pooled(PooledTelemetryBatch {
            subpool_idx: token.subpool_idx,
            slot_bit: token.slot_bit,
            epoch: token.epoch,
            ptr: slice as *const [TelemetryRecord],
            pool,
        })
    }

    /// Creates a fallback unpooled batch from an `Arc<[TelemetryRecord]>` slice and pre-computed summary.
    #[inline]
    pub fn from_arc_with_summary(
        arc: Arc<[TelemetryRecord]>,
        summary: TelemetryBatchSummary,
    ) -> Self {
        Self::Arc(arc, summary)
    }

    /// Creates a fallback unpooled batch from an `Arc<[TelemetryRecord]>` slice.
    #[inline]
    pub fn from_arc(arc: Arc<[TelemetryRecord]>) -> Self {
        let summary = TelemetryBatchSummary::from_slice(&arc);
        Self::Arc(arc, summary)
    }

    /// Returns the underlying record slice.
    #[inline(always)]
    pub fn as_slice(&self) -> &[TelemetryRecord] {
        match self {
            Self::Pooled(p) => unsafe { &*p.ptr },
            Self::Arc(a, _) => a.as_ref(),
        }
    }

    /// Returns the pre-computed batch summary metadata (`first_ts_ns`, `error_count`, byte hints).
    #[inline]
    pub fn summary(&self) -> TelemetryBatchSummary {
        match self {
            Self::Pooled(p) => p.summary(),
            Self::Arc(_, s) => *s,
        }
    }

    /// Current number of active leases or reference count sharing this batch.
    #[inline]
    pub fn lease_count(&self) -> usize {
        match self {
            Self::Pooled(p) => p.lease_count(),
            Self::Arc(a, _) => Arc::strong_count(a),
        }
    }
}

impl Deref for TelemetryBatch {
    type Target = [TelemetryRecord];

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl AsRef<[TelemetryRecord]> for TelemetryBatch {
    #[inline(always)]
    fn as_ref(&self) -> &[TelemetryRecord] {
        self.as_slice()
    }
}
