//! Pre-Allocated Contiguous Memory Page Batch Pool (`velda_observability::logs::pool`).
//!
//! ```text
//!  ┌────────────────────────────────────────────────────────────────────────────────────────┐
//!  │                     MULTI-BUFFER ROTATION (LUÂN CHUYỂN ĐA VÙNG ĐỆM)                    │
//!  │                                                                                        │
//!  │                            ┌────────────────────────┐                                  │
//!  │                            │    FREE / READY POOL   │◄──────────────────────┐           │
//!  │                            │  (Available Slot Stack)│                       │           │
//!  │                            │   [Slot 3, 4, ... 31]  │                       │           │
//!  │                            └───────────┬────────────┘                       │           │
//!  │                                        │ acquire_slot()                     │           │
//!  │                                        ▼                                    │           │
//!  │                            ┌────────────────────────┐                       │           │
//!  │                            │   SLOT N: FILL STAGE   │                       │           │
//!  │                            │   (Producer Active)    │                       │           │
//!  │       SpscLogRing ────────►│ drain_batch_into()     │                       │           │
//!  │       (Frontline Core)     │ AVX Non-Temporal Store │                       │           │
//!  │                            │ (16 Pages = 64KB Cont) │                       │           │
//!  │                            └───────────┬────────────┘                       │           │
//!  │                                        │ fan-out (leases = K)               │           │
//!  │                                        ▼                                    │           │
//!  │                            ┌────────────────────────┐                       │           │
//!  │                            │  SLOT N-1: READ STAGE  │                       │           │
//!  │                            │  (Consumers In-Flight) │                       │           │
//!  │                            │   Multi-Subscriber     │                       │           │
//!  │                            │   _MM_HINT_T1 Prefetch │                       │           │
//!  │                            └──┬───────────┬─────────┘                       │           │
//!  │          Subscriber 1 (Std)   │           │   Subscriber 2 (ClickHouse)     │           │
//!  │          drop (leases - 1)    ▼           ▼   drop (last lease -> 0)        │           │
//!  │                            ┌────────────────────────┐                       │           │
//!  │                            │ SLOT N-2: RECYCLE STAGE│                       │           │
//!  │                            │      (Fast Return)     │                       │           │
//!  │                            │  records.clear()       │                       │           │
//!  │                            │  (len=0, keep capacity)│───────────────────────┘           │
//!  │                            └────────────────────────┘                                   │
//!  │                                                                                        │
//!  │  [ TIME EVOLUTION / ROTATION TIMELINE ]                                                │
//!  │  Slot ID    │ Tick T             │ Tick T+1           │ Tick T+2           │ State     │
//!  │  ───────────┼────────────────────┼────────────────────┼────────────────────┼───────────│
//!  │  Slot 0     │ [1. FILL (Drain)]  │ [2. IN-FLIGHT READ]│ [3. RECYCLE/READY] │ ↻ Rotate  │
//!  │  Slot 1     │ [2. IN-FLIGHT READ]│ [3. RECYCLE/READY] │ [1. FILL (Drain)]  │ ↻ Rotate  │
//!  │  Slot 2     │ [3. RECYCLE/READY] │ [1. FILL (Drain)]  │ [2. IN-FLIGHT READ]│ ↻ Rotate  │
//!  │  Slot 3..31 │ [ READY STANDBY ]  │ [ READY STANDBY ]  │ [ READY STANDBY ]  │ ↻ Pool    │
//!  └────────────────────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Structural Geometry & Multi-Buffer Lifecycle
//! - **Slot Geometry**: Each slot holds 256 records × 256 bytes = 65,536 bytes (16 contiguous 4KB
//!   virtual memory pages, 64-byte cache line aligned).
//! - **Pool Sizing**: 32 pre-allocated slots occupy 2 MB of contiguous RAM, matching Linux 2MB
//!   HugePage sizing.
//! - **Multi-Buffer Rotation**: While Slot $N$ is being filled by the producer, Slot $N-1$ is
//!   concurrently being read by subscribers, and Slot $N-2$ is recycled back to the ready pool.
//! - **Recycling**: When `leases.fetch_sub(1) == 1`, the final reference resets `records.clear()`
//!   (retaining allocated page capacity) and pushes `slot_id` back onto `available_slots`.

use std::cell::UnsafeCell;
use std::ops::Deref;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use super::record::AccessLogRecord;

/// Default number of rotating batch slots in the pool (32 slots × 64 KB = 2 MB, 1 HugePage).
pub const DEFAULT_POOL_SLOTS: usize = 32;

/// Default capacity per batch (256 records × 256B = 64 KB = 16 contiguous 4KB pages).
pub const DEFAULT_BATCH_CAPACITY: usize = 256;

/// A pre-allocated, contiguous memory slot residing in the pool.
pub struct BatchSlot {
    pub slot_id: usize,
    pub records: UnsafeCell<Vec<AccessLogRecord>>,
    pub leases: AtomicUsize,
}

unsafe impl Send for BatchSlot {}
unsafe impl Sync for BatchSlot {}

/// Central contiguous memory page pool managing rotating access log batches.
pub struct LogBatchPool {
    pub slots: Box<[BatchSlot]>,
    available: Mutex<Vec<usize>>,
    pub batch_capacity: usize,
}

unsafe impl Send for LogBatchPool {}
unsafe impl Sync for LogBatchPool {}

impl LogBatchPool {
    /// Pre-allocates `slot_count` contiguous batch buffers of `batch_capacity` records each.
    pub fn new(slot_count: usize, batch_capacity: usize) -> Arc<Self> {
        let count = slot_count.max(8);
        let cap = batch_capacity.max(64);

        let mut slots = Vec::with_capacity(count);
        for id in 0..count {
            slots.push(BatchSlot {
                slot_id: id,
                records: UnsafeCell::new(Vec::with_capacity(cap)),
                leases: AtomicUsize::new(0),
            });
        }

        let available: Vec<usize> = (0..count).collect();

        Arc::new(Self {
            slots: slots.into_boxed_slice(),
            available: Mutex::new(available),
            batch_capacity: cap,
        })
    }

    /// Acquires an available slot index from the pool in $\mathcal{O}(1)$.
    #[inline]
    pub fn acquire_slot(&self) -> Option<usize> {
        let mut guard = self.available.lock().unwrap();
        guard.pop()
    }

    /// Returns a slot index back to the available pool in $\mathcal{O}(1)$.
    #[inline]
    pub fn return_slot(&self, slot_id: usize) {
        let mut guard = self.available.lock().unwrap();
        guard.push(slot_id);
    }

    /// Number of currently available idle slots.
    pub fn available_count(&self) -> usize {
        self.available.lock().unwrap().len()
    }
}

/// An active lease pointing to a pooled contiguous memory batch.
pub struct PooledLogBatch {
    slot_id: usize,
    ptr: *const [AccessLogRecord],
    pool: Arc<LogBatchPool>,
}

unsafe impl Send for PooledLogBatch {}
unsafe impl Sync for PooledLogBatch {}

impl PooledLogBatch {
    /// Current number of active leases sharing this pooled slot.
    #[inline]
    pub fn lease_count(&self) -> usize {
        self.pool.slots[self.slot_id].leases.load(Ordering::Acquire)
    }
}

impl Clone for PooledLogBatch {
    fn clone(&self) -> Self {
        self.pool.slots[self.slot_id]
            .leases
            .fetch_add(1, Ordering::Relaxed);
        Self {
            slot_id: self.slot_id,
            ptr: self.ptr,
            pool: Arc::clone(&self.pool),
        }
    }
}

impl Drop for PooledLogBatch {
    fn drop(&mut self) {
        let slot = &self.pool.slots[self.slot_id];
        if slot.leases.fetch_sub(1, Ordering::AcqRel) == 1 {
            // Last subscriber finished reading this batch.
            // 1. Reset record count in O(1) without memory zeroing or deallocations.
            let records = unsafe { &mut *slot.records.get() };
            records.clear();
            // 2. Recycle slot back into the pool.
            self.pool.return_slot(self.slot_id);
        }
    }
}

/// Canonical batched log representation.
///
/// Encapsulates either a pooled contiguous memory batch ([`PooledLogBatch`]) or
/// a standalone fallback batch ([`Arc<[AccessLogRecord]>`]).
pub enum LogBatch {
    Pooled(PooledLogBatch),
    Arc(Arc<[AccessLogRecord]>),
}

impl Clone for LogBatch {
    fn clone(&self) -> Self {
        match self {
            Self::Pooled(p) => Self::Pooled(p.clone()),
            Self::Arc(a) => Self::Arc(Arc::clone(a)),
        }
    }
}

impl LogBatch {
    /// Creates a pooled batch from an acquired pool slot and record slice.
    #[inline]
    pub fn from_pooled(slot_id: usize, slice: &[AccessLogRecord], pool: Arc<LogBatchPool>) -> Self {
        Self::Pooled(PooledLogBatch {
            slot_id,
            ptr: slice as *const [AccessLogRecord],
            pool,
        })
    }

    /// Creates a fallback unpooled batch from a standard Arc slice.
    #[inline]
    pub fn from_arc(arc: Arc<[AccessLogRecord]>) -> Self {
        Self::Arc(arc)
    }

    /// Returns the underlying record slice.
    #[inline(always)]
    pub fn as_slice(&self) -> &[AccessLogRecord] {
        match self {
            Self::Pooled(p) => unsafe { &*p.ptr },
            Self::Arc(a) => a.as_ref(),
        }
    }

    /// Current number of active leases or reference count sharing this batch.
    #[inline]
    pub fn lease_count(&self) -> usize {
        match self {
            Self::Pooled(p) => p.lease_count(),
            Self::Arc(a) => Arc::strong_count(a),
        }
    }
}

impl Deref for LogBatch {
    type Target = [AccessLogRecord];

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl AsRef<[AccessLogRecord]> for LogBatch {
    #[inline(always)]
    fn as_ref(&self) -> &[AccessLogRecord] {
        self.as_slice()
    }
}
