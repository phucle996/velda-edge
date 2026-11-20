//! Multi-Exporter Fan-Out Telemetry Store (`velda_observability::telemetry::store`).
//!
//! ```text
//!  ┌────────────────────────────────────────────────────────────────────────────────────────┐
//!  │                    TELEMETRYSTORE MULTI-EXPORTER ARCHITECTURE                          │
//!  │                                                                                        │
//!  │    Frontline Core 0      Frontline Core 1                Frontline Core N-1            │
//!  │   ┌─────────────────┐   ┌─────────────────┐             ┌─────────────────┐            │
//!  │   │ SpscTelemetryRing (0) │   │ SpscTelemetryRing (1) │   ... ...   │ SpscTelemetryRing(N-1)│            │
//!  │   └────────┬────────┘   └────────┬────────┘             └────────┬────────┘            │
//!  │            │                     │                               │                     │
//!  │            │ flush_lane(0)       │ flush_lane(1)                 │ flush_lane(N-1)     │
//!  │            ▼                     ▼                               ▼                     │
//!  │  ┌──────────────────────────────────────────────────────────────────────────────────┐  │
//!  │  │                      Shadow Logistics Worker (SCHED_IDLE)                        │  │
//!  │  │                                                                                  │  │
//!  │  │  1. acquire_slot() ──► TelemetryBatchPool (32 Slots × 64KB)                            │  │
//!  │  │  2. ring.drain_batch_into(&mut slot.records) (VMOVNTDQ / AVX streaming store)    │  │
//!  │  │  3. slot.leases.store(K)                                                         │  │
//!  │  │  4. TelemetryBatch::from_pooled(slot_id, slice, pool)                                  │  │
//!  │  └───────────────────────────────────┬──────────────────────────────────────────────┘  │
//!  │                                      │                                                 │
//!  │                        Fan-Out to K Active Subscribers                                 │
//!  │         ┌────────────────────────────┼────────────────────────────┐                    │
//!  │         ▼                            ▼                            ▼                    │
//!  │  ┌──────────────┐             ┌──────────────┐             ┌──────────────┐            │
//!  │  │ Subscriber 1 │             │ Subscriber 2 │             │ Subscriber K │            │
//!  │  │ (StdLogSink) │             │ (ClickHouse) │             │ (OTLP/Loki)  │            │
//!  │  │ Queue (256)  │             │ Queue (256)  │             │ Queue (256)  │            │
//!  │  └──────┬───────┘             └──────┬───────┘             └──────┬───────┘            │
//!  │         │                            │                            │                    │
//!  │         │ pop & serialize            │ pop & serialize            │ pop & serialize    │
//!  │         ▼                            ▼                            ▼                    │
//!  │    drop(TelemetryBatch)               drop(TelemetryBatch)               drop(TelemetryBatch)            │
//!  │    leases - 1                   leases - 1                   leases - 1                │
//!  │         │                            │                            │                    │
//!  │         └────────────────────────────┼────────────────────────────┘                    │
//!  │                                      │                                                 │
//!  │                                      ▼ (last drop -> leases == 0)                      │
//!  │                       ┌──────────────────────────────┐                                 │
//!  │                       │ slot.records.clear()         │                                 │
//!  │                       │ pool.return_slot(slot_id)    │                                 │
//!  │                       └──────────────────────────────┘                                 │
//!  │                                                                                        │
//!  │  [ MULTI-BUFFER ROTATION PIPELINE ]                                                    │
//!  │  Shadow Worker (Producer) ──► Slot N: Draining SPSC ring (AVX stream store)            │
//!  │  Subscribers (Consumers)  ──► Slot N-1: Reading and serializing payload                │
//!  │  Recycle (Last Drop)      ──► Slot N-2: Reset len=0 and push back to available stack   │
//!  │  Free Standby Pool        ──► Slots N+1..N+k: Ready slots for subsequent ticks         │
//!  └────────────────────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Mechanics
//! - **Core Isolation**: Frontline request workers push records into core-local SPSC
//!   rings without cross-core locks.
//! - **Multi-Buffer Rotation**: Shadow worker drains into a fresh pool slot while previous
//!   slots remain in-flight with subscribers or undergo zero-copy recycling.
//! - **Subscriber Fan-Out**: Each active subscriber receives a leased handle to the pooled batch.
//! - **Memory Recycling**: Once all subscribers finish processing, the slot is cleared and
//!   returned to `TelemetryBatchPool` without memory reallocation.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use crate::metrics::{MAX_METRIC_WORKERS, current_worker_lane};

use super::pool::{DEFAULT_BATCH_CAPACITY, DEFAULT_POOL_SLOTS, TelemetryBatch, TelemetryBatchPool};
use super::record::TelemetryRecord;
use super::ring::{DEFAULT_CORE_RING_CAPACITY, SpscTelemetryRing};

struct SubscriberInner {
    id: u64,
    max_batches: usize,
    queue: Mutex<VecDeque<TelemetryBatch>>,
    pub dropped_batches: AtomicU64,
}

/// RAII handle held by an active background log exporter plugin.
///
/// Dropping a received [`TelemetryBatch`] automatically recycles its contiguous memory
/// pages back into the pool without calling the OS allocator (`__rust_dealloc == 0`).
/// Dropping `TelemetrySubscriber` itself unregisters the exporter from [`TelemetryStore`].
pub struct TelemetrySubscriber {
    inner: Arc<SubscriberInner>,
    store: Arc<TelemetryStore>,
}

impl TelemetrySubscriber {
    /// Pops the next batch of [`TelemetryRecord`]s if available.
    pub fn try_recv_batch(&self) -> Option<TelemetryBatch> {
        self.inner.queue.lock().unwrap().pop_front()
    }

    /// Drains up to `max_batches` currently queued batches for bulk export.
    pub fn drain_batches(&self, max_batches: usize) -> Vec<TelemetryBatch> {
        let mut guard = self.inner.queue.lock().unwrap();
        let take = guard.len().min(max_batches);
        let mut out = Vec::with_capacity(take);
        for _ in 0..take {
            if let Some(b) = guard.pop_front() {
                out.push(b);
            }
        }
        out
    }

    /// Number of queued batches evicted from the front due to subscriber backpressure.
    #[inline]
    pub fn dropped_batches(&self) -> u64 {
        self.inner.dropped_batches.load(Ordering::Relaxed)
    }
}

impl Drop for TelemetrySubscriber {
    fn drop(&mut self) {
        let mut subs = self.store.subscribers.lock().unwrap();
        subs.retain(|s| s.id != self.inner.id);
        self.store
            .active_subscriber_count
            .store(subs.len(), Ordering::Release);
    }
}

/// Central Lock-Free Telemetry Store owned by `velda-observability`.
pub struct TelemetryStore {
    pub shards: Vec<SpscTelemetryRing>,
    pub active_cores: usize,
    shard_mask: usize,
    subscribers: Mutex<Vec<Arc<SubscriberInner>>>,
    pub active_subscriber_count: AtomicUsize,
    next_subscriber_id: AtomicU64,
    pub batch_pool: Arc<TelemetryBatchPool>,
}

impl Default for TelemetryStore {
    fn default() -> Self {
        Self::new()
    }
}

impl TelemetryStore {
    pub fn new() -> Self {
        let detected_cores = velda_core::hardware::global_hardware_topology()
            .cpu
            .available_cores;
        Self::with_cores_and_capacity(detected_cores, DEFAULT_CORE_RING_CAPACITY)
    }

    pub fn with_cores_and_capacity(cores: usize, ring_capacity: usize) -> Self {
        // Power-of-two shards let hot-path emit() index via 1-cycle bitmask instead of integer div.
        let active_cores = cores.clamp(1, MAX_METRIC_WORKERS).next_power_of_two();
        let shard_mask = active_cores - 1;
        let mut shards = Vec::with_capacity(active_cores);
        for _ in 0..active_cores {
            shards.push(SpscTelemetryRing::new(ring_capacity));
        }

        Self {
            shards,
            active_cores,
            shard_mask,
            subscribers: Mutex::new(Vec::new()),
            active_subscriber_count: AtomicUsize::new(0),
            next_subscriber_id: AtomicU64::new(1),
            batch_pool: TelemetryBatchPool::new(DEFAULT_POOL_SLOTS, DEFAULT_BATCH_CAPACITY),
        }
    }

    /// Emits a 256-byte [`TelemetryRecord`] onto the calling thread's core-local SPSC ring.
    #[inline(always)]
    pub fn emit(&self, record: &TelemetryRecord) -> bool {
        self.emit_on_core(current_worker_lane(), record)
    }

    /// Emits a 256-byte [`TelemetryRecord`] onto an explicit worker core's SPSC ring.
    #[inline(always)]
    pub fn emit_on_core(&self, core_id: usize, record: &TelemetryRecord) -> bool {
        let idx = core_id & self.shard_mask;
        unsafe { self.shards.get_unchecked(idx).push(record) }
    }

    /// Emits a 256-byte [`TelemetryRecord`] on cold error / early-rejection paths.
    ///
    /// Marked `#[cold]` and `#[inline(never)]` so that early error handlers (400, 404, 431,
    /// 502, 503, 504, client disconnects) do not duplicate 8 `vmovntdq` non-temporal stores
    /// and vector shuffles into frontline serving loops, preserving the 32 KB L1 Instruction
    /// Cache for hot request execution.
    #[cold]
    #[inline(never)]
    pub fn emit_cold(&self, record: &TelemetryRecord) -> bool {
        self.emit_cold_on_core(current_worker_lane(), record)
    }

    /// Emits a 256-byte [`TelemetryRecord`] onto an explicit worker core's SPSC ring on cold error paths.
    #[cold]
    #[inline(never)]
    pub fn emit_cold_on_core(&self, core_id: usize, record: &TelemetryRecord) -> bool {
        let idx = core_id & self.shard_mask;
        unsafe { self.shards.get_unchecked(idx).push(record) }
    }

    /// Registers a new background exporter subscriber (log shipper or trace exporter).
    ///
    /// Caps queue depth at half of `batch_pool` slots so a slow TCP/HTTP exporter drops its
    /// oldest unconsumed leases before it can pin the entire rotating pool.
    pub fn subscribe(self: &Arc<Self>, max_batches: usize) -> TelemetrySubscriber {
        let id = self.next_subscriber_id.fetch_add(1, Ordering::Relaxed);
        let pool_cap = (self.batch_pool.slots.len() / 2).max(4);
        let capped = max_batches.clamp(4, pool_cap);
        let inner = Arc::new(SubscriberInner {
            id,
            max_batches: capped,
            queue: Mutex::new(VecDeque::with_capacity(capped)),
            dropped_batches: AtomicU64::new(0),
        });

        let mut subs = self.subscribers.lock().unwrap();
        subs.push(Arc::clone(&inner));
        self.active_subscriber_count
            .store(subs.len(), Ordering::Release);
        drop(subs);

        TelemetrySubscriber {
            inner,
            store: Arc::clone(self),
        }
    }

    /// Executed by Shadow Worker (`velda-shadow-{lane}`) to drain its core-local SPSC ring
    /// and fan out zero-allocation [`TelemetryBatch`] buffers to active exporters.
    ///
    /// Uses pre-allocated contiguous memory pages from [`TelemetryBatchPool`], recycling memory
    /// in $\mathcal{O}(1)$ upon drop without returning pages to the OS allocator.
    pub fn flush_lane(&self, lane: usize, max_batch_size: usize) -> usize {
        let idx = lane & self.shard_mask;
        let ring = unsafe { self.shards.get_unchecked(idx) };

        if self.active_subscriber_count.load(Ordering::Acquire) == 0 {
            return ring.discard_all();
        }

        let subs = self.subscribers.lock().unwrap();
        if subs.is_empty() {
            return ring.discard_all();
        }
        let active_count = subs.len();

        let batch = if let Some(slot_id) = self.batch_pool.acquire_slot() {
            let slot = &self.batch_pool.slots[slot_id];
            let records_vec = unsafe { &mut *slot.records.get() };
            let count = ring.drain_batch_into(records_vec, max_batch_size);
            if count == 0 {
                self.batch_pool.return_slot(slot_id);
                return 0;
            }
            slot.leases.store(1, Ordering::Release);
            TelemetryBatch::from_pooled(
                slot_id,
                records_vec.as_slice(),
                Arc::clone(&self.batch_pool),
            )
        } else {
            let records = ring.drain_batch(max_batch_size.max(64));
            if records.is_empty() {
                return 0;
            }
            TelemetryBatch::from_arc(Arc::from(records))
        };

        let count = batch.len();

        for (i, sub) in subs.iter().enumerate() {
            let mut q = sub.queue.lock().unwrap();
            if q.len() >= sub.max_batches {
                q.pop_front();
                sub.dropped_batches.fetch_add(1, Ordering::Relaxed);
            }
            if i + 1 == active_count {
                q.push_back(batch);
                break;
            } else {
                q.push_back(batch.clone());
            }
        }

        count
    }

    /// Flushes all worker core lanes into registered exporter subscribers.
    pub fn flush_all_lanes(&self, max_batch_per_lane: usize) -> usize {
        let mut total = 0;
        for lane in 0..self.active_cores {
            total += self.flush_lane(lane, max_batch_per_lane);
        }
        total
    }

    /// Total records emitted across all core-local SPSC rings (computed cold on demand).
    pub fn emitted_total(&self) -> u64 {
        self.shards.iter().map(|s| s.emitted_count()).sum()
    }

    pub fn ring_dropped_total(&self) -> u64 {
        self.shards.iter().map(|s| s.dropped_count()).sum()
    }
}

static GLOBAL_TELEMETRY_STORE: OnceLock<Arc<TelemetryStore>> = OnceLock::new();

#[cold]
#[inline(never)]
fn init_default_telemetry_store() -> &'static Arc<TelemetryStore> {
    GLOBAL_TELEMETRY_STORE.get_or_init(|| Arc::new(TelemetryStore::new()))
}

/// Returns a reference to the globally shared in-memory telemetry store without cloning `Arc`.
#[inline(always)]
pub fn global_telemetry_store() -> &'static Arc<TelemetryStore> {
    match GLOBAL_TELEMETRY_STORE.get() {
        Some(store) => store,
        None => init_default_telemetry_store(),
    }
}

/// Initializes the globally shared access log store at bootstrap.
pub fn init_global_telemetry_store(store: Arc<TelemetryStore>) -> Result<(), Arc<TelemetryStore>> {
    GLOBAL_TELEMETRY_STORE.set(store)
}
