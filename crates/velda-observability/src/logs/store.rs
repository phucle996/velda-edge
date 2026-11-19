//! Multi-Exporter Fan-Out Access Log Store (`velda_observability::logs::store`).
//!
//! ```text
//!  ┌────────────────────────────────────────────────────────────────────────────────────────┐
//!  │                       LOGSTORE MULTI-EXPORTER ARCHITECTURE                             │
//!  │                                                                                        │
//!  │    Frontline Core 0      Frontline Core 1                Frontline Core N-1            │
//!  │   ┌─────────────────┐   ┌─────────────────┐             ┌─────────────────┐            │
//!  │   │ SpscLogRing (0) │   │ SpscLogRing (1) │   ... ...   │ SpscLogRing(N-1)│            │
//!  │   └────────┬────────┘   └────────┬────────┘             └────────┬────────┘            │
//!  │            │                     │                               │                     │
//!  │            │ flush_lane(0)       │ flush_lane(1)                 │ flush_lane(N-1)     │
//!  │            ▼                     ▼                               ▼                     │
//!  │  ┌──────────────────────────────────────────────────────────────────────────────────┐  │
//!  │  │                      Shadow Logistics Worker (SCHED_IDLE)                        │  │
//!  │  │                                                                                  │  │
//!  │  │  1. acquire_slot() ──► LogBatchPool (32 Slots × 64KB)                            │  │
//!  │  │  2. ring.drain_batch_into(&mut slot.records) (VMOVNTDQ / AVX streaming store)    │  │
//!  │  │  3. slot.leases.store(K)                                                         │  │
//!  │  │  4. LogBatch::from_pooled(slot_id, slice, pool)                                  │  │
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
//!  │    drop(LogBatch)               drop(LogBatch)               drop(LogBatch)            │
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
//!   returned to `LogBatchPool` without memory reallocation.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use crate::metrics::{MAX_METRIC_WORKERS, current_worker_lane};

use super::pool::{DEFAULT_BATCH_CAPACITY, DEFAULT_POOL_SLOTS, LogBatch, LogBatchPool};
use super::record::AccessLogRecord;
use super::ring::{DEFAULT_CORE_RING_CAPACITY, SpscLogRing};

struct SubscriberInner {
    id: u64,
    max_batches: usize,
    queue: Mutex<VecDeque<LogBatch>>,
    pub dropped_batches: AtomicU64,
}

/// RAII handle held by an active background log exporter plugin.
///
/// Dropping a received [`LogBatch`] automatically recycles its contiguous memory
/// pages back into the pool without calling the OS allocator (`__rust_dealloc == 0`).
/// Dropping `LogSubscriber` itself unregisters the exporter from [`LogStore`].
pub struct LogSubscriber {
    inner: Arc<SubscriberInner>,
    store: Arc<LogStore>,
}

impl LogSubscriber {
    /// Pops the next batch of [`AccessLogRecord`]s if available.
    pub fn try_recv_batch(&self) -> Option<LogBatch> {
        self.inner.queue.lock().unwrap().pop_front()
    }

    /// Drains up to `max_batches` currently queued batches for bulk export.
    pub fn drain_batches(&self, max_batches: usize) -> Vec<LogBatch> {
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
}

impl Drop for LogSubscriber {
    fn drop(&mut self) {
        let mut subs = self.store.subscribers.lock().unwrap();
        subs.retain(|s| s.id != self.inner.id);
        self.store
            .active_subscriber_count
            .store(subs.len(), Ordering::Release);
    }
}

/// Central Lock-Free Access Log Store owned by `velda-observability`.
pub struct LogStore {
    pub shards: Vec<SpscLogRing>,
    pub active_cores: usize,
    subscribers: Mutex<Vec<Arc<SubscriberInner>>>,
    pub active_subscriber_count: AtomicUsize,
    next_subscriber_id: AtomicU64,
    pub emitted_total: AtomicU64,
    pub batch_pool: Arc<LogBatchPool>,
}

impl Default for LogStore {
    fn default() -> Self {
        Self::new()
    }
}

impl LogStore {
    pub fn new() -> Self {
        let detected_cores = velda_core::hardware::global_hardware_topology()
            .cpu
            .available_cores;
        Self::with_cores_and_capacity(detected_cores, DEFAULT_CORE_RING_CAPACITY)
    }

    pub fn with_cores_and_capacity(cores: usize, ring_capacity: usize) -> Self {
        let active_cores = cores.clamp(1, MAX_METRIC_WORKERS);
        let mut shards = Vec::with_capacity(active_cores);
        for _ in 0..active_cores {
            shards.push(SpscLogRing::new(ring_capacity));
        }

        Self {
            shards,
            active_cores,
            subscribers: Mutex::new(Vec::new()),
            active_subscriber_count: AtomicUsize::new(0),
            next_subscriber_id: AtomicU64::new(1),
            emitted_total: AtomicU64::new(0),
            batch_pool: LogBatchPool::new(DEFAULT_POOL_SLOTS, DEFAULT_BATCH_CAPACITY),
        }
    }

    /// Emits a 64-byte [`AccessLogRecord`] onto the calling thread's core-local SPSC ring.
    #[inline]
    pub fn emit(&self, record: &AccessLogRecord) -> bool {
        self.emit_on_core(current_worker_lane(), record)
    }

    /// Emits a 64-byte [`AccessLogRecord`] onto an explicit worker core's SPSC ring.
    #[inline]
    pub fn emit_on_core(&self, core_id: usize, record: &AccessLogRecord) -> bool {
        let idx = core_id % self.active_cores;
        let ok = unsafe { self.shards.get_unchecked(idx).push(record) };
        if ok {
            self.emitted_total.fetch_add(1, Ordering::Relaxed);
        }
        ok
    }

    /// Registers a new background exporter plugin subscriber.
    pub fn subscribe(self: &Arc<Self>, max_batches: usize) -> LogSubscriber {
        let id = self.next_subscriber_id.fetch_add(1, Ordering::Relaxed);
        let inner = Arc::new(SubscriberInner {
            id,
            max_batches: max_batches.max(16),
            queue: Mutex::new(VecDeque::with_capacity(max_batches.min(256))),
            dropped_batches: AtomicU64::new(0),
        });

        let mut subs = self.subscribers.lock().unwrap();
        subs.push(Arc::clone(&inner));
        self.active_subscriber_count
            .store(subs.len(), Ordering::Release);
        drop(subs);

        LogSubscriber {
            inner,
            store: Arc::clone(self),
        }
    }

    /// Executed by Shadow Worker (`velda-shadow-{lane}`) to drain its core-local SPSC ring
    /// and fan out zero-allocation [`LogBatch`] buffers to active exporters.
    ///
    /// Uses pre-allocated contiguous memory pages from [`LogBatchPool`], recycling memory
    /// in $\mathcal{O}(1)$ upon drop without returning pages to the OS allocator.
    pub fn flush_lane(&self, lane: usize, max_batch_size: usize) -> usize {
        let idx = lane % self.active_cores;
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
            LogBatch::from_pooled(
                slot_id,
                records_vec.as_slice(),
                Arc::clone(&self.batch_pool),
            )
        } else {
            let records = ring.drain_batch(max_batch_size.max(64));
            if records.is_empty() {
                return 0;
            }
            LogBatch::from_arc(Arc::from(records))
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

    pub fn ring_dropped_total(&self) -> u64 {
        self.shards
            .iter()
            .map(|s| s.dropped.load(Ordering::Relaxed))
            .sum()
    }
}

static GLOBAL_LOG_STORE: OnceLock<Arc<LogStore>> = OnceLock::new();

/// Returns the globally shared in-memory access log store.
#[inline]
pub fn global_log_store() -> Arc<LogStore> {
    GLOBAL_LOG_STORE
        .get_or_init(|| Arc::new(LogStore::new()))
        .clone()
}

/// Initializes the globally shared access log store at bootstrap.
pub fn init_global_log_store(store: Arc<LogStore>) -> Result<(), Arc<LogStore>> {
    GLOBAL_LOG_STORE.set(store)
}
