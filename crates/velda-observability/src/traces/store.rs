//! Reference-Counted Multi-Exporter Trace Span Store (`velda_observability::traces::store`).
//!
//! ```text
//!                          Shadow Worker Drains SpscSpanRing
//!                                          │
//!                                          ▼
//!                        Create Batch: Arc<[SpanRecord]> (Init Ref = 1)
//!                                          │
//!                  ┌───────────────────────┴───────────────────────┐
//!                  ▼ (Iterate subscribers: K active)               ▼ (K = 0 subscribers)
//!     ┌───────────────────────────┐                         Loop runs 0 times
//!     │ Exporter 1: Clone (Ref+1) │                                │
//!     │ Exporter 2: Clone (Ref+1) │                                │
//!     └─────────────┬─────────────┘                                │
//!                   │                                              │
//!       shared_batch drops at exit (-1)                            │
//!                   │                                              ▼
//!         Ref Count = K                                shared_batch drops at exit (-1)
//!                   │                                              │
//!    ┌──────────────┴──────────────┐                               ▼
//!    ▼                             ▼                      Ref Count: 1 - 1 = 0
//!  Exporter 1 drops (-1)   Exporter 2 drops (-1)                   │
//!                   │                                              ▼
//!                   ▼                                    Deallocate (Zero Subscribers)
//!         Ref Count: K -> 0                                        │
//!                   │                                              ▼
//!                   ▼                                    Memory returned to allocator
//!     Deallocate (All Exporters Done)
//! ```

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use crate::metrics::{MAX_METRIC_WORKERS, current_worker_lane};

use super::record::SpanRecord;
use super::ring::{DEFAULT_TRACE_CORE_RING_CAPACITY, SpscSpanRing};

struct TraceSubscriberInner {
    id: u64,
    max_batches: usize,
    queue: Mutex<VecDeque<Arc<[SpanRecord]>>>,
    pub dropped_batches: AtomicU64,
}

/// RAII handle held by an active background trace exporter plugin.
///
/// Dropping a received `Arc<[SpanRecord]>` batch decrements its reference counter;
/// dropping `TraceSubscriber` itself unregisters the exporter from [`TraceStore`].
pub struct TraceSubscriber {
    inner: Arc<TraceSubscriberInner>,
    store: Arc<TraceStore>,
}

impl TraceSubscriber {
    /// Drains up to `max_batches` currently queued batches for bulk export.
    pub fn drain_batches(&self, max_batches: usize) -> Vec<Arc<[SpanRecord]>> {
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

impl Drop for TraceSubscriber {
    fn drop(&mut self) {
        let mut subs = self.store.subscribers.lock().unwrap();
        subs.retain(|s| s.id != self.inner.id);
        self.store
            .active_subscriber_count
            .store(subs.len(), Ordering::Release);
    }
}

/// Central Lock-Free Trace Span Store owned by `velda-observability`.
pub struct TraceStore {
    pub shards: Vec<SpscSpanRing>,
    pub active_cores: usize,
    subscribers: Mutex<Vec<Arc<TraceSubscriberInner>>>,
    pub active_subscriber_count: AtomicUsize,
    next_subscriber_id: AtomicU64,
    pub emitted_total: AtomicU64,
}

impl Default for TraceStore {
    fn default() -> Self {
        Self::new()
    }
}

impl TraceStore {
    pub fn new() -> Self {
        let detected_cores = velda_core::hardware::global_hardware_topology()
            .cpu
            .available_cores;
        Self::with_cores_and_capacity(detected_cores, DEFAULT_TRACE_CORE_RING_CAPACITY)
    }

    pub fn with_cores_and_capacity(cores: usize, ring_capacity: usize) -> Self {
        let active_cores = cores.clamp(1, MAX_METRIC_WORKERS);
        let mut shards = Vec::with_capacity(active_cores);
        for _ in 0..active_cores {
            shards.push(SpscSpanRing::new(ring_capacity));
        }

        Self {
            shards,
            active_cores,
            subscribers: Mutex::new(Vec::new()),
            active_subscriber_count: AtomicUsize::new(0),
            next_subscriber_id: AtomicU64::new(1),
            emitted_total: AtomicU64::new(0),
        }
    }

    /// Emits a 64-byte [`SpanRecord`] onto the calling thread's core-local SPSC ring.
    #[inline]
    pub fn emit(&self, record: &SpanRecord) -> bool {
        self.emit_on_core(current_worker_lane(), record)
    }

    /// Emits a 64-byte [`SpanRecord`] onto an explicit worker core's SPSC ring.
    #[inline]
    pub fn emit_on_core(&self, core_id: usize, record: &SpanRecord) -> bool {
        let idx = core_id % self.active_cores;
        let ok = unsafe { self.shards.get_unchecked(idx).push(record) };
        if ok {
            self.emitted_total.fetch_add(1, Ordering::Relaxed);
        }
        ok
    }

    /// Registers a new background exporter plugin subscriber.
    pub fn subscribe(self: &Arc<Self>, max_batches: usize) -> TraceSubscriber {
        let id = self.next_subscriber_id.fetch_add(1, Ordering::Relaxed);
        let inner = Arc::new(TraceSubscriberInner {
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

        TraceSubscriber {
            inner,
            store: Arc::clone(self),
        }
    }

    /// Executed by Shadow Worker (`velda-shadow-{lane}`) to drain its core-local SPSC ring
    /// and fan out reference-counted `Arc<[SpanRecord]>` batches to active exporters.
    ///
    /// Natural Reference-Counting:
    /// - `shared_batch` is created as `Arc<[SpanRecord]>`.
    /// - If $K$ subscribers are present, each gets an `Arc::clone`.
    /// - At function exit, `shared_batch` drops. If $K = 0$, strong count hits 0 and memory is freed immediately.
    /// - If $K > 0$, as each exporter drains and drops its batch, strong count decrements to 0 and frees memory.
    pub fn flush_lane(&self, lane: usize, max_batch_size: usize) -> usize {
        let idx = lane % self.active_cores;
        let ring = unsafe { self.shards.get_unchecked(idx) };

        if self.active_subscriber_count.load(Ordering::Acquire) == 0 {
            return ring.discard_all();
        }

        let records = ring.drain_batch(max_batch_size.max(64));
        if records.is_empty() {
            return 0;
        }

        let count = records.len();
        let shared_batch: Arc<[SpanRecord]> = Arc::from(records);

        let subs = self.subscribers.lock().unwrap();
        for sub in subs.iter() {
            let mut q = sub.queue.lock().unwrap();
            if q.len() >= sub.max_batches {
                q.pop_front();
                sub.dropped_batches.fetch_add(1, Ordering::Relaxed);
            }
            q.push_back(Arc::clone(&shared_batch));
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
}

static GLOBAL_TRACE_STORE: OnceLock<Arc<TraceStore>> = OnceLock::new();

/// Returns reference to global singleton [`TraceStore`].
pub fn global_trace_store() -> &'static Arc<TraceStore> {
    GLOBAL_TRACE_STORE.get_or_init(|| Arc::new(TraceStore::new()))
}
