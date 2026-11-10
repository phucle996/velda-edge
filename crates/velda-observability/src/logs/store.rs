//! Reference-Counted Multi-Exporter Access Log Store (`velda_observability::logs::store`).
//!
//! # Mechanism & Rationale
//! Shadow Workers drain their core-local [`SpscLogRing`] into an immutable `Arc<[AccessLogRecord]>`
//! slice. If $K$ background exporter plugins are subscribed, each receives an `Arc` clone
//! (reference count = $K$). As each exporter finishes serializing and drops its `Arc`, the atomic
//! counter decrements; when the last exporter drops its reference, Rust automatically deallocates
//! the batch buffer with zero contention and zero torn reads.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use crate::metrics::{MAX_METRIC_WORKERS, current_worker_lane};

use super::record::AccessLogRecord;
use super::ring::{DEFAULT_CORE_RING_CAPACITY, SpscLogRing};

struct SubscriberInner {
    id: u64,
    max_batches: usize,
    queue: Mutex<VecDeque<Arc<[AccessLogRecord]>>>,
    pub dropped_batches: AtomicU64,
}

/// RAII handle held by an active background log exporter plugin.
///
/// Dropping a received `Arc<[AccessLogRecord]>` batch decrements its reference counter;
/// dropping `LogSubscriber` itself unregisters the exporter from [`LogStore`].
pub struct LogSubscriber {
    inner: Arc<SubscriberInner>,
    store: Arc<LogStore>,
}

impl LogSubscriber {
    /// Pops the next reference-counted batch of [`AccessLogRecord`]s if available.
    pub fn try_recv_batch(&self) -> Option<Arc<[AccessLogRecord]>> {
        self.inner.queue.lock().unwrap().pop_front()
    }

    /// Drains up to `max_batches` currently queued batches for bulk export.
    pub fn drain_batches(&self, max_batches: usize) -> Vec<Arc<[AccessLogRecord]>> {
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
}

impl Default for LogStore {
    fn default() -> Self {
        Self::new()
    }
}

impl LogStore {
    pub fn new() -> Self {
        let detected_cores = velda_core::global_hardware_topology().cpu.available_cores;
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
    /// and fan out reference-counted `Arc<[AccessLogRecord]>` batches to active exporters.
    ///
    /// If zero exporters are registered, discards queued records in $\mathcal{O}(1)$ without
    /// reading slots or allocating memory.
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
        // Convert Vec<AccessLogRecord> into reference-counted Arc<[AccessLogRecord]> once.
        // Each active exporter receives an 8-byte Arc clone incrementing the strong count.
        let shared_batch: Arc<[AccessLogRecord]> = Arc::from(records);

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
