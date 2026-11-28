//! Multi-Exporter Fan-Out Telemetry Store (`velda_observability::telemetry::store`).
//!
//! ```text
//!  ┌────────────────────────────────────────────────────────────────────────────────────────┐
//!  │                    TELEMETRYSTORE MULTI-EXPORTER ARCHITECTURE                          │
//!  │                                                                                        │
//!  │    Frontline Core 0      Frontline Core 1                Frontline Core N-1            │
//!  │   ┌─────────────────┐   ┌─────────────────┐             ┌─────────────────┐            │
//!  │   │SpscRing[0] (2MB)│   │SpscRing[1] (2MB)│   ... ...   │SpscRing[N-1](2M)│            │
//!  │   └────────┬────────┘   └────────┬────────┘             └────────┬────────┘            │
//!  │            │ flush_lane_budget   │ flush_lane_budget             │                     │
//!  │            ▼ (100ms or empty/32) ▼                               ▼                     │
//!  │  ┌──────────────────────────────────────────────────────────────────────────────────┐  │
//!  │  │            Sharded TelemetryBatchPool -> SubPool[0..N-1] (64 slots × AtomicU64) │  │
//!  │  │  1. acquire_slot_for_lane(c) -> TZCNT(!bitmap[c]) + CAS 0->1 (strict core-local) │  │
//!  │  │  2. drain_batch_into_with_summary() -> L3/RAM stream + 64B TelemetryBatchSlot   │  │
//!  │  │  3. leases.store(K, Release) -> Fan-out & wake subscribers if queue >= 32       │  │
//!  │  └───────────────────────────────────┬──────────────────────────────────────────────┘  │
//!  │                                      │                                                 │
//!  │                        Fan-Out to K Active Subscribers                                 │
//!  │         ┌────────────────────────────┼────────────────────────────┐                    │
//!  │         ▼                            ▼                            ▼                    │
//!  │  ┌──────────────┐             ┌──────────────┐             ┌──────────────┐            │
//!  │  │ Subscriber 1 │             │ Subscriber 2 │             │ Subscriber K │            │
//!  │  │ Wakes on:    │             │ Wakes on:    │             │ Wakes on:    │            │
//!  │  │ >= 32 batches│             │ >= 32 batches│             │ >= 32 batches│            │
//!  │  │ OR interval  │             │ OR interval  │             │ OR interval  │            │
//!  │  └──────┬───────┘             └──────┬───────┘             └──────┬───────┘            │
//!  │         └────────────────────────────┼────────────────────────────┘                    │
//!  │                                      ▼ (last drop -> leases == 0)                      │
//!  │                       ┌──────────────────────────────┐                                 │
//!  │                       │ records.clear()              │                                 │
//!  │                       │ sub_pool.bitmap bit 1 -> 0   │                                 │
//!  │                       └──────────────────────────────┘                                 │
//!  └────────────────────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Mechanics
//! - **Core Isolation**: Frontline request workers push records into core-local SPSC
//!   rings (`_mm256_stream_si256` directly to L3/RAM) without cross-core locks.
//! - **Sharded Bitmask Rotation**: Each core owns a dedicated 64-slot `SubPool` with a
//!   cache-line-aligned `AtomicU64` bitmap (`1` = full/in-flight, `0` = free). Wait-free
//!   acquisition touches only the local core's bitmap, isolating core-skew saturation.
//! - **Dual-Trigger Delivery**: Shadow Workers drain up to 32 batches per tick, and
//!   subscribers wake immediately when `>= 32` batches queue up (or on periodic timer).

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::task::{Context, Poll, Waker};

use crate::metrics::{MAX_METRIC_WORKERS, current_worker_lane};

use super::pool::{SUBPOOL_BATCHES, TelemetryBatch, TelemetryBatchPool};
use super::record::TelemetryRecord;
use super::ring::{DEFAULT_CORE_RING_CAPACITY, SpscTelemetryRing};

/// Queue depth threshold (batches) that wakes waiting background exporters early.
pub const DEFAULT_BATCH_TRIGGER_THRESHOLD: usize = 32;

struct SubscriberQueueState {
    queue: VecDeque<TelemetryBatch>,
    waker: Option<Waker>,
}

struct SubscriberInner {
    id: u64,
    max_batches: usize,
    trigger_batches: usize,
    state: Mutex<SubscriberQueueState>,
    pub dropped_batches: AtomicU64,
}

/// Future resolved as soon as a [`TelemetrySubscriber`] accumulates at least `threshold` batches.
pub struct BatchThresholdFuture<'a> {
    inner: &'a SubscriberInner,
    threshold: usize,
}

impl Future for BatchThresholdFuture<'_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self.inner.state.lock().unwrap();
        if state.queue.len() >= self.threshold {
            state.waker = None;
            Poll::Ready(())
        } else {
            state.waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

/// RAII handle held by an active background log or trace exporter plugin.
///
/// Dropping a received [`TelemetryBatch`] automatically recycles its contiguous memory
/// pages back into its owning [`super::pool::SubPool`] bitmap (`1 -> 0`) without calling
/// the OS allocator (`__rust_dealloc == 0`). Dropping `TelemetrySubscriber` unregisters it.
pub struct TelemetrySubscriber {
    inner: Arc<SubscriberInner>,
    store: Arc<TelemetryStore>,
}

impl TelemetrySubscriber {
    /// Pops the next batch of [`TelemetryRecord`]s if available.
    pub fn try_recv_batch(&self) -> Option<TelemetryBatch> {
        self.inner.state.lock().unwrap().queue.pop_front()
    }

    /// Drains up to `max_batches` currently queued batches for bulk export.
    pub fn drain_batches(&self, max_batches: usize) -> Vec<TelemetryBatch> {
        let mut guard = self.inner.state.lock().unwrap();
        let take = guard.queue.len().min(max_batches);
        let mut out = Vec::with_capacity(take);
        for _ in 0..take {
            if let Some(b) = guard.queue.pop_front() {
                out.push(b);
            }
        }
        out
    }

    /// Current number of batches waiting in this subscriber's queue.
    #[inline]
    pub fn queued_batches(&self) -> usize {
        self.inner.state.lock().unwrap().queue.len()
    }

    /// Configured batch-count trigger threshold (`min(32, max_batches)`).
    #[inline]
    pub fn trigger_threshold(&self) -> usize {
        self.inner.trigger_batches
    }

    /// Returns a future that resolves once at least `trigger_threshold()` batches are queued.
    #[inline]
    pub fn wait_for_batch_threshold(&self) -> BatchThresholdFuture<'_> {
        BatchThresholdFuture {
            inner: &self.inner,
            threshold: self.inner.trigger_batches,
        }
    }

    /// Returns a future that resolves once at least `threshold` batches (clamped to queue cap) queue up.
    #[inline]
    pub fn wait_for_batches(&self, threshold: usize) -> BatchThresholdFuture<'_> {
        BatchThresholdFuture {
            inner: &self.inner,
            threshold: threshold.clamp(1, self.inner.max_batches),
        }
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
        let topo = velda_core::hardware::global_hardware_topology();
        Self::with_hardware(
            topo.cpu.available_cores,
            DEFAULT_CORE_RING_CAPACITY,
            topo.memory.total_bytes,
        )
    }

    /// Creates a store for explicit core count and per-core ring capacity.
    pub fn with_cores_and_capacity(cores: usize, ring_capacity: usize) -> Self {
        Self::with_hardware(cores, ring_capacity, 0)
    }

    /// Creates a store sized continuously from `(cores, memory_bytes)` hardware profile.
    pub fn with_hardware(cores: usize, ring_capacity: usize, memory_bytes: usize) -> Self {
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
            batch_pool: TelemetryBatchPool::for_hardware(active_cores, memory_bytes),
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
    /// Caps per-subscriber queue depth at half of total rotating pool capacity (minimum
    /// `SUBPOOL_BATCHES / 2 = 32` batches) so a slow or stalled exporter drops its oldest
    /// unconsumed leases before starving healthy exporters or filling all core subpools.
    pub fn subscribe(self: &Arc<Self>, max_batches: usize) -> TelemetrySubscriber {
        let id = self.next_subscriber_id.fetch_add(1, Ordering::Relaxed);
        let pool_cap = (self.batch_pool.total_slots() / 2).clamp(SUBPOOL_BATCHES / 2, 512);
        let capped = max_batches.clamp(4, pool_cap);
        let trigger_batches = DEFAULT_BATCH_TRIGGER_THRESHOLD.min(capped);
        let inner = Arc::new(SubscriberInner {
            id,
            max_batches: capped,
            trigger_batches,
            state: Mutex::new(SubscriberQueueState {
                queue: VecDeque::with_capacity(capped),
                waker: None,
            }),
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

    /// Drains one batch from core `lane`'s SPSC ring using a subpool slot (or heap fallback)
    /// and distributes read-only leases across active subscribers.
    pub fn flush_lane(&self, lane: usize, max_batch_size: usize) -> usize {
        let idx = lane & self.shard_mask;
        let ring = unsafe { self.shards.get_unchecked(idx) };

        if self.active_subscriber_count.load(Ordering::Acquire) == 0 {
            return ring.discard_all();
        }

        let subs_guard = self.subscribers.lock().unwrap();
        if subs_guard.is_empty() {
            return ring.discard_all();
        }
        let subs: Vec<Arc<SubscriberInner>> = subs_guard.iter().cloned().collect();
        drop(subs_guard);

        let batch = if let Some(token) = self.batch_pool.acquire_slot_for_lane(idx) {
            let slot = self.batch_pool.slot(token.subpool_idx, token.slot_bit);
            let records_vec = unsafe { &mut *slot.records.get() };
            let (count, summary) = ring.drain_batch_into_with_summary(records_vec, max_batch_size);
            if count == 0 {
                self.batch_pool
                    .release_slot(token.subpool_idx, token.slot_bit);
                return 0;
            }
            unsafe {
                slot.write_summary(summary);
            }
            slot.leases.store(1, Ordering::Release);
            TelemetryBatch::from_pooled(token, records_vec.as_slice(), Arc::clone(&self.batch_pool))
        } else {
            let (records, summary) = ring.drain_batch_with_summary(max_batch_size.max(64));
            if records.is_empty() {
                return 0;
            }
            TelemetryBatch::from_arc_with_summary(Arc::from(records), summary)
        };

        let count = batch.len();
        let active_count = subs.len();
        let mut batch_opt = Some(batch);
        let mut wakers_to_wake: Vec<Waker> = Vec::new();

        for (i, sub) in subs.iter().enumerate() {
            let item = if i + 1 == active_count {
                batch_opt.take().unwrap()
            } else {
                batch_opt.as_ref().unwrap().clone()
            };
            let wake = {
                let mut state = sub.state.lock().unwrap();
                if state.queue.len() >= sub.max_batches {
                    state.queue.pop_front();
                    sub.dropped_batches.fetch_add(1, Ordering::Relaxed);
                }
                state.queue.push_back(item);
                if state.queue.len() >= sub.trigger_batches {
                    state.waker.take()
                } else {
                    None
                }
            };
            if let Some(w) = wake {
                wakers_to_wake.push(w);
            }
        }

        for w in wakers_to_wake {
            w.wake();
        }

        count
    }

    /// Drains up to `max_batches` batches in sequence from `lane` until empty or partial batch.
    pub fn flush_lane_budget(
        &self,
        lane: usize,
        max_batch_size: usize,
        max_batches: usize,
    ) -> usize {
        let mut total = 0;
        let batch_target = max_batch_size.min(self.batch_pool.batch_capacity);
        for _ in 0..max_batches.max(1) {
            let drained = self.flush_lane(lane, max_batch_size);
            if drained == 0 {
                break;
            }
            total += drained;
            if drained < batch_target {
                break;
            }
        }
        total
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

/// Initializes the globally shared telemetry store at bootstrap.
pub fn init_global_telemetry_store(store: Arc<TelemetryStore>) -> Result<(), Arc<TelemetryStore>> {
    GLOBAL_TELEMETRY_STORE.set(store)
}
