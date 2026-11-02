//! Central Lock-Free In-Memory Metrics Store.
//!
//! Organizes all 36 pipeline combinations into cacheline-aligned, per-core sharded
//! memory arrays (Shared-Nothing Architecture) with direct register-computed $\mathcal{O}(1)$ slot offsets.
//!
//! # Hardware Invariants
//! - Shared-Nothing Per-Core Sharding: Each worker core writes exclusively to its own dedicated
//!   local slot shard, completely eliminating cross-core cacheline invalidations (MESI bus traffic)
//!   even when 128+ CPU cores process identical routes simultaneously.
//! - 256-Bit Hardware Vector Aggregation: When exporting snapshots (`/metrics`), metric counters
//!   across all active core shards are reduced concurrently using AVX2/NEON vector instructions.

use std::cell::Cell;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use crate::global::GlobalMetrics;
use crate::pool::PoolMetrics;
use crate::slot::{
    IDX_BYTES_RX, IDX_BYTES_TX, IDX_DUR_1MS, IDX_DUR_5MS, IDX_DUR_10MS, IDX_DUR_50MS,
    IDX_DUR_250MS, IDX_DUR_1000MS, IDX_DUR_INF, IDX_REQ_2XX, IDX_REQ_3XX, IDX_REQ_4XX, IDX_REQ_5XX,
    IDX_REQ_OTHER, IDX_TTFB_1MS, IDX_TTFB_5MS, IDX_TTFB_10MS, IDX_TTFB_50MS, IDX_TTFB_250MS,
    IDX_TTFB_INF, PipelineSlot,
};
use crate::snapshot::{GlobalSnapshot, MetricsSnapshot, PipelineSnapshot};
use crate::types::{DownstreamProto, PipeStrategy, StreamCompletion, UpstreamProto};
use crate::vector::PIPELINE_COUNTERS_LEN;

/// Maximum number of hardware worker core shards supported.
pub const MAX_METRIC_WORKERS: usize = 128;

thread_local! {
    static THREAD_METRIC_LANE: Cell<Option<usize>> = const { Cell::new(None) };
}
static LANE_SEQUENCE: AtomicUsize = AtomicUsize::new(0);

/// Sets the explicit zero-based metrics lane for the calling thread.
#[inline]
pub fn set_current_worker_lane(lane: usize) {
    THREAD_METRIC_LANE.with(|cell| cell.set(Some(lane)));
}

/// Returns the zero-based metrics lane for the calling thread.
/// Automatically self-allocates an incremental lane index if unconfigured.
#[inline]
pub fn current_worker_lane() -> usize {
    THREAD_METRIC_LANE.with(|cell| match cell.get() {
        Some(lane) => lane,
        None => {
            let lane = LANE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
            cell.set(Some(lane));
            lane
        }
    })
}

/// Total number of distinct pipeline combinations:
/// 3 (Downstream) * 3 (Upstream) * 4 (PipeStrategy) = 36 slots.
pub const TOTAL_PIPELINE_SLOTS: usize =
    DownstreamProto::COUNT * UpstreamProto::COUNT * PipeStrategy::COUNT;

/// Contiguous, lock-free metrics store housing all pipeline, global, and pool metrics.
pub struct MetricsStore {
    core_slots: Vec<[PipelineSlot; TOTAL_PIPELINE_SLOTS]>,
    active_cores: usize,
    global: GlobalMetrics,
    pool: PoolMetrics,
}

impl Default for MetricsStore {
    fn default() -> Self {
        Self::new()
    }
}

impl MetricsStore {
    /// Creates a newly initialized metrics store sharded across detected hardware CPU cores.
    pub fn new() -> Self {
        let detected_cores = velda_core::global_hardware_topology().cpu.available_cores;
        let active_cores = detected_cores.clamp(1, MAX_METRIC_WORKERS);
        let mut core_slots = Vec::with_capacity(active_cores);
        for _ in 0..active_cores {
            core_slots.push(std::array::from_fn(|_| PipelineSlot::new()));
        }

        Self {
            core_slots,
            active_cores,
            global: GlobalMetrics::new(),
            pool: PoolMetrics::new(),
        }
    }

    /// Creates a metrics store explicitly bounded to `active_cores` shards.
    pub fn with_cores(active_cores: usize) -> Self {
        let cores = active_cores.clamp(1, MAX_METRIC_WORKERS);
        let mut core_slots = Vec::with_capacity(cores);
        for _ in 0..cores {
            core_slots.push(std::array::from_fn(|_| PipelineSlot::new()));
        }

        Self {
            core_slots,
            active_cores: cores,
            global: GlobalMetrics::new(),
            pool: PoolMetrics::new(),
        }
    }

    /// Returns the active number of sharded core lanes.
    #[inline(always)]
    pub fn active_cores(&self) -> usize {
        self.active_cores
    }

    /// Computes the exact array index for a given pipeline combination.
    ///
    /// # Performance
    /// Pure arithmetic calculation directly on CPU registers:
    /// `(downstream * 12) + (upstream * 4) + pipe`
    /// Zero bounds checking failure risk, zero hashing, zero allocations.
    #[inline(always)]
    pub const fn slot_index(
        downstream: DownstreamProto,
        upstream: UpstreamProto,
        pipe: PipeStrategy,
    ) -> usize {
        (downstream as usize) * (UpstreamProto::COUNT * PipeStrategy::COUNT)
            + (upstream as usize) * PipeStrategy::COUNT
            + (pipe as usize)
    }

    /// Decodes a slot index back to its constituent enum components.
    #[inline(always)]
    pub const fn decode_slot_index(index: usize) -> (DownstreamProto, UpstreamProto, PipeStrategy) {
        let pipe_idx = (index % PipeStrategy::COUNT) as u8;
        let rem = index / PipeStrategy::COUNT;
        let up_idx = (rem % UpstreamProto::COUNT) as u8;
        let down_idx = (rem / UpstreamProto::COUNT) as u8;

        let down = match DownstreamProto::from_discriminant(down_idx) {
            Some(d) => d,
            None => DownstreamProto::H1,
        };
        let up = match UpstreamProto::from_discriminant(up_idx) {
            Some(u) => u,
            None => UpstreamProto::H1,
        };
        let pipe = match PipeStrategy::from_discriminant(pipe_idx) {
            Some(p) => p,
            None => PipeStrategy::Buffered,
        };

        (down, up, pipe)
    }

    /// Returns a reference to the specific pipeline slot bound to a specific worker core.
    #[inline(always)]
    pub fn slot_on_core(
        &self,
        core_id: usize,
        downstream: DownstreamProto,
        upstream: UpstreamProto,
        pipe: PipeStrategy,
    ) -> &PipelineSlot {
        let core_idx = core_id % self.active_cores;
        let idx = Self::slot_index(downstream, upstream, pipe);
        // SAFETY: slot_index is mathematically in [0, 36) and core_idx is in [0, active_cores).
        unsafe { self.core_slots.get_unchecked(core_idx).get_unchecked(idx) }
    }

    /// Returns a reference to the pipeline slot assigned to the current thread's core lane.
    #[inline(always)]
    pub fn slot(
        &self,
        downstream: DownstreamProto,
        upstream: UpstreamProto,
        pipe: PipeStrategy,
    ) -> &PipelineSlot {
        self.slot_on_core(current_worker_lane(), downstream, upstream, pipe)
    }

    /// Returns a reference to the global L4 and transport metrics.
    #[inline(always)]
    pub fn global(&self) -> &GlobalMetrics {
        &self.global
    }

    /// Returns a reference to the upstream connection pool metrics.
    #[inline(always)]
    pub fn pool(&self) -> &PoolMetrics {
        &self.pool
    }

    /// Records stream start across the designated pipeline slot on the current thread's core lane.
    #[inline(always)]
    pub fn record_stream_start(
        &self,
        downstream: DownstreamProto,
        upstream: UpstreamProto,
        pipe: PipeStrategy,
    ) {
        self.slot(downstream, upstream, pipe).stream_started();
    }

    /// Records stream start across the designated pipeline slot on an explicit worker core.
    #[inline(always)]
    pub fn record_stream_start_on_core(
        &self,
        core_id: usize,
        downstream: DownstreamProto,
        upstream: UpstreamProto,
        pipe: PipeStrategy,
    ) {
        self.slot_on_core(core_id, downstream, upstream, pipe)
            .stream_started();
    }

    /// Records stream completion across the designated pipeline slot on the current thread's core lane.
    #[inline(always)]
    pub fn record_stream_completion(
        &self,
        downstream: DownstreamProto,
        upstream: UpstreamProto,
        pipe: PipeStrategy,
        completion: StreamCompletion,
    ) {
        let target_slot = self.slot(downstream, upstream, pipe);
        target_slot.stream_ended();
        target_slot.record_completion(&completion);
    }

    /// Records stream completion across the designated pipeline slot on an explicit worker core.
    #[inline(always)]
    pub fn record_stream_completion_on_core(
        &self,
        core_id: usize,
        downstream: DownstreamProto,
        upstream: UpstreamProto,
        pipe: PipeStrategy,
        completion: StreamCompletion,
    ) {
        let target_slot = self.slot_on_core(core_id, downstream, upstream, pipe);
        target_slot.stream_ended();
        target_slot.record_completion(&completion);
    }

    /// Captures a point-in-time pure snapshot of all metrics across all sharded core lanes.
    ///
    /// Non-blocking, reads atomics with `Ordering::Relaxed` and aggregates counter slices
    /// using 256-bit SIMD / SWAR vector additions (`add_counters_256`).
    pub fn snapshot(&self) -> MetricsSnapshot {
        let mut pipelines = Vec::with_capacity(TOTAL_PIPELINE_SLOTS);

        for slot_idx in 0..TOTAL_PIPELINE_SLOTS {
            let (down, up, pipe) = Self::decode_slot_index(slot_idx);

            let mut aggregated = [0u64; PIPELINE_COUNTERS_LEN];
            let mut active_streams = 0i64;

            for core_shard in &self.core_slots {
                let slot = &core_shard[slot_idx];
                active_streams += slot.active_streams.load(Ordering::Relaxed);

                let mut core_buf = [0u64; PIPELINE_COUNTERS_LEN];
                slot.load_counters(&mut core_buf);

                crate::vector::add_counters_256(&mut aggregated, &core_buf);
            }

            // Latency Distribution Buckets (Cumulative transformation outside traffic hot path)
            let d0 = aggregated[IDX_DUR_1MS];
            let d1 = aggregated[IDX_DUR_5MS];
            let d2 = aggregated[IDX_DUR_10MS];
            let d3 = aggregated[IDX_DUR_50MS];
            let d4 = aggregated[IDX_DUR_250MS];
            let d5 = aggregated[IDX_DUR_1000MS];
            let d6 = aggregated[IDX_DUR_INF];

            let dur_1 = d0;
            let dur_5 = dur_1.saturating_add(d1);
            let dur_10 = dur_5.saturating_add(d2);
            let dur_50 = dur_10.saturating_add(d3);
            let dur_250 = dur_50.saturating_add(d4);
            let dur_1000 = dur_250.saturating_add(d5);
            let dur_inf = d6;

            // Time to First Byte (TTFB) Buckets (Cumulative transformation)
            let t0 = aggregated[IDX_TTFB_1MS];
            let t1 = aggregated[IDX_TTFB_5MS];
            let t2 = aggregated[IDX_TTFB_10MS];
            let t3 = aggregated[IDX_TTFB_50MS];
            let t4 = aggregated[IDX_TTFB_250MS];
            let t5 = aggregated[IDX_TTFB_INF];

            let ttfb_1 = t0;
            let ttfb_5 = ttfb_1.saturating_add(t1);
            let ttfb_10 = ttfb_5.saturating_add(t2);
            let ttfb_50 = ttfb_10.saturating_add(t3);
            let ttfb_250 = ttfb_50.saturating_add(t4);
            let ttfb_inf = t5;

            pipelines.push(PipelineSnapshot {
                downstream: down,
                upstream: up,
                pipe,
                requests_2xx: aggregated[IDX_REQ_2XX],
                requests_3xx: aggregated[IDX_REQ_3XX],
                requests_4xx: aggregated[IDX_REQ_4XX],
                requests_5xx: aggregated[IDX_REQ_5XX],
                requests_other: aggregated[IDX_REQ_OTHER],
                active_streams,
                bytes_rx: aggregated[IDX_BYTES_RX],
                bytes_tx: aggregated[IDX_BYTES_TX],
                duration_le_1ms: dur_1,
                duration_le_5ms: dur_5,
                duration_le_10ms: dur_10,
                duration_le_50ms: dur_50,
                duration_le_250ms: dur_250,
                duration_le_1000ms: dur_1000,
                duration_gt_1000ms: dur_inf,
                ttfb_le_1ms: ttfb_1,
                ttfb_le_5ms: ttfb_5,
                ttfb_le_10ms: ttfb_10,
                ttfb_le_50ms: ttfb_50,
                ttfb_le_250ms: ttfb_250,
                ttfb_gt_250ms: ttfb_inf,
            });
        }

        let global = GlobalSnapshot {
            connections_total: self.global.connections_total.load(Ordering::Relaxed),
            connections_active: self.global.connections_active.load(Ordering::Relaxed),
            connections_rejected: self.global.connections_rejected.load(Ordering::Relaxed),
            tls_handshakes_total: self.global.tls_handshakes_total.load(Ordering::Relaxed),
            tls_handshake_errors: self.global.tls_handshake_errors.load(Ordering::Relaxed),
            h2_rst_stream_total: self.global.h2_rst_stream_total.load(Ordering::Relaxed),
            config_reloads_total: self.global.config_reloads_total.load(Ordering::Relaxed),
            config_reload_errors: self.global.config_reload_errors.load(Ordering::Relaxed),
        };

        MetricsSnapshot {
            captured_at: Instant::now(),
            pipelines,
            global,
            pool: self.pool.snapshot(),
        }
    }
}

static GLOBAL_METRICS: std::sync::OnceLock<std::sync::Arc<MetricsStore>> =
    std::sync::OnceLock::new();

/// Returns the globally shared in-memory metrics store.
pub fn global_metrics() -> std::sync::Arc<MetricsStore> {
    GLOBAL_METRICS
        .get_or_init(|| std::sync::Arc::new(MetricsStore::new()))
        .clone()
}

/// Initializes the globally shared in-memory metrics store during cold-start bootstrap.
pub fn init_global_metrics(
    metrics: std::sync::Arc<MetricsStore>,
) -> Result<(), std::sync::Arc<MetricsStore>> {
    GLOBAL_METRICS.set(metrics)
}
