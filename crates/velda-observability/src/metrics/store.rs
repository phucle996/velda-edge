//! Central Lock-Free In-Memory Metrics Store (`velda_observability::metrics::store`).
//!
//! ```text
//! ┌────────────────────────────────────────────────────────────────────────────────────────┐
//! │                         METRICS STORE ARCHITECTURAL TOPOLOGY                           │
//! ├────────────────────────────────────────────────────────────────────────────────────────┤
//! │ SHARED-NOTHING PER-CORE MEMORY SHARDING:                                               │
//! │                                                                                        │
//! │  Worker Core 0               Worker Core 1               Worker Core C-1               │
//! │ ┌──────────────────────────┐┌──────────────────────────┐┌──────────────────────────┐   │
//! │ │ PipeSlot[0..64] (192B)   ││ PipeSlot[0..64] (192B)   ││ PipeSlot[0..64] (192B)   │   │
//! │ │ Relaxed Atomic Store     ││ Relaxed Atomic Store     ││ Relaxed Atomic Store     │   │
//! │ │ Zero Cross-Core Coherence││ Zero Cross-Core Coherence││ Zero Cross-Core Coherence│   │
//! │ └─────────────┬────────────┘└─────────────┬────────────┘└─────────────┬────────────┘   │
//! │               │                           │                           │                │
//! │               └───────────────────────────┼───────────────────────────┘                │
//! │                                           ▼                                            │
//! │                            SNAPSHOT SIMD REDUCTION LOOP:                               │
//! │                            Traverses core shards along memory layout                   │
//! │                            add_pipeline_counters (AVX2 / NEON)                         │
//! │                                           │                                            │
//! │                                           ▼                                            │
//! │                              [ MetricsSnapshot Object ]                                │
//! │                              Point-in-time immutable export                            │
//! └────────────────────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! Organizes all 64 pipeline combinations into cacheline-aligned, per-core sharded
//! memory arrays (Shared-Nothing Architecture) with direct register-computed $\mathcal{O}(1)$ slot offsets.
//!
//! # Hardware Invariants
//! - Shared-Nothing Per-Core Sharding: Each worker core writes exclusively to its own dedicated
//!   local slot shard, completely eliminating cross-core cacheline invalidations (MESI bus traffic)
//!   even when 128+ CPU cores process identical routes simultaneously.
//! - Hardware Vector Aggregation: When exporting snapshots (`/metrics`), metric counters
//!   across all active core shards are reduced concurrently via direct hardware SIMD vector
//!   accumulation (AVX2 256-bit -> SSE2/NEON 128-bit -> Scalar 64-bit).

use std::sync::atomic::Ordering;

use velda_core::lut::PipeKind;

use crate::worker_tls::{WORKER_TLS, init_auto_lane};

use super::counter::{PIPELINE_COUNTERS_LEN, add_pipeline_counters};
use super::global::GlobalMetrics;
use super::pool::PoolMetrics;
use super::slot::{
    IDX_BYTES_RX, IDX_BYTES_TX, IDX_DUR_1MS, IDX_DUR_5MS, IDX_DUR_10MS, IDX_DUR_50MS,
    IDX_DUR_250MS, IDX_DUR_1000MS, IDX_DUR_INF, IDX_REQ_2XX, IDX_REQ_3XX, IDX_REQ_4XX, IDX_REQ_5XX,
    IDX_REQ_OTHER, IDX_TTFB_1MS, IDX_TTFB_5MS, IDX_TTFB_10MS, IDX_TTFB_50MS, IDX_TTFB_250MS,
    IDX_TTFB_INF, PipelineSlot,
};
use super::snapshot::{GlobalSnapshot, MetricsSnapshot, PipelineSnapshot};
use super::types::StreamCompletion;

/// Maximum number of hardware worker core shards supported.
pub const MAX_METRIC_WORKERS: usize = 128;

/// Sets the explicit zero-based metrics lane for the calling thread.
#[inline]
pub fn set_current_worker_lane(lane: usize) {
    WORKER_TLS.with(|tls| {
        tls.lane.set(lane);
        tls.lane_set.set(true);
    });
}

/// Returns the zero-based metrics lane for the calling thread.
/// Automatically self-allocates an incremental lane index if unconfigured.
#[inline(always)]
pub fn current_worker_lane() -> usize {
    WORKER_TLS.with(|tls| {
        if tls.lane_set.get() {
            tls.lane.get()
        } else {
            init_auto_lane(tls)
        }
    })
}

/// Total number of distinct pipeline combinations: exactly 64 slots matching canonical [`PipeKind::COUNT`].
pub const TOTAL_PIPELINE_SLOTS: usize = PipeKind::COUNT;

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
        let detected_cores = velda_core::hardware::global_hardware_topology()
            .cpu
            .available_cores;
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

    /// Converts a [`PipeKind`] variant directly to its integer array index.
    #[inline(always)]
    pub const fn slot_index(pipe: PipeKind) -> usize {
        pipe as usize
    }

    /// Decodes a slot index back to its [`PipeKind`] enum variant.
    #[inline(always)]
    pub const fn decode_slot_index(index: usize) -> Option<PipeKind> {
        PipeKind::from_u8(index as u8)
    }

    /// Returns a reference to the specific pipeline slot bound to a specific worker core.
    #[inline(always)]
    pub fn slot_on_core(&self, core_id: usize, pipe: PipeKind) -> &PipelineSlot {
        let core_idx = core_id % self.active_cores;
        let idx = pipe as usize;
        // SAFETY: pipe as usize is in [0, 64) and core_idx is in [0, active_cores).
        unsafe { self.core_slots.get_unchecked(core_idx).get_unchecked(idx) }
    }

    /// Returns a reference to the pipeline slot assigned to the current thread's core lane.
    #[inline(always)]
    pub fn slot(&self, pipe: PipeKind) -> &PipelineSlot {
        self.slot_on_core(current_worker_lane(), pipe)
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
    pub fn record_stream_start(&self, pipe: PipeKind) {
        self.slot(pipe).stream_started();
    }

    /// Records stream start across the designated pipeline slot on an explicit worker core.
    #[inline(always)]
    pub fn record_stream_start_on_core(&self, core_id: usize, pipe: PipeKind) {
        self.slot_on_core(core_id, pipe).stream_started();
    }

    /// Records stream completion across the designated pipeline slot on the current thread's core lane.
    #[inline(always)]
    pub fn record_stream_completion(&self, pipe: PipeKind, completion: StreamCompletion) {
        let target_slot = self.slot(pipe);
        target_slot.stream_ended();
        target_slot.record_completion(&completion);
    }

    /// Records an early stream rejection/error that terminated before `record_stream_start`.
    ///
    /// Preserves request status, transferred bytes, and latency metrics without decrementing
    /// `active_streams` (preventing negative active-stream gauge drift on early 4xx/5xx exits).
    #[inline(always)]
    pub fn record_early_completion(&self, pipe: PipeKind, completion: StreamCompletion) {
        self.slot(pipe).record_completion(&completion);
    }

    /// Records stream completion across the designated pipeline slot on an explicit worker core.
    #[inline(always)]
    pub fn record_stream_completion_on_core(
        &self,
        core_id: usize,
        pipe: PipeKind,
        completion: StreamCompletion,
    ) {
        let target_slot = self.slot_on_core(core_id, pipe);
        target_slot.stream_ended();
        target_slot.record_completion(&completion);
    }

    /// Captures a point-in-time pure snapshot of all metrics across all sharded core lanes.
    ///
    /// Non-blocking, reads atomics with `Ordering::Relaxed` and aggregates counter slices
    /// linearly across memory using SIMD acceleration.
    ///
    /// # Performance
    /// Memory-linear physical scan: outer loop iterates over `core_slots`, inner loop iterates
    /// over contiguous pipeline slots, eliminating cacheline churn across cores.
    pub fn snapshot(&self) -> MetricsSnapshot {
        let mut slot_totals = [[0u64; PIPELINE_COUNTERS_LEN]; TOTAL_PIPELINE_SLOTS];
        let mut active_streams = [0i64; TOTAL_PIPELINE_SLOTS];

        for core_shard in &self.core_slots {
            for pipe_idx in 0..TOTAL_PIPELINE_SLOTS {
                let slot = &core_shard[pipe_idx];
                active_streams[pipe_idx] += slot.active_streams.load(Ordering::Relaxed);
                let mut temp = [0u64; PIPELINE_COUNTERS_LEN];
                slot.load_counters(&mut temp);
                add_pipeline_counters(&mut slot_totals[pipe_idx], &temp);
            }
        }

        let mut pipelines = Vec::with_capacity(TOTAL_PIPELINE_SLOTS);
        for &pipe in &PipeKind::ALL {
            let pipe_idx = pipe as usize;
            let aggregated = &slot_totals[pipe_idx];

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
                pipe,
                requests_2xx: aggregated[IDX_REQ_2XX],
                requests_3xx: aggregated[IDX_REQ_3XX],
                requests_4xx: aggregated[IDX_REQ_4XX],
                requests_5xx: aggregated[IDX_REQ_5XX],
                requests_other: aggregated[IDX_REQ_OTHER],
                active_streams: active_streams[pipe_idx],
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
            captured_at_nanos: velda_core::clock::now_unix_nanos(),
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
