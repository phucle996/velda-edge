//! Central Lock-Free In-Memory Metrics Store.
//!
//! Organizes all 36 pipeline combinations into a contiguous, cacheline-aligned array
//! with direct register-computed $\mathcal{O}(1)$ slot offsets.

use std::sync::atomic::Ordering;
use std::time::Instant;

use crate::global::GlobalMetrics;
use crate::pool::PoolMetrics;
use crate::slot::PipelineSlot;
use crate::snapshot::{GlobalSnapshot, MetricsSnapshot, PipelineSnapshot};
use crate::types::{DownstreamProto, PipeStrategy, StreamCompletion, UpstreamProto};

/// Total number of distinct pipeline combinations:
/// 3 (Downstream) * 3 (Upstream) * 4 (PipeStrategy) = 36 slots.
pub const TOTAL_PIPELINE_SLOTS: usize =
    DownstreamProto::COUNT * UpstreamProto::COUNT * PipeStrategy::COUNT;

/// Contiguous, lock-free metrics store housing all pipeline, global, and pool metrics.
pub struct MetricsStore {
    slots: [PipelineSlot; TOTAL_PIPELINE_SLOTS],
    global: GlobalMetrics,
    pool: PoolMetrics,
}

impl Default for MetricsStore {
    fn default() -> Self {
        Self::new()
    }
}

impl MetricsStore {
    /// Creates a newly initialized, zeroed metrics store.
    pub fn new() -> Self {
        Self {
            slots: std::array::from_fn(|_| PipelineSlot::new()),
            global: GlobalMetrics::new(),
            pool: PoolMetrics::new(),
        }
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

    /// Returns a reference to the specific pipeline slot.
    #[inline(always)]
    pub fn slot(
        &self,
        downstream: DownstreamProto,
        upstream: UpstreamProto,
        pipe: PipeStrategy,
    ) -> &PipelineSlot {
        let idx = Self::slot_index(downstream, upstream, pipe);
        // SAFETY: slot_index is mathematically guaranteed to be in range [0, 36).
        unsafe { self.slots.get_unchecked(idx) }
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

    /// Records stream start across the designated pipeline slot.
    #[inline(always)]
    pub fn record_stream_start(
        &self,
        downstream: DownstreamProto,
        upstream: UpstreamProto,
        pipe: PipeStrategy,
    ) {
        self.slot(downstream, upstream, pipe).stream_started();
    }

    /// Records stream completion across the designated pipeline slot.
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

    /// Captures a point-in-time pure snapshot of all metrics.
    ///
    /// Non-blocking, reads atomics with `Ordering::Relaxed`.
    pub fn snapshot(&self) -> MetricsSnapshot {
        let mut pipelines = Vec::with_capacity(TOTAL_PIPELINE_SLOTS);

        for (idx, slot) in self.slots.iter().enumerate() {
            let (down, up, pipe) = Self::decode_slot_index(idx);

            // Compute cumulative duration histogram outside the traffic hot path
            let d0 = slot.duration_b0_le_1ms.load(Ordering::Relaxed);
            let d1 = slot.duration_b1_le_5ms.load(Ordering::Relaxed);
            let d2 = slot.duration_b2_le_10ms.load(Ordering::Relaxed);
            let d3 = slot.duration_b3_le_50ms.load(Ordering::Relaxed);
            let d4 = slot.duration_b4_le_250ms.load(Ordering::Relaxed);
            let d5 = slot.duration_b5_le_1000ms.load(Ordering::Relaxed);
            let d6 = slot.duration_b6_gt_1000ms.load(Ordering::Relaxed);

            let dur_1 = d0;
            let dur_5 = dur_1.saturating_add(d1);
            let dur_10 = dur_5.saturating_add(d2);
            let dur_50 = dur_10.saturating_add(d3);
            let dur_250 = dur_50.saturating_add(d4);
            let dur_1000 = dur_250.saturating_add(d5);
            let dur_inf = d6;

            // Compute cumulative TTFB histogram outside the traffic hot path
            let t0 = slot.ttfb_b0_le_1ms.load(Ordering::Relaxed);
            let t1 = slot.ttfb_b1_le_5ms.load(Ordering::Relaxed);
            let t2 = slot.ttfb_b2_le_10ms.load(Ordering::Relaxed);
            let t3 = slot.ttfb_b3_le_50ms.load(Ordering::Relaxed);
            let t4 = slot.ttfb_b4_le_250ms.load(Ordering::Relaxed);
            let t5 = slot.ttfb_b5_gt_250ms.load(Ordering::Relaxed);

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
                requests_2xx: slot.requests_2xx.load(Ordering::Relaxed),
                requests_3xx: slot.requests_3xx.load(Ordering::Relaxed),
                requests_4xx: slot.requests_4xx.load(Ordering::Relaxed),
                requests_5xx: slot.requests_5xx.load(Ordering::Relaxed),
                requests_other: slot.requests_other.load(Ordering::Relaxed),
                active_streams: slot.active_streams.load(Ordering::Relaxed),
                bytes_rx: slot.bytes_rx.load(Ordering::Relaxed),
                bytes_tx: slot.bytes_tx.load(Ordering::Relaxed),
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
