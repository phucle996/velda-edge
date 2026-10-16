//! Central Lock-Free In-Memory Metrics Store.
//!
//! Organizes all 36 pipeline combinations into a contiguous, cacheline-aligned array
//! with direct register-computed $\mathcal{O}(1)$ slot offsets.

use std::sync::atomic::Ordering;
use std::time::Instant;

use crate::global::GlobalMetrics;
use crate::slot::PipelineSlot;
use crate::snapshot::{GlobalSnapshot, MetricsSnapshot, PipelineSnapshot};
use crate::types::{DownstreamProto, PipeStrategy, StreamCompletion, UpstreamProto};

/// Total number of distinct pipeline combinations:
/// 3 (Downstream) * 3 (Upstream) * 4 (PipeStrategy) = 36 slots.
pub const TOTAL_PIPELINE_SLOTS: usize =
    DownstreamProto::COUNT * UpstreamProto::COUNT * PipeStrategy::COUNT;

/// Contiguous, lock-free metrics store housing all pipeline and global metrics.
pub struct MetricsStore {
    slots: [PipelineSlot; TOTAL_PIPELINE_SLOTS],
    global: GlobalMetrics,
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
                duration_le_1ms: slot.duration_le_1ms.load(Ordering::Relaxed),
                duration_le_5ms: slot.duration_le_5ms.load(Ordering::Relaxed),
                duration_le_10ms: slot.duration_le_10ms.load(Ordering::Relaxed),
                duration_le_50ms: slot.duration_le_50ms.load(Ordering::Relaxed),
                duration_le_250ms: slot.duration_le_250ms.load(Ordering::Relaxed),
                duration_le_1000ms: slot.duration_le_1000ms.load(Ordering::Relaxed),
                duration_gt_1000ms: slot.duration_gt_1000ms.load(Ordering::Relaxed),
                ttfb_le_1ms: slot.ttfb_le_1ms.load(Ordering::Relaxed),
                ttfb_le_5ms: slot.ttfb_le_5ms.load(Ordering::Relaxed),
                ttfb_le_10ms: slot.ttfb_le_10ms.load(Ordering::Relaxed),
                ttfb_le_50ms: slot.ttfb_le_50ms.load(Ordering::Relaxed),
                ttfb_le_250ms: slot.ttfb_le_250ms.load(Ordering::Relaxed),
                ttfb_gt_250ms: slot.ttfb_gt_250ms.load(Ordering::Relaxed),
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
        }
    }
}
