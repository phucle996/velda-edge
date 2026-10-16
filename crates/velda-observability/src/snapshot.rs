//! Point-in-Time Pure In-Memory Metrics Snapshots.
//!
//! Provides flat, non-atomic data snapshots representing point-in-time values
//! captured from the atomic metrics store.
//!
//! # Architectural Invariants
//! - Pure In-Memory Types: These structs contain ONLY standard primitive integers (`u64`, `i64`)
//!   and enums. They have zero awareness of external protocols, serializers, or Prometheus.
//! - Non-blocking Read: Snapshots are captured by reading atomics with `Relaxed` ordering,
//!   guaranteeing that snapshot generation never blocks or stalls hot-path writers.

use std::time::Instant;

use crate::types::{DownstreamProto, PipeStrategy, UpstreamProto};

/// Point-in-time snapshot of metrics for a single pipeline combination.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PipelineSnapshot {
    /// Downstream ingress protocol.
    pub downstream: DownstreamProto,
    /// Upstream egress protocol.
    pub upstream: UpstreamProto,
    /// Payload streaming strategy.
    pub pipe: PipeStrategy,

    /// Total 2xx success requests.
    pub requests_2xx: u64,
    /// Total 3xx redirection requests.
    pub requests_3xx: u64,
    /// Total 4xx client error requests.
    pub requests_4xx: u64,
    /// Total 5xx server error requests.
    pub requests_5xx: u64,
    /// Total non-standard requests.
    pub requests_other: u64,

    /// Number of streams actively in-flight at snapshot time.
    pub active_streams: i64,

    /// Total payload bytes received.
    pub bytes_rx: u64,
    /// Total payload bytes transmitted.
    pub bytes_tx: u64,

    // Latency Distribution Buckets
    pub duration_le_1ms: u64,
    pub duration_le_5ms: u64,
    pub duration_le_10ms: u64,
    pub duration_le_50ms: u64,
    pub duration_le_250ms: u64,
    pub duration_le_1000ms: u64,
    pub duration_gt_1000ms: u64,

    // TTFB Buckets
    pub ttfb_le_1ms: u64,
    pub ttfb_le_5ms: u64,
    pub ttfb_le_10ms: u64,
    pub ttfb_le_50ms: u64,
    pub ttfb_le_250ms: u64,
    pub ttfb_gt_250ms: u64,
}

impl PipelineSnapshot {
    /// Computes total requests served across all status code classes.
    #[inline(always)]
    pub fn total_requests(&self) -> u64 {
        self.requests_2xx
            .saturating_add(self.requests_3xx)
            .saturating_add(self.requests_4xx)
            .saturating_add(self.requests_5xx)
            .saturating_add(self.requests_other)
    }

    /// Computes the ratio of successful requests (2xx / total).
    ///
    /// Returns 1.0 (100%) if no requests have been recorded yet.
    #[inline]
    pub fn success_rate(&self) -> f64 {
        let total = self.total_requests();
        if total == 0 {
            1.0
        } else {
            self.requests_2xx as f64 / total as f64
        }
    }
}

/// Point-in-time snapshot of global L4 connection and transport events.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GlobalSnapshot {
    /// Total established L4 connections.
    pub connections_total: u64,
    /// Active L4 connections at snapshot time.
    pub connections_active: i64,
    /// Total rejected L4 connections.
    pub connections_rejected: u64,

    /// Total successful TLS handshakes.
    pub tls_handshakes_total: u64,
    /// Total failed TLS handshakes.
    pub tls_handshake_errors: u64,

    /// Total HTTP/2 RST_STREAM frames.
    pub h2_rst_stream_total: u64,

    /// Total configuration reload cycles.
    pub config_reloads_total: u64,
    /// Failed configuration reload cycles.
    pub config_reload_errors: u64,
}

/// Complete point-in-time snapshot of the entire metrics store.
#[derive(Debug, Clone)]
pub struct MetricsSnapshot {
    /// Monotonic timestamp when this snapshot was captured.
    pub captured_at: Instant,
    /// Snapshots for all 36 pipeline combinations.
    pub pipelines: Vec<PipelineSnapshot>,
    /// Global L4 and transport snapshot.
    pub global: GlobalSnapshot,
}

impl MetricsSnapshot {
    /// Computes the sum of all requests served across all 36 pipelines.
    #[inline]
    pub fn total_requests(&self) -> u64 {
        self.pipelines.iter().map(|p| p.total_requests()).sum()
    }

    /// Computes the total number of in-flight active streams across all pipelines.
    #[inline]
    pub fn total_active_streams(&self) -> i64 {
        self.pipelines.iter().map(|p| p.active_streams).sum()
    }

    /// Finds a specific pipeline snapshot by its coordinates.
    pub fn find_pipeline(
        &self,
        downstream: DownstreamProto,
        upstream: UpstreamProto,
        pipe: PipeStrategy,
    ) -> Option<&PipelineSnapshot> {
        self.pipelines
            .iter()
            .find(|p| p.downstream == downstream && p.upstream == upstream && p.pipe == pipe)
    }
}
