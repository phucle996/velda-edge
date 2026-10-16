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

/// Point-in-time snapshot of connection pool telemetry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PoolSnapshot {
    /// Total number of active SubPool key containers resident in memory.
    pub pool_containers_total: u64,
    /// Number of sequential (H1) connections currently idle in LIFO queues.
    pub connections_idle: u64,
    /// Number of sequential (H1) connections actively checked out and in-use.
    pub connections_leased: i64,
    /// Number of multiplexed (H2/H3) connections actively serving streams.
    pub connections_multiplexed: u64,
    /// Total concurrent streams currently active across all multiplexed connections.
    pub multiplexed_active_streams: i64,

    /// Number of successful pool acquisitions reusing an existing warm connection.
    pub lease_hits_total: u64,
    /// Number of acquisitions requiring establishment of a fresh TCP/TLS socket.
    pub lease_misses_total: u64,
    /// Number of acquisitions that timed out waiting for connection availability.
    pub lease_timeouts_total: u64,
    /// Number of connection leases returned and accepted back into the pool.
    pub lease_releases_total: u64,

    /// Cumulative count of physical upstream connections created since startup.
    pub connections_created_total: u64,
    /// Evictions caused by idle timeout expiration.
    pub evictions_idle_timeout: u64,
    /// Evictions caused by reaching maximum connection lifetime.
    pub evictions_max_lifetime: u64,
    /// Evictions triggered by an upstream HTTP/2 GOAWAY frame.
    pub evictions_goaway: u64,
    /// Evictions caused by socket errors or failed health checks.
    pub evictions_unhealthy: u64,
    /// Evictions caused by subpool exceeding maximum allowed idle capacity.
    pub evictions_capacity_exceeded: u64,

    // Lifespan Cumulative Buckets
    pub lifespan_le_5s: u64,
    pub lifespan_le_30s: u64,
    pub lifespan_le_5m: u64,
    pub lifespan_le_30m: u64,
    pub lifespan_le_1h: u64,
    pub lifespan_gt_1h: u64,
}

impl PoolSnapshot {
    /// Computes the total number of lease acquisition attempts (hits + misses + timeouts).
    #[inline(always)]
    pub fn total_lease_attempts(&self) -> u64 {
        self.lease_hits_total
            .saturating_add(self.lease_misses_total)
            .saturating_add(self.lease_timeouts_total)
    }

    /// Computes the connection pool hit rate (hits / (hits + misses)).
    ///
    /// Returns 1.0 (100%) if no lease acquisitions have been recorded yet.
    #[inline]
    pub fn hit_rate(&self) -> f64 {
        let attempts = self
            .lease_hits_total
            .saturating_add(self.lease_misses_total);
        if attempts == 0 {
            1.0
        } else {
            self.lease_hits_total as f64 / attempts as f64
        }
    }

    /// Computes the total number of connection evictions across all reasons.
    #[inline(always)]
    pub fn total_evictions(&self) -> u64 {
        self.evictions_idle_timeout
            .saturating_add(self.evictions_max_lifetime)
            .saturating_add(self.evictions_goaway)
            .saturating_add(self.evictions_unhealthy)
            .saturating_add(self.evictions_capacity_exceeded)
    }

    /// Computes the total number of closed connections whose lifespan was measured.
    ///
    /// Corresponds to the `+Inf` bucket in cumulative Prometheus histograms.
    #[inline(always)]
    pub fn total_lifespan_recorded(&self) -> u64 {
        self.lifespan_le_1h.saturating_add(self.lifespan_gt_1h)
    }

    /// Computes the average number of active streams multiplexed per connection.
    ///
    /// Returns 0.0 if no multiplexed connections are active.
    #[inline]
    pub fn avg_streams_per_mux_conn(&self) -> f64 {
        if self.connections_multiplexed == 0 {
            0.0
        } else {
            self.multiplexed_active_streams.max(0) as f64 / self.connections_multiplexed as f64
        }
    }
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
    /// Upstream connection pool snapshot ("The Soul of Velda").
    pub pool: PoolSnapshot,
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
