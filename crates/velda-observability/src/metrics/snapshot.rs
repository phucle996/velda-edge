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

use super::types::{DownstreamProtocol, PipeStrategy, UpstreamProtocol};
use super::vector::{PIPELINE_COUNTERS_LEN, PipelineMetricsTotals};

/// Point-in-time snapshot of metrics for a single pipeline combination.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PipelineSnapshot {
    /// Downstream ingress protocol.
    pub downstream: DownstreamProtocol,
    /// Upstream egress protocol.
    pub upstream: UpstreamProtocol,
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
    /// Converts pipeline metric counters into a 20-counter array.
    #[inline(always)]
    pub fn to_counters(&self) -> [u64; PIPELINE_COUNTERS_LEN] {
        [
            self.requests_2xx,
            self.requests_3xx,
            self.requests_4xx,
            self.requests_5xx,
            self.requests_other,
            self.bytes_rx,
            self.bytes_tx,
            self.duration_le_1ms,
            self.duration_le_5ms,
            self.duration_le_10ms,
            self.duration_le_50ms,
            self.duration_le_250ms,
            self.duration_le_1000ms,
            self.duration_gt_1000ms,
            self.ttfb_le_1ms,
            self.ttfb_le_5ms,
            self.ttfb_le_10ms,
            self.ttfb_le_50ms,
            self.ttfb_le_250ms,
            self.ttfb_gt_250ms,
        ]
    }

    /// Computes total requests served across all status code classes.
    #[inline(always)]
    pub fn total_requests(&self) -> u64 {
        self.requests_2xx
            .saturating_add(self.requests_3xx)
            .saturating_add(self.requests_4xx)
            .saturating_add(self.requests_5xx)
            .saturating_add(self.requests_other)
    }

    /// Computes the ratio of successful requests (2xx / total) in basis points (1 bp = 0.01%, 10,000 = 100.00%).
    ///
    /// Operates purely in integer arithmetic (zero FPU instructions, zero float conversions).
    /// Returns 10,000 (100.00%) if no requests have been recorded yet.
    #[inline]
    pub fn success_rate_basis_points(&self) -> u32 {
        let total = self.total_requests();
        if total == 0 {
            10_000
        } else {
            ((self.requests_2xx as u128 * 10_000) / total as u128) as u32
        }
    }

    /// Computes the ratio of successful requests (2xx / total).
    ///
    /// Returns 1.0 (100%) if no requests have been recorded yet.
    #[inline]
    pub fn success_rate(&self) -> f64 {
        self.success_rate_basis_points() as f64 / 10_000.0
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

    /// Computes the connection pool hit rate (hits / (hits + misses)) in basis points (1 bp = 0.01%, 10,000 = 100.00%).
    ///
    /// Operates purely in integer arithmetic.
    /// Returns 10,000 (100.00%) if no lease acquisitions have been recorded yet.
    #[inline]
    pub fn hit_rate_basis_points(&self) -> u32 {
        let attempts = self
            .lease_hits_total
            .saturating_add(self.lease_misses_total);
        if attempts == 0 {
            10_000
        } else {
            ((self.lease_hits_total as u128 * 10_000) / attempts as u128) as u32
        }
    }

    /// Computes the connection pool hit rate (hits / (hits + misses)).
    ///
    /// Returns 1.0 (100%) if no lease acquisitions have been recorded yet.
    #[inline]
    pub fn hit_rate(&self) -> f64 {
        self.hit_rate_basis_points() as f64 / 10_000.0
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

    /// Computes the average number of active streams multiplexed per connection, scaled by 1,000 (millistreams).
    ///
    /// For example, 2.5 streams per connection returns 2,500.
    /// Operates purely in integer arithmetic.
    #[inline]
    pub fn avg_streams_per_mux_conn_scaled(&self) -> u32 {
        if self.connections_multiplexed == 0 {
            0
        } else {
            let active = self.multiplexed_active_streams.max(0) as u128;
            ((active * 1_000) / self.connections_multiplexed as u128) as u32
        }
    }

    /// Computes the average number of active streams multiplexed per connection.
    ///
    /// Returns 0.0 if no multiplexed connections are active.
    #[inline]
    pub fn avg_streams_per_mux_conn(&self) -> f64 {
        self.avg_streams_per_mux_conn_scaled() as f64 / 1_000.0
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
    /// Computes aggregate totals across all pipelines using vectorized SIMD integer arithmetic.
    #[inline]
    pub fn aggregate_pipelines(&self) -> PipelineMetricsTotals {
        let mut total_counters = [0u64; PIPELINE_COUNTERS_LEN];
        for p in &self.pipelines {
            let counters = p.to_counters();
            super::vector::add_counters(&mut total_counters, &counters);
        }
        PipelineMetricsTotals::from_counters(&total_counters)
    }

    /// Computes aggregate totals across all pipelines using an explicit function pointer.
    #[inline]
    pub fn aggregate_pipelines_with(
        &self,
        agg_fn: super::vector::AccumulateCountersFn,
    ) -> PipelineMetricsTotals {
        let mut total_counters = [0u64; PIPELINE_COUNTERS_LEN];
        for p in &self.pipelines {
            let counters = p.to_counters();
            agg_fn(&mut total_counters, &counters);
        }
        PipelineMetricsTotals::from_counters(&total_counters)
    }

    /// Computes the sum of all requests served across all 36 pipelines.
    #[inline]
    pub fn total_requests(&self) -> u64 {
        self.aggregate_pipelines().total_requests()
    }

    /// Computes the total number of in-flight active streams across all pipelines.
    #[inline]
    pub fn total_active_streams(&self) -> i64 {
        self.pipelines.iter().map(|p| p.active_streams).sum()
    }

    /// Finds a specific pipeline snapshot by its coordinates.
    pub fn find_pipeline(
        &self,
        downstream: DownstreamProtocol,
        upstream: UpstreamProtocol,
        pipe: PipeStrategy,
    ) -> Option<&PipelineSnapshot> {
        self.pipelines
            .iter()
            .find(|p| p.downstream == downstream && p.upstream == upstream && p.pipe == pipe)
    }

    /// Creates a zero-initialized snapshot covering all 36 pipeline coordinate slots.
    pub fn zero() -> Self {
        let mut pipelines = Vec::with_capacity(super::store::TOTAL_PIPELINE_SLOTS);
        for slot_idx in 0..super::store::TOTAL_PIPELINE_SLOTS {
            let (downstream, upstream, pipe) =
                super::store::MetricsStore::decode_slot_index(slot_idx);
            pipelines.push(PipelineSnapshot {
                downstream,
                upstream,
                pipe,
                requests_2xx: 0,
                requests_3xx: 0,
                requests_4xx: 0,
                requests_5xx: 0,
                requests_other: 0,
                active_streams: 0,
                bytes_rx: 0,
                bytes_tx: 0,
                duration_le_1ms: 0,
                duration_le_5ms: 0,
                duration_le_10ms: 0,
                duration_le_50ms: 0,
                duration_le_250ms: 0,
                duration_le_1000ms: 0,
                duration_gt_1000ms: 0,
                ttfb_le_1ms: 0,
                ttfb_le_5ms: 0,
                ttfb_le_10ms: 0,
                ttfb_le_50ms: 0,
                ttfb_le_250ms: 0,
                ttfb_gt_250ms: 0,
            });
        }
        Self {
            captured_at: Instant::now(),
            pipelines,
            global: GlobalSnapshot {
                connections_total: 0,
                connections_active: 0,
                connections_rejected: 0,
                tls_handshakes_total: 0,
                tls_handshake_errors: 0,
                h2_rst_stream_total: 0,
                config_reloads_total: 0,
                config_reload_errors: 0,
            },
            pool: PoolSnapshot {
                pool_containers_total: 0,
                connections_idle: 0,
                connections_leased: 0,
                connections_multiplexed: 0,
                multiplexed_active_streams: 0,
                lease_hits_total: 0,
                lease_misses_total: 0,
                lease_timeouts_total: 0,
                lease_releases_total: 0,
                connections_created_total: 0,
                evictions_idle_timeout: 0,
                evictions_max_lifetime: 0,
                evictions_goaway: 0,
                evictions_unhealthy: 0,
                evictions_capacity_exceeded: 0,
                lifespan_le_5s: 0,
                lifespan_le_30s: 0,
                lifespan_le_5m: 0,
                lifespan_le_30m: 0,
                lifespan_le_1h: 0,
                lifespan_gt_1h: 0,
            },
        }
    }

    /// Computes an interval delta snapshot against an earlier snapshot (`self - prev`).
    ///
    /// Monotonic cumulative counters are subtracted (`saturating_sub`) so delta-based sinks
    /// like StatsD (`|c`) receive only the increment since `prev`, while instantaneous gauges
    /// (`|g`) preserve their current point-in-time values from `self`.
    pub fn delta_since(&self, prev: &Self) -> Self {
        let pipelines = self
            .pipelines
            .iter()
            .enumerate()
            .map(|(i, curr)| {
                let p = prev.pipelines.get(i).filter(|p| {
                    p.downstream == curr.downstream
                        && p.upstream == curr.upstream
                        && p.pipe == curr.pipe
                });
                match p {
                    Some(prev_p) => PipelineSnapshot {
                        downstream: curr.downstream,
                        upstream: curr.upstream,
                        pipe: curr.pipe,
                        requests_2xx: curr.requests_2xx.saturating_sub(prev_p.requests_2xx),
                        requests_3xx: curr.requests_3xx.saturating_sub(prev_p.requests_3xx),
                        requests_4xx: curr.requests_4xx.saturating_sub(prev_p.requests_4xx),
                        requests_5xx: curr.requests_5xx.saturating_sub(prev_p.requests_5xx),
                        requests_other: curr.requests_other.saturating_sub(prev_p.requests_other),
                        active_streams: curr.active_streams,
                        bytes_rx: curr.bytes_rx.saturating_sub(prev_p.bytes_rx),
                        bytes_tx: curr.bytes_tx.saturating_sub(prev_p.bytes_tx),
                        duration_le_1ms: curr
                            .duration_le_1ms
                            .saturating_sub(prev_p.duration_le_1ms),
                        duration_le_5ms: curr
                            .duration_le_5ms
                            .saturating_sub(prev_p.duration_le_5ms),
                        duration_le_10ms: curr
                            .duration_le_10ms
                            .saturating_sub(prev_p.duration_le_10ms),
                        duration_le_50ms: curr
                            .duration_le_50ms
                            .saturating_sub(prev_p.duration_le_50ms),
                        duration_le_250ms: curr
                            .duration_le_250ms
                            .saturating_sub(prev_p.duration_le_250ms),
                        duration_le_1000ms: curr
                            .duration_le_1000ms
                            .saturating_sub(prev_p.duration_le_1000ms),
                        duration_gt_1000ms: curr
                            .duration_gt_1000ms
                            .saturating_sub(prev_p.duration_gt_1000ms),
                        ttfb_le_1ms: curr.ttfb_le_1ms.saturating_sub(prev_p.ttfb_le_1ms),
                        ttfb_le_5ms: curr.ttfb_le_5ms.saturating_sub(prev_p.ttfb_le_5ms),
                        ttfb_le_10ms: curr.ttfb_le_10ms.saturating_sub(prev_p.ttfb_le_10ms),
                        ttfb_le_50ms: curr.ttfb_le_50ms.saturating_sub(prev_p.ttfb_le_50ms),
                        ttfb_le_250ms: curr.ttfb_le_250ms.saturating_sub(prev_p.ttfb_le_250ms),
                        ttfb_gt_250ms: curr.ttfb_gt_250ms.saturating_sub(prev_p.ttfb_gt_250ms),
                    },
                    None => *curr,
                }
            })
            .collect();

        Self {
            captured_at: self.captured_at,
            pipelines,
            global: GlobalSnapshot {
                connections_total: self
                    .global
                    .connections_total
                    .saturating_sub(prev.global.connections_total),
                connections_active: self.global.connections_active,
                connections_rejected: self
                    .global
                    .connections_rejected
                    .saturating_sub(prev.global.connections_rejected),
                tls_handshakes_total: self
                    .global
                    .tls_handshakes_total
                    .saturating_sub(prev.global.tls_handshakes_total),
                tls_handshake_errors: self
                    .global
                    .tls_handshake_errors
                    .saturating_sub(prev.global.tls_handshake_errors),
                h2_rst_stream_total: self
                    .global
                    .h2_rst_stream_total
                    .saturating_sub(prev.global.h2_rst_stream_total),
                config_reloads_total: self
                    .global
                    .config_reloads_total
                    .saturating_sub(prev.global.config_reloads_total),
                config_reload_errors: self
                    .global
                    .config_reload_errors
                    .saturating_sub(prev.global.config_reload_errors),
            },
            pool: PoolSnapshot {
                pool_containers_total: self.pool.pool_containers_total,
                connections_idle: self.pool.connections_idle,
                connections_leased: self.pool.connections_leased,
                connections_multiplexed: self.pool.connections_multiplexed,
                multiplexed_active_streams: self.pool.multiplexed_active_streams,
                lease_hits_total: self
                    .pool
                    .lease_hits_total
                    .saturating_sub(prev.pool.lease_hits_total),
                lease_misses_total: self
                    .pool
                    .lease_misses_total
                    .saturating_sub(prev.pool.lease_misses_total),
                lease_timeouts_total: self
                    .pool
                    .lease_timeouts_total
                    .saturating_sub(prev.pool.lease_timeouts_total),
                lease_releases_total: self
                    .pool
                    .lease_releases_total
                    .saturating_sub(prev.pool.lease_releases_total),
                connections_created_total: self
                    .pool
                    .connections_created_total
                    .saturating_sub(prev.pool.connections_created_total),
                evictions_idle_timeout: self
                    .pool
                    .evictions_idle_timeout
                    .saturating_sub(prev.pool.evictions_idle_timeout),
                evictions_max_lifetime: self
                    .pool
                    .evictions_max_lifetime
                    .saturating_sub(prev.pool.evictions_max_lifetime),
                evictions_goaway: self
                    .pool
                    .evictions_goaway
                    .saturating_sub(prev.pool.evictions_goaway),
                evictions_unhealthy: self
                    .pool
                    .evictions_unhealthy
                    .saturating_sub(prev.pool.evictions_unhealthy),
                evictions_capacity_exceeded: self
                    .pool
                    .evictions_capacity_exceeded
                    .saturating_sub(prev.pool.evictions_capacity_exceeded),
                lifespan_le_5s: self
                    .pool
                    .lifespan_le_5s
                    .saturating_sub(prev.pool.lifespan_le_5s),
                lifespan_le_30s: self
                    .pool
                    .lifespan_le_30s
                    .saturating_sub(prev.pool.lifespan_le_30s),
                lifespan_le_5m: self
                    .pool
                    .lifespan_le_5m
                    .saturating_sub(prev.pool.lifespan_le_5m),
                lifespan_le_30m: self
                    .pool
                    .lifespan_le_30m
                    .saturating_sub(prev.pool.lifespan_le_30m),
                lifespan_le_1h: self
                    .pool
                    .lifespan_le_1h
                    .saturating_sub(prev.pool.lifespan_le_1h),
                lifespan_gt_1h: self
                    .pool
                    .lifespan_gt_1h
                    .saturating_sub(prev.pool.lifespan_gt_1h),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::MetricsStore;
    use std::time::Duration;

    #[test]
    fn test_snapshot_delta_since_preserves_gauges_and_subtracts_counters() {
        let store = MetricsStore::new();
        store.global().connection_opened();
        store.record_stream_start(
            DownstreamProtocol::H1,
            UpstreamProtocol::H1,
            PipeStrategy::Buffered,
        );
        store.record_stream_completion(
            DownstreamProtocol::H1,
            UpstreamProtocol::H1,
            PipeStrategy::Buffered,
            crate::metrics::StreamCompletion::new(200, 100, 250, Duration::from_millis(2), None),
        );

        let snap1 = store.snapshot();
        let d1 = snap1.delta_since(&MetricsSnapshot::zero());
        assert_eq!(d1.global.connections_total, 1);
        assert_eq!(d1.global.connections_active, 1);
        let p1 = d1
            .find_pipeline(
                DownstreamProtocol::H1,
                UpstreamProtocol::H1,
                PipeStrategy::Buffered,
            )
            .unwrap();
        assert_eq!(p1.requests_2xx, 1);
        assert_eq!(p1.bytes_rx, 100);
        assert_eq!(p1.bytes_tx, 250);

        // Idle interval: counters must be 0, active gauges keep current state
        let snap2 = store.snapshot();
        let d2 = snap2.delta_since(&snap1);
        assert_eq!(d2.global.connections_total, 0);
        assert_eq!(d2.global.connections_active, 1);
        let p2 = d2
            .find_pipeline(
                DownstreamProtocol::H1,
                UpstreamProtocol::H1,
                PipeStrategy::Buffered,
            )
            .unwrap();
        assert_eq!(p2.requests_2xx, 0);
        assert_eq!(p2.bytes_rx, 0);
        assert_eq!(p2.bytes_tx, 0);

        // Second request + connection closed
        store.global().connection_closed();
        store.record_stream_start(
            DownstreamProtocol::H1,
            UpstreamProtocol::H1,
            PipeStrategy::Buffered,
        );
        store.record_stream_completion(
            DownstreamProtocol::H1,
            UpstreamProtocol::H1,
            PipeStrategy::Buffered,
            crate::metrics::StreamCompletion::new(404, 50, 75, Duration::from_millis(3), None),
        );
        let snap3 = store.snapshot();
        let d3 = snap3.delta_since(&snap2);
        assert_eq!(d3.global.connections_active, 0);
        let p3 = d3
            .find_pipeline(
                DownstreamProtocol::H1,
                UpstreamProtocol::H1,
                PipeStrategy::Buffered,
            )
            .unwrap();
        assert_eq!(p3.requests_2xx, 0);
        assert_eq!(p3.requests_4xx, 1);
        assert_eq!(p3.bytes_rx, 50);
        assert_eq!(p3.bytes_tx, 75);
    }
}
