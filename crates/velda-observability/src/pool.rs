//! Cacheline-Aligned In-Memory Connection Pool Metrics.
//!
//! Provides lock-free, zero-allocation atomic accounting for connection pooling,
//! subpool container lifecycle, reusability (hit/miss rate), eviction classification,
//! and connection lifespan distributions.
//!
//! # Hardware Invariants
//! - `#[repr(align(64))]`: Aligned to 64 bytes (L1 cacheline boundary) to prevent
//!   cross-core cache invalidation between pool operations and request routing.
//! - Lock-Free: Employs `Ordering::Relaxed` atomics for hot-path leasing and release.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::Duration;

use crate::snapshot::PoolSnapshot;

/// Reason why an upstream connection was closed or evicted from the pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum EvictionReason {
    /// Closed after exceeding idle timeout threshold without being leased.
    IdleTimeout = 0,
    /// Closed after exceeding maximum configured connection lifetime.
    MaxLifetime = 1,
    /// Closed in response to an upstream HTTP/2 or HTTP/3 GOAWAY frame.
    Goaway = 2,
    /// Evicted because health check detected a broken, reset, or closed socket.
    Unhealthy = 3,
    /// Evicted because the pool container exceeded maximum idle capacity.
    CapacityExceeded = 4,
}

impl EvictionReason {
    /// Returns the canonical static string slice for this eviction reason.
    #[inline(always)]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::IdleTimeout => "idle_timeout",
            Self::MaxLifetime => "max_lifetime",
            Self::Goaway => "goaway",
            Self::Unhealthy => "unhealthy",
            Self::CapacityExceeded => "capacity_exceeded",
        }
    }
}

/// Cacheline-aligned atomic metrics structure for connection pool operations.
#[repr(align(64))]
pub struct PoolMetrics {
    // ------------------------------------------------------------------------
    // 1. Capacity & Container Scales (Gauges)
    // ------------------------------------------------------------------------
    /// Total number of active SubPool key containers resident in memory.
    /// Crucial for detecting "Pool Explosion" (unbounded endpoint proliferation).
    pub pool_containers_total: AtomicU64,
    /// Number of sequential (H1) connections currently idle in LIFO queues.
    pub connections_idle: AtomicU64,
    /// Number of sequential (H1) connections actively checked out and in-use.
    pub connections_leased: AtomicI64,
    /// Number of multiplexed (H2/H3) connections actively serving streams.
    pub connections_multiplexed: AtomicU64,
    /// Total concurrent streams currently active across all multiplexed connections.
    pub multiplexed_active_streams: AtomicI64,

    // ------------------------------------------------------------------------
    // 2. Reusability & Efficiency (Counters)
    // ------------------------------------------------------------------------
    /// Number of successful pool acquisitions reusing an existing warm connection.
    pub lease_hits_total: AtomicU64,
    /// Number of acquisitions requiring establishment of a fresh TCP/TLS socket.
    pub lease_misses_total: AtomicU64,
    /// Number of acquisitions that timed out waiting for connection availability.
    pub lease_timeouts_total: AtomicU64,
    /// Number of connection leases returned and accepted back into the pool.
    pub lease_releases_total: AtomicU64,

    // ------------------------------------------------------------------------
    // 3. Lifecycle & Eviction Classification (Counters)
    // ------------------------------------------------------------------------
    /// Cumulative count of physical upstream connections created since startup.
    pub connections_created_total: AtomicU64,
    /// Evictions caused by idle timeout expiration.
    pub evictions_idle_timeout: AtomicU64,
    /// Evictions caused by reaching maximum connection lifetime.
    pub evictions_max_lifetime: AtomicU64,
    /// Evictions triggered by an upstream HTTP/2 GOAWAY frame.
    pub evictions_goaway: AtomicU64,
    /// Evictions caused by socket errors or failed health checks.
    pub evictions_unhealthy: AtomicU64,
    /// Evictions caused by subpool exceeding maximum allowed idle capacity.
    pub evictions_capacity_exceeded: AtomicU64,

    // ------------------------------------------------------------------------
    // 4. Connection Lifespan Buckets (Cumulative Histogram)
    // ------------------------------------------------------------------------
    /// Closed connections that lived <= 5 seconds.
    pub lifespan_le_5s: AtomicU64,
    /// Closed connections that lived <= 30 seconds.
    pub lifespan_le_30s: AtomicU64,
    /// Closed connections that lived <= 5 minutes (300s).
    pub lifespan_le_5m: AtomicU64,
    /// Closed connections that lived <= 30 minutes (1800s).
    pub lifespan_le_30m: AtomicU64,
    /// Closed connections that lived <= 1 hour (3600s).
    pub lifespan_le_1h: AtomicU64,
    /// Closed connections that lived > 1 hour.
    pub lifespan_gt_1h: AtomicU64,
}

impl Default for PoolMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl PoolMetrics {
    /// Creates a zeroed pool metrics instance.
    pub const fn new() -> Self {
        Self {
            pool_containers_total: AtomicU64::new(0),
            connections_idle: AtomicU64::new(0),
            connections_leased: AtomicI64::new(0),
            connections_multiplexed: AtomicU64::new(0),
            multiplexed_active_streams: AtomicI64::new(0),
            lease_hits_total: AtomicU64::new(0),
            lease_misses_total: AtomicU64::new(0),
            lease_timeouts_total: AtomicU64::new(0),
            lease_releases_total: AtomicU64::new(0),
            connections_created_total: AtomicU64::new(0),
            evictions_idle_timeout: AtomicU64::new(0),
            evictions_max_lifetime: AtomicU64::new(0),
            evictions_goaway: AtomicU64::new(0),
            evictions_unhealthy: AtomicU64::new(0),
            evictions_capacity_exceeded: AtomicU64::new(0),
            lifespan_le_5s: AtomicU64::new(0),
            lifespan_le_30s: AtomicU64::new(0),
            lifespan_le_5m: AtomicU64::new(0),
            lifespan_le_30m: AtomicU64::new(0),
            lifespan_le_1h: AtomicU64::new(0),
            lifespan_gt_1h: AtomicU64::new(0),
        }
    }

    // ------------------------------------------------------------------------
    // Hot-Path Mutators
    // ------------------------------------------------------------------------

    /// Records a successful pool lease hit (connection reused).
    #[inline(always)]
    pub fn record_hit(&self) {
        self.lease_hits_total.fetch_add(1, Ordering::Relaxed);
        self.connections_leased.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a pool miss (must connect to backend).
    #[inline(always)]
    pub fn record_miss(&self) {
        self.lease_misses_total.fetch_add(1, Ordering::Relaxed);
        self.connections_leased.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a timeout while waiting for pool lease.
    #[inline(always)]
    pub fn record_timeout(&self) {
        self.lease_timeouts_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a connection lease returned and accepted back into the pool.
    #[inline(always)]
    pub fn record_release(&self) {
        self.lease_releases_total.fetch_add(1, Ordering::Relaxed);
        self.connections_leased.fetch_sub(1, Ordering::Relaxed);
    }

    /// Records a leased connection that was closed or dropped without returning to the pool
    /// (e.g. broken socket, protocol error, or connection reset during active upstream request).
    #[inline(always)]
    pub fn record_lease_dropped(&self) {
        self.connections_leased.fetch_sub(1, Ordering::Relaxed);
    }

    /// Records the creation of a physical upstream connection.
    #[inline(always)]
    pub fn record_connection_created(&self) {
        self.connections_created_total
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Records the closing/eviction of a connection with its lifespan and reason.
    ///
    /// # Histogram Semantics
    /// Buckets are cumulative (standard Prometheus convention):
    /// Any connection living `<= X` seconds increments all buckets with threshold `>= X`.
    /// Total connections closed with lifespan tracked = `lifespan_le_1h + lifespan_gt_1h`.
    #[inline(always)]
    pub fn record_eviction(&self, reason: EvictionReason, lifespan: Duration) {
        match reason {
            EvictionReason::IdleTimeout => {
                self.evictions_idle_timeout.fetch_add(1, Ordering::Relaxed);
            }
            EvictionReason::MaxLifetime => {
                self.evictions_max_lifetime.fetch_add(1, Ordering::Relaxed);
            }
            EvictionReason::Goaway => {
                self.evictions_goaway.fetch_add(1, Ordering::Relaxed);
            }
            EvictionReason::Unhealthy => {
                self.evictions_unhealthy.fetch_add(1, Ordering::Relaxed);
            }
            EvictionReason::CapacityExceeded => {
                self.evictions_capacity_exceeded
                    .fetch_add(1, Ordering::Relaxed);
            }
        }

        // Lifespan Bucketing (Cumulative)
        let secs = lifespan.as_secs();
        if secs <= 5 {
            self.lifespan_le_5s.fetch_add(1, Ordering::Relaxed);
        }
        if secs <= 30 {
            self.lifespan_le_30s.fetch_add(1, Ordering::Relaxed);
        }
        if secs <= 300 {
            self.lifespan_le_5m.fetch_add(1, Ordering::Relaxed);
        }
        if secs <= 1800 {
            self.lifespan_le_30m.fetch_add(1, Ordering::Relaxed);
        }
        if secs <= 3600 {
            self.lifespan_le_1h.fetch_add(1, Ordering::Relaxed);
        } else {
            self.lifespan_gt_1h.fetch_add(1, Ordering::Relaxed);
        }
    }

    // ------------------------------------------------------------------------
    // Gauge Synchronizers
    // ------------------------------------------------------------------------

    /// Updates the total number of resident SubPool containers.
    #[inline(always)]
    pub fn set_pool_containers(&self, count: u64) {
        self.pool_containers_total.store(count, Ordering::Relaxed);
    }

    /// Updates the instantaneous count of idle connections.
    #[inline(always)]
    pub fn set_idle_connections(&self, count: u64) {
        self.connections_idle.store(count, Ordering::Relaxed);
    }

    /// Updates the instantaneous count of multiplexed connections.
    #[inline(always)]
    pub fn set_multiplexed_connections(&self, count: u64) {
        self.connections_multiplexed.store(count, Ordering::Relaxed);
    }

    /// Increments the active multiplexed streams gauge.
    #[inline(always)]
    pub fn mux_stream_started(&self) {
        self.multiplexed_active_streams
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Decrements the active multiplexed streams gauge.
    #[inline(always)]
    pub fn mux_stream_ended(&self) {
        self.multiplexed_active_streams
            .fetch_sub(1, Ordering::Relaxed);
    }

    // ------------------------------------------------------------------------
    // Snapshot Generation
    // ------------------------------------------------------------------------

    /// Captures a point-in-time pure snapshot of pool metrics.
    ///
    /// Non-blocking, reads atomics with `Ordering::Relaxed`.
    pub fn snapshot(&self) -> PoolSnapshot {
        PoolSnapshot {
            pool_containers_total: self.pool_containers_total.load(Ordering::Relaxed),
            connections_idle: self.connections_idle.load(Ordering::Relaxed),
            connections_leased: self.connections_leased.load(Ordering::Relaxed),
            connections_multiplexed: self.connections_multiplexed.load(Ordering::Relaxed),
            multiplexed_active_streams: self.multiplexed_active_streams.load(Ordering::Relaxed),
            lease_hits_total: self.lease_hits_total.load(Ordering::Relaxed),
            lease_misses_total: self.lease_misses_total.load(Ordering::Relaxed),
            lease_timeouts_total: self.lease_timeouts_total.load(Ordering::Relaxed),
            lease_releases_total: self.lease_releases_total.load(Ordering::Relaxed),
            connections_created_total: self.connections_created_total.load(Ordering::Relaxed),
            evictions_idle_timeout: self.evictions_idle_timeout.load(Ordering::Relaxed),
            evictions_max_lifetime: self.evictions_max_lifetime.load(Ordering::Relaxed),
            evictions_goaway: self.evictions_goaway.load(Ordering::Relaxed),
            evictions_unhealthy: self.evictions_unhealthy.load(Ordering::Relaxed),
            evictions_capacity_exceeded: self.evictions_capacity_exceeded.load(Ordering::Relaxed),
            lifespan_le_5s: self.lifespan_le_5s.load(Ordering::Relaxed),
            lifespan_le_30s: self.lifespan_le_30s.load(Ordering::Relaxed),
            lifespan_le_5m: self.lifespan_le_5m.load(Ordering::Relaxed),
            lifespan_le_30m: self.lifespan_le_30m.load(Ordering::Relaxed),
            lifespan_le_1h: self.lifespan_le_1h.load(Ordering::Relaxed),
            lifespan_gt_1h: self.lifespan_gt_1h.load(Ordering::Relaxed),
        }
    }
}
