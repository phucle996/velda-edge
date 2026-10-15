use std::time::Duration;

use crate::container::{max_concurrent_streams_for_mem_tier, probed_max_idle_per_key};

/// Default idle timeout before an idle connection is considered expired (30 seconds).
pub const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(30);

/// Default maximum connection lifetime before forced retirement (1 hour).
pub const DEFAULT_MAX_LIFETIME: Duration = Duration::from_secs(3600);

/// Configuration defining capacity, idle timeouts, and maximum connection lifetimes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PoolConfig {
    /// Maximum number of idle connections retained per key container (default: dynamically computed from worker concurrency).
    /// Excess connections returned to the pool are closed immediately to prevent FD exhaustion (`EMFILE`).
    pub max_idle_per_key: usize,
    /// Maximum concurrent streams per multiplexed connection (default: dynamically probed from MemoryTier).
    pub max_concurrent_streams: u32,
    /// Default idle timeout before an idle connection is considered expired (default: 30s).
    pub idle_timeout: Duration,
    /// Maximum connection lifetime from creation before forced retirement (default: 1 hour).
    /// Prevents connection reset (RST) races when backend upstream servers enforce keepalive limits.
    pub max_lifetime: Option<Duration>,
}

impl Default for PoolConfig {
    fn default() -> Self {
        let mem = velda_core::global_hardware_topology().memory_tier();
        Self {
            max_idle_per_key: probed_max_idle_per_key(),
            max_concurrent_streams: max_concurrent_streams_for_mem_tier(mem),
            idle_timeout: DEFAULT_IDLE_TIMEOUT,
            max_lifetime: Some(DEFAULT_MAX_LIFETIME),
        }
    }
}

impl PoolConfig {
    /// Returns pool configuration and concurrency shards directly using pre-computed values from [`HardwareTopology`].
    pub fn for_topology(topo: &velda_core::HardwareTopology) -> (Self, usize) {
        let config = Self {
            max_idle_per_key: topo.optimal_max_idle_per_key,
            max_concurrent_streams: max_concurrent_streams_for_mem_tier(topo.memory_tier()),
            idle_timeout: DEFAULT_IDLE_TIMEOUT,
            max_lifetime: Some(DEFAULT_MAX_LIFETIME),
        };
        (config, topo.concurrency_shards)
    }
}

/// Real-time metrics and accounting of pool operations aggregated across shards.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PoolStats {
    /// Number of successful pool hits (reused idle connection or stream slot).
    pub hits: u64,
    /// Number of pool misses (required creating a fresh connection).
    pub misses: u64,
    /// Total connections successfully returned and accepted into the pool.
    pub releases: u64,
    /// Total connections closed during idle expiration sweeps.
    pub evictions: u64,
}

impl PoolStats {
    /// Returns the cache hit ratio as a floating-point value in `[0.0, 1.0]`.
    #[inline]
    pub fn hit_ratio(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            self.hits as f64 / total as f64
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pool_config_for_topology() {
        // Small 2-core edge
        let topo_2 = velda_core::HardwareTopology::with_workers_and_memory(2, 512 * 1024 * 1024);
        let (cfg, shards) = PoolConfig::for_topology(&topo_2);
        assert_eq!(shards, 8);
        assert_eq!(cfg.max_idle_per_key, 128);
        assert_eq!(cfg.max_concurrent_streams, 1024);

        // 16-core regional edge (4GB RAM -> MemoryTier::Medium)
        let topo_16 =
            velda_core::HardwareTopology::with_workers_and_memory(16, 4 * 1024 * 1024 * 1024);
        let (cfg, shards) = PoolConfig::for_topology(&topo_16);
        assert_eq!(shards, 32);
        assert_eq!(cfg.max_idle_per_key, 1024);
        assert_eq!(cfg.max_concurrent_streams, 4096);

        // 64-core hyperscale node (64GB RAM -> MemoryTier::XLarge)
        let topo_64 =
            velda_core::HardwareTopology::with_workers_and_memory(64, 64 * 1024 * 1024 * 1024);
        let (cfg, shards) = PoolConfig::for_topology(&topo_64);
        assert_eq!(shards, 128);
        assert_eq!(cfg.max_idle_per_key, 4096);
        assert_eq!(cfg.max_concurrent_streams, 16384);
    }
}
