use std::time::Duration;

use velda_core::hardware::{CpuTier, MemoryTier};

use crate::container::{
    concurrency_shards_for_cpu_tier, max_concurrent_streams_for_mem_tier,
    max_idle_per_key_for_mem_tier, probed_max_idle_per_key,
};

/// Default idle timeout before an idle connection is considered expired (30 seconds).
pub const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(30);

/// Default maximum connection lifetime before forced retirement (1 hour).
pub const DEFAULT_MAX_LIFETIME: Duration = Duration::from_secs(3600);

/// Configuration defining capacity, idle timeouts, and maximum connection lifetimes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PoolConfig {
    /// Maximum number of idle connections retained per key container (default: dynamically probed from MemoryTier).
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
    /// Returns pool configuration and concurrency shard count explicitly tuned for given CPU and Memory tiers.
    pub fn for_tiers(cpu: CpuTier, mem: MemoryTier) -> (Self, usize) {
        let shards = concurrency_shards_for_cpu_tier(cpu);
        let config = Self {
            max_idle_per_key: max_idle_per_key_for_mem_tier(mem),
            max_concurrent_streams: max_concurrent_streams_for_mem_tier(mem),
            idle_timeout: DEFAULT_IDLE_TIMEOUT,
            max_lifetime: Some(DEFAULT_MAX_LIFETIME),
        };
        (config, shards)
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
    fn test_pool_config_tiers() {
        // Constrained CPU + Constrained Memory
        let (cfg, shards) = PoolConfig::for_tiers(CpuTier::Constrained, MemoryTier::Constrained);
        assert_eq!(shards, 1);
        assert_eq!(cfg.max_idle_per_key, 8);
        assert_eq!(cfg.max_concurrent_streams, 32);

        // Ultra CPU + Constrained Memory (Compute-heavy, low RAM)
        let (cfg, shards) = PoolConfig::for_tiers(CpuTier::Ultra, MemoryTier::Constrained);
        assert_eq!(shards, 64);
        assert_eq!(cfg.max_idle_per_key, 8);
        assert_eq!(cfg.max_concurrent_streams, 32);

        // Constrained CPU + Ultra Memory (Low core, massive RAM)
        let (cfg, shards) = PoolConfig::for_tiers(CpuTier::Constrained, MemoryTier::Ultra);
        assert_eq!(shards, 1);
        assert_eq!(cfg.max_idle_per_key, 512);
        assert_eq!(cfg.max_concurrent_streams, 512);

        // Ultra CPU + Ultra Memory (Hyperscale server)
        let (cfg, shards) = PoolConfig::for_tiers(CpuTier::Ultra, MemoryTier::Ultra);
        assert_eq!(shards, 64);
        assert_eq!(cfg.max_idle_per_key, 512);
        assert_eq!(cfg.max_concurrent_streams, 512);
    }
}
