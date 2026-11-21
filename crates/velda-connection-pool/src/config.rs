use std::time::Duration;

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
    /// Maximum concurrent streams per multiplexed connection (default: dynamically computed from HardwareSizing).
    pub max_concurrent_streams: u32,
    /// Default idle timeout before an idle connection is considered expired (default: 30s).
    pub idle_timeout: Duration,
    /// Maximum connection lifetime from creation before forced retirement (default: 1 hour).
    /// Prevents connection reset (RST) races when backend upstream servers enforce keepalive limits.
    pub max_lifetime: Option<Duration>,
}

impl Default for PoolConfig {
    fn default() -> Self {
        let hw = velda_core::hardware::global_hardware_topology();
        Self {
            max_idle_per_key: hw.optimal_max_idle_per_key,
            max_concurrent_streams: ((hw.available_cores.max(1) * 64) as u32).clamp(128, 1024),
            idle_timeout: DEFAULT_IDLE_TIMEOUT,
            max_lifetime: Some(DEFAULT_MAX_LIFETIME),
        }
    }
}

impl PoolConfig {
    /// Returns pool configuration and concurrency shards directly using pre-computed values from [`HardwareTopology`].
    pub fn for_topology(topo: &velda_core::hardware::HardwareTopology) -> (Self, usize) {
        let config = Self {
            max_idle_per_key: topo.optimal_max_idle_per_key,
            max_concurrent_streams: ((topo.available_cores.max(1) * 64) as u32).clamp(128, 1024),
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

/// Minimum number of idle connections retained per key container.
pub const MIN_IDLE_PER_KEY: usize = 16;

/// Maximum number of idle connections retained per key container to protect from OS `EMFILE`.
pub const MAX_IDLE_PER_KEY: usize = 4096;

/// Minimum lane count for concurrency distribution.
pub const MIN_SHARDS: usize = 8;

/// Maximum lane count for concurrency distribution on ultra-high-core server hardware.
pub const MAX_SHARDS: usize = 1024;

/// Computes the recommended maximum concurrent streams per multiplexed connection scaled continuously
/// from available CPU cores and physical RAM bytes.
#[inline]
pub fn optimal_max_concurrent_streams(cores: usize, _ram_bytes: usize) -> u32 {
    ((cores.max(1) * 32) as u32).clamp(64, 256)
}

/// Reads the optimal maximum concurrent streams pre-computed during cold-start bootstrap.
#[inline]
pub fn probed_max_concurrent_streams() -> u32 {
    let hw = velda_core::hardware::global_hardware_topology();
    optimal_max_concurrent_streams(hw.available_cores, hw.memory.total_bytes)
}

/// Computes the optimal maximum idle connections per key container based on worker concurrency.
#[inline]
pub fn optimal_max_idle_per_key(workers: usize) -> usize {
    let hw = velda_core::hardware::global_hardware_topology();
    velda_core::hardware::compute_optimal_pool_concurrency(workers, hw.memory_bytes).0
}

/// Reads the optimal maximum idle connections per key pre-computed during cold-start bootstrap.
#[inline]
pub fn probed_max_idle_per_key() -> usize {
    velda_core::hardware::global_hardware_topology().optimal_max_idle_per_key
}

/// Computes the optimal power-of-two lane count scaled to worker concurrency.
#[inline]
pub fn optimal_shard_count(workers: usize) -> usize {
    let target = (workers.max(1) * 2).next_power_of_two();
    target.clamp(MIN_SHARDS, MAX_SHARDS)
}

/// Reads the optimal lane count pre-computed during cold-start bootstrap.
#[inline]
pub fn probed_shard_count() -> usize {
    velda_core::hardware::global_hardware_topology().concurrency_shards
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
        let topo_2 =
            velda_core::hardware::HardwareTopology::with_workers_and_memory(2, 512 * 1024 * 1024);
        let (cfg, shards) = PoolConfig::for_topology(&topo_2);
        assert_eq!(shards, 8);
        assert_eq!(cfg.max_idle_per_key, 256);
        assert_eq!(cfg.max_concurrent_streams, 128);

        // 16-core regional edge (4GB RAM)
        let topo_16 = velda_core::hardware::HardwareTopology::with_workers_and_memory(
            16,
            4 * 1024 * 1024 * 1024,
        );
        let (cfg, shards) = PoolConfig::for_topology(&topo_16);
        assert_eq!(shards, 32);
        assert_eq!(cfg.max_idle_per_key, 256);
        assert_eq!(cfg.max_concurrent_streams, 1024);

        // 64-core hyperscale node (64GB RAM)
        let topo_64 = velda_core::hardware::HardwareTopology::with_workers_and_memory(
            64,
            64 * 1024 * 1024 * 1024,
        );
        let (cfg, shards) = PoolConfig::for_topology(&topo_64);
        assert_eq!(shards, 128);
        assert_eq!(cfg.max_idle_per_key, 256);
        assert_eq!(cfg.max_concurrent_streams, 1024);
    }
}
