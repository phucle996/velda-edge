//! Hardware topology and CPU probing for the Velda Edge Data Plane.
//!
//! Executed once during process cold-start bootstrap. Probes:
//! 1. Logical CPU core count and cgroups quota limits.
//! 2. Target worker thread concurrency for the edge data plane.
//! 3. Optimal shard sizing recommendations for downstream subsystems (`velda-lb`, `velda-pool`).

use std::sync::OnceLock;

/// Hardware topology profile probed at bootstrap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HardwareTopology {
    /// Number of logical CPU cores available to the process (respects cgroups limits).
    pub available_cores: usize,
    /// Number of worker threads allocated for traffic processing.
    pub worker_threads: usize,
}

impl HardwareTopology {
    /// Probes the host hardware environment during bootstrap.
    ///
    /// Respects container CPU limits (cgroups) via `std::thread::available_parallelism`.
    pub fn probe() -> Self {
        let available_cores = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);

        // Allow explicit override via environment variable if operators pin thread pools
        let worker_threads = std::env::var("VELDA_WORKER_THREADS")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|&w| w > 0)
            .unwrap_or(available_cores);

        let topo = Self {
            available_cores,
            worker_threads,
        };

        tracing::info!(
            available_cores = topo.available_cores,
            worker_threads = topo.worker_threads,
            lb_rr_shards = topo.lb_rr_shards(),
            lb_metrics_shards = topo.lb_metrics_shards(),
            "Probed hardware topology successfully"
        );

        topo
    }

    /// Creates a hardware topology with an explicit worker thread count.
    #[inline]
    pub const fn with_workers(worker_threads: usize) -> Self {
        Self {
            available_cores: worker_threads,
            worker_threads,
        }
    }

    /// Optimal shard count for RoundRobin and WeightedRoundRobin load balancers.
    ///
    /// OPTIMIZATION: Zero read-sum overhead algorithms scale 1:1 with worker count
    /// (rounded to next power of two) to completely eliminate cross-core lock/atomic contention.
    #[inline]
    pub fn lb_rr_shards(&self) -> usize {
        self.worker_threads.max(1).next_power_of_two()
    }

    /// Optimal shard count for `EndpointMetrics`.
    ///
    /// OPTIMIZATION: Bounded dynamic sharding balances write concurrency against read-sum latency:
    /// - 1..4 workers: 1:1 mapping (1..4 shards), read-sum takes ~2 ns.
    /// - 5..16 workers: 1:1 mapping (5..16 shards), read-sum takes ~8 ns.
    /// - 17..64 workers: clamped at 16 shards to maintain read-sum under 10 ns.
    /// - 64+ workers: clamped at 32 shards to span NUMA nodes while keeping read-sum under 15 ns.
    #[inline]
    pub fn lb_metrics_shards(&self) -> usize {
        match self.worker_threads {
            0..=4 => self.worker_threads.max(1),
            5..=16 => self.worker_threads,
            17..=64 => 16,
            _ => 32,
        }
    }

    /// Optimal shard count for connection pooling (`velda-pool`).
    #[inline]
    pub fn pool_shards(&self) -> usize {
        self.worker_threads.max(1).next_power_of_two().clamp(4, 64)
    }
}

/// Global cached hardware topology probed once during cold-start.
pub fn global_hardware_topology() -> &'static HardwareTopology {
    static TOPOLOGY: OnceLock<HardwareTopology> = OnceLock::new();
    TOPOLOGY.get_or_init(HardwareTopology::probe)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hardware_probe_returns_valid_topology() {
        let topo = HardwareTopology::probe();
        assert!(topo.available_cores >= 1);
        assert!(topo.worker_threads >= 1);
        assert!(topo.lb_rr_shards() >= 1);
        assert!(topo.lb_metrics_shards() >= 1);
    }

    #[test]
    fn test_shard_recommendation_scaling() {
        let topo_1 = HardwareTopology::with_workers(1);
        assert_eq!(topo_1.lb_rr_shards(), 1);
        assert_eq!(topo_1.lb_metrics_shards(), 1);

        let topo_4 = HardwareTopology::with_workers(4);
        assert_eq!(topo_4.lb_rr_shards(), 4);
        assert_eq!(topo_4.lb_metrics_shards(), 4);

        let topo_8 = HardwareTopology::with_workers(8);
        assert_eq!(topo_8.lb_rr_shards(), 8);
        assert_eq!(topo_8.lb_metrics_shards(), 8);

        let topo_16 = HardwareTopology::with_workers(16);
        assert_eq!(topo_16.lb_rr_shards(), 16);
        assert_eq!(topo_16.lb_metrics_shards(), 16);

        let topo_32 = HardwareTopology::with_workers(32);
        assert_eq!(topo_32.lb_rr_shards(), 32);
        assert_eq!(topo_32.lb_metrics_shards(), 16);

        let topo_64 = HardwareTopology::with_workers(64);
        assert_eq!(topo_64.lb_rr_shards(), 64);
        assert_eq!(topo_64.lb_metrics_shards(), 16);

        let topo_128 = HardwareTopology::with_workers(128);
        assert_eq!(topo_128.lb_rr_shards(), 128);
        assert_eq!(topo_128.lb_metrics_shards(), 32);
    }

    #[test]
    fn test_global_hardware_topology_singleton() {
        let t1 = global_hardware_topology();
        let t2 = global_hardware_topology();
        assert_eq!(t1, t2);
    }
}
