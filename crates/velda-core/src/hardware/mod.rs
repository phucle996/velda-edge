//! Hardware topology and concurrency profile for the Velda Edge Data Plane.
//!
//! Provides the canonical representation of available CPU cores and worker threads
//! probed once and cached in RAM (`OnceLock`).
//!
//! # Continuous Hardware Sizing Architecture:
//! - CPU Topology: Driven by available logical cores and cgroup CPU limits.
//!   Controls worker thread count, channel capacity, and concurrency sharding.
//! - Memory Topology: Driven by total RAM capacity and cgroup memory limits.
//!   Derives continuous socket buffers, backlog depths, and in-memory cache capacities
//!   via [`HardwareSizing`].
//!
//! Strict Invariant:
//! - This module ONLY owns host hardware inspection and continuous capacity profiling.
//! - Sizing is derived continuously without rigid discrete tier boundaries.

pub mod cpu;
pub mod kernel;
pub mod memory;
pub mod overload;
pub mod partition;
pub mod sizing;

use std::sync::OnceLock;

pub use cpu::{CpuProfile, CpuVectorCapabilities, copy_bulk_payload, probe_cpu};
pub use kernel::{
    AccelerationTier, BusyPollTier, CoreSteeringTier, DeadPeerTeardownTier,
    KernelAccelerationLadder, KernelProfile, KernelVersion, MultipathResilienceTier,
    MultiplexPacingTier, OutboundPortScalingTier, UdpEgressTier, UdpOffloadTier, probe_kernel,
};
pub use memory::{MemoryProfile, probe_memory};
pub use overload::{OverloadConfig, OverloadLevel, OverloadTracker, probe_memory_usage};
pub use partition::{CorePartition, CorePartitionMode};
pub use sizing::HardwareSizing;

/// Hardware topology profile probed once and cached in RAM.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HardwareTopology {
    /// Fine-grained CPU topology and concurrency profile.
    pub cpu: CpuProfile,
    /// Fine-grained memory topology and RAM capacity profile.
    pub memory: MemoryProfile,
    /// Kernel release and hardware I/O acceleration capabilities.
    pub kernel: KernelProfile,
    /// Number of logical CPU cores available to the process (respects container cgroups limits).
    pub available_cores: usize,
    /// Number of worker threads allocated for traffic processing.
    pub worker_threads: usize,
    /// Total/available memory in bytes (respects container cgroups limits).
    pub memory_bytes: usize,
    /// Pre-computed hardware-driven resource capacities across all networking subsystems.
    pub sizing: HardwareSizing,
    /// Pre-computed optimal idle connection capacity per upstream target (scaled from CPU concurrency).
    pub optimal_max_idle_per_key: usize,
    /// Pre-computed optimal shard count for concurrency distribution (power-of-two, bounded [8, 1024]).
    pub concurrency_shards: usize,
}

/// Continuously computes the optimal pool concurrency and idle capacity per key
/// scaled from hardware CPU parallelism and available physical RAM bytes.
///
/// # Mathematical Formulation
/// - `shards`: `(worker_threads * 2).next_power_of_two().clamp(8, 1024)`: Prevents Mutex false sharing across cores.
/// - `target_total`: `(worker_threads * 256).clamp(1024, 65536)`: Absorbs high multiplexed stream bursts.
/// - `ram_safe_cap`: `(memory_bytes / 262144).max(128)`: Caps max sockets at ~1/64 of RAM capacity (4KB socket buffer).
/// - `max_idle`: `((min(target_total, ram_safe_cap) / shards) * 2).clamp(128, 4096)`.
#[inline]
pub const fn compute_optimal_pool_concurrency(
    worker_threads: usize,
    memory_bytes: usize,
) -> (usize, usize) {
    let workers = if worker_threads == 0 {
        1
    } else {
        worker_threads
    };
    let shards = {
        let val = (workers * 2).next_power_of_two();
        if val < 8 {
            8
        } else if val > 1024 {
            1024
        } else {
            val
        }
    };

    let target_total = {
        let val = workers * 256;
        if val < 1024 {
            1024
        } else if val > 65536 {
            65536
        } else {
            val
        }
    };

    let ram_safe_cap = if memory_bytes == 0 {
        1024
    } else {
        let cap = memory_bytes / 262_144;
        if cap < 128 { 128 } else { cap }
    };

    let total_pool = if target_total < ram_safe_cap {
        target_total
    } else {
        ram_safe_cap
    };

    let per_shard_unclamped = (total_pool / shards) * 2;
    let max_idle = if per_shard_unclamped < 128 {
        128
    } else if per_shard_unclamped > 4096 {
        4096
    } else {
        per_shard_unclamped
    };

    (max_idle, shards)
}

impl HardwareTopology {
    /// Probes the host hardware environment during cold-start bootstrap.
    ///
    /// Respects container CPU/memory limits (cgroups v1 & v2) and environment overrides.
    pub fn probe() -> Self {
        let cpu = probe_cpu();
        let memory = probe_memory();
        let kernel = probe_kernel();
        let sizing = HardwareSizing::from_hardware(cpu.available_cores, memory.total_bytes);

        Self {
            cpu,
            memory,
            kernel,
            available_cores: cpu.available_cores,
            worker_threads: cpu.available_cores,
            memory_bytes: memory.total_bytes,
            sizing,
            optimal_max_idle_per_key: sizing.optimal_max_idle_per_key,
            concurrency_shards: sizing.concurrency_shards,
        }
    }

    /// Creates a hardware topology profile from explicit CPU and Memory profiles.
    #[inline]
    pub fn new(cpu: CpuProfile, memory: MemoryProfile) -> Self {
        let kernel = probe_kernel();
        let sizing = HardwareSizing::from_hardware(cpu.available_cores, memory.total_bytes);
        Self {
            cpu,
            memory,
            kernel,
            available_cores: cpu.available_cores,
            worker_threads: cpu.available_cores,
            memory_bytes: memory.total_bytes,
            sizing,
            optimal_max_idle_per_key: sizing.optimal_max_idle_per_key,
            concurrency_shards: sizing.concurrency_shards,
        }
    }

    /// Attaches an explicit [`KernelProfile`] to this hardware topology (useful for testing acceleration tiers).
    #[inline]
    pub fn with_kernel(mut self, kernel: KernelProfile) -> Self {
        self.kernel = kernel;
        self
    }

    /// Creates a hardware topology profile with an explicit worker thread count and default 1GB memory.
    #[inline]
    pub fn with_workers(worker_threads: usize) -> Self {
        let cpu = CpuProfile::new(worker_threads, "explicit");
        let memory = MemoryProfile::new(1024 * 1024 * 1024, "default");
        Self::new(cpu, memory)
    }

    /// Creates a hardware topology profile with explicit worker threads and memory bytes.
    #[inline]
    pub fn with_workers_and_memory(worker_threads: usize, memory_bytes: usize) -> Self {
        let cpu = CpuProfile::new(worker_threads, "explicit");
        let memory = MemoryProfile::new(memory_bytes, "explicit");
        Self::new(cpu, memory)
    }

    /// Returns the number of worker threads allocated for processing.
    #[inline]
    pub fn worker_threads(&self) -> usize {
        self.worker_threads
    }

    /// Returns the number of available logical CPU cores on the host.
    #[inline]
    pub fn available_cores(&self) -> usize {
        self.available_cores
    }

    /// Returns the detected memory size in bytes.
    #[inline]
    pub fn memory_bytes(&self) -> usize {
        self.memory_bytes
    }

    /// Returns the classified hardware I/O acceleration tier.
    #[inline]
    pub fn acceleration_tier(&self) -> AccelerationTier {
        self.kernel.acceleration
    }

    /// Returns `true` if kernel acceleration fast-path is enabled.
    #[inline]
    pub fn is_accelerated(&self) -> bool {
        self.kernel.acceleration.is_accelerated()
    }

    /// Returns the pre-computed optimal idle connection capacity per upstream target.
    #[inline]
    pub fn optimal_max_idle_per_key(&self) -> usize {
        self.optimal_max_idle_per_key
    }

    /// Returns the pre-computed optimal concurrency shard count.
    #[inline]
    pub fn concurrency_shards(&self) -> usize {
        self.concurrency_shards
    }

    /// Resolves the evolutionary acceleration ladder selecting the highest supported kernel path per goal.
    #[inline]
    pub fn acceleration_ladder(&self) -> KernelAccelerationLadder {
        KernelAccelerationLadder::from_kernel(&self.kernel)
    }
}

static TOPOLOGY: OnceLock<HardwareTopology> = OnceLock::new();

/// Returns the global hardware topology cached in RAM.
///
/// If not previously initialized, automatically probes host environment once and caches the result.
/// Subsequent invocations are 100% in-memory with zero syscalls and zero file reads.
pub fn global_hardware_topology() -> &'static HardwareTopology {
    TOPOLOGY.get_or_init(HardwareTopology::probe)
}

/// Reads the globally cached optimal maximum idle connections per upstream key.
#[inline]
pub fn optimal_max_idle_per_key() -> usize {
    global_hardware_topology().optimal_max_idle_per_key()
}

/// Reads the globally cached concurrency shard count.
#[inline]
pub fn concurrency_shards() -> usize {
    global_hardware_topology().concurrency_shards()
}

/// Explicitly sets the global hardware topology in RAM during cold-start bootstrap.
///
/// Returns `Ok(())` if set, or `Err(existing)` if already initialized.
#[allow(clippy::result_large_err)]
pub fn init_hardware_topology(topology: HardwareTopology) -> Result<(), HardwareTopology> {
    TOPOLOGY.set(topology)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hardware_probe_returns_valid_topology() {
        let topo = HardwareTopology::probe();
        assert!(topo.available_cores() >= 1);
        assert!(topo.worker_threads() >= 1);
        assert!(topo.memory_bytes() >= 1024 * 1024);
        assert!(topo.concurrency_shards() >= 8);
        assert!(topo.optimal_max_idle_per_key() >= 128);
    }

    #[test]
    fn test_hardware_topology_with_workers() {
        let topo = HardwareTopology::with_workers(8);
        assert_eq!(topo.worker_threads(), 8);
        assert_eq!(topo.available_cores(), 8);
        assert_eq!(topo.concurrency_shards(), 16);
    }

    #[test]
    fn test_hardware_topology_with_memory() {
        let topo = HardwareTopology::with_workers_and_memory(4, 512 * 1024 * 1024);
        assert_eq!(topo.available_cores(), 4);
        assert_eq!(topo.worker_threads(), 4);
        assert_eq!(topo.memory_bytes(), 512 * 1024 * 1024);
        assert_eq!(topo.concurrency_shards(), 8);
    }

    #[test]
    fn test_orthogonal_cpu_and_memory_hardware() {
        // High CPU (16 cores), Constrained RAM (256 MB) - Compute-optimized
        let topo_compute = HardwareTopology::with_workers_and_memory(16, 256 * 1024 * 1024);
        assert_eq!(topo_compute.available_cores(), 16);
        assert_eq!(topo_compute.memory_bytes(), 256 * 1024 * 1024);
        assert_eq!(topo_compute.concurrency_shards(), 32);

        // Low CPU (2 cores), High RAM (256 GB) - Memory-optimized caching proxy
        let topo_memory = HardwareTopology::with_workers_and_memory(2, 256 * 1024 * 1024 * 1024);
        assert_eq!(topo_memory.available_cores(), 2);
        assert_eq!(topo_memory.memory_bytes(), 256 * 1024 * 1024 * 1024);
        assert_eq!(topo_memory.concurrency_shards(), 8);
    }
}
