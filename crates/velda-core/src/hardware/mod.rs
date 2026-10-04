//! Hardware topology and concurrency profile for the Velda Edge Data Plane.
//!
//! Provides the canonical representation of available CPU cores and worker threads
//! probed once and cached in RAM (`OnceLock`).
//!
//! # Orthogonal Tier Architecture:
//! - [`CpuTier`]: Driven by CPU core count and cgroups CPU bandwidth.
//!   Controls worker thread count, channel capacity, and concurrency sharding.
//! - [`MemoryTier`]: Driven by RAM capacity and cgroups memory limits.
//!   Controls socket kernel buffers, backlog depth, and in-memory cache capacity.
//!
//! Strict Invariant:
//! - This module ONLY owns host hardware inspection and concurrency profiling.
//! - Subsystem-specific sizing (such as shard counts, socket buffers, or connection pool capacities)
//!   MUST be calculated by the respective subsystem crates (`velda-transport`, `velda-discovery`),
//!   NOT hardcoded here.

pub mod cpu;
pub mod kernel;
pub mod memory;

use std::sync::OnceLock;

pub use cpu::{CpuProfile, CpuTier, probe_cpu};
pub use kernel::{AccelerationTier, KernelProfile, KernelVersion, probe_kernel};
pub use memory::{MemoryProfile, MemoryTier, probe_memory};

/// Error returned when a hardware tier string fails to parse.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseTierError(pub String);

impl std::fmt::Display for ParseTierError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for ParseTierError {}

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
}

impl HardwareTopology {
    /// Probes the host hardware environment during cold-start bootstrap.
    ///
    /// Respects container CPU/memory limits (cgroups v1 & v2) and environment overrides.
    pub fn probe() -> Self {
        let cpu = probe_cpu();
        let memory = probe_memory();
        let kernel = probe_kernel();

        Self {
            cpu,
            memory,
            kernel,
            available_cores: cpu.available_cores,
            worker_threads: cpu.available_cores,
            memory_bytes: memory.total_bytes,
        }
    }

    /// Creates a hardware topology profile from explicit CPU and Memory profiles.
    #[inline]
    pub fn new(cpu: CpuProfile, memory: MemoryProfile) -> Self {
        let kernel = probe_kernel();
        Self {
            cpu,
            memory,
            kernel,
            available_cores: cpu.available_cores,
            worker_threads: cpu.available_cores,
            memory_bytes: memory.total_bytes,
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

    /// Returns the classified CPU resource tier.
    #[inline]
    pub fn cpu_tier(&self) -> CpuTier {
        self.cpu.tier
    }

    /// Returns the classified memory resource tier.
    #[inline]
    pub fn memory_tier(&self) -> MemoryTier {
        self.memory.tier
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
}

static TOPOLOGY: OnceLock<HardwareTopology> = OnceLock::new();

/// Returns the global hardware topology cached in RAM.
///
/// If not previously initialized, automatically probes host environment once and caches the result.
/// Subsequent invocations are 100% in-memory with zero syscalls and zero file reads.
pub fn global_hardware_topology() -> &'static HardwareTopology {
    TOPOLOGY.get_or_init(HardwareTopology::probe)
}

/// Explicitly sets the global hardware topology in RAM during cold-start bootstrap.
///
/// Returns `Ok(())` if set, or `Err(existing)` if already initialized.
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
        assert_eq!(topo.cpu_tier(), topo.cpu.tier);
        assert_eq!(topo.memory_tier(), topo.memory.tier);
    }

    #[test]
    fn test_hardware_topology_with_workers() {
        let topo = HardwareTopology::with_workers(8);
        assert_eq!(topo.worker_threads(), 8);
        assert_eq!(topo.available_cores(), 8);
        assert_eq!(topo.cpu_tier(), CpuTier::Medium);
    }

    #[test]
    fn test_hardware_topology_with_memory() {
        let topo = HardwareTopology::with_workers_and_memory(4, 512 * 1024 * 1024);
        assert_eq!(topo.available_cores(), 4);
        assert_eq!(topo.worker_threads(), 4);
        assert_eq!(topo.memory_bytes(), 512 * 1024 * 1024);
        assert_eq!(topo.cpu_tier(), CpuTier::Small);
        assert_eq!(topo.memory_tier(), MemoryTier::Small);
    }

    #[test]
    fn test_orthogonal_cpu_and_memory_tiers() {
        // High CPU (16 cores), Constrained RAM (256 MB) - Compute-optimized
        let topo_compute = HardwareTopology::with_workers_and_memory(16, 256 * 1024 * 1024);
        assert_eq!(topo_compute.cpu_tier(), CpuTier::Large);
        assert_eq!(topo_compute.memory_tier(), MemoryTier::Constrained);

        // Low CPU (2 cores), Ultra RAM (256 GB) - Memory-optimized caching proxy
        let topo_memory = HardwareTopology::with_workers_and_memory(2, 256 * 1024 * 1024 * 1024);
        assert_eq!(topo_memory.cpu_tier(), CpuTier::Constrained);
        assert_eq!(topo_memory.memory_tier(), MemoryTier::Ultra);
    }
}
