//! Hardware topology and CPU concurrency profile for the Velda Edge Data Plane.
//!
//! Provides the canonical representation of available CPU cores and worker threads
//! probed once and cached in RAM (`OnceLock`).
//!
//! Strict Invariant:
//! - This module ONLY owns host hardware inspection and thread concurrency.
//! - Subsystem-specific sizing (such as shard counts or connection pool capacities)
//!   MUST be calculated by the respective subsystem crates (`velda-lb`, `velda-connection-pool`),
//!   NOT here.

use std::sync::OnceLock;

/// Hardware topology profile probed once and cached in RAM.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HardwareTopology {
    /// Number of logical CPU cores available to the process (respects container cgroups limits).
    pub available_cores: usize,
    /// Number of worker threads allocated for traffic processing.
    pub worker_threads: usize,
}

impl HardwareTopology {
    /// Probes the host hardware environment during bootstrap.
    ///
    /// Respects container CPU limits (cgroups) via `std::thread::available_parallelism`.
    /// Allows operator override via `VELDA_WORKER_THREADS` environment variable.
    pub fn probe() -> Self {
        let available_cores = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);

        let worker_threads = std::env::var("VELDA_WORKER_THREADS")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|&w| w > 0)
            .unwrap_or(available_cores);

        Self {
            available_cores,
            worker_threads,
        }
    }

    /// Creates a hardware topology profile with an explicit worker thread count.
    #[inline]
    pub const fn with_workers(worker_threads: usize) -> Self {
        Self {
            available_cores: worker_threads,
            worker_threads,
        }
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
    }

    #[test]
    fn test_hardware_topology_with_workers() {
        let topo = HardwareTopology::with_workers(8);
        assert_eq!(topo.worker_threads(), 8);
        assert_eq!(topo.available_cores(), 8);
    }

    #[test]
    fn test_global_hardware_topology_cached_in_ram() {
        let t1 = global_hardware_topology();
        let t2 = global_hardware_topology();
        assert_eq!(t1 as *const _, t2 as *const _);
    }
}
