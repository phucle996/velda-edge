//! Hardware Core Partitioning & Boundary Manager.
//!
//! Provides a unified, zero-waste CPU core allocation architecture for Velda Edge.
//!
//! # Architectural Rationale: All-Worker Balanced Sharding & Idle CPU Harvesting
//!
//! In legacy proxy architectures, systems often arbitrarily reserve one or more dedicated CPU cores
//! exclusively for background/auxiliary tasks (e.g. Prometheus metrics exporting, health probes,
//! buffer pool sweeping, and IPC administration). In high-performance edge computing, this approach
//! introduces severe operational drawbacks:
//!
//! 1. **Massive Capacity Waste on Small/Medium Hosts**:
//!    Reserving 1 core on a 2-core host burns 50% of the server's network throughput.
//!    Reserving 1 core on a 4-core host burns 25% of the server's network throughput.
//!    Even on larger servers, background tasks such as Prometheus scraping execute intermittently
//!    (e.g. once every 5–15 seconds), causing an isolated core to sit idle >99% of the time.
//!
//! 2. **Elimination of Arbitrary Mode Thresholds**:
//!    Earlier designs partitioned core allocation into artificial modes (`Dedicated`, `Balanced`,
//!    `Cooperative`) with arbitrary core count thresholds (e.g. `< 5` vs `>= 5`). This created
//!    accidental complexity, inconsistent latency characteristics, and branching overhead.
//!
//! 3. **The Velda Unified Model: 100% Cores for Traffic, Auxiliary on Idle Cycles**:
//!    - **100% Traffic Core Allocation**: Every permitted CPU core is allocated to a dedicated,
//!      core-pinned Data Plane worker thread running a single-threaded Tokio runtime (`new_current_thread()`).
//!    - **Floating Auxiliary Tasks**: Auxiliary services (Prometheus Exporter, IPC Server, Buffer Sweeper,
//!      Overload Monitor, Access Log Flusher) execute cooperatively as floating background tasks.
//!    - **Zero-Contention Kernel Scheduling (`SCHED_IDLE` / `nice(19)`)**:
//!      Auxiliary threads operate under the Linux `SCHED_IDLE` scheduling class (or POSIX `nice(19)`).
//!      The Linux Completely Fair Scheduler (CFS) guarantees that auxiliary tasks ONLY receive CPU slices
//!      when there are NO runnable normal-priority worker threads (e.g. during network I/O wait).
//!    - **Lock-Free Cacheline Isolation**:
//!      Shared metrics structures ([`velda_observability::PipelineSlot`]) are explicitly 64-byte aligned
//!      (`#[repr(align(64))]`), preventing False Sharing and ensuring auxiliary reads never contend
//!      with or stall traffic worker cachelines.

/// Core allocation strategy mapping host hardware cores directly to Data Plane traffic workers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CorePartition {
    /// Full list of logical CPU core IDs permitted by the host OS and container cgroups.
    pub allowed_cores: Vec<usize>,

    /// Logical core IDs dedicated strictly to Data Plane traffic workers (100% of allowed cores).
    ///
    /// # Invariant
    /// Every allowed core is assigned a traffic worker thread affined via `sched_setaffinity`.
    /// Worker threads execute single-threaded Tokio event loops to maximize network I/O throughput.
    pub traffic_cores: Vec<usize>,
}

impl CorePartition {
    /// Derives the zero-waste hardware core partitioning plan from allowed OS/cgroups core IDs.
    ///
    /// # Sizing Rules
    /// - **Empty input**: Defaults safely to `[0]` (1 traffic worker on Core 0).
    /// - **N Cores**: Allocates all `N` cores to `traffic_cores` (100% edge capacity).
    ///   Auxiliary tasks run floating on unpinned supervisor threads utilizing idle CPU cycles.
    pub fn plan(allowed: &[usize]) -> Self {
        let cores = if allowed.is_empty() {
            vec![0]
        } else {
            allowed.to_vec()
        };

        Self {
            allowed_cores: cores.clone(),
            traffic_cores: cores,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_core_partition_empty_fallback() {
        let p = CorePartition::plan(&[]);
        assert_eq!(p.allowed_cores, vec![0]);
        assert_eq!(p.traffic_cores, vec![0]);
    }

    #[test]
    fn test_core_partition_single_core() {
        let p = CorePartition::plan(&[0]);
        assert_eq!(p.allowed_cores, vec![0]);
        assert_eq!(p.traffic_cores, vec![0]);
    }

    #[test]
    fn test_core_partition_two_cores_all_traffic() {
        let p = CorePartition::plan(&[0, 1]);
        assert_eq!(p.allowed_cores, vec![0, 1]);
        assert_eq!(p.traffic_cores, vec![0, 1]);
    }

    #[test]
    fn test_core_partition_four_cores_all_traffic() {
        let p = CorePartition::plan(&[0, 1, 2, 3]);
        assert_eq!(p.allowed_cores, vec![0, 1, 2, 3]);
        assert_eq!(p.traffic_cores, vec![0, 1, 2, 3]);
    }

    #[test]
    fn test_core_partition_eight_cores_all_traffic_with_arbitrary_ids() {
        let allowed = vec![10, 11, 12, 13, 14, 15, 16, 17];
        let p = CorePartition::plan(&allowed);
        assert_eq!(p.allowed_cores, allowed);
        assert_eq!(p.traffic_cores, allowed);
    }
}
