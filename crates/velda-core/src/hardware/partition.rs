//! Hardware Core Partitioning & Boundary Manager.
//!
//! Enforces an impermeable architectural boundary between:
//! 1. **Data Plane (Traffic Cores)**: High-throughput, zero-allocation network I/O workers.
//! 2. **Management Plane (Auxiliary Cores)**: Cold-path administration, supervisor IPC,
//!    overload monitoring, OS signal traps, and Prometheus telemetry exporting.
//!
//! # Strict Subsystem Boundary Invariants
//! ```text
//! ========================================================================================
//!                          HARD CORE BOUNDARY INVARIANTS
//! ========================================================================================
//!   [ DATA PLANE: TRAFFIC CORES ]              [ MANAGEMENT PLANE: AUXILIARY CORES ]
//!   - Owns: TCP/UDP listener sockets           - Owns: Prometheus HTTP/TLS listener (:9090)
//!   - Owns: SO_REUSEPORT + SO_INCOMING_CPU     - Owns: Unix Domain Socket IPC (velda-sync)
//!   - Owns: L7 HTTP/1/2/3 & gRPC engines       - Owns: Overload Monitor & OOM protection
//!   - Owns: Upstream connection pool leasing   - Owns: SIGINT / SIGTERM signal trapping
//!   - Owns: 36 cacheline-aligned metrics slots - Owns: Healthcheck / readiness probes
//!
//!   STRICT INVARIANTS:                         STRICT INVARIANTS:
//!   x MUST NEVER serve /metrics endpoint       x MUST NEVER poll client traffic sockets
//!   x MUST NEVER format OpenMetrics text       x MUST NEVER execute proxy route dispatch
//!   x MUST NEVER poll IPC reload socket        x MUST NEVER allocate memory on traffic hot-path
//! ========================================================================================
//! ```

/// Operating mode derived for hardware core allocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CorePartitionMode {
    /// Dedicated Cores (Host has >= 4 cores):
    /// Traffic workers are assigned strictly to cores `0 .. N-2`.
    /// Auxiliary tasks (Supervisor, IPC, Exporter, Signals) are isolated strictly on core `N-1`.
    /// L1/L2 caches of traffic workers remain 100% clean and unpolluted by telemetry formatting.
    Dedicated,

    /// Balanced Sharding (Host has 2 to 3 cores):
    /// 100% of available cores are assigned to Traffic Workers to maximize edge network throughput.
    /// Auxiliary services run on the Supervisor thread unpinned, utilizing idle/I-O wait cycles
    /// without starving any core or wasting a large fraction of system capacity.
    Balanced,

    /// Single Core Cooperative (Host has exactly 1 core):
    /// Traffic workers and Auxiliary services co-exist on Core 0.
    /// Concurrency is managed cooperatively via async Tokio task yields without thread pinning.
    Cooperative,
}

/// Core allocation strategy and physical partition mapping between
/// Data Plane Traffic Workers and Management Plane Auxiliary services.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CorePartition {
    /// Full list of logical CPU core IDs permitted by the host OS and container cgroups.
    pub allowed_cores: Vec<usize>,

    /// Logical core IDs dedicated strictly to Data Plane traffic workers.
    ///
    /// # Invariant
    /// Worker threads pinned to these cores MUST NEVER bind, poll, or serve auxiliary endpoints.
    pub traffic_cores: Vec<usize>,

    /// Logical core IDs dedicated to Auxiliary/Management services (Supervisor, Exporter, IPC).
    ///
    /// # Invariant
    /// Auxiliary tasks pinned to these cores MUST NEVER accept or forward client API traffic.
    pub auxiliary_cores: Vec<usize>,

    /// Operational partitioning mode resolved for the host hardware topology.
    pub mode: CorePartitionMode,

    /// Whether auxiliary tasks share execution with traffic worker cores.
    pub is_shared: bool,
}

impl CorePartition {
    /// Derives the optimal core partitioning strategy from allowed OS/cgroups core IDs.
    ///
    /// # Sizing Rules
    /// - **Empty input**: Defaults safely to `[0]` in [`CorePartitionMode::Cooperative`].
    /// - **1 Core**: `traffic = [core_0]`, `auxiliary = [core_0]`, `is_shared = true`.
    /// - **2..3 Cores**: `traffic = [cores]`, `auxiliary = []`, `is_shared = true` (Balanced).
    /// - **>= 4 Cores**: `traffic = [cores[0..N-1]]`, `auxiliary = [cores[N-1]]`, `is_shared = false` (Dedicated).
    pub fn plan(allowed: &[usize]) -> Self {
        if allowed.is_empty() {
            return Self {
                allowed_cores: vec![0],
                traffic_cores: vec![0],
                auxiliary_cores: vec![0],
                mode: CorePartitionMode::Cooperative,
                is_shared: true,
            };
        }

        let total = allowed.len();

        match total {
            1 => Self {
                allowed_cores: allowed.to_vec(),
                traffic_cores: vec![allowed[0]],
                auxiliary_cores: vec![allowed[0]],
                mode: CorePartitionMode::Cooperative,
                is_shared: true,
            },
            2..=4 => Self {
                allowed_cores: allowed.to_vec(),
                traffic_cores: allowed.to_vec(),
                auxiliary_cores: Vec::new(),
                mode: CorePartitionMode::Balanced,
                is_shared: true,
            },
            _ => {
                // >= 5 Cores: Split strictly. Cores 0..N-2 for Traffic, Core N-1 for Auxiliary.
                let split_idx = total - 1;
                let traffic = allowed[..split_idx].to_vec();
                let auxiliary = vec![allowed[split_idx]];

                Self {
                    allowed_cores: allowed.to_vec(),
                    traffic_cores: traffic,
                    auxiliary_cores: auxiliary,
                    mode: CorePartitionMode::Dedicated,
                    is_shared: false,
                }
            }
        }
    }

    /// Returns the dedicated auxiliary core ID if running in Dedicated mode.
    #[inline(always)]
    pub fn dedicated_auxiliary_core(&self) -> Option<usize> {
        if !self.is_shared && !self.auxiliary_cores.is_empty() {
            Some(self.auxiliary_cores[0])
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_core_partition_empty_fallback() {
        let p = CorePartition::plan(&[]);
        assert_eq!(p.mode, CorePartitionMode::Cooperative);
        assert_eq!(p.traffic_cores, vec![0]);
        assert_eq!(p.auxiliary_cores, vec![0]);
        assert!(p.is_shared);
        assert_eq!(p.dedicated_auxiliary_core(), None);
    }

    #[test]
    fn test_core_partition_single_core() {
        let p = CorePartition::plan(&[0]);
        assert_eq!(p.mode, CorePartitionMode::Cooperative);
        assert_eq!(p.traffic_cores, vec![0]);
        assert_eq!(p.auxiliary_cores, vec![0]);
        assert!(p.is_shared);
        assert_eq!(p.dedicated_auxiliary_core(), None);
    }

    #[test]
    fn test_core_partition_two_cores_balanced() {
        let p = CorePartition::plan(&[0, 1]);
        assert_eq!(p.mode, CorePartitionMode::Balanced);
        assert_eq!(p.traffic_cores, vec![0, 1]);
        assert!(p.auxiliary_cores.is_empty());
        assert!(p.is_shared);
        assert_eq!(p.dedicated_auxiliary_core(), None);
    }

    #[test]
    fn test_core_partition_three_cores_balanced() {
        let p = CorePartition::plan(&[0, 1, 2]);
        assert_eq!(p.mode, CorePartitionMode::Balanced);
        assert_eq!(p.traffic_cores, vec![0, 1, 2]);
        assert!(p.auxiliary_cores.is_empty());
        assert!(p.is_shared);
    }

    #[test]
    fn test_core_partition_four_cores_balanced() {
        let p = CorePartition::plan(&[0, 1, 2, 3]);
        assert_eq!(p.mode, CorePartitionMode::Balanced);
        assert_eq!(p.traffic_cores, vec![0, 1, 2, 3]);
        assert!(p.auxiliary_cores.is_empty());
        assert!(p.is_shared);
        assert_eq!(p.dedicated_auxiliary_core(), None);
    }

    #[test]
    fn test_core_partition_eight_cores_dedicated_with_arbitrary_ids() {
        let allowed = vec![10, 11, 12, 13, 14, 15, 16, 17];
        let p = CorePartition::plan(&allowed);
        assert_eq!(p.mode, CorePartitionMode::Dedicated);
        assert_eq!(p.traffic_cores, vec![10, 11, 12, 13, 14, 15, 16]);
        assert_eq!(p.auxiliary_cores, vec![17]);
        assert!(!p.is_shared);
        assert_eq!(p.dedicated_auxiliary_core(), Some(17));
    }
}
