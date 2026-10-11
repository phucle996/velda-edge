//! Traffic Engine operational configuration and tier sizing.

use velda_core::hardware::{CpuTier, MemoryTier};

use crate::tcp::TcpListenerConfig;
use crate::udp::UdpSocketConfig;

/// Operational configuration for the edge traffic engine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EngineConfig {
    /// Number of network I/O event loop workers.
    pub io_workers: usize,
    /// Maximum concurrent active connections allowed before L4 rate-limiting/drop to protect RAM.
    pub max_active_connections: usize,
    /// Depth of the administrative reconciliation command channel.
    pub reconcile_channel_capacity: usize,
    /// TCP socket options and buffer sizing.
    pub tcp: TcpListenerConfig,
    /// UDP socket options and buffer sizing.
    pub udp: UdpSocketConfig,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            io_workers: 2,
            max_active_connections: 50_000,
            reconcile_channel_capacity: 64,
            tcp: TcpListenerConfig::default(),
            udp: UdpSocketConfig::default(),
        }
    }
}

impl EngineConfig {
    /// Infers traffic engine sizing and limits from host CPU and memory tiers.
    pub fn for_tiers(cpu: CpuTier, mem: MemoryTier) -> Self {
        let (io_workers, reconcile_channel_capacity) = match cpu {
            CpuTier::Constrained => (2, 64),
            CpuTier::Small => (4, 128),
            CpuTier::Medium => (8, 256),
            CpuTier::Large => (16, 512),
            CpuTier::XLarge => (32, 1024),
            CpuTier::TwoXLarge => (64, 2048),
            CpuTier::Ultra => (128, 4096),
        };

        let max_active_connections = match mem {
            MemoryTier::Constrained => 20_000,
            MemoryTier::Small => 100_000,
            MemoryTier::Medium => 500_000,
            MemoryTier::Large => 1_000_000,
            MemoryTier::XLarge => 2_000_000,
            MemoryTier::TwoXLarge => 5_000_000,
            MemoryTier::Ultra => 10_000_000,
        };

        Self {
            io_workers,
            max_active_connections,
            reconcile_channel_capacity,
            tcp: TcpListenerConfig::for_tiers(cpu, mem),
            udp: UdpSocketConfig::for_tiers(cpu, mem),
        }
    }

    /// Infers traffic engine operational configuration directly from hardware topology.
    ///
    /// The number of network I/O workers and listener concurrency shards are set 1:1 to the exact
    /// number of available CPU cores (`available_cores().max(1)`), eliminating under-utilization
    /// on odd core topologies (e.g. 1, 3, 6, 12 vCPUs) and avoiding CPU contention or over-subscription.
    pub fn for_topology(topo: &velda_core::HardwareTopology) -> Self {
        let mut cfg = Self::for_tiers(topo.cpu_tier(), topo.memory_tier());
        let exact_cores = topo.available_cores().max(1);
        cfg.io_workers = exact_cores;
        cfg.tcp = TcpListenerConfig::for_topology(topo);
        cfg.tcp.concurrency_shards = exact_cores;
        cfg.udp = UdpSocketConfig::for_topology(topo);
        cfg.udp.concurrency_shards = exact_cores;
        cfg
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use velda_core::HardwareTopology;

    #[test]
    fn test_engine_config_exact_cores() {
        // 1 vCPU constrained VPS
        let topo_1core = HardwareTopology::with_workers_and_memory(1, 512 * 1024 * 1024);
        let cfg_1core = EngineConfig::for_topology(&topo_1core);
        assert_eq!(cfg_1core.io_workers, 1);
        assert_eq!(cfg_1core.tcp.concurrency_shards, 1);
        assert_eq!(cfg_1core.udp.concurrency_shards, 1);

        // 3 vCPUs odd core host
        let topo_3core = HardwareTopology::with_workers_and_memory(3, 4 * 1024 * 1024 * 1024);
        let cfg_3core = EngineConfig::for_topology(&topo_3core);
        assert_eq!(cfg_3core.io_workers, 3);
        assert_eq!(cfg_3core.tcp.concurrency_shards, 3);
        assert_eq!(cfg_3core.udp.concurrency_shards, 3);

        // 6 vCPUs host
        let topo_6core = HardwareTopology::with_workers_and_memory(6, 16 * 1024 * 1024 * 1024);
        let cfg_6core = EngineConfig::for_topology(&topo_6core);
        assert_eq!(cfg_6core.io_workers, 6);
        assert_eq!(cfg_6core.tcp.concurrency_shards, 6);
        assert_eq!(cfg_6core.udp.concurrency_shards, 6);
    }
}
