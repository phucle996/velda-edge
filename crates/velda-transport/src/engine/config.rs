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
            CpuTier::Constrained => (1, 32),
            CpuTier::Small => (2, 64),
            CpuTier::Medium => (4, 128),
            CpuTier::Large => (8, 256),
            CpuTier::XLarge => (16, 512),
            CpuTier::TwoXLarge => (32, 1024),
            CpuTier::Ultra => (64, 2048),
        };

        let max_active_connections = match mem {
            MemoryTier::Constrained => 10_000,
            MemoryTier::Small => 50_000,
            MemoryTier::Medium => 200_000,
            MemoryTier::Large => 500_000,
            MemoryTier::XLarge => 1_000_000,
            MemoryTier::TwoXLarge => 2_000_000,
            MemoryTier::Ultra => 4_000_000,
        };

        Self {
            io_workers,
            max_active_connections,
            reconcile_channel_capacity,
            tcp: TcpListenerConfig::for_tiers(cpu, mem),
            udp: UdpSocketConfig::for_tier(mem),
        }
    }
}
