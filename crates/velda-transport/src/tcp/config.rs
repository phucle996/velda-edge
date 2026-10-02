//! TCP listener and socket configuration.

use std::time::Duration;

/// Configuration parameters for TCP listeners and accepted sockets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TcpListenerConfig {
    /// Enable `TCP_NODELAY` (disable Nagle's algorithm) on accepted connections.
    pub nodelay: bool,
    /// Keepalive probe interval.
    pub keepalive: Option<Duration>,
    /// Socket listen backlog depth.
    pub backlog: u32,
    /// Receive buffer size hint.
    pub recv_buffer_size: Option<usize>,
    /// Send buffer size hint.
    pub send_buffer_size: Option<usize>,
    /// Chunk buffer size for bidirectional L4 byte proxying.
    pub copy_buffer_size: usize,
}

impl Default for TcpListenerConfig {
    fn default() -> Self {
        Self {
            nodelay: true,
            keepalive: Some(Duration::from_secs(60)),
            backlog: 1024,
            recv_buffer_size: None,
            send_buffer_size: None,
            copy_buffer_size: 16 * 1024,
        }
    }
}

impl TcpListenerConfig {
    /// Creates a new configuration with sensible high-performance defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets whether to enable `TCP_NODELAY`.
    pub fn with_nodelay(mut self, nodelay: bool) -> Self {
        self.nodelay = nodelay;
        self
    }

    /// Sets the buffer size used for L4 direct byte forwarding.
    pub fn with_copy_buffer_size(mut self, size: usize) -> Self {
        self.copy_buffer_size = size;
        self
    }

    /// Sets the socket listen backlog size.
    pub fn with_backlog(mut self, backlog: u32) -> Self {
        self.backlog = backlog;
        self
    }

    /// Sets the TCP keepalive duration.
    pub fn with_keepalive(mut self, keepalive: Option<Duration>) -> Self {
        self.keepalive = keepalive;
        self
    }

    /// Sets the receive buffer size hint.
    pub fn with_recv_buffer_size(mut self, size: usize) -> Self {
        self.recv_buffer_size = Some(size);
        self
    }

    /// Sets the send buffer size hint.
    pub fn with_send_buffer_size(mut self, size: usize) -> Self {
        self.send_buffer_size = Some(size);
        self
    }

    /// Creates a TCP listener configuration sized appropriately for the host's [`velda_core::MemoryTier`].
    pub fn for_tier(tier: velda_core::MemoryTier) -> Self {
        const KB: usize = 1024;
        const MB: usize = 1024 * KB;
        match tier {
            velda_core::MemoryTier::Constrained => Self {
                nodelay: true,
                keepalive: Some(Duration::from_secs(60)),
                backlog: 512,
                recv_buffer_size: None,
                send_buffer_size: None,
                copy_buffer_size: 8 * KB,
            },
            velda_core::MemoryTier::Small => Self {
                nodelay: true,
                keepalive: Some(Duration::from_secs(60)),
                backlog: 1024,
                recv_buffer_size: Some(128 * KB),
                send_buffer_size: Some(128 * KB),
                copy_buffer_size: 16 * KB,
            },
            velda_core::MemoryTier::Medium => Self {
                nodelay: true,
                keepalive: Some(Duration::from_secs(60)),
                backlog: 2048,
                recv_buffer_size: Some(256 * KB),
                send_buffer_size: Some(256 * KB),
                copy_buffer_size: 16 * KB,
            },
            velda_core::MemoryTier::Large => Self {
                nodelay: true,
                keepalive: Some(Duration::from_secs(60)),
                backlog: 4096,
                recv_buffer_size: Some(512 * KB),
                send_buffer_size: Some(512 * KB),
                copy_buffer_size: 32 * KB,
            },
            velda_core::MemoryTier::XLarge => Self {
                nodelay: true,
                keepalive: Some(Duration::from_secs(60)),
                backlog: 8192,
                recv_buffer_size: Some(MB),
                send_buffer_size: Some(MB),
                copy_buffer_size: 32 * KB,
            },
            velda_core::MemoryTier::TwoXLarge => Self {
                nodelay: true,
                keepalive: Some(Duration::from_secs(60)),
                backlog: 16384,
                recv_buffer_size: Some(2 * MB),
                send_buffer_size: Some(2 * MB),
                copy_buffer_size: 64 * KB,
            },
            velda_core::MemoryTier::Ultra => Self {
                nodelay: true,
                keepalive: Some(Duration::from_secs(60)),
                backlog: 32768,
                recv_buffer_size: Some(4 * MB),
                send_buffer_size: Some(4 * MB),
                copy_buffer_size: 64 * KB,
            },
        }
    }

    /// Returns the recommended L4 copy buffer size for a given [`velda_core::CpuTier`].
    pub fn copy_buffer_size_for_cpu_tier(tier: velda_core::CpuTier) -> usize {
        const KB: usize = 1024;
        match tier {
            velda_core::CpuTier::Constrained => 8 * KB,
            velda_core::CpuTier::Small => 16 * KB,
            velda_core::CpuTier::Medium => 16 * KB,
            velda_core::CpuTier::Large => 32 * KB,
            velda_core::CpuTier::XLarge => 32 * KB,
            velda_core::CpuTier::TwoXLarge => 64 * KB,
            velda_core::CpuTier::Ultra => 64 * KB,
        }
    }

    /// Creates a TCP listener configuration combining CPU-driven and Memory-driven tiers.
    pub fn for_tiers(cpu: velda_core::CpuTier, mem: velda_core::MemoryTier) -> Self {
        let mut cfg = Self::for_tier(mem);
        cfg.copy_buffer_size = Self::copy_buffer_size_for_cpu_tier(cpu);
        cfg
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let cfg = TcpListenerConfig::default();
        assert!(cfg.nodelay);
        assert_eq!(cfg.backlog, 1024);
        assert_eq!(cfg.keepalive, Some(Duration::from_secs(60)));
        assert_eq!(cfg.recv_buffer_size, None);
        assert_eq!(cfg.send_buffer_size, None);
    }

    #[test]
    fn test_builder_pattern() {
        let cfg = TcpListenerConfig::new()
            .with_nodelay(false)
            .with_backlog(2048)
            .with_keepalive(None)
            .with_recv_buffer_size(65536)
            .with_send_buffer_size(32768);

        assert!(!cfg.nodelay);
        assert_eq!(cfg.backlog, 2048);
        assert_eq!(cfg.keepalive, None);
        assert_eq!(cfg.recv_buffer_size, Some(65536));
        assert_eq!(cfg.send_buffer_size, Some(32768));
    }

    #[test]
    fn test_tcp_config_for_tier() {
        let constrained = TcpListenerConfig::for_tier(velda_core::MemoryTier::Constrained);
        assert_eq!(constrained.backlog, 512);
        assert_eq!(constrained.recv_buffer_size, None);

        let small = TcpListenerConfig::for_tier(velda_core::MemoryTier::Small);
        assert_eq!(small.backlog, 1024);
        assert_eq!(small.recv_buffer_size, Some(128 * 1024));

        let medium = TcpListenerConfig::for_tier(velda_core::MemoryTier::Medium);
        assert_eq!(medium.backlog, 2048);
        assert_eq!(medium.recv_buffer_size, Some(256 * 1024));

        let large = TcpListenerConfig::for_tier(velda_core::MemoryTier::Large);
        assert_eq!(large.backlog, 4096);
        assert_eq!(large.recv_buffer_size, Some(512 * 1024));

        let xlarge = TcpListenerConfig::for_tier(velda_core::MemoryTier::XLarge);
        assert_eq!(xlarge.backlog, 8192);
        assert_eq!(xlarge.recv_buffer_size, Some(1024 * 1024));

        let two_xlarge = TcpListenerConfig::for_tier(velda_core::MemoryTier::TwoXLarge);
        assert_eq!(two_xlarge.backlog, 16384);
        assert_eq!(two_xlarge.recv_buffer_size, Some(2 * 1024 * 1024));

        let ultra = TcpListenerConfig::for_tier(velda_core::MemoryTier::Ultra);
        assert_eq!(ultra.backlog, 32768);
        assert_eq!(ultra.recv_buffer_size, Some(4 * 1024 * 1024));
    }

    #[test]
    fn test_copy_buffer_size_for_cpu_tier() {
        assert_eq!(
            TcpListenerConfig::copy_buffer_size_for_cpu_tier(velda_core::CpuTier::Constrained),
            8 * 1024
        );
        assert_eq!(
            TcpListenerConfig::copy_buffer_size_for_cpu_tier(velda_core::CpuTier::Small),
            16 * 1024
        );
        assert_eq!(
            TcpListenerConfig::copy_buffer_size_for_cpu_tier(velda_core::CpuTier::Medium),
            16 * 1024
        );
        assert_eq!(
            TcpListenerConfig::copy_buffer_size_for_cpu_tier(velda_core::CpuTier::Large),
            32 * 1024
        );
        assert_eq!(
            TcpListenerConfig::copy_buffer_size_for_cpu_tier(velda_core::CpuTier::XLarge),
            32 * 1024
        );
        assert_eq!(
            TcpListenerConfig::copy_buffer_size_for_cpu_tier(velda_core::CpuTier::TwoXLarge),
            64 * 1024
        );
        assert_eq!(
            TcpListenerConfig::copy_buffer_size_for_cpu_tier(velda_core::CpuTier::Ultra),
            64 * 1024
        );
    }

    #[test]
    fn test_tcp_config_for_tiers_combines_cpu_and_mem() {
        let cfg =
            TcpListenerConfig::for_tiers(velda_core::CpuTier::Ultra, velda_core::MemoryTier::Small);
        assert_eq!(cfg.copy_buffer_size, 64 * 1024); // from Ultra CPU
        assert_eq!(cfg.backlog, 1024); // from Small Memory
        assert_eq!(cfg.recv_buffer_size, Some(128 * 1024)); // from Small Memory
    }
}
