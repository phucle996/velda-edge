//! UDP socket configuration.

/// Configuration parameters for UDP sockets.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct UdpSocketConfig {
    /// Receive buffer size hint.
    pub recv_buffer_size: Option<usize>,
    /// Send buffer size hint.
    pub send_buffer_size: Option<usize>,
}

impl UdpSocketConfig {
    /// Creates a new configuration with defaults.
    pub fn new() -> Self {
        Self::default()
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

    /// Creates a UDP socket configuration sized appropriately for the host's [`velda_core::MemoryTier`].
    pub fn for_tier(tier: velda_core::MemoryTier) -> Self {
        const KB: usize = 1024;
        const MB: usize = 1024 * KB;
        match tier {
            velda_core::MemoryTier::Constrained => Self {
                recv_buffer_size: Some(256 * KB),
                send_buffer_size: Some(256 * KB),
            },
            velda_core::MemoryTier::Small => Self {
                recv_buffer_size: Some(512 * KB),
                send_buffer_size: Some(512 * KB),
            },
            velda_core::MemoryTier::Medium => Self {
                recv_buffer_size: Some(MB),
                send_buffer_size: Some(MB),
            },
            velda_core::MemoryTier::Large => Self {
                recv_buffer_size: Some(2 * MB),
                send_buffer_size: Some(2 * MB),
            },
            velda_core::MemoryTier::XLarge => Self {
                recv_buffer_size: Some(4 * MB),
                send_buffer_size: Some(4 * MB),
            },
            velda_core::MemoryTier::TwoXLarge => Self {
                recv_buffer_size: Some(8 * MB),
                send_buffer_size: Some(8 * MB),
            },
            velda_core::MemoryTier::Ultra => Self {
                recv_buffer_size: Some(16 * MB),
                send_buffer_size: Some(16 * MB),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_udp_config_builder() {
        let cfg = UdpSocketConfig::new()
            .with_recv_buffer_size(65536)
            .with_send_buffer_size(32768);

        assert_eq!(cfg.recv_buffer_size, Some(65536));
        assert_eq!(cfg.send_buffer_size, Some(32768));
    }

    #[test]
    fn test_udp_config_for_tier() {
        let constrained = UdpSocketConfig::for_tier(velda_core::MemoryTier::Constrained);
        assert_eq!(constrained.recv_buffer_size, Some(256 * 1024));

        let small = UdpSocketConfig::for_tier(velda_core::MemoryTier::Small);
        assert_eq!(small.recv_buffer_size, Some(512 * 1024));

        let medium = UdpSocketConfig::for_tier(velda_core::MemoryTier::Medium);
        assert_eq!(medium.recv_buffer_size, Some(1024 * 1024));

        let large = UdpSocketConfig::for_tier(velda_core::MemoryTier::Large);
        assert_eq!(large.recv_buffer_size, Some(2 * 1024 * 1024));

        let xlarge = UdpSocketConfig::for_tier(velda_core::MemoryTier::XLarge);
        assert_eq!(xlarge.recv_buffer_size, Some(4 * 1024 * 1024));

        let two_xlarge = UdpSocketConfig::for_tier(velda_core::MemoryTier::TwoXLarge);
        assert_eq!(two_xlarge.recv_buffer_size, Some(8 * 1024 * 1024));

        let ultra = UdpSocketConfig::for_tier(velda_core::MemoryTier::Ultra);
        assert_eq!(ultra.recv_buffer_size, Some(16 * 1024 * 1024));
    }
}
