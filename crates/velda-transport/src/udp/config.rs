//! UDP socket configuration.

/// Configuration parameters for UDP sockets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UdpSocketConfig {
    /// Receive buffer size hint.
    pub recv_buffer_size: Option<usize>,
    /// Send buffer size hint.
    pub send_buffer_size: Option<usize>,
    /// Enable `SO_REUSEPORT` for multi-socket kernel datagram hashing.
    pub reuseport: bool,
    /// Number of socket shards bound to the same port via `SO_REUSEPORT`.
    pub concurrency_shards: usize,
    /// Enables non-local IP binding (`IP_FREEBIND`), allowing UDP listeners to bind
    /// to floating VIPs and Anycast addresses prior to interface assignment during HA failovers.
    pub freebind: bool,
    /// Enables Generic Receive Offload (`UDP_GRO`), batching incoming datagrams into 64KB buffers.
    pub gro: bool,
    /// Enables socket queue overflow monitoring (`SO_RXQ_OVFL`) to trace dropped datagrams.
    pub rxq_ovfl: bool,
    /// Instructs kernel NAPI to suppress IRQs and prioritize polling (`SO_PREFER_BUSY_POLL`
    /// + `SO_BUSY_POLL_BUDGET`, Linux >= 5.11) on high-core latency-critical tiers.
    pub prefer_busy_poll: bool,
    /// Enables Generic Segmentation Offload (`UDP_SEGMENT`, Linux >= 4.18),
    /// offloading datagram segmentation up to 64KB to kernel or NIC hardware.
    pub gso: bool,
}

impl Default for UdpSocketConfig {
    fn default() -> Self {
        Self {
            recv_buffer_size: None,
            send_buffer_size: None,
            reuseport: cfg!(unix),
            concurrency_shards: 1,
            freebind: false,
            gro: false,
            rxq_ovfl: false,
            prefer_busy_poll: false,
            gso: false,
        }
    }
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

    /// Sets whether to enable `IP_FREEBIND` on UDP sockets (Linux).
    pub fn with_freebind(mut self, freebind: bool) -> Self {
        self.freebind = freebind;
        self
    }

    /// Sets whether to enable `UDP_GRO` on UDP sockets (Linux).
    pub fn with_gro(mut self, gro: bool) -> Self {
        self.gro = gro;
        self
    }

    /// Sets whether to enable `SO_RXQ_OVFL` on UDP sockets (Linux).
    pub fn with_rxq_ovfl(mut self, rxq_ovfl: bool) -> Self {
        self.rxq_ovfl = rxq_ovfl;
        self
    }

    /// Sets whether to enable `SO_PREFER_BUSY_POLL` on UDP sockets (Linux).
    pub fn with_prefer_busy_poll(mut self, enabled: bool) -> Self {
        self.prefer_busy_poll = enabled;
        self
    }

    /// Sets whether to enable `UDP_SEGMENT` (GSO) on UDP sockets (Linux).
    pub fn with_gso(mut self, enabled: bool) -> Self {
        self.gso = enabled;
        self
    }

    /// Sets whether to enable `SO_REUSEPORT`.
    pub fn with_reuseport(mut self, reuseport: bool) -> Self {
        self.reuseport = reuseport;
        self
    }

    /// Sets the number of socket shards bound via `SO_REUSEPORT`.
    pub fn with_concurrency_shards(mut self, shards: usize) -> Self {
        self.concurrency_shards = shards.max(1);
        self
    }

    /// Pre-compiles UDP socket acceleration settings from probed hardware topology and evolutionary ladder.
    pub fn for_topology(topo: &velda_core::HardwareTopology) -> Self {
        let mut cfg = Self {
            recv_buffer_size: Some(topo.sizing.udp_recv_buffer_size),
            send_buffer_size: Some(topo.sizing.udp_send_buffer_size),
            concurrency_shards: topo.available_cores().max(1),
            ..Self::default()
        };
        let ladder = topo.acceleration_ladder();
        if ladder.udp_offload == velda_core::UdpOffloadTier::GenericReceiveOffload {
            cfg.gro = true;
            cfg.rxq_ovfl = true;
        } else if ladder.udp_offload == velda_core::UdpOffloadTier::QueueMonitored {
            cfg.rxq_ovfl = true;
        }
        if ladder.udp_egress >= velda_core::UdpEgressTier::GenericSegmentationOffload {
            cfg.gso = true;
        }
        if ladder.busy_poll >= velda_core::BusyPollTier::PreferBusyPoll {
            cfg.prefer_busy_poll = true;
        }
        cfg
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_udp_config_builder() {
        let cfg = UdpSocketConfig::new()
            .with_recv_buffer_size(65536)
            .with_send_buffer_size(32768)
            .with_freebind(true)
            .with_gro(true)
            .with_rxq_ovfl(true);

        assert_eq!(cfg.recv_buffer_size, Some(65536));
        assert_eq!(cfg.send_buffer_size, Some(32768));
        assert!(cfg.freebind);
        assert!(cfg.gro);
        assert!(cfg.rxq_ovfl);
    }

    #[test]
    fn test_udp_config_for_topology() {
        let topo = velda_core::HardwareTopology::with_workers_and_memory(4, 4 * 1024 * 1024 * 1024);
        let cfg = UdpSocketConfig::for_topology(&topo);
        assert_eq!(cfg.concurrency_shards, 4);
        assert!(cfg.recv_buffer_size.is_some());
    }

    #[test]
    fn test_udp_evolutionary_ladder_for_topology() {
        use velda_core::hardware::{
            AccelerationTier, HardwareTopology, KernelProfile, KernelVersion,
        };

        let kernel_5_11 = KernelProfile::new(
            KernelVersion::new(5, 11, 0),
            AccelerationTier::Standard,
            "test_5_11",
        );
        let topo = HardwareTopology::with_workers_and_memory(16, 16 * 1024 * 1024 * 1024)
            .with_kernel(kernel_5_11);

        let cfg = UdpSocketConfig::for_topology(&topo);
        assert!(cfg.gso);
        assert!(cfg.prefer_busy_poll);

        let builder_cfg = UdpSocketConfig::new()
            .with_gso(true)
            .with_prefer_busy_poll(true);
        assert!(builder_cfg.gso);
        assert!(builder_cfg.prefer_busy_poll);
    }
}
