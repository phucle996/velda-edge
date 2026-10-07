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
    /// Distributes incoming SYN packets across independent worker threads directly in the Linux
    /// kernel via 4-tuple hashing (`SO_REUSEPORT`), eliminating single-acceptor lock bottlenecks.
    pub reuseport: bool,
    /// Scales listener socket count to match hardware CPU concurrency, preventing worker threads
    /// from contending on a single kernel listen queue.
    pub concurrency_shards: usize,
    /// Immediately acknowledges request headers/preambles (`TCP_QUICKACK`) to avoid 40ms delayed-ACK
    /// penalties with clients running Nagle's algorithm.
    pub quickack: bool,
    /// Postpones epoll wakeups (`TCP_DEFER_ACCEPT`) until the client transmits the initial payload
    /// (HTTP request / TLS ClientHello), eliminating wake-up thrashing on empty TCP handshakes.
    pub defer_accept_secs: Option<u32>,
    /// Enables server-side Fast Open cookie validation (`TCP_FASTOPEN`) so returning clients
    /// can send initial data in the SYN packet, cutting 1 RTT of downstream connection latency.
    pub fastopen_backlog: Option<u32>,
    /// Polls NIC ring buffers directly in kernel space (`SO_BUSY_POLL`) for up to N microseconds,
    /// bypassing thread sleep/wake epoll context switches on latency-critical tiers.
    pub busy_poll_us: Option<u32>,
    /// Affines listener socket queues to specific CPU cores (`SO_INCOMING_CPU`) to steer
    /// packet processing directly to the worker thread's local CPU, maximizing L1/L2 cache locality.
    pub incoming_cpu: bool,
    /// Caps unsent bytes in the socket write queue (`TCP_NOTSENT_LOWAT`) on accepted connections
    /// to combat bufferbloat and keep multiplexed protocols (HTTP/2, gRPC) responsive.
    pub notsent_lowat: Option<u32>,
    /// Explicit deadline for unacknowledged transmitted data (`TCP_USER_TIMEOUT`, RFC 5482)
    /// on accepted connections, tearing down silent/blackhole client connections fast.
    pub user_timeout: Option<Duration>,
    /// Enables non-local IP binding (`IP_FREEBIND`), allowing listeners to bind
    /// to floating VIPs and Anycast addresses prior to interface assignment during HA failovers.
    pub freebind: bool,
    /// Instructs kernel NAPI to suppress IRQs and prioritize polling (`SO_PREFER_BUSY_POLL`
    /// + `SO_BUSY_POLL_BUDGET`, Linux >= 5.11) on accepted sockets.
    pub prefer_busy_poll: bool,
    /// Microsecond minimum retransmission timeout floor (`TCP_RTO_MIN_US`, Linux >= 6.9) on accepted sockets.
    pub rto_min_us: Option<u32>,
    /// Caps delayed ACK timer in microseconds (`TCP_DELACK_MAX_US`, Linux >= 6.9) down to 2ms on accepted sockets.
    pub delack_max_us: Option<u32>,
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
            reuseport: cfg!(unix),
            concurrency_shards: 1,
            quickack: false,
            defer_accept_secs: None,
            fastopen_backlog: None,
            busy_poll_us: None,
            incoming_cpu: false,
            notsent_lowat: None,
            user_timeout: None,
            freebind: false,
            prefer_busy_poll: false,
            rto_min_us: None,
            delack_max_us: None,
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

    /// Sets whether to enable `SO_REUSEPORT`.
    pub fn with_reuseport(mut self, reuseport: bool) -> Self {
        self.reuseport = reuseport;
        self
    }

    /// Sets the number of listener shards bound via `SO_REUSEPORT`.
    pub fn with_concurrency_shards(mut self, shards: usize) -> Self {
        self.concurrency_shards = shards.max(1);
        self
    }

    /// Sets whether to enable `TCP_QUICKACK` (Linux).
    pub fn with_quickack(mut self, quickack: bool) -> Self {
        self.quickack = quickack;
        self
    }

    /// Sets the `TCP_DEFER_ACCEPT` seconds timeout (Linux).
    pub fn with_defer_accept(mut self, secs: Option<u32>) -> Self {
        self.defer_accept_secs = secs;
        self
    }

    /// Sets server-side `TCP_FASTOPEN` listen backlog depth (Linux).
    pub fn with_fastopen_backlog(mut self, backlog: Option<u32>) -> Self {
        self.fastopen_backlog = backlog;
        self
    }

    /// Sets `SO_BUSY_POLL` duration in microseconds on accepted sockets (Linux).
    pub fn with_busy_poll(mut self, busy_poll_us: Option<u32>) -> Self {
        self.busy_poll_us = busy_poll_us;
        self
    }

    /// Sets whether to enable `SO_INCOMING_CPU` affinity on listener shards (Linux).
    pub fn with_incoming_cpu(mut self, enabled: bool) -> Self {
        self.incoming_cpu = enabled;
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

    /// Sets `TCP_NOTSENT_LOWAT` bytes threshold on accepted sockets (Linux).
    pub fn with_notsent_lowat(mut self, lowat: Option<u32>) -> Self {
        self.notsent_lowat = lowat;
        self
    }

    /// Sets `TCP_USER_TIMEOUT` on accepted sockets (Linux).
    pub fn with_user_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.user_timeout = timeout;
        self
    }

    /// Sets whether to enable `IP_FREEBIND` on the listening socket (Linux).
    pub fn with_freebind(mut self, freebind: bool) -> Self {
        self.freebind = freebind;
        self
    }

    /// Sets whether to enable `SO_PREFER_BUSY_POLL` (Linux).
    pub fn with_prefer_busy_poll(mut self, enabled: bool) -> Self {
        self.prefer_busy_poll = enabled;
        self
    }

    /// Sets `TCP_RTO_MIN_US` minimum RTO in microseconds on accepted sockets (Linux).
    pub fn with_rto_min_us(mut self, us: Option<u32>) -> Self {
        self.rto_min_us = us;
        self
    }

    /// Sets `TCP_DELACK_MAX_US` maximum delayed ACK timer in microseconds on accepted sockets (Linux).
    pub fn with_delack_max_us(mut self, us: Option<u32>) -> Self {
        self.delack_max_us = us;
        self
    }

    /// Calculates sensible `TCP_NOTSENT_LOWAT` threshold in bytes for a given memory tier.
    #[inline]
    pub const fn notsent_lowat_for_mem_tier(tier: velda_core::MemoryTier) -> u32 {
        match tier {
            velda_core::MemoryTier::Constrained => 16 * 1024,
            velda_core::MemoryTier::Small => 16 * 1024,
            velda_core::MemoryTier::Medium => 32 * 1024,
            velda_core::MemoryTier::Large => 64 * 1024,
            velda_core::MemoryTier::XLarge => 64 * 1024,
            velda_core::MemoryTier::TwoXLarge => 128 * 1024,
            velda_core::MemoryTier::Ultra => 128 * 1024,
        }
    }

    /// Creates a TCP listener configuration sized appropriately for the host's [`velda_core::MemoryTier`].
    pub fn for_tier(tier: velda_core::MemoryTier) -> Self {
        const KB: usize = 1024;
        const MB: usize = 1024 * KB;
        let mut cfg = Self::default();
        match tier {
            velda_core::MemoryTier::Constrained => {
                cfg.backlog = 512;
                cfg.recv_buffer_size = None;
                cfg.send_buffer_size = None;
                cfg.copy_buffer_size = 8 * KB;
            }
            velda_core::MemoryTier::Small => {
                cfg.backlog = 1024;
                cfg.recv_buffer_size = Some(128 * KB);
                cfg.send_buffer_size = Some(128 * KB);
                cfg.copy_buffer_size = 16 * KB;
            }
            velda_core::MemoryTier::Medium => {
                cfg.backlog = 2048;
                cfg.recv_buffer_size = Some(256 * KB);
                cfg.send_buffer_size = Some(256 * KB);
                cfg.copy_buffer_size = 16 * KB;
            }
            velda_core::MemoryTier::Large => {
                cfg.backlog = 4096;
                cfg.recv_buffer_size = Some(512 * KB);
                cfg.send_buffer_size = Some(512 * KB);
                cfg.copy_buffer_size = 32 * KB;
            }
            velda_core::MemoryTier::XLarge => {
                cfg.backlog = 8192;
                cfg.recv_buffer_size = Some(MB);
                cfg.send_buffer_size = Some(MB);
                cfg.copy_buffer_size = 32 * KB;
            }
            velda_core::MemoryTier::TwoXLarge => {
                cfg.backlog = 16384;
                cfg.recv_buffer_size = Some(2 * MB);
                cfg.send_buffer_size = Some(2 * MB);
                cfg.copy_buffer_size = 64 * KB;
            }
            velda_core::MemoryTier::Ultra => {
                cfg.backlog = 32768;
                cfg.recv_buffer_size = Some(4 * MB);
                cfg.send_buffer_size = Some(4 * MB);
                cfg.copy_buffer_size = 64 * KB;
            }
        }
        cfg
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

    /// Returns the recommended listener shard count for a given [`velda_core::CpuTier`].
    pub fn concurrency_shards_for_cpu_tier(tier: velda_core::CpuTier) -> usize {
        match tier {
            velda_core::CpuTier::Constrained => 1,
            velda_core::CpuTier::Small => 2,
            velda_core::CpuTier::Medium => 4,
            velda_core::CpuTier::Large => 8,
            velda_core::CpuTier::XLarge => 16,
            velda_core::CpuTier::TwoXLarge => 32,
            velda_core::CpuTier::Ultra => 64,
        }
    }

    /// Creates a TCP listener configuration combining CPU-driven and Memory-driven tiers.
    pub fn for_tiers(cpu: velda_core::CpuTier, mem: velda_core::MemoryTier) -> Self {
        let mut cfg = Self::for_tier(mem);
        cfg.copy_buffer_size = Self::copy_buffer_size_for_cpu_tier(cpu);
        cfg.concurrency_shards = Self::concurrency_shards_for_cpu_tier(cpu);
        cfg
    }

    /// Pre-compiles TCP listener acceleration settings from probed hardware topology.
    pub fn for_topology(topo: &velda_core::HardwareTopology) -> Self {
        let mut cfg = Self::for_tiers(topo.cpu_tier(), topo.memory_tier());
        if topo.kernel.supports_tcp_fastopen_server() {
            cfg.fastopen_backlog = Some((cfg.backlog / 4).max(256));
        }
        if topo.kernel.supports_busy_poll()
            && matches!(
                topo.cpu_tier(),
                velda_core::CpuTier::Large
                    | velda_core::CpuTier::XLarge
                    | velda_core::CpuTier::TwoXLarge
                    | velda_core::CpuTier::Ultra
            )
        {
            cfg.busy_poll_us = Some(50);
        }
        let ladder = topo.acceleration_ladder();
        if ladder.core_steering >= velda_core::CoreSteeringTier::IncomingCpu {
            cfg.incoming_cpu = true;
        }
        if ladder.multiplex_pacing >= velda_core::MultiplexPacingTier::NotsentLowat {
            cfg.notsent_lowat = Some(Self::notsent_lowat_for_mem_tier(topo.memory_tier()));
        }
        if ladder.dead_peer_teardown >= velda_core::DeadPeerTeardownTier::UserTimeout {
            cfg.user_timeout = Some(Duration::from_secs(30));
        }
        if ladder.dead_peer_teardown >= velda_core::DeadPeerTeardownTier::MicrosecondPaced {
            cfg.rto_min_us = Some(5_000);
            cfg.delack_max_us = Some(2_000);
        }
        if ladder.busy_poll >= velda_core::BusyPollTier::PreferBusyPoll
            && matches!(
                topo.cpu_tier(),
                velda_core::CpuTier::Large
                    | velda_core::CpuTier::XLarge
                    | velda_core::CpuTier::TwoXLarge
                    | velda_core::CpuTier::Ultra
            )
        {
            cfg.prefer_busy_poll = true;
        }
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
    fn test_tcp_config_tiers() {
        let cfg =
            TcpListenerConfig::for_tiers(velda_core::CpuTier::Ultra, velda_core::MemoryTier::Small);
        assert_eq!(cfg.copy_buffer_size, 64 * 1024); // from Ultra CPU
        assert_eq!(cfg.backlog, 1024); // from Small Memory
        assert_eq!(cfg.recv_buffer_size, Some(128 * 1024)); // from Small Memory
        assert_eq!(cfg.concurrency_shards, 64); // from Ultra CPU
        assert_eq!(cfg.reuseport, cfg!(unix));
    }

    #[test]
    fn test_concurrency_shards_for_cpu_tier() {
        assert_eq!(
            TcpListenerConfig::concurrency_shards_for_cpu_tier(velda_core::CpuTier::Constrained),
            1
        );
        assert_eq!(
            TcpListenerConfig::concurrency_shards_for_cpu_tier(velda_core::CpuTier::Small),
            2
        );
        assert_eq!(
            TcpListenerConfig::concurrency_shards_for_cpu_tier(velda_core::CpuTier::Medium),
            4
        );
        assert_eq!(
            TcpListenerConfig::concurrency_shards_for_cpu_tier(velda_core::CpuTier::Large),
            8
        );
        assert_eq!(
            TcpListenerConfig::concurrency_shards_for_cpu_tier(velda_core::CpuTier::XLarge),
            16
        );
        assert_eq!(
            TcpListenerConfig::concurrency_shards_for_cpu_tier(velda_core::CpuTier::TwoXLarge),
            32
        );
        assert_eq!(
            TcpListenerConfig::concurrency_shards_for_cpu_tier(velda_core::CpuTier::Ultra),
            64
        );
    }

    #[test]
    fn test_incoming_cpu_builder() {
        let cfg = TcpListenerConfig::new().with_incoming_cpu(true);
        assert!(cfg.incoming_cpu);
    }

    #[test]
    fn test_acceleration_options_builder() {
        let cfg = TcpListenerConfig::new()
            .with_notsent_lowat(Some(32768))
            .with_user_timeout(Some(Duration::from_secs(45)))
            .with_freebind(true);

        assert_eq!(cfg.notsent_lowat, Some(32768));
        assert_eq!(cfg.user_timeout, Some(Duration::from_secs(45)));
        assert!(cfg.freebind);

        assert_eq!(
            TcpListenerConfig::notsent_lowat_for_mem_tier(velda_core::MemoryTier::Constrained),
            16 * 1024
        );
        assert_eq!(
            TcpListenerConfig::notsent_lowat_for_mem_tier(velda_core::MemoryTier::Medium),
            32 * 1024
        );
        assert_eq!(
            TcpListenerConfig::notsent_lowat_for_mem_tier(velda_core::MemoryTier::Ultra),
            128 * 1024
        );
    }

    #[test]
    fn test_tcp_evolutionary_ladder_for_topology() {
        use velda_core::hardware::{
            AccelerationTier, HardwareTopology, KernelProfile, KernelVersion,
        };

        let kernel_6_9 = KernelProfile::new(
            KernelVersion::new(6, 9, 0),
            AccelerationTier::IoUringFastPath,
            "test_6_9",
        );
        let topo = HardwareTopology::with_workers_and_memory(16, 16 * 1024 * 1024 * 1024)
            .with_kernel(kernel_6_9);

        let cfg = TcpListenerConfig::for_topology(&topo);
        assert!(cfg.prefer_busy_poll);
        assert_eq!(cfg.rto_min_us, Some(5_000));
        assert_eq!(cfg.delack_max_us, Some(2_000));

        let builder_cfg = TcpListenerConfig::new()
            .with_prefer_busy_poll(true)
            .with_rto_min_us(Some(3_000))
            .with_delack_max_us(Some(1_500));
        assert!(builder_cfg.prefer_busy_poll);
        assert_eq!(builder_cfg.rto_min_us, Some(3_000));
        assert_eq!(builder_cfg.delack_max_us, Some(1_500));
    }
}
