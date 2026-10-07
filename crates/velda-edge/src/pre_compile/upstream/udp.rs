//! Layer 4 UDP Upstream managing physical backend endpoint targets and socket acceleration.

use std::net::SocketAddr;

use super::lb::EdgeUpstream;

/// Pre-compiled Linux socket acceleration path for L4 UDP backend connections.
///
/// Pre-computed at bootstrap from host hardware topology and kernel profile to keep
/// UDP datagram forwarding and ephemeral session binding on a branchless hot path
/// without repeated runtime capability probing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct UdpAccelerationPath {
    /// Delays ephemeral source port selection until `connect(2)` (`IP_BIND_ADDRESS_NO_PORT`),
    /// eliminating the 64,000 ephemeral outbound port exhaustion ceiling on high-concurrency UDP gateways.
    pub bind_address_no_port: bool,
    /// Generic Receive Offload (`UDP_GRO`) combining consecutive backend datagrams into
    /// single contiguous buffers, cutting per-datagram syscall overhead on high-throughput flows.
    pub gro: bool,
    /// Monitors kernel UDP receive queue drops/overflows (`SO_RXQ_OVFL`) to trace dropped datagrams.
    pub rxq_ovfl: bool,
    /// Low-latency socket polling in kernel space (`SO_BUSY_POLL`) to bypass epoll context switches.
    pub busy_poll_us: Option<u32>,
    /// Socket receive buffer size hint (`SO_RCVBUF`).
    pub recv_buffer_size: Option<usize>,
    /// Socket send buffer size hint (`SO_SNDBUF`).
    pub send_buffer_size: Option<usize>,
    /// Generic Segmentation Offload segment size (`UDP_SEGMENT`, Linux >= 4.18),
    /// offloading datagram fragmentation of up to 64KB buffers to kernel or NIC.
    pub gso_segment: Option<u16>,
    /// Instructs kernel NAPI to suppress IRQs and prioritize polling (`SO_PREFER_BUSY_POLL`
    /// + `SO_BUSY_POLL_BUDGET`, Linux >= 5.11) on high-core latency-critical tiers.
    pub prefer_busy_poll: bool,
}

impl UdpAccelerationPath {
    /// Pre-compiles UDP acceleration path based on host hardware/kernel topology.
    pub fn for_topology(topo: &velda_core::HardwareTopology) -> Self {
        let ladder = topo.acceleration_ladder();

        let bind_address_no_port =
            ladder.outbound_port_scaling >= velda_core::OutboundPortScalingTier::BindAddressNoPort;

        let gro = ladder.udp_offload >= velda_core::UdpOffloadTier::GenericReceiveOffload;
        let rxq_ovfl = ladder.udp_offload >= velda_core::UdpOffloadTier::QueueMonitored;

        let gso_segment =
            if ladder.udp_egress >= velda_core::UdpEgressTier::GenericSegmentationOffload {
                Some(1472)
            } else {
                None
            };

        let prefer_busy_poll = ladder.busy_poll >= velda_core::BusyPollTier::PreferBusyPoll
            && matches!(
                topo.cpu_tier(),
                velda_core::CpuTier::Large
                    | velda_core::CpuTier::XLarge
                    | velda_core::CpuTier::TwoXLarge
                    | velda_core::CpuTier::Ultra
            );

        let busy_poll_us = if topo.kernel.supports_busy_poll()
            && matches!(
                topo.cpu_tier(),
                velda_core::CpuTier::Large
                    | velda_core::CpuTier::XLarge
                    | velda_core::CpuTier::TwoXLarge
                    | velda_core::CpuTier::Ultra
            ) {
            Some(50)
        } else {
            None
        };

        let (recv_buffer_size, send_buffer_size) = buffer_sizes_for_mem_tier(topo.memory_tier());

        Self {
            bind_address_no_port,
            gro,
            rxq_ovfl,
            busy_poll_us,
            recv_buffer_size: Some(recv_buffer_size),
            send_buffer_size: Some(send_buffer_size),
            gso_segment,
            prefer_busy_poll,
        }
    }

    /// Applies pre-bind socket acceleration options.
    pub fn apply_pre_bind(&self, fd: std::os::unix::io::RawFd, is_ipv4: bool) {
        #[cfg(target_os = "linux")]
        {
            if self.bind_address_no_port && is_ipv4 {
                let val: libc::c_int = 1;
                unsafe {
                    let ret = libc::setsockopt(
                        fd,
                        libc::IPPROTO_IP,
                        libc::IP_BIND_ADDRESS_NO_PORT,
                        &val as *const _ as *const libc::c_void,
                        std::mem::size_of_val(&val) as libc::socklen_t,
                    );
                    if ret != 0 {
                        tracing::trace!(
                            error = %std::io::Error::last_os_error(),
                            "IP_BIND_ADDRESS_NO_PORT skipped on UDP upstream"
                        );
                    }
                }
            }

            if let Some(rcvbuf) = self.recv_buffer_size {
                let val = rcvbuf as libc::c_int;
                unsafe {
                    let _ = libc::setsockopt(
                        fd,
                        libc::SOL_SOCKET,
                        libc::SO_RCVBUF,
                        &val as *const _ as *const libc::c_void,
                        std::mem::size_of_val(&val) as libc::socklen_t,
                    );
                }
            }

            if let Some(sndbuf) = self.send_buffer_size {
                let val = sndbuf as libc::c_int;
                unsafe {
                    let _ = libc::setsockopt(
                        fd,
                        libc::SOL_SOCKET,
                        libc::SO_SNDBUF,
                        &val as *const _ as *const libc::c_void,
                        std::mem::size_of_val(&val) as libc::socklen_t,
                    );
                }
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = (fd, is_ipv4);
        }
    }

    /// Applies post-bind socket acceleration options.
    pub fn apply_post_bind(&self, fd: std::os::unix::io::RawFd) {
        #[cfg(target_os = "linux")]
        unsafe {
            if self.gro {
                let val: libc::c_int = 1;
                let ret = libc::setsockopt(
                    fd,
                    libc::IPPROTO_UDP,
                    libc::UDP_GRO,
                    &val as *const _ as *const libc::c_void,
                    std::mem::size_of_val(&val) as libc::socklen_t,
                );
                if ret != 0 {
                    tracing::trace!(
                        error = %std::io::Error::last_os_error(),
                        "UDP_GRO skipped on UDP upstream socket"
                    );
                }
            }

            if self.rxq_ovfl {
                let val: libc::c_int = 1;
                let ret = libc::setsockopt(
                    fd,
                    libc::SOL_SOCKET,
                    libc::SO_RXQ_OVFL,
                    &val as *const _ as *const libc::c_void,
                    std::mem::size_of_val(&val) as libc::socklen_t,
                );
                if ret != 0 {
                    tracing::trace!(
                        error = %std::io::Error::last_os_error(),
                        "SO_RXQ_OVFL skipped on UDP upstream socket"
                    );
                }
            }

            if let Some(busy_poll) = self.busy_poll_us {
                let val = busy_poll as libc::c_int;
                let ret = libc::setsockopt(
                    fd,
                    libc::SOL_SOCKET,
                    libc::SO_BUSY_POLL,
                    &val as *const _ as *const libc::c_void,
                    std::mem::size_of_val(&val) as libc::socklen_t,
                );
                if ret != 0 {
                    tracing::trace!(
                        error = %std::io::Error::last_os_error(),
                        "SO_BUSY_POLL skipped on UDP upstream socket"
                    );
                }
            }

            // Linux 4.18+ UDP Generic Segmentation Offload (GSO)
            const UDP_SEGMENT: libc::c_int = 103;
            if let Some(segment) = self.gso_segment {
                let val = segment as libc::c_int;
                let ret = libc::setsockopt(
                    fd,
                    libc::IPPROTO_UDP,
                    UDP_SEGMENT,
                    &val as *const _ as *const libc::c_void,
                    std::mem::size_of_val(&val) as libc::socklen_t,
                );
                if ret != 0 {
                    tracing::trace!(
                        error = %std::io::Error::last_os_error(),
                        "UDP_SEGMENT (GSO) skipped on UDP upstream socket"
                    );
                }
            }

            // Linux 5.11+ SO_PREFER_BUSY_POLL & SO_BUSY_POLL_BUDGET
            const SO_PREFER_BUSY_POLL: libc::c_int = 69;
            const SO_BUSY_POLL_BUDGET: libc::c_int = 70;
            if self.prefer_busy_poll {
                let val: libc::c_int = 1;
                let ret = libc::setsockopt(
                    fd,
                    libc::SOL_SOCKET,
                    SO_PREFER_BUSY_POLL,
                    &val as *const _ as *const libc::c_void,
                    std::mem::size_of_val(&val) as libc::socklen_t,
                );
                if ret != 0 {
                    tracing::trace!(
                        error = %std::io::Error::last_os_error(),
                        "SO_PREFER_BUSY_POLL skipped on UDP upstream socket"
                    );
                }
                let budget: libc::c_int = 8;
                let _ = libc::setsockopt(
                    fd,
                    libc::SOL_SOCKET,
                    SO_BUSY_POLL_BUDGET,
                    &budget as *const _ as *const libc::c_void,
                    std::mem::size_of_val(&budget) as libc::socklen_t,
                );
            }
        }
        #[cfg(not(target_os = "linux"))]
        let _ = fd;
    }
}

/// Returns recommended socket buffer sizes (recv, send) for a given [`velda_core::MemoryTier`].
pub const fn buffer_sizes_for_mem_tier(tier: velda_core::MemoryTier) -> (usize, usize) {
    use velda_core::MemoryTier;
    const KB: usize = 1024;
    const MB: usize = 1024 * 1024;
    match tier {
        MemoryTier::Constrained => (64 * KB, 64 * KB),
        MemoryTier::Small => (128 * KB, 128 * KB),
        MemoryTier::Medium => (256 * KB, 256 * KB),
        MemoryTier::Large => (512 * KB, 512 * KB),
        MemoryTier::XLarge => (MB, MB),
        MemoryTier::TwoXLarge => (2 * MB, 2 * MB),
        MemoryTier::Ultra => (4 * MB, 4 * MB),
    }
}

/// Connects an ephemeral UDP socket to the target physical backend endpoint,
/// applying pre-bind socket acceleration, binding to an ephemeral port,
/// post-bind tuning, and connecting to the endpoint.
pub fn connect_udp_socket(
    endpoint: SocketAddr,
    acceleration: &UdpAccelerationPath,
) -> Result<tokio::net::UdpSocket, std::io::Error> {
    let domain = if endpoint.is_ipv4() {
        socket2::Domain::IPV4
    } else {
        socket2::Domain::IPV6
    };
    let socket = socket2::Socket::new(domain, socket2::Type::DGRAM, Some(socket2::Protocol::UDP))?;
    socket.set_nonblocking(true)?;

    #[cfg(target_os = "linux")]
    {
        use std::os::unix::io::AsRawFd;
        acceleration.apply_pre_bind(socket.as_raw_fd(), endpoint.is_ipv4());
    }

    let bind_addr: SocketAddr = if endpoint.is_ipv4() {
        "0.0.0.0:0".parse().unwrap()
    } else {
        "[::]:0".parse().unwrap()
    };
    socket.bind(&bind_addr.into())?;

    #[cfg(target_os = "linux")]
    {
        use std::os::unix::io::AsRawFd;
        acceleration.apply_post_bind(socket.as_raw_fd());
    }

    socket.connect(&endpoint.into())?;

    let std_sock: std::net::UdpSocket = socket.into();
    tokio::net::UdpSocket::from_std(std_sock)
}

/// Layer 4 UDP Upstream managing physical backend endpoint targets.
///
/// Pre-compiled with static load balancer, physical discovery endpoints, and socket acceleration path.
pub struct UdpUpstream {
    /// [PRE-COMPILED]: Pre-assembled upstream core holding discovery, health tracker,
    /// timeouts, and the selected `LbAlgorithm` enum variant.
    inner: EdgeUpstream,
    acceleration: UdpAccelerationPath,
}

impl std::fmt::Debug for UdpUpstream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UdpUpstream")
            .field("id", &self.inner.id())
            .field("acceleration", &self.acceleration)
            .finish()
    }
}

impl UdpUpstream {
    /// Creates a new [`UdpUpstream`] instance.
    pub fn new(inner: EdgeUpstream, acceleration: UdpAccelerationPath) -> Self {
        Self {
            inner,
            acceleration,
        }
    }

    /// Returns the upstream unique identifier.
    #[inline]
    pub fn id(&self) -> &str {
        self.inner.id()
    }

    /// Returns the pre-compiled socket acceleration path.
    #[inline]
    pub fn acceleration(&self) -> &UdpAccelerationPath {
        &self.acceleration
    }

    /// Selects an eligible backend endpoint via configured load balancing.
    #[inline]
    pub fn select_target(&self) -> Option<SocketAddr> {
        self.inner.select_endpoint().ok()
    }

    /// Connects an ephemeral upstream UDP socket to the given target endpoint,
    /// applying pre-compiled socket acceleration parameters.
    pub fn connect_socket(
        &self,
        endpoint: SocketAddr,
    ) -> Result<tokio::net::UdpSocket, std::io::Error> {
        connect_udp_socket(endpoint, &self.acceleration)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use velda_core::HardwareTopology;
    use velda_core::hardware::{AccelerationTier, KernelProfile, KernelVersion};

    #[test]
    fn test_udp_acceleration_path_for_topology() {
        let topo = HardwareTopology::with_workers_and_memory(8, 64 * 1024 * 1024 * 1024);
        let ale = UdpAccelerationPath::for_topology(&topo);
        assert!(ale.recv_buffer_size.is_some());
        assert!(ale.send_buffer_size.is_some());
    }

    #[test]
    fn test_udp_acceleration_ladder_linux_3_10() {
        let kernel = KernelProfile::new(
            KernelVersion::new(3, 10, 0),
            AccelerationTier::Standard,
            "test_legacy",
        );
        let topo = HardwareTopology::with_workers_and_memory(8, 8 * 1024 * 1024 * 1024)
            .with_kernel(kernel);
        let ale = UdpAccelerationPath::for_topology(&topo);
        assert!(ale.gso_segment.is_none());
        assert!(!ale.prefer_busy_poll);
    }

    #[test]
    fn test_udp_acceleration_ladder_linux_4_18() {
        let kernel = KernelProfile::new(
            KernelVersion::new(4, 18, 0),
            AccelerationTier::Standard,
            "test_gso",
        );
        let topo = HardwareTopology::with_workers_and_memory(8, 8 * 1024 * 1024 * 1024)
            .with_kernel(kernel);
        let ale = UdpAccelerationPath::for_topology(&topo);
        assert_eq!(ale.gso_segment, Some(1472));
        assert!(!ale.prefer_busy_poll);
    }

    #[test]
    fn test_udp_acceleration_ladder_linux_5_11() {
        let kernel = KernelProfile::new(
            KernelVersion::new(5, 11, 0),
            AccelerationTier::Standard,
            "test_prefer_busy_poll",
        );
        let topo = HardwareTopology::with_workers_and_memory(16, 16 * 1024 * 1024 * 1024)
            .with_kernel(kernel);
        let ale = UdpAccelerationPath::for_topology(&topo);
        assert_eq!(ale.gso_segment, Some(1472));
        assert!(ale.prefer_busy_poll);
    }
}
