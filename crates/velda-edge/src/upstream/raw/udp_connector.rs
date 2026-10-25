//! Layer 4 (L4) Raw UDP Connector & Linux Socket Acceleration.
//!
//! Provides ephemeral UDP socket mechanics, binding, and Linux kernel socket options for L4 UDP sessions.
//! Upstream logic purely leases or dispatches to these mechanics, enforcing Rule 2.8.

use std::net::SocketAddr;

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
    /// + `SO_BUSY_POLL_BUDGET`, Linux >= 5.11) in high-core latency-critical environments.
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
            && topo.available_cores >= 8;

        let busy_poll_us = if topo.kernel.supports_busy_poll() && topo.available_cores >= 8 {
            Some(50)
        } else {
            None
        };

        let (recv_buffer_size, send_buffer_size) = (
            topo.sizing.udp_recv_buffer_size,
            topo.sizing.udp_send_buffer_size,
        );

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
                            errno = std::io::Error::last_os_error().raw_os_error(),
                            "IP_BIND_ADDRESS_NO_PORT not supported or denied in container; skipping"
                        );
                    }
                }
            }

            if let Some(rcvbuf) = self.recv_buffer_size {
                let val = rcvbuf as libc::c_int;
                unsafe {
                    let ret = libc::setsockopt(
                        fd,
                        libc::SOL_SOCKET,
                        libc::SO_RCVBUF,
                        &val as *const _ as *const libc::c_void,
                        std::mem::size_of_val(&val) as libc::socklen_t,
                    );
                    if ret != 0 {
                        tracing::trace!(error = %std::io::Error::last_os_error(), "SO_RCVBUF skipped");
                    }
                }
            }

            if let Some(sndbuf) = self.send_buffer_size {
                let val = sndbuf as libc::c_int;
                unsafe {
                    let ret = libc::setsockopt(
                        fd,
                        libc::SOL_SOCKET,
                        libc::SO_SNDBUF,
                        &val as *const _ as *const libc::c_void,
                        std::mem::size_of_val(&val) as libc::socklen_t,
                    );
                    if ret != 0 {
                        tracing::trace!(error = %std::io::Error::last_os_error(), "SO_SNDBUF skipped");
                    }
                }
            }
        }
        #[cfg(not(target_os = "linux"))]
        let _ = (fd, is_ipv4);
    }

    /// Applies post-bind socket acceleration options.
    pub fn apply_post_bind(&self, fd: std::os::unix::io::RawFd) {
        #[cfg(target_os = "linux")]
        unsafe {
            const UDP_GRO: libc::c_int = 104;
            if self.gro {
                let val: libc::c_int = 1;
                let ret = libc::setsockopt(
                    fd,
                    libc::IPPROTO_UDP,
                    UDP_GRO,
                    &val as *const _ as *const libc::c_void,
                    std::mem::size_of_val(&val) as libc::socklen_t,
                );
                if ret != 0 {
                    tracing::trace!(error = %std::io::Error::last_os_error(), "UDP_GRO skipped");
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
                    tracing::trace!(error = %std::io::Error::last_os_error(), "SO_RXQ_OVFL skipped");
                }
            }

            // Note on UDP_SEGMENT (GSO): We deliberately do NOT set UDP_SEGMENT globally on the socket
            // via setsockopt here. Doing so would force the kernel to chop every outgoing datagram
            // to a fixed segment length, corrupting datagram boundaries for variable-length packets.
            // Instead, GSO segmentation is applied dynamically per-datagram via `cmsg` (IPPROTO_UDP, UDP_SEGMENT)
            // in `UdpSocket::try_send_gso_aware` when a coalesced GRO super-packet is forwarded.

            if let Some(busy_poll_us) = self.busy_poll_us {
                let val = busy_poll_us as libc::c_int;
                let ret = libc::setsockopt(
                    fd,
                    libc::SOL_SOCKET,
                    libc::SO_BUSY_POLL,
                    &val as *const _ as *const libc::c_void,
                    std::mem::size_of_val(&val) as libc::socklen_t,
                );
                if ret != 0 {
                    tracing::trace!(error = %std::io::Error::last_os_error(), "SO_BUSY_POLL skipped");
                }
            }

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
                    tracing::trace!(error = %std::io::Error::last_os_error(), "SO_PREFER_BUSY_POLL skipped");
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

    let bind_ip: std::net::IpAddr = if endpoint.is_ipv4() {
        std::net::Ipv4Addr::UNSPECIFIED.into()
    } else {
        std::net::Ipv6Addr::UNSPECIFIED.into()
    };
    socket.bind(&SocketAddr::new(bind_ip, 0).into())?;

    #[cfg(target_os = "linux")]
    {
        use std::os::unix::io::AsRawFd;
        acceleration.apply_post_bind(socket.as_raw_fd());
    }

    socket.connect(&endpoint.into())?;

    let std_sock: std::net::UdpSocket = socket.into();
    tokio::net::UdpSocket::from_std(std_sock)
}
