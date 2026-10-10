//! Layer 4 (L4) Raw TCP Connector & Linux Socket Acceleration.
//!
//! Provides protocol connection mechanics and Linux kernel socket options for L4 TCP streams.
//! Upstream logic purely leases or dispatches to these mechanics, enforcing Rule 2.8.

use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::TcpStream;

/// Pre-compiled Linux socket acceleration path for L4 TCP backend connections.
///
/// Pre-computed at bootstrap to keep connection establishment on a branchless hot path
/// without repeated hardware or kernel capability checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcpAccelerationPath {
    /// Disables Nagle's algorithm (`TCP_NODELAY`) to eliminate 40ms delayed-ACK buffering.
    pub nodelay: bool,
    /// Sends ClientHello directly in SYN packet (`TCP_FASTOPEN_CONNECT`), shaving 1 full RTT
    /// off TLS handshakes. Only enabled for TLS where replay protection is cryptographic.
    pub fastopen: bool,
    /// Caps unsent bytes in the socket write queue (`TCP_NOTSENT_LOWAT`) to prevent bufferbloat
    /// and keep epoll notification responsive to user-space backpressure.
    pub notsent_lowat: Option<u32>,
    /// Aborts stuck TCP streams (`TCP_USER_TIMEOUT`) when cloud NAT gateways or firewalls silently
    /// drop packets, replacing the default 15-minute OS retransmit timeout with upstream deadline.
    pub user_timeout: Option<Duration>,
    /// Periodically probes idle connections (`SO_KEEPALIVE` + `TCP_KEEPIDLE`) to keep stateful
    /// middlebox / NAT table entries warm and detect silent peer reboots.
    pub keepalive: Option<Duration>,
    /// Low-latency socket polling in kernel space (`SO_BUSY_POLL`) to bypass epoll sleep/wake
    /// context-switch latency spikes on high-core server tiers.
    pub busy_poll_us: Option<u32>,
    /// Bounds SYN retransmission attempts (`TCP_SYNCNT`) to fail fast and trigger upstream
    /// failover within 3-7s rather than stalling worker tasks for 60-127s on partitioned backends.
    pub syncnt: Option<u32>,
    /// Immediately acknowledges incoming upstream response packets (`TCP_QUICKACK`) to prevent
    /// 40ms delayed-ACK stalls on backend responses.
    pub quickack: bool,
    /// Attempts BBR congestion control (`TCP_CONGESTION`) for high-throughput, low-bufferbloat egress.
    pub bbr: bool,
    /// Delays ephemeral source port selection until `connect(2)` (`IP_BIND_ADDRESS_NO_PORT`),
    /// eliminating the 64,000 ephemeral outbound port exhaustion ceiling on high-concurrency gateways.
    pub bind_address_no_port: bool,
    /// Commands the kernel to rehash the IPv6 flow label / 4-tuple entropy hash on packet loss/RTO
    /// (`SO_TXREHASH`, Linux >= 5.18), automatically steering flows away from degraded ECMP paths.
    pub tx_rehash: bool,
    /// Instructs kernel NAPI to suppress IRQ interrupts in favor of busy-polling (`SO_PREFER_BUSY_POLL`
    /// + `SO_BUSY_POLL_BUDGET`, Linux >= 5.11), eliminating context-switch jitter.
    pub prefer_busy_poll: bool,
    /// Microsecond minimum retransmission timeout floor (`TCP_RTO_MIN_US`, Linux >= 6.9).
    /// Replaces the default 200ms RTO floor with 5ms for sub-millisecond loss recovery on intra-VPC backends.
    pub rto_min_us: Option<u32>,
    /// Caps delayed ACK timer in microseconds (`TCP_DELACK_MAX_US`, Linux >= 6.9) down to 2ms,
    /// eliminating 40ms delayed-ACK stalls without user-space `TCP_QUICKACK` polling.
    pub delack_max_us: Option<u32>,
    /// Caps maximum RTO in milliseconds (`TCP_RTO_MAX_MS`, Linux >= 6.9) down to 1000ms,
    /// stopping exponential backoff from stalling partitioned backends for 120s.
    pub rto_max_ms: Option<u32>,
}

impl Default for TcpAccelerationPath {
    fn default() -> Self {
        Self {
            nodelay: true,
            fastopen: false,
            notsent_lowat: None,
            user_timeout: None,
            keepalive: None,
            busy_poll_us: None,
            syncnt: None,
            quickack: true,
            bbr: false,
            bind_address_no_port: false,
            tx_rehash: false,
            prefer_busy_poll: false,
            rto_min_us: None,
            delack_max_us: None,
            rto_max_ms: None,
        }
    }
}

impl TcpAccelerationPath {
    /// Pre-compiles socket acceleration path based on host hardware/kernel topology,
    /// upstream timeouts, and whether TLS encryption is active.
    pub fn for_topology(
        topo: &velda_core::HardwareTopology,
        timeouts: &velda_upstream::UpstreamTimeouts,
        is_tls: bool,
    ) -> Self {
        let fastopen = is_tls && topo.kernel.supports_tcp_fastopen_connect();
        let ladder = topo.acceleration_ladder();

        let notsent_lowat =
            if ladder.multiplex_pacing >= velda_core::MultiplexPacingTier::NotsentLowat {
                Some(notsent_lowat_for_mem_tier(topo.memory_tier()))
            } else {
                None
            };

        let user_timeout = if timeouts.connect.as_millis() > 0 {
            Some(timeouts.connect)
        } else {
            None
        };

        let keepalive = if timeouts.idle.as_secs() >= 10 {
            Some(timeouts.idle / 2)
        } else {
            None
        };

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

        let syncnt = if timeouts.connect <= Duration::from_millis(1500) {
            Some(1)
        } else if timeouts.connect <= Duration::from_millis(3500) {
            Some(2)
        } else {
            Some(3)
        };

        let bind_address_no_port =
            ladder.outbound_port_scaling >= velda_core::OutboundPortScalingTier::BindAddressNoPort;
        let tx_rehash =
            ladder.multipath_resilience >= velda_core::MultipathResilienceTier::TxRehash;
        let prefer_busy_poll = ladder.busy_poll >= velda_core::BusyPollTier::PreferBusyPoll
            && matches!(
                topo.cpu_tier(),
                velda_core::CpuTier::Large
                    | velda_core::CpuTier::XLarge
                    | velda_core::CpuTier::TwoXLarge
                    | velda_core::CpuTier::Ultra
            );

        let (rto_min_us, delack_max_us, rto_max_ms) =
            if ladder.dead_peer_teardown >= velda_core::DeadPeerTeardownTier::MicrosecondPaced {
                (Some(5_000), Some(2_000), Some(1_000))
            } else {
                (None, None, None)
            };

        Self {
            nodelay: true,
            fastopen,
            notsent_lowat,
            user_timeout,
            keepalive,
            busy_poll_us,
            syncnt,
            quickack: true,
            bbr: false,
            bind_address_no_port,
            tx_rehash,
            prefer_busy_poll,
            rto_min_us,
            delack_max_us,
            rto_max_ms,
        }
    }

    /// Applies pre-connect socket acceleration options.
    pub fn apply_pre_connect(&self, fd: std::os::unix::io::RawFd) {
        #[cfg(target_os = "linux")]
        {
            if self.bind_address_no_port {
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
                            "IP_BIND_ADDRESS_NO_PORT not supported by kernel or denied in container; skipping"
                        );
                    }
                }
            }

            if self.fastopen {
                let val: libc::c_int = 1;
                unsafe {
                    let ret = libc::setsockopt(
                        fd,
                        libc::IPPROTO_TCP,
                        libc::TCP_FASTOPEN_CONNECT,
                        &val as *const _ as *const libc::c_void,
                        std::mem::size_of_val(&val) as libc::socklen_t,
                    );
                    if ret != 0 {
                        tracing::trace!(
                            errno = std::io::Error::last_os_error().raw_os_error(),
                            "TCP_FASTOPEN_CONNECT not supported by kernel or denied in container; skipping"
                        );
                    }
                }
            }

            if let Some(syncnt) = self.syncnt {
                let val = syncnt as libc::c_int;
                unsafe {
                    let ret = libc::setsockopt(
                        fd,
                        libc::IPPROTO_TCP,
                        libc::TCP_SYNCNT,
                        &val as *const _ as *const libc::c_void,
                        std::mem::size_of_val(&val) as libc::socklen_t,
                    );
                    if ret != 0 {
                        tracing::trace!(error = %std::io::Error::last_os_error(), "TCP_SYNCNT skipped");
                    }
                }
            }
        }
        #[cfg(not(target_os = "linux"))]
        let _ = fd;
    }

    /// Applies post-connect socket acceleration options.
    pub fn apply_post_connect(&self, fd: std::os::unix::io::RawFd) {
        #[cfg(target_os = "linux")]
        unsafe {
            if let Some(lowat) = self.notsent_lowat {
                let val = lowat as libc::c_uint;
                let ret = libc::setsockopt(
                    fd,
                    libc::IPPROTO_TCP,
                    libc::TCP_NOTSENT_LOWAT,
                    &val as *const _ as *const libc::c_void,
                    std::mem::size_of_val(&val) as libc::socklen_t,
                );
                if ret != 0 {
                    tracing::trace!(error = %std::io::Error::last_os_error(), "TCP_NOTSENT_LOWAT skipped");
                }
            }

            if let Some(user_timeout) = self.user_timeout {
                let val = user_timeout.as_millis() as libc::c_uint;
                let ret = libc::setsockopt(
                    fd,
                    libc::IPPROTO_TCP,
                    libc::TCP_USER_TIMEOUT,
                    &val as *const _ as *const libc::c_void,
                    std::mem::size_of_val(&val) as libc::socklen_t,
                );
                if ret != 0 {
                    tracing::trace!(error = %std::io::Error::last_os_error(), "TCP_USER_TIMEOUT skipped");
                }
            }

            if let Some(keepalive) = self.keepalive {
                let enable: libc::c_int = 1;
                let _ = libc::setsockopt(
                    fd,
                    libc::SOL_SOCKET,
                    libc::SO_KEEPALIVE,
                    &enable as *const _ as *const libc::c_void,
                    std::mem::size_of_val(&enable) as libc::socklen_t,
                );
                let idle_secs = keepalive.as_secs().max(1) as libc::c_int;
                let _ = libc::setsockopt(
                    fd,
                    libc::IPPROTO_TCP,
                    libc::TCP_KEEPIDLE,
                    &idle_secs as *const _ as *const libc::c_void,
                    std::mem::size_of_val(&idle_secs) as libc::socklen_t,
                );
                let interval_secs: libc::c_int = 5;
                let _ = libc::setsockopt(
                    fd,
                    libc::IPPROTO_TCP,
                    libc::TCP_KEEPINTVL,
                    &interval_secs as *const _ as *const libc::c_void,
                    std::mem::size_of_val(&interval_secs) as libc::socklen_t,
                );
                let count: libc::c_int = 3;
                let _ = libc::setsockopt(
                    fd,
                    libc::IPPROTO_TCP,
                    libc::TCP_KEEPCNT,
                    &count as *const _ as *const libc::c_void,
                    std::mem::size_of_val(&count) as libc::socklen_t,
                );
            }

            if self.quickack {
                let val: libc::c_int = 1;
                let _ = libc::setsockopt(
                    fd,
                    libc::IPPROTO_TCP,
                    libc::TCP_QUICKACK,
                    &val as *const _ as *const libc::c_void,
                    std::mem::size_of_val(&val) as libc::socklen_t,
                );
            }

            if self.bbr {
                let cc_name = b"bbr\0";
                let ret = libc::setsockopt(
                    fd,
                    libc::IPPROTO_TCP,
                    libc::TCP_CONGESTION,
                    cc_name.as_ptr() as *const libc::c_void,
                    cc_name.len() as libc::socklen_t,
                );
                if ret != 0 {
                    tracing::trace!("BBR congestion control skipped (not available in kernel)");
                }
            }

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
                    tracing::trace!(error = %std::io::Error::last_os_error(), "SO_BUSY_POLL skipped (unprivileged container)");
                }
            }

            // Linux 5.18+ SO_TXREHASH
            const SO_TXREHASH: libc::c_int = 74;
            if self.tx_rehash {
                let val: libc::c_int = 1;
                let ret = libc::setsockopt(
                    fd,
                    libc::SOL_SOCKET,
                    SO_TXREHASH,
                    &val as *const _ as *const libc::c_void,
                    std::mem::size_of_val(&val) as libc::socklen_t,
                );
                if ret != 0 {
                    tracing::trace!(error = %std::io::Error::last_os_error(), "SO_TXREHASH skipped");
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

            // Linux 6.9+ TCP_RTO_MIN_US, TCP_DELACK_MAX_US, TCP_RTO_MAX_MS
            const TCP_RTO_MAX_MS: libc::c_int = 44;
            const TCP_RTO_MIN_US: libc::c_int = 45;
            const TCP_DELACK_MAX_US: libc::c_int = 46;

            if let Some(rto_min) = self.rto_min_us {
                let val = rto_min as libc::c_uint;
                let ret = libc::setsockopt(
                    fd,
                    libc::IPPROTO_TCP,
                    TCP_RTO_MIN_US,
                    &val as *const _ as *const libc::c_void,
                    std::mem::size_of_val(&val) as libc::socklen_t,
                );
                if ret != 0 {
                    tracing::trace!(error = %std::io::Error::last_os_error(), "TCP_RTO_MIN_US skipped");
                }
            }

            if let Some(delack) = self.delack_max_us {
                let val = delack as libc::c_uint;
                let ret = libc::setsockopt(
                    fd,
                    libc::IPPROTO_TCP,
                    TCP_DELACK_MAX_US,
                    &val as *const _ as *const libc::c_void,
                    std::mem::size_of_val(&val) as libc::socklen_t,
                );
                if ret != 0 {
                    tracing::trace!(error = %std::io::Error::last_os_error(), "TCP_DELACK_MAX_US skipped");
                }
            }

            if let Some(rto_max) = self.rto_max_ms {
                let val = rto_max as libc::c_uint;
                let ret = libc::setsockopt(
                    fd,
                    libc::IPPROTO_TCP,
                    TCP_RTO_MAX_MS,
                    &val as *const _ as *const libc::c_void,
                    std::mem::size_of_val(&val) as libc::socklen_t,
                );
                if ret != 0 {
                    tracing::trace!(error = %std::io::Error::last_os_error(), "TCP_RTO_MAX_MS skipped");
                }
            }
        }
        #[cfg(not(target_os = "linux"))]
        let _ = fd;
    }
}

/// Maps a [`velda_core::MemoryTier`] to a `TCP_NOTSENT_LOWAT` byte threshold.
pub const fn notsent_lowat_for_mem_tier(tier: velda_core::MemoryTier) -> u32 {
    use velda_core::MemoryTier;
    const KB: u32 = 1024;
    match tier {
        MemoryTier::Constrained | MemoryTier::Small => 16 * KB,
        MemoryTier::Medium => 32 * KB,
        MemoryTier::Large | MemoryTier::XLarge => 64 * KB,
        MemoryTier::TwoXLarge | MemoryTier::Ultra => 128 * KB,
    }
}

/// Connects a raw TCP stream to the target physical backend endpoint,
/// applying pre-connect socket acceleration, connection timeout, and post-connect tuning.
pub async fn connect_tcp_stream(
    endpoint: SocketAddr,
    acceleration: &TcpAccelerationPath,
    timeout: Duration,
) -> Result<TcpStream, std::io::Error> {
    let socket = if endpoint.is_ipv4() {
        tokio::net::TcpSocket::new_v4()?
    } else {
        tokio::net::TcpSocket::new_v6()?
    };

    #[cfg(target_os = "linux")]
    {
        use std::os::unix::io::AsRawFd;
        acceleration.apply_pre_connect(socket.as_raw_fd());
    }

    let connect_fut = socket.connect(endpoint);
    let stream = tokio::time::timeout(timeout, connect_fut)
        .await
        .map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!("Connect timeout ({timeout:?}) to {endpoint}"),
            )
        })??;

    let _ = stream.set_nodelay(acceleration.nodelay);
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::io::AsRawFd;
        acceleration.apply_post_connect(stream.as_raw_fd());
    }

    Ok(stream)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tcp_acceleration_defaults() {
        let path = TcpAccelerationPath::default();
        assert!(path.nodelay);
        assert!(!path.fastopen);
        assert!(path.quickack);
        assert!(!path.bbr);
    }

    #[test]
    fn test_notsent_lowat_tier_scaling() {
        use velda_core::MemoryTier;
        assert_eq!(
            notsent_lowat_for_mem_tier(MemoryTier::Constrained),
            16 * 1024
        );
        assert_eq!(notsent_lowat_for_mem_tier(MemoryTier::Medium), 32 * 1024);
        assert_eq!(notsent_lowat_for_mem_tier(MemoryTier::Large), 64 * 1024);
        assert_eq!(notsent_lowat_for_mem_tier(MemoryTier::Ultra), 128 * 1024);
    }
}
