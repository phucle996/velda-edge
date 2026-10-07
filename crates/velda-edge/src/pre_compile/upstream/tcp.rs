//! Layer 4 TCP Upstream managing connection acquisition and stream forwarding.

use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::TcpStream;

use super::lb::EdgeUpstream;
use crate::error::EdgeError;

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

        let user_timeout =
            if ladder.dead_peer_teardown >= velda_core::DeadPeerTeardownTier::UserTimeout {
                let candidate = timeouts.connect.saturating_mul(3);
                let cap = timeouts.idle.min(Duration::from_secs(30));
                Some(candidate.max(cap).max(Duration::from_secs(10)))
            } else {
                None
            };

        let keepalive = Some(timeouts.idle / 2);

        let syncnt = if topo.kernel.supports_tcp_syncnt() {
            Some(3)
        } else {
            None
        };

        let quickack = topo.kernel.supports_quickack();
        let bbr = topo.kernel.supports_bbr();

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
                // For intra-VPC / cloud datacenter backend upstreams:
                // 5ms min RTO (vs 200ms default) enables sub-millisecond recovery on dropped packets.
                // 2ms max delayed ACK (vs 40ms default) eliminates ACK stalls without user-space quickack polling.
                // 1000ms max RTO bounds exponential backoff freezes on partitioned backends.
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
            quickack,
            bbr,
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
                let val: libc::c_int = 1;
                let _ = libc::setsockopt(
                    fd,
                    libc::SOL_SOCKET,
                    libc::SO_KEEPALIVE,
                    &val as *const _ as *const libc::c_void,
                    std::mem::size_of_val(&val) as libc::socklen_t,
                );

                let idle_secs = keepalive.as_secs().max(1) as libc::c_int;
                let _ = libc::setsockopt(
                    fd,
                    libc::IPPROTO_TCP,
                    libc::TCP_KEEPIDLE,
                    &idle_secs as *const _ as *const libc::c_void,
                    std::mem::size_of_val(&idle_secs) as libc::socklen_t,
                );

                let intvl_secs = (idle_secs / 3).clamp(3, 10);
                let _ = libc::setsockopt(
                    fd,
                    libc::IPPROTO_TCP,
                    libc::TCP_KEEPINTVL,
                    &intvl_secs as *const _ as *const libc::c_void,
                    std::mem::size_of_val(&intvl_secs) as libc::socklen_t,
                );

                let cnt: libc::c_int = 3;
                let _ = libc::setsockopt(
                    fd,
                    libc::IPPROTO_TCP,
                    libc::TCP_KEEPCNT,
                    &cnt as *const _ as *const libc::c_void,
                    std::mem::size_of_val(&cnt) as libc::socklen_t,
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
                let bbr_name = b"bbr\0";
                let _ = libc::setsockopt(
                    fd,
                    libc::IPPROTO_TCP,
                    libc::TCP_CONGESTION,
                    bbr_name.as_ptr() as *const libc::c_void,
                    bbr_name.len() as libc::socklen_t,
                );
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
                    tracing::trace!(error = %std::io::Error::last_os_error(), "SO_BUSY_POLL skipped (unprivileged container)");
                }
            }

            // Linux 5.18+ SO_TXREHASH: Automatic ECMP flow label rehash upon loss/RTO
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

/// Layer 4 TCP Upstream managing connection acquisition and stream forwarding.
///
/// Pre-compiled with static load balancer, physical discovery endpoints, and socket acceleration path.
pub struct TcpUpstream {
    /// [PRE-COMPILED]: Pre-assembled upstream core holding discovery, health tracker,
    /// timeouts, and the selected `LbAlgorithm` enum variant.
    inner: EdgeUpstream,
    acceleration: TcpAccelerationPath,
}

impl std::fmt::Debug for TcpUpstream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TcpUpstream")
            .field("id", &self.inner.id())
            .field("acceleration", &self.acceleration)
            .finish()
    }
}

impl TcpUpstream {
    /// Creates a new [`TcpUpstream`] instance.
    pub fn new(inner: EdgeUpstream, acceleration: TcpAccelerationPath) -> Self {
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

    /// Returns the upstream timeouts.
    #[inline]
    pub fn timeouts(&self) -> velda_upstream::UpstreamTimeouts {
        *self.inner.timeouts()
    }

    /// Hands off downstream byte forwarding to an acquired upstream backend connection.
    pub async fn dispatch_stream<F, Fut, T, E>(&self, pipe: F) -> Result<T, EdgeError>
    where
        F: FnOnce(TcpStream) -> Fut,
        Fut: std::future::Future<Output = Result<T, E>>,
        E: std::fmt::Display,
    {
        let socket_accel = self.acceleration;
        let connect_timeout = self.inner.timeouts().connect;

        let stream = self
            .inner
            .execute(|endpoint| async move {
                connect_tcp_stream(endpoint, &socket_accel, connect_timeout)
                    .await
                    .map_err(|e| e.to_string())
            })
            .await
            .map_err(EdgeError::Upstream)?;

        pipe(stream).await.map_err(|e| {
            EdgeError::Upstream(velda_upstream::UpstreamError::Protocol(e.to_string()))
        })
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;
    use velda_core::hardware::{AccelerationTier, HardwareTopology, KernelProfile, KernelVersion};
    use velda_upstream::UpstreamTimeouts;

    use super::*;

    fn test_timeouts() -> UpstreamTimeouts {
        UpstreamTimeouts {
            connect: Duration::from_millis(500),
            idle: Duration::from_secs(60),
            request: Some(Duration::from_secs(5)),
        }
    }

    #[test]
    fn test_tcp_acceleration_ladder_linux_3_10() {
        let kernel = KernelProfile::new(
            KernelVersion::new(3, 10, 0),
            AccelerationTier::Standard,
            "test_legacy",
        );
        let topo = HardwareTopology::with_workers_and_memory(8, 8 * 1024 * 1024 * 1024)
            .with_kernel(kernel);
        let timeouts = test_timeouts();

        let path = TcpAccelerationPath::for_topology(&topo, &timeouts, false);
        assert!(!path.tx_rehash);
        assert!(!path.prefer_busy_poll);
        assert!(path.rto_min_us.is_none());
        assert!(path.delack_max_us.is_none());
        assert!(path.rto_max_ms.is_none());
        assert!(!path.bind_address_no_port);
    }

    #[test]
    fn test_tcp_acceleration_ladder_linux_5_11() {
        let kernel = KernelProfile::new(
            KernelVersion::new(5, 11, 0),
            AccelerationTier::Standard,
            "test_prefer_busy_poll",
        );
        let topo = HardwareTopology::with_workers_and_memory(16, 16 * 1024 * 1024 * 1024)
            .with_kernel(kernel);
        let timeouts = test_timeouts();

        let path = TcpAccelerationPath::for_topology(&topo, &timeouts, false);
        assert!(path.prefer_busy_poll);
        assert!(!path.tx_rehash);
        assert!(path.rto_min_us.is_none());

        // Verify that on Medium / constrained CPU (< 9 cores), busy polling is disabled to protect CPU budgets
        let topo_medium = HardwareTopology::with_workers_and_memory(8, 8 * 1024 * 1024 * 1024)
            .with_kernel(kernel);
        let path_medium = TcpAccelerationPath::for_topology(&topo_medium, &timeouts, false);
        assert!(!path_medium.prefer_busy_poll);
    }

    #[test]
    fn test_tcp_acceleration_ladder_linux_5_18() {
        let kernel = KernelProfile::new(
            KernelVersion::new(5, 18, 0),
            AccelerationTier::Standard,
            "test_txrehash",
        );
        let topo = HardwareTopology::with_workers_and_memory(16, 16 * 1024 * 1024 * 1024)
            .with_kernel(kernel);
        let timeouts = test_timeouts();

        let path = TcpAccelerationPath::for_topology(&topo, &timeouts, false);
        assert!(path.prefer_busy_poll);
        assert!(path.tx_rehash);
        assert!(path.rto_min_us.is_none());
    }

    #[test]
    fn test_tcp_acceleration_ladder_linux_6_9() {
        let kernel = KernelProfile::new(
            KernelVersion::new(6, 9, 0),
            AccelerationTier::IoUringFastPath,
            "test_microsecond_rto",
        );
        let topo = HardwareTopology::with_workers_and_memory(16, 16 * 1024 * 1024 * 1024)
            .with_kernel(kernel);
        let timeouts = test_timeouts();

        let path = TcpAccelerationPath::for_topology(&topo, &timeouts, false);
        assert!(path.prefer_busy_poll);
        assert!(path.tx_rehash);
        assert_eq!(path.rto_min_us, Some(5_000));
        assert_eq!(path.delack_max_us, Some(2_000));
        assert_eq!(path.rto_max_ms, Some(1_000));
    }
}
