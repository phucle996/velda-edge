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
