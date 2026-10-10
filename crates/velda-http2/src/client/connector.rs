//! Upstream HTTP/2 Client Connection Establishment (RFC 9113).
//!
//! Handles TCP connection establishment, TLS/generic stream handshakes, and background H2
//! connection driver execution.

use std::net::SocketAddr;

use bytes::Bytes;
use velda_tls::TlsClientEngine;

use crate::config::Http2Config;
use crate::error::Http2Error;

/// Pre-compiled Linux socket acceleration path for HTTP/2 backend client streams (RFC 9113).
///
/// Pre-computed at bootstrap to keep connection establishment on a branchless hot path
/// without repeated hardware or kernel capability checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Http2AccelerationPath {
    /// Disables Nagle's algorithm (`TCP_NODELAY`) to eliminate 40ms delayed-ACK packet coalescing delays.
    pub nodelay: bool,
    /// Sends ClientHello directly in SYN packet (`TCP_FASTOPEN_CONNECT`), shaving 1 full RTT
    /// off TLS handshakes. Only enabled for TLS where replay protection is cryptographic.
    pub fastopen: bool,
    /// Caps unsent bytes in the socket write queue (`TCP_NOTSENT_LOWAT`).
    ///
    /// In HTTP/2, streams share a single TCP connection. Large kernel send buffers cause
    /// frames to sit in kernel queues, defeating user-space stream prioritization and window updates.
    /// A lower threshold ensures backpressure propagates immediately so high-priority frames preempt low-priority data.
    pub notsent_lowat: Option<u32>,
    /// Aborts stuck TCP streams (`TCP_USER_TIMEOUT`) when cloud NAT gateways or firewalls silently
    /// drop packets, replacing the default 15-minute OS retransmit timeout with upstream deadline.
    pub user_timeout: Option<std::time::Duration>,
    /// Periodically probes idle connections (`SO_KEEPALIVE` + `TCP_KEEPIDLE`) to keep stateful
    /// middlebox / NAT table entries warm and detect silent peer reboots.
    pub keepalive: Option<std::time::Duration>,
    /// Low-latency socket polling in kernel space (`SO_BUSY_POLL`) to bypass epoll sleep/wake
    /// context-switch latency spikes on high-core server tiers.
    pub busy_poll_us: Option<u32>,
    /// Commands the kernel to rehash IPv6 flow label / 4-tuple on loss/RTO (`SO_TXREHASH`, Linux >= 5.18).
    pub tx_rehash: bool,
    /// Instructs kernel NAPI to suppress IRQs and prioritize polling (`SO_PREFER_BUSY_POLL` + `SO_BUSY_POLL_BUDGET`, Linux >= 5.11).
    pub prefer_busy_poll: bool,
    /// Microsecond minimum retransmission timeout floor (`TCP_RTO_MIN_US`, Linux >= 6.9).
    pub rto_min_us: Option<u32>,
    /// Caps delayed ACK timer in microseconds (`TCP_DELACK_MAX_US`, Linux >= 6.9) down to 2ms.
    pub delack_max_us: Option<u32>,
    /// Caps maximum RTO in milliseconds (`TCP_RTO_MAX_MS`, Linux >= 6.9) down to 1000ms.
    pub rto_max_ms: Option<u32>,
    /// Optional socket receive and send buffer size hint (`SO_RCVBUF` / `SO_SNDBUF`).
    pub socket_buffer_size: Option<usize>,
    /// Attempts BBR congestion control (`TCP_CONGESTION`) for high-throughput egress.
    pub bbr: bool,
    /// Delays ephemeral source port selection until `connect(2)` (`IP_BIND_ADDRESS_NO_PORT`),
    /// eliminating the 64,000 ephemeral outbound port ceiling.
    pub bind_address_no_port: bool,
    /// Immediately acknowledges incoming upstream response packets (`TCP_QUICKACK`) to prevent
    /// 40ms delayed-ACK stalls on backend responses.
    pub quickack: bool,
}

impl Default for Http2AccelerationPath {
    fn default() -> Self {
        Self {
            nodelay: true,
            fastopen: false,
            notsent_lowat: None,
            user_timeout: None,
            keepalive: None,
            busy_poll_us: None,
            tx_rehash: false,
            prefer_busy_poll: false,
            rto_min_us: None,
            delack_max_us: None,
            rto_max_ms: None,
            socket_buffer_size: None,
            bbr: false,
            bind_address_no_port: false,
            quickack: true,
        }
    }
}

impl Http2AccelerationPath {
    /// Pre-compiles HTTP/2 socket acceleration path from hardware topology, timeouts, and TLS status.
    ///
    /// Fast Open is only enabled for TLS endpoints because raw HTTP requests (like POST) are not
    /// idempotent and could suffer from TCP retransmission replays.
    pub fn for_topology(
        topo: &velda_core::HardwareTopology,
        connect_timeout: std::time::Duration,
        idle_timeout: std::time::Duration,
        is_tls: bool,
    ) -> Self {
        let fastopen = is_tls && topo.kernel.supports_tcp_fastopen_connect();

        let notsent_lowat = if topo.kernel.supports_tcp_notsent_lowat() {
            Some(notsent_lowat_for_mem_tier(topo.memory_tier()))
        } else {
            None
        };

        let user_timeout = if topo.kernel.supports_tcp_user_timeout() {
            let candidate = connect_timeout.saturating_mul(3);
            let cap = idle_timeout.min(std::time::Duration::from_secs(30));
            Some(candidate.max(cap).max(std::time::Duration::from_secs(10)))
        } else {
            None
        };

        let keepalive = Some(idle_timeout / 2);

        // Enable SO_BUSY_POLL only on large multi-core hardware tiers where dedicated core polling
        // yields sub-millisecond tail latency wins without starving small-tier CPU budgets.
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

        let ladder = topo.acceleration_ladder();
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

        let socket_buffer_size = match topo.memory_tier() {
            velda_core::MemoryTier::Constrained => None,
            velda_core::MemoryTier::Small => Some(128 * 1024),
            velda_core::MemoryTier::Medium => Some(256 * 1024),
            velda_core::MemoryTier::Large => Some(512 * 1024),
            _ => Some(1024 * 1024),
        };

        let bbr = topo.kernel.supports_bbr();
        let bind_address_no_port =
            ladder.outbound_port_scaling >= velda_core::OutboundPortScalingTier::BindAddressNoPort;
        let quickack = true;

        Self {
            nodelay: true,
            fastopen,
            notsent_lowat,
            user_timeout,
            keepalive,
            busy_poll_us,
            tx_rehash,
            prefer_busy_poll,
            rto_min_us,
            delack_max_us,
            rto_max_ms,
            socket_buffer_size,
            bbr,
            bind_address_no_port,
            quickack,
        }
    }

    /// Applies pre-connect socket acceleration options (e.g. `TCP_FASTOPEN_CONNECT`, `IP_BIND_ADDRESS_NO_PORT`).
    ///
    /// Unprivileged containers (Docker default seccomp, K8s unprivileged pods) often block
    /// Fast Open with `EPERM` or `ENOPROTOOPT`. Logging at trace level and proceeding ensures
    /// traffic still flows without hard connection failures.
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
        }
        #[cfg(not(target_os = "linux"))]
        let _ = fd;
    }

    /// Applies post-connect socket acceleration options (`TCP_NODELAY`, `TCP_NOTSENT_LOWAT`,
    /// `TCP_USER_TIMEOUT`, `SO_KEEPALIVE`, `TCP_KEEPIDLE`, `SO_BUSY_POLL`, `SO_RCVBUF`, `SO_SNDBUF`).
    ///
    /// Invoked immediately after connection handshake. Any option rejected by host seccomp
    /// or legacy kernel is skipped at trace level rather than dropping an established backend stream.
    pub fn apply_post_connect(&self, fd: std::os::unix::io::RawFd) {
        #[cfg(target_os = "linux")]
        unsafe {
            if let Some(buf_size) = self.socket_buffer_size {
                let val = buf_size as libc::c_int;
                let _ = libc::setsockopt(
                    fd,
                    libc::SOL_SOCKET,
                    libc::SO_RCVBUF,
                    &val as *const _ as *const libc::c_void,
                    std::mem::size_of_val(&val) as libc::socklen_t,
                );
                let _ = libc::setsockopt(
                    fd,
                    libc::SOL_SOCKET,
                    libc::SO_SNDBUF,
                    &val as *const _ as *const libc::c_void,
                    std::mem::size_of_val(&val) as libc::socklen_t,
                );
            }

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
                let ret = libc::setsockopt(
                    fd,
                    libc::IPPROTO_TCP,
                    libc::TCP_CONGESTION,
                    bbr_name.as_ptr() as *const libc::c_void,
                    bbr_name.len() as libc::socklen_t,
                );
                if ret != 0 {
                    tracing::trace!(error = %std::io::Error::last_os_error(), "TCP_CONGESTION bbr skipped");
                }
            }
        }
        #[cfg(not(target_os = "linux"))]
        let _ = fd;
    }
}

/// Maps a [`velda_core::MemoryTier`] to a `TCP_NOTSENT_LOWAT` byte threshold.
///
/// Thresholds are scaled to memory tier with a 16KB minimum:
/// - Setting the threshold too low (< 16KB) starves NIC TCP Segmentation Offload (TSO) and
///   causes epoll to wake on almost every single packet, degrading CPU cache locality.
/// - Setting it too high (> 128KB) causes bufferbloat in the kernel socket buffer and defeats
///   multiplexed stream prioritization.
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

/// Connects to the upstream target over TCP with optional socket acceleration, timeout, optional TLS,
/// and performs the HTTP/2 client handshake.
///
/// `tls` is `Some((engine, sni))` for TLS upstreams; the SNI is the upstream's pre-compiled static
/// property. The protocol is always HTTP/2 as declared by the upstream: ALPN is only validated
/// (a negotiated value other than `h2` is rejected), never used to choose the protocol.
///
/// Spawns the H2 connection driver onto a background Tokio task.
pub async fn connect(
    target: SocketAddr,
    tls: Option<(&TlsClientEngine, &str)>,
    config: &Http2Config,
    acceleration: Option<&Http2AccelerationPath>,
    timeout: Option<std::time::Duration>,
) -> Result<h2::client::SendRequest<Bytes>, Http2Error> {
    let socket = if target.is_ipv4() {
        tokio::net::TcpSocket::new_v4()?
    } else {
        tokio::net::TcpSocket::new_v6()?
    };

    #[cfg(target_os = "linux")]
    if let Some(accel) = acceleration {
        use std::os::unix::io::AsRawFd;
        accel.apply_pre_connect(socket.as_raw_fd());
    }

    let connect_fut = socket.connect(target);
    let stream = if let Some(to) = timeout {
        tokio::time::timeout(to, connect_fut).await.map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::TimedOut, "TCP connect timed out")
        })??
    } else {
        connect_fut.await?
    };

    if let Some(accel) = acceleration {
        let _ = stream.set_nodelay(accel.nodelay);
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::io::AsRawFd;
            accel.apply_post_connect(stream.as_raw_fd());
        }
    } else {
        let _ = stream.set_nodelay(true);
    }

    let Some((engine, sni)) = tls else {
        return connect_stream(stream, config).await;
    };

    let tls_stream = engine
        .connect(sni, stream)
        .await
        .map_err(|e| Http2Error::Tls(e.to_string()))?;
    if let Some(alpn) = tls_stream.get_ref().1.alpn_protocol()
        && alpn != b"h2"
    {
        return Err(Http2Error::Tls(format!(
            "upstream negotiated ALPN {:?}, expected \"h2\"",
            String::from_utf8_lossy(alpn)
        )));
    }
    connect_stream(tls_stream, config).await
}

/// Performs the HTTP/2 client handshake over an arbitrary asynchronous I/O stream (e.g. TLS or TCP).
///
/// Spawns the H2 connection driver onto a background Tokio task.
pub async fn connect_stream<IO>(
    stream: IO,
    config: &Http2Config,
) -> Result<h2::client::SendRequest<Bytes>, Http2Error>
where
    IO: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let mut builder = h2::client::Builder::default();
    builder
        .initial_connection_window_size(config.initial_connection_window_size)
        .initial_window_size(config.initial_stream_window_size)
        .max_concurrent_streams(config.max_concurrent_streams)
        .max_frame_size(config.max_frame_size)
        .max_header_list_size(config.max_header_list_size)
        .enable_push(config.enable_push)
        .max_send_buffer_size(config.max_send_buffer_size)
        .max_concurrent_reset_streams(config.max_consecutive_resets as usize)
        .max_pending_accept_reset_streams(config.max_consecutive_resets as usize);

    let (client, h2_conn) = builder.handshake(stream).await?;
    tokio::spawn(async move {
        let _ = h2_conn.await;
    });

    Ok(client)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_notsent_lowat_scaling() {
        use velda_core::MemoryTier;
        assert_eq!(
            notsent_lowat_for_mem_tier(MemoryTier::Constrained),
            16 * 1024
        );
        assert_eq!(notsent_lowat_for_mem_tier(MemoryTier::Medium), 32 * 1024);
        assert_eq!(notsent_lowat_for_mem_tier(MemoryTier::Large), 64 * 1024);
        assert_eq!(notsent_lowat_for_mem_tier(MemoryTier::Ultra), 128 * 1024);
    }

    #[test]
    fn test_http2_acceleration_path_for_topology() {
        let topo = velda_core::global_hardware_topology();
        let connect_to = std::time::Duration::from_millis(500);
        let idle_to = std::time::Duration::from_secs(30);

        let accel_plain = Http2AccelerationPath::for_topology(topo, connect_to, idle_to, false);
        assert!(accel_plain.nodelay);
        assert!(!accel_plain.fastopen);

        let accel_tls = Http2AccelerationPath::for_topology(topo, connect_to, idle_to, true);
        assert!(accel_tls.nodelay);
        #[cfg(target_os = "linux")]
        {
            if topo.kernel.supports_tcp_fastopen_connect() {
                assert!(accel_tls.fastopen);
            }
            if topo.kernel.supports_bbr() {
                assert!(accel_plain.bbr);
                assert!(accel_tls.bbr);
            }
            if topo.kernel.supports_ip_bind_address_no_port() {
                assert!(accel_plain.bind_address_no_port);
                assert!(accel_tls.bind_address_no_port);
            }
            assert!(accel_plain.quickack);
        }
    }
}
