//! Upstream HTTP/1.1 Client Connection Establishment and Socket Acceleration (RFC 9112).
//!
//! Architectural Perspective (Góc nhìn Ingress):
//! Handles outbound HTTP/1.1 client connection establishment, Linux TCP acceleration paths,
//! and TLS ALPN negotiation directly to backend microservices.

use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use velda_tls::{TlsClientEngine, TlsStream};

use crate::error::Http1Error;

/// Upstream stream connection either over cleartext TCP or encrypted TLS.
pub enum UpstreamHttp1Stream {
    Plain(tokio::net::TcpStream),
    Tls(Box<TlsStream<tokio::net::TcpStream>>),
}

impl UpstreamHttp1Stream {
    /// Validates whether the underlying TCP stream is still open and healthy.
    ///
    /// Performs a non-blocking 1-byte read without blocking:
    /// - `Err(WouldBlock)`: Connection is open and idle (no pending data) -> healthy.
    /// - `Ok(0)`: Remote peer closed the connection (FIN/EOF) -> unhealthy.
    /// - `Ok(n)`: Unexpected unread bytes on idle keep-alive connection -> unhealthy.
    /// - `Err(_)`: Socket reset or I/O error -> unhealthy.
    pub fn is_healthy(&self) -> bool {
        let mut buf = [0u8; 1];
        let res = match self {
            Self::Plain(s) => s.try_read(&mut buf),
            Self::Tls(s) => s.get_ref().0.try_read(&mut buf),
        };
        matches!(res, Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock)
    }
}

impl std::fmt::Debug for UpstreamHttp1Stream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Plain(s) => f.debug_tuple("Plain").field(s).finish(),
            Self::Tls(_) => f.debug_tuple("Tls").finish(),
        }
    }
}

impl AsyncRead for UpstreamHttp1Stream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Plain(s) => Pin::new(s).poll_read(cx, buf),
            Self::Tls(s) => Pin::new(s.as_mut()).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for UpstreamHttp1Stream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            Self::Plain(s) => Pin::new(s).poll_write(cx, buf),
            Self::Tls(s) => Pin::new(s.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Plain(s) => Pin::new(s).poll_flush(cx),
            Self::Tls(s) => Pin::new(s.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Plain(s) => Pin::new(s).poll_shutdown(cx),
            Self::Tls(s) => Pin::new(s.as_mut()).poll_shutdown(cx),
        }
    }
}

/// Pre-compiled Linux socket acceleration path for HTTP/1.1 backend client streams.
///
/// Pre-computed at bootstrap to keep connection establishment on a branchless hot path
/// without repeated hardware or kernel capability checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Http1AccelerationPath {
    /// Disables Nagle's algorithm (`TCP_NODELAY`) to eliminate 40ms delayed-ACK packet coalescing delays.
    pub nodelay: bool,
    /// Sends ClientHello directly in SYN packet (`TCP_FASTOPEN_CONNECT`), shaving 1 full RTT
    /// off TLS handshakes. Only enabled for TLS where cryptographic replay protection is guaranteed.
    pub fastopen: bool,
    /// Caps unsent bytes in the socket write queue (`TCP_NOTSENT_LOWAT`) to prevent bufferbloat
    /// and keep epoll notification responsive to user-space backpressure.
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
}

impl Default for Http1AccelerationPath {
    fn default() -> Self {
        Self {
            nodelay: true,
            fastopen: false,
            notsent_lowat: None,
            user_timeout: None,
            keepalive: None,
            busy_poll_us: None,
        }
    }
}

impl Http1AccelerationPath {
    /// Pre-compiles HTTP/1.1 socket acceleration path from hardware topology, timeouts, and TLS status.
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

        Self {
            nodelay: true,
            fastopen,
            notsent_lowat,
            user_timeout,
            keepalive,
            busy_poll_us,
        }
    }

    /// Applies pre-connect socket acceleration options (e.g. `TCP_FASTOPEN_CONNECT`).
    ///
    /// Unprivileged containers (Docker default seccomp, K8s unprivileged pods) often block
    /// Fast Open with `EPERM` or `ENOPROTOOPT`. Logging at trace level and proceeding ensures
    /// traffic still flows without hard connection failures.
    pub fn apply_pre_connect(&self, fd: std::os::unix::io::RawFd) {
        #[cfg(target_os = "linux")]
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
        #[cfg(not(target_os = "linux"))]
        let _ = fd;
    }

    /// Applies post-connect socket acceleration options (`TCP_NODELAY`, `TCP_NOTSENT_LOWAT`,
    /// `TCP_USER_TIMEOUT`, `SO_KEEPALIVE`, `TCP_KEEPIDLE`, `SO_BUSY_POLL`).
    ///
    /// Invoked immediately after handshake. Any option rejected by host seccomp or legacy
    /// kernel is skipped at trace level rather than dropping an established backend stream.
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
///
/// Keeps a 16KB floor so NIC TCP Segmentation Offload (TSO) is not starved and epoll is not woken per packet,
/// while scaling up to 128KB on large servers to prevent kernel bufferbloat.
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

/// Connects directly to an HTTP/1.1 backend target with optional socket acceleration, timeout, and TLS encryption.
///
/// `tls` is `Some((engine, sni))` for TLS upstreams. The SNI is the upstream's pre-compiled
/// static property; it is never derived from the request Host or the target IP.
pub async fn connect(
    target: SocketAddr,
    tls: Option<(&TlsClientEngine, &str)>,
    acceleration: Option<&Http1AccelerationPath>,
    timeout: Option<std::time::Duration>,
) -> Result<UpstreamHttp1Stream, Http1Error> {
    let socket = if target.is_ipv4() {
        tokio::net::TcpSocket::new_v4().map_err(Http1Error::Io)?
    } else {
        tokio::net::TcpSocket::new_v6().map_err(Http1Error::Io)?
    };

    #[cfg(target_os = "linux")]
    if let Some(accel) = acceleration {
        use std::os::unix::io::AsRawFd;
        accel.apply_pre_connect(socket.as_raw_fd());
    }

    let connect_fut = socket.connect(target);
    let tcp_stream = if let Some(to) = timeout {
        tokio::time::timeout(to, connect_fut)
            .await
            .map_err(|_| {
                Http1Error::Io(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "TCP connect timed out",
                ))
            })?
            .map_err(Http1Error::Io)?
    } else {
        connect_fut.await.map_err(Http1Error::Io)?
    };

    if let Some(accel) = acceleration {
        let _ = tcp_stream.set_nodelay(accel.nodelay);
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::io::AsRawFd;
            accel.apply_post_connect(tcp_stream.as_raw_fd());
        }
    } else {
        let _ = tcp_stream.set_nodelay(true);
    }

    if let Some((engine, sni)) = tls {
        let tls_stream = engine
            .connect(sni, tcp_stream)
            .await
            .map_err(|e| Http1Error::Io(std::io::Error::other(e.to_string())))?;
        if let Some(alpn) = tls_stream.get_ref().1.alpn_protocol()
            && alpn != b"http/1.1"
        {
            return Err(Http1Error::Io(std::io::Error::other(format!(
                "upstream negotiated ALPN {:?}, expected \"http/1.1\"",
                String::from_utf8_lossy(alpn)
            ))));
        }
        Ok(UpstreamHttp1Stream::Tls(Box::new(tls_stream)))
    } else {
        Ok(UpstreamHttp1Stream::Plain(tcp_stream))
    }
}
