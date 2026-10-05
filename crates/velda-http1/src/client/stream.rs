//! Upstream HTTP/1.1 stream connection (Plain TCP or encrypted TLS).

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

/// Upstream HTTP/1.1 socket acceleration path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Http1AccelerationPath {
    /// Disable Nagle's algorithm (`TCP_NODELAY`). Defaults to `true`.
    pub nodelay: bool,
    /// Enable TCP Fast Open Connect (`TCP_FASTOPEN_CONNECT`).
    pub fastopen: bool,
    /// Sndbuf low-watermark threshold (`TCP_NOTSENT_LOWAT`) in bytes.
    pub notsent_lowat: Option<u32>,
    /// Maximum time that transmitted data may remain unacknowledged (`TCP_USER_TIMEOUT`).
    pub user_timeout: Option<std::time::Duration>,
    /// TCP Keep-Alive interval (`SO_KEEPALIVE` + `TCP_KEEPIDLE`).
    pub keepalive: Option<std::time::Duration>,
}

impl Default for Http1AccelerationPath {
    fn default() -> Self {
        Self {
            nodelay: true,
            fastopen: false,
            notsent_lowat: None,
            user_timeout: None,
            keepalive: None,
        }
    }
}

impl Http1AccelerationPath {
    /// Pre-compiles HTTP/1.1 socket acceleration path from hardware topology, timeouts, and TLS status.
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

        Self {
            nodelay: true,
            fastopen,
            notsent_lowat,
            user_timeout,
            keepalive,
        }
    }
}

const fn notsent_lowat_for_mem_tier(tier: velda_core::MemoryTier) -> u32 {
    use velda_core::MemoryTier;
    const KB: u32 = 1024;
    match tier {
        MemoryTier::Constrained | MemoryTier::Small => 16 * KB,
        MemoryTier::Medium => 32 * KB,
        MemoryTier::Large | MemoryTier::XLarge => 64 * KB,
        MemoryTier::TwoXLarge | MemoryTier::Ultra => 128 * KB,
    }
}

#[cfg(target_os = "linux")]
fn apply_tcp_fastopen(fd: std::os::unix::io::RawFd) {
    let val: libc::c_int = 1;
    unsafe {
        libc::setsockopt(
            fd,
            libc::IPPROTO_TCP,
            libc::TCP_FASTOPEN_CONNECT,
            &val as *const _ as *const libc::c_void,
            std::mem::size_of_val(&val) as libc::socklen_t,
        );
    }
}

#[cfg(target_os = "linux")]
fn apply_post_connect_options(fd: std::os::unix::io::RawFd, accel: &Http1AccelerationPath) {
    unsafe {
        if let Some(lowat) = accel.notsent_lowat {
            let val = lowat as libc::c_uint;
            libc::setsockopt(
                fd,
                libc::IPPROTO_TCP,
                libc::TCP_NOTSENT_LOWAT,
                &val as *const _ as *const libc::c_void,
                std::mem::size_of_val(&val) as libc::socklen_t,
            );
        }

        if let Some(user_timeout) = accel.user_timeout {
            let val = user_timeout.as_millis() as libc::c_uint;
            libc::setsockopt(
                fd,
                libc::IPPROTO_TCP,
                libc::TCP_USER_TIMEOUT,
                &val as *const _ as *const libc::c_void,
                std::mem::size_of_val(&val) as libc::socklen_t,
            );
        }

        if let Some(keepalive) = accel.keepalive {
            let val: libc::c_int = 1;
            libc::setsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_KEEPALIVE,
                &val as *const _ as *const libc::c_void,
                std::mem::size_of_val(&val) as libc::socklen_t,
            );

            let idle_secs = keepalive.as_secs().max(1) as libc::c_int;
            libc::setsockopt(
                fd,
                libc::IPPROTO_TCP,
                libc::TCP_KEEPIDLE,
                &idle_secs as *const _ as *const libc::c_void,
                std::mem::size_of_val(&idle_secs) as libc::socklen_t,
            );
        }
    }
}

/// Connects directly to an HTTP/1.1 backend target with optional socket acceleration, timeout, and TLS encryption.
pub async fn connect_stream(
    target: SocketAddr,
    is_tls: bool,
    target_sni: Option<&str>,
    tls_client: Option<&TlsClientEngine>,
    host: Option<&str>,
    acceleration: Option<&Http1AccelerationPath>,
    timeout: Option<std::time::Duration>,
) -> Result<UpstreamHttp1Stream, Http1Error> {
    let socket = if target.is_ipv4() {
        tokio::net::TcpSocket::new_v4().map_err(Http1Error::Io)?
    } else {
        tokio::net::TcpSocket::new_v6().map_err(Http1Error::Io)?
    };

    #[cfg(target_os = "linux")]
    if acceleration.is_some_and(|accel| accel.fastopen) {
        use std::os::unix::io::AsRawFd;
        apply_tcp_fastopen(socket.as_raw_fd());
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
            apply_post_connect_options(tcp_stream.as_raw_fd(), accel);
        }
    } else {
        let _ = tcp_stream.set_nodelay(true);
    }

    if is_tls {
        let target_ip_str = target.ip().to_string();
        let sni = target_sni.or(host).unwrap_or(&target_ip_str);
        let client_engine = tls_client.ok_or_else(|| {
            Http1Error::InvalidConfig(format!(
                "Upstream requires TLS for {sni}, but no tls_client engine configured"
            ))
        })?;
        let tls_stream = client_engine
            .connect(sni, tcp_stream)
            .await
            .map_err(|e| Http1Error::Io(std::io::Error::other(e.to_string())))?;
        Ok(UpstreamHttp1Stream::Tls(Box::new(tls_stream)))
    } else {
        Ok(UpstreamHttp1Stream::Plain(tcp_stream))
    }
}
