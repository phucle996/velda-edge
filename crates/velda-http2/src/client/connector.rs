//! Upstream HTTP/2 Client Connection Establishment (RFC 9113).
//!
//! Handles TCP connection establishment, TLS/generic stream handshakes, and background H2
//! connection driver execution.

use std::net::SocketAddr;

use bytes::Bytes;

use crate::config::Http2Config;
use crate::error::Http2Error;

/// Upstream HTTP/2 socket acceleration path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Http2AccelerationPath {
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

impl Default for Http2AccelerationPath {
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

impl Http2AccelerationPath {
    /// Pre-compiles HTTP/2 socket acceleration path from hardware topology, timeouts, and TLS status.
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
fn apply_post_connect_options(fd: std::os::unix::io::RawFd, accel: &Http2AccelerationPath) {
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

/// Connects to the upstream target over TCP with optional socket acceleration, timeout, and performs the HTTP/2 client handshake.
///
/// Spawns the H2 connection driver onto a background Tokio task.
pub async fn connect(
    target: SocketAddr,
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
    if acceleration.is_some_and(|accel| accel.fastopen) {
        use std::os::unix::io::AsRawFd;
        apply_tcp_fastopen(socket.as_raw_fd());
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
            apply_post_connect_options(stream.as_raw_fd(), accel);
        }
    } else {
        let _ = stream.set_nodelay(true);
    }

    connect_stream(stream, config).await
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
