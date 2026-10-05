//! Layer 4 TCP Upstream managing connection acquisition and stream forwarding.

use tokio::net::TcpStream;

use super::lb::EdgeUpstream;
use crate::error::EdgeError;

/// Layer 4 TCP Upstream managing connection acquisition and stream forwarding.
///
/// Pre-compiled with static load balancer, physical discovery endpoints, and connection pool.
pub struct TcpUpstream {
    /// [PRE-COMPILED]: Pre-assembled upstream core holding discovery, health tracker,
    /// timeouts, connection pool, and the selected `LbAlgorithm` enum variant.
    inner: EdgeUpstream,
    acceleration: velda_upstream::SocketAccelerationPath,
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
    pub fn new(inner: EdgeUpstream, acceleration: velda_upstream::SocketAccelerationPath) -> Self {
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
                let socket = if endpoint.is_ipv4() {
                    tokio::net::TcpSocket::new_v4().map_err(|e| e.to_string())?
                } else {
                    tokio::net::TcpSocket::new_v6().map_err(|e| e.to_string())?
                };

                #[cfg(target_os = "linux")]
                if socket_accel.fastopen {
                    use std::os::unix::io::AsRawFd;
                    apply_tcp_fastopen(socket.as_raw_fd());
                }

                let connect_fut = socket.connect(endpoint);
                let stream = tokio::time::timeout(connect_timeout, connect_fut)
                    .await
                    .map_err(|_| format!("Connect timeout ({connect_timeout:?}) to {endpoint}"))?
                    .map_err(|e| e.to_string())?;

                let _ = stream.set_nodelay(socket_accel.nodelay);
                #[cfg(target_os = "linux")]
                {
                    use std::os::unix::io::AsRawFd;
                    apply_post_connect_acceleration(stream.as_raw_fd(), &socket_accel);
                }

                Ok::<_, String>(stream)
            })
            .await
            .map_err(EdgeError::Upstream)?;

        pipe(stream).await.map_err(|e| {
            EdgeError::Upstream(velda_upstream::UpstreamError::Protocol(e.to_string()))
        })
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
fn apply_post_connect_acceleration(
    fd: std::os::unix::io::RawFd,
    accel: &velda_upstream::SocketAccelerationPath,
) {
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
