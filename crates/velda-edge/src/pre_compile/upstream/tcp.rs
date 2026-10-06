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
                let socket = if endpoint.is_ipv4() {
                    tokio::net::TcpSocket::new_v4().map_err(|e| e.to_string())?
                } else {
                    tokio::net::TcpSocket::new_v6().map_err(|e| e.to_string())?
                };

                #[cfg(target_os = "linux")]
                {
                    use std::os::unix::io::AsRawFd;
                    socket_accel.apply_pre_connect(socket.as_raw_fd());
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
                    socket_accel.apply_post_connect(stream.as_raw_fd());
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
