//! Layer 4 TCP Upstream managing connection lifecycle and stream dispatch.
//!
//! Enforces Rule 2.8 by delegating all raw socket creation, Linux setsockopt flags,
//! and transport handshakes to `super::tcp_connector`. Upstream strictly owns
//! target endpoint selection, health state, and stream dispatch.

use tokio::net::TcpStream;

use super::tcp_connector::{TcpAccelerationPath, connect_tcp_stream};
use crate::error::EdgeError;
use crate::upstream::lb::EdgeUpstream;

/// Layer 4 TCP Upstream managing connection acquisition and stream forwarding.
///
/// Pre-compiled with static load balancer, physical discovery endpoints, and pre-computed socket acceleration.
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
