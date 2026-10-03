//! Layer 4 TCP Upstream managing connection acquisition and stream forwarding.

use tokio::net::TcpStream;
use velda_upstream::UpstreamError;

use super::lb::EdgeUpstream;
use crate::error::EdgeError;

/// Layer 4 TCP Upstream managing connection acquisition and stream forwarding.
///
/// Pre-compiled with static load balancer, physical discovery endpoints, and connection pool.
pub struct TcpUpstream {
    /// [PRE-COMPILED]: Pre-assembled upstream core holding discovery, health tracker,
    /// timeouts, connection pool, and the selected `LbAlgorithm` enum variant.
    inner: EdgeUpstream,
}

impl std::fmt::Debug for TcpUpstream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TcpUpstream")
            .field("id", &self.inner.id())
            .finish()
    }
}

impl TcpUpstream {
    /// Creates a new [`TcpUpstream`] instance.
    pub fn new(inner: EdgeUpstream) -> Self {
        Self { inner }
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
        let lease = self.inner.acquire().await.map_err(EdgeError::Upstream)?;
        let target = lease.endpoint();
        let Some(stream) = lease.into_tcp_stream() else {
            return Err(EdgeError::Internal(
                "L4 TCP upstream lease did not contain raw TcpStream".into(),
            ));
        };
        pipe(stream).await.map_err(|e| {
            EdgeError::Upstream(UpstreamError::ConnectionFailed {
                endpoint: target,
                reason: e.to_string(),
            })
        })
    }
}
