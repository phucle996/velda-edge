//! Layer 7 HTTP/2 Upstream managing persistent client multiplexing (RFC 9113) and pipe handoff.

use std::net::SocketAddr;

use bytes::Bytes;
use velda_connection_pool::{MultiplexedPool, PoolableResource};
use velda_core::StreamingMode;
use velda_http2::pipe::Http2PipeStrategy;

use super::lb::EdgeUpstream;
use crate::error::EdgeError;

/// Pooled HTTP/2 client resource wrapping [`h2::client::SendRequest<Bytes>`].
#[derive(Clone, Debug)]
pub struct Http2ClientResource {
    pub client: h2::client::SendRequest<Bytes>,
}

impl Http2ClientResource {
    pub fn new(client: h2::client::SendRequest<Bytes>) -> Self {
        Self { client }
    }
}

impl PoolableResource for Http2ClientResource {
    fn is_healthy(&self) -> bool {
        true
    }

    fn close(&mut self) {}
}

/// Layer 7 HTTP/2 Upstream managing persistent client multiplexing (RFC 9113) and pipe handoff.
///
/// Pre-compiled with static load balancer, wire pipe strategy, and sharded multiplexed client pool.
pub struct Http2Upstream {
    /// [PRE-COMPILED]: Pre-assembled upstream core holding discovery, health tracker,
    /// timeouts, and the selected `LbAlgorithm` enum variant.
    inner: EdgeUpstream,
    /// Declarative streaming mode from upstream configuration.
    pub streaming: StreamingMode,
    /// [PRE-COMPILED]: Pre-computed wire forwarding strategy (`Http2PipeStrategy`) derived from
    /// `streaming` at compile time; eliminates runtime enum branching.
    pub strategy: Http2PipeStrategy,
    /// Lock-sharded persistent HTTP/2 multiplexed client connection pool.
    pool: MultiplexedPool<SocketAddr, Http2ClientResource>,
    /// Maximum concurrent streams per multiplexed connection.
    pub max_concurrent_streams: u32,
    /// [PRE-COMPILED]: Protocol-specific socket acceleration path.
    pub acceleration: velda_http2::Http2AccelerationPath,
}

impl std::fmt::Debug for Http2Upstream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Http2Upstream")
            .field("id", &self.inner.id())
            .field("streaming", &self.streaming)
            .field("strategy", &self.strategy)
            .field("max_concurrent_streams", &self.max_concurrent_streams)
            .field("acceleration", &self.acceleration)
            .finish()
    }
}

impl Http2Upstream {
    /// Creates a new pre-compiled [`Http2Upstream`].
    pub fn new(
        inner: EdgeUpstream,
        streaming: StreamingMode,
        shard_count: usize,
        max_concurrent_streams: u32,
        acceleration: velda_http2::Http2AccelerationPath,
    ) -> Self {
        let strategy = Http2PipeStrategy::from_streaming(streaming);
        Self {
            inner,
            streaming,
            strategy,
            pool: MultiplexedPool::with_shards(shard_count),
            max_concurrent_streams,
            acceleration,
        }
    }

    /// Returns the upstream unique identifier.
    #[inline]
    pub fn id(&self) -> &str {
        self.inner.id()
    }

    /// Hands off downstream HTTP/2 pipe execution to a persistent multiplexed client connection.
    ///
    /// Hundreds of concurrent downstream streams share the same underlying TCP connection
    /// without repeated handshakes. Dead connections are automatically replaced.
    pub async fn dispatch_pipe<F, Fut, T, E>(
        &self,
        config: &velda_http2::Http2Config,
        pipe: F,
    ) -> Result<T, EdgeError>
    where
        F: FnOnce(h2::client::SendRequest<Bytes>) -> Fut,
        Fut: std::future::Future<Output = Result<T, E>>,
        E: std::fmt::Display,
    {
        let max_streams = self.max_concurrent_streams;
        let acceleration = self.acceleration;
        let connect_timeout = self.inner.timeouts().connect;

        let client = self
            .inner
            .execute(|endpoint| async move {
                if let Some(lease) = self.pool.acquire_stream(&endpoint) {
                    let ready_client = lease.client.clone();
                    match ready_client.ready().await {
                        Ok(ready_client) => return Ok::<_, String>(ready_client),
                        Err(_) => {
                            lease.mark_goaway();
                        }
                    }
                }

                let fresh = velda_http2::client::connect(
                    endpoint,
                    config,
                    Some(&acceleration),
                    Some(connect_timeout),
                )
                .await
                .map_err(|e| e.to_string())?;

                let ready_fresh = fresh
                    .ready()
                    .await
                    .map_err(|err| format!("Reconnected H2 client not ready: {err}"))?;

                let _ = self.pool.register(
                    endpoint,
                    Http2ClientResource::new(ready_fresh.clone()),
                    max_streams,
                );

                Ok::<_, String>(ready_fresh)
            })
            .await
            .map_err(EdgeError::Upstream)?;

        pipe(client).await.map_err(|e| {
            EdgeError::Upstream(velda_upstream::UpstreamError::Protocol(e.to_string()))
        })
    }
}
