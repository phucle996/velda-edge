//! Layer 7 HTTP/3 Upstream managing persistent QUIC client multiplexing and request forwarding (RFC 9114).

use std::net::SocketAddr;
use std::sync::Arc;

use velda_connection_pool::{MultiplexedPool, PoolableResource};
use velda_core::StreamingMode;
use velda_http3::Http3Config;
use velda_http3::client::Http3Client;
use velda_http3::pipe::Http3PipeStrategy;

use crate::error::EdgeError;
use crate::upstream::lb::EdgeUpstream;

/// Pooled HTTP/3 client resource wrapping [`Http3Client`].
#[derive(Clone)]
pub struct Http3ClientResource {
    pub client: Http3Client,
}

impl PoolableResource for Http3ClientResource {
    fn is_healthy(&self) -> bool {
        !self.client.is_closed()
    }

    fn close(&mut self) {}
}

impl std::fmt::Debug for Http3ClientResource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Http3ClientResource")
            .field("target", &self.client.target())
            .field("is_closed", &self.client.is_closed())
            .finish()
    }
}

/// Layer 7 HTTP/3 Upstream managing persistent QUIC client multiplexing and pipe forwarding.
///
/// Pre-compiled with static load balancer, streaming strategy, target SNI, and sharded multiplexed client pool.
pub struct Http3Upstream {
    /// [PRE-COMPILED]: Pre-assembled upstream core holding discovery, health tracker,
    /// timeouts, and the selected `LbAlgorithm` enum variant.
    inner: EdgeUpstream,
    /// [PRE-COMPILED]: Target TLS SNI for upstream QUIC connection handshake.
    pub target_sni: Arc<str>,
    /// Declarative streaming mode from upstream configuration.
    pub streaming: StreamingMode,
    /// [PRE-COMPILED]: Pre-computed wire forwarding strategy (`Http3PipeStrategy`) derived from
    /// `streaming` at compile time; eliminates runtime enum branching.
    pub strategy: Http3PipeStrategy,
    /// Lock-sharded persistent HTTP/3 QUIC client connection pool.
    pool: MultiplexedPool<SocketAddr, Http3ClientResource>,
    /// Maximum concurrent streams per multiplexed connection.
    pub max_concurrent_streams: u32,
}

impl std::fmt::Debug for Http3Upstream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Http3Upstream")
            .field("id", &self.inner.id())
            .field("target_sni", &self.target_sni)
            .field("streaming", &self.streaming)
            .field("strategy", &self.strategy)
            .field("max_concurrent_streams", &self.max_concurrent_streams)
            .finish()
    }
}

impl Http3Upstream {
    /// Creates a new pre-compiled [`Http3Upstream`].
    pub fn new(
        inner: EdgeUpstream,
        target_sni: Arc<str>,
        streaming: StreamingMode,
        shard_count: usize,
        max_concurrent_streams: u32,
    ) -> Self {
        let strategy = Http3PipeStrategy::from_streaming(streaming);
        Self {
            inner,
            target_sni,
            streaming,
            strategy,
            pool: MultiplexedPool::with_shards(shard_count),
            max_concurrent_streams,
        }
    }

    /// Returns the upstream unique identifier.
    #[inline]
    pub fn id(&self) -> &str {
        self.inner.id()
    }

    /// Acquires an active multiplexed HTTP/3 client from the pool or establishes a new QUIC connection.
    ///
    /// Uses pre-compiled upstream `target_sni` for the TLS handshake inside the connector.
    pub async fn acquire(&self, config: &Http3Config) -> Result<Http3Client, EdgeError> {
        let max_streams = self.max_concurrent_streams;
        let sni = Arc::clone(&self.target_sni);
        let idle_timeout = self.inner.timeouts().idle;

        let client = self
            .inner
            .execute(|endpoint| {
                let sni = Arc::clone(&sni);
                async move {
                    if let Some(lease) = self.pool.acquire_stream(&endpoint, idle_timeout) {
                        return Ok::<_, String>(lease.client.clone());
                    }

                    let client = velda_http3::client::connect(endpoint, &sni, config)
                        .await
                        .map_err(|e| e.to_string())?;

                    let res = Http3ClientResource {
                        client: client.clone(),
                    };
                    let _lease = self.pool.register(endpoint, res, max_streams);

                    Ok::<_, String>(client)
                }
            })
            .await
            .map_err(EdgeError::Upstream)?;

        Ok(client)
    }

    /// Forcefully establishes a fresh HTTP/3 client connection directly, bypassing existing connections in the pool.
    pub async fn acquire_fresh(&self, config: &Http3Config) -> Result<Http3Client, EdgeError> {
        let max_streams = self.max_concurrent_streams;
        let sni = Arc::clone(&self.target_sni);

        let client = self
            .inner
            .execute(|endpoint| {
                let sni = Arc::clone(&sni);
                async move {
                    let client = velda_http3::client::connect(endpoint, &sni, config)
                        .await
                        .map_err(|e| e.to_string())?;

                    let res = Http3ClientResource {
                        client: client.clone(),
                    };
                    let _lease = self.pool.register(endpoint, res, max_streams);

                    Ok::<_, String>(client)
                }
            })
            .await
            .map_err(EdgeError::Upstream)?;

        Ok(client)
    }
}
