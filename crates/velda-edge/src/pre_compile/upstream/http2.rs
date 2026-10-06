//! Layer 7 HTTP/2 Upstream managing persistent client multiplexing (RFC 9113) and pipe forwarding.

use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use velda_connection_pool::{MultiplexedPool, PoolableResource};
use velda_core::StreamingMode;
use velda_http2::pipe::Http2PipeStrategy;
use velda_tls::TlsClientEngine;

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

/// Layer 7 HTTP/2 Upstream managing persistent client multiplexing (RFC 9113) and pipe forwarding.
///
/// Pre-compiled with static load balancer, wire pipe strategy, and sharded multiplexed client pool.
pub struct Http2Upstream {
    /// [PRE-COMPILED]: Pre-assembled upstream core holding discovery, health tracker,
    /// timeouts, and the selected `LbAlgorithm` enum variant.
    inner: EdgeUpstream,
    /// [PRE-COMPILED]: Upstream TLS identity: client engine plus the static target SNI.
    /// `None` for cleartext (h2c) upstreams. TLS without SNI is rejected at compile time.
    tls: Option<(Arc<TlsClientEngine>, Arc<str>)>,
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
        tls: Option<(Arc<TlsClientEngine>, Arc<str>)>,
        streaming: StreamingMode,
        shard_count: usize,
        max_concurrent_streams: u32,
        acceleration: velda_http2::Http2AccelerationPath,
    ) -> Self {
        let strategy = Http2PipeStrategy::from_streaming(streaming);
        Self {
            inner,
            tls,
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

    /// Acquires an active, ready multiplexed HTTP/2 client connection from the pool or connects a fresh one.
    ///
    /// Hundreds of concurrent downstream streams share the same underlying TCP connection
    /// without repeated handshakes. Dead connections are automatically replaced.
    pub async fn acquire(
        &self,
        config: &velda_http2::Http2Config,
    ) -> Result<h2::client::SendRequest<Bytes>, EdgeError> {
        let max_streams = self.max_concurrent_streams;
        let acceleration = self.acceleration;
        let connect_timeout = self.inner.timeouts().connect;
        let idle_timeout = self.inner.timeouts().idle;
        let tls = self
            .tls
            .as_ref()
            .map(|(engine, sni)| (engine.as_ref(), sni.as_ref()));

        self.inner
            .execute(|endpoint| async move {
                if let Some(lease) = self.pool.acquire_stream(&endpoint, idle_timeout) {
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
                    tls,
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
            .map_err(EdgeError::Upstream)
    }

    /// Connects a brand new HTTP/2 client connection directly, bypassing existing pool leases.
    ///
    /// Used for transparent 1-shot self-healing when an existing multiplexed connection encounters
    /// GOAWAY, REFUSED_STREAM, or silent connection reset.
    pub async fn acquire_fresh(
        &self,
        config: &velda_http2::Http2Config,
    ) -> Result<h2::client::SendRequest<Bytes>, EdgeError> {
        let max_streams = self.max_concurrent_streams;
        let acceleration = self.acceleration;
        let connect_timeout = self.inner.timeouts().connect;
        let tls = self
            .tls
            .as_ref()
            .map(|(engine, sni)| (engine.as_ref(), sni.as_ref()));

        self.inner
            .execute(|endpoint| async move {
                let fresh = velda_http2::client::connect(
                    endpoint,
                    tls,
                    config,
                    Some(&acceleration),
                    Some(connect_timeout),
                )
                .await
                .map_err(|e| e.to_string())?;

                let ready_fresh = fresh
                    .ready()
                    .await
                    .map_err(|err| format!("Fresh H2 client not ready: {err}"))?;

                let _ = self.pool.register(
                    endpoint,
                    Http2ClientResource::new(ready_fresh.clone()),
                    max_streams,
                );

                Ok::<_, String>(ready_fresh)
            })
            .await
            .map_err(EdgeError::Upstream)
    }
}
