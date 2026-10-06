//! Layer 7 gRPC over TCP Upstream managing connection lifecycle, multiplexing, and endpoint health tracking.

use std::net::SocketAddr;
use std::sync::Arc;

use velda_connection_pool::{MultiplexedPool, PoolableResource};
use velda_core::StreamingMode;
use velda_grpc::GrpcConfig;
use velda_grpc::tcp::client::{GrpcAccelerationPath, GrpcUpstreamConnector};
use velda_grpc::tcp::pipe::GrpcPipeStrategy;
use velda_tls::TlsClientEngine;

use super::super::lb::EdgeUpstream;
use crate::error::EdgeError;

/// Pooled gRPC over TCP client resource wrapping [`GrpcUpstreamConnector`].
#[derive(Clone)]
pub struct GrpcTcpClientResource {
    pub client: GrpcUpstreamConnector,
}

impl PoolableResource for GrpcTcpClientResource {
    fn is_healthy(&self) -> bool {
        true
    }

    fn close(&mut self) {}
}

impl std::fmt::Debug for GrpcTcpClientResource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GrpcTcpClientResource").finish()
    }
}

/// Layer 7 gRPC over TCP Upstream managing persistent HTTP/2 multiplexing and endpoint health tracking.
///
/// Pre-compiled with static load balancer, streaming strategy, socket acceleration, and sharded multiplexed client pool.
pub struct GrpcTcpUpstream {
    /// [PRE-COMPILED]: Pre-assembled upstream core holding discovery, health tracker,
    /// timeouts, and the selected `LbAlgorithm` enum variant.
    inner: EdgeUpstream,
    /// [PRE-COMPILED]: Upstream TLS identity: client engine plus the static target SNI.
    /// `None` for cleartext upstreams. TLS without SNI is rejected at compile time.
    tls: Option<(Arc<TlsClientEngine>, Arc<str>)>,
    /// Declarative streaming mode from upstream configuration.
    pub streaming: StreamingMode,
    /// [PRE-COMPILED]: Pre-computed wire forwarding strategy (`GrpcPipeStrategy`) derived from
    /// `streaming` at compile time; eliminates runtime enum branching.
    pub strategy: GrpcPipeStrategy,
    /// Lock-sharded persistent gRPC HTTP/2 multiplexed client connection pool.
    pool: MultiplexedPool<SocketAddr, GrpcTcpClientResource>,
    /// Maximum concurrent streams per multiplexed connection.
    pub max_concurrent_streams: u32,
    /// [PRE-COMPILED]: Protocol-specific socket acceleration path.
    pub acceleration: GrpcAccelerationPath,
}

impl std::fmt::Debug for GrpcTcpUpstream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GrpcTcpUpstream")
            .field("id", &self.inner.id())
            .field("streaming", &self.streaming)
            .field("strategy", &self.strategy)
            .field("max_concurrent_streams", &self.max_concurrent_streams)
            .field("acceleration", &self.acceleration)
            .finish()
    }
}

impl GrpcTcpUpstream {
    /// Creates a new [`GrpcTcpUpstream`] instance.
    pub fn new(
        inner: EdgeUpstream,
        tls: Option<(Arc<TlsClientEngine>, Arc<str>)>,
        streaming: StreamingMode,
        shard_count: usize,
        max_concurrent_streams: u32,
        acceleration: GrpcAccelerationPath,
    ) -> Self {
        let strategy = GrpcPipeStrategy::from_streaming(streaming);
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

    /// Acquires an active, ready multiplexed gRPC over TCP client connection from the pool or connects a fresh one.
    pub async fn acquire(&self, config: &GrpcConfig) -> Result<GrpcUpstreamConnector, EdgeError> {
        let max_streams = self.max_concurrent_streams;
        let acceleration = self.acceleration;
        let connect_timeout = self.inner.timeouts().connect;
        let tls = self
            .tls
            .as_ref()
            .map(|(engine, sni)| (engine.as_ref(), sni.as_ref()));

        self.inner
            .execute(|endpoint| async move {
                if let Some(lease) = self.pool.acquire_stream(&endpoint) {
                    let mut connector = lease.client.clone();
                    match connector.ready().await {
                        Ok(()) => return Ok::<_, String>(connector),
                        Err(_) => {
                            lease.mark_goaway();
                        }
                    }
                }

                let mut fresh = GrpcUpstreamConnector::connect(
                    endpoint,
                    tls,
                    config,
                    Some(&acceleration),
                    Some(connect_timeout),
                )
                .await
                .map_err(|e| e.to_string())?;

                fresh
                    .ready()
                    .await
                    .map_err(|err| format!("Reconnected gRPC TCP client not ready: {err}"))?;

                let _ = self.pool.register(
                    endpoint,
                    GrpcTcpClientResource {
                        client: fresh.clone(),
                    },
                    max_streams,
                );

                Ok::<_, String>(fresh)
            })
            .await
            .map_err(EdgeError::Upstream)
    }

    /// Connects a brand new gRPC over TCP client connector directly, bypassing pool leases.
    ///
    /// Used for transparent 1-shot self-healing when an existing multiplexed connection encounters
    /// GOAWAY, REFUSED_STREAM, or silent connection drop.
    pub async fn acquire_fresh(
        &self,
        config: &GrpcConfig,
    ) -> Result<GrpcUpstreamConnector, EdgeError> {
        let max_streams = self.max_concurrent_streams;
        let acceleration = self.acceleration;
        let connect_timeout = self.inner.timeouts().connect;
        let tls = self
            .tls
            .as_ref()
            .map(|(engine, sni)| (engine.as_ref(), sni.as_ref()));

        self.inner
            .execute(|endpoint| async move {
                let mut fresh = GrpcUpstreamConnector::connect(
                    endpoint,
                    tls,
                    config,
                    Some(&acceleration),
                    Some(connect_timeout),
                )
                .await
                .map_err(|e| e.to_string())?;

                fresh
                    .ready()
                    .await
                    .map_err(|err| format!("Fresh gRPC TCP client not ready: {err}"))?;

                let _ = self.pool.register(
                    endpoint,
                    GrpcTcpClientResource {
                        client: fresh.clone(),
                    },
                    max_streams,
                );

                Ok::<_, String>(fresh)
            })
            .await
            .map_err(EdgeError::Upstream)
    }
}
