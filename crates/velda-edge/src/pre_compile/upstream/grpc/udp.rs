//! Layer 7 gRPC over UDP Upstream managing persistent QUIC client multiplexing and RPC request forwarding.

use std::net::SocketAddr;
use std::sync::Arc;

use velda_connection_pool::{MultiplexedPool, PoolableResource};
use velda_core::StreamingMode;
use velda_grpc::udp::client::GrpcUdpClient;
use velda_grpc::udp::pipe::GrpcUdpPipeStrategy;

use super::super::lb::EdgeUpstream;
use crate::error::EdgeError;

/// Pooled gRPC UDP client resource wrapping [`GrpcUdpClient`].
#[derive(Clone)]
pub struct GrpcUdpClientResource {
    pub client: GrpcUdpClient,
}

impl GrpcUdpClientResource {
    pub fn new(client: GrpcUdpClient) -> Self {
        Self { client }
    }
}

impl std::fmt::Debug for GrpcUdpClientResource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GrpcUdpClientResource").finish()
    }
}

impl PoolableResource for GrpcUdpClientResource {
    fn is_healthy(&self) -> bool {
        !self.client.is_closed()
    }

    fn close(&mut self) {}
}

/// Layer 7 gRPC over UDP Upstream managing persistent QUIC client multiplexing and RPC request forwarding.
///
/// Pre-compiled with static load balancer, streaming strategy, target SNI, and sharded multiplexed client pool.
pub struct GrpcUdpUpstream {
    /// [PRE-COMPILED]: Pre-assembled upstream core holding discovery, health tracker,
    /// timeouts, and the selected `LbAlgorithm` enum variant.
    inner: EdgeUpstream,
    /// [PRE-COMPILED]: Target TLS SNI for upstream QUIC connection handshake.
    pub target_sni: Arc<str>,
    /// Declarative streaming mode from upstream configuration.
    pub streaming: StreamingMode,
    /// [PRE-COMPILED]: Pre-computed wire forwarding strategy (`GrpcUdpPipeStrategy`) derived from
    /// `streaming` at compile time; eliminates runtime enum branching.
    pub strategy: GrpcUdpPipeStrategy,
    /// Lock-sharded persistent gRPC QUIC client connection pool.
    pool: MultiplexedPool<SocketAddr, GrpcUdpClientResource>,
    /// Maximum concurrent streams per multiplexed connection.
    pub max_concurrent_streams: u32,
}

impl std::fmt::Debug for GrpcUdpUpstream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GrpcUdpUpstream")
            .field("id", &self.inner.id())
            .field("target_sni", &self.target_sni)
            .field("streaming", &self.streaming)
            .field("strategy", &self.strategy)
            .field("max_concurrent_streams", &self.max_concurrent_streams)
            .finish()
    }
}

impl GrpcUdpUpstream {
    /// Creates a new pre-compiled [`GrpcUdpUpstream`].
    pub fn new(
        inner: EdgeUpstream,
        target_sni: Arc<str>,
        streaming: StreamingMode,
        shard_count: usize,
        max_concurrent_streams: u32,
    ) -> Self {
        let strategy = GrpcUdpPipeStrategy::from_streaming(streaming);
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

    /// Acquires an active multiplexed gRPC UDP client from the pool or establishes a new connection.
    ///
    /// Uses pre-compiled upstream `target_sni` for the TLS handshake inside the connector.
    pub async fn acquire(
        &self,
        config: &velda_grpc::GrpcConfig,
    ) -> Result<GrpcUdpClient, EdgeError> {
        let max_streams = self.max_concurrent_streams;
        let server_name: &str = &self.target_sni;

        self.inner
            .execute(|endpoint| async move {
                if let Some(lease) = self.pool.acquire_stream(&endpoint) {
                    if !lease.client.is_closed() {
                        return Ok::<_, String>(lease.client.clone());
                    }
                    lease.mark_goaway();
                }

                let fresh = velda_grpc::udp::connect_udp(endpoint, server_name, config)
                    .await
                    .map_err(|e| e.to_string())?;

                let _ = self.pool.register(
                    endpoint,
                    GrpcUdpClientResource::new(fresh.clone()),
                    max_streams,
                );

                Ok::<_, String>(fresh)
            })
            .await
            .map_err(EdgeError::Upstream)
    }
}
