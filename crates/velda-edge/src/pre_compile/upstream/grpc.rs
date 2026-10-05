//! Layer 7 gRPC Upstream managing streaming and unary proxying handoff.

use std::net::SocketAddr;

use velda_connection_pool::{MultiplexedPool, PoolableResource};
use velda_core::{L7Request, L7Response, StreamingMode};
use velda_grpc::client::GrpcUpstreamConnector;
use velda_grpc::pipe::{GrpcPipeStrategy, pipe_grpc_stream};
use velda_grpc::server::GrpcServerStream;

use super::lb::EdgeUpstream;
use crate::error::EdgeError;

/// Pooled gRPC client resource wrapping [`GrpcUpstreamConnector`].
#[derive(Clone)]
pub struct GrpcClientResource {
    pub connector: GrpcUpstreamConnector,
}

impl GrpcClientResource {
    pub fn new(connector: GrpcUpstreamConnector) -> Self {
        Self { connector }
    }
}

impl std::fmt::Debug for GrpcClientResource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GrpcClientResource").finish()
    }
}

impl PoolableResource for GrpcClientResource {
    fn is_healthy(&self) -> bool {
        true
    }

    fn close(&mut self) {}
}

/// Layer 7 gRPC Upstream managing streaming and unary proxying handoff.
///
/// Pre-compiled with static load balancer, streaming strategy, and sharded multiplexed client pool.
pub struct GrpcUpstream {
    /// [PRE-COMPILED]: Pre-assembled upstream core holding discovery, health tracker,
    /// timeouts, and the selected `LbAlgorithm` enum variant.
    inner: EdgeUpstream,
    /// Declarative streaming mode from upstream configuration.
    pub streaming: StreamingMode,
    /// [PRE-COMPILED]: Pre-computed wire forwarding strategy (`GrpcPipeStrategy`) derived from
    /// `streaming` at compile time; eliminates runtime enum branching.
    pub strategy: GrpcPipeStrategy,
    /// Lock-sharded persistent multiplexed gRPC client connection pool for Unary RPC reuse.
    pool: MultiplexedPool<SocketAddr, GrpcClientResource>,
    /// Maximum concurrent streams per multiplexed connection.
    pub max_concurrent_streams: u32,
    /// [PRE-COMPILED]: Protocol-specific socket acceleration path.
    pub acceleration: velda_grpc::GrpcAccelerationPath,
}

impl std::fmt::Debug for GrpcUpstream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GrpcUpstream")
            .field("id", &self.inner.id())
            .field("streaming", &self.streaming)
            .field("strategy", &self.strategy)
            .field("max_concurrent_streams", &self.max_concurrent_streams)
            .field("acceleration", &self.acceleration)
            .finish()
    }
}

impl GrpcUpstream {
    /// Creates a new [`GrpcUpstream`] instance.
    pub fn new(
        inner: EdgeUpstream,
        streaming: StreamingMode,
        shard_count: usize,
        max_concurrent_streams: u32,
        acceleration: velda_grpc::GrpcAccelerationPath,
    ) -> Self {
        let strategy = GrpcPipeStrategy::from_streaming(streaming);
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

    /// Hands off downstream bidirectional gRPC server stream to upstream backend.
    pub async fn dispatch_stream(
        &self,
        server_stream: GrpcServerStream,
        config: &velda_grpc::GrpcConfig,
    ) -> Result<(), EdgeError> {
        let strategy = self.strategy;
        let endpoint = self.inner.select_endpoint().map_err(EdgeError::Upstream)?;

        pipe_grpc_stream(server_stream, endpoint, strategy, config)
            .await
            .map_err(|e| {
                EdgeError::Upstream(velda_upstream::UpstreamError::Protocol(e.to_string()))
            })
    }

    /// Hands off downstream unary gRPC request to upstream backend.
    /// Reuses established multiplexed HTTP/2 client connections to eliminate TCP/TLS handshake storm.
    pub async fn dispatch_unary(
        &self,
        req: &L7Request,
        config: &velda_grpc::GrpcConfig,
    ) -> Result<L7Response, EdgeError> {
        let max_streams = self.max_concurrent_streams;
        let acceleration = self.acceleration;
        let connect_timeout = self.inner.timeouts().connect;

        let mut connector = self
            .inner
            .execute(|endpoint| async move {
                if let Some(lease) = self.pool.acquire_stream(&endpoint) {
                    let mut c = lease.connector.clone();
                    if c.ready().await.is_ok() {
                        return Ok::<_, String>(c);
                    }
                    lease.mark_goaway();
                }

                let mut fresh = GrpcUpstreamConnector::connect(
                    endpoint,
                    config,
                    Some(&acceleration),
                    Some(connect_timeout),
                )
                .await
                .map_err(|e| e.to_string())?;

                fresh.ready().await.map_err(|e| e.to_string())?;

                let _ = self.pool.register(
                    endpoint,
                    GrpcClientResource::new(fresh.clone()),
                    max_streams,
                );

                Ok::<_, String>(fresh)
            })
            .await
            .map_err(EdgeError::Upstream)?;

        connector.invoke_unary(req, config).await.map_err(|e| {
            EdgeError::Upstream(velda_upstream::UpstreamError::Protocol(e.to_string()))
        })
    }
}
