//! Layer 7 gRPC over RDMA Upstream managing connection multiplexing and hardware pooling.

use std::net::SocketAddr;
use std::sync::Arc;

use velda_connection_pool::MultiplexedPool;
use velda_core::StreamingMode;
use velda_grpc::GrpcConfig;
use velda_grpc::tcp::GrpcPipeStrategy;
use velda_grpc::tcp::client::GrpcUpstreamConnector;
use velda_rdma::{RdmaConfig, RdmaConnector};

use super::super::lb::EdgeUpstream;
use super::tcp::GrpcTcpClientResource;
use crate::error::EdgeError;

/// Layer 7 gRPC over RDMA Upstream managing multiplexed connections over kernel-bypass RDMA channels.
pub struct GrpcRdmaUpstream {
    /// Pre-assembled upstream core holding discovery, health tracking, timeouts, and LB algorithm.
    inner: EdgeUpstream,
    /// RDMA hardware transport configuration.
    pub rdma_config: RdmaConfig,
    /// Declarative streaming mode from upstream configuration.
    pub streaming: StreamingMode,
    /// Pre-computed wire forwarding strategy.
    pub strategy: GrpcPipeStrategy,
    /// Persistent multiplexed client connection pool.
    pool: Arc<MultiplexedPool<SocketAddr, GrpcTcpClientResource>>,
    /// Maximum concurrent streams per multiplexed connection.
    pub max_concurrent_streams: u32,
}

impl std::fmt::Debug for GrpcRdmaUpstream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GrpcRdmaUpstream")
            .field("id", &self.inner.id())
            .field("streaming", &self.streaming)
            .field("strategy", &self.strategy)
            .field("max_concurrent_streams", &self.max_concurrent_streams)
            .finish()
    }
}

impl GrpcRdmaUpstream {
    /// Creates a new [`GrpcRdmaUpstream`] instance.
    pub fn new(
        inner: EdgeUpstream,
        rdma_config: RdmaConfig,
        streaming: StreamingMode,
        shard_count: usize,
        max_concurrent_streams: u32,
    ) -> Self {
        let strategy = GrpcPipeStrategy::from_streaming(streaming);
        Self {
            inner,
            rdma_config,
            streaming,
            strategy,
            pool: Arc::new(MultiplexedPool::with_shards(shard_count)),
            max_concurrent_streams,
        }
    }

    /// Returns the upstream unique identifier.
    #[inline]
    pub fn id(&self) -> &str {
        self.inner.id()
    }

    /// Returns the number of multiplexed connections currently cached in a specific core lane.
    #[inline(always)]
    pub fn connection_count_for_lane(&self, endpoint: &SocketAddr, lane: usize) -> usize {
        self.pool.connection_count_for_lane(endpoint, lane)
    }

    /// Acquires an active, ready multiplexed gRPC client connector over an RDMA stream.
    pub async fn acquire(&self, config: &GrpcConfig) -> Result<GrpcUpstreamConnector, EdgeError> {
        let max_streams = self.max_concurrent_streams;
        let connect_timeout = self.inner.timeouts().connect;
        let idle_timeout = self.inner.timeouts().idle;
        let rdma_config = self.rdma_config.clone();
        let pool = Arc::clone(&self.pool);
        let grpc_config = *config;

        let (connector, _endpoint) =
            self.inner
                .execute(move |endpoint| {
                    let pool = Arc::clone(&pool);
                    let rdma_config = rdma_config.clone();
                    async move {
                        if let Some(lease) = pool.acquire_stream(&endpoint, idle_timeout) {
                            let mut connector = lease.client.clone();
                            match connector.ready().await {
                                Ok(()) => return Ok::<_, String>((connector, endpoint)),
                                Err(_) => {
                                    lease.mark_goaway();
                                }
                            }
                        }

                        // Connect RDMA kernel-bypass stream
                        let stream = RdmaConnector::connect_stream(endpoint, &rdma_config)
                            .await
                            .map_err(|e| e.to_string())?;

                        // Run gRPC HTTP/2 framing handshake over RDMA stream
                        let mut fresh = GrpcUpstreamConnector::handshake_stream(
                            stream,
                            &grpc_config,
                            Some(connect_timeout),
                        )
                        .await
                        .map_err(|e| e.to_string())?;

                        fresh.ready().await.map_err(|err| {
                            format!("Reconnected gRPC RDMA client not ready: {err}")
                        })?;

                        let _ = pool.register(
                            endpoint,
                            GrpcTcpClientResource {
                                client: fresh.clone(),
                            },
                            max_streams,
                        );

                        Ok::<_, String>((fresh, endpoint))
                    }
                })
                .await
                .map_err(EdgeError::Upstream)?;

        Ok(connector)
    }

    /// Connects a brand new gRPC client connector directly over RDMA, bypassing pool leases.
    pub async fn acquire_fresh(
        &self,
        config: &GrpcConfig,
    ) -> Result<GrpcUpstreamConnector, EdgeError> {
        let connect_timeout = self.inner.timeouts().connect;
        let rdma_config = self.rdma_config.clone();
        let grpc_config = *config;

        let (connector, _) = self
            .inner
            .execute(move |endpoint| {
                let rdma_config = rdma_config.clone();
                async move {
                    let stream = RdmaConnector::connect_stream(endpoint, &rdma_config)
                        .await
                        .map_err(|e| e.to_string())?;

                    let mut fresh = GrpcUpstreamConnector::handshake_stream(
                        stream,
                        &grpc_config,
                        Some(connect_timeout),
                    )
                    .await
                    .map_err(|e| e.to_string())?;

                    fresh
                        .ready()
                        .await
                        .map_err(|err| format!("Fresh gRPC RDMA client not ready: {err}"))?;

                    Ok::<_, String>((fresh, endpoint))
                }
            })
            .await
            .map_err(EdgeError::Upstream)?;

        Ok(connector)
    }

    /// Autonomous background pre-warming for a specific worker core lane (`lane = worker_id`).
    pub async fn prewarm_lane(&self, lane: usize, config: &GrpcConfig) {
        if let Ok(endpoint) = self.inner.select_endpoint() {
            let rdma_config = self.rdma_config.clone();
            let connect_timeout = self.inner.timeouts().connect;
            let max_streams = self.max_concurrent_streams;

            if let Ok(stream) = RdmaConnector::connect_stream(endpoint, &rdma_config).await
                && let Ok(mut connector) =
                    GrpcUpstreamConnector::handshake_stream(stream, config, Some(connect_timeout))
                        .await
                && connector.ready().await.is_ok()
            {
                self.pool.register_warmed_to_lane(
                    endpoint,
                    lane,
                    GrpcTcpClientResource { client: connector },
                    max_streams,
                );
            }
        }
    }
}
