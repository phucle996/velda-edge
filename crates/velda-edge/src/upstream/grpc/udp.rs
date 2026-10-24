//! Layer 7 gRPC over UDP Upstream managing persistent QUIC client multiplexing and RPC request forwarding.

use std::net::SocketAddr;
use std::sync::Arc;

use velda_connection_pool::{MultiplexedPool, PoolableResource};
use velda_core::StreamingMode;
use velda_grpc::udp::GrpcUdpPipeStrategy;
use velda_grpc::udp::client::GrpcUdpClient;

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
    pool: Arc<MultiplexedPool<SocketAddr, GrpcUdpClientResource>>,
    /// Maximum concurrent streams per multiplexed connection.
    pub max_concurrent_streams: u32,
    /// [PRE-COMPILED]: Pre-compiled QUIC client TLS configuration compiled directly
    /// from upstream `tls` config (custom CA roots, client mTLS cert, verify settings).
    pub quic_config: Arc<velda_grpc::udp::quinn_proto::ClientConfig>,
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
        quic_config: Arc<velda_grpc::udp::quinn_proto::ClientConfig>,
    ) -> Self {
        let strategy = GrpcUdpPipeStrategy::from_streaming(streaming);
        Self {
            inner,
            target_sni,
            streaming,
            strategy,
            pool: Arc::new(MultiplexedPool::with_shards(shard_count)),
            max_concurrent_streams,
            quic_config,
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
        let idle_timeout = self.inner.timeouts().idle;
        let quic_tls = Arc::clone(&self.quic_config);

        let (fresh, _endpoint) = self
            .inner
            .execute(|endpoint| {
                let quic_tls = Arc::clone(&quic_tls);
                async move {
                    if let Some(lease) = self.pool.acquire_stream(&endpoint, idle_timeout) {
                        if !lease.client.is_closed() {
                            return Ok::<_, String>((lease.client.clone(), endpoint));
                        }
                        lease.mark_goaway();
                    }

                    let fresh = velda_grpc::udp::connect_udp(
                        endpoint,
                        server_name,
                        config,
                        (*quic_tls).clone(),
                    )
                    .await
                    .map_err(|e| e.to_string())?;

                    let _ = self.pool.register(
                        endpoint,
                        GrpcUdpClientResource::new(fresh.clone()),
                        max_streams,
                    );

                    Ok::<_, String>((fresh, endpoint))
                }
            })
            .await
            .map_err(EdgeError::Upstream)?;

        Ok(fresh)
    }

    /// Connects a brand new gRPC over UDP client directly, bypassing existing pool leases.
    ///
    /// Used for transparent 1-shot self-healing when an existing QUIC session drops or closes.
    pub async fn acquire_fresh(
        &self,
        config: &velda_grpc::GrpcConfig,
    ) -> Result<GrpcUdpClient, EdgeError> {
        let max_streams = self.max_concurrent_streams;
        let server_name: &str = &self.target_sni;
        let quic_tls = Arc::clone(&self.quic_config);

        let (fresh, _endpoint) = self
            .inner
            .execute(|endpoint| {
                let quic_tls = Arc::clone(&quic_tls);
                async move {
                    let fresh = velda_grpc::udp::connect_udp(
                        endpoint,
                        server_name,
                        config,
                        (*quic_tls).clone(),
                    )
                    .await
                    .map_err(|e| e.to_string())?;

                    let _ = self.pool.register(
                        endpoint,
                        GrpcUdpClientResource::new(fresh.clone()),
                        max_streams,
                    );

                    Ok::<_, String>((fresh, endpoint))
                }
            })
            .await
            .map_err(EdgeError::Upstream)?;

        Ok(fresh)
    }

    /// Pre-warms gRPC UDP client connections for the specified worker lane.
    ///
    /// Executed autonomously by the core-local Shadow Worker (`SCHED_IDLE`) whenever
    /// lane capacity drops below available headroom.
    pub async fn prewarm_lane(&self, lane: usize, config: &velda_grpc::GrpcConfig) {
        let endpoints = self.inner.current_endpoints();
        let all_eps = endpoints.all_endpoints();
        if all_eps.is_empty() {
            return;
        }

        for ep in all_eps {
            let addr = ep.address;
            if !self.inner.health().is_healthy(&addr) {
                continue;
            }

            if !self
                .pool
                .has_headroom_for_lane(&addr, lane, self.max_concurrent_streams)
            {
                let sni = Arc::clone(&self.target_sni);
                let quic_tls = Arc::clone(&self.quic_config);

                match velda_grpc::udp::connect_udp(addr, &sni, config, (*quic_tls).clone()).await {
                    Ok(fresh) => {
                        let res = GrpcUdpClientResource::new(fresh);
                        self.pool.register_warmed_to_lane(
                            addr,
                            lane,
                            res,
                            self.max_concurrent_streams,
                        );
                        tracing::debug!(endpoint = %addr, lane, "Shadow Worker pre-warmed gRPC UDP connection");
                    }
                    Err(e) => {
                        tracing::debug!(endpoint = %addr, lane, error = %e, "Shadow Worker pre-warm connect failed for gRPC UDP");
                    }
                }
            }
        }
    }
}
