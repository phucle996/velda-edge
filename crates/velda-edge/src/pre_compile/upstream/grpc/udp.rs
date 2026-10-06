//! Layer 7 gRPC over UDP Upstream managing persistent QUIC client multiplexing and RPC request handoff.

use std::net::SocketAddr;

use velda_connection_pool::{MultiplexedPool, PoolableResource};
use velda_core::{L7Request, L7Response, StreamingMode};
use velda_grpc::udp::client::GrpcUdpClient;
use velda_grpc::udp::pipe::{GrpcUdpPipeStrategy, pipe_grpc_udp_stream};

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

/// Layer 7 gRPC over UDP Upstream managing persistent QUIC client multiplexing and RPC request handoff.
///
/// Pre-compiled with static load balancer, streaming strategy, target SNI, and sharded multiplexed client pool.
pub struct GrpcUdpUpstream {
    /// [PRE-COMPILED]: Pre-assembled upstream core holding discovery, health tracker,
    /// timeouts, and the selected `LbAlgorithm` enum variant.
    inner: EdgeUpstream,
    /// [PRE-COMPILED]: Target SNI hostname validated and pre-resolved from configuration.
    target_sni: Option<String>,
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
        target_sni: Option<String>,
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

    /// Returns the configured target SNI name, if any.
    #[inline]
    pub fn target_sni(&self) -> Option<&str> {
        self.target_sni.as_deref()
    }

    /// Resolves SNI domain for gRPC QUIC connection.
    pub fn resolve_sni(&self, req: &L7Request, authority: Option<&str>) -> String {
        if let Some(ref sni) = self.target_sni {
            sni.clone()
        } else if let Some(a) = authority {
            a.split(':').next().unwrap_or(a).to_string()
        } else if let Some(h) = req.headers.get(":authority").and_then(|v| v.to_str().ok()) {
            h.split(':').next().unwrap_or(h).to_string()
        } else {
            "localhost".to_string()
        }
    }

    /// Hands off downstream gRPC over UDP request to a persistent multiplexed QUIC client.
    ///
    /// Requests stream concurrently over UDP datagrams without Head-of-Line blocking.
    pub async fn dispatch_request(
        &self,
        req: &L7Request,
        config: &velda_grpc::GrpcConfig,
    ) -> Result<L7Response, EdgeError> {
        let authority = req.headers.get(":authority").and_then(|v| v.to_str().ok());
        let server_name = self.resolve_sni(req, authority);
        let max_streams = self.max_concurrent_streams;
        let strategy = self.strategy;

        let client = self
            .inner
            .execute(|endpoint| {
                let server_name = server_name.clone();
                async move {
                    if let Some(lease) = self.pool.acquire_stream(&endpoint) {
                        if !lease.client.is_closed() {
                            return Ok::<_, String>(lease.client.clone());
                        }
                        lease.mark_goaway();
                    }

                    let fresh = velda_grpc::udp::connect_udp(endpoint, &server_name, config)
                        .await
                        .map_err(|e| e.to_string())?;

                    let _ = self.pool.register(
                        endpoint,
                        GrpcUdpClientResource::new(fresh.clone()),
                        max_streams,
                    );

                    Ok::<_, String>(fresh)
                }
            })
            .await
            .map_err(EdgeError::Upstream)?;

        pipe_grpc_udp_stream(&client, req, strategy, config)
            .await
            .map_err(|e| {
                EdgeError::Upstream(velda_upstream::UpstreamError::Protocol(e.to_string()))
            })
    }
}
