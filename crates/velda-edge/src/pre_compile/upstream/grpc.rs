//! Layer 7 gRPC Upstream managing streaming and unary proxying handoff.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use tokio::sync::RwLock;
use velda_core::{L7Request, L7Response, StreamingMode};
use velda_grpc::client::GrpcUpstreamConnector;
use velda_grpc::pipe::{GrpcPipeStrategy, pipe_grpc_stream};
use velda_grpc::server::GrpcServerStream;

use super::lb::EdgeUpstream;
use crate::error::EdgeError;

/// Layer 7 gRPC Upstream managing streaming and unary proxying handoff.
///
/// Pre-compiled with static load balancer, streaming strategy, and persistent multiplexed client cache.
pub struct GrpcUpstream {
    /// [PRE-COMPILED]: Pre-assembled upstream core holding discovery, health tracker,
    /// timeouts, and the selected `LbAlgorithm` enum variant.
    inner: EdgeUpstream,
    /// Declarative streaming mode from upstream configuration.
    pub streaming: StreamingMode,
    /// [PRE-COMPILED]: Pre-computed wire forwarding strategy (`GrpcPipeStrategy`) derived from
    /// `streaming` at compile time; eliminates runtime enum branching.
    pub strategy: GrpcPipeStrategy,
    /// Persistent multiplexed gRPC client connection cache for Unary RPC reuse.
    clients: RwLock<HashMap<SocketAddr, GrpcUpstreamConnector>>,
}

impl std::fmt::Debug for GrpcUpstream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GrpcUpstream")
            .field("id", &self.inner.id())
            .field("streaming", &self.streaming)
            .field("strategy", &self.strategy)
            .finish()
    }
}

impl GrpcUpstream {
    /// Creates a new [`GrpcUpstream`] instance.
    pub fn new(inner: EdgeUpstream, streaming: StreamingMode) -> Self {
        let strategy = GrpcPipeStrategy::from_streaming(streaming);
        Self {
            inner,
            streaming,
            strategy,
            clients: RwLock::new(HashMap::new()),
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
        let stream_cell = Arc::new(std::sync::Mutex::new(Some(server_stream)));
        let strategy = self.strategy;
        let config = config.clone();

        self.inner
            .execute(|endpoint| {
                let cell = Arc::clone(&stream_cell);
                let cfg = config.clone();
                async move {
                    let stream = cell
                        .lock()
                        .unwrap()
                        .take()
                        .ok_or_else(|| "gRPC server stream already consumed".to_string())?;
                    pipe_grpc_stream(stream, endpoint, strategy, &cfg)
                        .await
                        .map_err(|e| e.to_string())
                }
            })
            .await
            .map_err(EdgeError::Upstream)
    }

    /// Hands off downstream unary gRPC request to upstream backend.
    /// Reuses established multiplexed HTTP/2 client connections to eliminate TCP/TLS handshake storm.
    pub async fn dispatch_unary(
        &self,
        req: &L7Request,
        config: &velda_grpc::GrpcConfig,
    ) -> Result<L7Response, EdgeError> {
        self.inner
            .execute(|endpoint| async move {
                let existing = {
                    let guard = self.clients.read().await;
                    guard.get(&endpoint).cloned()
                };

                let mut connector = match existing {
                    Some(mut c) => {
                        if c.ready().await.is_ok() {
                            c
                        } else {
                            self.clients.write().await.remove(&endpoint);
                            let mut fresh = GrpcUpstreamConnector::connect(endpoint, config)
                                .await
                                .map_err(|e| e.to_string())?;
                            fresh.ready().await.map_err(|e| e.to_string())?;
                            self.clients.write().await.insert(endpoint, fresh.clone());
                            fresh
                        }
                    }
                    None => {
                        let mut fresh = GrpcUpstreamConnector::connect(endpoint, config)
                            .await
                            .map_err(|e| e.to_string())?;
                        fresh.ready().await.map_err(|e| e.to_string())?;
                        self.clients.write().await.insert(endpoint, fresh.clone());
                        fresh
                    }
                };

                connector
                    .invoke_unary(req, config)
                    .await
                    .map_err(|e| e.to_string())
            })
            .await
            .map_err(EdgeError::Upstream)
    }
}
