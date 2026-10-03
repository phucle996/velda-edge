//! Layer 7 gRPC Upstream managing streaming and unary proxying handoff.

use std::sync::Arc;

use velda_core::{L7Request, L7Response, StreamingMode};

use super::lb::EdgeUpstream;
use crate::error::EdgeError;

/// Layer 7 gRPC Upstream managing streaming and unary proxying handoff.
///
/// Pre-compiled with static load balancer and streaming strategy.
pub struct GrpcUpstream {
    /// [PRE-COMPILED]: Pre-assembled upstream core holding discovery, health tracker,
    /// timeouts, and the selected `LbAlgorithm` enum variant.
    inner: EdgeUpstream,
    /// Declarative streaming mode from upstream configuration.
    pub streaming: StreamingMode,
}

impl std::fmt::Debug for GrpcUpstream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GrpcUpstream")
            .field("id", &self.inner.id())
            .field("streaming", &self.streaming)
            .finish()
    }
}

impl GrpcUpstream {
    /// Creates a new [`GrpcUpstream`] instance.
    pub fn new(inner: EdgeUpstream, streaming: StreamingMode) -> Self {
        Self { inner, streaming }
    }

    /// Returns the upstream unique identifier.
    #[inline]
    pub fn id(&self) -> &str {
        self.inner.id()
    }

    /// Hands off downstream bidirectional gRPC server stream to upstream backend.
    pub async fn dispatch_stream(
        &self,
        server_stream: velda_grpc::composer_parse::GrpcServerStream,
    ) -> Result<(), EdgeError> {
        let stream_cell = Arc::new(std::sync::Mutex::new(Some(server_stream)));

        self.inner
            .execute(|endpoint| {
                let cell = Arc::clone(&stream_cell);
                async move {
                    let stream = cell
                        .lock()
                        .unwrap()
                        .take()
                        .ok_or_else(|| "gRPC server stream already consumed".to_string())?;
                    velda_grpc::pipe_grpc_stream(stream, endpoint)
                        .await
                        .map_err(|e| e.to_string())
                }
            })
            .await
            .map_err(EdgeError::Upstream)
    }

    /// Hands off downstream unary gRPC request to upstream backend.
    pub async fn dispatch_unary(
        &self,
        req: &L7Request,
        config: &velda_grpc::GrpcConfig,
    ) -> Result<L7Response, EdgeError> {
        self.inner
            .execute(|endpoint| async move {
                velda_grpc::upstream_connector::GrpcUpstreamConnector::forward_unary(
                    req, endpoint, config,
                )
                .await
                .map_err(|e| e.to_string())
            })
            .await
            .map_err(EdgeError::Upstream)
    }
}
