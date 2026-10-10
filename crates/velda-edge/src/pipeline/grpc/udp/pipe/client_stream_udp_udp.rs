//! Layer 7 gRPC over UDP to UDP (QUIC) Client Streaming Pipeline.
//!
//! Handles Client-side streaming RPCs forwarded natively over upstream
//! gRPC over UDP / QUIC connections.

use std::sync::Arc;

use velda_core::{L7Request, L7Response};
use velda_grpc::udp::pipe::pipe_client_stream;
use velda_grpc::{GrpcConfig, GrpcError};

use crate::upstream::GrpcUdpUpstream;

/// Serves a client-streaming gRPC request over UDP forwarded to a gRPC UDP backend.
pub async fn serve(
    req: &L7Request,
    upstream: &Arc<GrpcUdpUpstream>,
    config: &GrpcConfig,
) -> Result<L7Response, GrpcError> {
    let client = upstream.acquire(config).await.map_err(|e| {
        tracing::warn!(
            error = %e,
            upstream = %upstream.id(),
            "Failed to acquire upstream gRPC over UDP client"
        );
        GrpcError::Io(std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            e.to_string(),
        ))
    })?;

    pipe_client_stream(&client, req, config).await
}
