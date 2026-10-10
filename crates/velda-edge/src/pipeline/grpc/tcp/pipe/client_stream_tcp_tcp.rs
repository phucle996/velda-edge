//! Layer 7 gRPC over TCP to TCP Client Streaming Pipeline.
//!
//! Handles client-streaming RPCs forwarded natively over upstream HTTP/2 binary framing connections.

use std::sync::Arc;

use velda_grpc::tcp::pipe::pipe_client_stream;
use velda_grpc::tcp::server::GrpcServerStream;
use velda_grpc::{GrpcConfig, GrpcError, GrpcStatus};

use crate::upstream::GrpcTcpUpstream;

/// Serves a client-streaming gRPC request over TCP forwarded to a gRPC TCP backend.
pub async fn serve(
    mut server_stream: GrpcServerStream,
    upstream: &Arc<GrpcTcpUpstream>,
    config: &GrpcConfig,
) -> Result<(), GrpcError> {
    let mut client = match upstream.acquire(config).await {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(
                error = %e,
                upstream = %upstream.id(),
                "Failed to acquire upstream gRPC over TCP connection"
            );
            let _ = server_stream
                .respond
                .send_trailers_only(GrpcStatus::Unavailable, Some("upstream unavailable"));
            return Ok(());
        }
    };

    pipe_client_stream(server_stream, &mut client, config).await
}
