//! Layer 7 gRPC over UDP to UDP (QUIC) Buffered Pipeline.
//!
//! Handles standard Unary request-response RPCs forwarded natively over upstream
//! gRPC over UDP / QUIC connections.

use std::sync::Arc;

use velda_core::{L7Request, L7Response};
use velda_grpc::udp::pipe::pipe_buffered;
use velda_grpc::{GrpcConfig, GrpcError};

use crate::upstream::GrpcUdpUpstream;

/// Serves a buffered gRPC request over UDP forwarded to a gRPC UDP backend.
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

    let mut pipe_res = pipe_buffered(&client, req, config).await;

    // Self-Healing Retry:
    // If upstream QUIC connection dropped or reset, acquire a fresh client and retry once.
    if let Err(ref e) = pipe_res
        && e.is_stale_or_refused()
    {
        tracing::debug!(
            upstream = %upstream.id(),
            error = %e,
            "gRPC over UDP connection dropped; self-healing with fresh connection"
        );
        if let Ok(fresh_client) = upstream.acquire_fresh(config).await {
            pipe_res = pipe_buffered(&fresh_client, req, config).await;
        }
    }

    pipe_res
}
