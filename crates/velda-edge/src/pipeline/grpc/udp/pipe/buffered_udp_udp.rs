//! Layer 7 gRPC over UDP to UDP (QUIC) Buffered Pipeline.
//!
//! Handles standard Unary request-response RPCs forwarded natively over upstream
//! gRPC over UDP / QUIC connections.
//!
//! Self-contained pipeline adhering to the Hourglass Architecture & Flat Workflow.

use std::sync::Arc;
use std::time::Duration;

use velda_core::{Body, L7Request, L7Response};
use velda_grpc::udp::client::GrpcUdpClient;
use velda_grpc::{GrpcConfig, GrpcError};

use crate::upstream::GrpcUdpUpstream;

/// Dispatches a buffered gRPC request over UDP / QUIC within configured timeout and message limits.
async fn pipe_buffered_wire(
    client: &GrpcUdpClient,
    req: &L7Request,
    config: &GrpcConfig,
) -> Result<L7Response, GrpcError> {
    let timeout = Duration::from_millis(config.max_call_duration_ms);

    let workflow = async {
        // Step 1: Validate downstream request LPM frame if body present
        if let Body::Bytes(ref b) = req.body
            && !b.is_empty()
        {
            if b.len() >= 5 {
                let declared_len = u32::from_be_bytes([b[1], b[2], b[3], b[4]]) as usize;
                if declared_len > config.max_message_size {
                    return Err(GrpcError::PayloadTooLarge(declared_len));
                }
            }
            if b.len() > config.max_message_size + 5 {
                return Err(GrpcError::PayloadTooLarge(b.len()));
            }
        }

        // Step 2: Dispatch through multiplexed QUIC client
        let resp = client.send_request_ref(req).await?;

        // Step 3: Validate upstream response LPM frame if body present
        if let Body::Bytes(ref b) = resp.body
            && !b.is_empty()
        {
            if b.len() >= 5 {
                let declared_len = u32::from_be_bytes([b[1], b[2], b[3], b[4]]) as usize;
                if declared_len > config.max_message_size {
                    return Err(GrpcError::PayloadTooLarge(declared_len));
                }
            }
            if b.len() > config.max_message_size + 5 {
                return Err(GrpcError::PayloadTooLarge(b.len()));
            }
        }

        Ok(resp)
    };

    tokio::time::timeout(timeout, workflow)
        .await
        .map_err(|_| GrpcError::Timeout)?
}

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

    let mut pipe_res = pipe_buffered_wire(&client, req, config).await;

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
            pipe_res = pipe_buffered_wire(&fresh_client, req, config).await;
        }
    }

    pipe_res
}
