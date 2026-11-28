//! Layer 7 gRPC over UDP to UDP (QUIC) Client Streaming Pipeline.
//!
//! Handles Client-side streaming RPCs forwarded natively over upstream
//! gRPC over UDP / QUIC connections.
//!
//! Self-contained pipeline adhering to the Hourglass Architecture & Flat Workflow.

use std::sync::Arc;
use std::time::Duration;

use velda_core::{L7Request, L7Response};
use velda_grpc::config::GrpcConfig;
use velda_grpc::outcome::GrpcOutcome;
use velda_grpc::udp::client::connector::GrpcUdpClient;

use crate::upstream::GrpcUdpUpstream;

/// Dispatches a client-streaming gRPC request over UDP / QUIC.
async fn pipe_client_stream_wire(
    client: &GrpcUdpClient,
    req: &L7Request,
    config: &GrpcConfig,
) -> Result<L7Response, GrpcOutcome> {
    let timeout = Duration::from_millis(config.max_call_duration_ms);

    let workflow = async {
        let resp = client.send_request(req.clone()).await?;

        // Validate response LPM length if present
        let b = resp.body.as_slice();
        if !b.is_empty() && b.len() >= 5 {
            let declared_len = u32::from_be_bytes([b[1], b[2], b[3], b[4]]) as usize;
            if declared_len > config.max_message_size {
                return Err(GrpcOutcome::PayloadTooLarge(declared_len));
            }
        }

        Ok(resp)
    };

    tokio::time::timeout(timeout, workflow)
        .await
        .map_err(|_| GrpcOutcome::Timeout)?
}

/// Serves a client-streaming gRPC request over UDP forwarded to a gRPC UDP backend.
pub async fn serve(
    req: &L7Request,
    upstream: &Arc<GrpcUdpUpstream>,
    config: &GrpcConfig,
) -> Result<(L7Response, Option<(u128, u16)>), GrpcOutcome> {
    let client = upstream.acquire(config).await.map_err(|e| {
        tracing::warn!(
            error = %e,
            upstream = %upstream.id(),
            "Failed to acquire upstream gRPC over UDP client"
        );
        GrpcOutcome::Io(std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            e.to_string(),
        ))
    })?;
    let upstream_addr = Some(client.target());

    pipe_client_stream_wire(&client, req, config)
        .await
        .map(|resp| (resp, upstream_addr))
}
