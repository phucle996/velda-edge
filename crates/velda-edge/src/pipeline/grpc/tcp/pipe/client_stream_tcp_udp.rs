//! Layer 7 gRPC over TCP to UDP (QUIC) Client Streaming Bridge Pipeline.
//!
//! Handles client streaming RPCs where downstream gRPC requests over TCP / HTTP/2
//! are bridged and forwarded over upstream gRPC UDP / QUIC connections.
//!
//! Self-contained pipeline adhering to the Hourglass Architecture & Flat Workflow.

use std::sync::Arc;
use std::time::Duration;

use velda_core::{Body, L7Request, L7Response};
use velda_grpc::tcp::server::GrpcServerStream;
use velda_grpc::udp::client::GrpcUdpClient;
use velda_grpc::{GrpcConfig, GrpcError, GrpcStatus};

use crate::pipeline::PipeOutcome;
use crate::upstream::GrpcUdpUpstream;

/// Dispatches a client-streaming gRPC request over UDP / QUIC.
async fn pipe_client_stream_udp(
    client: &GrpcUdpClient,
    req: &L7Request,
    config: &GrpcConfig,
) -> Result<L7Response, GrpcError> {
    let timeout = Duration::from_millis(config.max_call_duration_ms);

    let workflow = async {
        let resp = client.send_request_ref(req).await?;

        // Validate response LPM length if present
        let b = resp.body.as_slice();
        if !b.is_empty() && b.len() >= 5 {
            let declared_len = u32::from_be_bytes([b[1], b[2], b[3], b[4]]) as usize;
            if declared_len > config.max_message_size {
                return Err(GrpcError::PayloadTooLarge(declared_len));
            }
        }

        Ok(resp)
    };

    tokio::time::timeout(timeout, workflow)
        .await
        .map_err(|_| GrpcError::Timeout)?
}

/// Serves a client-streaming gRPC request over TCP bridged to a gRPC UDP / QUIC backend.
pub async fn serve(
    mut server_stream: GrpcServerStream,
    upstream: &Arc<GrpcUdpUpstream>,
    config: &GrpcConfig,
) -> PipeOutcome {
    let req_data = match server_stream
        .read_raw_message(config.max_message_size)
        .await
    {
        Ok(data) => data,
        Err(_) => {
            let _ = server_stream.respond.send_trailers_only(
                GrpcStatus::ResourceExhausted,
                Some("request message size exceeds limit"),
            );
            return PipeOutcome::grpc(
                200,
                velda_observability::LogGrpcStatus::ResourceExhausted,
                0,
                0,
                None,
                velda_observability::LogCompletionReason::Rejected,
            );
        }
    };
    let bytes_rx = req_data.len() as u64;

    let l7_req = L7Request::new(
        server_stream.parts.method.clone(),
        server_stream.parts.uri.clone(),
        http::Version::HTTP_3,
        server_stream.parts.headers.clone(),
        Body::Bytes(req_data),
    );

    let client = match upstream.acquire(config).await {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(
                error = %e,
                upstream = %upstream.id(),
                "Failed to acquire upstream gRPC over UDP connection"
            );
            let _ = server_stream
                .respond
                .send_trailers_only(GrpcStatus::Unavailable, Some("upstream unavailable"));
            return PipeOutcome::grpc(
                200,
                velda_observability::LogGrpcStatus::Unavailable,
                bytes_rx,
                0,
                None,
                velda_observability::LogCompletionReason::UpstreamError,
            );
        }
    };
    let mut upstream_addr = Some(client.target());

    let mut pipe_res = pipe_client_stream_udp(&client, &l7_req, config).await;

    // 1-Shot Self-Healing Retry
    if let Err(ref e) = pipe_res
        && e.is_stale_or_refused()
    {
        tracing::debug!(
            upstream = %upstream.id(),
            error = %e,
            "gRPC UDP connection dropped; self-healing with fresh connection"
        );
        if let Ok(fresh_client) = upstream.acquire_fresh(config).await {
            upstream_addr = Some(fresh_client.target());
            pipe_res = pipe_client_stream_udp(&fresh_client, &l7_req, config).await;
        }
    }

    match pipe_res {
        Ok(resp) => {
            let status = resp
                .headers
                .get(velda_grpc::wire::GrpcWire::STATUS_NAME)
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<u32>().ok())
                .map(GrpcStatus::from_code)
                .unwrap_or(GrpcStatus::Ok);

            let bytes_tx = resp.body.len() as u64;
            let body_bytes = if resp.body.is_empty() {
                None
            } else {
                Some(resp.body.to_bytes())
            };

            let _ = server_stream.respond.send_unary_response(
                status,
                body_bytes.as_deref(),
                Some(resp.headers.into()),
            );
            PipeOutcome::grpc(
                200,
                velda_observability::LogGrpcStatus::from_u8(status as u8),
                bytes_rx,
                bytes_tx,
                upstream_addr,
                velda_observability::LogCompletionReason::Complete,
            )
        }
        Err(_) => {
            let _ = server_stream
                .respond
                .send_trailers_only(GrpcStatus::Unavailable, Some("upstream bridge failure"));
            PipeOutcome::grpc(
                200,
                velda_observability::LogGrpcStatus::Unavailable,
                bytes_rx,
                0,
                upstream_addr,
                velda_observability::LogCompletionReason::UpstreamError,
            )
        }
    }
}
