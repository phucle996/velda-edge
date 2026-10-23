//! Layer 7 gRPC over TCP to UDP (QUIC) Buffered Bridge Pipeline.
//!
//! Handles standard Unary request-response RPCs where downstream gRPC requests
//! over TCP / HTTP/2 binary framing are bridged and forwarded over upstream
//! gRPC UDP / QUIC connections.
//!
//! Self-contained pipeline adhering to the Hourglass Architecture & Flat Workflow.

use std::sync::Arc;
use std::time::Duration;

use velda_core::{Body, L7Request, L7Response};
use velda_grpc::tcp::server::GrpcServerStream;
use velda_grpc::udp::client::GrpcUdpClient;
use velda_grpc::{GrpcConfig, GrpcError, GrpcStatus};

use crate::upstream::GrpcUdpUpstream;

/// Dispatches a gRPC request over UDP / QUIC within configured timeout and message limits.
async fn pipe_buffered_udp(
    client: &GrpcUdpClient,
    req: &L7Request,
    config: &GrpcConfig,
) -> Result<L7Response, GrpcError> {
    let timeout = Duration::from_millis(config.max_call_duration_ms);

    let workflow = async {
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

        let resp = client.send_request_ref(req).await?;

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

/// Serves a buffered gRPC request over TCP bridged to a gRPC UDP / QUIC backend.
pub async fn serve(
    mut server_stream: GrpcServerStream,
    upstream: &Arc<GrpcUdpUpstream>,
    config: &GrpcConfig,
) -> Result<(), GrpcError> {
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
            return Ok(());
        }
    };

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
            return Ok(());
        }
    };

    let mut pipe_res = pipe_buffered_udp(&client, &l7_req, config).await;

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
            pipe_res = pipe_buffered_udp(&fresh_client, &l7_req, config).await;
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

            let body_bytes = if resp.body.is_empty() {
                None
            } else {
                Some(resp.body.to_bytes())
            };

            server_stream.respond.send_unary_response(
                status,
                body_bytes.as_deref(),
                Some(resp.headers.into()),
            )?;
            Ok(())
        }
        Err(e) => {
            let _ = server_stream
                .respond
                .send_trailers_only(GrpcStatus::Unavailable, Some("upstream bridge failure"));
            Err(e)
        }
    }
}
