//! Layer 7 gRPC over TCP to UDP (QUIC) Buffered Bridge Pipeline.
//!
//! Handles standard Unary request-response RPCs where downstream gRPC requests
//! over TCP / HTTP/2 binary framing are bridged and forwarded over upstream
//! gRPC UDP / QUIC connections.

use std::sync::Arc;

use velda_core::{Body, L7Request};
use velda_grpc::tcp::server::GrpcServerStream;
use velda_grpc::udp::pipe::pipe_buffered;
use velda_grpc::{GrpcConfig, GrpcError, GrpcStatus};

use crate::upstream::GrpcUdpUpstream;

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

    let mut pipe_res = pipe_buffered(&client, &l7_req, config).await;

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
            pipe_res = pipe_buffered(&fresh_client, &l7_req, config).await;
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

            let body_bytes = match resp.body {
                Body::Bytes(b) => Some(b),
                Body::Empty => None,
            };

            server_stream.respond.send_unary_response(
                status,
                body_bytes.as_deref(),
                Some(resp.headers),
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
