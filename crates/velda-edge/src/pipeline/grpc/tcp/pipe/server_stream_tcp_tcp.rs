//! Layer 7 gRPC over TCP to TCP Server Streaming Pipeline.
//!
//! Handles server-streaming RPCs forwarded natively over upstream HTTP/2 binary framing connections.
//! Enforces early LPM verification and transparent 1-shot self-healing retry on GOAWAY / REFUSED_STREAM.

use std::sync::Arc;

use velda_grpc::tcp::pipe::pipe_server_stream;
use velda_grpc::tcp::server::GrpcServerStream;
use velda_grpc::{GrpcConfig, GrpcError, GrpcStatus};

use crate::upstream::GrpcTcpUpstream;

/// Serves a server-streaming gRPC request over TCP forwarded to a gRPC TCP backend.
pub async fn serve(
    mut server_stream: GrpcServerStream,
    upstream: &Arc<GrpcTcpUpstream>,
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

    let mut res = pipe_server_stream(
        &server_stream.parts,
        &req_data,
        &mut server_stream.respond,
        &mut client,
        config,
    )
    .await;

    // Self-Healing Retry (RFC 9113 §8.1.4):
    // If upstream connection dropped or remote peer returned REFUSED_STREAM before
    // stream frames were sent, retry transparently 1-shot on a fresh connection.
    if let Err(ref e) = res
        && e.is_stale_or_refused()
    {
        tracing::debug!(
            upstream = %upstream.id(),
            error = %e,
            "gRPC upstream connection dropped or refused; self-healing server-stream with fresh connection"
        );
        if let Ok(mut fresh_client) = upstream.acquire_fresh(config).await {
            res = pipe_server_stream(
                &server_stream.parts,
                &req_data,
                &mut server_stream.respond,
                &mut fresh_client,
                config,
            )
            .await;
        }
    }

    res
}
