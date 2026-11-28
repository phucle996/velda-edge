//! # Canonical Pipeline Classification: Standard Pipe (Kernel Network Stack)
//!
//! Unlike **Acceleration Pipes** (`*_tcp_rdma`) that bypass the OS kernel via hardware RDMA Verbs,
//! this is a **Standard Pipe** routing outbound traffic through the Linux kernel TCP network stack
//! using pre-compiled socket acceleration flags (`TCP_NODELAY`,
//! `TCP_FASTOPEN_CONNECT`, `TCP_NOTSENT_LOWAT`, `SO_BUSY_POLL`, `TCP_QUICKACK`, `BBR`).
//!
//! ### Activation Conditions
//! 1. **Upstream Configuration**: Route resolves to an upstream backend with `protocol.transport = "tcp"`.
//! 2. **Transport & Framing**: Standard OS TCP sockets over IPv4/IPv6 with HTTP/2 binary framing.
//! 3. **Pipe Pattern Match**: Upstream strategy explicitly configured as `ClientStream` (N request chunks to 1 response).
//!
//! Self-contained pipeline adhering to the Hourglass Architecture & Flat Workflow.

use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use velda_grpc::GrpcStatus;
use velda_grpc::config::GrpcConfig;
use velda_grpc::outcome::GrpcOutcome;
use velda_grpc::tcp::server::stream::GrpcServerStream;

use crate::pipeline::PipeOutcome;
use crate::upstream::GrpcTcpUpstream;

/// Serves a client-streaming gRPC request over TCP forwarded to a gRPC TCP backend.
pub async fn serve(
    mut server_stream: GrpcServerStream,
    upstream: &Arc<GrpcTcpUpstream>,
    config: &GrpcConfig,
) -> PipeOutcome {
    // Stage 1: Lease pre-warmed connection handle from worker core lane shard without cross-thread lock contention.
    let client = match upstream.acquire(config).await {
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
            return PipeOutcome::grpc(
                velda_grpc::GrpcStatus::Unavailable,
                0,
                0,
                None,
                velda_core::InternalStatus::UpstreamError,
            );
        }
    };
    let upstream_addr = client.target();

    let timeout = Duration::from_millis(config.max_call_duration_ms);

    // Stage 2: Execute client streaming workflow bounded by max_call_duration_ms deadline.
    let workflow = async {
        let mut bytes_rx: u64 = 0;

        // Step 1: Open outbound HTTP/2 stream directly from HeaderWorkspace
        let (upstream_resp_fut, mut upstream_send_stream) = client
            .open_stream(
                server_stream.head.method,
                server_stream.head.uri,
                server_stream.head.headers,
                false,
            )
            .await?;

        // Step 2: Pump multiple client request chunks to upstream with backpressure
        if !server_stream.receiver.is_end_stream() {
            while let Some(chunk) = server_stream
                .receiver
                .recv_chunk()
                .await
                .map_err(GrpcOutcome::from_http2)?
            {
                bytes_rx += chunk.len() as u64;
                upstream_send_stream
                    .send_chunk(chunk, false)
                    .await
                    .map_err(GrpcOutcome::from_http2)?;
            }

            if let Some(trailers) = server_stream.receiver.take_trailers() {
                upstream_send_stream
                    .send_trailers(trailers)
                    .map_err(GrpcOutcome::from_http2)?;
            } else {
                upstream_send_stream
                    .send_chunk(Bytes::new(), true)
                    .await
                    .map_err(GrpcOutcome::from_http2)?;
            }
        } else {
            upstream_send_stream
                .send_chunk(Bytes::new(), true)
                .await
                .map_err(GrpcOutcome::from_http2)?;
        }

        // Step 3: Await single upstream response headers
        let mut response = upstream_resp_fut.await.map_err(GrpcOutcome::from_http2)?;

        let mut grpc_status = response
            .headers
            .get("grpc-status")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u8>().ok())
            .map(velda_grpc::GrpcStatus::from_u8)
            .unwrap_or(velda_grpc::GrpcStatus::Ok);

        let is_end_of_stream = response.body.is_end_stream();
        let Some(mut downstream_send_stream) = server_stream.respond.send_response_headers(
            response.status,
            &response.headers,
            is_end_of_stream,
        )?
        else {
            return Ok((bytes_rx, 0, grpc_status));
        };

        let mut resp_len: u64 = 0;

        // Step 4: Forward response chunks and trailers, enforcing max_message_size
        let mut has_trailers = false;
        let mut checked_resp_lpm = false;
        let mut resp_head = BytesMut::new();

        while let Some(chunk) = response
            .body
            .recv_chunk()
            .await
            .map_err(GrpcOutcome::from_http2)?
        {
            let len = chunk.len();
            resp_len += len as u64;

            // Early LPM header check: reject on first 5 bytes if declared length exceeds limit
            if !checked_resp_lpm {
                resp_head.extend_from_slice(&chunk);
                if resp_head.len() >= 5 {
                    let declared_len = u32::from_be_bytes([
                        resp_head[1],
                        resp_head[2],
                        resp_head[3],
                        resp_head[4],
                    ]) as usize;
                    if declared_len > config.max_message_size {
                        return Err(GrpcOutcome::PayloadTooLarge(declared_len));
                    }
                    checked_resp_lpm = true;
                }
            }

            if resp_len > (config.max_message_size + 5) as u64 {
                return Err(GrpcOutcome::PayloadTooLarge(resp_len as usize));
            }

            downstream_send_stream
                .send_chunk(chunk)
                .await
                .map_err(GrpcOutcome::from_http2)?;
        }

        if let Some(trailers) = response
            .body
            .trailers()
            .await
            .map_err(GrpcOutcome::from_http2)?
        {
            if let Some(code) = trailers
                .get("grpc-status")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<u8>().ok())
                .map(velda_grpc::GrpcStatus::from_u8)
            {
                grpc_status = code;
            }
            downstream_send_stream
                .send_trailers(trailers)
                .map_err(GrpcOutcome::from_http2)?;
            has_trailers = true;
        }

        if !has_trailers {
            downstream_send_stream
                .finish()
                .map_err(GrpcOutcome::from_http2)?;
        }

        Ok((bytes_rx, resp_len, grpc_status))
    };

    match tokio::time::timeout(timeout, workflow).await {
        Ok(Ok((rx, tx, status))) => PipeOutcome::grpc(
            status,
            rx,
            tx,
            upstream_addr,
            velda_core::InternalStatus::Completed,
        ),
        Ok(Err(_)) => PipeOutcome::grpc(
            velda_grpc::GrpcStatus::Unavailable,
            0,
            0,
            upstream_addr,
            velda_core::InternalStatus::UpstreamError,
        ),
        Err(_) => {
            let _ = server_stream.respond.send_trailers_only(
                GrpcStatus::DeadlineExceeded,
                Some("gRPC call duration deadline exceeded"),
            );
            PipeOutcome::grpc(
                velda_grpc::GrpcStatus::DeadlineExceeded,
                0,
                0,
                upstream_addr,
                velda_core::InternalStatus::UpstreamTimeout,
            )
        }
    }
}
