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
//! 3. **Pipe Pattern Match**: Upstream strategy explicitly configured as `Duplex` (Bidirectional streaming).
//!
//! Self-contained pipeline adhering to the Hourglass Architecture & Flat Workflow.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use velda_grpc::GrpcStatus;
use velda_grpc::config::GrpcConfig;
use velda_grpc::outcome::GrpcOutcome;
use velda_grpc::tcp::server::stream::GrpcServerStream;

use crate::pipeline::PipeOutcome;
use crate::upstream::GrpcTcpUpstream;

/// Serves a full-duplex bidirectional gRPC request over TCP forwarded to a gRPC TCP backend.
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

    // Stage 2: Concurrently drive full-duplex bidirectional streaming bounded by call deadline.
    let workflow = async {
        let GrpcServerStream {
            head,
            mut receiver,
            mut respond,
        } = server_stream;

        // Step 1: Open upstream request stream directly from HeaderWorkspace
        let (upstream_resp_fut, mut upstream_send_stream) = client
            .open_stream(head.method, head.uri, head.headers, false)
            .await?;

        // Step 2: Downstream -> Upstream pump task
        let client_to_upstream = async move {
            let mut bytes_rx: u64 = 0;
            if !receiver.is_end_stream() {
                while let Some(chunk) = receiver
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

                if let Some(trailers) = receiver.take_trailers() {
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

            Ok::<u64, GrpcOutcome>(bytes_rx)
        };

        // Step 3: Upstream -> Downstream pump task
        let upstream_to_client = async move {
            let mut response = upstream_resp_fut.await.map_err(GrpcOutcome::from_http2)?;

            let mut grpc_status = response
                .headers
                .get("grpc-status")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<u8>().ok())
                .map(velda_grpc::GrpcStatus::from_u8)
                .unwrap_or(velda_grpc::GrpcStatus::Ok);

            let is_end_of_stream = response.body.is_end_stream();
            let Some(mut downstream_send_stream) = respond.send_response_headers(
                response.status,
                &response.headers,
                is_end_of_stream,
            )?
            else {
                return Ok((0, grpc_status));
            };

            let mut bytes_tx: u64 = 0;
            let mut has_trailers = false;

            while let Some(chunk) = response
                .body
                .recv_chunk()
                .await
                .map_err(GrpcOutcome::from_http2)?
            {
                bytes_tx += chunk.len() as u64;
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

            Ok::<(u64, velda_grpc::GrpcStatus), GrpcOutcome>((bytes_tx, grpc_status))
        };

        // Step 4: Concurrently drive both stream directions
        let (bytes_rx, (bytes_tx, grpc_status)) =
            tokio::try_join!(client_to_upstream, upstream_to_client)?;

        Ok::<_, GrpcOutcome>((bytes_rx, bytes_tx, grpc_status))
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
        Err(_) => PipeOutcome::grpc(
            velda_grpc::GrpcStatus::DeadlineExceeded,
            0,
            0,
            upstream_addr,
            velda_core::InternalStatus::UpstreamTimeout,
        ),
    }
}
