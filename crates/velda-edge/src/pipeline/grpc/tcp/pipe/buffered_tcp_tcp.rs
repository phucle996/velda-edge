//! # Canonical Pipeline Classification: Standard Pipe (Kernel Network Stack)
//!
//! Unlike **Acceleration Pipes** (`*_tcp_rdma`) that bypass the OS kernel via hardware RDMA Verbs,
//! this is a **Standard Pipe** routing outbound traffic through the Linux kernel TCP network stack
//! using pre-compiled socket acceleration flags (`TcpAccelerationPath` / `GrpcAccelerationPath`:
//! `TCP_NODELAY`, `TCP_FASTOPEN_CONNECT`, `TCP_NOTSENT_LOWAT`, `SO_BUSY_POLL`, `TCP_QUICKACK`, `BBR`).
//!
//! ### Activation Conditions
//! 1. **Upstream Configuration**: Route resolves to an upstream backend with `protocol.transport = "tcp"`.
//! 2. **Transport & Framing**: Standard OS TCP sockets over IPv4/IPv6 with HTTP/2 binary framing.
//! 3. **Pipe Pattern Match**: Upstream strategy explicitly configured as `Buffered` (Unary 1-to-1 RPC).
//!
//! Self-contained pipeline adhering to the Hourglass Architecture & Flat Workflow.

use bytes::{Bytes, BytesMut};
use http::Version;
use std::sync::Arc;
use std::time::Duration;

use velda_grpc::tcp::client::GrpcUpstreamConnector;
use velda_grpc::tcp::server::{GrpcResponder, GrpcServerStream};
use velda_grpc::{GrpcConfig, GrpcError, GrpcStatus};

use crate::pipeline::PipeOutcome;
use crate::upstream::GrpcTcpUpstream;

/// Pipes a Unary (non-streaming) gRPC call between downstream responder and upstream connector.
async fn pipe_buffered_wire(
    parts: &http::request::Parts,
    req_data: &Bytes,
    respond: &mut GrpcResponder,
    connector: &mut GrpcUpstreamConnector,
    config: &GrpcConfig,
) -> Result<(u64, velda_observability::LogGrpcStatus), GrpcError> {
    let timeout = Duration::from_millis(config.max_call_duration_ms);

    let workflow = async {
        // Step 1: Build outbound HTTP/2 request frame from downstream parts
        let mut req_builder = http::Request::builder()
            .method(&parts.method)
            .uri(&parts.uri)
            .version(Version::HTTP_2);

        for (name, val) in &parts.headers {
            req_builder = req_builder.header(name, val);
        }

        let upstream_req = req_builder.body(()).map_err(GrpcError::Http)?;
        let has_body = !req_data.is_empty();

        // Step 2: Transmit request to upstream
        let (upstream_resp_fut, mut upstream_send_stream) =
            connector.open_stream(upstream_req, !has_body)?;

        if has_body {
            upstream_send_stream
                .send_data(req_data.clone(), true)
                .map_err(GrpcError::H2)?;
        }

        // Step 3: Await upstream response headers
        let response = upstream_resp_fut.await.map_err(GrpcError::H2)?;
        let (mut resp_parts, mut upstream_recv_stream) = response.into_parts();

        let mut resp_builder = http::Response::builder()
            .status(resp_parts.status)
            .version(Version::HTTP_2);

        for (name, val) in resp_parts.headers.drain() {
            if let Some(name) = name {
                resp_builder = resp_builder.header(name, val);
            }
        }

        let mut grpc_status = resp_parts
            .headers
            .get("grpc-status")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u8>().ok())
            .map(velda_observability::LogGrpcStatus::from_u8)
            .unwrap_or(velda_observability::LogGrpcStatus::Ok);

        let is_end_of_stream = upstream_recv_stream.is_end_stream();
        let resp = resp_builder.body(()).map_err(GrpcError::Http)?;
        let mut downstream_send_stream = respond
            .send_response(resp, is_end_of_stream)
            .map_err(GrpcError::H2)?;

        let mut resp_len = 0;

        // Step 4: Forward response chunks and trailers downstream
        if !is_end_of_stream {
            let mut has_trailers = false;
            let mut checked_resp_lpm = false;
            let mut resp_head = BytesMut::new();

            while let Some(chunk_res) = upstream_recv_stream.data().await {
                let chunk = chunk_res.map_err(GrpcError::H2)?;
                let len = chunk.len();
                resp_len += len;

                // Validate the 5-byte Length-Prefixed Message (LPM) header (1 compression flag + 4 length bytes).
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
                            return Err(GrpcError::PayloadTooLarge(declared_len));
                        }
                        checked_resp_lpm = true;
                    }
                }

                if resp_len > config.max_message_size + 5 {
                    return Err(GrpcError::PayloadTooLarge(resp_len));
                }

                downstream_send_stream
                    .send_data(chunk, false)
                    .map_err(GrpcError::H2)?;
                // Replenish HTTP/2 receive window to prevent upstream sender stalls
                let _ = upstream_recv_stream.flow_control().release_capacity(len);
            }

            // Step 5: Forward response trailers or synthesize empty frame with END_STREAM.
            if let Some(trailers) = upstream_recv_stream
                .trailers()
                .await
                .map_err(GrpcError::H2)?
            {
                if let Some(code) = trailers
                    .get("grpc-status")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|s| s.parse::<u8>().ok())
                    .map(velda_observability::LogGrpcStatus::from_u8)
                {
                    grpc_status = code;
                }
                downstream_send_stream
                    .send_trailers(trailers)
                    .map_err(GrpcError::H2)?;
                has_trailers = true;
            }

            if !has_trailers {
                downstream_send_stream
                    .send_data(Bytes::new(), true)
                    .map_err(GrpcError::H2)?;
            }
        }

        Ok((resp_len as u64, grpc_status))
    };

    match tokio::time::timeout(timeout, workflow).await {
        Ok(res) => res,
        Err(_) => {
            let _ = respond.send_trailers_only(
                GrpcStatus::DeadlineExceeded,
                Some("gRPC call duration deadline exceeded"),
            );
            Err(GrpcError::Io(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "gRPC call timed out",
            )))
        }
    }
}

/// Serves a buffered gRPC request over TCP forwarded to a gRPC TCP backend.
pub async fn serve(
    mut server_stream: GrpcServerStream,
    upstream: &Arc<GrpcTcpUpstream>,
    config: &GrpcConfig,
) -> PipeOutcome {
    // Stage 1: Ingest downstream raw request payload and enforce max_message_size limit.
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

    // Stage 2: Lease a pre-warmed connection handle from the worker core's local lane shard without cross-thread lock contention.
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
    let mut upstream_addr = client.target();

    // Stage 3: Forward unary call over HTTP/2 wire protocol.
    let mut res = pipe_buffered_wire(
        &server_stream.parts,
        &req_data,
        &mut server_stream.respond,
        &mut client,
        config,
    )
    .await;

    // Stage 4: Self-Healing Retry (RFC 9113 §8.1.4):
    // If an idle pooled connection was silently dropped by peer or returned REFUSED_STREAM,
    // retry transparently 1-shot on a guaranteed fresh connection before reporting error.
    if let Err(ref e) = res
        && e.is_stale_or_refused()
    {
        tracing::debug!(
            upstream = %upstream.id(),
            error = %e,
            "gRPC upstream connection dropped or refused; self-healing with fresh connection"
        );
        if let Ok(mut fresh_client) = upstream.acquire_fresh(config).await {
            upstream_addr = fresh_client.target();
            res = pipe_buffered_wire(
                &server_stream.parts,
                &req_data,
                &mut server_stream.respond,
                &mut fresh_client,
                config,
            )
            .await;
        }
    }

    match res {
        Ok((bytes_tx, grpc_status)) => PipeOutcome::grpc(
            200,
            grpc_status,
            bytes_rx,
            bytes_tx,
            upstream_addr,
            velda_observability::LogCompletionReason::Complete,
        ),
        Err(_) => PipeOutcome::grpc(
            200,
            velda_observability::LogGrpcStatus::Unavailable,
            bytes_rx,
            0,
            upstream_addr,
            velda_observability::LogCompletionReason::UpstreamError,
        ),
    }
}
