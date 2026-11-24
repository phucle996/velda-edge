//! # Canonical Pipeline Classification: Acceleration Pipe (Hardware Kernel-Bypass)
//!
//! Unlike **Standard Pipes** (`*_tcp_tcp`) that route outbound traffic through the Linux kernel
//! network stack (incurring syscall context switches, socket buffer copies, and OS interrupt overhead),
//! this is an **Acceleration Pipe** utilizing `velda-rdma` to bypass the OS kernel entirely.
//! Data is DMA-transferred directly between host user-space RAM and backend hardware via RoCEv2 / InfiniBand.
//!
//! ### Activation Conditions
//! 1. **Upstream Configuration**: Route resolves to an upstream backend configured with `protocol.transport = "rdma"`.
//! 2. **Hardware & Driver Availability**: Host possesses an RDMA-capable NIC (e.g. NVIDIA/Mellanox ConnectX
//!    with RoCEv2/IB fabric via `libibverbs.so.1`) or runs with verified software mock simulation fallback.
//! 3. **Backend Target Protocol**: Target physical endpoint responds to Out-of-Band (OOB) QP handshakes and
//!    supports HTTP/2 binary framing directly over established RDMA Queue Pairs.
//! 4. **Pre-warmed Core Lane State**: Frontline Traffic Worker (`velda-worker-{id}`) leases an established
//!    ready connection from the local core lane shard pre-populated by the core-local
//!    Shadow Logistics Worker (`velda-shadow-{id}`, `SCHED_IDLE`).
//! 5. **Pipe Pattern Match**: Upstream strategy explicitly configured as `ServerStream` (1 request to N response chunks).
//!
//! Self-contained pipeline adhering to the Hourglass Architecture & Flat Workflow.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use velda_core::GrpcStatus;
use velda_grpc::config::GrpcConfig;
use velda_grpc::error::GrpcError;
use velda_grpc::tcp::client::connector::GrpcUpstreamConnector;
use velda_grpc::tcp::server::responder::GrpcResponder;
use velda_grpc::tcp::server::stream::GrpcServerStream;
use velda_http2::server::request::Http2ServerRequestHead;

use crate::pipeline::PipeOutcome;
use crate::upstream::grpc::GrpcRdmaUpstream;

/// Pipes a server-streaming gRPC call over RDMA between downstream responder and upstream connector.
async fn pipe_server_stream_wire(
    head: &Http2ServerRequestHead,
    req_data: &Bytes,
    respond: &mut GrpcResponder,
    connector: &mut GrpcUpstreamConnector,
    config: &GrpcConfig,
) -> Result<(u64, velda_core::GrpcStatus), GrpcError> {
    let timeout = Duration::from_millis(config.max_call_duration_ms);

    let workflow = async {
        // Step 1: Validate LPM header on req_data if present.
        if !req_data.is_empty() {
            if req_data.len() >= 5 {
                let declared_len =
                    u32::from_be_bytes([req_data[1], req_data[2], req_data[3], req_data[4]])
                        as usize;
                if declared_len > config.max_message_size {
                    let _ = respond.send_trailers_only(
                        GrpcStatus::ResourceExhausted,
                        Some("request message length exceeds limit"),
                    );
                    return Err(GrpcError::PayloadTooLarge(declared_len));
                }
            }
            if req_data.len() > config.max_message_size + 5 {
                let _ = respond.send_trailers_only(
                    GrpcStatus::ResourceExhausted,
                    Some("request message size exceeds limit"),
                );
                return Err(GrpcError::PayloadTooLarge(req_data.len()));
            }
        }

        // Step 2: Open outbound HTTP/2 stream over RDMA directly from HeaderWorkspace.
        let has_body = !req_data.is_empty();
        let (upstream_resp_fut, mut upstream_send_stream) = connector
            .open_stream(
                head.method.clone(),
                head.uri.clone(),
                head.headers.clone(),
                !has_body,
            )
            .await?;

        if has_body {
            upstream_send_stream
                .send_chunk(req_data.clone(), true)
                .await?;
        }

        // Step 3: Await upstream response headers over RDMA.
        let mut response = upstream_resp_fut.await?;

        let mut grpc_status = response
            .headers
            .get("grpc-status")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u8>().ok())
            .map(velda_core::GrpcStatus::from_u8)
            .unwrap_or(velda_core::GrpcStatus::Ok);

        let is_end_of_stream = response.body.is_end_stream();
        let Some(mut downstream_send_stream) =
            respond.send_response_headers(response.status, &response.headers, is_end_of_stream)?
        else {
            return Ok((0, grpc_status));
        };

        let mut resp_len = 0;

        // Step 4: Progressively stream data chunks and trailers downstream.
        let mut has_trailers = false;
        while let Some(chunk) = response.body.recv_chunk().await? {
            resp_len += chunk.len();
            downstream_send_stream.send_chunk(chunk).await?;
        }

        if let Some(trailers) = response.body.trailers().await? {
            if let Some(code) = trailers
                .get("grpc-status")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<u8>().ok())
                .map(velda_core::GrpcStatus::from_u8)
            {
                grpc_status = code;
            }
            downstream_send_stream.send_trailers(trailers)?;
            has_trailers = true;
        }

        if !has_trailers {
            downstream_send_stream.finish()?;
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
                "gRPC RDMA server streaming call timed out",
            )))
        }
    }
}

/// Serves a server-streaming gRPC request over TCP forwarded to a gRPC RDMA backend.
pub async fn serve(
    mut server_stream: GrpcServerStream,
    upstream: &Arc<GrpcRdmaUpstream>,
    config: &GrpcConfig,
) -> PipeOutcome {
    // Stage 1: Ingest downstream initial request payload and validate message size.
    let req_data = match server_stream
        .read_raw_message(config.max_message_size)
        .await
    {
        Ok(data) => data,
        Err(_) => {
            let _ = server_stream.respond.send_trailers_only(
                GrpcStatus::ResourceExhausted,
                Some("request message too large"),
            );
            return PipeOutcome::grpc(
                200,
                velda_core::GrpcStatus::ResourceExhausted,
                0,
                0,
                None,
                velda_core::CompletionReason::Rejected,
            );
        }
    };
    let bytes_rx = req_data.len() as u64;

    // Stage 2: Lease pre-warmed RDMA connection handle from worker core lane shard.
    let mut client = match upstream.acquire(config).await {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(
                error = %e,
                upstream = %upstream.id(),
                "Failed to acquire upstream gRPC over RDMA connection"
            );
            let _ = server_stream
                .respond
                .send_trailers_only(GrpcStatus::Unavailable, Some("rdma upstream unavailable"));
            return PipeOutcome::grpc(
                200,
                velda_core::GrpcStatus::Unavailable,
                bytes_rx,
                0,
                None,
                velda_core::CompletionReason::UpstreamError,
            );
        }
    };
    let mut upstream_addr = client.target();

    // Stage 3: Forward server-streaming call over HTTP/2 RDMA wire protocol.
    let mut res = pipe_server_stream_wire(
        &server_stream.head,
        &req_data,
        &mut server_stream.respond,
        &mut client,
        config,
    )
    .await;

    // Stage 4: Self-Healing Retry (RFC 9113 §8.1.4)
    if let Err(ref e) = res
        && e.is_stale_or_refused()
    {
        tracing::debug!(
            upstream = %upstream.id(),
            error = %e,
            "gRPC RDMA upstream connection dropped or refused; self-healing with fresh connection"
        );
        if let Ok(mut fresh_client) = upstream.acquire_fresh(config).await {
            upstream_addr = fresh_client.target();
            res = pipe_server_stream_wire(
                &server_stream.head,
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
            velda_core::CompletionReason::Complete,
        ),
        Err(_) => PipeOutcome::grpc(
            200,
            velda_core::GrpcStatus::Unavailable,
            bytes_rx,
            0,
            upstream_addr,
            velda_core::CompletionReason::UpstreamError,
        ),
    }
}
