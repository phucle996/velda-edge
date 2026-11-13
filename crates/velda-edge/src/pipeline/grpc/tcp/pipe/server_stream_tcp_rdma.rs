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

use bytes::Bytes;
use http::Version;
use std::sync::Arc;
use std::time::Duration;

use velda_grpc::tcp::client::GrpcUpstreamConnector;
use velda_grpc::tcp::server::{GrpcResponder, GrpcServerStream};
use velda_grpc::{GrpcConfig, GrpcError, GrpcStatus};

use crate::pipeline::PipeOutcome;
use crate::upstream::grpc::GrpcRdmaUpstream;

/// Pipes a server-streaming gRPC call over RDMA between downstream responder and upstream connector.
async fn pipe_server_stream_wire(
    parts: &http::request::Parts,
    req_data: &Bytes,
    respond: &mut GrpcResponder,
    connector: &mut GrpcUpstreamConnector,
    config: &GrpcConfig,
) -> Result<(u64, velda_observability::LogGrpcStatus), GrpcError> {
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

        // Step 2: Build outbound HTTP/2 request frame from downstream parts.
        let mut req_builder = http::Request::builder()
            .method(&parts.method)
            .uri(&parts.uri)
            .version(Version::HTTP_2);

        for (name, val) in &parts.headers {
            req_builder = req_builder.header(name, val);
        }

        let upstream_req = req_builder.body(()).map_err(GrpcError::Http)?;
        let has_body = !req_data.is_empty();
        let (upstream_resp_fut, mut upstream_send_stream) =
            connector.open_stream(upstream_req, !has_body)?;

        if has_body {
            upstream_send_stream
                .send_data(req_data.clone(), true)
                .map_err(GrpcError::H2)?;
        }

        // Step 3: Await upstream response headers over RDMA.
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

        // Step 4: Progressively stream data chunks and trailers downstream.
        if !is_end_of_stream {
            let mut has_trailers = false;
            while let Some(chunk_res) = upstream_recv_stream.data().await {
                let chunk = chunk_res.map_err(GrpcError::H2)?;
                let len = chunk.len();
                resp_len += len;
                downstream_send_stream
                    .send_data(chunk, false)
                    .map_err(GrpcError::H2)?;
                // Replenish HTTP/2 receive stream window
                let _ = upstream_recv_stream.flow_control().release_capacity(len);
            }

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
                velda_observability::LogGrpcStatus::ResourceExhausted,
                0,
                0,
                None,
                velda_observability::LogCompletionReason::Rejected,
            );
        }
    };
    let bytes_rx = req_data.len() as u64;

    // Stage 2: Lease pre-warmed RDMA connection handle from worker core lane shard without cross-thread lock contention.
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
                velda_observability::LogGrpcStatus::Unavailable,
                bytes_rx,
                0,
                None,
                velda_observability::LogCompletionReason::UpstreamError,
            );
        }
    };
    let mut upstream_addr = client.target();

    // Stage 3: Forward server-streaming call over HTTP/2 RDMA wire protocol.
    let mut res = pipe_server_stream_wire(
        &server_stream.parts,
        &req_data,
        &mut server_stream.respond,
        &mut client,
        config,
    )
    .await;

    // Stage 4: Self-Healing Retry (RFC 9113 §8.1.4):
    // If upstream connection dropped or remote peer returned REFUSED_STREAM before
    // stream frames were sent, retry transparently 1-shot on a fresh connection.
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
