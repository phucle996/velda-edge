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
//! 5. **Pipe Pattern Match**: Upstream strategy explicitly configured as `ClientStream` (N request chunks to 1 response).
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
use crate::upstream::grpc::GrpcRdmaUpstream;

/// Serves a client-streaming gRPC request over TCP forwarded to a gRPC RDMA backend.
pub async fn serve(
    mut server_stream: GrpcServerStream,
    upstream: &Arc<GrpcRdmaUpstream>,
    config: &GrpcConfig,
) -> PipeOutcome {
    // Step 1: Lease pre-warmed RDMA connection handle from worker core lane shard without cross-thread lock contention.
    let client = match upstream.acquire(config).await {
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

    // Step 2: Execute the client streaming workflow bounded by max_call_duration_ms deadline.
    let workflow = async {
        let mut bytes_rx: u64 = 0;

        // Step 3: Open outbound HTTP/2 stream directly from HeaderWorkspace.
        let (upstream_resp_fut, mut upstream_send_stream) = client
            .open_stream(
                server_stream.head.method,
                server_stream.head.uri,
                server_stream.head.headers,
                false,
            )
            .await?;

        // Step 4: Stream inbound client chunks to upstream RDMA channel with concurrent early-response monitoring.
        tokio::pin!(upstream_resp_fut);
        let mut early_response = None;
        let is_end = server_stream.receiver.is_end_stream();

        if !is_end {
            let mut upload_done = false;

            while !upload_done {
                tokio::select! {
                    biased;
                    // Branch 1: If upstream responds early (e.g. error/trailers), abort upload immediately
                    resp_res = &mut upstream_resp_fut => {
                        let response = resp_res.map_err(GrpcOutcome::from_http2)?;
                        early_response = Some(response);
                        upload_done = true;
                    }
                    // Branch 2: Stream next client chunk to upstream with flow-control backpressure
                    maybe_chunk = server_stream.receiver.recv_chunk(), if !upload_done => {
                        match maybe_chunk.map_err(GrpcOutcome::from_http2)? {
                            Some(chunk) => {
                                bytes_rx += chunk.len() as u64;
                                upstream_send_stream.send_chunk(chunk, false).await.map_err(GrpcOutcome::from_http2)?;
                            }
                            None => {
                                upload_done = true;
                            }
                        }
                    }
                }
            }

            if early_response.is_none() {
                // Step 5: Finalize upstream request stream with trailers or empty END_STREAM frame.
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
            }
        } else {
            upstream_send_stream
                .send_chunk(Bytes::new(), true)
                .await
                .map_err(GrpcOutcome::from_http2)?;
        }

        // Step 6: Await upstream response headers if not already captured via early response.
        let mut response = match early_response {
            Some(resp) => resp,
            None => (&mut upstream_resp_fut)
                .await
                .map_err(GrpcOutcome::from_http2)?,
        };

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

        // Step 7: Forward single response body chunk from upstream to downstream client.
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

            // Validate LPM header (5 bytes: 1 compression flag + 4 big-endian length)
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

        // Step 8: Forward upstream response trailers to complete RPC.
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

    // Step 9: Catch deadline expiration and emit canonical DeadlineExceeded trailer.
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
