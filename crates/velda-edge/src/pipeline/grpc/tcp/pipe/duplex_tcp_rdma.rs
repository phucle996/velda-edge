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
//! 5. **Pipe Pattern Match**: Upstream strategy explicitly configured as `Duplex` (Bidirectional streaming).
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
use crate::upstream::grpc::GrpcRdmaUpstream;

/// Serves a bidirectional duplex streaming gRPC request over TCP forwarded to a gRPC RDMA backend.
pub async fn serve(
    server_stream: GrpcServerStream,
    upstream: &Arc<GrpcRdmaUpstream>,
    config: &GrpcConfig,
) -> PipeOutcome {
    // Step 1: Deconstruct downstream stream into request head, receive body stream, and response handle.
    let GrpcServerStream {
        head,
        mut receiver,
        mut respond,
    } = server_stream;

    // Step 2: Lease pre-warmed RDMA connection handle from worker core lane shard.
    let client = match upstream.acquire(config).await {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(
                error = %e,
                upstream = %upstream.id(),
                "Failed to acquire upstream gRPC over RDMA connection"
            );
            let _ = respond
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

    // Step 3: Execute concurrent bidirectional streaming workflow bounded by max_call_duration_ms.
    let workflow = async {
        // Step 4: Open bidirectional HTTP/2 stream over RDMA directly from HeaderWorkspace.
        let (upstream_resp_fut, mut upstream_send_stream) = client
            .open_stream(head.method, head.uri, head.headers, false)
            .await?;

        // Step 5: Task A (Client-to-Upstream Pump) - Forwards client streaming frames to RDMA.
        let upstream_forward = async {
            let mut bytes_rx: u64 = 0;
            if !receiver.is_end_stream() {
                while let Some(data) = receiver
                    .recv_chunk()
                    .await
                    .map_err(GrpcOutcome::from_http2)?
                {
                    bytes_rx += data.len() as u64;
                    upstream_send_stream
                        .send_chunk(data, false)
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

        // Step 6: Task B (Upstream-to-Client Pump) - Awaits headers and streams RDMA responses downstream.
        let downstream_forward = async {
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

            while let Some(data) = response
                .body
                .recv_chunk()
                .await
                .map_err(GrpcOutcome::from_http2)?
            {
                bytes_tx += data.len() as u64;
                downstream_send_stream
                    .send_chunk(data)
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

        // Step 7: Concurrently join both pump tasks; abort if either fails.
        let (bytes_rx, (bytes_tx, grpc_status)) =
            tokio::try_join!(upstream_forward, downstream_forward)?;
        Ok::<_, GrpcOutcome>((bytes_rx, bytes_tx, grpc_status))
    };

    // Step 8: Catch deadline expiration and emit canonical DeadlineExceeded trailer.
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
            let _ = respond.send_trailers_only(
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
