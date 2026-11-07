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

use bytes::{Bytes, BytesMut};
use http::Version;
use std::sync::Arc;
use std::time::Duration;

use velda_grpc::tcp::server::GrpcServerStream;
use velda_grpc::{GrpcConfig, GrpcError, GrpcStatus};

use crate::upstream::grpc::GrpcRdmaUpstream;

/// Serves a client-streaming gRPC request over TCP forwarded to a gRPC RDMA backend.
pub async fn serve(
    mut server_stream: GrpcServerStream,
    upstream: &Arc<GrpcRdmaUpstream>,
    config: &GrpcConfig,
) -> Result<(), GrpcError> {
    // Step 1: Lease pre-warmed RDMA connection handle from worker core lane shard without cross-thread lock contention.
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
            return Ok(());
        }
    };

    let timeout = Duration::from_millis(config.max_call_duration_ms);

    // Step 2: Execute the client streaming workflow bounded by max_call_duration_ms deadline.
    let workflow = async {
        // Step 3: Synthesize outbound HTTP/2 request frame with finalized ingress headers.
        let mut req_builder = http::Request::builder()
            .method(server_stream.parts.method)
            .uri(server_stream.parts.uri)
            .version(Version::HTTP_2);

        for (name, val) in server_stream.parts.headers.drain() {
            if let Some(name) = name {
                req_builder = req_builder.header(name, val);
            }
        }

        let upstream_req = req_builder.body(()).map_err(GrpcError::Http)?;
        // Open upstream stream without end_stream=true since client chunks follow
        let (upstream_resp_fut, mut upstream_send_stream) =
            client.open_stream(upstream_req, false)?;

        // Step 4: Stream inbound client chunks to upstream RDMA channel with concurrent early-response monitoring.
        // Concurrently awaits upstream_resp_fut so early errors (e.g. 401/403/ResourceExhausted) immediately abort
        // upload and upstream receive loop continuously drains transport credit frames (Fixes Scenario 2).
        tokio::pin!(upstream_resp_fut);
        let mut early_response = None;
        let is_end = server_stream.recv_stream.is_end_stream();

        if !is_end {
            let mut upload_done = false;
            let mut has_trailers = false;

            while !upload_done {
                tokio::select! {
                    biased;
                    // Branch 1: If upstream responds early (e.g. error/trailers), abort upload immediately
                    resp_res = &mut upstream_resp_fut => {
                        let response = resp_res.map_err(GrpcError::H2)?;
                        early_response = Some(response);
                        upload_done = true;
                    }
                    // Branch 2: Stream next client chunk to upstream with flow-control backpressure
                    maybe_chunk = server_stream.recv_stream.data(), if !upload_done => {
                        match maybe_chunk {
                            Some(chunk_res) => {
                                let chunk = chunk_res.map_err(GrpcError::H2)?;
                                let len = chunk.len();
                                upstream_send_stream
                                    .send_data(chunk, false)
                                    .map_err(GrpcError::H2)?;
                                let _ = server_stream
                                    .recv_stream
                                    .flow_control()
                                    .release_capacity(len);
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
                if let Some(trailers) = server_stream
                    .recv_stream
                    .trailers()
                    .await
                    .map_err(GrpcError::H2)?
                {
                    upstream_send_stream
                        .send_trailers(trailers)
                        .map_err(GrpcError::H2)?;
                    has_trailers = true;
                }

                if !has_trailers {
                    upstream_send_stream
                        .send_data(Bytes::new(), true)
                        .map_err(GrpcError::H2)?;
                }
            }
        } else {
            upstream_send_stream
                .send_data(Bytes::new(), true)
                .map_err(GrpcError::H2)?;
        }

        // Step 6: Await upstream response headers if not already captured via early response.
        let response = match early_response {
            Some(resp) => resp,
            None => (&mut upstream_resp_fut).await.map_err(GrpcError::H2)?,
        };
        let (mut resp_parts, mut upstream_recv_stream) = response.into_parts();

        let mut resp_builder = http::Response::builder()
            .status(resp_parts.status)
            .version(Version::HTTP_2);

        for (name, val) in resp_parts.headers.drain() {
            if let Some(name) = name {
                resp_builder = resp_builder.header(name, val);
            }
        }

        let is_end_of_stream = upstream_recv_stream.is_end_stream();
        let resp = resp_builder.body(()).map_err(GrpcError::Http)?;
        let mut downstream_send_stream = server_stream
            .respond
            .send_response(resp, is_end_of_stream)
            .map_err(GrpcError::H2)?;

        // Step 7: Forward single response body chunk from upstream to downstream client.
        if !is_end_of_stream {
            let mut resp_len = 0;
            let mut checked_resp_lpm = false;
            let mut resp_head = BytesMut::new();

            while let Some(chunk_res) = upstream_recv_stream.data().await {
                let chunk = chunk_res.map_err(GrpcError::H2)?;
                let len = chunk.len();
                resp_len += len;

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
                            return Err(GrpcError::PayloadTooLarge(declared_len));
                        }
                        checked_resp_lpm = true;
                    }
                }

                if resp_len > config.max_message_size + 5 {
                    return Err(GrpcError::PayloadTooLarge(resp_len));
                }

                let is_last = upstream_recv_stream.is_end_stream();
                downstream_send_stream
                    .send_data(chunk, is_last)
                    .map_err(GrpcError::H2)?;
                let _ = upstream_recv_stream.flow_control().release_capacity(len);
                if is_last {
                    break;
                }
            }

            // Step 8: Forward upstream response trailers to complete RPC.
            if let Some(trailers) = upstream_recv_stream
                .trailers()
                .await
                .map_err(GrpcError::H2)?
            {
                downstream_send_stream
                    .send_trailers(trailers)
                    .map_err(GrpcError::H2)?;
            }
        }

        Ok::<(), GrpcError>(())
    };

    // Step 9: Catch deadline expiration and emit canonical DeadlineExceeded trailer.
    match tokio::time::timeout(timeout, workflow).await {
        Ok(res) => res,
        Err(_) => {
            let _ = server_stream.respond.send_trailers_only(
                GrpcStatus::DeadlineExceeded,
                Some("gRPC call duration deadline exceeded"),
            );
            Err(GrpcError::Io(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "gRPC RDMA client streaming call timed out",
            )))
        }
    }
}
