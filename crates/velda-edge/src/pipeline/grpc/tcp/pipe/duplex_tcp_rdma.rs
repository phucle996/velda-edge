//! # Canonical Pipeline Classification: Acceleration Pipe (Hardware Kernel-Bypass)
//!
//! Unlike **Standard Pipes** (`*_tcp_tcp`) that route outbound traffic through the Linux kernel
//! network stack (incurring syscall context switches, socket buffer copies, and OS interrupt overhead),
//! this is an **Acceleration Pipe** utilizing `velda-rdma` to bypass the OS kernel entirely.
//! Data is DMA-transferred directly between host user-space RAM and backend hardware via RoCEv2 / InfiniBand.
//!
//! ### Activation Conditions (Điều kiện kích hoạt)
//! 1. **Upstream Configuration**: Route resolves to an upstream backend configured with `protocol.transport = "rdma"`.
//! 2. **Hardware & Driver Availability**: Host possesses an RDMA-capable NIC (e.g. NVIDIA/Mellanox ConnectX
//!    with RoCEv2/IB fabric via `libibverbs.so.1`) or runs with verified software mock simulation fallback.
//! 3. **Backend Target Protocol**: Target physical endpoint responds to Out-of-Band (OOB) QP handshakes and
//!    supports HTTP/2 binary framing directly over established RDMA Queue Pairs.
//! 4. **Pre-warmed Core Lane State**: Frontline Traffic Worker (`velda-worker-{id}`) leases an established
//!    ready connection in `< 10ns` from the local core lane shard pre-populated by the core-local
//!    Shadow Logistics Worker (`velda-shadow-{id}`, `SCHED_IDLE`).
//! 5. **Pipe Pattern Match**: Upstream strategy explicitly configured as `Duplex` (Bidirectional streaming).
//!
//! Self-contained pipeline adhering to the Hourglass Architecture & Flat Workflow.

use bytes::Bytes;
use http::Version;
use std::sync::Arc;
use std::time::Duration;

use velda_grpc::tcp::server::GrpcServerStream;
use velda_grpc::{GrpcConfig, GrpcError, GrpcStatus};

use crate::upstream::grpc::GrpcRdmaUpstream;

/// Serves a bidirectional duplex streaming gRPC request over TCP forwarded to a gRPC RDMA backend.
pub async fn serve(
    server_stream: GrpcServerStream,
    upstream: &Arc<GrpcRdmaUpstream>,
    config: &GrpcConfig,
) -> Result<(), GrpcError> {
    let GrpcServerStream {
        parts,
        mut recv_stream,
        mut respond,
    } = server_stream;

    let mut client = match upstream.acquire(config).await {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(
                error = %e,
                upstream = %upstream.id(),
                "Failed to acquire upstream gRPC over RDMA connection"
            );
            let _ = respond
                .send_trailers_only(GrpcStatus::Unavailable, Some("rdma upstream unavailable"));
            return Ok(());
        }
    };

    let timeout = Duration::from_millis(config.max_call_duration_ms);

    let workflow = async {
        let mut req_builder = http::Request::builder()
            .method(parts.method)
            .uri(parts.uri)
            .version(Version::HTTP_2);

        for (name, val) in &parts.headers {
            req_builder = req_builder.header(name, val);
        }

        let upstream_req = req_builder.body(()).map_err(GrpcError::Http)?;
        let (upstream_resp_fut, mut upstream_send_stream) =
            client.open_stream(upstream_req, false)?;

        let upstream_forward = async {
            while let Some(chunk) = recv_stream.data().await {
                let data = chunk.map_err(GrpcError::H2)?;
                let len = data.len();
                let is_last = recv_stream.is_end_stream();
                upstream_send_stream
                    .send_data(data, is_last)
                    .map_err(GrpcError::H2)?;
                let _ = recv_stream.flow_control().release_capacity(len);
                if is_last {
                    return Ok::<(), GrpcError>(());
                }
            }
            if let Some(trailers) = recv_stream.trailers().await.map_err(GrpcError::H2)? {
                upstream_send_stream
                    .send_trailers(trailers)
                    .map_err(GrpcError::H2)?;
            } else {
                upstream_send_stream
                    .send_data(Bytes::new(), true)
                    .map_err(GrpcError::H2)?;
            }
            Ok(())
        };

        let downstream_forward = async {
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

            let is_end_of_stream = upstream_recv_stream.is_end_stream();
            let resp = resp_builder.body(()).map_err(GrpcError::Http)?;
            let mut downstream_send_stream = respond
                .send_response(resp, is_end_of_stream)
                .map_err(GrpcError::H2)?;

            if !is_end_of_stream {
                while let Some(chunk) = upstream_recv_stream.data().await {
                    let data = chunk.map_err(GrpcError::H2)?;
                    let len = data.len();
                    let is_last = upstream_recv_stream.is_end_stream();
                    downstream_send_stream
                        .send_data(data, is_last)
                        .map_err(GrpcError::H2)?;
                    let _ = upstream_recv_stream.flow_control().release_capacity(len);
                    if is_last {
                        break;
                    }
                }
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
            Ok(())
        };

        tokio::try_join!(upstream_forward, downstream_forward)?;
        Ok::<(), GrpcError>(())
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
                "gRPC RDMA duplex call timed out",
            )))
        }
    }
}
