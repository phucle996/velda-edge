//! Layer 7 gRPC over TCP downstream to RDMA upstream Client Streaming Pipeline.
//!
//! Handles Client-side streaming RPCs (N request chunks to 1 response) where downstream
//! requests over TCP are forwarded natively over an upstream RDMA kernel-bypass connection.
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

    let workflow = async {
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
        let (upstream_resp_fut, mut upstream_send_stream) =
            client.open_stream(upstream_req, false)?;

        let is_end = server_stream.recv_stream.is_end_stream();
        if !is_end {
            let mut has_trailers = false;
            while let Some(chunk_res) = server_stream.recv_stream.data().await {
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
        } else {
            upstream_send_stream
                .send_data(Bytes::new(), true)
                .map_err(GrpcError::H2)?;
        }

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
        let mut downstream_send_stream = server_stream
            .respond
            .send_response(resp, is_end_of_stream)
            .map_err(GrpcError::H2)?;

        if !is_end_of_stream {
            let mut resp_len = 0;
            let mut checked_resp_lpm = false;
            let mut resp_head = BytesMut::new();

            while let Some(chunk_res) = upstream_recv_stream.data().await {
                let chunk = chunk_res.map_err(GrpcError::H2)?;
                let len = chunk.len();
                resp_len += len;

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
