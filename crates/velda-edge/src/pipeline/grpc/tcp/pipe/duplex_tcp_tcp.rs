//! Layer 7 gRPC over TCP to TCP Duplex Pipeline.
//!
//! Handles full-duplex bidirectional streaming RPCs forwarded natively over upstream HTTP/2 binary framing connections.
//!
//! Self-contained pipeline adhering to the Hourglass Architecture & Flat Workflow.

use bytes::Bytes;
use http::Version;
use std::sync::Arc;
use std::time::Duration;

use velda_grpc::tcp::server::GrpcServerStream;
use velda_grpc::{GrpcConfig, GrpcError, GrpcStatus};

use crate::upstream::GrpcTcpUpstream;

/// Serves a full-duplex bidirectional gRPC request over TCP forwarded to a gRPC TCP backend.
pub async fn serve(
    mut server_stream: GrpcServerStream,
    upstream: &Arc<GrpcTcpUpstream>,
    config: &GrpcConfig,
) -> Result<(), GrpcError> {
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
            return Ok(());
        }
    };

    let timeout = Duration::from_millis(config.max_call_duration_ms);

    let workflow = async {
        let GrpcServerStream {
            parts,
            mut recv_stream,
            mut respond,
        } = server_stream;

        // Step 1: Build outbound HTTP/2 request frame from downstream parts
        let mut req_builder = http::Request::builder()
            .method(parts.method)
            .uri(parts.uri)
            .version(Version::HTTP_2);

        for (name, val) in parts.headers {
            if let Some(name) = name {
                req_builder = req_builder.header(name, val);
            }
        }

        let upstream_req = req_builder.body(()).map_err(GrpcError::Http)?;

        // Step 2: Initiate upstream request stream
        let (upstream_resp_fut, mut upstream_send_stream) =
            client.open_stream(upstream_req, false)?;

        // Step 3: Downstream -> Upstream pump task
        let client_to_upstream = async move {
            let is_end = recv_stream.is_end_stream();
            if !is_end {
                let mut has_trailers = false;
                while let Some(chunk_res) = recv_stream.data().await {
                    let chunk = chunk_res.map_err(GrpcError::H2)?;
                    let len = chunk.len();
                    upstream_send_stream
                        .send_data(chunk, false)
                        .map_err(GrpcError::H2)?;
                    let _ = recv_stream.flow_control().release_capacity(len);
                }

                if let Some(trailers) = recv_stream.trailers().await.map_err(GrpcError::H2)? {
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

            Ok::<(), GrpcError>(())
        };

        // Step 4: Upstream -> Downstream pump task
        let upstream_to_client = async move {
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
                let mut has_trailers = false;
                while let Some(chunk_res) = upstream_recv_stream.data().await {
                    let chunk = chunk_res.map_err(GrpcError::H2)?;
                    let len = chunk.len();
                    downstream_send_stream
                        .send_data(chunk, false)
                        .map_err(GrpcError::H2)?;
                    let _ = upstream_recv_stream.flow_control().release_capacity(len);
                }

                if let Some(trailers) = upstream_recv_stream
                    .trailers()
                    .await
                    .map_err(GrpcError::H2)?
                {
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

            Ok::<(), GrpcError>(())
        };

        // Step 5: Concurrently drive both stream directions
        tokio::try_join!(client_to_upstream, upstream_to_client)?;

        Ok(())
    };

    match tokio::time::timeout(timeout, workflow).await {
        Ok(res) => res,
        Err(_) => Err(GrpcError::Io(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "gRPC duplex streaming session timed out",
        ))),
    }
}
