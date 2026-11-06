//! Layer 7 gRPC over TCP downstream to RDMA upstream Buffered Pipeline.
//!
//! Handles standard Unary request-response RPCs where downstream gRPC requests
//! over TCP are forwarded natively over an upstream RDMA kernel-bypass connection.
//!
//! Self-contained pipeline adhering to the Hourglass Architecture & Flat Workflow.

use http::Version;
use std::sync::Arc;
use std::time::Duration;

use velda_grpc::tcp::server::GrpcServerStream;
use velda_grpc::{GrpcConfig, GrpcError, GrpcStatus};

use crate::upstream::grpc::GrpcRdmaUpstream;

/// Serves a buffered gRPC request over TCP forwarded to a gRPC RDMA backend.
pub async fn serve(
    mut server_stream: GrpcServerStream,
    upstream: &Arc<GrpcRdmaUpstream>,
    config: &GrpcConfig,
) -> Result<(), GrpcError> {
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
            return Ok(());
        }
    };

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
    let parts = server_stream.parts;
    let mut respond = server_stream.respond;

    let res = tokio::time::timeout(timeout, async {
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
            client.open_stream(upstream_req, !has_body)?;

        if has_body {
            upstream_send_stream
                .send_data(req_data.clone(), true)
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
        let mut downstream_send_stream = respond
            .send_response(resp, is_end_of_stream)
            .map_err(GrpcError::H2)?;

        if !is_end_of_stream {
            while let Some(chunk) = upstream_recv_stream.data().await {
                let data = chunk.map_err(GrpcError::H2)?;
                let is_last = upstream_recv_stream.is_end_stream();
                downstream_send_stream
                    .send_data(data, is_last)
                    .map_err(GrpcError::H2)?;
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
    })
    .await;

    match res {
        Ok(inner_res) => inner_res,
        Err(_) => {
            let _ = respond.send_trailers_only(
                GrpcStatus::DeadlineExceeded,
                Some("gRPC call duration deadline exceeded"),
            );
            Err(GrpcError::Io(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "gRPC RDMA call timed out",
            )))
        }
    }
}
