//! Layer 7 gRPC downstream over TCP forwarded to gRPC kernel-bypass RDMA backend.
//!
//! Self-contained pipeline adhering to the Hourglass Architecture & Flat Workflow.

use bytes::Bytes;
use http::Version;
use std::sync::Arc;
use std::time::Duration;

use velda_grpc::tcp::GrpcPipeStrategy;
use velda_grpc::tcp::server::GrpcServerStream;
use velda_grpc::{GrpcConfig, GrpcError, GrpcStatus};

use crate::upstream::grpc::GrpcRdmaUpstream;

/// Serves an inbound gRPC request over TCP forwarded to a gRPC RDMA backend according to its pipe strategy.
pub async fn serve(
    server_stream: GrpcServerStream,
    upstream: &Arc<GrpcRdmaUpstream>,
    config: &GrpcConfig,
) -> Result<(), GrpcError> {
    match upstream.strategy {
        GrpcPipeStrategy::Buffered => serve_buffered(server_stream, upstream, config).await,
        GrpcPipeStrategy::ServerStream => {
            serve_server_stream(server_stream, upstream, config).await
        }
        GrpcPipeStrategy::ClientStream => {
            serve_client_stream(server_stream, upstream, config).await
        }
        GrpcPipeStrategy::Duplex => serve_duplex(server_stream, upstream, config).await,
    }
}

async fn serve_buffered(
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
            tracing::warn!(error = %e, upstream = %upstream.id(), "Failed to acquire upstream gRPC over RDMA connection");
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

async fn serve_server_stream(
    server_stream: GrpcServerStream,
    upstream: &Arc<GrpcRdmaUpstream>,
    config: &GrpcConfig,
) -> Result<(), GrpcError> {
    serve_buffered(server_stream, upstream, config).await
}

async fn serve_client_stream(
    server_stream: GrpcServerStream,
    upstream: &Arc<GrpcRdmaUpstream>,
    config: &GrpcConfig,
) -> Result<(), GrpcError> {
    serve_buffered(server_stream, upstream, config).await
}

async fn serve_duplex(
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
            tracing::warn!(error = %e, upstream = %upstream.id(), "Failed to acquire upstream gRPC over RDMA connection");
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
                let is_last = recv_stream.is_end_stream();
                upstream_send_stream
                    .send_data(data, is_last)
                    .map_err(GrpcError::H2)?;
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
