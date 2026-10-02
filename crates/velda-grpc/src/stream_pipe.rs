//! Bidirectional gRPC stream pumping between `composer_parse` and `upstream_connector`.
//!
//! Provides true streaming proxying without buffering message bodies in RAM.
//! Seamlessly handles Unary (1 chiều), Server Streaming, Client Streaming, and Bi-directional Streaming.

use bytes::Bytes;
use http::Version;
use std::net::SocketAddr;

use crate::composer_parse::GrpcServerStream;
use crate::error::GrpcError;
use crate::status::GrpcStatus;
use crate::upstream_connector::GrpcUpstreamConnector;

/// Pipes an active downstream gRPC stream directly to an upstream backend endpoint
/// in bidirectional streaming mode.
pub async fn pipe_grpc_stream(
    server_stream: GrpcServerStream,
    target: SocketAddr,
) -> Result<(), GrpcError> {
    let GrpcServerStream {
        parts,
        mut recv_stream,
        mut respond,
    } = server_stream;

    // 1. Establish upstream connection via upstream_connector
    let mut connector = match GrpcUpstreamConnector::connect(target).await {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(target = %target, error = %e, "Failed to connect to gRPC upstream");
            let mut err_builder = http::Response::builder()
                .status(http::StatusCode::OK)
                .version(Version::HTTP_2)
                .header(http::header::CONTENT_TYPE, "application/grpc");
            let trailers =
                GrpcStatus::Unavailable.to_trailers(Some(&format!("upstream connect error: {e}")));
            for (name, val) in trailers {
                if let Some(name) = name {
                    err_builder = err_builder.header(name, val);
                }
            }
            if let Ok(resp) = err_builder.body(()) {
                let _ = respond.send_response(resp, true);
            }
            return Err(e);
        }
    };

    // 2. Build upstream request from downstream parts
    let mut req_builder = http::Request::builder()
        .method(parts.method)
        .uri(parts.uri)
        .version(Version::HTTP_2);

    for (name, val) in &parts.headers {
        req_builder = req_builder.header(name, val);
    }

    let upstream_req = req_builder.body(()).map_err(GrpcError::Http)?;

    // 3. Initiate upstream request stream
    let (upstream_resp_fut, mut upstream_send_stream) =
        connector.open_stream(upstream_req, false)?;

    // 4. Client -> Upstream stream pump task (handles single frame unary and multi-frame streaming)
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

    // 5. Upstream -> Client stream pump task (handles single response unary and multi-frame streaming)
    let upstream_to_client = async move {
        let response = upstream_resp_fut.await.map_err(GrpcError::H2)?;
        let (resp_parts, mut upstream_recv_stream) = response.into_parts();

        let mut resp_builder = http::Response::builder()
            .status(resp_parts.status)
            .version(Version::HTTP_2);

        for (name, val) in &resp_parts.headers {
            resp_builder = resp_builder.header(name, val);
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

    // 6. Concurrently drive both stream directions
    let (res_up, res_down) = tokio::join!(client_to_upstream, upstream_to_client);
    res_up?;
    res_down?;

    Ok(())
}
