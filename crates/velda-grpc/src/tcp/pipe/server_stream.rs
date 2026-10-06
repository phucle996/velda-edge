//! Server-streaming gRPC pipe forwarding.
//!
//! Client sends a single request message, and upstream streams back multiple response messages
//! followed by trailers. Data chunks are pumped progressively to downstream without buffering in RAM.
//!
//! Enforces client request `max_message_size` (with early LPM check) and `max_call_duration_ms` timeout.

use bytes::{Bytes, BytesMut};
use http::Version;
use std::net::SocketAddr;
use std::time::Duration;

use crate::config::GrpcConfig;
use crate::error::GrpcError;
use crate::status::GrpcStatus;
use crate::tcp::client::GrpcUpstreamConnector;
use crate::tcp::server::GrpcServerStream;

/// Pipes a server-streaming gRPC call between downstream and upstream.
///
/// Workflow:
/// 1. Consume client request message, validating size early via 5-byte LPM header.
/// 2. Establish connection to upstream backend endpoint with `config` limits.
/// 3. Build and transmit outbound HTTP/2 request frame.
/// 4. Await upstream response headers frame.
/// 5. Progressively stream response data frames and trailers downstream without buffering in RAM.
/// 6. Bound the entire call within `max_call_duration_ms` timeout.
pub async fn pipe_server_stream(
    mut server_stream: GrpcServerStream,
    target: SocketAddr,
    config: &GrpcConfig,
) -> Result<(), GrpcError> {
    let timeout = Duration::from_millis(config.max_call_duration_ms);

    let workflow = async {
        // Step 1: Consume downstream request message and validate early
        let mut req_data = BytesMut::new();
        let mut checked_lpm = false;
        let is_end = server_stream.recv_stream.is_end_stream();

        if !is_end {
            while let Some(chunk_res) = server_stream.recv_stream.data().await {
                let chunk = chunk_res.map_err(GrpcError::H2)?;
                let len = chunk.len();
                req_data.extend_from_slice(&chunk);
                let _ = server_stream
                    .recv_stream
                    .flow_control()
                    .release_capacity(len);

                if !checked_lpm && req_data.len() >= 5 {
                    let declared_len =
                        u32::from_be_bytes([req_data[1], req_data[2], req_data[3], req_data[4]])
                            as usize;
                    if declared_len > config.max_message_size {
                        let _ = server_stream.respond.send_trailers_only(
                            GrpcStatus::ResourceExhausted,
                            Some("request message length exceeds limit"),
                        );
                        return Err(GrpcError::PayloadTooLarge(declared_len));
                    }
                    checked_lpm = true;
                }

                if req_data.len() > config.max_message_size + 5 {
                    let _ = server_stream.respond.send_trailers_only(
                        GrpcStatus::ResourceExhausted,
                        Some("request message size exceeds limit"),
                    );
                    return Err(GrpcError::PayloadTooLarge(req_data.len()));
                }
            }
        }

        // Step 2: Establish upstream connection applying config limits
        let mut connector = match GrpcUpstreamConnector::connect(target, config, None, None).await {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(target = %target, error = %e, "Failed to connect to gRPC upstream");
                let _ = server_stream.respond.send_trailers_only(
                    GrpcStatus::Unavailable,
                    Some(&format!("upstream connect error: {e}")),
                );
                return Err(e);
            }
        };

        // Step 3: Build outbound HTTP/2 request frame from downstream parts
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
        let has_body = !req_data.is_empty();
        let (upstream_resp_fut, mut upstream_send_stream) =
            connector.open_stream(upstream_req, !has_body)?;

        if has_body {
            upstream_send_stream
                .send_data(req_data.freeze(), true)
                .map_err(GrpcError::H2)?;
        }

        // Step 4: Await upstream response headers
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

        // Step 5: Progressively stream data chunks and trailers downstream
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

        Ok(())
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
                "gRPC call timed out",
            )))
        }
    }
}
