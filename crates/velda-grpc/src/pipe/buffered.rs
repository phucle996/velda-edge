//! Unary (buffered) gRPC request-response pipe forwarding.
//!
//! Expects a single downstream request message, transmits it to the upstream endpoint,
//! awaits the upstream response, and forwards the response frame and trailers downstream.
//!
//! Enforces `max_message_size` (with early LPM length check) and `max_call_duration_ms` timeout.

use bytes::{Bytes, BytesMut};
use http::Version;
use std::net::SocketAddr;
use std::time::Duration;

use crate::client::GrpcUpstreamConnector;
use crate::config::GrpcConfig;
use crate::error::GrpcError;
use crate::server::GrpcServerStream;
use crate::status::GrpcStatus;

/// Pipes a Unary (non-streaming) gRPC call between downstream and upstream.
///
/// Workflow:
/// 1. Consume downstream request body (single message), validating early via 5-byte LPM header.
/// 2. Establish connection to upstream backend endpoint with `config` limits.
/// 3. Build and transmit outbound HTTP/2 request frame.
/// 4. Await upstream response headers.
/// 5. Stream response message and trailers downstream, enforcing `max_message_size`.
/// 6. Bound the entire call within `max_call_duration_ms` timeout.
pub async fn pipe_buffered(
    mut server_stream: GrpcServerStream,
    target: SocketAddr,
    config: &GrpcConfig,
) -> Result<(), GrpcError> {
    let timeout = Duration::from_millis(config.max_call_duration_ms);

    let workflow = async {
        // Step 1: Consume downstream request body with early LPM size verification
        let req_data = if server_stream.recv_stream.is_end_stream() {
            Bytes::new()
        } else if let Some(first_chunk_res) = server_stream.recv_stream.data().await {
            let chunk = first_chunk_res.map_err(GrpcError::H2)?;
            let len = chunk.len();
            let _ = server_stream
                .recv_stream
                .flow_control()
                .release_capacity(len);

            if len >= 5 {
                let declared_len =
                    u32::from_be_bytes([chunk[1], chunk[2], chunk[3], chunk[4]]) as usize;
                if declared_len > config.max_message_size {
                    let _ = server_stream.respond.send_trailers_only(
                        GrpcStatus::ResourceExhausted,
                        Some("request message length exceeds limit"),
                    );
                    return Err(GrpcError::PayloadTooLarge(declared_len));
                }
            }

            if server_stream.recv_stream.is_end_stream() {
                // Zero-copy, zero-allocation fast path for single-chunk requests
                chunk
            } else {
                let mut buf = BytesMut::with_capacity(len * 2);
                buf.extend_from_slice(&chunk);
                let mut checked_lpm = len >= 5;

                while let Some(chunk_res) = server_stream.recv_stream.data().await {
                    let chunk = chunk_res.map_err(GrpcError::H2)?;
                    let len = chunk.len();
                    buf.extend_from_slice(&chunk);
                    let _ = server_stream
                        .recv_stream
                        .flow_control()
                        .release_capacity(len);

                    if !checked_lpm && buf.len() >= 5 {
                        let declared_len =
                            u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
                        if declared_len > config.max_message_size {
                            let _ = server_stream.respond.send_trailers_only(
                                GrpcStatus::ResourceExhausted,
                                Some("request message length exceeds limit"),
                            );
                            return Err(GrpcError::PayloadTooLarge(declared_len));
                        }
                        checked_lpm = true;
                    }

                    if buf.len() > config.max_message_size + 5 {
                        let _ = server_stream.respond.send_trailers_only(
                            GrpcStatus::ResourceExhausted,
                            Some("request message size exceeds limit"),
                        );
                        return Err(GrpcError::PayloadTooLarge(buf.len()));
                    }
                }
                buf.freeze()
            }
        } else {
            Bytes::new()
        };

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

        // Step 4: Transmit request to upstream
        let (upstream_resp_fut, mut upstream_send_stream) =
            connector.open_stream(upstream_req, !has_body)?;

        if has_body {
            upstream_send_stream
                .send_data(req_data, true)
                .map_err(GrpcError::H2)?;
        }

        // Step 5: Await upstream response headers
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

        // Step 6: Forward response chunks and trailers downstream
        if !is_end_of_stream {
            let mut has_trailers = false;
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
