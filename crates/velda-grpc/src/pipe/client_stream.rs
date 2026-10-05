//! Client-streaming gRPC pipe forwarding.
//!
//! Client streams multiple request messages, and upstream responds with a single
//! response message followed by trailers.
//!
//! Enforces upstream response `max_message_size` (with early LPM check) and `max_call_duration_ms` timeout.

use bytes::{Bytes, BytesMut};
use http::Version;
use std::net::SocketAddr;
use std::time::Duration;

use crate::client::GrpcUpstreamConnector;
use crate::config::GrpcConfig;
use crate::error::GrpcError;
use crate::server::GrpcServerStream;
use crate::status::GrpcStatus;

/// Pipes a client-streaming gRPC call between downstream and upstream.
///
/// Workflow:
/// 1. Connect to upstream endpoint with `config` limits.
/// 2. Build and transmit outbound HTTP/2 request frame.
/// 3. Pump progressive stream of client request data chunks and trailers to upstream.
/// 4. Await upstream response headers frame.
/// 5. Stream single response message (enforcing `max_message_size` and early LPM guard) and trailers downstream.
/// 6. Bound the entire call within `max_call_duration_ms` timeout.
pub async fn pipe_client_stream(
    mut server_stream: GrpcServerStream,
    target: SocketAddr,
    config: &GrpcConfig,
) -> Result<(), GrpcError> {
    let timeout = Duration::from_millis(config.max_call_duration_ms);

    let workflow = async {
        // Step 1: Establish upstream connection applying config limits
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

        // Step 2: Build outbound HTTP/2 request frame from downstream parts
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
            connector.open_stream(upstream_req, false)?;

        // Step 3: Pump multiple client request chunks to upstream with backpressure
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

        // Step 4: Await single upstream response headers
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

        // Step 5: Forward response chunks and trailers, enforcing max_message_size
        if !is_end_of_stream {
            let mut has_trailers = false;
            let mut resp_len = 0;
            let mut checked_resp_lpm = false;
            let mut resp_head = BytesMut::new();

            while let Some(chunk_res) = upstream_recv_stream.data().await {
                let chunk = chunk_res.map_err(GrpcError::H2)?;
                let len = chunk.len();
                resp_len += len;

                // Early LPM header check: reject on first 5 bytes if declared length exceeds limit
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
