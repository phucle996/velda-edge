//! Unary (buffered) gRPC request-response pipe forwarding.
//!
//! Expects a single downstream request message, transmits it to the upstream endpoint,
//! awaits the upstream response, and forwards the response frame and trailers downstream.
//!
//! Enforces `max_message_size` and `max_call_duration_ms` timeout.

use bytes::{Bytes, BytesMut};
use http::Version;
use std::time::Duration;

use crate::config::GrpcConfig;
use crate::error::GrpcError;
use crate::status::GrpcStatus;
use crate::tcp::client::GrpcUpstreamConnector;
use crate::tcp::server::GrpcResponder;

/// Pipes a Unary (non-streaming) gRPC call between downstream responder and upstream connector.
///
/// Uses borrowed parts and pre-buffered wire data to enable 1-shot self-healing retries with ZERO header cloning.
pub async fn pipe_buffered(
    parts: &http::request::Parts,
    req_data: &Bytes,
    respond: &mut GrpcResponder,
    connector: &mut GrpcUpstreamConnector,
    config: &GrpcConfig,
) -> Result<(), GrpcError> {
    let timeout = Duration::from_millis(config.max_call_duration_ms);

    let workflow = async {
        // Step 1: Build outbound HTTP/2 request frame from downstream parts
        let mut req_builder = http::Request::builder()
            .method(&parts.method)
            .uri(&parts.uri)
            .version(Version::HTTP_2);

        for (name, val) in &parts.headers {
            req_builder = req_builder.header(name, val);
        }

        let upstream_req = req_builder.body(()).map_err(GrpcError::Http)?;
        let has_body = !req_data.is_empty();

        // Step 2: Transmit request to upstream
        let (upstream_resp_fut, mut upstream_send_stream) =
            connector.open_stream(upstream_req, !has_body)?;

        if has_body {
            upstream_send_stream
                .send_data(req_data.clone(), true)
                .map_err(GrpcError::H2)?;
        }

        // Step 3: Await upstream response headers
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

        // Step 4: Forward response chunks and trailers downstream
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
            let _ = respond.send_trailers_only(
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
