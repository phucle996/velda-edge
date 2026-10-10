//! Progressive server response streaming (SSE, LLM tokens, large downloads).
//!
//! Enforces unidirectional server-to-client streaming:
//! - Downstream request body is read completely into RAM and forwarded to upstream.
//! - Upstream response headers are immediately sent downstream.
//! - Upstream DATA frames are pumped downstream as they arrive, maintaining flow-control.
//! - Cleanly detects downstream client disconnect to abort upstream processing and avoid wasting compute.

use bytes::Bytes;
use h2::client::SendRequest;
use http::Version;
use velda_core::{Body, L7Response};

use crate::config::Http2Config;
use crate::error::Http2Error;
use crate::server::{Http2Responder, Http2ServerRequestHead};

/// Pipes a server-streaming HTTP/2 request (buffered request, streaming response).
///
/// Uses borrowed head and body to eliminate heap allocations on hot path and enable transparent self-healing.
pub async fn pipe_server_stream(
    head: &Http2ServerRequestHead,
    body: &Body,
    responder: &mut Http2Responder,
    client: &mut SendRequest<Bytes>,
    config: &Http2Config,
) -> Result<(), Http2Error> {
    // 1. Build outbound upstream H2 request from borrowed head
    let is_head = head.method == http::Method::HEAD;
    let mut builder = http::Request::builder()
        .method(&head.method)
        .uri(&head.uri)
        .version(Version::HTTP_2);

    for (k, v) in &head.headers {
        builder = builder.header(k, v);
    }
    let http_req = builder
        .body(())
        .map_err(|e| Http2Error::Parse(e.to_string()))?;

    // 2. Transmit HEADERS frame and optional DATA body frame to upstream
    let has_body = !body.is_empty();
    let (response_fut, mut send_stream) = client.send_request(http_req, !has_body)?;
    if let Body::Bytes(b) = body
        && !b.is_empty()
    {
        send_stream.send_data(b.clone(), true)?;
    }

    // 4. Await upstream response headers
    let response = response_fut.await?;
    let (parts, mut body_stream) = response.into_parts();

    // 5. Send stream response headers downstream
    let is_no_body_status = parts.status.is_informational()
        || parts.status == http::StatusCode::NO_CONTENT
        || parts.status == http::StatusCode::NOT_MODIFIED;

    if is_head || is_no_body_status {
        let resp = L7Response::new(parts.status, Version::HTTP_2, parts.headers, Body::Empty);
        responder.send_response(&resp)?;
        return Ok(());
    }

    let mut sender = responder.send_stream_response(parts.status, &parts.headers)?;

    // 6. Pump upstream chunks downstream with batched flow-control release and disconnect detection
    let batch_threshold = (config.initial_stream_window_size as usize / 4).clamp(32_768, 262_144);
    let mut total_bytes = 0;
    let mut unreleased_bytes = 0;

    while let Some(chunk) = body_stream.data().await {
        let data = chunk?;
        let len = data.len();
        total_bytes += len;
        unreleased_bytes += len;

        if total_bytes > config.max_body_size {
            let _ = body_stream
                .flow_control()
                .release_capacity(unreleased_bytes);
            return Err(Http2Error::PayloadTooLarge(total_bytes));
        }

        if sender.send_chunk(data).await.is_err() {
            // Downstream client disconnected mid-stream
            tracing::debug!("Downstream client disconnected during H2 response streaming");
            let _ = body_stream
                .flow_control()
                .release_capacity(unreleased_bytes);
            return Ok(());
        }

        if unreleased_bytes >= batch_threshold {
            let _ = body_stream
                .flow_control()
                .release_capacity(unreleased_bytes);
            unreleased_bytes = 0;
        }
    }

    if unreleased_bytes > 0 {
        let _ = body_stream
            .flow_control()
            .release_capacity(unreleased_bytes);
    }

    let _ = sender.finish();
    Ok(())
}
