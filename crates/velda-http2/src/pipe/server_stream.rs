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
use velda_core::Body;

use crate::config::Http2Config;
use crate::error::Http2Error;
use crate::server::{Http2RequestHead, Http2Responder, Http2StreamReceiver};

/// Pipes a server-streaming HTTP/2 request (buffered request, streaming response).
pub async fn pipe_server_stream(
    mut head: Http2RequestHead,
    mut body_rx: Http2StreamReceiver,
    responder: Http2Responder,
    client: &mut SendRequest<Bytes>,
    config: &Http2Config,
) -> Result<(), Http2Error> {
    // 1. Read complete downstream request body into RAM
    let body = body_rx.consume_all().await?;

    // 2. Build and sanitize outbound upstream H2 request (zero-clone)
    crate::headers::sanitize_h2_headers(&mut head.headers);
    let mut builder = http::Request::builder()
        .method(head.method)
        .uri(head.uri)
        .version(Version::HTTP_2);

    for (k, v) in head.headers.drain() {
        if let Some(k) = k {
            builder = builder.header(k, v);
        }
    }
    let http_req = builder
        .body(())
        .map_err(|e| Http2Error::Parse(e.to_string()))?;

    // 3. Transmit HEADERS frame and optional DATA body frame to upstream
    let has_body = !body.is_empty();
    let (response_fut, mut send_stream) = client.send_request(http_req, !has_body)?;
    if let Body::Bytes(b) = body
        && !b.is_empty()
    {
        send_stream.send_data(b, true)?;
    }

    // 4. Await upstream response headers
    let response = response_fut.await?;
    let (parts, mut body_stream) = response.into_parts();

    // 5. Send stream response headers downstream
    let mut sender = responder.send_stream_response(parts.status, &parts.headers)?;

    // 6. Pump upstream chunks downstream with client disconnect detection and flow-control release
    let mut total_bytes = 0;
    while let Some(chunk) = body_stream.data().await {
        let data = chunk?;
        let len = data.len();
        total_bytes += len;
        if total_bytes > config.max_body_size {
            let _ = body_stream.flow_control().release_capacity(len);
            return Err(Http2Error::PayloadTooLarge(total_bytes));
        }

        if sender.send_chunk(data).await.is_err() {
            // Downstream client disconnected mid-stream
            tracing::debug!("Downstream client disconnected during H2 response streaming");
            return Ok(());
        }

        let _ = body_stream.flow_control().release_capacity(len);
    }

    let _ = sender.finish();
    Ok(())
}
