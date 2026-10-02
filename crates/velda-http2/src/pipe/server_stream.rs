//! Progressive server response streaming (SSE, LLM tokens, large downloads).
//!
//! Enforces unidirectional server-to-client streaming:
//! - Downstream request body is read completely into RAM and forwarded to upstream.
//! - Upstream response headers are immediately sent downstream.
//! - Upstream DATA frames are pumped downstream as they arrive, maintaining flow-control.
//! - Cleanly detects downstream client disconnect to abort upstream processing and avoid wasting compute.

use bytes::Bytes;
use h2::client::SendRequest;
use velda_core::Body;

use super::{build_outbound_request, pump_response_stream};
use crate::config::Http2Config;
use crate::error::Http2Error;
use crate::server::{Http2RequestHead, Http2Responder, Http2StreamReceiver};

/// Pipes a server-streaming HTTP/2 request (buffered request, streaming response).
pub async fn pipe_server_stream(
    head: Http2RequestHead,
    mut body_rx: Http2StreamReceiver,
    responder: Http2Responder,
    client: &mut SendRequest<Bytes>,
    config: &Http2Config,
) -> Result<(), Http2Error> {
    // 1. Read complete downstream request body into RAM
    let body = body_rx.consume_all().await?;

    // 2. Build and sanitize outbound upstream H2 request (zero-clone)
    let http_req = build_outbound_request(head)?;

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
    let (parts, body_stream) = response.into_parts();

    // 5. Send stream response headers downstream
    let sender = responder.send_stream_response(parts.status, &parts.headers)?;

    // 6. Pump upstream chunks downstream with client disconnect detection and flow-control release
    pump_response_stream(body_stream, sender, config.max_body_size).await
}
