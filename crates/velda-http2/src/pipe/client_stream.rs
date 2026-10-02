//! Progressive client request upload streaming with buffered response.
//!
//! Enforces unidirectional client-to-server streaming:
//! - Downstream request DATA frames are progressively pumped to upstream send stream.
//! - Upstream response headers are awaited and validated (no unexpected SSE).
//! - Upstream response body is accumulated into RAM and sent downstream.

use bytes::Bytes;
use h2::client::SendRequest;
use http::Version;
use velda_core::L7Response;

use super::{accumulate_response_body, build_outbound_request};
use crate::config::Http2Config;
use crate::error::Http2Error;
use crate::server::{Http2RequestHead, Http2Responder, Http2StreamReceiver};

/// Pipes an incoming streaming request upload to upstream, returning a buffered response downstream.
pub async fn pipe_client_stream(
    head: Http2RequestHead,
    mut body_rx: Http2StreamReceiver,
    responder: Http2Responder,
    client: &mut SendRequest<Bytes>,
    config: &Http2Config,
) -> Result<(), Http2Error> {
    // 1. Build and sanitize outbound upstream H2 request with end_of_stream = false (zero-clone)
    let http_req = build_outbound_request(head)?;
    let (response_fut, mut send_stream) = client.send_request(http_req, false)?;

    // 2. Progressively pump downstream DATA chunks to upstream send stream
    while let Some(chunk) = body_rx.recv_chunk().await? {
        send_stream.send_data(chunk, false)?;
    }
    // Signal end of request stream to upstream
    send_stream.send_data(Bytes::new(), true)?;

    // 3. Await upstream response headers
    let response = response_fut.await?;
    let (parts, mut body_stream) = response.into_parts();

    // 4. Validate non-streaming response invariant
    if let Some(content_type) = parts.headers.get(http::header::CONTENT_TYPE)
        && let Ok(ct_str) = content_type.to_str()
        && ct_str
            .as_bytes()
            .windows(b"text/event-stream".len())
            .any(|w| w.eq_ignore_ascii_case(b"text/event-stream"))
    {
        return Err(Http2Error::StreamingViolation(
            "Upstream returned text/event-stream (SSE) on client-only streaming route".into(),
        ));
    }

    // 5. Accumulate bounded response body
    let resp_body = accumulate_response_body(&mut body_stream, config.max_body_size).await?;

    // 6. Send response downstream
    let resp = L7Response::new(parts.status, Version::HTTP_2, parts.headers, resp_body);
    responder.send_response(&resp)?;

    Ok(())
}
