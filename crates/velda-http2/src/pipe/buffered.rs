//! Pure in-memory buffered HTTP/2 request-response forwarding.
//!
//! Enforces non-streaming invariants:
//! - Request body is read completely into RAM with limit enforcement.
//! - Request is transmitted to upstream over an active H2 client send stream.
//! - Upstream response headers are inspected: if upstream returns `text/event-stream` (SSE)
//!   when server streaming is disabled on this listener, returns [`Http2Error::StreamingViolation`].
//! - Upstream response body is accumulated into RAM with flow-control (`release_capacity`) and limits.
//! - Full response is sent downstream.

use bytes::{Bytes, BytesMut};
use h2::client::SendRequest;
use http::Version;
use velda_core::{Body, L7Response};

use crate::config::Http2Config;
use crate::error::Http2Error;
use crate::server::{Http2Responder, Http2ServerRequestHead};

/// Pipes a non-streaming HTTP/2 request between downstream responder and upstream client.
///
/// Uses borrowed head and body to eliminate heap allocations on hot path and enable transparent self-healing.
pub async fn pipe_buffered(
    head: &Http2ServerRequestHead,
    body: &Body,
    responder: &mut Http2Responder,
    client: &mut SendRequest<Bytes>,
    config: &Http2Config,
) -> Result<(), Http2Error> {
    // 1. Build outbound upstream H2 request from borrowed head
    let is_head = head.method == http::Method::HEAD;
    let mut http_req = http::Request::new(());
    *http_req.method_mut() = head.method.clone();
    *http_req.uri_mut() = head.uri.clone();
    *http_req.version_mut() = Version::HTTP_2;
    *http_req.headers_mut() = head.headers.clone();

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

    // 5. Enforce non-streaming invariant: reject SSE if listener has server streaming disabled
    if let Some(content_type) = parts.headers.get(http::header::CONTENT_TYPE)
        && let Ok(ct_str) = content_type.to_str()
        && ct_str
            .as_bytes()
            .windows(b"text/event-stream".len())
            .any(|w| w.eq_ignore_ascii_case(b"text/event-stream"))
    {
        return Err(Http2Error::StreamingViolation(
            "Upstream returned text/event-stream (SSE), but server streaming is disabled on this listener".into(),
        ));
    }

    // 6. Accumulate bounded response body with batched H2 flow-control release
    let is_no_body_status = parts.status.is_informational()
        || parts.status == http::StatusCode::NO_CONTENT
        || parts.status == http::StatusCode::NOT_MODIFIED;

    let batch_threshold = (config.initial_stream_window_size as usize / 4).clamp(32_768, 262_144);

    let resp_body = if is_head || is_no_body_status || body_stream.is_end_stream() {
        Body::Empty
    } else if let Some(first_chunk) = body_stream.data().await {
        let data = first_chunk?;
        let len = data.len();
        if len > config.max_body_size {
            let _ = body_stream.flow_control().release_capacity(len);
            return Err(Http2Error::PayloadTooLarge(len));
        }

        if body_stream.is_end_stream() {
            let _ = body_stream.flow_control().release_capacity(len);
            Body::Bytes(data)
        } else {
            let mut body_buf = BytesMut::with_capacity(len * 2);
            body_buf.extend_from_slice(&data);
            let mut unreleased_bytes = len;

            while let Some(chunk) = body_stream.data().await {
                let chunk_data = chunk?;
                let chunk_len = chunk_data.len();
                if body_buf.len() + chunk_len > config.max_body_size {
                    let _ = body_stream
                        .flow_control()
                        .release_capacity(unreleased_bytes + chunk_len);
                    return Err(Http2Error::PayloadTooLarge(body_buf.len() + chunk_len));
                }
                body_buf.extend_from_slice(&chunk_data);
                unreleased_bytes += chunk_len;

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

            Body::Bytes(body_buf.freeze())
        }
    } else {
        Body::Empty
    };

    // 7. Send full response downstream
    let resp = L7Response::new(parts.status, Version::HTTP_2, parts.headers, resp_body);
    responder.send_response(&resp)?;

    Ok(())
}
