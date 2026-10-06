//! Progressive client request upload streaming with buffered response.
//!
//! Enforces unidirectional client-to-server streaming:
//! - Downstream request DATA frames are progressively pumped to upstream send stream.
//! - Upstream response headers are awaited and validated (no unexpected SSE).
//! - Upstream response body is accumulated into RAM and sent downstream.

use bytes::{Bytes, BytesMut};
use h2::client::SendRequest;
use http::Version;
use velda_core::{Body, L7Response};

use crate::config::Http2Config;
use crate::error::Http2Error;
use crate::server::{Http2RequestHead, Http2Responder, Http2StreamReceiver};

/// Pipes an incoming streaming request upload to upstream, returning a buffered response downstream.
pub async fn pipe_client_stream(
    mut head: Http2RequestHead,
    mut body_rx: Http2StreamReceiver,
    mut responder: Http2Responder,
    client: &mut SendRequest<Bytes>,
    config: &Http2Config,
) -> Result<(), Http2Error> {
    // 1. Build and sanitize outbound upstream H2 request with end_of_stream = false (zero-clone)
    let is_head = head.method == http::Method::HEAD;
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

    // 5. Accumulate bounded response body while releasing H2 flow-control window
    let is_no_body_status = parts.status.is_informational()
        || parts.status == http::StatusCode::NO_CONTENT
        || parts.status == http::StatusCode::NOT_MODIFIED;

    let resp_body = if is_head || is_no_body_status || body_stream.is_end_stream() {
        Body::Empty
    } else if let Some(first_chunk) = body_stream.data().await {
        let data = first_chunk?;
        let len = data.len();
        if len > config.max_body_size {
            let _ = body_stream.flow_control().release_capacity(len);
            return Err(Http2Error::PayloadTooLarge(len));
        }
        let _ = body_stream.flow_control().release_capacity(len);

        if body_stream.is_end_stream() {
            Body::Bytes(data)
        } else {
            let mut body_buf = BytesMut::with_capacity(len * 2);
            body_buf.extend_from_slice(&data);

            while let Some(chunk) = body_stream.data().await {
                let chunk_data = chunk?;
                let chunk_len = chunk_data.len();
                if body_buf.len() + chunk_len > config.max_body_size {
                    let _ = body_stream.flow_control().release_capacity(chunk_len);
                    return Err(Http2Error::PayloadTooLarge(body_buf.len() + chunk_len));
                }
                body_buf.extend_from_slice(&chunk_data);
                let _ = body_stream.flow_control().release_capacity(chunk_len);
            }

            Body::Bytes(body_buf.freeze())
        }
    } else {
        Body::Empty
    };

    // 6. Send response downstream
    let resp = L7Response::new(parts.status, Version::HTTP_2, parts.headers, resp_body);
    responder.send_response(&resp)?;

    Ok(())
}
