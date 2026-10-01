//! Upstream HTTP/2 Stream Response Decoding (RFC 9113).
//!
//! Awaits upstream response headers, manages flow control windows via `release_capacity`,
//! and enforces `max_body_size` limits on the incoming upstream body.

use bytes::BytesMut;
use h2::RecvStream;
use h2::client::ResponseFuture;
use velda_core::{Body, IngressLimits, L7Response};

use super::response::{Http2Response, Http2ResponseHead};
use crate::error::Http2Error;

/// Reads and accumulates the response body from an upstream [`RecvStream`].
pub async fn decode_response_body(
    mut body_stream: RecvStream,
    max_body_size: usize,
) -> Result<Body, Http2Error> {
    let mut body_buf = BytesMut::new();

    while let Some(chunk) = body_stream.data().await {
        let data = chunk?;
        if body_buf.len() + data.len() > max_body_size {
            return Err(Http2Error::PayloadTooLarge(body_buf.len() + data.len()));
        }
        body_buf.extend_from_slice(&data);
        let _ = body_stream.flow_control().release_capacity(data.len());
    }

    if body_buf.is_empty() {
        Ok(Body::Empty)
    } else {
        Ok(Body::Bytes(body_buf.freeze()))
    }
}

/// Awaits upstream response headers and streams the body into an [`Http2Response`].
pub async fn decode_response(
    response_fut: ResponseFuture,
    limits: &IngressLimits,
) -> Result<Http2Response, Http2Error> {
    let response = response_fut.await?;
    let (parts, body_stream) = response.into_parts();
    let body = decode_response_body(body_stream, limits.max_body_size).await?;
    let head = Http2ResponseHead::new(parts.status, parts.headers);
    Ok(Http2Response::new(head, body))
}

/// Awaits upstream response headers and streams the body into a canonical [`L7Response`].
pub async fn decode_l7_response(
    response_fut: ResponseFuture,
    limits: &IngressLimits,
) -> Result<L7Response, Http2Error> {
    let h2_resp = decode_response(response_fut, limits).await?;
    Ok(h2_resp.into_l7_response())
}

/// Awaits upstream response headers and returns an [`Http2ResponseHead`] along with
/// an [`Http2StreamReceiver`] to progressively consume the upstream response body.
pub async fn decode_streaming_response(
    response_fut: ResponseFuture,
    limits: &IngressLimits,
) -> Result<
    (
        Http2ResponseHead,
        crate::server::decode::Http2StreamReceiver,
    ),
    Http2Error,
> {
    let response = response_fut.await?;
    let (parts, body_stream) = response.into_parts();
    let head = Http2ResponseHead::new(parts.status, parts.headers);
    let receiver =
        crate::server::decode::Http2StreamReceiver::new(body_stream, limits.max_body_size);
    Ok((head, receiver))
}
