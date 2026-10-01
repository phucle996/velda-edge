//! Upstream HTTP/2 Request Framing and Transmission (RFC 9113).
//!
//! Translates outgoing gateway requests into H2 HEADERS and DATA frames across
//! client send streams.

use bytes::Bytes;
use h2::SendStream;
use h2::client::{ResponseFuture, SendRequest};
use http::{HeaderMap, Method, Request, Uri, Version};
use velda_core::{Body, L7Request};

use crate::error::Http2Error;
use crate::server::request::Http2Request;

/// Encodes and initiates an HTTP/2 request on a client send stream.
pub fn encode_and_send_request(
    client: &mut SendRequest<Bytes>,
    method: Method,
    uri: Uri,
    headers: &HeaderMap,
    body: &Body,
) -> Result<ResponseFuture, Http2Error> {
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .version(Version::HTTP_2);

    for (k, v) in headers {
        builder = builder.header(k, v);
    }

    let has_body = !body.is_empty();
    let is_empty_body = !has_body;
    let http_req = builder
        .body(())
        .map_err(|e| Http2Error::Parse(e.to_string()))?;

    let (response_fut, mut send_stream): (ResponseFuture, SendStream<Bytes>) =
        client.send_request(http_req, is_empty_body)?;

    if let Body::Bytes(b) = body
        && !b.is_empty()
    {
        send_stream.send_data(b.clone(), true)?;
    }

    Ok(response_fut)
}

/// Helper to encode a canonical [`L7Request`] over an H2 client.
#[inline]
pub fn send_l7_request(
    client: &mut SendRequest<Bytes>,
    req: &L7Request,
) -> Result<ResponseFuture, Http2Error> {
    encode_and_send_request(
        client,
        req.method.clone(),
        req.uri.clone(),
        &req.headers,
        &req.body,
    )
}

/// Helper to encode a protocol-owned [`Http2Request`] over an H2 client.
#[inline]
pub fn send_h2_request(
    client: &mut SendRequest<Bytes>,
    req: &Http2Request,
) -> Result<ResponseFuture, Http2Error> {
    encode_and_send_request(
        client,
        req.head.method.clone(),
        req.head.uri.clone(),
        &req.head.headers,
        &req.body,
    )
}

/// Initiates an upstream HTTP/2 streaming request, returning the [`ResponseFuture`]
/// and an [`Http2StreamSender`] handle for progressively writing request DATA chunks.
pub fn start_streaming_request(
    client: &mut SendRequest<Bytes>,
    method: Method,
    uri: Uri,
    headers: &HeaderMap,
) -> Result<(ResponseFuture, crate::server::encode::Http2StreamSender), Http2Error> {
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .version(Version::HTTP_2);

    for (k, v) in headers {
        builder = builder.header(k, v);
    }

    let http_req = builder
        .body(())
        .map_err(|e| Http2Error::Parse(e.to_string()))?;

    // is_empty_body: false keeps the send stream open
    let (response_fut, send_stream) = client.send_request(http_req, false)?;
    Ok((
        response_fut,
        crate::server::encode::Http2StreamSender::new(send_stream),
    ))
}
