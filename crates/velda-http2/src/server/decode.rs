//! Downstream HTTP/2 Stream Decoding and Body Ingestion (RFC 9113).
//!
//! Decodes stream frames, manages HTTP/2 flow control windows via `release_capacity`,
//! and enforces `max_body_size` ingress limits for both buffered and streaming requests.

use bytes::{Bytes, BytesMut};
use h2::RecvStream;
use http::Request;
use velda_core::{Body, IngressLimits};

use super::request::{Http2Request, Http2RequestHead};
use crate::error::Http2Error;

/// Progressive stream receiver for an incoming HTTP/2 request or response body.
///
/// Reads DATA chunks incrementally while enforcing flow-control (`release_capacity`)
/// and maximum payload size limits without buffering the entire body into RAM.
#[derive(Debug)]
pub struct Http2StreamReceiver {
    body_stream: RecvStream,
    max_body_size: usize,
    bytes_received: usize,
}

impl Http2StreamReceiver {
    /// Creates a new [`Http2StreamReceiver`] with ingress safety limits.
    #[inline]
    pub fn new(body_stream: RecvStream, max_body_size: usize) -> Self {
        Self {
            body_stream,
            max_body_size,
            bytes_received: 0,
        }
    }

    /// Asynchronously receives the next DATA chunk from the incoming stream.
    ///
    /// Automatically informs the peer's send window via `release_capacity`
    /// to maintain optimal flow control.
    ///
    /// Returns:
    /// - `Ok(Some(chunk))` when a new DATA chunk is available.
    /// - `Ok(None))` when the peer has signaled `END_STREAM`.
    /// - `Err(Http2Error::PayloadTooLarge)` if accumulated bytes exceed `max_body_size`.
    pub async fn recv_chunk(&mut self) -> Result<Option<Bytes>, Http2Error> {
        let Some(chunk_res) = self.body_stream.data().await else {
            return Ok(None);
        };

        let chunk = chunk_res?;
        let len = chunk.len();
        self.bytes_received += len;
        if self.bytes_received > self.max_body_size {
            let _ = self.body_stream.flow_control().release_capacity(len);
            return Err(Http2Error::PayloadTooLarge(self.bytes_received));
        }

        // Release flow control capacity back to the sender
        let _ = self.body_stream.flow_control().release_capacity(len);
        Ok(Some(chunk))
    }

    /// Returns the total number of body bytes received so far on this stream.
    #[inline]
    pub fn bytes_received(&self) -> usize {
        self.bytes_received
    }

    /// Returns whether the stream has reached the end of stream.
    #[inline]
    pub fn is_end_stream(&self) -> bool {
        self.body_stream.is_end_stream()
    }

    /// Unwraps the underlying [`RecvStream`].
    #[inline]
    pub fn into_inner(self) -> RecvStream {
        self.body_stream
    }
}

/// Reads and accumulates the request body from an active [`RecvStream`].
///
/// Ensures HTTP/2 flow control permits uninterrupted streaming by notifying the peer's
/// send window via `release_capacity`.
pub async fn decode_request_body(
    body_stream: RecvStream,
    max_body_size: usize,
) -> Result<Body, Http2Error> {
    let mut receiver = Http2StreamReceiver::new(body_stream, max_body_size);
    let mut body_buf = BytesMut::new();

    while let Some(chunk) = receiver.recv_chunk().await? {
        body_buf.extend_from_slice(&chunk);
    }

    if body_buf.is_empty() {
        Ok(Body::Empty)
    } else {
        Ok(Body::Bytes(body_buf.freeze()))
    }
}

/// Decodes an incoming H2 stream request into an [`Http2Request`].
pub async fn decode_request(
    request: Request<RecvStream>,
    limits: &IngressLimits,
) -> Result<Http2Request, Http2Error> {
    let (parts, body_stream) = request.into_parts();
    let body = decode_request_body(body_stream, limits.max_body_size).await?;
    let head = Http2RequestHead::new(parts.method, parts.uri, parts.headers, None);
    Ok(Http2Request::new(head, body))
}
