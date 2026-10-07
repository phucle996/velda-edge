//! HTTP/2 Downstream Ingress Request Entity (RFC 9113).
//!
//! Represents incoming requests initiated by downstream clients across multiplexed streams.

use http::{HeaderMap, Method, Uri, Version};
use velda_core::{Body, L7Request};

/// Header metadata and stream identity for an incoming HTTP/2 server request.
#[derive(Debug, Clone)]
pub struct Http2ServerRequestHead {
    /// HTTP method (GET, POST, PUT, etc.).
    pub method: Method,
    /// Target URI / path.
    pub uri: Uri,
    /// Protocol version (always HTTP/2.0).
    pub version: Version,
    /// Decompressed headers received in HEADERS frame(s).
    pub headers: HeaderMap,
    /// Logical HTTP/2 stream identifier.
    pub stream_id: Option<h2::StreamId>,
}

impl Http2ServerRequestHead {
    /// Creates a new [`Http2ServerRequestHead`].
    pub fn new(
        method: Method,
        uri: Uri,
        headers: HeaderMap,
        stream_id: Option<h2::StreamId>,
    ) -> Self {
        Self {
            method,
            uri,
            version: Version::HTTP_2,
            headers,
            stream_id,
        }
    }

    /// Returns the request path.
    #[inline]
    pub fn path(&self) -> &str {
        self.uri.path()
    }

    /// Returns the target host from the `Host` header or URI authority component.
    ///
    /// In HTTP/2, the `:authority` pseudo-header is extracted by `h2` into `Uri::authority()`
    /// and is not present in `HeaderMap`. This method checks both sources.
    #[inline]
    pub fn host(&self) -> Option<&str> {
        self.headers
            .get(http::header::HOST)
            .and_then(|v| v.to_str().ok())
            .or_else(|| self.uri.authority().map(|a| a.as_str()))
    }

    /// Normalizes and secures the HTTP/2 request URI path in-place (RFC 3986 & RFC 9113).
    #[inline]
    pub fn normalize_path(&mut self) -> Result<(), crate::error::Http2Error> {
        super::path::normalize_path(&mut self.uri)
    }

    /// Enriches HTTP/2 request headers with RFC 7239 and standard proxy forwarding metadata.
    #[inline]
    pub fn enrich_forwarded_headers(
        &mut self,
        peer: std::net::SocketAddr,
        local_addr: std::net::SocketAddr,
        is_tls: bool,
    ) {
        super::header::enrich_headers(&mut self.headers, &self.uri, peer, local_addr, is_tls);
    }
}

/// An incoming HTTP/2 server request received on an active multiplexed stream.
#[derive(Debug, Clone)]
pub struct Http2ServerRequest {
    /// Request head (method, URI, headers, stream ID).
    pub head: Http2ServerRequestHead,
    /// Request body payload received via DATA frame(s).
    pub body: Body,
}

impl Http2ServerRequest {
    /// Creates a new [`Http2ServerRequest`] from parts.
    #[inline]
    pub fn new(head: Http2ServerRequestHead, body: Body) -> Self {
        Self { head, body }
    }

    /// Deconstructs the request into its constituent head and body parts.
    #[inline]
    pub fn into_parts(self) -> (Http2ServerRequestHead, Body) {
        (self.head, self.body)
    }

    /// Converts this protocol-owned request into canonical [`L7Request`].
    pub fn into_l7_request(self) -> L7Request {
        L7Request::new(
            self.head.method,
            self.head.uri,
            Version::HTTP_2,
            self.head.headers,
            self.body,
        )
    }

    /// Constructs an [`Http2ServerRequest`] from a canonical [`L7Request`].
    pub fn from_l7_request(req: L7Request) -> Self {
        let head = Http2ServerRequestHead::new(req.method, req.uri, req.headers, None);
        Self::new(head, req.body)
    }
}

/// Progressive stream receiver for an incoming HTTP/2 request or response body.
///
/// Reads DATA chunks incrementally while enforcing flow-control (`release_capacity`)
/// and maximum payload size limits without buffering the entire body into RAM.
#[derive(Debug)]
pub struct Http2StreamReceiver {
    body_stream: h2::RecvStream,
    max_body_size: usize,
    bytes_received: usize,
}

impl Http2StreamReceiver {
    /// Creates a new [`Http2StreamReceiver`] with ingress safety limits.
    #[inline]
    pub fn new(body_stream: h2::RecvStream, max_body_size: usize) -> Self {
        Self {
            body_stream,
            max_body_size,
            bytes_received: 0,
        }
    }

    /// Asynchronously receives the next DATA chunk from the incoming stream.
    pub async fn recv_chunk(&mut self) -> Result<Option<bytes::Bytes>, crate::error::Http2Error> {
        let Some(chunk_res) = self.body_stream.data().await else {
            return Ok(None);
        };

        let chunk = chunk_res?;
        let len = chunk.len();
        self.bytes_received += len;
        if self.bytes_received > self.max_body_size {
            let _ = self.body_stream.flow_control().release_capacity(len);
            return Err(crate::error::Http2Error::PayloadTooLarge(
                self.bytes_received,
            ));
        }

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

    /// Accumulates all remaining chunks into a [`Body`] while enforcing `max_body_size`.
    pub async fn consume_all(&mut self) -> Result<Body, crate::error::Http2Error> {
        if self.is_end_stream() {
            return Ok(Body::Empty);
        }

        let Some(first_chunk) = self.recv_chunk().await? else {
            return Ok(Body::Empty);
        };

        if self.is_end_stream() {
            return Ok(Body::Bytes(first_chunk));
        }

        let mut body_buf = bytes::BytesMut::with_capacity(first_chunk.len() * 2);
        body_buf.extend_from_slice(&first_chunk);

        while let Some(chunk) = self.recv_chunk().await? {
            body_buf.extend_from_slice(&chunk);
        }

        Ok(Body::Bytes(body_buf.freeze()))
    }
}

/// Decodes an incoming multiplexed HTTP/2 request frame into a protocol-owned [`Http2ServerRequest`].
pub async fn decode_request(
    request: http::Request<h2::RecvStream>,
    config: &crate::config::Http2Config,
) -> Result<Http2ServerRequest, crate::error::Http2Error> {
    let (parts, body_stream) = request.into_parts();
    let head = Http2ServerRequestHead::new(parts.method, parts.uri, parts.headers, None);
    let mut receiver = Http2StreamReceiver::new(body_stream, config.max_body_size);
    let body = receiver.consume_all().await?;

    Ok(Http2ServerRequest::new(head, body))
}
