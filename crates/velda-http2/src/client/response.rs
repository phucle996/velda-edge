//! HTTP/2 Upstream Ingress Response Entity (RFC 9113).
//!
//! Represents incoming responses received from upstream backends across multiplexed streams.

use http::header::{HeaderName, HeaderValue};
use http::{HeaderMap, StatusCode, Version};
use velda_core::{Body, L7Response};

/// Header metadata and status code for an incoming HTTP/2 upstream response.
#[derive(Debug, Clone)]
pub struct Http2ClientResponseHead {
    /// HTTP status code (200, 404, 500, etc.).
    pub status: StatusCode,
    /// Protocol version (always HTTP/2.0).
    pub version: Version,
    /// Decompressed headers received in response HEADERS frame(s).
    pub headers: HeaderMap,
}

impl Http2ClientResponseHead {
    /// Creates a new [`Http2ClientResponseHead`].
    #[inline]
    pub fn new(status: StatusCode, headers: HeaderMap) -> Self {
        Self {
            status,
            version: Version::HTTP_2,
            headers,
        }
    }
}

/// An incoming HTTP/2 client response received from an upstream backend.
#[derive(Debug, Clone)]
pub struct Http2ClientResponse {
    /// Response head (status, headers, version).
    pub head: Http2ClientResponseHead,
    /// Response body payload received via DATA frame(s).
    pub body: Body,
}

impl Http2ClientResponse {
    /// Creates a new [`Http2ClientResponse`] from parts.
    #[inline]
    pub fn new(head: Http2ClientResponseHead, body: Body) -> Self {
        Self { head, body }
    }

    /// Creates an HTTP/2 response with a raw byte body.
    pub fn from_bytes(status: StatusCode, bytes: Vec<u8>) -> Self {
        let head = Http2ClientResponseHead::new(status, HeaderMap::new());
        let body = if bytes.is_empty() {
            Body::Empty
        } else {
            Body::Bytes(bytes.into())
        };
        Self::new(head, body)
    }

    /// Creates an empty HTTP/2 response.
    #[inline]
    pub fn empty(status: StatusCode) -> Self {
        Self::new(
            Http2ClientResponseHead::new(status, HeaderMap::new()),
            Body::Empty,
        )
    }

    /// Appends a header to this response in builder pattern.
    #[inline]
    pub fn with_header(mut self, name: HeaderName, val: HeaderValue) -> Self {
        self.head.headers.insert(name, val);
        self
    }

    /// Returns the HTTP status code.
    #[inline]
    pub fn status(&self) -> StatusCode {
        self.head.status
    }

    /// Returns a reference to the response headers.
    #[inline]
    pub fn headers(&self) -> &HeaderMap {
        &self.head.headers
    }

    /// Returns a mutable reference to the response headers.
    #[inline]
    pub fn headers_mut(&mut self) -> &mut HeaderMap {
        &mut self.head.headers
    }

    /// Returns a reference to the response body.
    #[inline]
    pub fn body(&self) -> &Body {
        &self.body
    }

    /// Returns whether this response has a non-empty body.
    #[inline]
    pub fn has_body(&self) -> bool {
        !self.body.is_empty()
    }

    /// Deconstructs the response into its constituent head and body parts.
    #[inline]
    pub fn into_parts(self) -> (Http2ClientResponseHead, Body) {
        (self.head, self.body)
    }

    /// Converts this protocol-owned response into canonical [`L7Response`].
    pub fn into_l7_response(self) -> L7Response {
        L7Response::new(
            self.head.status,
            Version::HTTP_2,
            self.head.headers,
            self.body,
        )
    }

    /// Constructs an [`Http2ClientResponse`] from a canonical [`L7Response`].
    pub fn from_l7_response(resp: L7Response) -> Self {
        let head = Http2ClientResponseHead::new(resp.status, resp.headers);
        Self::new(head, resp.body)
    }

    /// Progressively consumes and decodes the response body from an incoming stream with flow-control.
    pub async fn decode_body(
        body_stream: &mut h2::RecvStream,
        max_body_size: usize,
    ) -> Result<Body, crate::error::Http2Error> {
        if body_stream.is_end_stream() {
            return Ok(Body::Empty);
        }

        let Some(first_chunk) = body_stream.data().await else {
            return Ok(Body::Empty);
        };

        let data = first_chunk?;
        let len = data.len();
        if len > max_body_size {
            let _ = body_stream.flow_control().release_capacity(len);
            return Err(crate::error::Http2Error::PayloadTooLarge(len));
        }
        let _ = body_stream.flow_control().release_capacity(len);

        if body_stream.is_end_stream() {
            return Ok(Body::Bytes(data));
        }

        let mut body_buf = bytes::BytesMut::with_capacity(len * 2);
        body_buf.extend_from_slice(&data);

        while let Some(chunk) = body_stream.data().await {
            let chunk_data = chunk?;
            let chunk_len = chunk_data.len();
            if body_buf.len() + chunk_len > max_body_size {
                let _ = body_stream.flow_control().release_capacity(chunk_len);
                return Err(crate::error::Http2Error::PayloadTooLarge(
                    body_buf.len() + chunk_len,
                ));
            }
            body_buf.extend_from_slice(&chunk_data);
            let _ = body_stream.flow_control().release_capacity(chunk_len);
        }

        Ok(Body::Bytes(body_buf.freeze()))
    }
}
