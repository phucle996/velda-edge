//! HTTP/2 Upstream Ingress Response Entity (RFC 9113).
//!
//! Represents incoming responses received from upstream backends across multiplexed streams.

use http::header::{HeaderName, HeaderValue};
use http::{HeaderMap, StatusCode, Version};
use velda_core::{Body, L7Response};

/// Header metadata and status code for an incoming HTTP/2 upstream response.
#[derive(Debug, Clone)]
pub struct Http2ResponseHead {
    /// HTTP status code (200, 404, 500, etc.).
    pub status: StatusCode,
    /// Protocol version (always HTTP/2.0).
    pub version: Version,
    /// Decompressed headers received in response HEADERS frame(s).
    pub headers: HeaderMap,
}

impl Http2ResponseHead {
    /// Creates a new [`Http2ResponseHead`].
    #[inline]
    pub fn new(status: StatusCode, headers: HeaderMap) -> Self {
        Self {
            status,
            version: Version::HTTP_2,
            headers,
        }
    }
}

/// An incoming HTTP/2 response received from an upstream backend.
#[derive(Debug, Clone)]
pub struct Http2Response {
    /// Response head (status, headers, version).
    pub head: Http2ResponseHead,
    /// Response body payload received via DATA frame(s).
    pub body: Body,
}

impl Http2Response {
    /// Creates a new [`Http2Response`] from parts.
    #[inline]
    pub fn new(head: Http2ResponseHead, body: Body) -> Self {
        Self { head, body }
    }

    /// Creates an HTTP/2 response with a raw byte body.
    pub fn from_bytes(status: StatusCode, bytes: Vec<u8>) -> Self {
        let head = Http2ResponseHead::new(status, HeaderMap::new());
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
            Http2ResponseHead::new(status, HeaderMap::new()),
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
    pub fn into_parts(self) -> (Http2ResponseHead, Body) {
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

    /// Constructs an [`Http2Response`] from a canonical [`L7Response`].
    pub fn from_l7_response(resp: L7Response) -> Self {
        let head = Http2ResponseHead::new(resp.status, resp.headers);
        Self::new(head, resp.body)
    }
}
