//! HTTP/2 Downstream Ingress Request Entity (RFC 9113).
//!
//! Represents incoming requests initiated by downstream clients across multiplexed streams.

use http::{HeaderMap, Method, Uri, Version};
use velda_core::{Body, L7Request};

/// Header metadata and stream identity for an incoming HTTP/2 request.
#[derive(Debug, Clone)]
pub struct Http2RequestHead {
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

impl Http2RequestHead {
    /// Creates a new [`Http2RequestHead`].
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
}

/// An incoming HTTP/2 request received on an active multiplexed stream.
#[derive(Debug, Clone)]
pub struct Http2Request {
    /// Request head (method, URI, headers, stream ID).
    pub head: Http2RequestHead,
    /// Request body payload received via DATA frame(s).
    pub body: Body,
}

impl Http2Request {
    /// Creates a new [`Http2Request`] from parts.
    #[inline]
    pub fn new(head: Http2RequestHead, body: Body) -> Self {
        Self { head, body }
    }

    /// Deconstructs the request into its constituent head and body parts.
    #[inline]
    pub fn into_parts(self) -> (Http2RequestHead, Body) {
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

    /// Constructs an [`Http2Request`] from a canonical [`L7Request`].
    pub fn from_l7_request(req: L7Request) -> Self {
        let head = Http2RequestHead::new(req.method, req.uri, req.headers, None);
        Self::new(head, req.body)
    }
}
