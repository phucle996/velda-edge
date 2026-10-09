use std::net::SocketAddr;

use http::{HeaderMap, Uri};
use velda_core::L7Request;

/// Downstream HTTP/3 request head metadata.
#[derive(Debug, Clone)]
pub struct Http3ServerRequestHead {
    /// HTTP method.
    pub method: http::Method,
    /// Request URI.
    pub uri: Uri,
    /// Protocol version (HTTP/3.0).
    pub version: http::Version,
    /// Request headers.
    pub headers: HeaderMap,
}

impl Http3ServerRequestHead {
    /// Creates a new [`Http3ServerRequestHead`].
    #[inline]
    pub fn new(method: http::Method, uri: Uri, headers: HeaderMap) -> Self {
        Self {
            method,
            uri,
            version: http::Version::HTTP_3,
            headers,
        }
    }

    /// Enriches proxy forwarding headers.
    #[inline]
    pub fn enrich_forwarded_headers(&mut self, peer: SocketAddr, local_addr: SocketAddr) {
        super::header::enrich_headers(&mut self.headers, &self.uri, peer, local_addr);
    }
}

/// Downstream protocol-owned HTTP/3 request.
#[derive(Debug, Clone)]
pub struct Http3ServerRequest {
    /// Request head metadata.
    pub head: Http3ServerRequestHead,
    /// Payload body.
    pub body: velda_core::Body,
}

impl Http3ServerRequest {
    /// Creates a new [`Http3ServerRequest`].
    #[inline]
    pub fn new(head: Http3ServerRequestHead, body: velda_core::Body) -> Self {
        Self { head, body }
    }

    /// Converts into canonical [`L7Request`].
    pub fn into_l7_request(self) -> L7Request {
        L7Request::new(
            self.head.method,
            self.head.uri,
            self.head.version,
            self.head.headers,
            self.body,
        )
    }

    /// Constructs from canonical [`L7Request`].
    pub fn from_l7_request(req: L7Request) -> Self {
        let head = Http3ServerRequestHead::new(req.method, req.uri, req.headers);
        Self::new(head, req.body)
    }
}
