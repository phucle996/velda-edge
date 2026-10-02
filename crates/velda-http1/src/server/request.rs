//! Ingress Domain Model: HTTP/1.1 Request (RFC 9112).
//!
//! Architectural Perspective (Góc nhìn Ingress):
//! The Gateway Server is the receiver and owner of the incoming downstream Request entity.
//! Defines native protocol-owned request models and head metadata, decoupled from generic L7 types.

use http::header::{HeaderName, HeaderValue};
use http::{HeaderMap, Method, Uri, Version};
pub use velda_core::Body;

/// HTTP/1.1 request head metadata (method, URI, version, and headers).
#[derive(Debug, Clone)]
pub struct Http1RequestHead {
    /// HTTP method.
    pub method: Method,
    /// Request URI.
    pub uri: Uri,
    /// HTTP protocol version.
    pub version: Version,
    /// Request headers.
    pub headers: HeaderMap,
}

impl Http1RequestHead {
    /// Creates a new HTTP/1.1 request head.
    #[inline]
    pub fn new(method: Method, uri: Uri, version: Version, headers: HeaderMap) -> Self {
        Self {
            method,
            uri,
            version,
            headers,
        }
    }

    /// Fast-path lookup for the `Host` header.
    #[inline]
    pub fn host(&self) -> Option<&HeaderValue> {
        self.headers.get(http::header::HOST)
    }

    /// Fast-path lookup for request path.
    #[inline]
    pub fn path(&self) -> &str {
        self.uri.path()
    }
}

/// HTTP/1.1 body framing determined during head parsing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Http1BodyFraming {
    /// No body present (e.g. GET/HEAD without payload).
    Empty,
    /// Fixed-length body defined by `Content-Length`.
    ContentLength(usize),
    /// Chunked-encoded body (`Transfer-Encoding: chunked`).
    Chunked,
}

/// Protocol-owned HTTP/1.1 request.
#[derive(Debug, Clone)]
pub struct Http1Request {
    /// Request method.
    pub method: Method,
    /// Request URI.
    pub uri: Uri,
    /// Protocol version.
    pub version: Version,
    /// Request headers.
    pub headers: HeaderMap,
    /// Request payload body.
    pub body: Body,
}

impl Http1Request {
    /// Creates a new HTTP/1.1 request from individual parts.
    #[inline]
    pub fn new(method: Method, uri: Uri, version: Version, headers: HeaderMap, body: Body) -> Self {
        Self {
            method,
            uri,
            version,
            headers,
            body,
        }
    }

    /// Constructs an [`Http1Request`] from a parsed head and decoded body.
    #[inline]
    pub fn from_parts(head: Http1RequestHead, body: Body) -> Self {
        Self {
            method: head.method,
            uri: head.uri,
            version: head.version,
            headers: head.headers,
            body,
        }
    }

    /// Returns a shared reference to the request head metadata.
    #[inline]
    pub fn head(&self) -> Http1RequestHead {
        Http1RequestHead {
            method: self.method.clone(),
            uri: self.uri.clone(),
            version: self.version,
            headers: self.headers.clone(),
        }
    }

    /// Fast-path lookup for request path.
    #[inline]
    pub fn path(&self) -> &str {
        self.uri.path()
    }

    /// Fast-path lookup for the `Host` header.
    #[inline]
    pub fn host(&self) -> Option<&HeaderValue> {
        self.headers.get(http::header::HOST)
    }

    /// Adds a header to the request.
    #[inline]
    pub fn with_header(mut self, name: HeaderName, value: HeaderValue) -> Self {
        self.headers.insert(name, value);
        self
    }
}
