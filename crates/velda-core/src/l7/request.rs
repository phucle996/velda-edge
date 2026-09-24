//! Layer 7 HTTP request model.
//!
//! This module contains HTTP semantics used by routing, plugins,
//! authentication, WAF, rate limiting, and upstream processing.

use bytes::Bytes;
use http::{HeaderMap, Method, Uri, Version};

/// HTTP request body.
///
/// The body is represented separately from request metadata so that
/// the transport/parser layer can evolve toward streaming without
/// changing the rest of the request model.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum Body {
    /// No request body.
    #[default]
    Empty,

    /// Body already available in memory (zero-copy reference counted slice).
    Bytes(Bytes),
}

impl Body {
    /// Returns `true` if the body is empty.
    #[inline]
    pub const fn is_empty(&self) -> bool {
        matches!(self, Self::Empty)
    }

    /// Returns the length of the body in bytes.
    #[inline]
    pub fn len(&self) -> usize {
        match self {
            Self::Empty => 0,
            Self::Bytes(b) => b.len(),
        }
    }
}

/// HTTP request processed by the Velda L7 pipeline.
#[derive(Debug, Clone)]
pub struct L7Request {
    /// HTTP method.
    pub method: Method,

    /// Request URI.
    pub uri: Uri,

    /// HTTP protocol version.
    pub version: Version,

    /// Request headers.
    pub headers: HeaderMap,

    /// Request body.
    pub body: Body,
}

impl L7Request {
    /// Creates a new HTTP request.
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

    /// Returns `true` when the request contains a non-empty body.
    #[inline]
    pub fn has_body(&self) -> bool {
        !self.body.is_empty()
    }

    /// Returns the value of a request header.
    #[inline]
    pub fn header(&self, name: &http::header::HeaderName) -> Option<&http::HeaderValue> {
        self.headers.get(name)
    }

    /// Returns the Host header when present.
    #[inline]
    pub fn host(&self) -> Option<&http::HeaderValue> {
        self.headers.get(http::header::HOST)
    }

    /// Returns the request path.
    #[inline]
    pub fn path(&self) -> &str {
        self.uri.path()
    }

    /// Returns the request query string when present.
    #[inline]
    pub fn query(&self) -> Option<&str> {
        self.uri.query()
    }

    /// Returns `true` if the request uses HTTP/2.
    #[inline]
    pub fn is_http2(&self) -> bool {
        self.version == Version::HTTP_2
    }

    /// Returns `true` if the request uses HTTP/1.1.
    #[inline]
    pub fn is_http11(&self) -> bool {
        self.version == Version::HTTP_11
    }
}
