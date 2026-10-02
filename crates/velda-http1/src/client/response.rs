//! Ingress Domain Model: HTTP/1.1 Response (RFC 9112).
//!
//! Architectural Perspective (Góc nhìn Ingress):
//! The Gateway Client is the receiver and owner of the incoming upstream Response entity.
//! Defines native protocol-owned response models and head metadata, decoupled from generic L7 types.

use http::header::{HeaderName, HeaderValue};
use http::{HeaderMap, StatusCode, Version};
pub use velda_core::Body;

/// HTTP/1.1 response head metadata (status code, version, and headers).
#[derive(Debug, Clone)]
pub struct Http1ResponseHead {
    /// HTTP status code.
    pub status: StatusCode,
    /// HTTP protocol version.
    pub version: Version,
    /// Response headers.
    pub headers: HeaderMap,
}

impl Http1ResponseHead {
    /// Creates a new HTTP/1.1 response head.
    #[inline]
    pub fn new(status: StatusCode, version: Version, headers: HeaderMap) -> Self {
        Self {
            status,
            version,
            headers,
        }
    }

    /// Fast-path lookup for content-length.
    #[inline]
    pub fn content_length(&self) -> Option<usize> {
        self.headers
            .get(http::header::CONTENT_LENGTH)
            .and_then(|val| val.to_str().ok())
            .and_then(|s| s.parse().ok())
    }

    /// Returns `true` if response status indicates connection close or `Connection: close` is present.
    #[inline]
    pub fn is_close(&self) -> bool {
        self.headers
            .get(http::header::CONNECTION)
            .and_then(|h| h.to_str().ok())
            .is_some_and(|s| s.eq_ignore_ascii_case("close"))
    }
}

/// Protocol-owned HTTP/1.1 response.
#[derive(Debug, Clone)]
pub struct Http1Response {
    /// Status code.
    pub status: StatusCode,
    /// Protocol version.
    pub version: Version,
    /// Response headers.
    pub headers: HeaderMap,
    /// Response payload body.
    pub body: Body,
}

impl Http1Response {
    /// Creates a new HTTP/1.1 response from individual parts.
    #[inline]
    pub fn new(status: StatusCode, version: Version, headers: HeaderMap, body: Body) -> Self {
        Self {
            status,
            version,
            headers,
            body,
        }
    }

    /// Constructs an [`Http1Response`] from a parsed head and decoded body.
    #[inline]
    pub fn from_parts(head: Http1ResponseHead, body: Body) -> Self {
        Self {
            status: head.status,
            version: head.version,
            headers: head.headers,
            body,
        }
    }

    /// Constructs an HTTP/1.1 response from status and raw byte payload.
    #[inline]
    pub fn from_bytes(status: StatusCode, bytes: Vec<u8>) -> Self {
        let body = if bytes.is_empty() {
            Body::Empty
        } else {
            Body::Bytes(bytes.into())
        };
        Self {
            status,
            version: Version::HTTP_11,
            headers: HeaderMap::new(),
            body,
        }
    }

    /// Returns a shared reference to the response head metadata.
    #[inline]
    pub fn head(&self) -> Http1ResponseHead {
        Http1ResponseHead {
            status: self.status,
            version: self.version,
            headers: self.headers.clone(),
        }
    }

    /// Adds a header to the response.
    #[inline]
    pub fn with_header(mut self, name: HeaderName, value: HeaderValue) -> Self {
        self.headers.insert(name, value);
        self
    }
}
