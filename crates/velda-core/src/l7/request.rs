//! Layer 7 HTTP request model.
//!
//! This module contains HTTP semantics used by routing, plugins,
//! authentication, WAF, rate limiting, and upstream processing.

use std::sync::Arc;

use bytes::Bytes;
use http::{Method, Uri, Version};

use crate::buffer::{HeaderWorkspace, PayloadSlabLease};

/// HTTP request and response body.
///
/// Supports pre-warmed pooled slabs (zero allocation, zero minor page fault),
/// reference-counted byte slices, and empty payloads.
#[derive(Clone)]
pub enum Body {
    /// No request body.
    Empty,

    /// Body backed by a pre-warmed physical slab leased from [`crate::buffer::PayloadSlabPool`].
    /// Automatically recycles back to the pool on final drop.
    Slab {
        lease: Arc<PayloadSlabLease>,
        len: usize,
    },

    /// Body already available in memory (zero-copy reference counted slice).
    Bytes(Bytes),
}

impl Default for Body {
    #[inline(always)]
    fn default() -> Self {
        Self::Empty
    }
}

impl Body {
    /// Creates a body wrapping a pre-warmed slab lease and valid byte length.
    #[inline]
    pub fn from_slab(lease: PayloadSlabLease, len: usize) -> Self {
        Self::Slab {
            lease: Arc::new(lease),
            len,
        }
    }

    /// Creates a body wrapping in-memory bytes.
    #[inline]
    pub fn from_bytes(bytes: impl Into<Bytes>) -> Self {
        Self::Bytes(bytes.into())
    }

    /// Returns `true` if the body is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        match self {
            Self::Empty => true,
            Self::Slab { len, .. } => *len == 0,
            Self::Bytes(b) => b.is_empty(),
        }
    }

    /// Returns the length of the body in bytes.
    #[inline]
    pub fn len(&self) -> usize {
        match self {
            Self::Empty => 0,
            Self::Slab { len, .. } => *len,
            Self::Bytes(b) => b.len(),
        }
    }

    /// Borrows the body as a byte slice.
    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        match self {
            Self::Empty => &[],
            Self::Slab { lease, len } => &lease[..*len],
            Self::Bytes(b) => b.as_ref(),
        }
    }

    /// Converts or clones the body into a [`Bytes`] object.
    #[inline]
    pub fn to_bytes(&self) -> Bytes {
        match self {
            Self::Empty => Bytes::new(),
            Self::Slab { lease, len } => Bytes::copy_from_slice(&lease[..*len]),
            Self::Bytes(b) => b.clone(),
        }
    }
}

impl std::ops::Deref for Body {
    type Target = [u8];

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl AsRef<[u8]> for Body {
    #[inline(always)]
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl PartialEq for Body {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl Eq for Body {}

impl std::fmt::Debug for Body {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Empty => write!(f, "Body::Empty"),
            Self::Slab { len, .. } => write!(f, "Body::Slab({len} bytes)"),
            Self::Bytes(b) => write!(f, "Body::Bytes({} bytes)", b.len()),
        }
    }
}

impl From<Bytes> for Body {
    #[inline]
    fn from(b: Bytes) -> Self {
        Self::Bytes(b)
    }
}

impl From<&'static [u8]> for Body {
    #[inline]
    fn from(s: &'static [u8]) -> Self {
        Self::Bytes(Bytes::from_static(s))
    }
}

impl From<Vec<u8>> for Body {
    #[inline]
    fn from(v: Vec<u8>) -> Self {
        Self::Bytes(Bytes::from(v))
    }
}

impl From<String> for Body {
    #[inline]
    fn from(s: String) -> Self {
        Self::Bytes(Bytes::from(s.into_bytes()))
    }
}

impl From<&'static str> for Body {
    #[inline]
    fn from(s: &'static str) -> Self {
        Self::Bytes(Bytes::from_static(s.as_bytes()))
    }
}

/// HTTP request processed by the Velda L7 pipeline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct L7Request {
    /// HTTP method.
    pub method: Method,

    /// Request URI.
    pub uri: Uri,

    /// HTTP protocol version.
    pub version: Version,

    /// Request headers backed by a pre-allocated [`HeaderWorkspace`].
    pub headers: HeaderWorkspace,

    /// Request body.
    pub body: Body,
}

impl L7Request {
    /// Creates a new HTTP request from components.
    #[inline]
    pub fn new(
        method: Method,
        uri: Uri,
        version: Version,
        headers: impl Into<HeaderWorkspace>,
        body: Body,
    ) -> Self {
        Self {
            method,
            uri,
            version,
            headers: headers.into(),
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
