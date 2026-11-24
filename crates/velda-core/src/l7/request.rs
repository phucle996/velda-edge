//! Layer 7 HTTP Request & Zero-Copy Body Model (`velda_core::l7::request`).
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────────────────────┐
//! │                        L7Request & ZERO-COPY BODY STORAGE                               │
//! ├─────────────────────────────────────────────────────────────────────────────────────────┤
//! │  L7Request { method: Method, uri: Uri, version: Version, headers: HeaderWorkspace, body}│
//! │                                                                                         │
//! │  Body Variants:                                                                         │
//! │    ├── Empty                           (0B payload)                                     │
//! │    ├── Slab { lease, len }             (Pre-warmed PayloadSlabPool, auto-recycled)      │
//! │    └── Bytes(Bytes)                    (Ref-counted zero-copy slice)                    │
//! └─────────────────────────────────────────────────────────────────────────────────────────┘
//! ```

use std::sync::Arc;

use bytes::Bytes;
use http::{Method, Uri, Version};

use crate::buffer::header::HeaderWorkspace;
use crate::buffer::slab::PayloadSlabLease;
use crate::context::RequestState;

/// HTTP request and response body.
///
/// Supports pre-warmed pooled slabs (zero allocation, zero minor page fault),
/// reference-counted byte slices, and empty payloads.
#[derive(Clone)]
pub enum Body {
    /// No request body.
    Empty,

    /// Body backed by a pre-warmed physical slab leased from [`crate::buffer::slab::PayloadSlabPool`].
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

    /// Ingress timestamp snapshot (Unix epoch nanoseconds) captured once at ingress from hardware clock.
    /// Used by auth, rate limiting, and quota plugins to eliminate redundant clock reads.
    pub ingress_time_nanos: u64,

    /// Mutable strongly-typed state accumulated throughout the request lifecycle.
    pub state: RequestState,
}

impl L7Request {
    /// Creates a new HTTP request from components, capturing the hardware time snapshot.
    #[inline]
    pub fn new(
        method: Method,
        uri: Uri,
        version: Version,
        headers: impl Into<HeaderWorkspace>,
        body: Body,
    ) -> Self {
        let headers = headers.into();
        let ingress_time_nanos = headers.ingress_time_nanos;
        Self {
            method,
            uri,
            version,
            headers,
            body,
            ingress_time_nanos,
            state: RequestState::default(),
        }
    }

    /// Returns the ingress timestamp in Unix epoch seconds.
    #[inline(always)]
    pub fn ingress_time_sec(&self) -> u64 {
        self.ingress_time_nanos / 1_000_000_000
    }
}
