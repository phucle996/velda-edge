//! HTTP/3 Header Slot Definitions & QPACK Static Table Mappings (RFC 9204 / RFC 9114).
//!
//! # Architecture & Invariants (SRP: Protocol Header Semantics Only)
//! 1. **Zero-Overhead QPACK Mapping**: Connects RFC 9204 static table indices directly
//!    to deterministic integer slots in the anonymous slot table.
//! 2. **QUIC-Native Isolation**: Protocol framing runs cleanly on QUIC streams without
//!    coupling to HTTP/1 or HTTP/2 codecs.

use http::header::HeaderName;

/// Well-known HTTP/3 header slot indices (`0..=15`), aligned with HTTP/2.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Http3HeaderSlot {
    Authority = 0,
    Authorization = 1,
    ContentType = 2,
    ContentLength = 3,
    Accept = 4,
    AcceptEncoding = 5,
    ContentEncoding = 6,
    UserAgent = 7,
    Origin = 8,
    Cookie = 9,
    SetCookie = 10,
    IfNoneMatch = 11,
    Vary = 12,
    CacheControl = 13,
    Scheme = 14,
    Path = 15,
}

impl Http3HeaderSlot {
    /// Returns the integer slot index.
    #[inline(always)]
    pub const fn index(self) -> u8 {
        self as u8
    }

    /// Resolves an HTTP/3 header name to its fast-path slot index.
    #[inline]
    pub fn from_name(name: &HeaderName) -> Option<Self> {
        match name.as_str() {
            ":authority" | "host" => Some(Self::Authority),
            "authorization" => Some(Self::Authorization),
            "content-type" => Some(Self::ContentType),
            "content-length" => Some(Self::ContentLength),
            "accept" => Some(Self::Accept),
            "accept-encoding" => Some(Self::AcceptEncoding),
            "content-encoding" => Some(Self::ContentEncoding),
            "user-agent" => Some(Self::UserAgent),
            "origin" => Some(Self::Origin),
            "cookie" => Some(Self::Cookie),
            "set-cookie" => Some(Self::SetCookie),
            "if-none-match" => Some(Self::IfNoneMatch),
            "vary" => Some(Self::Vary),
            "cache-control" => Some(Self::CacheControl),
            ":scheme" => Some(Self::Scheme),
            ":path" => Some(Self::Path),
            _ => None,
        }
    }

    /// Resolves a QPACK static table index (RFC 9204 Appendix A) to a fast-path slot.
    #[inline]
    pub const fn from_qpack_static_index(index: usize) -> Option<Self> {
        match index {
            0 => Some(Self::Authority),
            1 => Some(Self::Path),
            4 => Some(Self::ContentLength),
            5 => Some(Self::Cookie),
            29 | 30 => Some(Self::Accept),
            31 => Some(Self::AcceptEncoding),
            36..=46 => Some(Self::ContentType),
            84 => Some(Self::Authorization),
            95 => Some(Self::UserAgent),
            106 => Some(Self::Vary),
            108 | 109 => Some(Self::Scheme),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_qpack_static_mapping() {
        assert_eq!(
            Http3HeaderSlot::from_qpack_static_index(0),
            Some(Http3HeaderSlot::Authority)
        );
        assert_eq!(
            Http3HeaderSlot::from_qpack_static_index(1),
            Some(Http3HeaderSlot::Path)
        );
        assert_eq!(
            Http3HeaderSlot::from_qpack_static_index(31),
            Some(Http3HeaderSlot::AcceptEncoding)
        );
        assert_eq!(
            Http3HeaderSlot::from_qpack_static_index(84),
            Some(Http3HeaderSlot::Authorization)
        );
        assert_eq!(
            Http3HeaderSlot::from_qpack_static_index(95),
            Some(Http3HeaderSlot::UserAgent)
        );
    }
}
