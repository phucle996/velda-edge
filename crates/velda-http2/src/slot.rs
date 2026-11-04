//! HTTP/2 Header Slot Definitions & HPACK Static Table Mappings (RFC 7541 / RFC 9113).
//!
//! # Architecture & Invariants (SRP: Protocol Header Semantics Only)
//! 1. **Zero-Overhead HPACK Mapping**: Connects RFC 7541 static table indices directly
//!    to deterministic integer slots in the anonymous slot table.
//! 2. **Pseudo-Header Separation**: HTTP/2 pseudo-headers (`:authority`, `:path`, `:method`)
//!    are mapped distinctly to preserve binary wire framing semantics.

use http::header::HeaderName;

/// Well-known HTTP/2 header slot indices (`0..=15`).
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Http2HeaderSlot {
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

impl Http2HeaderSlot {
    /// Returns the integer slot index.
    #[inline(always)]
    pub const fn index(self) -> u8 {
        self as u8
    }

    /// Resolves an HTTP/2 header name to its fast-path slot index.
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

    /// Resolves an HPACK static table index (RFC 7541 Appendix A) to a fast-path slot.
    #[inline]
    pub const fn from_hpack_static_index(index: usize) -> Option<Self> {
        match index {
            1 => Some(Self::Authority),
            4 | 5 => Some(Self::Path),
            6 | 7 => Some(Self::Scheme),
            16 => Some(Self::AcceptEncoding),
            19 => Some(Self::Accept),
            23 => Some(Self::Authorization),
            28 => Some(Self::ContentLength),
            31 => Some(Self::ContentType),
            32 => Some(Self::Cookie),
            58 => Some(Self::UserAgent),
            59 => Some(Self::Vary),
            _ => None,
        }
    }

    /// Returns the standard HPACK static table index for fast binary header encoding.
    #[inline]
    pub const fn to_hpack_static_index(self) -> Option<usize> {
        match self {
            Self::Authority => Some(1),
            Self::Path => Some(4),
            Self::Scheme => Some(7),
            Self::AcceptEncoding => Some(16),
            Self::Accept => Some(19),
            Self::Authorization => Some(23),
            Self::ContentLength => Some(28),
            Self::ContentType => Some(31),
            Self::Cookie => Some(32),
            Self::UserAgent => Some(58),
            Self::Vary => Some(59),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hpack_static_mapping() {
        assert_eq!(
            Http2HeaderSlot::from_hpack_static_index(1),
            Some(Http2HeaderSlot::Authority)
        );
        assert_eq!(
            Http2HeaderSlot::from_hpack_static_index(31),
            Some(Http2HeaderSlot::ContentType)
        );
        assert_eq!(
            Http2HeaderSlot::from_hpack_static_index(23),
            Some(Http2HeaderSlot::Authorization)
        );
        assert_eq!(
            Http2HeaderSlot::ContentType.to_hpack_static_index(),
            Some(31)
        );
    }
}
