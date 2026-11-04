//! HTTP/1.1 Well-Known Header Slot Definitions.
//!
//! # Architecture & Invariants (SRP: Protocol Header Semantics Only)
//! 1. **Index-Based Header Dispatch**: Assigns deterministic integer indices (`0..=15`)
//!    to top well-known HTTP/1.1 headers for O(1) slot table storage.
//! 2. **Pre-compiled Wire Byte Arrays**: Provides static ASCII byte slices for single-cycle
//!    wire serialization (`b"host: "`, `b"content-type: "`) avoiding string allocations.

use http::header::HeaderName;

/// Well-known HTTP/1.1 header slot indices (`0..=15`).
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Http1HeaderSlot {
    Host = 0,
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
    Connection = 14,
    Upgrade = 15,
}

impl Http1HeaderSlot {
    /// Returns the integer slot index.
    #[inline(always)]
    pub const fn index(self) -> u8 {
        self as u8
    }

    /// Resolves a well-known HTTP/1.1 header name to its fast-path slot index.
    #[inline]
    pub fn from_name(name: &HeaderName) -> Option<Self> {
        match name.as_str() {
            "host" => Some(Self::Host),
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
            "connection" => Some(Self::Connection),
            "upgrade" => Some(Self::Upgrade),
            _ => None,
        }
    }

    /// Static byte slice of header name plus colon and space for wire formatting.
    #[inline(always)]
    pub const fn wire_prefix(self) -> &'static [u8] {
        match self {
            Self::Host => b"host: ",
            Self::Authorization => b"authorization: ",
            Self::ContentType => b"content-type: ",
            Self::ContentLength => b"content-length: ",
            Self::Accept => b"accept: ",
            Self::AcceptEncoding => b"accept-encoding: ",
            Self::ContentEncoding => b"content-encoding: ",
            Self::UserAgent => b"user-agent: ",
            Self::Origin => b"origin: ",
            Self::Cookie => b"cookie: ",
            Self::SetCookie => b"set-cookie: ",
            Self::IfNoneMatch => b"if-none-match: ",
            Self::Vary => b"vary: ",
            Self::CacheControl => b"cache-control: ",
            Self::Connection => b"connection: ",
            Self::Upgrade => b"upgrade: ",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_http1_slot_mapping() {
        assert_eq!(
            Http1HeaderSlot::from_name(&http::header::HOST),
            Some(Http1HeaderSlot::Host)
        );
        assert_eq!(
            Http1HeaderSlot::from_name(&http::header::AUTHORIZATION),
            Some(Http1HeaderSlot::Authorization)
        );
        assert_eq!(
            Http1HeaderSlot::from_name(&http::header::CONTENT_TYPE),
            Some(Http1HeaderSlot::ContentType)
        );
        assert_eq!(Http1HeaderSlot::Host.wire_prefix(), b"host: ");
        assert_eq!(
            Http1HeaderSlot::ContentType.wire_prefix(),
            b"content-type: "
        );
    }
}
