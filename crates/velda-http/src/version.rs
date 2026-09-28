//! Concrete HTTP protocol version indicator.

use std::fmt;

/// Concrete HTTP protocol version for connection dispatching.
///
/// Abstracted from ALPN, TLS, or transport socket details so the HTTP engine
/// only cares about the wire protocol version to parse.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum HttpVersion {
    /// HTTP/1.0 or HTTP/1.1 (plain text framing, keep-alive pipelining).
    #[default]
    Http1,
    /// HTTP/2 (binary framing, stream multiplexing).
    Http2,
    /// HTTP/3 (QUIC transport datagram multiplexing).
    Http3,
}

impl HttpVersion {
    /// Returns the canonical protocol version name in the Velda system.
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Http1 => "http1",
            Self::Http2 => "http2",
            Self::Http3 => "http3",
        }
    }

    /// Parses the canonical Velda system protocol identifier into a concrete HTTP version.
    pub fn from_proto_str(s: &str) -> Option<Self> {
        match s {
            "http1" => Some(Self::Http1),
            "http2" => Some(Self::Http2),
            "http3" => Some(Self::Http3),
            _ => None,
        }
    }
}

impl fmt::Display for HttpVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_canonical_version_roundtrip() {
        assert_eq!(
            HttpVersion::from_proto_str("http1"),
            Some(HttpVersion::Http1)
        );
        assert_eq!(
            HttpVersion::from_proto_str("http2"),
            Some(HttpVersion::Http2)
        );
        assert_eq!(
            HttpVersion::from_proto_str("http3"),
            Some(HttpVersion::Http3)
        );
        assert_eq!(HttpVersion::from_proto_str("other"), None);

        assert_eq!(HttpVersion::Http1.as_str(), "http1");
        assert_eq!(HttpVersion::Http2.as_str(), "http2");
        assert_eq!(HttpVersion::Http3.as_str(), "http3");
    }
}
