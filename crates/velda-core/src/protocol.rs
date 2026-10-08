//! Canonical protocol families for Velda Edge.
//!
//! Enforces the explicit protocol isolation invariant:
//! Listeners declare their application protocol family upfront,
//! completely eliminating dynamic runtime sniffing on the hot path.

use serde::{Deserialize, Serialize};
use std::fmt;

use crate::l4::request::TransportProtocol;

/// Canonical application protocol family.
///
/// In Velda Edge, listeners and routing pipelines are strictly isolated by protocol family:
/// - `Raw`: L4 direct byte forwarding / UDP datagram forwarding.
/// - `Http`: Web/REST traffic across generations (HTTP/1.1, HTTP/2, HTTP/3).
/// - `Grpc`: RPC traffic over length-prefixed streaming (HTTP/2 binary framing or QUIC).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ProtocolFamily {
    /// Raw Layer 4 transport pipeline (no L7 interpretation).
    Raw,
    /// HTTP family (HTTP/1.1, HTTP/2, HTTP/3).
    Http,
    /// gRPC family (gRPC over HTTP/2, gRPC over QUIC).
    Grpc,
}

impl ProtocolFamily {
    /// Returns the canonical string representation of the protocol family.
    #[inline]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Raw => "raw",
            Self::Http => "http",
            Self::Grpc => "grpc",
        }
    }

    /// Parses a string slice into a known protocol family.
    #[inline]
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "raw" | "l4" | "tcp" | "udp" => Some(Self::Raw),
            "http" | "http1" | "http2" | "http3" => Some(Self::Http),
            "grpc" => Some(Self::Grpc),
            _ => None,
        }
    }

    /// Returns the ordered list of ALPN identifiers valid for this family
    /// given the transport protocol and TLS state.
    ///
    /// ALPN negotiation is strictly confined to generations *within* the same family:
    /// - `Http` on TCP: `["h2", "http/1.1"]`
    /// - `Http` on UDP: `["h3"]`
    /// - `Grpc` on TCP: `["h2"]`
    /// - `Grpc` on UDP: `["grpc-quic", "h3"]`
    /// - `Raw`: `[]` (no ALPN)
    #[inline]
    pub const fn alpn_protocols(
        &self,
        transport: TransportProtocol,
        tls_enabled: bool,
    ) -> &'static [&'static str] {
        if !tls_enabled {
            return &[];
        }

        match (self, transport) {
            (Self::Http, TransportProtocol::Tcp) => &["h2", "http/1.1"],
            (Self::Http, TransportProtocol::Udp) => &["h3"],
            (Self::Grpc, TransportProtocol::Tcp) => &["h2"],
            (Self::Grpc, TransportProtocol::Udp) => &["grpc-quic", "h3"],
            (Self::Raw, _) => &[],
        }
    }

    /// Validates whether an ALPN identifier belongs to this protocol family.
    #[inline]
    pub fn matches_alpn(&self, alpn: &str) -> bool {
        match self {
            Self::Http => matches!(alpn, "h2" | "http/1.1" | "h3"),
            Self::Grpc => matches!(alpn, "h2" | "grpc-quic" | "h3"),
            Self::Raw => false,
        }
    }
}

impl fmt::Display for ProtocolFamily {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_protocol_family_alpn() {
        let http = ProtocolFamily::Http;
        assert_eq!(
            http.alpn_protocols(TransportProtocol::Tcp, true),
            &["h2", "http/1.1"]
        );
        assert_eq!(http.alpn_protocols(TransportProtocol::Udp, true), &["h3"]);
        assert!(
            http.alpn_protocols(TransportProtocol::Tcp, false)
                .is_empty()
        );
        assert!(http.matches_alpn("h2"));
        assert!(http.matches_alpn("http/1.1"));
        assert!(http.matches_alpn("h3"));
        assert!(!http.matches_alpn("grpc-quic"));

        let grpc = ProtocolFamily::Grpc;
        assert_eq!(grpc.alpn_protocols(TransportProtocol::Tcp, true), &["h2"]);
        assert_eq!(
            grpc.alpn_protocols(TransportProtocol::Udp, true),
            &["grpc-quic", "h3"]
        );
        assert!(grpc.matches_alpn("h2"));
        assert!(grpc.matches_alpn("grpc-quic"));
        assert!(!grpc.matches_alpn("http/1.1"));

        let raw = ProtocolFamily::Raw;
        assert!(raw.alpn_protocols(TransportProtocol::Tcp, true).is_empty());
        assert!(!raw.matches_alpn("h2"));
    }

    #[test]
    fn test_protocol_family_parsing() {
        assert_eq!(ProtocolFamily::parse("http"), Some(ProtocolFamily::Http));
        assert_eq!(ProtocolFamily::parse("http1"), Some(ProtocolFamily::Http));
        assert_eq!(ProtocolFamily::parse("http2"), Some(ProtocolFamily::Http));
        assert_eq!(ProtocolFamily::parse("grpc"), Some(ProtocolFamily::Grpc));
        assert_eq!(ProtocolFamily::parse("raw"), Some(ProtocolFamily::Raw));
        assert_eq!(ProtocolFamily::parse("unknown"), None);
    }
}
