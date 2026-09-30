//! Compiled composition configuration for L7 listeners.

use std::fmt;

/// Application layer protocol target configured for an ingress listener.
///
/// Every listener must declare its protocol explicitly. There is no generic
/// "http" variant — use `Http1`, `Http2`, `Http3`, or `Grpc`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ApplicationProtocol {
    /// Explicit HTTP/1.1 (cleartext or over TLS).
    Http1,
    /// Explicit HTTP/2 (H2C or H2 over TLS).
    Http2,
    /// HTTP/3 over QUIC (UDP transport).
    Http3,
    /// Dedicated gRPC pipeline (transported over HTTP/2 framing).
    Grpc,
}

impl ApplicationProtocol {
    /// Parses an application protocol string into a typed protocol variant.
    ///
    /// Evaluates case-insensitively with zero heap allocations on the hot path.
    pub fn from_str_proto(s: &str) -> Option<Self> {
        let bytes = s.as_bytes();
        match bytes.len() {
            4 => {
                if bytes.eq_ignore_ascii_case(b"grpc") {
                    Some(Self::Grpc)
                } else {
                    None
                }
            }
            5 => {
                if bytes[..4].eq_ignore_ascii_case(b"http") {
                    match bytes[4] {
                        b'1' => Some(Self::Http1),
                        b'2' => Some(Self::Http2),
                        b'3' => Some(Self::Http3),
                        _ => None,
                    }
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    /// Returns the canonical protocol name.
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Http1 => "http1",
            Self::Http2 => "http2",
            Self::Http3 => "http3",
            Self::Grpc => "grpc",
        }
    }
}

impl fmt::Display for ApplicationProtocol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

use velda_core::IngressLimits;

/// Compiled runtime composition rules for a specific listener.
///
/// Pre-validated and stored in RAM during bootstrap/reload without JSON parsing overhead.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompiledListenerComposition {
    /// Listener identifier (e.g. "http", "https", "grpc-h2c").
    pub listener_id: String,
    /// Target application protocol.
    pub protocol: ApplicationProtocol,
    /// Whether TLS termination is required before protocol handling.
    pub tls_enabled: bool,
    /// TLS handshake timeout in milliseconds (default 5,000 ms).
    pub handshake_timeout_ms: u64,
    /// Generic ingress safety limits (body size, header size, max headers, timeout).
    pub limits: IngressLimits,
}

impl CompiledListenerComposition {
    /// Creates a new compiled listener composition with default IngressLimits.
    pub fn new(
        listener_id: impl Into<String>,
        protocol: ApplicationProtocol,
        tls_enabled: bool,
    ) -> Self {
        Self {
            listener_id: listener_id.into(),
            protocol,
            tls_enabled,
            handshake_timeout_ms: 5000,
            limits: IngressLimits::default(),
        }
    }

    /// Configures an explicit TLS handshake timeout.
    pub fn with_handshake_timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.handshake_timeout_ms = timeout_ms;
        self
    }

    /// Configures explicit ingress safety limits.
    pub fn with_limits(mut self, limits: IngressLimits) -> Self {
        self.limits = limits;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_str_proto_valid() {
        assert_eq!(
            ApplicationProtocol::from_str_proto("http1"),
            Some(ApplicationProtocol::Http1)
        );
        assert_eq!(
            ApplicationProtocol::from_str_proto("HTTP1"),
            Some(ApplicationProtocol::Http1)
        );
        assert_eq!(
            ApplicationProtocol::from_str_proto("Http2"),
            Some(ApplicationProtocol::Http2)
        );
        assert_eq!(
            ApplicationProtocol::from_str_proto("HTTP3"),
            Some(ApplicationProtocol::Http3)
        );
        assert_eq!(
            ApplicationProtocol::from_str_proto("hTtP3"),
            Some(ApplicationProtocol::Http3)
        );
        assert_eq!(
            ApplicationProtocol::from_str_proto("grpc"),
            Some(ApplicationProtocol::Grpc)
        );
        assert_eq!(
            ApplicationProtocol::from_str_proto("GRPC"),
            Some(ApplicationProtocol::Grpc)
        );
    }

    #[test]
    fn test_from_str_proto_invalid() {
        assert_eq!(ApplicationProtocol::from_str_proto("http"), None);
        assert_eq!(ApplicationProtocol::from_str_proto("http4"), None);
        assert_eq!(ApplicationProtocol::from_str_proto("grp"), None);
        assert_eq!(ApplicationProtocol::from_str_proto("grpc1"), None);
        assert_eq!(ApplicationProtocol::from_str_proto(""), None);
        assert_eq!(ApplicationProtocol::from_str_proto("random_garbage"), None);
    }
}
