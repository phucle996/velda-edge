//! Compiled composition configuration for L7 listeners.

use std::fmt;

/// Application layer protocol target configured for an ingress listener.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum ApplicationProtocol {
    /// Generic HTTP (can negotiate HTTP/1.1 or HTTP/2 via ALPN or cleartext upgrade).
    #[default]
    Http,
    /// Explicit cleartext or secure HTTP/1.1.
    Http1,
    /// Explicit HTTP/2 (H2C or H2 over TLS).
    Http2,
    /// HTTP/3 over QUIC (UDP transport).
    Http3,
}

impl ApplicationProtocol {
    /// Parses an application protocol string into a typed protocol variant.
    pub fn from_str_proto(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "http" => Some(Self::Http),
            "http1" | "http/1.1" | "http/1.0" => Some(Self::Http1),
            "http2" | "h2" | "h2c" => Some(Self::Http2),
            "http3" | "h3" | "quic" => Some(Self::Http3),
            _ => None,
        }
    }

    /// Returns the canonical protocol name.
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Http => "http",
            Self::Http1 => "http1",
            Self::Http2 => "http2",
            Self::Http3 => "http3",
        }
    }
}

impl fmt::Display for ApplicationProtocol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

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
    /// TLS profile reference name if TLS is enabled (e.g. "default", "prod-tls").
    pub tls_profile: Option<String>,
    /// TLS handshake timeout in milliseconds (default 5,000 ms).
    pub handshake_timeout_ms: u64,
}

impl CompiledListenerComposition {
    /// Creates a new compiled listener composition.
    pub fn new(
        listener_id: impl Into<String>,
        protocol: ApplicationProtocol,
        tls_enabled: bool,
        tls_profile: Option<String>,
    ) -> Self {
        Self {
            listener_id: listener_id.into(),
            protocol,
            tls_enabled,
            tls_profile,
            handshake_timeout_ms: 5000,
        }
    }

    /// Configures an explicit TLS handshake timeout.
    pub fn with_handshake_timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.handshake_timeout_ms = timeout_ms;
        self
    }
}
