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
    pub fn from_str_proto(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "http1" => Some(Self::Http1),
            "http2" => Some(Self::Http2),
            "http3" => Some(Self::Http3),
            "grpc" => Some(Self::Grpc),
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
}

impl CompiledListenerComposition {
    /// Creates a new compiled listener composition.
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
        }
    }

    /// Configures an explicit TLS handshake timeout.
    pub fn with_handshake_timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.handshake_timeout_ms = timeout_ms;
        self
    }
}
