//! Connection-level ingress context.
//!
//! Holds connection metadata and optional TLS negotiation details (SNI, ALPN)
//! across pipeline stages without binding to a specific application protocol.

use std::borrow::Cow;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use velda_core::{ConnectionContext, ConnectionId, L4Request, StreamingMode};

use crate::error::EdgeError;

/// Metadata captured from TLS handshake (if TLS was terminated).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TlsMetadata {
    /// Server Name Indication (SNI) requested by client.
    pub sni: Option<String>,
    /// Application-Layer Protocol Negotiation (ALPN) token selected.
    /// Uses `Cow<'static, str>` to eliminate heap allocations for standard protocols.
    pub alpn: Option<Cow<'static, str>>,
    /// Negotiated TLS protocol version (e.g. "TLSv1.3").
    pub version: Option<String>,
    /// Negotiated cipher suite (e.g. "TLS_AES_128_GCM_SHA256").
    pub cipher: Option<String>,
}

impl TlsMetadata {
    /// Creates a new TLS metadata container with zero-allocation ALPN representation.
    pub fn new(sni: Option<String>, alpn: Option<Cow<'static, str>>) -> Self {
        Self {
            sni,
            alpn,
            version: None,
            cipher: None,
        }
    }

    /// Creates TLS metadata from handshake info.
    pub fn from_handshake(info: velda_tls::TlsHandshakeInfo) -> Self {
        Self::new(info.sni, info.alpn)
    }
}

/// Metadata describing an accepted connection that has entered the ingress pipeline.
///
/// Created at L4 accept and passed through TLS and L7 layers.
/// Preserves zero-allocation properties on the hot path.
#[derive(Debug, Clone)]
pub struct IngressContext {
    /// Monotonically increasing unique connection identifier.
    pub connection_id: ConnectionId,
    /// Transport protocol (TCP or UDP).
    pub transport: velda_core::TransportProtocol,
    /// Identifier of the listener that accepted this connection.
    pub listener_id: Arc<str>,
    /// Remote client socket address.
    pub peer: SocketAddr,
    /// Local server socket address.
    pub local_addr: SocketAddr,
    /// Whether downstream TLS is enabled on this listener.
    pub tls_enabled: bool,
    /// Directional streaming capabilities declared for this listener.
    pub streaming: StreamingMode,
    /// TLS session metadata (present if TLS was active and completed).
    pub tls: Option<TlsMetadata>,
    /// Time when the connection entered the pipeline.
    pub created_at: Instant,
}

impl IngressContext {
    /// Creates a new ingress context for a TCP connection handoff.
    pub fn new_tcp(
        connection_id: ConnectionId,
        listener_id: impl Into<Arc<str>>,
        peer: SocketAddr,
        local_addr: SocketAddr,
        tls_enabled: bool,
        streaming: StreamingMode,
    ) -> Self {
        Self {
            connection_id,
            transport: velda_core::TransportProtocol::Tcp,
            listener_id: listener_id.into(),
            peer,
            local_addr,
            tls_enabled,
            streaming,
            tls: None,
            created_at: Instant::now(),
        }
    }

    /// Creates a new ingress context for a UDP datagram handoff.
    ///
    /// UDP is connectionless, so a fresh [`ConnectionId`] is generated
    /// from the global monotonic counter for tracing and observability.
    pub fn new_udp(
        listener_id: impl Into<Arc<str>>,
        peer: SocketAddr,
        local_addr: SocketAddr,
        tls_enabled: bool,
        streaming: StreamingMode,
    ) -> Self {
        Self {
            connection_id: velda_transport::next_connection_id(),
            transport: velda_core::TransportProtocol::Udp,
            listener_id: listener_id.into(),
            peer,
            local_addr,
            tls_enabled,
            streaming,
            tls: None,
            created_at: Instant::now(),
        }
    }

    /// Enriches the context with TLS metadata after successful handshake.
    pub fn with_tls_metadata(mut self, metadata: TlsMetadata) -> Self {
        self.tls = Some(metadata);
        self
    }

    /// Validates whether the negotiated ALPN matches the listener's declared application protocol.
    ///
    /// Returns `Ok(())` if:
    /// - No TLS was used (cleartext stream).
    /// - Client did not send ALPN extension (permissive fallback).
    /// - Negotiated ALPN matches expected protocol string.
    ///
    /// Returns `Err(EdgeError::AlpnMismatch)` if negotiated ALPN explicitly conflicts.
    pub fn validate_alpn(&self, expected_protocol: &str) -> Result<(), EdgeError> {
        let Some(tls) = &self.tls else {
            return Ok(());
        };

        let Some(alpn) = &tls.alpn else {
            return Ok(());
        };

        let expected_token = match expected_protocol {
            "http1" => "http/1.1",
            "http2" => "h2",
            "http3" => "h3",
            "grpc" => "h2",
            _ => expected_protocol,
        };

        if alpn.as_ref() == expected_token {
            Ok(())
        } else {
            Err(EdgeError::AlpnMismatch {
                listener_id: self.listener_id.to_string(),
                expected: expected_token.to_string(),
                actual: alpn.to_string(),
            })
        }
    }

    /// Converts this `IngressContext` into a `velda_core::ConnectionContext` for plugin evaluation.
    pub fn to_core_connection_context(&self) -> ConnectionContext {
        ConnectionContext {
            connection: L4Request::new(
                self.connection_id,
                self.transport,
                self.peer,
                self.local_addr,
            ),
        }
    }

    /// Enriches downstream request headers with standard proxy forwarding metadata (RFC 7239).
    pub fn enrich_forwarded_headers(&self, headers: &mut http::HeaderMap, host: Option<&str>) {
        use http::header::{HeaderName, HeaderValue};

        let client_ip = self.peer.ip();
        let is_tls = self.tls_enabled || self.tls.is_some();
        let proto = if is_tls { "https" } else { "http" };

        // Format peer IP directly into stack buffer
        let mut ip_buf = [0u8; 64];
        let ip_len = {
            use std::io::Write;
            let mut cursor = std::io::Cursor::new(&mut ip_buf[..]);
            let _ = write!(cursor, "{}", client_ip);
            cursor.position() as usize
        };
        let client_ip_bytes = &ip_buf[..ip_len];

        // 1. X-Forwarded-For
        let x_forwarded_for = HeaderName::from_static("x-forwarded-for");
        if let Some(existing) = headers.get(&x_forwarded_for) {
            if let Ok(existing_bytes) = existing.to_str() {
                let mut combined = Vec::with_capacity(existing_bytes.len() + 2 + ip_len);
                combined.extend_from_slice(existing_bytes.as_bytes());
                combined.extend_from_slice(b", ");
                combined.extend_from_slice(client_ip_bytes);
                if let Ok(val) = HeaderValue::from_bytes(&combined) {
                    headers.insert(x_forwarded_for, val);
                }
            }
        } else if let Ok(val) = HeaderValue::from_bytes(client_ip_bytes) {
            headers.insert(x_forwarded_for, val);
        }

        // 2. X-Forwarded-Proto
        headers.insert(
            HeaderName::from_static("x-forwarded-proto"),
            HeaderValue::from_static(proto),
        );

        // 3. X-Forwarded-Port
        let mut port_buf = [0u8; 8];
        let port_len = {
            use std::io::Write;
            let mut cursor = std::io::Cursor::new(&mut port_buf[..]);
            let _ = write!(cursor, "{}", self.local_addr.port());
            cursor.position() as usize
        };
        if let Ok(val) = HeaderValue::from_bytes(&port_buf[..port_len]) {
            headers.insert(HeaderName::from_static("x-forwarded-port"), val);
        }

        // 4. X-Forwarded-Host
        let x_forwarded_host = HeaderName::from_static("x-forwarded-host");
        if !headers.contains_key(&x_forwarded_host)
            && let Some(h) = host
            && let Ok(val) = HeaderValue::from_str(h)
        {
            headers.insert(x_forwarded_host, val);
        }

        // 5. X-Real-IP
        let x_real_ip = HeaderName::from_static("x-real-ip");
        if !headers.contains_key(&x_real_ip)
            && let Ok(val) = HeaderValue::from_bytes(client_ip_bytes)
        {
            headers.insert(x_real_ip, val);
        }

        // 6. Standard: RFC 7239 (Official IETF "Forwarded" HTTP Extension)
        // Format: for=<client>;proto=<http|https>;by=<local>;host=<host>
        // RFC 7239 §5.2: IPv6 addresses must be quoted with square brackets: "[...]"
        let forwarded = HeaderName::from_static("forwarded");
        let mut fwd_buf = [0u8; 256];
        let fwd_len = {
            use std::io::Write;
            let mut cursor = std::io::Cursor::new(&mut fwd_buf[..]);
            match client_ip {
                std::net::IpAddr::V4(v4) => {
                    let _ = write!(cursor, "for={v4}");
                }
                std::net::IpAddr::V6(v6) => {
                    let _ = write!(cursor, "for=\"[{v6}]\"");
                }
            }
            let _ = write!(cursor, ";proto={proto};by=");
            match self.local_addr.ip() {
                std::net::IpAddr::V4(v4) => {
                    let _ = write!(cursor, "{v4}");
                }
                std::net::IpAddr::V6(v6) => {
                    let _ = write!(cursor, "\"[{v6}]\"");
                }
            }
            if let Some(h) = host {
                let _ = write!(cursor, ";host=\"{h}\"");
            }
            cursor.position() as usize
        };
        let fwd_bytes = &fwd_buf[..fwd_len];

        if let Some(existing) = headers.get(&forwarded) {
            if let Ok(existing_bytes) = existing.to_str() {
                let mut combined = Vec::with_capacity(existing_bytes.len() + 2 + fwd_len);
                combined.extend_from_slice(existing_bytes.as_bytes());
                combined.extend_from_slice(b", ");
                combined.extend_from_slice(fwd_bytes);
                if let Ok(val) = HeaderValue::from_bytes(&combined) {
                    headers.insert(forwarded, val);
                }
            }
        } else if let Ok(val) = HeaderValue::from_bytes(fwd_bytes) {
            headers.insert(forwarded, val);
        }
    }
}

/// Stack-buffered host string representation to eliminate borrow checker conflicts
/// when mutating headers on the hot path.
pub struct HostHeaderBuffer {
    buf: [u8; 128],
    len: usize,
    heap: Option<String>,
}

impl HostHeaderBuffer {
    /// Extracts the target host from headers (`Host`) or URI authority with zero heap allocation
    /// for hosts up to 128 bytes.
    #[inline]
    pub fn extract(headers: &http::HeaderMap, uri: &http::Uri) -> Self {
        let mut s = Self {
            buf: [0u8; 128],
            len: 0,
            heap: None,
        };
        if let Some(h) = headers
            .get(http::header::HOST)
            .and_then(|h| h.to_str().ok())
            .or_else(|| uri.authority().map(|a| a.as_str()).or_else(|| uri.host()))
        {
            if h.len() <= s.buf.len() {
                s.buf[..h.len()].copy_from_slice(h.as_bytes());
                s.len = h.len();
            } else {
                s.heap = Some(h.to_string());
            }
        }
        s
    }

    /// Returns the extracted host string slice, if present and valid UTF-8.
    #[inline]
    pub fn as_deref(&self) -> Option<&str> {
        if self.len > 0 {
            std::str::from_utf8(&self.buf[..self.len]).ok()
        } else {
            self.heap.as_deref()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ingress_context_tcp_creation() {
        let addr = "127.0.0.1:8080".parse().unwrap();
        let ctx = IngressContext::new_tcp(
            ConnectionId::new(42),
            "http-listener",
            addr,
            addr,
            false,
            StreamingMode::DISABLED,
        );

        assert_eq!(ctx.connection_id, ConnectionId::new(42));
        assert_eq!(&*ctx.listener_id, "http-listener");
        assert_eq!(ctx.peer, addr);
        assert_eq!(ctx.local_addr, addr);
        assert!(!ctx.tls_enabled);
        assert!(ctx.tls.is_none());
    }

    #[test]
    fn test_ingress_context_udp_creation() {
        let addr = "127.0.0.1:8443".parse().unwrap();
        let ctx =
            IngressContext::new_udp("udp-listener", addr, addr, false, StreamingMode::DISABLED);

        assert_eq!(&*ctx.listener_id, "udp-listener");
        assert_eq!(ctx.peer, addr);
        assert!(!ctx.tls_enabled);
        assert!(ctx.tls.is_none());
    }

    #[test]
    fn test_with_tls_metadata_and_alpn_validation() {
        let addr = "127.0.0.1:8080".parse().unwrap();
        let ctx = IngressContext::new_tcp(
            ConnectionId::new(1),
            "h2-listener",
            addr,
            addr,
            true,
            StreamingMode::DISABLED,
        );

        // Cleartext: validate_alpn passes
        assert!(ctx.validate_alpn("http2").is_ok());

        // With matching ALPN
        let enriched = ctx.with_tls_metadata(TlsMetadata::new(
            Some("example.com".into()),
            Some("h2".into()),
        ));
        assert!(enriched.validate_alpn("http2").is_ok());

        // With mismatched ALPN
        let mismatched = enriched.clone();
        assert!(mismatched.validate_alpn("http1").is_err());
    }

    #[test]
    fn test_to_core_connection_context() {
        let addr = "127.0.0.1:9090".parse().unwrap();
        let ctx = IngressContext::new_tcp(
            ConnectionId::new(10),
            "test-listener",
            addr,
            addr,
            false,
            StreamingMode::DISABLED,
        );
        let core_ctx = ctx.to_core_connection_context();
        assert_eq!(core_ctx.connection.connection_id, ConnectionId::new(10));
        assert_eq!(core_ctx.connection.peer.address, addr);
    }
}
