//! Traffic classification and protocol sniffing for incoming connections.

use std::fmt;
use tokio::net::TcpStream;

use crate::error::{Result, TransportError};

/// Identified traffic protocol/path kind for an incoming connection or datagram.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PathKind {
    /// Pure L4 raw streaming (direct byte forward to upstream, e.g. DB, custom TCP/UDP).
    L4Direct,
    /// TLS encrypted traffic (needs TLS termination via `velda-tls`).
    Tls,
    /// Cleartext HTTP/1.x traffic.
    Http1,
    /// Cleartext HTTP/2 traffic (HTTP/2 cleartext preface H2C).
    Http2,
    /// HTTP/3 traffic over QUIC/UDP.
    Http3,
    /// Generic cleartext HTTP traffic (HTTP/1 or HTTP/2).
    Http,
    /// QUIC packet for HTTP/3 over UDP.
    Quic,
    /// Traffic could not be definitively classified with current bytes.
    Unknown,
}

impl PathKind {
    /// Returns `true` if this path is any HTTP version (HTTP/1, HTTP/2, HTTP/3, or generic HTTP).
    #[inline]
    pub const fn is_http(&self) -> bool {
        matches!(self, Self::Http | Self::Http1 | Self::Http2 | Self::Http3)
    }

    /// Returns `true` if this path is HTTP/1.x.
    #[inline]
    pub const fn is_http1(&self) -> bool {
        matches!(self, Self::Http1)
    }

    /// Returns `true` if this path is HTTP/2.
    #[inline]
    pub const fn is_http2(&self) -> bool {
        matches!(self, Self::Http2)
    }

    /// Returns `true` if this path is HTTP/3 or QUIC.
    #[inline]
    pub const fn is_http3(&self) -> bool {
        matches!(self, Self::Http3 | Self::Quic)
    }

    /// Returns `true` if this path is raw L4 direct forwarding.
    #[inline]
    pub const fn is_l4(&self) -> bool {
        matches!(self, Self::L4Direct)
    }

    /// Returns `true` if this path requires TLS termination.
    #[inline]
    pub const fn is_tls(&self) -> bool {
        matches!(self, Self::Tls)
    }
}

impl fmt::Display for PathKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::L4Direct => f.write_str("L4Direct"),
            Self::Tls => f.write_str("Tls"),
            Self::Http1 => f.write_str("Http1"),
            Self::Http2 => f.write_str("Http2"),
            Self::Http3 => f.write_str("Http3"),
            Self::Http => f.write_str("Http"),
            Self::Quic => f.write_str("Quic"),
            Self::Unknown => f.write_str("Unknown"),
        }
    }
}

/// Inspects bytes peeked from a stream to determine if it is TLS handshake or raw L4 stream.
pub fn classify_bytes(bytes: &[u8]) -> PathKind {
    if bytes.len() >= 3 {
        // TLS record header: ContentType::handshake (0x16), Major version (0x03), Minor (0x00..=0x04)
        if bytes[0] == 0x16 && bytes[1] == 0x03 && bytes[2] <= 0x04 {
            return PathKind::Tls;
        }
    }

    if bytes.len() >= 16 {
        // Sufficient bytes peeked without matching TLS record -> raw L4 protocol
        PathKind::L4Direct
    } else {
        PathKind::Unknown
    }
}

/// Peeks into a [`TcpStream`] to classify incoming traffic without consuming socket bytes.
pub async fn peek_and_classify(stream: &TcpStream) -> Result<PathKind> {
    let mut buf = [0u8; 32];
    let n = stream.peek(&mut buf).await.map_err(TransportError::Io)?;

    if n == 0 {
        return Ok(PathKind::Unknown);
    }

    Ok(classify_bytes(&buf[..n]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_classify_tls_handshake() {
        // Standard TLS 1.2 / 1.3 ClientHello record starts with 0x16, 0x03, 0x01 / 0x03
        let tls_record = [0x16, 0x03, 0x01, 0x00, 0xa5, 0x01, 0x00];
        assert_eq!(classify_bytes(&tls_record), PathKind::Tls);

        let tls13_record = [0x16, 0x03, 0x03, 0x01, 0x00];
        assert_eq!(classify_bytes(&tls13_record), PathKind::Tls);
    }

    #[test]
    fn test_classify_l4_raw() {
        // Postgres SSLRequest / startup packet or arbitrary TCP binary stream
        let postgres_startup = [
            0x00, 0x00, 0x00, 0x08, 0x04, 0xd2, 0x16, 0x2f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00,
        ];
        assert_eq!(classify_bytes(&postgres_startup), PathKind::L4Direct);
        assert!(PathKind::L4Direct.is_l4());
        assert!(!PathKind::L4Direct.is_http());
    }

    #[test]
    fn test_classify_insufficient_bytes() {
        let short_bytes = [0x42, 0x43];
        assert_eq!(classify_bytes(&short_bytes), PathKind::Unknown);
    }
}
