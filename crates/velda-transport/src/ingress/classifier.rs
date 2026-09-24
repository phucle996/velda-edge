//! Traffic classification and protocol sniffing for incoming connections.

use std::fmt;
use tokio::net::TcpStream;

use crate::error::{Result, TransportError};

/// Identified traffic protocol/path kind for an incoming connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PathKind {
    /// Pure L4 raw streaming (direct byte forward to upstream, e.g. DB, custom TCP).
    L4Direct,
    /// TLS encrypted traffic (needs TLS termination via `velda-tls`).
    Tls,
    /// Cleartext HTTP traffic (HTTP/1.1 or HTTP/2 cleartext preface H2C).
    Http,
    /// Traffic could not be definitively classified with current bytes.
    Unknown,
}

impl fmt::Display for PathKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::L4Direct => f.write_str("L4Direct"),
            Self::Tls => f.write_str("Tls"),
            Self::Http => f.write_str("Http"),
            Self::Unknown => f.write_str("Unknown"),
        }
    }
}

/// Known HTTP/1.1 method prefixes for sniffing cleartext HTTP.
const HTTP_METHODS: &[&[u8]] = &[
    b"GET ",
    b"POST ",
    b"PUT ",
    b"DELETE ",
    b"HEAD ",
    b"OPTIONS ",
    b"CONNECT ",
    b"PATCH ",
    b"TRACE ",
];

/// HTTP/2 client connection preface magic string.
const HTTP2_PREFACE_PREFIX: &[u8] = b"PRI * HTTP/2.0";

/// Inspects bytes peeked from a stream to determine its likely [`PathKind`].
pub fn classify_bytes(bytes: &[u8]) -> PathKind {
    if bytes.len() >= 3 {
        // TLS record header: ContentType::handshake (0x16), Major version (0x03), Minor (0x00..=0x04)
        if bytes[0] == 0x16 && bytes[1] == 0x03 && bytes[2] <= 0x04 {
            return PathKind::Tls;
        }
    }

    if bytes.len() >= 14 && bytes.starts_with(HTTP2_PREFACE_PREFIX) {
        return PathKind::Http;
    }

    for method in HTTP_METHODS {
        if bytes.len() >= method.len() && bytes.starts_with(method) {
            return PathKind::Http;
        }
    }

    if bytes.len() >= 16 {
        // Sufficient bytes peeked without matching TLS or HTTP headers -> L4 raw protocol
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
    fn test_classify_http1() {
        assert_eq!(
            classify_bytes(b"GET /index.html HTTP/1.1\r\n"),
            PathKind::Http
        );
        assert_eq!(
            classify_bytes(b"POST /api/v1/resource HTTP/1.1\r\n"),
            PathKind::Http
        );
        assert_eq!(
            classify_bytes(b"DELETE /item/1 HTTP/1.1\r\n"),
            PathKind::Http
        );
        assert_eq!(classify_bytes(b"OPTIONS * HTTP/1.1\r\n"), PathKind::Http);
    }

    #[test]
    fn test_classify_http2() {
        let h2_preface = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";
        assert_eq!(classify_bytes(h2_preface), PathKind::Http);
    }

    #[test]
    fn test_classify_l4_raw() {
        // Postgres SSLRequest / startup packet or arbitrary TCP binary stream
        let postgres_startup = [
            0x00, 0x00, 0x00, 0x08, 0x04, 0xd2, 0x16, 0x2f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00,
        ];
        assert_eq!(classify_bytes(&postgres_startup), PathKind::L4Direct);
    }

    #[test]
    fn test_classify_insufficient_bytes() {
        let short_bytes = [0x42, 0x43];
        assert_eq!(classify_bytes(&short_bytes), PathKind::Unknown);
    }
}
