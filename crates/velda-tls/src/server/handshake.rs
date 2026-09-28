//! Handshake inspection and metadata extraction.

use rustls::pki_types::CertificateDer;
use tokio_rustls::server::TlsStream;

/// Metadata captured from a successful TLS handshake.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TlsHandshakeInfo {
    /// Negotiated Application-Layer Protocol Negotiation (e.g. "h2", "http/1.1").
    pub alpn: Option<String>,
    /// Server Name Indication presented by the client.
    pub sni: Option<String>,
    /// Client certificates presented during mutual TLS (mTLS) authentication.
    pub peer_certs: Option<Vec<CertificateDer<'static>>>,
}

/// Extracts handshake metadata from an established server TLS stream.
pub fn extract_handshake_info<IO>(stream: &TlsStream<IO>) -> TlsHandshakeInfo {
    let (_, server_conn) = stream.get_ref();
    let alpn = server_conn
        .alpn_protocol()
        .map(|b| String::from_utf8_lossy(b).to_string());
    let sni = server_conn.server_name().map(|s| s.to_string());
    let peer_certs = server_conn.peer_certificates().map(|certs| certs.to_vec());

    TlsHandshakeInfo {
        alpn,
        sni,
        peer_certs,
    }
}
