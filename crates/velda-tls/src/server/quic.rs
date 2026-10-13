//! QUIC TLS server configuration bridge.

use quinn_proto::ServerConfig as QuinnServerConfig;
use quinn_proto::crypto::rustls::QuicServerConfig;
use rustls::ServerConfig as RustlsServerConfig;
use std::sync::Arc;

use crate::error::TlsError;

/// Compiles an active [`RustlsServerConfig`] into a QUIC [`QuinnServerConfig`].
pub fn build_quic_server_config(
    rustls_config: Arc<RustlsServerConfig>,
) -> Result<QuinnServerConfig, TlsError> {
    // RFC 9001 / rustls QUIC invariant:
    // QUIC sessions require max_early_data_size to be exactly 0 (disabled) or u32::MAX (enabled).
    // Intermediate byte thresholds configured for TCP TLS (e.g. 4096, 8192) cause
    // rustls::quic::ServerConnection::new() to panic during handshake.
    let rustls_config = if (rustls_config.max_early_data_size != 0
        && rustls_config.max_early_data_size != u32::MAX)
        || (!rustls_config.alpn_protocols.iter().any(|p| p == b"h3"))
    {
        let mut cloned = (*rustls_config).clone();
        if cloned.max_early_data_size != 0 && cloned.max_early_data_size != u32::MAX {
            cloned.max_early_data_size = u32::MAX;
        }
        if !cloned.alpn_protocols.iter().any(|p| p == b"h3") {
            cloned.alpn_protocols.push(b"h3".to_vec());
        }
        Arc::new(cloned)
    } else {
        rustls_config
    };

    let quic_crypto = QuicServerConfig::try_from(rustls_config)
        .map_err(|e| TlsError::HandshakeFailed(e.to_string()))?;
    Ok(QuinnServerConfig::with_crypto(Arc::new(quic_crypto)))
}
