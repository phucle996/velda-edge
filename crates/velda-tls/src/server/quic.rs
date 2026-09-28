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
    let quic_crypto = QuicServerConfig::try_from(rustls_config)
        .map_err(|e| TlsError::HandshakeFailed(e.to_string()))?;
    Ok(QuinnServerConfig::with_crypto(Arc::new(quic_crypto)))
}
