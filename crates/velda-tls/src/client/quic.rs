//! QUIC TLS client configuration bridge.

use std::sync::Arc;

use quinn_proto::ClientConfig as QuinnClientConfig;
use quinn_proto::crypto::rustls::QuicClientConfig;
use rustls::ClientConfig as RustlsClientConfig;

use crate::error::TlsError;

/// Compiles an active [`RustlsClientConfig`] into a QUIC [`QuinnClientConfig`].
pub fn build_quic_client_config(
    rustls_config: Arc<RustlsClientConfig>,
) -> Result<QuinnClientConfig, TlsError> {
    let quic_crypto = QuicClientConfig::try_from(rustls_config)
        .map_err(|e| TlsError::HandshakeFailed(e.to_string()))?;
    Ok(QuinnClientConfig::new(Arc::new(quic_crypto)))
}
