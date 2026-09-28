//! In-memory HTTP/3 engine compilation from TLS server engine.

use std::sync::Arc;
use tokio::sync::Mutex;
use velda_http::Http3Engine;
use velda_tls::TlsServerEngine;

/// Compiles a shared in-memory [`Http3Engine`] from the compiled downstream TLS engine.
pub(crate) fn compile_h3_engine(
    tls_server: Option<&TlsServerEngine>,
) -> Option<Arc<Mutex<Http3Engine>>> {
    let tls = tls_server?;
    match tls.build_quic_config() {
        Ok(quic_cfg) => Some(Arc::new(Mutex::new(Http3Engine::new(Arc::new(quic_cfg))))),
        Err(e) => {
            tracing::warn!(error = %e, "Failed to compile HTTP/3 server configuration from TLS config");
            None
        }
    }
}
