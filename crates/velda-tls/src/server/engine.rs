//! Downstream TLS server engine and async handshake execution.

use std::sync::Arc;

use rustls::ServerConfig;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_rustls::TlsAcceptor;
use tokio_rustls::server::TlsStream;

use super::config::ServerTlsConfig;
use super::handshake::{TlsHandshakeInfo, extract_handshake_info};
use crate::error::TlsError;

/// Standalone stateless execution function: performs downstream TLS termination on a stream using provided ServerConfig.
pub async fn accept<IO>(stream: IO, config: &Arc<ServerConfig>) -> Result<TlsStream<IO>, TlsError>
where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    let acceptor = TlsAcceptor::from(config.clone());
    acceptor
        .accept(stream)
        .await
        .map_err(|e| TlsError::HandshakeFailed(e.to_string()))
}

/// Downstream TLS server engine wrapper holding a compiled `Arc<ServerConfig>`.
#[derive(Clone)]
pub struct TlsServerEngine {
    config: Arc<ServerConfig>,
    acceptor: TlsAcceptor,
}

impl std::fmt::Debug for TlsServerEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TlsServerEngine")
            .field("alpn_protocols", &self.config.alpn_protocols)
            .finish()
    }
}

impl TlsServerEngine {
    /// Builds a new downstream TLS server engine from server configurations.
    pub fn new(servers: &[ServerTlsConfig]) -> Result<Self, TlsError> {
        let config = ServerTlsConfig::build(servers)?;
        let acceptor = TlsAcceptor::from(config.clone());
        Ok(Self { config, acceptor })
    }

    /// Creates an engine directly from an existing `Arc<ServerConfig>`.
    pub fn from_config(config: Arc<ServerConfig>) -> Self {
        let acceptor = TlsAcceptor::from(config.clone());
        Self { config, acceptor }
    }

    /// Performs downstream TLS termination on an incoming connection.
    pub async fn accept<IO>(&self, stream: IO) -> Result<TlsStream<IO>, TlsError>
    where
        IO: AsyncRead + AsyncWrite + Unpin,
    {
        self.acceptor
            .accept(stream)
            .await
            .map_err(|e| TlsError::HandshakeFailed(e.to_string()))
    }

    /// Returns a reference to the compiled `ServerConfig`.
    pub fn config(&self) -> &Arc<ServerConfig> {
        &self.config
    }

    /// Compiles a QUIC [`quinn_proto::ServerConfig`] sharing this engine's TLS state,
    /// certificates, and unified session resumption cache / 0-RTT tickets.
    pub fn build_quic_config(&self) -> Result<quinn_proto::ServerConfig, TlsError> {
        crate::server::quic::build_quic_server_config(self.config.clone())
    }

    /// Extracts connection metadata after a successful downstream TLS handshake.
    pub fn extract_handshake_info<IO>(stream: &TlsStream<IO>) -> TlsHandshakeInfo {
        extract_handshake_info(stream)
    }
}
