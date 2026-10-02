//! Unified TLS Engine coordinating Downstream server termination and Upstream client connectors.

use tokio::io::{AsyncRead, AsyncWrite};
use tokio_rustls::client::TlsStream as ClientTlsStream;
use tokio_rustls::server::TlsStream as ServerTlsStream;

use crate::client::{ClientTlsConfig, TlsClientEngine};
use crate::error::TlsError;
use crate::server::{ServerTlsConfig, TlsHandshakeInfo, TlsServerEngine, TlsServerParams};

/// Self-contained in-memory TLS engine ready for hot-path zero-IO execution.
#[derive(Debug, Clone)]
pub struct TlsEngine {
    server: Option<TlsServerEngine>,
    client: TlsClientEngine,
}

impl TlsEngine {
    /// Builds a new `TlsEngine` from server configurations and upstream client configurations using hardware probe.
    pub fn new(
        servers: &[ServerTlsConfig],
        upstreams: &[ClientTlsConfig],
    ) -> Result<Self, TlsError> {
        let params = TlsServerParams::from_hardware();
        Self::new_with_params(servers, upstreams, &params)
    }

    /// Builds a new `TlsEngine` from server configurations, upstream client configurations, and explicit server parameters.
    pub fn new_with_params(
        servers: &[ServerTlsConfig],
        upstreams: &[ClientTlsConfig],
        params: &TlsServerParams,
    ) -> Result<Self, TlsError> {
        let server = if !servers.is_empty() {
            Some(TlsServerEngine::new_with_params(servers, params)?)
        } else {
            None
        };

        let client = TlsClientEngine::new(upstreams)?;

        Ok(Self { server, client })
    }

    /// Performs downstream TLS termination on an incoming connection bounded by the hardware-tuned handshake timeout
    /// (mitigating Slowloris connection exhaustion attacks).
    pub async fn accept<IO>(&self, stream: IO) -> Result<ServerTlsStream<IO>, TlsError>
    where
        IO: AsyncRead + AsyncWrite + Unpin,
    {
        let server = self.server.as_ref().ok_or_else(|| {
            TlsError::HandshakeFailed("No downstream TLS servers configured".into())
        })?;
        server.accept_with_timeout(stream).await
    }

    /// Performs upstream TLS connection to a target SNI backend.
    pub async fn connect<IO>(&self, sni: &str, stream: IO) -> Result<ClientTlsStream<IO>, TlsError>
    where
        IO: AsyncRead + AsyncWrite + Unpin,
    {
        self.client.connect(sni, stream).await
    }

    /// Returns a reference to the downstream server engine, if configured.
    pub fn server(&self) -> Option<&TlsServerEngine> {
        self.server.as_ref()
    }

    /// Returns a reference to the upstream client engine.
    pub fn client(&self) -> &TlsClientEngine {
        &self.client
    }

    /// Extracts connection metadata after a successful downstream TLS handshake.
    pub fn extract_handshake_info<IO>(stream: &ServerTlsStream<IO>) -> TlsHandshakeInfo {
        TlsServerEngine::extract_handshake_info(stream)
    }
}
