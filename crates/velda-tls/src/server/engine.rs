//! Downstream TLS server engine and async handshake execution.

use std::sync::Arc;

use rustls::ServerConfig;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_rustls::TlsAcceptor;
use tokio_rustls::server::TlsStream;

use super::config::{ServerTlsConfig, TlsServerParams};
use super::handshake::{TlsHandshakeInfo, extract_handshake_info};
use super::resolver::SniConfigResolver;
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

/// Downstream TLS server engine holding compiled [`ServerConfig`] state and runtime parameters.
#[derive(Clone)]
pub struct TlsServerEngine {
    config: Arc<ServerConfig>,
    acceptor: TlsAcceptor,
    sni_configs: Option<Arc<SniConfigResolver>>,
    params: TlsServerParams,
}

impl std::fmt::Debug for TlsServerEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TlsServerEngine")
            .field("alpn_protocols", &self.config.alpn_protocols)
            .field("has_sni_configs", &self.sni_configs.is_some())
            .field("params", &self.params)
            .finish()
    }
}

impl TlsServerEngine {
    /// Builds a new downstream TLS server engine from server configurations using hardware probe.
    pub fn new(servers: &[ServerTlsConfig]) -> Result<Self, TlsError> {
        let params = TlsServerParams::from_hardware();
        Self::new_with_params(servers, &params)
    }

    /// Builds a new downstream TLS server engine from server configurations and explicit runtime parameters.
    pub fn new_with_params(
        servers: &[ServerTlsConfig],
        params: &TlsServerParams,
    ) -> Result<Self, TlsError> {
        let config = ServerTlsConfig::build_with_params(servers, params)?;
        let acceptor = TlsAcceptor::from(config.clone());
        let sni_configs = if servers.len() > 1 {
            let mut resolver = SniConfigResolver::new(config.clone());
            for server in servers {
                let single = server.build_single(params)?;
                resolver.add_config(&server.sni, single);
            }
            Some(Arc::new(resolver))
        } else {
            None
        };

        Ok(Self {
            config,
            acceptor,
            sni_configs,
            params: params.clone(),
        })
    }

    /// Creates an engine directly from an existing `Arc<ServerConfig>`.
    pub fn from_config(config: Arc<ServerConfig>) -> Self {
        Self::from_config_with_params(config, TlsServerParams::from_hardware())
    }

    /// Creates an engine directly from an existing `Arc<ServerConfig>` and explicit runtime parameters.
    pub fn from_config_with_params(config: Arc<ServerConfig>, params: TlsServerParams) -> Self {
        let acceptor = TlsAcceptor::from(config.clone());
        Self {
            config,
            acceptor,
            sni_configs: None,
            params,
        }
    }

    /// Attaches an explicit `SniConfigResolver` for multi-domain ALPN and certificate isolation.
    pub fn with_sni_configs(mut self, sni_configs: Arc<SniConfigResolver>) -> Self {
        self.sni_configs = Some(sni_configs);
        self
    }

    /// Returns a reference to the active `SniConfigResolver`, if multi-domain isolation is enabled.
    pub fn sni_configs(&self) -> Option<&Arc<SniConfigResolver>> {
        self.sni_configs.as_ref()
    }

    /// Performs downstream TLS termination on an incoming connection.
    ///
    /// If multiple domain certificates are configured (`sni_configs` is present),
    /// uses [`tokio_rustls::LazyConfigAcceptor`] to inspect the `ClientHello` SNI
    /// and negotiate domain-specific ALPN protocols (e.g. `["h2"]` vs `["http/1.1"]`).
    /// Otherwise, uses the zero-overhead fast-path direct acceptor.
    pub async fn accept<IO>(&self, stream: IO) -> Result<TlsStream<IO>, TlsError>
    where
        IO: AsyncRead + AsyncWrite + Unpin,
    {
        if let Some(ref resolver) = self.sni_configs {
            let lazy =
                tokio_rustls::LazyConfigAcceptor::new(rustls::server::Acceptor::default(), stream);
            let start = lazy
                .await
                .map_err(|e| TlsError::HandshakeFailed(e.to_string()))?;
            let config = {
                let ch = start.client_hello();
                resolver.resolve(ch.server_name())
            };
            start
                .into_stream(config)
                .await
                .map_err(|e| TlsError::HandshakeFailed(e.to_string()))
        } else {
            self.acceptor
                .accept(stream)
                .await
                .map_err(|e| TlsError::HandshakeFailed(e.to_string()))
        }
    }

    /// Performs downstream TLS termination on an incoming connection bounded by the configured handshake timeout.
    pub async fn accept_with_timeout<IO>(&self, stream: IO) -> Result<TlsStream<IO>, TlsError>
    where
        IO: AsyncRead + AsyncWrite + Unpin,
    {
        self.accept_with_explicit_timeout(stream, self.params.handshake_timeout)
            .await
    }

    /// Performs downstream TLS termination on an incoming connection bounded by an explicit handshake timeout.
    pub async fn accept_with_explicit_timeout<IO>(
        &self,
        stream: IO,
        timeout: std::time::Duration,
    ) -> Result<TlsStream<IO>, TlsError>
    where
        IO: AsyncRead + AsyncWrite + Unpin,
    {
        match tokio::time::timeout(timeout, self.accept(stream)).await {
            Ok(res) => res,
            Err(_) => Err(TlsError::HandshakeFailed(format!(
                "Downstream TLS handshake timed out after {:?}",
                timeout
            ))),
        }
    }

    /// Returns a reference to the active parameters.
    pub fn params(&self) -> &TlsServerParams {
        &self.params
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
