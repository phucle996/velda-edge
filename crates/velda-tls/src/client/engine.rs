//! Upstream TLS client execution engine and connector.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use rustls::ClientConfig;
use rustls::pki_types::ServerName;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;

use super::config::ClientTlsConfig;
use crate::error::TlsError;

/// Standalone stateless execution function: initiates an outbound TLS handshake on a stream using provided ClientConfig.
pub async fn connect<IO>(
    stream: IO,
    config: &Arc<ClientConfig>,
    sni: &str,
) -> Result<TlsStream<IO>, TlsError>
where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    let connector = TlsConnector::from(config.clone());
    let sni_trimmed = sni.trim().to_ascii_lowercase();
    let server_name = ServerName::try_from(sni_trimmed)
        .map_err(|_| TlsError::SniNotFound(format!("Invalid DNS name for SNI: {sni}")))?;

    connector
        .connect(server_name, stream)
        .await
        .map_err(|e| TlsError::HandshakeFailed(e.to_string()))
}

/// Upstream TLS client engine managing outbound TLS connections indexed by target SNI.
#[derive(Clone)]
pub struct TlsClientEngine {
    connectors: HashMap<String, (Arc<ClientConfig>, TlsConnector, ServerName<'static>)>,
}

impl fmt::Debug for TlsClientEngine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TlsClientEngine")
            .field(
                "configured_snis",
                &self.connectors.keys().collect::<Vec<_>>(),
            )
            .finish()
    }
}

impl TlsClientEngine {
    /// Builds upstream client engine from upstream TLS configurations.
    ///
    /// Validates all SNI DNS names immediately at compilation time to fail fast.
    pub fn new(upstreams: &[ClientTlsConfig]) -> Result<Self, TlsError> {
        let mut connectors = HashMap::new();

        for upstream in upstreams {
            let client_config = upstream.build()?;
            let connector = TlsConnector::from(client_config.clone());

            for sni in &upstream.sni {
                let trimmed = sni.trim().to_ascii_lowercase();
                let server_name = ServerName::try_from(trimmed.clone()).map_err(|_| {
                    TlsError::SniNotFound(format!("Invalid DNS name for upstream SNI: {sni}"))
                })?;
                connectors.insert(
                    trimmed,
                    (client_config.clone(), connector.clone(), server_name),
                );
            }
        }

        Ok(Self { connectors })
    }

    /// Connects to an upstream backend using TLS.
    pub async fn connect<IO>(&self, sni: &str, stream: IO) -> Result<TlsStream<IO>, TlsError>
    where
        IO: AsyncRead + AsyncWrite + Unpin,
    {
        let sni_key = sni.trim().to_ascii_lowercase();
        let (_, connector, server_name) = self
            .connectors
            .get(&sni_key)
            .ok_or_else(|| TlsError::UpstreamTargetNotFound(sni.to_string()))?;

        connector
            .connect(server_name.clone(), stream)
            .await
            .map_err(|e| TlsError::HandshakeFailed(e.to_string()))
    }

    /// Checks if a target SNI is supported by this client engine.
    pub fn contains_sni(&self, sni: &str) -> bool {
        self.connectors
            .contains_key(&sni.trim().to_ascii_lowercase())
    }

    /// Returns the compiled TLS connector and client config for a target SNI, if present.
    pub fn get_connector(&self, sni: &str) -> Option<(&Arc<ClientConfig>, &TlsConnector)> {
        self.connectors
            .get(&sni.trim().to_ascii_lowercase())
            .map(|(cfg, conn, _)| (cfg, conn))
    }

    /// Returns the number of configured upstream SNI hostnames.
    pub fn len(&self) -> usize {
        self.connectors.len()
    }

    /// Returns whether the upstream client engine is empty.
    pub fn is_empty(&self) -> bool {
        self.connectors.is_empty()
    }
}
