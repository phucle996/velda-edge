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
    // Maintain exact case-sensitivity on SNI target hostname
    let sni_trimmed = sni.trim();
    let server_name = ServerName::try_from(sni_trimmed.to_string())
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
    /// Preserves exact case-sensitive SNI hostnames to prevent accidental cross-domain collisions.
    pub fn new(upstreams: &[ClientTlsConfig]) -> Result<Self, TlsError> {
        let mut connectors = HashMap::new();

        for upstream in upstreams {
            let client_config = upstream.build()?;
            let connector = TlsConnector::from(client_config.clone());

            for sni in &upstream.sni {
                let trimmed = sni.trim();
                if trimmed.starts_with('*') {
                    continue;
                }
                let server_name = ServerName::try_from(trimmed.to_string()).map_err(|_| {
                    TlsError::SniNotFound(format!("Invalid DNS name for upstream SNI: {sni}"))
                })?;
                connectors.insert(
                    trimmed.to_string(),
                    (client_config.clone(), connector.clone(), server_name),
                );
            }
        }

        Ok(Self { connectors })
    }

    /// Connects to an upstream backend using TLS.
    ///
    /// Performs exact case-sensitive lookup without heap allocation or lossy case folding.
    pub async fn connect<IO>(&self, sni: &str, stream: IO) -> Result<TlsStream<IO>, TlsError>
    where
        IO: AsyncRead + AsyncWrite + Unpin,
    {
        let trimmed = sni.trim();
        let connector_entry = self.connectors.get(trimmed);

        let (_, connector, server_name) =
            connector_entry.ok_or_else(|| TlsError::UpstreamTargetNotFound(sni.to_string()))?;

        connector
            .connect(server_name.clone(), stream)
            .await
            .map_err(|e| TlsError::HandshakeFailed(e.to_string()))
    }

    /// Checks if a target SNI is supported by this client engine (case-sensitive).
    #[inline]
    pub fn contains_sni(&self, sni: &str) -> bool {
        self.connectors.contains_key(sni.trim())
    }

    /// Returns the compiled TLS connector and client config for a target SNI, if present (case-sensitive).
    #[inline]
    pub fn get_connector(&self, sni: &str) -> Option<(&Arc<ClientConfig>, &TlsConnector)> {
        self.connectors
            .get(sni.trim())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_engine_strict_case_sensitive() {
        let upstream = ClientTlsConfig {
            sni: vec!["Service.Internal".to_string()],
            insecure_skip_verify: true,
            ..Default::default()
        };
        let engine = TlsClientEngine::new(&[upstream]).expect("Valid client engine");

        // Exact case matches
        assert!(engine.contains_sni("Service.Internal"));
        assert!(engine.get_connector("Service.Internal").is_some());

        // Different casing does NOT match
        assert!(!engine.contains_sni("service.internal"));
        assert!(engine.get_connector("service.internal").is_none());
        assert!(!engine.contains_sni("SERVICE.INTERNAL"));
    }
}
