//! Upstream backend TLS client configuration.

use std::sync::Arc;

use rustls::ClientConfig;

use crate::error::TlsError;
use crate::pem::{parse_ca_bundle_pem, parse_certs_pem, parse_private_key_pem};

/// Upstream backend TLS configuration for initiating secure outbound connections.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientTlsConfig {
    /// Target SNI hostnames to match and send in the ClientHello.
    pub sni: Vec<String>,
    /// Allowed TLS protocol versions (e.g. "tls1.2", "tls1.3").
    pub versions: Vec<String>,
    /// Supported ALPN identifiers (e.g. "h2", "http/1.1").
    pub alpn: Vec<String>,
    /// Optional custom CA certificate bundle in PEM format to verify backend certificates.
    pub ca_pem: Option<String>,
    /// Optional client certificate chain in PEM format for upstream mTLS.
    pub client_cert_pem: Option<String>,
    /// Optional client private key in PEM format for upstream mTLS.
    pub client_key_pem: Option<String>,
}

impl ClientTlsConfig {
    /// Compiles a single `ClientTlsConfig` definition into an `Arc<ClientConfig>`.
    pub fn build_client_config(&self) -> Result<Arc<ClientConfig>, TlsError> {
        let mut root_store = rustls::RootCertStore::empty();
        if let Some(ca_pem) = &self.ca_pem {
            root_store = parse_ca_bundle_pem(ca_pem)?;
        }

        let builder =
            ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
                .with_safe_default_protocol_versions()
                .map_err(|e| TlsError::InvalidCertificate(e.to_string()))?
                .with_root_certificates(root_store);

        let mut client_config = match (&self.client_cert_pem, &self.client_key_pem) {
            (Some(cert_pem), Some(key_pem)) => {
                let certs = parse_certs_pem(cert_pem)?;
                let key = parse_private_key_pem(key_pem)?;
                builder
                    .with_client_auth_cert(certs, key)
                    .map_err(|e| TlsError::InvalidPrivateKey(e.to_string()))?
            }
            _ => builder.with_no_client_auth(),
        };

        if !self.alpn.is_empty() {
            client_config.alpn_protocols =
                self.alpn.iter().map(|s| s.as_bytes().to_vec()).collect();
        } else {
            client_config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
        }

        Ok(Arc::new(client_config))
    }
}
