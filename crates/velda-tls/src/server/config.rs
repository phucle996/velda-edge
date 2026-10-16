//! Downstream server TLS configuration and ServerConfig builder.

use std::sync::Arc;
use std::time::Duration;

use rustls::ServerConfig;
use rustls::server::WebPkiClientVerifier;

use super::resolver::SniResolver;
use crate::error::TlsError;
use crate::pem::{parse_certs_pem, parse_private_key_pem};

/// Downstream server TLS certificate configuration for SNI matching and termination.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerTlsConfig {
    /// Server Name Indication (SNI) hostnames supported by this certificate.
    pub sni: Vec<String>,
    /// Allowed TLS protocol versions (e.g. "tls1.2", "tls1.3").
    pub versions: Vec<String>,
    /// Supported ALPN identifiers (e.g. "h2", "http/1.1").
    pub alpn: Vec<String>,
    /// Leaf certificate and intermediate CA chain in PEM format.
    pub cert_pem: String,
    /// Private key in PEM format.
    pub key_pem: String,
    /// Optional CA certificate bundle in PEM format for client mTLS authentication.
    pub client_ca_pem: Option<String>,
}

/// Tuning parameters for downstream TLS server termination and session management.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TlsServerParams {
    /// In-memory TLS session resumption cache capacity (number of sessions).
    pub session_cache_capacity: usize,
    /// Number of cache-aligned shards partitioning session storage.
    pub session_shards: usize,
    /// Maximum size in bytes of TLS 1.3 0-RTT early data accepted from clients.
    pub max_early_data_size: u32,
    /// Number of single-use TLS 1.3 session tickets issued per handshake.
    pub send_tls13_tickets: usize,
    /// Maximum duration allowed for downstream TLS handshake completion before dropping connection.
    pub handshake_timeout: Duration,
}

impl TlsServerParams {
    /// Creates parameters sized appropriately for the host hardware topology.
    pub fn for_topology(topo: &velda_core::HardwareTopology) -> Self {
        Self {
            session_cache_capacity: topo.sizing.tls_session_cache_capacity,
            session_shards: topo.sizing.concurrency_shards,
            max_early_data_size: topo.sizing.tls_max_early_data_size,
            send_tls13_tickets: topo.sizing.tls_send_tls13_tickets as usize,
            handshake_timeout: Duration::from_secs(if topo.available_cores >= 8 { 5 } else { 10 }),
        }
    }

    /// Creates parameters sized using the global hardware topology probe.
    pub fn from_hardware() -> Self {
        Self::for_topology(velda_core::global_hardware_topology())
    }

    /// Sets the session cache capacity.
    pub fn with_session_cache_capacity(mut self, capacity: usize) -> Self {
        self.session_cache_capacity = capacity;
        self
    }

    /// Sets the number of session shards.
    pub fn with_session_shards(mut self, shards: usize) -> Self {
        self.session_shards = shards;
        self
    }

    /// Sets the max early data size.
    pub fn with_max_early_data_size(mut self, size: u32) -> Self {
        self.max_early_data_size = size;
        self
    }

    /// Sets the number of TLS 1.3 tickets sent.
    pub fn with_send_tls13_tickets(mut self, tickets: usize) -> Self {
        self.send_tls13_tickets = tickets;
        self
    }

    /// Sets the handshake timeout.
    pub fn with_handshake_timeout(mut self, timeout: Duration) -> Self {
        self.handshake_timeout = timeout;
        self
    }
}

impl ServerTlsConfig {
    /// Compiles a list of `ServerTlsConfig` definitions into an `Arc<ServerConfig>`
    /// using hardware-probed parameters.
    #[inline]
    pub fn build(servers: &[Self]) -> Result<Arc<ServerConfig>, TlsError> {
        Self::build_with_params(servers, &TlsServerParams::from_hardware())
    }

    /// Compiles a list of `ServerTlsConfig` definitions into an `Arc<ServerConfig>`
    /// using explicit [`TlsServerParams`].
    pub fn build_with_params(
        servers: &[Self],
        params: &TlsServerParams,
    ) -> Result<Arc<ServerConfig>, TlsError> {
        let mut sni_resolver = SniResolver::new();
        let mut client_root_store = rustls::RootCertStore::empty();
        // Note: rustls enforces client_cert_verifier at the ServerConfig level.
        // In multi-tenant setups, listeners with distinct mTLS policies should compile separate ServerConfigs.
        let mut requires_client_auth = false;
        let mut alpn_protocols = Vec::new();
        let mut all_versions = Vec::new();

        for server in servers {
            let certs = parse_certs_pem(&server.cert_pem)?;
            let key = parse_private_key_pem(&server.key_pem)?;
            sni_resolver.add_certificate(&server.sni, certs, key)?;

            if let Some(ca_pem) = &server.client_ca_pem {
                let ca_certs = parse_certs_pem(ca_pem)?;
                for cert in ca_certs {
                    client_root_store
                        .add(cert)
                        .map_err(|e| TlsError::InvalidCaBundle(e.to_string()))?;
                }
                requires_client_auth = true;
            }

            for alpn in &server.alpn {
                let alpn_bytes = alpn.as_bytes().to_vec();
                if !alpn_protocols.contains(&alpn_bytes) {
                    alpn_protocols.push(alpn_bytes);
                }
            }

            for v in &server.versions {
                if !all_versions.contains(v) {
                    all_versions.push(v.clone());
                }
            }
        }

        let protocol_versions = crate::version::resolve_protocol_versions(&all_versions)?;

        let builder = ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::aws_lc_rs::default_provider(),
        ))
        .with_protocol_versions(&protocol_versions)
        .map_err(|e| TlsError::InvalidCertificate(e.to_string()))?;

        let mut server_config = if requires_client_auth {
            let verifier = WebPkiClientVerifier::builder_with_provider(
                Arc::new(client_root_store),
                Arc::new(rustls::crypto::aws_lc_rs::default_provider()),
            )
            .build()
            .map_err(|e| TlsError::InvalidCaBundle(e.to_string()))?;
            builder
                .with_client_cert_verifier(verifier)
                .with_cert_resolver(Arc::new(sni_resolver))
        } else {
            builder
                .with_no_client_auth()
                .with_cert_resolver(Arc::new(sni_resolver))
        };

        server_config.alpn_protocols = alpn_protocols;
        server_config.max_early_data_size = params.max_early_data_size;
        server_config.send_tls13_tickets = params.send_tls13_tickets;
        server_config.session_storage =
            crate::server::session::ShardedServerSessionCache::with_shards(
                params.session_cache_capacity,
                params.session_shards,
            );

        Ok(Arc::new(server_config))
    }

    /// Compiles a single `ServerTlsConfig` definition into an `Arc<ServerConfig>`
    /// configured strictly with this certificate's specific ALPN protocols and versions.
    pub fn build_single(&self, params: &TlsServerParams) -> Result<Arc<ServerConfig>, TlsError> {
        let mut sni_resolver = SniResolver::new();
        let certs = parse_certs_pem(&self.cert_pem)?;
        let key = parse_private_key_pem(&self.key_pem)?;
        sni_resolver.add_certificate(&self.sni, certs, key)?;

        let mut client_root_store = rustls::RootCertStore::empty();
        let mut requires_client_auth = false;

        if let Some(ca_pem) = &self.client_ca_pem {
            let ca_certs = parse_certs_pem(ca_pem)?;
            for cert in ca_certs {
                client_root_store
                    .add(cert)
                    .map_err(|e| TlsError::InvalidCaBundle(e.to_string()))?;
            }
            requires_client_auth = true;
        }

        let alpn_protocols: Vec<Vec<u8>> =
            self.alpn.iter().map(|a| a.as_bytes().to_vec()).collect();
        let protocol_versions = crate::version::resolve_protocol_versions(&self.versions)?;

        let builder = ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::aws_lc_rs::default_provider(),
        ))
        .with_protocol_versions(&protocol_versions)
        .map_err(|e| TlsError::InvalidCertificate(e.to_string()))?;

        let mut server_config = if requires_client_auth {
            let verifier = WebPkiClientVerifier::builder_with_provider(
                Arc::new(client_root_store),
                Arc::new(rustls::crypto::aws_lc_rs::default_provider()),
            )
            .build()
            .map_err(|e| TlsError::InvalidCaBundle(e.to_string()))?;
            builder
                .with_client_cert_verifier(verifier)
                .with_cert_resolver(Arc::new(sni_resolver))
        } else {
            builder
                .with_no_client_auth()
                .with_cert_resolver(Arc::new(sni_resolver))
        };

        server_config.alpn_protocols = alpn_protocols;
        server_config.max_early_data_size = params.max_early_data_size;
        server_config.send_tls13_tickets = params.send_tls13_tickets;
        server_config.session_storage =
            crate::server::session::ShardedServerSessionCache::with_shards(
                params.session_cache_capacity,
                params.session_shards,
            );

        Ok(Arc::new(server_config))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tls_server_params_for_topology() {
        let topo_1c_1g =
            velda_core::HardwareTopology::with_workers_and_memory(1, 1024 * 1024 * 1024);
        let p_1c = TlsServerParams::for_topology(&topo_1c_1g);
        assert_eq!(
            p_1c.session_cache_capacity,
            topo_1c_1g.sizing.tls_session_cache_capacity
        );
        assert_eq!(p_1c.session_shards, topo_1c_1g.sizing.concurrency_shards);
        assert_eq!(p_1c.handshake_timeout, Duration::from_secs(10));

        let topo_16c_32g =
            velda_core::HardwareTopology::with_workers_and_memory(16, 32 * 1024 * 1024 * 1024);
        let p_16c = TlsServerParams::for_topology(&topo_16c_32g);
        assert_eq!(
            p_16c.session_cache_capacity,
            topo_16c_32g.sizing.tls_session_cache_capacity
        );
        assert_eq!(p_16c.session_shards, topo_16c_32g.sizing.concurrency_shards);
        assert_eq!(p_16c.handshake_timeout, Duration::from_secs(5));
    }
}
