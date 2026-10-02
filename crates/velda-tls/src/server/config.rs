//! Downstream server TLS configuration and ServerConfig builder.

use std::sync::Arc;
use std::time::Duration;

use rustls::ServerConfig;
use rustls::server::WebPkiClientVerifier;
use velda_core::hardware::{CpuTier, MemoryTier};

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
    /// Maximum size in bytes of TLS 1.3 0-RTT early data accepted from clients.
    pub max_early_data_size: u32,
    /// Number of single-use TLS 1.3 session tickets issued per handshake.
    pub send_tls13_tickets: usize,
    /// Maximum duration allowed for downstream TLS handshake completion before dropping connection.
    pub handshake_timeout: Duration,
}

impl TlsServerParams {
    /// Creates parameters sized appropriately for the host's [`CpuTier`] and [`MemoryTier`].
    pub fn for_tiers(cpu: CpuTier, mem: MemoryTier) -> Self {
        let (session_cache_capacity, max_early_data_size) = match mem {
            MemoryTier::Constrained => (1024, 0),
            MemoryTier::Small => (2 * 1024, 4 * 1024),
            MemoryTier::Medium => (8 * 1024, 8 * 1024),
            MemoryTier::Large => (16 * 1024, 8 * 1024),
            MemoryTier::XLarge => (32 * 1024, 16 * 1024),
            MemoryTier::TwoXLarge => (64 * 1024, 16 * 1024),
            MemoryTier::Ultra => (128 * 1024, 32 * 1024),
        };

        let (send_tls13_tickets, timeout_secs) = match cpu {
            CpuTier::Constrained => (1, 10),
            CpuTier::Small => (2, 8),
            CpuTier::Medium => (4, 5),
            CpuTier::Large => (4, 5),
            CpuTier::XLarge => (6, 3),
            CpuTier::TwoXLarge => (8, 3),
            CpuTier::Ultra => (8, 2),
        };

        Self {
            session_cache_capacity,
            max_early_data_size,
            send_tls13_tickets,
            handshake_timeout: Duration::from_secs(timeout_secs),
        }
    }

    /// Creates parameters sized using the global hardware topology probe.
    pub fn from_hardware() -> Self {
        let hw = velda_core::global_hardware_topology();
        Self::for_tiers(hw.cpu_tier(), hw.memory_tier())
    }

    /// Sets the session cache capacity.
    pub fn with_session_cache_capacity(mut self, capacity: usize) -> Self {
        self.session_cache_capacity = capacity;
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

        let builder =
            ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
                .with_protocol_versions(&protocol_versions)
                .map_err(|e| TlsError::InvalidCertificate(e.to_string()))?;

        let mut server_config = if requires_client_auth {
            let verifier = WebPkiClientVerifier::builder(Arc::new(client_root_store))
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
            rustls::server::ServerSessionMemoryCache::new(params.session_cache_capacity);

        Ok(Arc::new(server_config))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tls_server_params_across_all_tiers() {
        // Constrained
        let p_constrained =
            TlsServerParams::for_tiers(CpuTier::Constrained, MemoryTier::Constrained);
        assert_eq!(p_constrained.session_cache_capacity, 1024);
        assert_eq!(p_constrained.max_early_data_size, 0);
        assert_eq!(p_constrained.send_tls13_tickets, 1);
        assert_eq!(p_constrained.handshake_timeout, Duration::from_secs(10));

        // Small
        let p_small = TlsServerParams::for_tiers(CpuTier::Small, MemoryTier::Small);
        assert_eq!(p_small.session_cache_capacity, 2048);
        assert_eq!(p_small.max_early_data_size, 4096);
        assert_eq!(p_small.send_tls13_tickets, 2);
        assert_eq!(p_small.handshake_timeout, Duration::from_secs(8));

        // Medium
        let p_med = TlsServerParams::for_tiers(CpuTier::Medium, MemoryTier::Medium);
        assert_eq!(p_med.session_cache_capacity, 8192);
        assert_eq!(p_med.max_early_data_size, 8192);
        assert_eq!(p_med.send_tls13_tickets, 4);
        assert_eq!(p_med.handshake_timeout, Duration::from_secs(5));

        // Large
        let p_large = TlsServerParams::for_tiers(CpuTier::Large, MemoryTier::Large);
        assert_eq!(p_large.session_cache_capacity, 16384);
        assert_eq!(p_large.max_early_data_size, 8192);
        assert_eq!(p_large.send_tls13_tickets, 4);
        assert_eq!(p_large.handshake_timeout, Duration::from_secs(5));

        // XLarge
        let p_xlarge = TlsServerParams::for_tiers(CpuTier::XLarge, MemoryTier::XLarge);
        assert_eq!(p_xlarge.session_cache_capacity, 32768);
        assert_eq!(p_xlarge.max_early_data_size, 16384);
        assert_eq!(p_xlarge.send_tls13_tickets, 6);
        assert_eq!(p_xlarge.handshake_timeout, Duration::from_secs(3));

        // TwoXLarge
        let p_2xlarge = TlsServerParams::for_tiers(CpuTier::TwoXLarge, MemoryTier::TwoXLarge);
        assert_eq!(p_2xlarge.session_cache_capacity, 65536);
        assert_eq!(p_2xlarge.max_early_data_size, 16384);
        assert_eq!(p_2xlarge.send_tls13_tickets, 8);
        assert_eq!(p_2xlarge.handshake_timeout, Duration::from_secs(3));

        // Ultra
        let p_ultra = TlsServerParams::for_tiers(CpuTier::Ultra, MemoryTier::Ultra);
        assert_eq!(p_ultra.session_cache_capacity, 131072);
        assert_eq!(p_ultra.max_early_data_size, 32768);
        assert_eq!(p_ultra.send_tls13_tickets, 8);
        assert_eq!(p_ultra.handshake_timeout, Duration::from_secs(2));
    }
}
