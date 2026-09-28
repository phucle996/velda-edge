//! Downstream server TLS configuration and ServerConfig builder.

use std::sync::Arc;

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

/// Minimum session cache capacity to ensure basic resumption capability even in resource-constrained environments.
pub const MIN_SESSION_CACHE_CAPACITY: usize = 1024;

/// Maximum session cache capacity to bound memory footprint on ultra-high-core hosts.
pub const MAX_SESSION_CACHE_CAPACITY: usize = 65536;

/// Default number of cached TLS sessions allocated per worker thread.
pub const SESSIONS_PER_WORKER: usize = 1024;

/// Default maximum size in bytes for TLS 1.3 0-RTT early data.
pub const DEFAULT_MAX_EARLY_DATA_SIZE: u32 = 8192;

/// Default number of single-use TLS 1.3 session tickets issued per connection.
pub const DEFAULT_TLS13_TICKETS: usize = 4;

/// Calculates optimal TLS session cache capacity dynamically based on worker threads and available CPU cores.
///
/// Proportional scaling: 1,024 cached sessions per worker thread, clamped within `[MIN_SESSION_CACHE_CAPACITY, MAX_SESSION_CACHE_CAPACITY]`.
/// Allows operator override via `VELDA_TLS_SESSION_CACHE_CAPACITY` environment variable.
#[inline]
pub fn optimal_session_cache_capacity(workers: usize) -> usize {
    if let Some(parsed) = std::env::var("VELDA_TLS_SESSION_CACHE_CAPACITY")
        .ok()
        .and_then(|val| val.parse::<usize>().ok())
    {
        return parsed.clamp(MIN_SESSION_CACHE_CAPACITY, MAX_SESSION_CACHE_CAPACITY);
    }
    (workers.max(1) * SESSIONS_PER_WORKER)
        .clamp(MIN_SESSION_CACHE_CAPACITY, MAX_SESSION_CACHE_CAPACITY)
}

/// Reads the optimal TLS session cache capacity from global hardware topology cached in RAM.
#[inline]
pub fn probed_session_cache_capacity() -> usize {
    optimal_session_cache_capacity(velda_core::global_hardware_topology().worker_threads())
}

impl ServerTlsConfig {
    /// Compiles a list of `ServerTlsConfig` definitions into an `Arc<ServerConfig>`
    /// using hardware-probed session cache capacity.
    #[inline]
    pub fn build(servers: &[Self]) -> Result<Arc<ServerConfig>, TlsError> {
        Self::build_with_cache(servers, probed_session_cache_capacity())
    }

    /// Compiles a list of `ServerTlsConfig` definitions into an `Arc<ServerConfig>`
    /// using an explicit session cache capacity.
    pub fn build_with_cache(
        servers: &[Self],
        session_cache_capacity: usize,
    ) -> Result<Arc<ServerConfig>, TlsError> {
        let mut sni_resolver = SniResolver::new();
        let mut client_root_store = rustls::RootCertStore::empty();
        let mut requires_client_auth = false;
        let mut alpn_protocols = Vec::new();

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
        }

        if alpn_protocols.is_empty() {
            alpn_protocols.push(b"h2".to_vec());
            alpn_protocols.push(b"http/1.1".to_vec());
        }

        let builder =
            ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
                .with_safe_default_protocol_versions()
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
        server_config.max_early_data_size = DEFAULT_MAX_EARLY_DATA_SIZE;
        server_config.send_tls13_tickets = DEFAULT_TLS13_TICKETS;
        server_config.session_storage =
            rustls::server::ServerSessionMemoryCache::new(session_cache_capacity);

        Ok(Arc::new(server_config))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_optimal_session_cache_capacity_scaling() {
        assert_eq!(
            optimal_session_cache_capacity(1),
            MIN_SESSION_CACHE_CAPACITY
        );
        assert_eq!(optimal_session_cache_capacity(4), 4096);
        assert_eq!(optimal_session_cache_capacity(16), 16384);
        assert_eq!(
            optimal_session_cache_capacity(128),
            MAX_SESSION_CACHE_CAPACITY
        );
    }

    #[test]
    fn test_probed_session_cache_capacity_in_range() {
        let probed = probed_session_cache_capacity();
        assert!((MIN_SESSION_CACHE_CAPACITY..=MAX_SESSION_CACHE_CAPACITY).contains(&probed));
    }
}
