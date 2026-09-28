//! TLS server engine compilation from TLS configurations.
//!
//! Compiles `TlsConfig` definitions into an in-memory [`TlsServerEngine`]
//! ready for O(1) downstream TLS termination on the request serving hot path.

use velda_sync::post_sync::tls::TlsConfig;
use velda_tls::{ServerTlsConfig, TlsServerEngine};

use crate::error::EdgeError;

/// Compiles a slice of declarative [`TlsConfig`] definitions into an in-memory [`TlsServerEngine`].
///
/// ### Why Pre-compilation is Required (Zero-IO Hot-Path Invariant):
/// - Parsing X.509 certificate chains and private keys from PEM format, initialising
///   cryptographic providers, and constructing the [`SniResolver`] certificate tree
///   are computationally expensive operations involving disk-format decoding and heap allocations.
/// - Performing these operations on every incoming connection would introduce severe latency spikes
///   (several milliseconds per request) and exhaust CPU resources.
/// - Therefore, this function pre-compiles all certificates, keys, ALPN protocols, and SNI lookup
///   tables once during bootstrap or atomic hot-reload into RAM.
/// - On the request serving hot path, [`TlsServerEngine`] resolves SNI and terminates TLS handshakes
///   in $O(1)$ lock-free time without any dynamic allocation or PEM re-parsing.
///
/// Returns `Ok(None)` if no TLS configurations are provided.
pub(crate) fn compile_tls_server(
    tls_configs: &[TlsConfig],
) -> Result<Option<TlsServerEngine>, EdgeError> {
    if tls_configs.is_empty() {
        return Ok(None);
    }

    let server_configs: Vec<ServerTlsConfig> = tls_configs
        .iter()
        .map(|c| ServerTlsConfig {
            sni: c.sni.clone(),
            versions: c.versions.clone(),
            alpn: c.alpn.clone(),
            cert_pem: c.cert_pem.clone(),
            key_pem: c.key_pem.clone(),
            client_ca_pem: c.client_ca_pem.clone(),
        })
        .collect();

    let engine = TlsServerEngine::new(&server_configs).map_err(EdgeError::Tls)?;
    Ok(Some(engine))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rcgen::generate_simple_self_signed;

    #[test]
    fn test_compile_tls_server_empty() {
        let engine = compile_tls_server(&[]).unwrap();
        assert!(engine.is_none());
    }

    #[test]
    fn test_compile_tls_server_with_cert() {
        let cert = generate_simple_self_signed(vec!["example.com".into()]).unwrap();
        let cert_pem = cert.cert.pem();
        let key_pem = cert.signing_key.serialize_pem();

        let configs = vec![TlsConfig {
            sni: vec!["example.com".into()],
            cert_pem,
            key_pem,
            client_ca_pem: None,
            versions: vec!["tls1.3".into()],
            alpn: vec!["h2".into(), "http/1.1".into()],
        }];

        let engine = compile_tls_server(&configs).unwrap();
        assert!(engine.is_some());
    }
}
