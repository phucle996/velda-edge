//! TLS server engine compilation from TLS configurations.
//!
//! Compiles `TlsConfig` definitions into an in-memory [`TlsServerEngine`]
//! ready for O(1) downstream TLS termination on the request serving hot path.

use velda_sync::post_sync::tls::TlsConfig;
use velda_tls::{
    ClientTlsConfig, ServerTlsConfig, TlsClientEngine, TlsServerEngine, TlsServerParams,
};

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
pub fn compile_tls_server(
    tls_configs: &[TlsConfig],
    params: &TlsServerParams,
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

    let engine =
        TlsServerEngine::new_with_params(&server_configs, params).map_err(EdgeError::Tls)?;
    Ok(Some(engine))
}

/// Compiles declarative upstream TLS configurations into an in-memory [`TlsClientEngine`].
///
/// Pre-compiles all client certificates, root stores, ALPN protocols, and SNI connectors
/// into RAM ready for O(1) lock-free lookup and zero-IO handshake execution on the hot path.
pub fn compile_tls_client(
    upstreams: &[velda_sync::post_sync::upstream::UpstreamConfig],
) -> Result<Option<TlsClientEngine>, EdgeError> {
    let client_configs: Vec<ClientTlsConfig> = upstreams
        .iter()
        .filter_map(|u| {
            u.tls.as_ref().map(|t| ClientTlsConfig {
                sni: t.sni.clone(),
                versions: t.versions.clone(),
                alpn: t.alpn.clone(),
                ca_pem: t.ca_pem.clone(),
                client_cert_pem: t.client_cert_pem.clone(),
                client_key_pem: t.client_key_pem.clone(),
                insecure_skip_verify: t.insecure_skip_verify,
            })
        })
        .collect();

    if client_configs.is_empty() {
        return Ok(None);
    }

    let engine = TlsClientEngine::new(&client_configs).map_err(EdgeError::Tls)?;
    Ok(Some(engine))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rcgen::generate_simple_self_signed;

    #[test]
    fn test_compile_tls_server_empty() {
        let params = TlsServerParams::from_hardware();
        let engine = compile_tls_server(&[], &params).unwrap();
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

        let params = TlsServerParams::from_hardware();
        let engine = compile_tls_server(&configs, &params).unwrap();
        assert!(engine.is_some());
    }

    #[test]
    fn test_compile_tls_client_empty() {
        let engine = compile_tls_client(&[]).unwrap();
        assert!(engine.is_none());
    }

    #[test]
    fn test_compile_tls_client_with_sni() {
        use velda_sync::post_sync::upstream::{
            EndpointConfig, LoadBalancerConfig, UpstreamConfig, UpstreamProtocolConfig,
            UpstreamTimeouts, UpstreamTlsConfig,
        };

        let cert = generate_simple_self_signed(vec!["backend.internal".into()]).unwrap();
        let cert_pem = cert.cert.pem();

        let upstreams = vec![UpstreamConfig {
            id: "secure-backend".into(),
            mode: "endpoints".into(),
            protocol: UpstreamProtocolConfig {
                transport: "tcp".into(),
                application: "http2".into(),
                streaming: velda_sync::StreamingMode::DISABLED,
            },
            target: None,
            resolver: None,
            endpoints: vec![EndpointConfig {
                address: "127.0.0.1:8443".into(),
                weight: 1,
            }],
            load_balancer: LoadBalancerConfig {
                algorithm: "round_robin".into(),
            },
            timeouts: UpstreamTimeouts {
                connect_ms: 1000,
                idle_ms: 5000,
                request_ms: None,
            },
            health_check: None,
            tls: Some(UpstreamTlsConfig {
                ca_pem: Some(cert_pem),
                client_cert_pem: None,
                client_key_pem: None,
                versions: vec!["tls1.3".into()],
                alpn: vec!["h2".into()],
                sni: vec!["backend.internal".into()],
                insecure_skip_verify: false,
            }),
            pool: None,
        }];

        let engine = compile_tls_client(&upstreams)
            .unwrap()
            .expect("engine should be compiled");
        assert!(engine.contains_sni("backend.internal"));
        assert!(!engine.contains_sni("unknown.internal"));
        assert!(engine.get_connector("backend.internal").is_some());
    }
}
