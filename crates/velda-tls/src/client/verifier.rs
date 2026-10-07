//! Custom certificate verifiers for upstream TLS client.

use std::sync::Arc;

use rustls::ClientConfig;
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};

/// Insecure certificate verifier for development, local testing, and internal mesh backends.
///
/// Bypasses TLS certificate chain and hostname verification.
/// NEVER use in production against untrusted public endpoints.
#[derive(Debug, Clone, Copy)]
pub struct InsecureCertVerifier;

impl rustls::client::danger::ServerCertVerifier for InsecureCertVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::aws_lc_rs::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

/// Constructs a client [`ClientConfig`] using [`InsecureCertVerifier`] and safe default protocol versions (TLS 1.2 + TLS 1.3).
pub fn build_insecure_client_config(alpn_protocols: Vec<Vec<u8>>) -> Arc<ClientConfig> {
    build_insecure_client_config_with_versions(alpn_protocols, rustls::DEFAULT_VERSIONS)
        .expect("Valid default protocol versions")
}

/// Constructs a client [`ClientConfig`] using [`InsecureCertVerifier`] and strict TLS 1.3 only (e.g. for HTTP/3 QUIC).
pub fn build_insecure_tls13_client_config(alpn_protocols: Vec<Vec<u8>>) -> Arc<ClientConfig> {
    build_insecure_client_config_with_versions(alpn_protocols, &[&rustls::version::TLS13])
        .expect("Valid TLS 1.3 protocol version")
}

/// Constructs a client [`ClientConfig`] using [`InsecureCertVerifier`] and explicit protocol versions.
pub fn build_insecure_client_config_with_versions(
    alpn_protocols: Vec<Vec<u8>>,
    versions: &[&'static rustls::SupportedProtocolVersion],
) -> Result<Arc<ClientConfig>, crate::error::TlsError> {
    let mut rustls_client = ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_protocol_versions(versions)
    .map_err(|e| crate::error::TlsError::InvalidCertificate(e.to_string()))?
    .dangerous()
    .with_custom_certificate_verifier(Arc::new(InsecureCertVerifier))
    .with_no_client_auth();

    if !alpn_protocols.is_empty() {
        rustls_client.alpn_protocols = alpn_protocols;
    }

    Ok(Arc::new(rustls_client))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustls::client::danger::ServerCertVerifier;

    #[test]
    fn test_supported_schemes() {
        let verifier = InsecureCertVerifier;
        let schemes = verifier.supported_verify_schemes();
        // Must contain modern curves, RSA variations, and PQ schemes
        assert!(schemes.contains(&rustls::SignatureScheme::ED25519));
        assert!(schemes.contains(&rustls::SignatureScheme::ECDSA_NISTP256_SHA256));
        assert!(schemes.contains(&rustls::SignatureScheme::ECDSA_NISTP384_SHA384));
        assert!(schemes.contains(&rustls::SignatureScheme::ECDSA_NISTP521_SHA512));
        assert!(schemes.contains(&rustls::SignatureScheme::RSA_PSS_SHA256));
        assert!(schemes.contains(&rustls::SignatureScheme::RSA_PSS_SHA384));
        assert!(schemes.contains(&rustls::SignatureScheme::RSA_PSS_SHA512));
        assert!(schemes.contains(&rustls::SignatureScheme::RSA_PKCS1_SHA256));
        assert!(schemes.contains(&rustls::SignatureScheme::RSA_PKCS1_SHA384));
        assert!(schemes.contains(&rustls::SignatureScheme::RSA_PKCS1_SHA512));
    }

    #[test]
    fn test_insecure_client_config() {
        let default_cfg = build_insecure_client_config(vec![b"h2".to_vec()]);
        assert_eq!(default_cfg.alpn_protocols, vec![b"h2".to_vec()]);

        let tls13_cfg = build_insecure_tls13_client_config(vec![b"h3".to_vec()]);
        assert_eq!(tls13_cfg.alpn_protocols, vec![b"h3".to_vec()]);

        let tls12_cfg = build_insecure_client_config_with_versions(
            vec![b"http/1.1".to_vec()],
            &[&rustls::version::TLS12],
        )
        .expect("TLS 1.2 client config");
        assert_eq!(tls12_cfg.alpn_protocols, vec![b"http/1.1".to_vec()]);
    }
}
