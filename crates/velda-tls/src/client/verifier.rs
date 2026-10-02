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
        vec![
            rustls::SignatureScheme::ED25519,
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rustls::SignatureScheme::ECDSA_NISTP384_SHA384,
            rustls::SignatureScheme::RSA_PSS_SHA256,
            rustls::SignatureScheme::RSA_PKCS1_SHA256,
        ]
    }
}

/// Constructs a client [`ClientConfig`] using [`InsecureCertVerifier`] and specified ALPN protocols.
pub fn build_insecure_client_config(alpn_protocols: Vec<Vec<u8>>) -> Arc<ClientConfig> {
    let mut rustls_client = ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(InsecureCertVerifier))
        .with_no_client_auth();

    if !alpn_protocols.is_empty() {
        rustls_client.alpn_protocols = alpn_protocols;
    }

    Arc::new(rustls_client)
}
