//! In-memory PEM to DER parser and RootCertStore builder.

use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};

use crate::error::TlsError;

/// Parses a certificate chain from a PEM-encoded string into DER certificates.
pub fn parse_certs_pem(pem: &str) -> Result<Vec<CertificateDer<'static>>, TlsError> {
    let cert_list: Vec<CertificateDer<'static>> = CertificateDer::pem_slice_iter(pem.as_bytes())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| {
            TlsError::InvalidCertificate(format!("Failed to parse certificate PEM: {e}"))
        })?;

    if cert_list.is_empty() {
        return Err(TlsError::InvalidCertificate(
            "Certificate PEM contains no certificates".into(),
        ));
    }

    Ok(cert_list)
}

/// Parses a private key from a PEM-encoded string (PKCS#1, PKCS#8, or SEC1).
pub fn parse_private_key_pem(pem: &str) -> Result<PrivateKeyDer<'static>, TlsError> {
    let key = PrivateKeyDer::from_pem_slice(pem.as_bytes()).map_err(|e| {
        TlsError::InvalidPrivateKey(format!("Failed to parse private key PEM: {e}"))
    })?;

    Ok(key)
}

/// Parses a CA bundle from a PEM-encoded string into a `rustls::RootCertStore`.
pub fn parse_ca_bundle_pem(pem: &str) -> Result<rustls::RootCertStore, TlsError> {
    let mut store = rustls::RootCertStore::empty();
    let cert_list = parse_certs_pem(pem)?;

    for cert in cert_list {
        store
            .add(cert)
            .map_err(|e| TlsError::InvalidCaBundle(format!("Failed to add CA certificate: {e}")))?;
    }

    Ok(store)
}
