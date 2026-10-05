use std::collections::HashSet;
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;

use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use x509_parser::extensions::{GeneralName, ParsedExtension};
use x509_parser::oid_registry::OID_X509_COMMON_NAME;
use x509_parser::parse_x509_certificate;

use crate::SyncError;

// ============================================================================
// Binary Protocol Constants & Domain Header
// ============================================================================

pub const MAGIC_BYTES: [u8; 4] = *b"VLDA";
pub const CURRENT_FORMAT_VERSION: u32 = 1;
pub const CURRENT_COMPILER_VERSION: u32 = 1;
pub const TAG_TLS: [u8; 8] = *b"TLS\0\0\0\0\0";

/// Self-describing binary header prepended to tls.bin.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DomainHeader {
    pub magic: [u8; 4],
    pub format_version: u32,
    pub compiler_version: u32,
    pub schema_version: u32,
    pub domain_tag: [u8; 8],
    pub revision: u64,
    pub source_checksum: [u8; 32],
    pub payload_size: u64,
    pub payload_checksum: [u8; 32],
}

pub const DOMAIN_HEADER_SIZE: usize = std::mem::size_of::<DomainHeader>();

// ============================================================================
// Phase 1: Entity & Schema Definitions
// ============================================================================

/// JSON representation of tls.json with inline PEM strings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TlsFile {
    pub schema_version: u32,
    #[serde(default)]
    pub tls: Vec<TlsConfig>,
}

/// TLS configuration definition.
/// In JSON input, `sni` is omitted and auto-extracted by Sync from the X.509 certificate.
/// In compiled binary artifacts, `sni` is populated and verified.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TlsConfig {
    #[serde(default)]
    pub sni: Vec<String>,
    pub cert_pem: String,
    pub key_pem: String,
    #[serde(default)]
    pub client_ca_pem: Option<String>,
    #[serde(default)]
    pub versions: Vec<String>,
    #[serde(default)]
    pub alpn: Vec<String>,
}

/// Validates invariants required for zero-IO hot-path execution in Edge runtime.
///
/// Invariant: Every compiled TLS configuration MUST contain at least one valid, non-empty SNI.
/// Empty SNI lists are strictly forbidden because Edge cannot match or route handshakes
/// under the strict No-SNI reject policy.
pub fn validate_tls_invariants(tls_list: &[TlsConfig]) -> Result<(), SyncError> {
    for (idx, item) in tls_list.iter().enumerate() {
        if item.sni.is_empty() {
            return Err(SyncError::Compile {
                domain: "tls".into(),
                reason: format!(
                    "Compiled TLS config at index {idx} has an empty SNI list; TLS handshake cannot be routed without SNI"
                ),
            });
        }
        for sni in &item.sni {
            if sni.trim().is_empty() {
                return Err(SyncError::Compile {
                    domain: "tls".into(),
                    reason: format!(
                        "Compiled TLS config at index {idx} contains an empty SNI entry"
                    ),
                });
            }
        }
    }

    Ok(())
}

// ============================================================================
// Phase 2: Ingest & Parse (JSON -> Typed Struct)
// ============================================================================

/// Parses raw JSON into TLS configuration.
pub fn parse_tls(payload: &[u8]) -> Result<Vec<TlsConfig>, SyncError> {
    let file: TlsFile = serde_json::from_slice(payload).map_err(|e| SyncError::InvalidJson {
        domain: "tls".into(),
        reason: e.to_string(),
    })?;

    if file.schema_version != 1 {
        return Err(SyncError::Validation {
            domain: "tls".into(),
            reason: format!(
                "Unsupported schema_version {}; expected 1",
                file.schema_version
            ),
        });
    }

    Ok(file.tls)
}

// ============================================================================
// Phase 3: Integrity Check, X.509 SNI Extraction & Semantic Validation
// ============================================================================

#[inline]
fn trim_in_place(s: &mut String) {
    let trimmed_len = s.trim().len();
    if trimmed_len != s.len() {
        *s = s.trim().to_string();
    }
}

fn is_supported_tls_version(v: &str) -> bool {
    matches!(v, "tls1.2" | "tls1.3" | "tlsv1.2" | "tlsv1.3")
}

fn is_valid_domain_sni(s: &str) -> bool {
    !s.is_empty()
        && !s.contains(' ')
        && s.chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '-' || c == '_' || c == '*')
}

/// Extracts all valid DNS Subject Alternative Names (SAN) or Common Names (CN) from a PEM certificate chain.
/// Returns error if the certificate is invalid, malformed, or contains NO DNS SAN / CN.
pub fn extract_cert_snis(cert_pem: &str) -> Result<Vec<String>, SyncError> {
    let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_slice_iter(cert_pem.as_bytes())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| SyncError::Validation {
            domain: "tls".into(),
            reason: format!("Failed to parse certificate PEM: {e}"),
        })?;

    if certs.is_empty() {
        return Err(SyncError::Validation {
            domain: "tls".into(),
            reason: "Certificate PEM contains no certificates".into(),
        });
    }

    // End-entity leaf certificate is the first certificate in the chain
    let leaf_der = &certs[0];
    let (_, x509) =
        parse_x509_certificate(leaf_der.as_ref()).map_err(|e| SyncError::Validation {
            domain: "tls".into(),
            reason: format!("Failed to parse X.509 DER certificate: {e}"),
        })?;

    let mut snis = Vec::new();

    // 1. Subject Alternative Name (SAN) extension
    for ext in x509.extensions() {
        if let ParsedExtension::SubjectAlternativeName(san) = ext.parsed_extension() {
            for name in &san.general_names {
                if let GeneralName::DNSName(dns) = name {
                    let s = dns.trim().to_ascii_lowercase();
                    if is_valid_domain_sni(&s) && !snis.contains(&s) {
                        snis.push(s);
                    }
                }
            }
        }
    }

    // 2. Fallback to Common Name (CN) if no DNS SAN was found
    if snis.is_empty() {
        for rdn in x509.subject().iter_rdn() {
            for attr in rdn.iter() {
                if attr.attr_type() == &OID_X509_COMMON_NAME
                    && let Ok(cn) = attr.as_str()
                {
                    let s = cn.trim().to_ascii_lowercase();
                    if is_valid_domain_sni(&s) && !snis.contains(&s) {
                        snis.push(s);
                    }
                }
            }
        }
    }

    if snis.is_empty() {
        return Err(SyncError::Validation {
            domain: "tls".into(),
            reason: "Certificate does not contain any DNS Subject Alternative Name (SAN) or Common Name (CN); TLS handshake would be rejected".into(),
        });
    }

    Ok(snis)
}

/// Validates private key PEM format (PKCS#1, PKCS#8, or SEC1).
pub fn validate_private_key_pem(key_pem: &str) -> Result<(), SyncError> {
    PrivateKeyDer::from_pem_slice(key_pem.as_bytes()).map_err(|e| SyncError::Validation {
        domain: "tls".into(),
        reason: format!("Failed to parse private key PEM: {e}"),
    })?;

    Ok(())
}

/// Validates CA bundle PEM format.
pub fn validate_ca_bundle_pem(ca_pem: &str) -> Result<(), SyncError> {
    let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_slice_iter(ca_pem.as_bytes())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| SyncError::Validation {
            domain: "tls".into(),
            reason: format!("Failed to parse CA certificate PEM: {e}"),
        })?;

    if certs.is_empty() {
        return Err(SyncError::Validation {
            domain: "tls".into(),
            reason: "CA certificate PEM contains no certificates".into(),
        });
    }

    Ok(())
}

/// Validates TLS configurations locally without silent fallbacks.
/// Automatically parses certificates, verifies keys, and checks for duplicate SNIs.
pub fn validate_tls(tls: &mut [TlsConfig]) -> Result<(), SyncError> {
    let mut seen_snis = HashSet::new();

    for item in tls.iter_mut() {
        trim_in_place(&mut item.cert_pem);
        trim_in_place(&mut item.key_pem);

        if item.cert_pem.is_empty() {
            return Err(SyncError::Validation {
                domain: "tls".into(),
                reason: "TLS cert_pem cannot be empty".into(),
            });
        }

        if item.key_pem.is_empty() {
            return Err(SyncError::Validation {
                domain: "tls".into(),
                reason: "TLS key_pem cannot be empty".into(),
            });
        }

        // Auto-extract and validate SNIs directly from X.509 cert
        let snis = extract_cert_snis(&item.cert_pem)?;
        item.sni = snis.clone();

        // Ensure no collision across certificates
        for sni in &snis {
            if !seen_snis.insert(sni.clone()) {
                return Err(SyncError::Validation {
                    domain: "tls".into(),
                    reason: format!("Duplicate TLS SNI '{sni}' extracted across certificates"),
                });
            }
        }

        // Validate private key format
        validate_private_key_pem(&item.key_pem)?;

        // Validate client CA if mTLS is enabled
        if let Some(ca) = &mut item.client_ca_pem {
            trim_in_place(ca);
            if ca.is_empty() {
                return Err(SyncError::Validation {
                    domain: "tls".into(),
                    reason: "TLS client_ca_pem cannot be empty when specified".into(),
                });
            }
            validate_ca_bundle_pem(ca)?;
        }

        for v in &mut item.versions {
            trim_in_place(v);
            v.make_ascii_lowercase();
            if !is_supported_tls_version(v) {
                return Err(SyncError::Validation {
                    domain: "tls".into(),
                    reason: format!(
                        "Unsupported TLS version '{v}' in TLS configuration; must be tls1.2 or tls1.3"
                    ),
                });
            }
        }

        for alpn in &mut item.alpn {
            trim_in_place(alpn);
            alpn.make_ascii_lowercase();
            if alpn.is_empty() {
                return Err(SyncError::Validation {
                    domain: "tls".into(),
                    reason: "ALPN entry cannot be empty in TLS configuration".into(),
                });
            }
        }
    }

    Ok(())
}

// ============================================================================
// Phase 4: Binary Compilation & Unpack (DomainHeader + Bincode)
// ============================================================================

/// Compiles TLS configuration into a self-describing binary artifact (tls.bin).
/// Extracts SNIs from certificates and embeds full PEM bytes into the binary payload.
pub fn compile_tls_to_binary(
    tls: &[TlsConfig],
    revision: u64,
    source_checksum: [u8; 32],
) -> Result<Vec<u8>, SyncError> {
    let mut compiled_tls = Vec::with_capacity(tls.len());
    for item in tls {
        let mut entry = item.clone();
        if entry.sni.is_empty() {
            entry.sni = extract_cert_snis(&entry.cert_pem)?;
        }
        compiled_tls.push(entry);
    }

    // Invariant: Verify all compiled entities maintain strictly non-empty SNIs
    validate_tls_invariants(&compiled_tls)?;

    // Single contiguous buffer: reserve header slot then serialize payload directly
    let mut binary_output = vec![0; DOMAIN_HEADER_SIZE];

    bincode::serialize_into(&mut binary_output, &compiled_tls).map_err(|e| SyncError::Compile {
        domain: "tls".into(),
        reason: e.to_string(),
    })?;

    let payload_len = binary_output.len() - DOMAIN_HEADER_SIZE;
    let mut hasher = Sha256::new();
    hasher.update(&binary_output[DOMAIN_HEADER_SIZE..]);
    let payload_hash: [u8; 32] = hasher.finalize().into();

    let header = DomainHeader {
        magic: MAGIC_BYTES,
        format_version: CURRENT_FORMAT_VERSION,
        compiler_version: CURRENT_COMPILER_VERSION,
        schema_version: 1,
        domain_tag: TAG_TLS,
        revision,
        source_checksum,
        payload_size: payload_len as u64,
        payload_checksum: payload_hash,
    };

    let header_bytes = bincode::serialize(&header).map_err(|e| SyncError::Compile {
        domain: "tls".into(),
        reason: e.to_string(),
    })?;
    binary_output[..DOMAIN_HEADER_SIZE].copy_from_slice(&header_bytes);

    Ok(binary_output)
}

/// Reads and verifies a compiled TLS binary artifact into `Vec<TlsConfig>`.
pub fn unpack_tls_from_binary(bytes: &[u8]) -> Result<(DomainHeader, Vec<TlsConfig>), SyncError> {
    if bytes.len() < DOMAIN_HEADER_SIZE {
        return Err(SyncError::Compile {
            domain: "tls".into(),
            reason: "Artifact smaller than header size".into(),
        });
    }

    let header: DomainHeader =
        bincode::deserialize(&bytes[..DOMAIN_HEADER_SIZE]).map_err(|e| SyncError::Compile {
            domain: "tls".into(),
            reason: format!("Corrupted header: {e}"),
        })?;

    if header.magic != MAGIC_BYTES {
        return Err(SyncError::Compile {
            domain: "tls".into(),
            reason: "Invalid magic bytes".into(),
        });
    }

    if header.domain_tag != TAG_TLS {
        return Err(SyncError::Compile {
            domain: "tls".into(),
            reason: "Domain tag mismatch".into(),
        });
    }

    let payload_bytes = &bytes[DOMAIN_HEADER_SIZE..];
    if payload_bytes.len() as u64 != header.payload_size {
        return Err(SyncError::Compile {
            domain: "tls".into(),
            reason: "Payload size mismatch".into(),
        });
    }

    let mut hasher = Sha256::new();
    hasher.update(payload_bytes);
    let actual_hash: [u8; 32] = hasher.finalize().into();
    if actual_hash != header.payload_checksum {
        return Err(SyncError::Compile {
            domain: "tls".into(),
            reason: "Payload checksum mismatch".into(),
        });
    }

    let tls: Vec<TlsConfig> =
        bincode::deserialize(payload_bytes).map_err(|e| SyncError::Compile {
            domain: "tls".into(),
            reason: format!("Failed to deserialize: {e}"),
        })?;

    // Invariant: Enforce that no TLS entry in the binary artifact has an empty SNI list
    validate_tls_invariants(&tls)?;

    Ok((header, tls))
}

// ============================================================================
// Phase 5: Atomic Persistence (Durable LKG)
// ============================================================================

/// Atomically persists TLS canonical JSON and binary artifact to the LKG storage directory.
pub fn persist_tls(
    storage_dir: &Path,
    json_bytes: &[u8],
    bin_bytes: &[u8],
) -> Result<(), SyncError> {
    let json_path = storage_dir.join("config").join("tls.json");
    let bin_path = storage_dir.join("runtime").join("tls.bin");

    for (target, content) in [(&json_path, json_bytes), (&bin_path, bin_bytes)] {
        let parent = target.parent().unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(parent)?;
        let file_name = target
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("file");
        let tmp_path = parent.join(format!(".{file_name}.tmp.{}", std::process::id()));
        {
            let mut file = File::create(&tmp_path)?;
            file.write_all(content)?;
            file.sync_all()?;
        }
        fs::rename(&tmp_path, target)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rcgen::generate_simple_self_signed;
    use tempfile::tempdir;

    fn make_test_cert(sans: Vec<String>) -> (String, String) {
        let certified_key = generate_simple_self_signed(sans).unwrap();
        let cert_pem = certified_key.cert.pem();
        let key_pem = certified_key.signing_key.serialize_pem();
        (cert_pem, key_pem)
    }

    #[test]
    fn test_extract_cert_snis_and_validate() {
        let (cert_pem, key_pem) =
            make_test_cert(vec!["api.example.com".into(), "gateway.example.com".into()]);

        let snis = extract_cert_snis(&cert_pem).unwrap();
        assert_eq!(
            snis,
            vec![
                "api.example.com".to_string(),
                "gateway.example.com".to_string()
            ]
        );
        validate_private_key_pem(&key_pem).unwrap();
    }

    #[test]
    fn test_parse_and_validate_flat_tls_json_without_sni() {
        let (server_cert, server_key) = make_test_cert(vec!["api.example.com".into()]);
        let (ca_cert, _) = make_test_cert(vec!["client-ca.internal".into()]);

        let json = serde_json::json!({
            "schema_version": 1,
            "tls": [
                {
                    "cert_pem": server_cert,
                    "key_pem": server_key,
                    "client_ca_pem": ca_cert,
                    "versions": ["TLS1.3"],
                    "alpn": ["H2"]
                }
            ]
        });

        let mut tls = parse_tls(&serde_json::to_vec(&json).unwrap()).unwrap();
        assert_eq!(tls.len(), 1);

        validate_tls(&mut tls).unwrap();
        assert_eq!(tls[0].versions[0], "tls1.3");
        assert_eq!(tls[0].alpn[0], "h2");

        let bin = compile_tls_to_binary(&tls, 42, [0xAAu8; 32]).unwrap();
        let (header, restored) = unpack_tls_from_binary(&bin).unwrap();
        assert_eq!(header.revision, 42);
        assert_eq!(restored[0].sni, vec!["api.example.com"]);
    }

    #[test]
    fn test_duplicate_extracted_sni_fails() {
        let (cert1, key1) = make_test_cert(vec!["common.example.com".into()]);
        let (cert2, key2) = make_test_cert(vec!["common.example.com".into()]);

        let mut tls = vec![
            TlsConfig {
                sni: vec![],
                cert_pem: cert1,
                key_pem: key1,
                client_ca_pem: None,
                versions: vec![],
                alpn: vec![],
            },
            TlsConfig {
                sni: vec![],
                cert_pem: cert2,
                key_pem: key2,
                client_ca_pem: None,
                versions: vec![],
                alpn: vec![],
            },
        ];

        let err = validate_tls(&mut tls).unwrap_err();
        assert!(
            err.to_string().contains("Duplicate TLS SNI"),
            "Error was: {err}"
        );
    }

    #[test]
    fn test_invalid_cert_fails_validation() {
        let mut tls = vec![TlsConfig {
            sni: vec![],
            cert_pem: "NOT_A_VALID_PEM".into(),
            key_pem: "NOT_A_VALID_KEY".into(),
            client_ca_pem: None,
            versions: vec![],
            alpn: vec![],
        }];

        assert!(matches!(
            validate_tls(&mut tls),
            Err(SyncError::Validation { .. })
        ));
    }

    #[test]
    fn test_tls_binary_roundtrip_and_persistence() {
        let tmp = tempdir().unwrap();
        let (cert, key) = make_test_cert(vec!["default.com".into()]);

        let tls = vec![TlsConfig {
            sni: vec![],
            cert_pem: cert,
            key_pem: key,
            client_ca_pem: None,
            versions: vec!["tls1.3".into()],
            alpn: vec!["h2".into()],
        }];

        let json_bytes = b"{\"schema_version\": 1, \"tls\": []}";
        let binary = compile_tls_to_binary(&tls, 99, [0x55u8; 32]).expect("Should compile tls");

        let (header, restored) = unpack_tls_from_binary(&binary).expect("Should unpack tls");
        assert_eq!(header.revision, 99);
        assert_eq!(restored[0].sni, vec!["default.com"]);

        persist_tls(tmp.path(), json_bytes, &binary).expect("Should persist tls");
        assert!(tmp.path().join("config/tls.json").exists());
        assert!(tmp.path().join("runtime/tls.bin").exists());
    }

    #[test]
    fn test_compiled_tls_empty_sni_fails_invariants() {
        let (cert, key) = make_test_cert(vec!["valid.com".into()]);

        let list_empty_sni = vec![TlsConfig {
            sni: vec![],
            versions: vec!["tls1.3".into()],
            alpn: vec!["h2".into()],
            cert_pem: cert,
            key_pem: key,
            client_ca_pem: None,
        }];
        let err = validate_tls_invariants(&list_empty_sni).unwrap_err();
        assert!(
            err.to_string().contains("empty SNI list"),
            "Expected empty SNI error, got: {err}"
        );
    }

    #[test]
    fn test_unpack_tls_with_empty_sni_fails() {
        let (cert, key) = make_test_cert(vec!["valid.com".into()]);

        let corrupted_compiled = vec![TlsConfig {
            sni: vec![],
            versions: vec!["tls1.3".into()],
            alpn: vec!["h2".into()],
            cert_pem: cert,
            key_pem: key,
            client_ca_pem: None,
        }];

        let mut binary_output = vec![0; DOMAIN_HEADER_SIZE];
        bincode::serialize_into(&mut binary_output, &corrupted_compiled).unwrap();
        let payload_len = binary_output.len() - DOMAIN_HEADER_SIZE;
        let mut hasher = Sha256::new();
        hasher.update(&binary_output[DOMAIN_HEADER_SIZE..]);
        let payload_hash: [u8; 32] = hasher.finalize().into();

        let header = DomainHeader {
            magic: MAGIC_BYTES,
            format_version: CURRENT_FORMAT_VERSION,
            compiler_version: CURRENT_COMPILER_VERSION,
            schema_version: 1,
            domain_tag: TAG_TLS,
            revision: 1,
            source_checksum: [0u8; 32],
            payload_size: payload_len as u64,
            payload_checksum: payload_hash,
        };
        let header_bytes = bincode::serialize(&header).unwrap();
        binary_output[..DOMAIN_HEADER_SIZE].copy_from_slice(&header_bytes);

        let err = unpack_tls_from_binary(&binary_output).unwrap_err();
        assert!(
            err.to_string().contains("empty SNI list"),
            "Expected empty SNI unpack rejection, got: {err}"
        );
    }
}
