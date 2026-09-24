use std::collections::HashSet;
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TlsFile {
    pub schema_version: u32,
    pub profiles: Vec<TlsProfileConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TlsProfileConfig {
    pub id: String,
    pub mode: String, // "server" or "client"
    pub certificate: CertificateFiles,
    #[serde(default)]
    pub protocols: Vec<String>,
    #[serde(default)]
    pub alpn: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CertificateFiles {
    pub cert_file: String,
    pub key_file: String,
}

// ============================================================================
// Phase 2: Ingest & Parse (JSON -> Typed Struct)
// ============================================================================

/// Parses raw JSON into TLS configuration.
pub fn parse_tls(payload: &[u8]) -> Result<TlsFile, SyncError> {
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

    Ok(file)
}

// ============================================================================
// Phase 3: Integrity Check & Semantic Validation
#[inline]
fn trim_in_place(s: &mut String) {
    let trimmed_len = s.trim().len();
    if trimmed_len != s.len() {
        *s = s.trim().to_string();
    }
}

/// Validates TLS profile rules locally without silent fallbacks.
pub fn validate_tls(tls: &mut TlsFile) -> Result<(), SyncError> {
    let mut seen_ids = HashSet::with_capacity(tls.profiles.len());

    for profile in &mut tls.profiles {
        trim_in_place(&mut profile.id);
        trim_in_place(&mut profile.mode);
        profile.mode.make_ascii_lowercase();
        trim_in_place(&mut profile.certificate.cert_file);
        trim_in_place(&mut profile.certificate.key_file);

        // Fail fast: empty profile ID
        if profile.id.is_empty() {
            return Err(SyncError::Validation {
                domain: "tls".into(),
                reason: "TLS profile ID cannot be empty".into(),
            });
        }

        // Fail fast: duplicate profile ID
        if !seen_ids.insert(profile.id.as_str()) {
            return Err(SyncError::Validation {
                domain: "tls".into(),
                reason: format!("Duplicate TLS profile ID '{}'", profile.id),
            });
        }

        // Fail fast: invalid mode
        if profile.mode != "server" && profile.mode != "client" {
            return Err(SyncError::Validation {
                domain: "tls".into(),
                reason: format!(
                    "TLS profile '{}' has unsupported mode '{}'; must be explicitly 'server' or 'client'",
                    profile.id, profile.mode
                ),
            });
        }

        // Fail fast: empty cert/key paths
        if profile.certificate.cert_file.is_empty() {
            return Err(SyncError::Validation {
                domain: "tls".into(),
                reason: format!("TLS profile '{}' must specify cert_file", profile.id),
            });
        }

        if profile.certificate.key_file.is_empty() {
            return Err(SyncError::Validation {
                domain: "tls".into(),
                reason: format!("TLS profile '{}' must specify key_file", profile.id),
            });
        }
    }

    Ok(())
}

// ============================================================================
// Phase 4: Binary Compilation & Unpack (DomainHeader + Bincode)
// ============================================================================

/// Compiles TLS configuration into a self-describing binary artifact (tls.bin).
pub fn compile_tls_to_binary(
    tls: &TlsFile,
    revision: u64,
    source_checksum: [u8; 32],
) -> Result<Vec<u8>, SyncError> {
    // Single contiguous buffer: reserve header slot then serialize payload directly
    let mut binary_output = vec![0; DOMAIN_HEADER_SIZE];

    bincode::serialize_into(&mut binary_output, tls).map_err(|e| SyncError::Compile {
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

/// Reads and verifies a compiled TLS binary artifact.
pub fn unpack_tls_from_binary(bytes: &[u8]) -> Result<(DomainHeader, TlsFile), SyncError> {
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

    let tls: TlsFile = bincode::deserialize(payload_bytes).map_err(|e| SyncError::Compile {
        domain: "tls".into(),
        reason: format!("Failed to deserialize: {e}"),
    })?;

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
    use tempfile::tempdir;

    #[test]
    fn test_parse_and_validate_tls() {
        let json = r#"{
            "schema_version": 1,
            "profiles": [
                {
                    "id": " default ",
                    "mode": "server",
                    "certificate": {
                        "cert_file": " /etc/velda/tls/server.crt ",
                        "key_file": " /etc/velda/tls/server.key "
                    }
                }
            ]
        }"#;

        let mut tls = parse_tls(json.as_bytes()).unwrap();
        assert_eq!(tls.profiles.len(), 1);
        validate_tls(&mut tls).unwrap();
        assert_eq!(tls.profiles[0].id, "default");
        assert_eq!(
            tls.profiles[0].certificate.cert_file,
            "/etc/velda/tls/server.crt"
        );
    }

    #[test]
    fn test_empty_cert_file_fails() {
        let mut tls = TlsFile {
            schema_version: 1,
            profiles: vec![TlsProfileConfig {
                id: "p1".into(),
                mode: "server".into(),
                certificate: CertificateFiles {
                    cert_file: "   ".into(),
                    key_file: "/path/key".into(),
                },
                protocols: vec![],
                alpn: vec![],
            }],
        };

        assert!(matches!(
            validate_tls(&mut tls),
            Err(SyncError::Validation { .. })
        ));
    }

    #[test]
    fn test_tls_binary_roundtrip_and_persistence() {
        let tmp = tempdir().unwrap();
        let tls = TlsFile {
            schema_version: 1,
            profiles: vec![TlsProfileConfig {
                id: "default".into(),
                mode: "server".into(),
                certificate: CertificateFiles {
                    cert_file: "/etc/velda/tls/cert.pem".into(),
                    key_file: "/etc/velda/tls/key.pem".into(),
                },
                protocols: vec!["tls1.3".into()],
                alpn: vec!["h2".into()],
            }],
        };

        let json_bytes = b"{\"profiles\": []}";
        let binary = compile_tls_to_binary(&tls, 99, [0x55u8; 32]).expect("Should compile tls");

        let (header, restored) = unpack_tls_from_binary(&binary).expect("Should unpack tls");
        assert_eq!(header.revision, 99);
        assert_eq!(restored, tls);

        persist_tls(tmp.path(), json_bytes, &binary).expect("Should persist tls");
        assert!(tmp.path().join("config/tls.json").exists());
        assert!(tmp.path().join("runtime/tls.bin").exists());
    }
}
