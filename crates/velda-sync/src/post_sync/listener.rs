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
pub const TAG_LISTENERS: [u8; 8] = *b"LISTENER";

/// Self-describing binary header prepended to listeners.bin.
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
pub struct ListenersFile {
    pub schema_version: u32,
    pub listeners: Vec<ListenerConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListenerConfig {
    pub id: String,
    pub address: String,  // e.g. "0.0.0.0:80" or "0.0.0.0:443"
    pub protocol: String, // "http", "tcp"
    pub enabled: bool,
    pub tls: ListenerTlsConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ListenerTlsConfig {
    pub enabled: bool,
    #[serde(default)]
    pub profile: Option<String>,
}

// ============================================================================
// Phase 2: Ingest & Parse (JSON -> Typed Struct)
// ============================================================================

/// Parses raw JSON into listener configurations.
pub fn parse_listeners(payload: &[u8]) -> Result<Vec<ListenerConfig>, SyncError> {
    let file: ListenersFile =
        serde_json::from_slice(payload).map_err(|e| SyncError::InvalidJson {
            domain: "listeners".into(),
            reason: e.to_string(),
        })?;

    if file.schema_version != 1 {
        return Err(SyncError::Validation {
            domain: "listeners".into(),
            reason: format!(
                "Unsupported schema_version {}; expected 1",
                file.schema_version
            ),
        });
    }

    Ok(file.listeners)
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

/// Validates listener rules strictly without silent fallbacks.
pub fn validate_listeners(listeners: &mut [ListenerConfig]) -> Result<(), SyncError> {
    let mut seen_ids = HashSet::with_capacity(listeners.len());

    for listener in listeners {
        trim_in_place(&mut listener.id);
        trim_in_place(&mut listener.address);
        trim_in_place(&mut listener.protocol);
        listener.protocol.make_ascii_lowercase();

        if listener.id.is_empty() {
            return Err(SyncError::Validation {
                domain: "listeners".into(),
                reason: "Listener ID cannot be empty".into(),
            });
        }

        if !seen_ids.insert(listener.id.as_str()) {
            return Err(SyncError::Validation {
                domain: "listeners".into(),
                reason: format!("Duplicate listener ID '{}'", listener.id),
            });
        }

        if listener.address.is_empty() {
            return Err(SyncError::Validation {
                domain: "listeners".into(),
                reason: format!(
                    "Listener '{}' must specify an address (e.g. '0.0.0.0:80')",
                    listener.id
                ),
            });
        }

        let is_http = listener.protocol == "http";
        let is_tcp = listener.protocol == "tcp";
        if !is_http && !is_tcp {
            return Err(SyncError::Validation {
                domain: "listeners".into(),
                reason: format!(
                    "Listener '{}' has unsupported protocol '{}'; must be explicitly 'http' or 'tcp'",
                    listener.id, listener.protocol
                ),
            });
        }

        if listener.tls.enabled {
            match &listener.tls.profile {
                Some(p) if !p.trim().is_empty() => {}
                _ => {
                    return Err(SyncError::Validation {
                        domain: "listeners".into(),
                        reason: format!(
                            "Listener '{}' has TLS enabled but no TLS profile is specified",
                            listener.id
                        ),
                    });
                }
            }
        }
    }

    Ok(())
}

// ============================================================================
// Phase 4: Binary Compilation & Unpack (DomainHeader + Bincode)
// ============================================================================

/// Compiles listener configurations into a self-describing binary artifact (listeners.bin).
pub fn compile_listeners_to_binary(
    listeners: &[ListenerConfig],
    revision: u64,
    source_checksum: [u8; 32],
) -> Result<Vec<u8>, SyncError> {
    let mut binary_output = vec![0; DOMAIN_HEADER_SIZE];

    bincode::serialize_into(&mut binary_output, listeners).map_err(|e| SyncError::Compile {
        domain: "listeners".into(),
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
        domain_tag: TAG_LISTENERS,
        revision,
        source_checksum,
        payload_size: payload_len as u64,
        payload_checksum: payload_hash,
    };

    let header_bytes = bincode::serialize(&header).map_err(|e| SyncError::Compile {
        domain: "listeners".into(),
        reason: e.to_string(),
    })?;
    binary_output[..DOMAIN_HEADER_SIZE].copy_from_slice(&header_bytes);

    Ok(binary_output)
}

/// Reads and verifies a compiled listeners binary artifact.
pub fn unpack_listeners_from_binary(
    bytes: &[u8],
) -> Result<(DomainHeader, Vec<ListenerConfig>), SyncError> {
    if bytes.len() < DOMAIN_HEADER_SIZE {
        return Err(SyncError::Compile {
            domain: "listeners".into(),
            reason: "Artifact smaller than header size".into(),
        });
    }

    let header: DomainHeader =
        bincode::deserialize(&bytes[..DOMAIN_HEADER_SIZE]).map_err(|e| SyncError::Compile {
            domain: "listeners".into(),
            reason: format!("Corrupted header: {e}"),
        })?;

    if header.magic != MAGIC_BYTES {
        return Err(SyncError::Compile {
            domain: "listeners".into(),
            reason: "Invalid magic bytes".into(),
        });
    }

    if header.domain_tag != TAG_LISTENERS {
        return Err(SyncError::Compile {
            domain: "listeners".into(),
            reason: "Domain tag mismatch".into(),
        });
    }

    let payload_bytes = &bytes[DOMAIN_HEADER_SIZE..];
    if payload_bytes.len() as u64 != header.payload_size {
        return Err(SyncError::Compile {
            domain: "listeners".into(),
            reason: "Payload size mismatch".into(),
        });
    }

    let mut hasher = Sha256::new();
    hasher.update(payload_bytes);
    let actual_hash: [u8; 32] = hasher.finalize().into();
    if actual_hash != header.payload_checksum {
        return Err(SyncError::Compile {
            domain: "listeners".into(),
            reason: "Payload checksum mismatch".into(),
        });
    }

    let listeners: Vec<ListenerConfig> =
        bincode::deserialize(payload_bytes).map_err(|e| SyncError::Compile {
            domain: "listeners".into(),
            reason: format!("Failed to deserialize: {e}"),
        })?;

    Ok((header, listeners))
}

// ============================================================================
// Phase 5: Atomic Persistence (Durable LKG)
// ============================================================================

/// Atomically persists listeners canonical JSON and binary artifact to the LKG storage directory.
pub fn persist_listeners(
    storage_dir: &Path,
    json_bytes: &[u8],
    bin_bytes: &[u8],
) -> Result<(), SyncError> {
    let json_path = storage_dir.join("config").join("listeners.json");
    let bin_path = storage_dir.join("runtime").join("listeners.bin");

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
    fn test_parse_valid_listeners() {
        let json = r#"{
            "schema_version": 1,
            "listeners": [
                {
                    "id": "http",
                    "address": " 0.0.0.0:80 ",
                    "protocol": "HTTP",
                    "enabled": true,
                    "tls": {
                        "enabled": false
                    }
                }
            ]
        }"#;

        let mut listeners = parse_listeners(json.as_bytes()).unwrap();
        assert_eq!(listeners.len(), 1);
        validate_listeners(&mut listeners).unwrap();
        assert_eq!(listeners[0].address, "0.0.0.0:80");
        assert_eq!(listeners[0].protocol, "http");
    }

    #[test]
    fn test_unsupported_protocol_fails() {
        let mut listeners = vec![ListenerConfig {
            id: "bad".into(),
            address: "0.0.0.0:80".into(),
            protocol: "unsupported_proto".into(),
            enabled: true,
            tls: Default::default(),
        }];

        assert!(matches!(
            validate_listeners(&mut listeners),
            Err(SyncError::Validation { .. })
        ));
    }

    #[test]
    fn test_duplicate_listener_id_fails() {
        let mut listeners = vec![
            ListenerConfig {
                id: "http".into(),
                address: "0.0.0.0:80".into(),
                protocol: "http".into(),
                enabled: true,
                tls: Default::default(),
            },
            ListenerConfig {
                id: "http".into(),
                address: "0.0.0.0:8080".into(),
                protocol: "http".into(),
                enabled: true,
                tls: Default::default(),
            },
        ];

        assert!(matches!(
            validate_listeners(&mut listeners),
            Err(SyncError::Validation { .. })
        ));
    }

    #[test]
    fn test_listener_binary_roundtrip_and_persistence() {
        let tmp = tempdir().unwrap();
        let listeners = vec![ListenerConfig {
            id: "http".into(),
            address: "0.0.0.0:80".into(),
            protocol: "http".into(),
            enabled: true,
            tls: Default::default(),
        }];

        let json_bytes = b"{\"listeners\": []}";
        let binary = compile_listeners_to_binary(&listeners, 5, [0x11u8; 32])
            .expect("Should compile listeners");

        let (header, restored) =
            unpack_listeners_from_binary(&binary).expect("Should unpack listeners");
        assert_eq!(header.revision, 5);
        assert_eq!(restored, listeners);

        persist_listeners(tmp.path(), json_bytes, &binary)
            .expect("Should persist listeners atomically");
        assert!(tmp.path().join("config/listeners.json").exists());
        assert!(tmp.path().join("runtime/listeners.bin").exists());
    }
}
