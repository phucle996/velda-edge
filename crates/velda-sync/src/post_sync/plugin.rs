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
pub const TAG_PLUGINS: [u8; 8] = *b"PLUGINS\0";

/// Self-describing binary header prepended to plugins.bin.
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
pub struct PluginsFile {
    pub schema_version: u32,
    pub plugins: Vec<PluginConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PluginConfig {
    pub id: String,
    #[serde(rename = "type")]
    pub plugin_type: String,
    pub phase: String, // "pre_route", "pre_upstream", "post_response"
    #[serde(with = "json_value_bincode")]
    pub config: serde_json::Value,
}

mod json_value_bincode {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S>(value: &serde_json::Value, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if serializer.is_human_readable() {
            value.serialize(serializer)
        } else {
            let json_str = value.to_string();
            serializer.serialize_str(&json_str)
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<serde_json::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        if deserializer.is_human_readable() {
            serde_json::Value::deserialize(deserializer)
        } else {
            let s = String::deserialize(deserializer)?;
            serde_json::from_str(&s).map_err(serde::de::Error::custom)
        }
    }
}

// ============================================================================
// Phase 2: Ingest & Parse (JSON -> Typed Struct)
// ============================================================================

/// Parses raw JSON into plugin configurations.
pub fn parse_plugins(payload: &[u8]) -> Result<Vec<PluginConfig>, SyncError> {
    let file: PluginsFile =
        serde_json::from_slice(payload).map_err(|e| SyncError::InvalidJson {
            domain: "plugins".into(),
            reason: e.to_string(),
        })?;

    if file.schema_version != 1 {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Unsupported schema_version {}; expected 1",
                file.schema_version
            ),
        });
    }

    Ok(file.plugins)
}

// ============================================================================
// Phase 3: Integrity Check & Semantic Validation
// ============================================================================

/// Validates plugin rules locally.
#[inline]
fn trim_in_place(s: &mut String) {
    let trimmed_len = s.trim().len();
    if trimmed_len != s.len() {
        *s = s.trim().to_string();
    }
}

/// Validates plugin configuration rules locally without silent fallbacks.
///
/// Internal side effects (auto-trimming whitespace, lowercasing phase) are handled in place.
/// External side effects (duplicate ID, missing type, invalid phase) return an error to prevent
/// deploying a broken hook chain to the Data Plane.
pub fn validate_plugins(plugins: &mut [PluginConfig]) -> Result<(), SyncError> {
    let mut seen_ids = HashSet::with_capacity(plugins.len());

    for plugin in plugins {
        trim_in_place(&mut plugin.id);
        trim_in_place(&mut plugin.plugin_type);
        trim_in_place(&mut plugin.phase);
        plugin.phase.make_ascii_lowercase();

        // Fail fast: empty plugin ID
        if plugin.id.is_empty() {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: "Plugin ID cannot be empty".into(),
            });
        }

        // Fail fast: duplicate plugin ID
        if !seen_ids.insert(plugin.id.as_str()) {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!("Duplicate plugin ID '{}'", plugin.id),
            });
        }

        // Fail fast: empty plugin type
        if plugin.plugin_type.is_empty() {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!("Plugin '{}' must specify a type", plugin.id),
            });
        }

        let is_valid_phase = matches!(
            plugin.phase.as_str(),
            "pre_route" | "pre_upstream" | "post_response"
        );

        if !is_valid_phase {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{}' has invalid phase '{}' (must be pre_route, pre_upstream, or post_response)",
                    plugin.id, plugin.phase
                ),
            });
        }
    }

    Ok(())
}

// ============================================================================
// Phase 4: Binary Compilation & Unpack (DomainHeader + Bincode)
// ============================================================================

/// Compiles plugin configurations into a self-describing binary artifact (plugins.bin).
pub fn compile_plugins_to_binary(
    plugins: &[PluginConfig],
    revision: u64,
    source_checksum: [u8; 32],
) -> Result<Vec<u8>, SyncError> {
    // Single contiguous buffer: reserve header slot then serialize payload directly
    let mut binary_output = vec![0; DOMAIN_HEADER_SIZE];

    bincode::serialize_into(&mut binary_output, plugins).map_err(|e| SyncError::Compile {
        domain: "plugins".into(),
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
        domain_tag: TAG_PLUGINS,
        revision,
        source_checksum,
        payload_size: payload_len as u64,
        payload_checksum: payload_hash,
    };

    let header_bytes = bincode::serialize(&header).map_err(|e| SyncError::Compile {
        domain: "plugins".into(),
        reason: e.to_string(),
    })?;
    binary_output[..DOMAIN_HEADER_SIZE].copy_from_slice(&header_bytes);

    Ok(binary_output)
}

/// Reads and verifies a compiled plugins binary artifact.
pub fn unpack_plugins_from_binary(
    bytes: &[u8],
) -> Result<(DomainHeader, Vec<PluginConfig>), SyncError> {
    if bytes.len() < DOMAIN_HEADER_SIZE {
        return Err(SyncError::Compile {
            domain: "plugins".into(),
            reason: "Artifact smaller than header size".into(),
        });
    }

    let header: DomainHeader =
        bincode::deserialize(&bytes[..DOMAIN_HEADER_SIZE]).map_err(|e| SyncError::Compile {
            domain: "plugins".into(),
            reason: format!("Corrupted header: {e}"),
        })?;

    if header.magic != MAGIC_BYTES {
        return Err(SyncError::Compile {
            domain: "plugins".into(),
            reason: "Invalid magic bytes".into(),
        });
    }

    if header.domain_tag != TAG_PLUGINS {
        return Err(SyncError::Compile {
            domain: "plugins".into(),
            reason: "Domain tag mismatch".into(),
        });
    }

    let payload_bytes = &bytes[DOMAIN_HEADER_SIZE..];
    if payload_bytes.len() as u64 != header.payload_size {
        return Err(SyncError::Compile {
            domain: "plugins".into(),
            reason: "Payload size mismatch".into(),
        });
    }

    let mut hasher = Sha256::new();
    hasher.update(payload_bytes);
    let actual_hash: [u8; 32] = hasher.finalize().into();
    if actual_hash != header.payload_checksum {
        return Err(SyncError::Compile {
            domain: "plugins".into(),
            reason: "Payload checksum mismatch".into(),
        });
    }

    let plugins: Vec<PluginConfig> =
        bincode::deserialize(payload_bytes).map_err(|e| SyncError::Compile {
            domain: "plugins".into(),
            reason: format!("Failed to deserialize: {e}"),
        })?;

    Ok((header, plugins))
}

// ============================================================================
// Phase 5: Atomic Persistence (Durable LKG)
// ============================================================================

/// Atomically persists plugins canonical JSON and binary artifact to the LKG storage directory.
pub fn persist_plugins(
    storage_dir: &Path,
    json_bytes: &[u8],
    bin_bytes: &[u8],
) -> Result<(), SyncError> {
    let json_path = storage_dir.join("config").join("plugins.json");
    let bin_path = storage_dir.join("runtime").join("plugins.bin");

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
    fn test_parse_valid_plugins() {
        let json = r#"{
            "schema_version": 1,
            "plugins": [
                {
                    "id": " jwt-auth ",
                    "type": "jwt",
                    "phase": "PRE_ROUTE",
                    "config": { "issuer": "auth.example.com" }
                }
            ]
        }"#;

        let mut plugins = parse_plugins(json.as_bytes()).unwrap();
        assert_eq!(plugins.len(), 1);
        validate_plugins(&mut plugins).unwrap();
        assert_eq!(plugins[0].id, "jwt-auth");
        assert_eq!(plugins[0].phase, "pre_route");
    }

    #[test]
    fn test_invalid_phase_fails() {
        let mut plugins = vec![PluginConfig {
            id: "p1".into(),
            plugin_type: "rate_limit".into(),
            phase: "invalid_phase".into(),
            config: serde_json::json!({}),
        }];

        assert!(matches!(
            validate_plugins(&mut plugins),
            Err(SyncError::Validation { .. })
        ));
    }

    #[test]
    fn test_plugin_binary_roundtrip_and_persistence() {
        let tmp = tempdir().unwrap();
        let plugins = vec![PluginConfig {
            id: "jwt-auth".into(),
            plugin_type: "jwt".into(),
            phase: "pre_route".into(),
            config: serde_json::json!({ "issuer": "auth.example.com", "max_age": 3600 }),
        }];

        let json_bytes = b"{\"plugins\": []}";
        let binary =
            compile_plugins_to_binary(&plugins, 15, [0x44u8; 32]).expect("Should compile plugins");

        let (header, restored) =
            unpack_plugins_from_binary(&binary).expect("Should unpack plugins");
        assert_eq!(header.revision, 15);
        assert_eq!(restored, plugins);

        persist_plugins(tmp.path(), json_bytes, &binary).expect("Should persist plugins");
        assert!(tmp.path().join("config/plugins.json").exists());
        assert!(tmp.path().join("runtime/plugins.bin").exists());
    }
}
