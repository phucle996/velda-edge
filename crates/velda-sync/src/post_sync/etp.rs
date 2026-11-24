//! External Third-Party (`etp`) Domain Compiler & Lifecycle Engine (`velda-sync::post_sync::etp`).
//!
//! ```text
//!   Raw etp.json bytes (from Control Plane / LocalFileProvider)
//!         │
//!         ▼
//!   [1] parse_etps(raw_json) ──► EtpFile { schema_version == 1, revision, etps }
//!         │
//!         ▼
//!   [2] validate_etps(&file) ──► unique non-empty id, EtpKind, pool > 0, TLS PEM, endpoint
//!         │
//!         ▼
//!   [3] validate_plugin_etp_references(&plugins, &etps) ──► verify etp_id & kind match
//!         │
//!         ▼
//!   [4] compile_etps(&file)  ──► bincode(EtpFile) + SHA-256 + DomainHeader(VETP)
//!         │
//!         ▼
//!   [5] persist_etps()       ──► atomic tmp+fsync+rename config/etp.json & runtime/etp.bin
//!
//!   Compiled etp.bin Binary Frame Layout (72-Byte DomainHeader + Bincode Payload):
//!   ┌────────────────────────────────────────────────────────────────────────────┐
//!   │ Bytes  0..24 : magic ("VETP":4B) | fmt_ver:4B | comp_ver:4B | schema:4B    │
//!   │                domain_tag ("ETP\0\0\0\0\0":8B)                             │
//!   │ Bytes 24..72 : revision:8B | payload_size:8B | payload_checksum(SHA256:32B)│
//!   ├────────────────────────────────────────────────────────────────────────────┤
//!   │ Bytes 72..   : bincode::serialize(EtpFile) [json_value_bincode for settings│
//!   └────────────────────────────────────────────────────────────────────────────┘
//! ```

use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use velda_etp::config::json_value_bincode;
use velda_etp::{EtpEntryConfig, EtpKind, EtpPoolSettings, EtpTlsConfig};
use velda_plugin::PluginConfig;

use crate::SyncError;

// ============================================================================
// Binary Protocol Constants & Domain Header
// ============================================================================

pub const DOMAIN_MAGIC: [u8; 4] = *b"VETP";
pub const CURRENT_FORMAT_VERSION: u32 = 1;
pub const CURRENT_COMPILER_VERSION: u32 = 1;
pub const TAG_ETP: [u8; 8] = *b"ETP\0\0\0\0\0";

/// Self-describing binary header prepended to `etp.bin`.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DomainHeader {
    pub magic: [u8; 4],
    pub format_version: u32,
    pub compiler_version: u32,
    pub schema_version: u32,
    pub domain_tag: [u8; 8],
    pub revision: u64,
    pub payload_size: u64,
    pub payload_checksum: [u8; 32],
}

pub const DOMAIN_HEADER_SIZE: usize = std::mem::size_of::<DomainHeader>();

// ============================================================================
// Phase 1: Entity & Schema Definitions
// ============================================================================

fn default_settings() -> serde_json::Value {
    serde_json::Value::Object(serde_json::Map::new())
}

/// Declarative ETP configuration entry persisted in `etp.json` and compiled into `etp.bin`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EtpSyncConfig {
    pub id: String,
    pub kind: String,
    #[serde(default)]
    pub pool: EtpPoolSettings,
    #[serde(default)]
    pub tls: Option<EtpTlsConfig>,
    #[serde(default = "default_settings", with = "json_value_bincode")]
    pub settings: serde_json::Value,
}

impl EtpSyncConfig {
    /// Converts this sync configuration entry into a runtime [`EtpEntryConfig`].
    pub fn to_entry_config(&self) -> Result<EtpEntryConfig, SyncError> {
        let trimmed_id = self.id.trim().to_string();
        let kind = EtpKind::from_str(&self.kind).ok_or_else(|| SyncError::Validation {
            domain: "etp".into(),
            reason: format!("ETP '{}' has unsupported kind '{}'", self.id, self.kind),
        })?;
        Ok(EtpEntryConfig {
            id: trimmed_id,
            kind,
            pool: self.pool.clone(),
            tls: self.tls.clone(),
            settings: self.settings.clone(),
        })
    }
}

/// Top-level `etp.json` / `etp.bin` payload structure.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EtpFile {
    pub schema_version: u32,
    #[serde(default)]
    pub revision: u64,
    #[serde(default)]
    pub etps: Vec<EtpSyncConfig>,
}

// ============================================================================
// Phase 2: Ingest & Parse (JSON -> Typed Struct)
// ============================================================================

/// Parses raw `etp.json` bytes into a typed [`EtpFile`].
pub fn parse_etps(raw_json: &[u8]) -> Result<EtpFile, SyncError> {
    let file: EtpFile = serde_json::from_slice(raw_json).map_err(|e| SyncError::InvalidJson {
        domain: "etp".into(),
        reason: e.to_string(),
    })?;

    if file.schema_version != 1 {
        return Err(SyncError::Validation {
            domain: "etp".into(),
            reason: format!(
                "Unsupported schema_version {}; expected 1",
                file.schema_version
            ),
        });
    }

    Ok(file)
}

// ============================================================================
// Phase 3: Semantic & Cross-Domain Validation
// ============================================================================

/// Validates ETP entries for unique IDs, supported kinds, pool bounds, TLS PEM syntax,
/// and endpoint target configuration.
pub fn validate_etps(file: &EtpFile) -> Result<(), SyncError> {
    if file.schema_version != 1 {
        return Err(SyncError::Validation {
            domain: "etp".into(),
            reason: format!(
                "Unsupported schema_version {}; expected 1",
                file.schema_version
            ),
        });
    }

    let mut seen_ids = HashSet::with_capacity(file.etps.len());

    for entry in &file.etps {
        let id = entry.id.trim();
        if id.is_empty() {
            return Err(SyncError::Validation {
                domain: "etp".into(),
                reason: "ETP entry 'id' cannot be empty".into(),
            });
        }
        if !seen_ids.insert(id.to_string()) {
            return Err(SyncError::Validation {
                domain: "etp".into(),
                reason: format!("Duplicate ETP id '{id}'"),
            });
        }

        let runtime_entry = entry.to_entry_config()?;
        runtime_entry
            .validate()
            .map_err(|e| SyncError::Validation {
                domain: "etp".into(),
                reason: e.to_string(),
            })?;
    }

    Ok(())
}

fn expected_etp_kind_for_plugin(plugin_id: &str) -> Option<EtpKind> {
    match plugin_id {
        "openai_token_quota" | "rate_limit" | "api_metering" => Some(EtpKind::Redis),
        "clickhouse_log_shipper" => Some(EtpKind::ClickHouse),
        "s3_log_shipper" => Some(EtpKind::S3),
        "loki_log_shipper" => Some(EtpKind::Loki),
        "elasticsearch_log_shipper" => Some(EtpKind::Elasticsearch),
        "otlp_metrics_exporter" | "otlp_log_shipper" | "otlp_trace_exporter" => Some(EtpKind::Otlp),
        "kafka_log_shipper" => Some(EtpKind::Kafka),
        "statsd_exporter" => Some(EtpKind::Statsd),
        _ => None,
    }
}

/// Validates that every plugin config referencing `"etp_id"` resolves to a valid ETP entry
/// in `etps` and matches any declared `"etp_kind"` or canonical plugin-to-ETP kind mapping.
pub fn validate_plugin_etp_references(
    plugins: &[PluginConfig],
    etps: &[EtpSyncConfig],
) -> Result<(), SyncError> {
    let mut etp_kinds: HashMap<&str, EtpKind> = HashMap::with_capacity(etps.len());
    for etp in etps {
        let kind = EtpKind::from_str(&etp.kind).ok_or_else(|| SyncError::Validation {
            domain: "etp".into(),
            reason: format!("ETP '{}' has unsupported kind '{}'", etp.id, etp.kind),
        })?;
        etp_kinds.insert(etp.id.trim(), kind);
    }

    for plugin in plugins {
        let Some(obj) = plugin.config.as_object() else {
            continue;
        };
        let Some(etp_id_val) = obj.get("etp_id") else {
            continue;
        };
        let etp_id = etp_id_val
            .as_str()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| SyncError::Validation {
                domain: "plugins".into(),
                reason: format!("Plugin '{}' has empty or non-string 'etp_id'", plugin.id),
            })?;

        let actual_kind = etp_kinds
            .get(etp_id)
            .copied()
            .ok_or_else(|| SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{}' references unknown etp_id '{}'",
                    plugin.id, etp_id
                ),
            })?;

        if let Some(declared_kind_val) = obj.get("etp_kind") {
            let declared_raw = declared_kind_val
                .as_str()
                .ok_or_else(|| SyncError::Validation {
                    domain: "plugins".into(),
                    reason: format!(
                        "Plugin '{}' has non-string 'etp_kind' for etp_id '{}'",
                        plugin.id, etp_id
                    ),
                })?;
            let declared_kind =
                EtpKind::from_str(declared_raw).ok_or_else(|| SyncError::Validation {
                    domain: "plugins".into(),
                    reason: format!(
                        "Plugin '{}' specifies unsupported etp_kind '{}'",
                        plugin.id, declared_raw
                    ),
                })?;
            if actual_kind != declared_kind {
                return Err(SyncError::Validation {
                    domain: "plugins".into(),
                    reason: format!(
                        "Plugin '{}' expects etp_id '{}' to have kind '{}', got '{}'",
                        plugin.id,
                        etp_id,
                        declared_kind.as_str(),
                        actual_kind.as_str()
                    ),
                });
            }
        }

        if let Some(expected_kind) = expected_etp_kind_for_plugin(&plugin.id)
            && actual_kind != expected_kind
        {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{}' requires ETP kind '{}', but etp_id '{}' has kind '{}'",
                    plugin.id,
                    expected_kind.as_str(),
                    etp_id,
                    actual_kind.as_str()
                ),
            });
        }
    }

    Ok(())
}

// ============================================================================
// Phase 4: Binary Compilation & Unpack (DomainHeader + Bincode)
// ============================================================================

/// Compiles an [`EtpFile`] into a self-describing binary artifact (`etp.bin`).
pub fn compile_etps(file: &EtpFile) -> Result<Vec<u8>, SyncError> {
    validate_etps(file)?;

    let mut binary_output = vec![0u8; DOMAIN_HEADER_SIZE];
    bincode::serialize_into(&mut binary_output, file).map_err(|e| SyncError::Compile {
        domain: "etp".into(),
        reason: e.to_string(),
    })?;

    let payload_len = binary_output.len() - DOMAIN_HEADER_SIZE;
    let mut hasher = Sha256::new();
    hasher.update(&binary_output[DOMAIN_HEADER_SIZE..]);
    let payload_checksum: [u8; 32] = hasher.finalize().into();

    let header = DomainHeader {
        magic: DOMAIN_MAGIC,
        format_version: CURRENT_FORMAT_VERSION,
        compiler_version: CURRENT_COMPILER_VERSION,
        schema_version: file.schema_version,
        domain_tag: TAG_ETP,
        revision: file.revision,
        payload_size: payload_len as u64,
        payload_checksum,
    };

    let header_bytes = bincode::serialize(&header).map_err(|e| SyncError::Compile {
        domain: "etp".into(),
        reason: e.to_string(),
    })?;
    binary_output[..DOMAIN_HEADER_SIZE].copy_from_slice(&header_bytes);

    Ok(binary_output)
}

/// Unpacks and integrity-verifies a compiled `etp.bin` artifact into an [`EtpFile`].
pub fn unpack_etps(bytes: &[u8]) -> Result<EtpFile, SyncError> {
    if bytes.len() < DOMAIN_HEADER_SIZE {
        return Err(SyncError::Compile {
            domain: "etp".into(),
            reason: "Artifact smaller than header size".into(),
        });
    }

    let header: DomainHeader =
        bincode::deserialize(&bytes[..DOMAIN_HEADER_SIZE]).map_err(|e| SyncError::Compile {
            domain: "etp".into(),
            reason: format!("Corrupted header: {e}"),
        })?;

    if header.magic != DOMAIN_MAGIC {
        return Err(SyncError::Compile {
            domain: "etp".into(),
            reason: "Invalid ETP magic bytes".into(),
        });
    }
    if header.format_version != CURRENT_FORMAT_VERSION {
        return Err(SyncError::Compile {
            domain: "etp".into(),
            reason: format!("Unsupported ETP format version {}", header.format_version),
        });
    }
    if header.domain_tag != TAG_ETP {
        return Err(SyncError::Compile {
            domain: "etp".into(),
            reason: "Domain tag mismatch".into(),
        });
    }

    let payload_bytes = &bytes[DOMAIN_HEADER_SIZE..];
    if payload_bytes.len() as u64 != header.payload_size {
        return Err(SyncError::Compile {
            domain: "etp".into(),
            reason: "Payload size mismatch".into(),
        });
    }

    let mut hasher = Sha256::new();
    hasher.update(payload_bytes);
    let actual_hash: [u8; 32] = hasher.finalize().into();
    if actual_hash != header.payload_checksum {
        return Err(SyncError::Compile {
            domain: "etp".into(),
            reason: "Payload checksum mismatch".into(),
        });
    }

    let file: EtpFile = bincode::deserialize(payload_bytes).map_err(|e| SyncError::Compile {
        domain: "etp".into(),
        reason: format!("Failed to deserialize payload: {e}"),
    })?;

    validate_etps(&file)?;
    Ok(file)
}

// ============================================================================
// Phase 5: Atomic Persistence (Durable LKG)
// ============================================================================

fn write_atomic(target: &Path, content: &[u8]) -> Result<(), SyncError> {
    let parent = target.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)?;
    let file_name = target.file_name().and_then(|n| n.to_str()).unwrap_or("etp");
    let tmp_path = parent.join(format!(".{file_name}.tmp.{}", std::process::id()));
    {
        let mut file = File::create(&tmp_path)?;
        file.write_all(content)?;
        file.sync_all()?;
    }
    fs::rename(&tmp_path, target)?;
    Ok(())
}

/// Parses, validates, compiles, and atomically persists `config_dir/etp.json` and
/// `runtime_dir/etp.bin`. Returns the parsed file revision.
pub fn persist_etps(
    config_dir: &Path,
    runtime_dir: &Path,
    raw_json: &[u8],
) -> Result<u64, SyncError> {
    let file = parse_etps(raw_json)?;
    validate_etps(&file)?;
    let bin = compile_etps(&file)?;

    write_atomic(&config_dir.join("etp.json"), raw_json)?;
    write_atomic(&runtime_dir.join("etp.bin"), &bin)?;

    Ok(file.revision)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rcgen::generate_simple_self_signed;
    use tempfile::tempdir;

    fn sample_json() -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({
            "schema_version": 1,
            "revision": 7,
            "etps": [
                {
                    "id": "redis-main",
                    "kind": "redis",
                    "pool": {
                        "pool_size": 8,
                        "connect_timeout_ms": 2000,
                        "idle_timeout_ms": 30000
                    },
                    "settings": {
                        "host": "127.0.0.1",
                        "port": 6379
                    }
                },
                {
                    "id": "loki-primary",
                    "kind": "loki",
                    "settings": {
                        "endpoint": "https://loki.example.com:3100/loki/api/v1/push"
                    }
                }
            ]
        }))
        .unwrap()
    }

    #[test]
    fn test_parse_validate_compile_unpack_and_persist_roundtrip() {
        let raw = sample_json();
        let file = parse_etps(&raw).unwrap();
        assert_eq!(file.revision, 7);
        assert_eq!(file.etps.len(), 2);

        validate_etps(&file).unwrap();
        let bin = compile_etps(&file).unwrap();
        assert_eq!(&bin[..4], &DOMAIN_MAGIC);

        let unpacked = unpack_etps(&bin).unwrap();
        assert_eq!(unpacked, file);

        let tmp = tempdir().unwrap();
        let config_dir = tmp.path().join("config");
        let runtime_dir = tmp.path().join("runtime");
        let rev = persist_etps(&config_dir, &runtime_dir, &raw).unwrap();
        assert_eq!(rev, 7);
        assert!(config_dir.join("etp.json").exists());
        assert!(runtime_dir.join("etp.bin").exists());
    }

    #[test]
    fn test_reject_duplicate_ids_and_invalid_tls_pem() {
        let dup_json = serde_json::to_vec(&serde_json::json!({
            "schema_version": 1,
            "revision": 1,
            "etps": [
                { "id": "dup", "kind": "redis", "settings": { "host": "127.0.0.1", "port": 6379 } },
                { "id": "dup", "kind": "loki", "settings": { "host": "127.0.0.1", "port": 3100 } }
            ]
        }))
        .unwrap();
        let file = parse_etps(&dup_json).unwrap();
        assert!(validate_etps(&file).is_err());

        let bad_pem_json = serde_json::to_vec(&serde_json::json!({
            "schema_version": 1,
            "revision": 1,
            "etps": [
                {
                    "id": "tls-bad",
                    "kind": "loki",
                    "tls": { "ca_cert_pem": "NOT A PEM" },
                    "settings": { "host": "127.0.0.1", "port": 3100 }
                }
            ]
        }))
        .unwrap();
        let file = parse_etps(&bad_pem_json).unwrap();
        assert!(validate_etps(&file).is_err());

        // Valid self-signed PEM passes validation
        let cert = generate_simple_self_signed(vec!["loki.local".into()]).unwrap();
        let good_pem_json = serde_json::to_vec(&serde_json::json!({
            "schema_version": 1,
            "revision": 1,
            "etps": [
                {
                    "id": "tls-ok",
                    "kind": "loki",
                    "tls": { "ca_cert_pem": cert.cert.pem() },
                    "settings": { "host": "127.0.0.1", "port": 3100 }
                }
            ]
        }))
        .unwrap();
        let file = parse_etps(&good_pem_json).unwrap();
        validate_etps(&file).unwrap();
    }

    #[test]
    fn test_validate_plugin_etp_references() {
        let etp_file = parse_etps(&sample_json()).unwrap();

        // Valid reference
        let ok_plugins = vec![PluginConfig {
            id: "loki_log_shipper".into(),
            config: serde_json::json!({ "etp_id": "loki-primary" }),
        }];
        validate_plugin_etp_references(&ok_plugins, &etp_file.etps).unwrap();

        // Missing etp_id
        let missing_plugins = vec![PluginConfig {
            id: "loki_log_shipper".into(),
            config: serde_json::json!({ "etp_id": "non-existent" }),
        }];
        assert!(validate_plugin_etp_references(&missing_plugins, &etp_file.etps).is_err());

        // Incompatible kind for plugin
        let wrong_kind_plugins = vec![PluginConfig {
            id: "loki_log_shipper".into(),
            config: serde_json::json!({ "etp_id": "redis-main" }),
        }];
        assert!(validate_plugin_etp_references(&wrong_kind_plugins, &etp_file.etps).is_err());

        // Explicit etp_kind mismatch
        let explicit_mismatch = vec![PluginConfig {
            id: "rate_limit".into(),
            config: serde_json::json!({ "etp_id": "redis-main", "etp_kind": "s3" }),
        }];
        assert!(validate_plugin_etp_references(&explicit_mismatch, &etp_file.etps).is_err());
    }
}
