//! Plugin Domain Compiler & Lifecycle Engine (`velda-sync::post_sync::plugin`).
//!
//! Converts raw plugin configurations (`plugins.json`) into optimized, checksummed
//! binary runtime artifacts (`plugins.bin`) for consumption by the Data Plane and Shadow Logistics Workers.
//!
//! # Modular Plugin Schema Architecture
//! Individual plugin configuration schemas and validation rules are organized under
//! dedicated sub-modules by plugin type:
//! - [`log_exporter`], [`metrics_exporter`], [`trace_exporter`]: Consolidated observability exporters.
//! - Future plugins (`jwt_auth`, `rate_limit`, `ip_restriction`) are isolated similarly.

use std::collections::HashSet;
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::SyncError;

// Re-export core plugin definitions and validation dispatcher from velda-plugin
pub use velda_plugin::{
    PluginConfig, canonical_protocol_family_for_id, is_known_plugin_id, validate_plugin_config,
    validate_plugin_dependencies,
};

// Re-export plugin configuration validation types compiled by velda-plugin
pub use velda_plugin::background::acme_cert_rotator::validation::*;
pub use velda_plugin::background::health_probe::validation::*;
pub use velda_plugin::background::log_exporter::validation::*;
pub use velda_plugin::background::metrics_exporter::validation::*;
pub use velda_plugin::background::trace_exporter::validation::*;
pub use velda_plugin::post_response::cors::validation::*;
pub use velda_plugin::post_response::http_cache_store::validation::*;
pub use velda_plugin::post_response::response_compression::validation::*;
pub use velda_plugin::post_response::response_header_mutation::validation::*;
pub use velda_plugin::post_response::security_headers::validation::*;
pub use velda_plugin::pre_route::cors_preflight::validation::*;
pub use velda_plugin::pre_route::ip_filter::validation::*;
pub use velda_plugin::pre_route::resource_limit::validation::*;
pub use velda_plugin::pre_route::waf_lite::validation::*;
pub use velda_plugin::pre_upstream::api_key_auth::validation::*;
pub use velda_plugin::pre_upstream::bot_detection::validation::*;
pub use velda_plugin::pre_upstream::canary::validation::*;
pub use velda_plugin::pre_upstream::csrf::validation::*;
pub use velda_plugin::pre_upstream::fault_injection::validation::*;
pub use velda_plugin::pre_upstream::header_mutation::validation::*;
pub use velda_plugin::pre_upstream::hmac_auth::validation::*;
pub use velda_plugin::pre_upstream::http_cache_lookup::validation::*;
pub use velda_plugin::pre_upstream::ja4_filter::validation::*;
pub use velda_plugin::pre_upstream::jwt_auth::validation::*;
pub use velda_plugin::pre_upstream::oauth2_introspection::validation::*;
pub use velda_plugin::pre_upstream::rate_limit::validation::*;
pub use velda_plugin::pre_upstream::request_mirror::validation::*;
pub use velda_plugin::pre_upstream::request_termination::validation::*;
pub use velda_plugin::pre_upstream::url_rewrite::validation::*;

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
// Phase 2: Ingest & Parse (JSON -> Typed Struct)
// ============================================================================

/// Plugins configuration file schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PluginsFile {
    pub schema_version: u32,
    pub plugins: Vec<PluginConfig>,
}

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
// Phase 3: Integrity Check & Semantic Validation (Dispatcher)
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
/// Internal side effects (auto-trimming whitespace on ID) are handled in place.
/// External side effects (empty/duplicate/unknown ID, invalid config) return an error
/// to prevent deploying a broken hook chain to the Data Plane.
pub fn validate_plugins(plugins: &mut [PluginConfig]) -> Result<(), SyncError> {
    let mut seen_ids = HashSet::with_capacity(plugins.len());

    for plugin in &mut *plugins {
        trim_in_place(&mut plugin.id);

        // Fail fast: empty plugin ID
        if plugin.id.is_empty() {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: "Plugin ID cannot be empty".into(),
            });
        }

        // Fail fast: duplicate plugin ID
        if !seen_ids.insert(plugin.id.clone()) {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!("Duplicate plugin ID '{}'", plugin.id),
            });
        }

        // Fail fast: unknown plugin ID
        if !is_known_plugin_id(&plugin.id) {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!("Unknown plugin ID '{}'", plugin.id),
            });
        }

        // Dispatch semantic validation to velda-plugin
        velda_plugin::validate_plugin_config(plugin).map_err(|e| SyncError::Validation {
            domain: "plugins".into(),
            reason: e.to_string(),
        })?;
    }

    velda_plugin::validate_plugin_dependencies(plugins).map_err(|e| SyncError::Validation {
        domain: "plugins".into(),
        reason: e.to_string(),
    })?;

    Ok(())
}

/// Validates plugin schema/dependencies and enforces ETP existence + allowed kind mapping
/// against already-validated ETP entries.
pub fn validate_plugins_with_etps(
    plugins: &mut [PluginConfig],
    etps: &[crate::post_sync::etp::EtpSyncConfig],
) -> Result<(), SyncError> {
    validate_plugins(plugins)?;
    crate::post_sync::etp::validate_plugin_etp_references(plugins, etps)?;
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
                    "id": " jwt_auth ",
                    "config": {
                        "algorithm": "HS256",
                        "secret_key": "valid-secret-key-for-test",
                        "token_sources": [{ "type": "header", "name": "authorization", "prefix": "Bearer " }],
                        "validate_exp": true,
                        "clock_skew_secs": 60,
                        "issuer": "auth.example.com",
                        "claims_to_headers": {},
                        "assert_claims": {},
                        "cache_ttl_secs": 300
                    }
                }
            ]
        }"#;

        let mut plugins = parse_plugins(json.as_bytes()).unwrap();
        assert_eq!(plugins.len(), 1);
        validate_plugins(&mut plugins).unwrap();
        assert_eq!(plugins[0].id, "jwt_auth");
    }

    #[test]
    fn test_unknown_plugin_id_fails() {
        let mut plugins = vec![PluginConfig {
            id: "unknown_custom_plugin".into(),
            config: serde_json::json!({}),
        }];

        assert!(matches!(
            validate_plugins(&mut plugins),
            Err(SyncError::Validation { .. })
        ));
    }

    #[test]
    fn test_plugin_binary_roundtrip() {
        let tmp = tempdir().unwrap();
        let plugins = vec![PluginConfig {
            id: "jwt_auth".into(),
            config: serde_json::json!({ "issuer": "auth.example.com", "secret_key": "valid-secret-key-for-test", "max_age": 3600 }),
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

    #[test]
    fn test_valid_metrics_exporter_plugin() {
        let json = r#"{
            "schema_version": 1,
            "plugins": [
                {
                    "id": "metrics_exporter",
                    "config": {
                        "targets": [
                            {
                                "mode": "pull",
                                "listen": "0.0.0.0:9090",
                                "path": "/metrics",
                                "protocol": "http",
                                "scrape": {
                                    "timeout_ms": 5000,
                                    "max_concurrency": 16,
                                    "max_buffer_size_kb": 256
                                },
                                "tls": {
                                    "mode": "none"
                                },
                                "cors": {
                                    "enabled": false,
                                    "allow_origins": [],
                                    "allow_methods": [],
                                    "allow_headers": []
                                },
                                "auth": {
                                    "mode": "none"
                                }
                            }
                        ]
                    }
                }
            ]
        }"#;

        let mut plugins = parse_plugins(json.as_bytes()).expect("parse should succeed");
        validate_plugins(&mut plugins).expect("validation should succeed");
    }

    #[test]
    fn test_request_termination_validation() {
        // 1. Valid request_termination config should succeed
        let mut plugins = vec![PluginConfig {
            id: "request_termination".into(),
            config: serde_json::json!({
                "status_code": 503,
                "body": "Service Unavailable",
                "content_type": "text/plain",
                "headers": {}
            }),
        }];

        assert!(validate_plugins(&mut plugins).is_ok());

        // 2. Status code out of range (< 200 or > 599) should fail
        let mut invalid_status_plugins = vec![PluginConfig {
            id: "request_termination".into(),
            config: serde_json::json!({
                "status_code": 199,
                "body": "Bad",
                "content_type": "text/plain",
                "headers": {}
            }),
        }];
        assert!(validate_plugins(&mut invalid_status_plugins).is_err());

        let mut invalid_status_high = vec![PluginConfig {
            id: "request_termination".into(),
            config: serde_json::json!({
                "status_code": 600,
                "body": "Bad",
                "content_type": "text/plain",
                "headers": {}
            }),
        }];
        assert!(validate_plugins(&mut invalid_status_high).is_err());

        // 3. Unknown fields, 204 body, and forbidden framing headers must fail
        let mut unknown_field_plugins = vec![PluginConfig {
            id: "request_termination".into(),
            config: serde_json::json!({
                "status_code": 503,
                "body": "down",
                "content_type": "text/plain",
                "headers": {},
                "bypass_headers": []
            }),
        }];
        assert!(validate_plugins(&mut unknown_field_plugins).is_err());

        let mut body_on_204_plugins = vec![PluginConfig {
            id: "request_termination".into(),
            config: serde_json::json!({
                "status_code": 204,
                "body": "not allowed",
                "content_type": "text/plain",
                "headers": {}
            }),
        }];
        assert!(validate_plugins(&mut body_on_204_plugins).is_err());

        let mut framing_hdr_plugins = vec![PluginConfig {
            id: "request_termination".into(),
            config: serde_json::json!({
                "status_code": 503,
                "body": "",
                "content_type": "",
                "headers": { "transfer-encoding": "chunked" }
            }),
        }];
        assert!(validate_plugins(&mut framing_hdr_plugins).is_err());
    }

    #[test]
    fn test_url_rewrite_validation() {
        let mut valid = vec![PluginConfig {
            id: "url_rewrite".into(),
            config: serde_json::json!({
                "path_rules": [{ "find": "/api/v1/", "replace": "/" }],
                "query_mode": "keep",
                "query_replace": null,
                "remove_params": ["utm_source"],
                "query_append": "edge=velda"
            }),
        }];
        assert!(validate_plugins(&mut valid).is_ok());

        let mut legacy_unknown = vec![PluginConfig {
            id: "url_rewrite".into(),
            config: serde_json::json!({
                "path_rules": [],
                "query_mode": "drop",
                "query_replace": null,
                "remove_params": [],
                "query_append": null,
                "sanitize_path": true
            }),
        }];
        assert!(validate_plugins(&mut legacy_unknown).is_err());
    }

    #[test]
    fn test_validate_plugins_with_etps_rejects_mismatched_etp_kind() {
        let etps = vec![
            crate::post_sync::etp::EtpSyncConfig {
                id: "redis-main".into(),
                kind: "redis".into(),
                security: velda_etp::EtpTransportSecurity::Plain,
                pool: velda_etp::EtpPoolSettings::default(),
                tls: None,
                settings: serde_json::json!({ "host": "127.0.0.1", "port": 6379 }),
            },
            crate::post_sync::etp::EtpSyncConfig {
                id: "kc-main".into(),
                kind: "keycloak".into(),
                security: velda_etp::EtpTransportSecurity::Tls,
                pool: velda_etp::EtpPoolSettings::default(),
                tls: None,
                settings: serde_json::json!({ "host": "kc.local", "port": 8443 }),
            },
        ];

        let oauth2_cfg = |etp_id: &str| PluginConfig {
            id: "oauth2_introspection".into(),
            config: serde_json::json!({
                "etp_id": etp_id,
                "client_id": "gw",
                "client_secret": "sec",
                "auth_method": "client_secret_basic",
                "token_headers": ["authorization"],
                "token_cookies": [],
                "token_query_params": [],
                "token_type_hint": "access_token",
                "cache_ttl_seconds": 60,
                "negative_cache_ttl_seconds": 10,
                "request_timeout_ms": 500,
                "required_scopes": [],
                "claims_to_headers": [],
                "hide_credentials": true,
                "fallback_behavior": "reject"
            }),
        };

        let mut ok_plugins = vec![oauth2_cfg("kc-main")];
        assert!(validate_plugins_with_etps(&mut ok_plugins, &etps).is_ok());

        let mut bad_plugins = vec![oauth2_cfg("redis-main")];
        let err = validate_plugins_with_etps(&mut bad_plugins, &etps).unwrap_err();
        assert!(err.to_string().contains("has kind 'redis'"));
    }
}
