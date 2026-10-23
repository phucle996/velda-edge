//! Plugin Domain Compiler & Lifecycle Engine (`velda-sync::post_sync::plugin`).
//!
//! Converts raw plugin configurations (`plugins.json`) into optimized, checksummed
//! binary runtime artifacts (`plugins.bin`) for consumption by the Data Plane and Auxiliary Subsystem.
//!
//! # Modular Plugin Schema Architecture
//! Individual plugin configuration schemas and validation rules are organized under
//! dedicated sub-modules by plugin type:
//! - [`prometheus_exporter`]: Schema and strict semantic validation for Prometheus Exporter.
//! - Future plugins (`jwt_auth`, `rate_limit`, `ip_restriction`) are isolated similarly.

use std::collections::HashSet;
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::SyncError;

pub mod pre_route;
pub use pre_route::*;

pub mod pre_upstream;
pub use pre_upstream::*;

pub mod post_response;
pub use post_response::*;

pub mod health_probe;
pub use health_probe::*;

pub mod prometheus_exporter;
pub use prometheus_exporter::*;

pub mod otlp_metrics_exporter;
pub use otlp_metrics_exporter::*;

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
    #[serde(default)]
    pub phase: String, // "pre_route", "pre_upstream", "post_response", "background"
    #[serde(with = "json_value_bincode")]
    pub config: serde_json::Value,
}

/// Returns the canonical execution phase for a known plugin identifier.
pub fn canonical_phase_for_id(id: &str) -> Option<&'static str> {
    match id {
        "api_key_auth" => Some("pre_upstream"),
        "jwt_auth" => Some("pre_upstream"),
        "hmac_auth" => Some("pre_upstream"),
        "oauth2_introspection" => Some("pre_upstream"),
        "ip_filter" => Some("pre_route"),
        "cors_preflight" => Some("pre_route"),
        "resource_limit" => Some("pre_route"),
        "waf_lite" => Some("pre_route"),
        "rate_limit" => Some("pre_upstream"),
        "header_mutation" => Some("pre_upstream"),
        "canary" => Some("pre_upstream"),
        "request_mirror" => Some("pre_upstream"),
        "url_rewrite" => Some("pre_upstream"),
        "http_cache_lookup" => Some("pre_upstream"),
        "http_fault_injection" => Some("pre_upstream"),
        "grpc_fault_injection" => Some("pre_upstream"),
        "request_termination" => Some("pre_upstream"),
        "w3c_trace_context" => Some("pre_upstream"),
        "csrf" => Some("pre_upstream"),
        "csrf_protection" => Some("pre_upstream"),
        "security_headers" => Some("post_response"),
        "cors" => Some("post_response"),
        "http_cache_store" => Some("post_response"),
        "response_header_mutation" => Some("post_response"),
        "response_compression" => Some("post_response"),
        "access_log" => Some("post_response"),
        "opentelemetry_logger" => Some("post_response"),
        "opentelemetry_tracer" => Some("post_response"),
        "prometheus_exporter" => Some("background"),
        "health_probe" => Some("background"),
        "otlp_metrics_exporter" => Some("background"),
        _ => None,
    }
}

/// Returns the restricted protocol family for a known plugin identifier, if any.
/// Plugins returning `Some("grpc")` are strictly forbidden on HTTP/Raw routes.
/// Plugins returning `Some("http")` are strictly forbidden on gRPC/Raw routes.
pub fn canonical_protocol_family_for_id(id: &str) -> Option<&'static str> {
    match id {
        "grpc_fault_injection" => Some("grpc"),
        "http_fault_injection" => Some("http"),
        "cors_preflight" => Some("http"),
        "cors" => Some("http"),
        "http_cache_lookup" => Some("http"),
        "http_cache_store" => Some("http"),
        "response_header_mutation" => Some("http"),
        "response_compression" => Some("http"),
        "request_termination" => Some("http"),
        _ => None,
    }
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
/// Internal side effects (auto-trimming whitespace, lowercasing phase) are handled in place.
/// External side effects (duplicate ID, missing type, invalid phase) return an error to prevent
/// deploying a broken hook chain to the Data Plane.
pub fn validate_plugins(plugins: &mut [PluginConfig]) -> Result<(), SyncError> {
    let mut seen_ids = HashSet::with_capacity(plugins.len());

    for plugin in plugins {
        trim_in_place(&mut plugin.id);
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

        // Auto-infer empty phase from canonical phase mapping
        if plugin.phase.is_empty()
            && let Some(canonical) = canonical_phase_for_id(&plugin.id)
        {
            plugin.phase = canonical.to_string();
        }

        let is_valid_phase = matches!(
            plugin.phase.as_str(),
            "pre_route" | "pre_upstream" | "post_response" | "background"
        );

        if !is_valid_phase {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{}' has invalid phase '{}' (must be pre_route, pre_upstream, post_response, or background)",
                    plugin.id, plugin.phase
                ),
            });
        }

        // Modular semantic validation dispatched per plugin ID
        match plugin.id.as_str() {
            "ip_filter" => {
                if plugin.phase != "pre_route" {
                    return Err(SyncError::Validation {
                        domain: "plugins".into(),
                        reason: format!("Plugin '{}' must specify phase 'pre_route'", plugin.id),
                    });
                }
                let normalized = pre_route::validate_ip_filter_config(&plugin.id, &plugin.config)?;
                plugin.config =
                    serde_json::to_value(normalized).map_err(|e| SyncError::Validation {
                        domain: "plugins".into(),
                        reason: format!(
                            "Plugin '{}' failed to serialize normalized config: {e}",
                            plugin.id
                        ),
                    })?;
            }
            "cors_preflight" => {
                if plugin.phase != "pre_route" {
                    return Err(SyncError::Validation {
                        domain: "plugins".into(),
                        reason: format!("Plugin '{}' must specify phase 'pre_route'", plugin.id),
                    });
                }
                pre_route::validate_cors_preflight_config(&plugin.id, &plugin.config)?;
            }
            "resource_limit" => {
                if plugin.phase != "pre_route" {
                    return Err(SyncError::Validation {
                        domain: "plugins".into(),
                        reason: format!("Plugin '{}' must specify phase 'pre_route'", plugin.id),
                    });
                }
                pre_route::validate_resource_limit_config(&plugin.id, &plugin.config)?;
            }
            "waf_lite" => {
                if plugin.phase != "pre_route" {
                    return Err(SyncError::Validation {
                        domain: "plugins".into(),
                        reason: format!("Plugin '{}' must specify phase 'pre_route'", plugin.id),
                    });
                }
                let normalized = pre_route::validate_waf_lite_config(&plugin.id, &plugin.config)?;
                plugin.config =
                    serde_json::to_value(normalized).map_err(|e| SyncError::Validation {
                        domain: "plugins".into(),
                        reason: format!(
                            "Plugin '{}' failed to serialize normalized config: {e}",
                            plugin.id
                        ),
                    })?;
            }
            "api_key_auth" => {
                if plugin.phase != "pre_upstream" {
                    return Err(SyncError::Validation {
                        domain: "plugins".into(),
                        reason: format!("Plugin '{}' must specify phase 'pre_upstream'", plugin.id),
                    });
                }
                pre_upstream::validate_api_key_auth_config(&plugin.id, &plugin.config)?;
            }
            "jwt_auth" => {
                if plugin.phase != "pre_upstream" {
                    return Err(SyncError::Validation {
                        domain: "plugins".into(),
                        reason: format!("Plugin '{}' must specify phase 'pre_upstream'", plugin.id),
                    });
                }
                pre_upstream::validate_jwt_auth_config(&plugin.id, &plugin.config)?;
            }
            "rate_limit" => {
                if plugin.phase != "pre_upstream" {
                    return Err(SyncError::Validation {
                        domain: "plugins".into(),
                        reason: format!("Plugin '{}' must specify phase 'pre_upstream'", plugin.id),
                    });
                }
                pre_upstream::validate_rate_limit_config(&plugin.id, &plugin.config)?;
            }
            "canary" => {
                if plugin.phase != "pre_upstream" {
                    return Err(SyncError::Validation {
                        domain: "plugins".into(),
                        reason: format!("Plugin '{}' must specify phase 'pre_upstream'", plugin.id),
                    });
                }
                pre_upstream::validate_canary_config(&plugin.id, &plugin.config)?;
            }
            "request_mirror" => {
                if plugin.phase != "pre_upstream" {
                    return Err(SyncError::Validation {
                        domain: "plugins".into(),
                        reason: format!("Plugin '{}' must specify phase 'pre_upstream'", plugin.id),
                    });
                }
                pre_upstream::validate_request_mirror_config(&plugin.id, &plugin.config)?;
            }
            "url_rewrite" => {
                if plugin.phase != "pre_upstream" {
                    return Err(SyncError::Validation {
                        domain: "plugins".into(),
                        reason: format!("Plugin '{}' must specify phase 'pre_upstream'", plugin.id),
                    });
                }
                pre_upstream::validate_url_rewrite_config(&plugin.id, &plugin.config)?;
            }
            "grpc_fault_injection" => {
                if plugin.phase != "pre_upstream" {
                    return Err(SyncError::Validation {
                        domain: "plugins".into(),
                        reason: format!("Plugin '{}' must specify phase 'pre_upstream'", plugin.id),
                    });
                }
                pre_upstream::validate_grpc_fault_injection_config(&plugin.id, &plugin.config)?;
            }
            "http_fault_injection" => {
                if plugin.phase != "pre_upstream" {
                    return Err(SyncError::Validation {
                        domain: "plugins".into(),
                        reason: format!("Plugin '{}' must specify phase 'pre_upstream'", plugin.id),
                    });
                }
                pre_upstream::validate_http_fault_injection_config(&plugin.id, &plugin.config)?;
            }
            "request_termination" => {
                if plugin.phase != "pre_upstream" {
                    return Err(SyncError::Validation {
                        domain: "plugins".into(),
                        reason: format!("Plugin '{}' must specify phase 'pre_upstream'", plugin.id),
                    });
                }
                pre_upstream::validate_request_termination_config(&plugin.id, &plugin.config)?;
            }
            "header_mutation" => {
                if plugin.phase != "pre_upstream" {
                    return Err(SyncError::Validation {
                        domain: "plugins".into(),
                        reason: format!("Plugin '{}' must specify phase 'pre_upstream'", plugin.id),
                    });
                }
                pre_upstream::validate_header_mutation_config(&plugin.id, &plugin.config)?;
            }
            "hmac_auth" => {
                if plugin.phase != "pre_upstream" {
                    return Err(SyncError::Validation {
                        domain: "plugins".into(),
                        reason: format!("Plugin '{}' must specify phase 'pre_upstream'", plugin.id),
                    });
                }
                pre_upstream::validate_hmac_auth_config(&plugin.id, &plugin.config)?;
            }
            "oauth2_introspection" => {
                if plugin.phase != "pre_upstream" {
                    return Err(SyncError::Validation {
                        domain: "plugins".into(),
                        reason: format!("Plugin '{}' must specify phase 'pre_upstream'", plugin.id),
                    });
                }
                pre_upstream::validate_oauth2_introspection_config(&plugin.id, &plugin.config)?;
            }
            "prometheus_exporter" => {
                if plugin.phase != "background" {
                    return Err(SyncError::Validation {
                        domain: "plugins".into(),
                        reason: format!("Plugin '{}' must specify phase 'background'", plugin.id),
                    });
                }

                prometheus_exporter::validate_prometheus_exporter_config(
                    &plugin.id,
                    &plugin.config,
                )?;
            }
            "http_cache_lookup" => {
                if plugin.phase != "pre_upstream" {
                    return Err(SyncError::Validation {
                        domain: "plugins".into(),
                        reason: format!("Plugin '{}' must specify phase 'pre_upstream'", plugin.id),
                    });
                }
                pre_upstream::validate_http_cache_lookup_config(&plugin.id, &plugin.config)?;
            }
            "http_cache_store" => {
                if plugin.phase != "post_response" {
                    return Err(SyncError::Validation {
                        domain: "plugins".into(),
                        reason: format!(
                            "Plugin '{}' must specify phase 'post_response'",
                            plugin.id
                        ),
                    });
                }
                post_response::validate_http_cache_store_config(&plugin.id, &plugin.config)?;
            }
            "security_headers" => {
                if plugin.phase != "post_response" {
                    return Err(SyncError::Validation {
                        domain: "plugins".into(),
                        reason: format!(
                            "Plugin '{}' must specify phase 'post_response'",
                            plugin.id
                        ),
                    });
                }
                post_response::validate_security_headers_config(&plugin.id, &plugin.config)?;
            }
            "cors" => {
                if plugin.phase != "post_response" {
                    return Err(SyncError::Validation {
                        domain: "plugins".into(),
                        reason: format!(
                            "Plugin '{}' must specify phase 'post_response'",
                            plugin.id
                        ),
                    });
                }
                post_response::validate_cors_response_config(&plugin.id, &plugin.config)?;
            }
            "response_header_mutation" => {
                if plugin.phase != "post_response" {
                    return Err(SyncError::Validation {
                        domain: "plugins".into(),
                        reason: format!(
                            "Plugin '{}' must specify phase 'post_response'",
                            plugin.id
                        ),
                    });
                }
                post_response::validate_response_header_mutation_config(
                    &plugin.id,
                    &plugin.config,
                )?;
            }
            "response_compression" => {
                if plugin.phase != "post_response" {
                    return Err(SyncError::Validation {
                        domain: "plugins".into(),
                        reason: format!(
                            "Plugin '{}' must specify phase 'post_response'",
                            plugin.id
                        ),
                    });
                }
                post_response::validate_response_compression_config(&plugin.id, &plugin.config)?;
            }
            "access_log" => {
                if plugin.phase != "post_response" {
                    return Err(SyncError::Validation {
                        domain: "plugins".into(),
                        reason: format!(
                            "Plugin '{}' must specify phase 'post_response'",
                            plugin.id
                        ),
                    });
                }
                post_response::validate_access_log_config(&plugin.id, &plugin.config)?;
            }
            "health_probe" => {
                if plugin.phase != "background" {
                    return Err(SyncError::Validation {
                        domain: "plugins".into(),
                        reason: format!("Plugin '{}' must specify phase 'background'", plugin.id),
                    });
                }
                health_probe::validate_health_probe_config(&plugin.id, &plugin.config)?;
            }
            "otlp_metrics_exporter" => {
                if plugin.phase != "background" {
                    return Err(SyncError::Validation {
                        domain: "plugins".into(),
                        reason: format!("Plugin '{}' must specify phase 'background'", plugin.id),
                    });
                }
                otlp_metrics_exporter::validate_otlp_metrics_exporter_config(
                    &plugin.id,
                    &plugin.config,
                )?;
            }
            "opentelemetry_logger" => {
                if plugin.phase != "post_response" {
                    return Err(SyncError::Validation {
                        domain: "plugins".into(),
                        reason: format!(
                            "Plugin '{}' must specify phase 'post_response'",
                            plugin.id
                        ),
                    });
                }
                post_response::validate_opentelemetry_logger_config(&plugin.id, &plugin.config)?;
            }
            "opentelemetry_tracer" => {
                if plugin.phase != "post_response" {
                    return Err(SyncError::Validation {
                        domain: "plugins".into(),
                        reason: format!(
                            "Plugin '{}' must specify phase 'post_response'",
                            plugin.id
                        ),
                    });
                }
                post_response::validate_opentelemetry_tracer_config(&plugin.id, &plugin.config)?;
            }
            "w3c_trace_context" => {
                if plugin.phase != "pre_upstream" {
                    return Err(SyncError::Validation {
                        domain: "plugins".into(),
                        reason: format!("Plugin '{}' must specify phase 'pre_upstream'", plugin.id),
                    });
                }
                pre_upstream::validate_w3c_trace_context_config(&plugin.id, &plugin.config)?;
            }
            _ => {
                // Future plugins will dispatch to their respective validation modules
            }
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
                    "id": " jwt_auth ",
                    "phase": "PRE_UPSTREAM",
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
        assert_eq!(plugins[0].phase, "pre_upstream");
    }

    #[test]
    fn test_invalid_phase_fails() {
        let mut plugins = vec![PluginConfig {
            id: "rate_limit".into(),
            phase: "invalid_phase".into(),
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
            phase: "pre_upstream".into(),
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
    fn test_valid_prometheus_exporter_plugin() {
        let json = r#"{
            "schema_version": 1,
            "plugins": [
                {
                    "id": "prometheus_exporter",
                    "phase": "background",
                    "config": {
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
                }
            ]
        }"#;

        let mut plugins = parse_plugins(json.as_bytes()).expect("parse should succeed");
        validate_plugins(&mut plugins).expect("validation should succeed");
    }

    #[test]
    fn test_prometheus_exporter_must_be_background() {
        let mut plugins = vec![PluginConfig {
            id: "prometheus_exporter".into(),
            phase: "pre_route".into(),
            config: serde_json::json!({
                "listen": "0.0.0.0:9090",
                "path": "/metrics",
                "protocol": "http",
                "scrape": {
                    "timeout_ms": 5000,
                    "max_concurrency": 16,
                    "max_buffer_size_kb": 256
                },
                "tls": { "mode": "none" },
                "cors": { "enabled": false },
                "auth": { "mode": "none" }
            }),
        }];

        let res = validate_plugins(&mut plugins);
        assert!(res.is_err());
        assert!(
            res.unwrap_err()
                .to_string()
                .contains("must specify phase 'background'")
        );
    }

    #[test]
    fn test_request_termination_validation_and_phase_inference() {
        // 1. Empty phase should be auto-inferred to pre_upstream
        let mut plugins = vec![PluginConfig {
            id: "request_termination".into(),
            phase: "".into(),
            config: serde_json::json!({
                "status_code": 503,
                "body": "Service Unavailable",
                "content_type": "text/plain",
                "headers": {}
            }),
        }];

        assert!(validate_plugins(&mut plugins).is_ok());
        assert_eq!(plugins[0].phase, "pre_upstream");

        // 2. Mismatched phase should fail
        let mut invalid_phase_plugins = vec![PluginConfig {
            id: "request_termination".into(),
            phase: "post_response".into(),
            config: serde_json::json!({
                "status_code": 200,
                "body": "OK",
                "content_type": "text/plain",
                "headers": {}
            }),
        }];
        assert!(validate_plugins(&mut invalid_phase_plugins).is_err());

        // 3. Status code out of range (< 200 or > 599) should fail
        let mut invalid_status_plugins = vec![PluginConfig {
            id: "request_termination".into(),
            phase: "pre_upstream".into(),
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
            phase: "pre_upstream".into(),
            config: serde_json::json!({
                "status_code": 600,
                "body": "Bad",
                "content_type": "text/plain",
                "headers": {}
            }),
        }];
        assert!(validate_plugins(&mut invalid_status_high).is_err());
    }
}
