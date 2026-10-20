//! Response Compression Configuration Schema & Semantic Validation (`phase: "post_response"`).
//!
//! Enforces a strict declarative contract for HTTP payload compression algorithms and thresholds.
//!
//! # Strict Declarative Contract (Zero Silent Fallbacks)
//! - **Valid Algorithms**: Every algorithm must be supported (`"gzip"`, `"brotli"`, `"br"`, `"zstd"`, `"deflate"`).
//! - **Safe Length Threshold**: `min_length_bytes` must be within 64B to 10MB.
//! - **Explicit Content-Types**: `content_types` must contain at least one MIME type prefix.

use serde::{Deserialize, Serialize};

use crate::SyncError;

/// Declarative configuration for the Response Compression plugin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResponseCompressionConfig {
    /// Supported compression algorithms in order of preference (e.g. `["brotli", "gzip", "zstd"]`).
    pub algorithms: Vec<String>,
    /// Minimum response body byte length required before applying compression (64B to 10,485,760B).
    pub min_length_bytes: usize,
    /// MIME content-type prefixes eligible for compression (e.g. `["text/", "application/json", "application/javascript"]`).
    pub content_types: Vec<String>,
}

/// Validates Response Compression configuration strictly according to production standards.
pub fn validate_response_compression_config(
    plugin_id: &str,
    config_val: &serde_json::Value,
) -> Result<ResponseCompressionConfig, SyncError> {
    let cfg: ResponseCompressionConfig =
        serde_json::from_value(config_val.clone()).map_err(|e| SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}' has malformed or incomplete response_compression config: {e}"
            ),
        })?;

    // 1. Validate algorithms
    if cfg.algorithms.is_empty() {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!("Plugin '{plugin_id}': algorithms list cannot be empty"),
        });
    }

    for alg in &cfg.algorithms {
        let alg_lower = alg.trim().to_ascii_lowercase();
        match alg_lower.as_str() {
            "gzip" | "brotli" | "br" | "zstd" | "deflate" => {}
            other => {
                return Err(SyncError::Validation {
                    domain: "plugins".into(),
                    reason: format!(
                        "Plugin '{plugin_id}': unsupported compression algorithm '{other}' (expected 'gzip', 'brotli', 'zstd', or 'deflate')"
                    ),
                });
            }
        }
    }

    // 2. Validate min_length_bytes (64 B ..= 10 MB)
    if !(64..=10_485_760).contains(&cfg.min_length_bytes) {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': min_length_bytes ({}) must be between 64 and 10,485,760 bytes (10MB)",
                cfg.min_length_bytes
            ),
        });
    }

    // 3. Validate content_types
    if cfg.content_types.is_empty() {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!("Plugin '{plugin_id}': content_types list cannot be empty"),
        });
    }

    for ct in &cfg.content_types {
        let trimmed = ct.trim();
        if trimmed.is_empty() {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!("Plugin '{plugin_id}': content_types cannot contain empty strings"),
            });
        }
    }

    Ok(cfg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_valid_response_compression() {
        let val = json!({
            "algorithms": ["brotli", "gzip"],
            "min_length_bytes": 1024,
            "content_types": ["text/html", "application/json", "application/javascript"]
        });
        let res = validate_response_compression_config("comp_1", &val);
        assert!(res.is_ok());
    }

    #[test]
    fn test_empty_algorithms_rejected() {
        let val = json!({
            "algorithms": [],
            "min_length_bytes": 1024,
            "content_types": ["application/json"]
        });
        let res = validate_response_compression_config("comp_bad_alg", &val);
        assert!(res.is_err());
        assert!(
            res.unwrap_err()
                .to_string()
                .contains("algorithms list cannot be empty")
        );
    }

    #[test]
    fn test_unsupported_algorithm_rejected() {
        let val = json!({
            "algorithms": ["lzma_unsupported"],
            "min_length_bytes": 1024,
            "content_types": ["application/json"]
        });
        let res = validate_response_compression_config("comp_bad_alg2", &val);
        assert!(res.is_err());
        assert!(
            res.unwrap_err()
                .to_string()
                .contains("unsupported compression algorithm")
        );
    }

    #[test]
    fn test_missing_required_fields_rejected() {
        let val = json!({
            "algorithms": ["gzip"]
            // Missing min_length_bytes, content_types
        });
        let res = validate_response_compression_config("comp_missing", &val);
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("missing field"));
    }
}
