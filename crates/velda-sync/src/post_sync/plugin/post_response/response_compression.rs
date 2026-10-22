//! Response Compression Configuration Schema & Semantic Validation (`phase: "post_response"`).
//!
//! Enforces a strict declarative contract for HTTP payload compression algorithms and thresholds.
//!
//! # Strict Declarative Contract (Zero Silent Fallbacks)
//! - **Valid Algorithms**: Every algorithm must be supported (`"gzip"`, `"brotli"`, `"br"`, `"zstd"`, `"deflate"`).
//! - **Safe Length Threshold**: `min_length_bytes` must be within 64B to 10MB.
//! - **Bounded Codec Levels**: `gzip_level` (1..=9), `brotli_level` (0..=11), `zstd_level` (1..=22).
//! - **Explicit Content-Types**: `content_types` must contain at least one MIME type prefix.

use serde::{Deserialize, Serialize};

use crate::SyncError;

/// Declarative configuration for the Response Compression plugin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResponseCompressionConfig {
    /// Supported compression algorithms in order of preference (e.g. `["zstd", "brotli", "gzip"]`).
    pub algorithms: Vec<String>,
    /// Minimum response body byte length required before applying compression (64B to 10,485,760B).
    pub min_length_bytes: usize,
    /// Gzip/Deflate compression level (1-9).
    pub gzip_level: u32,
    /// Brotli compression quality level (0-11).
    pub brotli_level: u32,
    /// Zstandard compression level (1-22).
    pub zstd_level: i32,
    /// Optional pre-trained Zstandard dictionary (RFC 8878) as Base64 string, or null.
    pub zstd_dict: Option<String>,
    /// MIME content-type prefixes eligible for compression (e.g. `["text/", "application/json"]`).
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

    // 3. Validate compression levels
    if !(1..=9).contains(&cfg.gzip_level) {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': gzip_level ({}) must be between 1 and 9",
                cfg.gzip_level
            ),
        });
    }

    if cfg.brotli_level > 11 {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': brotli_level ({}) must be between 0 and 11",
                cfg.brotli_level
            ),
        });
    }

    if !(1..=22).contains(&cfg.zstd_level) {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': zstd_level ({}) must be between 1 and 22",
                cfg.zstd_level
            ),
        });
    }

    // 4. Validate content_types
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
            "algorithms": ["zstd", "brotli", "gzip"],
            "min_length_bytes": 256,
            "gzip_level": 1,
            "brotli_level": 2,
            "zstd_level": 1,
            "zstd_dict": null,
            "content_types": ["text/", "application/json", "application/javascript"]
        });
        let res = validate_response_compression_config("compress_1", &val);
        assert!(res.is_ok());
    }

    #[test]
    fn test_unsupported_algorithm_rejected() {
        let val = json!({
            "algorithms": ["snappy"],
            "min_length_bytes": 256,
            "gzip_level": 1,
            "brotli_level": 2,
            "zstd_level": 1,
            "zstd_dict": null,
            "content_types": ["text/"]
        });
        let res = validate_response_compression_config("compress_bad_alg", &val);
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
            // Missing all other fields
        });
        let res = validate_response_compression_config("compress_missing", &val);
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("missing field"));
    }
}
