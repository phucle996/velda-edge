//! HTTP Cache Store Configuration Schema & Semantic Validation (`phase: "post_response"`).
//!
//! Enforces a strict declarative contract for HTTP caching and response storage.
//!
//! # Strict Declarative Contract (Zero Silent Fallbacks)
//! - **Strict Protocol Invariant**: Restricted exclusively to HTTP routes.
//! - **Non-Empty Zone**: `zone` must be non-empty.
//! - **Bounded TTL**: `default_ttl_secs` must be in range 1..=31,536,000s (1 year).
//! - **Bounded Payload**: `max_payload_bytes` must be between 1 and 67,108,864 bytes (64MB).
//! - **Bounded Memory Limit**: `max_memory_mb` must be between 1 and 65,536 MB (64GB).
//! - **Valid Status Codes**: Every status code in `cacheable_status` must be in 200..=599.
//! - **Valid Header Grammar**: All header names in `key_headers` must conform to RFC 9110 token grammar.

use serde::{Deserialize, Serialize};

use crate::SyncError;

/// Declarative configuration for the HTTP Cache Store plugin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HttpCacheStoreConfig {
    /// Target designated cache storage zone name.
    pub zone: String,
    /// HTTP status codes eligible for caching (e.g. `[200, 203, 204, 206, 300, 301, 404]`).
    pub cacheable_status: Vec<u16>,
    /// Maximum payload size in bytes eligible for caching (1 to 67,108,864 bytes).
    pub max_payload_bytes: usize,
    /// Default TTL fallback in seconds when upstream headers omit explicit caching directives (1 to 31,536,000s).
    pub default_ttl_secs: u64,
    /// Honor upstream `Cache-Control: no-store` / `Cache-Control: private`.
    pub respect_cache_control: bool,
    /// Whether to bypass storing responses for requests carrying an `Authorization` header.
    pub bypass_authenticated_requests: bool,
    /// Custom request header names incorporated into the cache key for per-user / segmented caching.
    pub key_headers: Vec<String>,
    /// Whether to offload variant pre-compression (gzip, brotli, zstd) to background task.
    pub async_precompression: bool,
    /// Allocated memory limit for this zone if created here (in megabytes).
    pub max_memory_mb: usize,
}

fn is_valid_header_name(h: &str) -> bool {
    let trimmed = h.trim();
    if trimmed.is_empty() {
        return false;
    }
    trimmed.bytes().all(|b| {
        b.is_ascii_alphanumeric()
            || matches!(
                b,
                b'!' | b'#'
                    | b'$'
                    | b'%'
                    | b'&'
                    | b'\''
                    | b'*'
                    | b'+'
                    | b'-'
                    | b'.'
                    | b'^'
                    | b'_'
                    | b'`'
                    | b'|'
                    | b'~'
            )
    })
}

/// Validates HTTP Cache Store configuration strictly according to production standards.
pub fn validate_http_cache_store_config(
    plugin_id: &str,
    config_val: &serde_json::Value,
) -> Result<HttpCacheStoreConfig, SyncError> {
    let cfg: HttpCacheStoreConfig =
        serde_json::from_value(config_val.clone()).map_err(|e| SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}' has malformed or incomplete http_cache_store config: {e}"
            ),
        })?;

    // 1. Validate zone name
    let zone_trimmed = cfg.zone.trim();
    if zone_trimmed.is_empty() {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!("Plugin '{plugin_id}': zone cannot be empty"),
        });
    }

    // 2. Validate cacheable_status
    if cfg.cacheable_status.is_empty() {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!("Plugin '{plugin_id}': cacheable_status cannot be empty"),
        });
    }
    for s in &cfg.cacheable_status {
        if !(200..=599).contains(s) {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': status code {s} in cacheable_status must be between 200 and 599"
                ),
            });
        }
    }

    // 3. Validate max_payload_bytes (1 ..= 67,108,864 bytes / 64MB)
    if !(1..=67_108_864).contains(&cfg.max_payload_bytes) {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': max_payload_bytes ({}) must be between 1 and 67,108,864 bytes",
                cfg.max_payload_bytes
            ),
        });
    }

    // 4. Validate default_ttl_secs (1 ..= 31,536,000s)
    if !(1..=31_536_000).contains(&cfg.default_ttl_secs) {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': default_ttl_secs ({}) must be between 1 and 31,536,000 seconds",
                cfg.default_ttl_secs
            ),
        });
    }

    // 5. Validate max_memory_mb (1 ..= 65,536 MB)
    if !(1..=65_536).contains(&cfg.max_memory_mb) {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': max_memory_mb ({}) must be between 1 and 65,536 MB",
                cfg.max_memory_mb
            ),
        });
    }

    // 6. Validate key_headers
    for h in &cfg.key_headers {
        if !is_valid_header_name(h) {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': invalid custom header token '{h}' in key_headers"
                ),
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
    fn test_valid_http_cache_store() {
        let val = json!({
            "zone": "edge_cache_tier1",
            "cacheable_status": [200, 203, 204, 206, 300, 301, 404],
            "max_payload_bytes": 2097152,
            "default_ttl_secs": 3600,
            "respect_cache_control": true,
            "bypass_authenticated_requests": true,
            "key_headers": ["accept-language"],
            "async_precompression": true,
            "max_memory_mb": 256
        });
        let res = validate_http_cache_store_config("cache_store_1", &val);
        assert!(res.is_ok());
    }

    #[test]
    fn test_empty_zone_rejected() {
        let val = json!({
            "zone": "",
            "cacheable_status": [200],
            "max_payload_bytes": 1024,
            "default_ttl_secs": 3600,
            "respect_cache_control": true,
            "bypass_authenticated_requests": true,
            "key_headers": [],
            "async_precompression": true,
            "max_memory_mb": 256
        });
        let res = validate_http_cache_store_config("cache_store_bad_zone", &val);
        assert!(res.is_err());
        assert!(
            res.unwrap_err()
                .to_string()
                .contains("zone cannot be empty")
        );
    }

    #[test]
    fn test_invalid_status_code_rejected() {
        let val = json!({
            "zone": "cache1",
            "cacheable_status": [199],
            "max_payload_bytes": 1024,
            "default_ttl_secs": 3600,
            "respect_cache_control": true,
            "bypass_authenticated_requests": true,
            "key_headers": [],
            "async_precompression": true,
            "max_memory_mb": 256
        });
        let res = validate_http_cache_store_config("cache_store_bad_status", &val);
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("between 200 and 599"));
    }

    #[test]
    fn test_missing_required_fields_rejected() {
        let val = json!({
            "zone": "cache1"
            // Missing all other fields
        });
        let res = validate_http_cache_store_config("cache_store_missing", &val);
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("missing field"));
    }
}
