//! HTTP Cache Store Configuration Schema & Semantic Validation (`phase: "post_response"`).
//!
//! Enforces a strict declarative contract for HTTP caching and response storage.
//!
//! # Strict Declarative Contract (Zero Silent Fallbacks)
//! - **Strict Protocol Invariant**: Restricted exclusively to HTTP routes.
//! - **Non-Empty Cache Target**: `cache_name` must be non-empty.
//! - **Bounded TTL**: `default_ttl_secs` must be in range 1..=31,536,000s (1 year).
//! - **Bounded Object Size**: `max_object_size_kb` must be between 1 and 65,536 KB (64MB).
//! - **Valid Status Codes**: Every status code in `cacheable_status_codes` must be in 200..=599.

use serde::{Deserialize, Serialize};

use crate::SyncError;

/// Declarative configuration for the HTTP Cache Store plugin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HttpCacheStoreConfig {
    /// Target designated cache storage zone name.
    pub cache_name: String,
    /// Default TTL in seconds when upstream headers omit explicit caching directives (1 to 31,536,000s).
    pub default_ttl_secs: u64,
    /// Maximum cacheable object size in kilobytes (1 to 65,536 KB).
    pub max_object_size_kb: usize,
    /// Whether upstream Cache-Control and Expires headers take precedence over default_ttl_secs.
    pub respect_origin_headers: bool,
    /// HTTP methods eligible for caching (e.g. ["GET", "HEAD"]).
    pub cacheable_methods: Vec<String>,
    /// HTTP status codes eligible for caching (e.g. [200, 203, 300, 301, 302]).
    pub cacheable_status_codes: Vec<u16>,
}

fn is_valid_http_method(m: &str) -> bool {
    let trimmed = m.trim();
    if trimmed.is_empty() {
        return false;
    }
    trimmed
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-')
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

    // 1. Validate cache_name
    let name_trimmed = cfg.cache_name.trim();
    if name_trimmed.is_empty() {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!("Plugin '{plugin_id}': cache_name cannot be empty"),
        });
    }

    // 2. Validate default_ttl_secs (1 ..= 31,536,000s)
    if !(1..=31_536_000).contains(&cfg.default_ttl_secs) {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': default_ttl_secs ({}) must be between 1 and 31,536,000 seconds",
                cfg.default_ttl_secs
            ),
        });
    }

    // 3. Validate max_object_size_kb (1 ..= 65536 KB)
    if !(1..=65536).contains(&cfg.max_object_size_kb) {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': max_object_size_kb ({}) must be between 1 and 65,536 KB",
                cfg.max_object_size_kb
            ),
        });
    }

    // 4. Validate cacheable_methods
    if cfg.cacheable_methods.is_empty() {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!("Plugin '{plugin_id}': cacheable_methods cannot be empty"),
        });
    }
    for m in &cfg.cacheable_methods {
        if !is_valid_http_method(m) {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': invalid HTTP method token '{m}' in cacheable_methods"
                ),
            });
        }
    }

    // 5. Validate cacheable_status_codes
    if cfg.cacheable_status_codes.is_empty() {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!("Plugin '{plugin_id}': cacheable_status_codes cannot be empty"),
        });
    }
    for s in &cfg.cacheable_status_codes {
        if !(200..=599).contains(s) {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': status code {s} in cacheable_status_codes must be between 200 and 599"
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
            "cache_name": "edge_cache_tier1",
            "default_ttl_secs": 3600,
            "max_object_size_kb": 2048,
            "respect_origin_headers": true,
            "cacheable_methods": ["GET", "HEAD"],
            "cacheable_status_codes": [200, 301, 302]
        });
        let res = validate_http_cache_store_config("cache_store_1", &val);
        assert!(res.is_ok());
    }

    #[test]
    fn test_empty_cache_name_rejected() {
        let val = json!({
            "cache_name": "",
            "default_ttl_secs": 3600,
            "max_object_size_kb": 2048,
            "respect_origin_headers": true,
            "cacheable_methods": ["GET"],
            "cacheable_status_codes": [200]
        });
        let res = validate_http_cache_store_config("cache_store_bad_name", &val);
        assert!(res.is_err());
        assert!(
            res.unwrap_err()
                .to_string()
                .contains("cache_name cannot be empty")
        );
    }

    #[test]
    fn test_invalid_status_code_rejected() {
        let val = json!({
            "cache_name": "cache1",
            "default_ttl_secs": 3600,
            "max_object_size_kb": 2048,
            "respect_origin_headers": true,
            "cacheable_methods": ["GET"],
            "cacheable_status_codes": [199]
        });
        let res = validate_http_cache_store_config("cache_store_bad_status", &val);
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("between 200 and 599"));
    }

    #[test]
    fn test_missing_required_fields_rejected() {
        let val = json!({
            "cache_name": "cache1"
            // Missing default_ttl_secs, max_object_size_kb, respect_origin_headers, cacheable_methods, cacheable_status_codes
        });
        let res = validate_http_cache_store_config("cache_store_missing", &val);
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("missing field"));
    }
}
