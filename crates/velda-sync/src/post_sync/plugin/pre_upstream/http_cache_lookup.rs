//! HTTP Cache Lookup Configuration Schema & Semantic Validation (`phase: "pre_upstream"`).
//!
//! Enforces a strict declarative contract for HTTP caching and in-memory cache lookup.
//!
//! # Strict Declarative Contract (Zero Silent Fallbacks)
//! - **Strict Protocol Invariant**: Restricted exclusively to HTTP routes.
//! - **Non-Empty Zone**: `zone` must be non-empty.
//! - **Cacheable Methods**: `cacheable_methods` must contain valid HTTP methods.
//! - **Valid Header Grammar**: All header names in `key_headers` must conform to RFC 9110 token grammar.

use serde::{Deserialize, Serialize};

use crate::SyncError;

/// Declarative configuration for the HTTP Cache Lookup plugin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HttpCacheLookupConfig {
    /// Shared cache zone identifier.
    pub zone: String,
    /// HTTP methods eligible for cache lookup (e.g. `["GET", "HEAD"]`).
    pub cacheable_methods: Vec<String>,
    /// Whether to sort query string keys alphabetically before hashing.
    pub ignore_query_order: bool,
    /// Honor client `Cache-Control: no-cache` / `Pragma: no-cache` directives.
    pub honor_client_no_cache: bool,
    /// Whether to bypass cache lookup on requests carrying an `Authorization` header.
    pub bypass_authenticated_requests: bool,
    /// Custom request header names to incorporate into the cache key for per-user / segmented caching.
    pub key_headers: Vec<String>,
    /// Allocated memory limit for this zone if created here (in megabytes).
    pub max_memory_mb: usize,
    /// Default TTL fallback in seconds if response does not specify Cache-Control.
    pub default_ttl_secs: u64,
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

/// Validates HTTP Cache Lookup configuration strictly according to production standards.
pub fn validate_http_cache_lookup_config(
    plugin_id: &str,
    config_val: &serde_json::Value,
) -> Result<HttpCacheLookupConfig, SyncError> {
    let cfg: HttpCacheLookupConfig =
        serde_json::from_value(config_val.clone()).map_err(|e| SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}' has malformed or incomplete http_cache_lookup config: {e}"
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

    // 2. Validate cacheable_methods
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
                    "Plugin '{plugin_id}': invalid HTTP method '{m}' in cacheable_methods"
                ),
            });
        }
    }

    // 3. Validate key_headers
    for h in &cfg.key_headers {
        if !is_valid_header_name(h) {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!("Plugin '{plugin_id}': invalid header name '{h}' in key_headers"),
            });
        }
    }

    // 4. Validate memory and TTL
    if cfg.max_memory_mb == 0 || cfg.max_memory_mb > 1_048_576 {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': max_memory_mb must be between 1 and 1,048,576 MB"
            ),
        });
    }

    if cfg.default_ttl_secs == 0 || cfg.default_ttl_secs > 31_536_000 {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': default_ttl_secs must be between 1 and 31,536,000s"
            ),
        });
    }

    Ok(cfg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_valid_http_cache_lookup() {
        let val = json!({
            "zone": "api_cache",
            "cacheable_methods": ["GET", "HEAD"],
            "ignore_query_order": true,
            "honor_client_no_cache": false,
            "bypass_authenticated_requests": true,
            "key_headers": ["x-user-id", "authorization"],
            "max_memory_mb": 256,
            "default_ttl_secs": 300
        });
        let res = validate_http_cache_lookup_config("cache_look_1", &val);
        assert!(res.is_ok());
    }

    #[test]
    fn test_empty_zone_rejected() {
        let val = json!({
            "zone": "",
            "cacheable_methods": ["GET"],
            "ignore_query_order": true,
            "honor_client_no_cache": false,
            "bypass_authenticated_requests": true,
            "key_headers": [],
            "max_memory_mb": 256,
            "default_ttl_secs": 300
        });
        let res = validate_http_cache_lookup_config("cache_bad_name", &val);
        assert!(res.is_err());
        assert!(
            res.unwrap_err()
                .to_string()
                .contains("zone cannot be empty")
        );
    }

    #[test]
    fn test_missing_required_fields_rejected() {
        let val = json!({
            "zone": "cache1"
        });
        let res = validate_http_cache_lookup_config("cache_missing", &val);
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("missing field"));
    }
}
