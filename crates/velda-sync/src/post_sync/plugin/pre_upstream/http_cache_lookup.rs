//! HTTP Cache Lookup Configuration Schema & Semantic Validation (`phase: "pre_upstream"`).
//!
//! Enforces a strict declarative contract for HTTP caching and in-memory cache lookup.
//!
//! # Strict Declarative Contract (Zero Silent Fallbacks)
//! - **Strict Protocol Invariant**: Restricted exclusively to HTTP routes.
//! - **Non-Empty Cache Target**: `cache_name` must be non-empty.
//! - **Valid Strategy**: `key_strategy` must be one of `"uri"`, `"uri_and_headers"`, or `"custom"`.
//! - **Valid Header Grammar**: All header names in `key_headers` must conform to RFC 9110 token grammar.

use serde::{Deserialize, Serialize};

use crate::SyncError;

/// Declarative configuration for the HTTP Cache Lookup plugin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HttpCacheLookupConfig {
    /// Target designated cache storage zone name.
    pub cache_name: String,
    /// Cache key derivation strategy: "uri", "uri_and_headers", or "custom".
    pub key_strategy: String,
    /// Headers included in cache key computation (when key_strategy is "uri_and_headers" or "custom").
    pub key_headers: Vec<String>,
    /// Optional header name that triggers an unconditional cache bypass.
    pub bypass_header: Option<String>,
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

    // 1. Validate cache_name
    let name_trimmed = cfg.cache_name.trim();
    if name_trimmed.is_empty() {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!("Plugin '{plugin_id}': cache_name cannot be empty"),
        });
    }

    // 2. Validate key_strategy
    let strat_lower = cfg.key_strategy.trim().to_ascii_lowercase();
    match strat_lower.as_str() {
        "uri" | "uri_and_headers" | "custom" => {}
        other => {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': invalid key_strategy '{other}' (expected 'uri', 'uri_and_headers', or 'custom')"
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

    // 4. Validate bypass_header
    if let Some(ref bh) = cfg.bypass_header
        && !is_valid_header_name(bh)
    {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!("Plugin '{plugin_id}': invalid header name '{bh}' in bypass_header"),
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
            "cache_name": "edge_cache_tier1",
            "key_strategy": "uri_and_headers",
            "key_headers": ["Accept-Encoding", "Accept"],
            "bypass_header": "X-Cache-Bypass"
        });
        let res = validate_http_cache_lookup_config("cache_look_1", &val);
        assert!(res.is_ok());
    }

    #[test]
    fn test_empty_cache_name_rejected() {
        let val = json!({
            "cache_name": "",
            "key_strategy": "uri",
            "key_headers": [],
            "bypass_header": null
        });
        let res = validate_http_cache_lookup_config("cache_bad_name", &val);
        assert!(res.is_err());
        assert!(
            res.unwrap_err()
                .to_string()
                .contains("cache_name cannot be empty")
        );
    }

    #[test]
    fn test_invalid_key_strategy_rejected() {
        let val = json!({
            "cache_name": "cache1",
            "key_strategy": "ip_hash",
            "key_headers": [],
            "bypass_header": null
        });
        let res = validate_http_cache_lookup_config("cache_bad_strat", &val);
        assert!(res.is_err());
        assert!(
            res.unwrap_err()
                .to_string()
                .contains("invalid key_strategy")
        );
    }

    #[test]
    fn test_missing_required_fields_rejected() {
        let val = json!({
            "cache_name": "cache1"
            // Missing key_strategy, key_headers, bypass_header
        });
        let res = validate_http_cache_lookup_config("cache_missing", &val);
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("missing field"));
    }
}
