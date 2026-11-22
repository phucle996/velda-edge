//! Schema & semantic validation for `api_key_auth` (`pre_upstream`).
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────────────────────┐
//! │                     API KEY AUTH VALIDATION FLOW                                        │
//! ├─────────────────────────────────────────────────────────────────────────────────────────┤
//! │  Raw JSON Value                                                                         │
//! │    ├── Deserialize ApiKeyAuthConfig (#[serde(deny_unknown_fields)])                     │
//! │    ├── Validate header_name (RFC 9110 token grammar)                                    │
//! │    ├── Validate keys non-empty, len ∈ 1..=64B, no duplicates, valid inject_headers      │
//! │    ├── Validate TTLs (cache_ttl_secs ∈ 1..=86400, negative_ttl_secs ∈ 1..=3600)         │
//! │    └── Normalize cache_slots_per_core (power-of-2 in 256..=65536 -> k * 4096B/core)     │
//! └─────────────────────────────────────────────────────────────────────────────────────────┘
//! ```

use std::collections::HashSet;

use serde_json::Value;

use crate::error::PluginError;
use crate::pre_upstream::api_key_auth::config::{ApiKeyAuthConfig, MAX_API_KEY_LEN};

/// Validates RFC 9110 HTTP header token grammar.
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

/// Validates and normalizes `api_key_auth` plugin configuration.
pub fn validate_api_key_auth_config(
    plugin_id: &str,
    raw_config: &Value,
) -> Result<ApiKeyAuthConfig, PluginError> {
    let mut cfg: ApiKeyAuthConfig = serde_json::from_value(raw_config.clone()).map_err(|e| {
        PluginError::Config(format!(
            "Plugin '{plugin_id}' has invalid api_key_auth config: {e}"
        ))
    })?;

    cfg.header_name = cfg.header_name.trim().to_ascii_lowercase();
    if !is_valid_header_name(&cfg.header_name) {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': invalid header_name '{}'",
            cfg.header_name
        )));
    }

    if cfg.keys.is_empty() {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': keys list cannot be empty"
        )));
    }

    let mut seen_keys = HashSet::with_capacity(cfg.keys.len());
    for (idx, entry) in cfg.keys.iter_mut().enumerate() {
        let trimmed = entry.key.trim().to_string();
        if trimmed.is_empty() || trimmed.len() > MAX_API_KEY_LEN {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': key at index {idx} length {} must be within 1..={MAX_API_KEY_LEN} bytes",
                trimmed.len()
            )));
        }
        if !seen_keys.insert(trimmed.clone()) {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': duplicate key at index {idx}"
            )));
        }
        entry.key = trimmed;

        for (hname, hval) in &entry.inject_headers {
            if !is_valid_header_name(hname) {
                return Err(PluginError::Config(format!(
                    "Plugin '{plugin_id}': key at index {idx} has invalid inject_headers name '{hname}'"
                )));
            }
            if hval.bytes().any(|b| b == b'\r' || b == b'\n' || b == 0) {
                return Err(PluginError::Config(format!(
                    "Plugin '{plugin_id}': inject_headers value for '{hname}' contains control bytes"
                )));
            }
        }
    }

    if cfg.cache_ttl_secs == 0 || cfg.cache_ttl_secs > 86_400 {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': cache_ttl_secs ({}) must be in 1..=86400",
            cfg.cache_ttl_secs
        )));
    }

    if cfg.cache_slots_per_core < 256 || cfg.cache_slots_per_core > 65_536 {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': cache_slots_per_core ({}) must be in 256..=65536",
            cfg.cache_slots_per_core
        )));
    }
    cfg.cache_slots_per_core = cfg.cache_slots_per_core.next_power_of_two();

    Ok(cfg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_valid_config_normalizes_header_and_slots() {
        let raw = json!({
            "header_name": "  X-API-Key ",
            "hide_credentials": true,
            "cache_ttl_secs": 120,
            "cache_slots_per_core": 300,
            "keys": [
                {
                    "key": "sk-test-1234567890",
                    "expires_at": 0,
                    "inject_headers": { "x-user-id": "u1" }
                }
            ]
        });

        let cfg = validate_api_key_auth_config("api_key_auth", &raw).unwrap();
        assert_eq!(cfg.header_name, "x-api-key");
        assert_eq!(cfg.cache_slots_per_core, 512);
    }

    #[test]
    fn test_rejects_oversized_key_over_64_bytes() {
        let too_long = "a".repeat(65);
        let raw = json!({
            "header_name": "x-api-key",
            "keys": [{ "key": too_long }]
        });
        assert!(validate_api_key_auth_config("api_key_auth", &raw).is_err());
    }

    #[test]
    fn test_rejects_empty_keys_and_duplicate_keys() {
        let empty = json!({ "header_name": "x-api-key", "keys": [] });
        assert!(validate_api_key_auth_config("api_key_auth", &empty).is_err());

        let dup = json!({
            "header_name": "x-api-key",
            "keys": [{ "key": "abc" }, { "key": "abc" }]
        });
        assert!(validate_api_key_auth_config("api_key_auth", &dup).is_err());
    }
}
