//! API Key Authentication Configuration Schema & Semantic Validation (`phase: "pre_upstream"`).
//!
//! Enforces a strict declarative contract for API Key authentication and token extraction.
//!
//! # Strict Declarative Contract (Zero Silent Fallbacks)
//! - **Non-Empty Key Store**: `keys` must contain at least one configured key-consumer mapping.
//! - **Valid Key Token**: Each key entry must have a non-empty `key` string.
//! - **Valid Key Sources**: Every source in `sources` must have type `"header"` or `"query"` with valid names.
//! - **Header Name RFC Grammar**: Header names and injected header names must conform to RFC 9110 token grammar.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::SyncError;

/// Declarative extraction source definition for API keys.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ApiKeySourceConfig {
    /// Extract API key from an HTTP request header (e.g. `X-API-Key: <token>`).
    #[serde(rename = "header")]
    Header {
        /// Header field name (e.g. "x-api-key").
        name: String,
        /// Optional prefix to strip (e.g. "ApiKey " or "Bearer ").
        prefix: Option<String>,
    },
    /// Extract API key from a URL query parameter (e.g. `?api_key=<token>`).
    #[serde(rename = "query")]
    Query {
        /// Query parameter name (e.g. "api_key").
        param: String,
    },
}

/// Declarative key entry mapping a secret key to a consumer identity and optional headers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiKeyEntryConfig {
    /// Secret API key token.
    pub key: String,
    /// Consumer identifier associated with this key.
    pub consumer_id: Option<String>,
    /// Headers injected into upstream request upon successful authentication.
    pub inject_headers: HashMap<String, String>,
}

/// Declarative configuration for the API Key Authentication plugin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiKeyAuthConfig {
    /// Extraction sources (e.g. header and/or query).
    pub sources: Vec<ApiKeySourceConfig>,
    /// Configured valid keys and their consumer identities.
    pub keys: Vec<ApiKeyEntryConfig>,
    /// Whether to strip the API key from request headers before forwarding upstream.
    pub hide_credentials: bool,
    /// Dual-cache TTL in seconds for positive/negative validation caching.
    pub cache_ttl_secs: u64,
}

/// Helper to validate RFC 9110 header token grammar.
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

/// Validates API Key Authentication configuration strictly according to production standards.
pub fn validate_api_key_auth_config(
    plugin_id: &str,
    config_val: &serde_json::Value,
) -> Result<ApiKeyAuthConfig, SyncError> {
    let cfg: ApiKeyAuthConfig =
        serde_json::from_value(config_val.clone()).map_err(|e| SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}' has malformed or incomplete api_key_auth config: {e}"
            ),
        })?;

    // 1. Validate Keys Store
    if cfg.keys.is_empty() {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': keys list cannot be empty (at least one key is required)"
            ),
        });
    }

    let mut seen_keys = std::collections::HashSet::with_capacity(cfg.keys.len());
    for (idx, entry) in cfg.keys.iter().enumerate() {
        let key_trimmed = entry.key.trim();
        if key_trimmed.is_empty() {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': key entry at index {idx} has empty 'key' field"
                ),
            });
        }
        if !seen_keys.insert(key_trimmed) {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': duplicate key entry detected for key at index {idx}"
                ),
            });
        }

        // Validate injected headers
        for (hname, hval) in &entry.inject_headers {
            if !is_valid_header_name(hname) {
                return Err(SyncError::Validation {
                    domain: "plugins".into(),
                    reason: format!(
                        "Plugin '{plugin_id}': key at index {idx} has invalid injected header name '{hname}'"
                    ),
                });
            }
            if hval.bytes().any(|b| b == b'\r' || b == b'\n') {
                return Err(SyncError::Validation {
                    domain: "plugins".into(),
                    reason: format!(
                        "Plugin '{plugin_id}': injected header value for '{hname}' contains CRLF control characters"
                    ),
                });
            }
        }
    }

    // 2. Validate Sources
    for (idx, src) in cfg.sources.iter().enumerate() {
        match src {
            ApiKeySourceConfig::Header { name, .. } => {
                if !is_valid_header_name(name) {
                    return Err(SyncError::Validation {
                        domain: "plugins".into(),
                        reason: format!(
                            "Plugin '{plugin_id}': source at index {idx} has invalid header name '{name}'"
                        ),
                    });
                }
            }
            ApiKeySourceConfig::Query { param } => {
                if param.trim().is_empty() {
                    return Err(SyncError::Validation {
                        domain: "plugins".into(),
                        reason: format!(
                            "Plugin '{plugin_id}': source at index {idx} has empty query parameter name"
                        ),
                    });
                }
            }
        }
    }

    // 3. Validate Cache TTL (max 86400s)
    if cfg.cache_ttl_secs > 86400 {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': cache_ttl_secs ({}) exceeds maximum allowable 86,400s (24 hours)",
                cfg.cache_ttl_secs
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
    fn test_valid_api_key_auth_config() {
        let val = json!({
            "keys": [
                {
                    "key": "secret-key-12345",
                    "consumer_id": "payment-service",
                    "inject_headers": {
                        "X-Consumer-ID": "payment-service",
                        "X-Consumer-Tier": "premium"
                    }
                }
            ],
            "sources": [
                { "type": "header", "name": "x-api-key" },
                { "type": "query", "param": "key" }
            ],
            "hide_credentials": true,
            "cache_ttl_secs": 600
        });
        let res = validate_api_key_auth_config("api_key_1", &val);
        assert!(res.is_ok());
    }

    #[test]
    fn test_empty_keys_rejected() {
        let val = json!({
            "sources": [{ "type": "header", "name": "x-api-key" }],
            "keys": [],
            "hide_credentials": true,
            "cache_ttl_secs": 300
        });
        let res = validate_api_key_auth_config("api_key_empty", &val);
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("cannot be empty"));
    }

    #[test]
    fn test_duplicate_keys_rejected() {
        let val = json!({
            "sources": [{ "type": "header", "name": "x-api-key" }],
            "keys": [
                { "key": "duplicate-key-token", "consumer_id": "user1", "inject_headers": {} },
                { "key": "duplicate-key-token", "consumer_id": "user2", "inject_headers": {} }
            ],
            "hide_credentials": true,
            "cache_ttl_secs": 300
        });
        let res = validate_api_key_auth_config("api_key_dup", &val);
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("duplicate key"));
    }

    #[test]
    fn test_invalid_header_name_rejected() {
        let val = json!({
            "sources": [{ "type": "header", "name": "x-api-key" }],
            "keys": [
                {
                    "key": "valid-key-xyz",
                    "consumer_id": "user1",
                    "inject_headers": {
                        "Invalid Header Name: Bad": "val"
                    }
                }
            ],
            "hide_credentials": true,
            "cache_ttl_secs": 300
        });
        let res = validate_api_key_auth_config("api_key_bad_hdr", &val);
        assert!(res.is_err());
        assert!(
            res.unwrap_err()
                .to_string()
                .contains("invalid injected header name")
        );
    }

    #[test]
    fn test_missing_required_fields_rejected() {
        let val = json!({
            "keys": [
                { "key": "valid-key-xyz", "consumer_id": "user1", "inject_headers": {} }
            ]
            // Missing sources, hide_credentials, cache_ttl_secs
        });
        let res = validate_api_key_auth_config("api_key_missing", &val);
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("missing field"));
    }
}
