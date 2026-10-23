//! OAuth 2.0 Token Introspection Configuration Schema & Semantic Validation (`phase: "pre_upstream"`).
//!
//! Enforces a strict declarative contract for RFC 7662 OAuth 2.0 Token Introspection.
//! Allows edge verification of opaque reference tokens against an upstream Authorization Server (IdP)
//! with multi-source extraction, lock-striped RAM caching, and persistent HTTP keep-alive connections.
//!
//! # Strict Declarative Contract (Zero Silent Fallbacks)
//! - **Explicit Credentials**: Non-empty `client_id` and `client_secret`.
//! - **Valid Endpoint**: `introspection_url` must be a valid HTTP or HTTPS URI.
//! - **Strict Auth Methods**: Supported auth methods are strictly `"client_secret_basic"` and `"client_secret_post"`.
//! - **Strict Extraction Sources**: At least one extraction source (`token_headers`, `token_cookies`, or `token_query_params`) must be defined.
//! - **RFC 9110 Headers**: All header names in `token_headers` and `claims_to_headers` must conform to RFC 9110 token grammar.
//! - **Strict Timeouts & Limits**: All timeouts, pool sizes, and cache TTLs must be strictly positive within sane bounds.
//! - **Strict Fallback Behavior**: Must be strictly `"reject"` (Fail-Closed) or `"allow"` (Fail-Open).

use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::SyncError;

/// Mapping from an IdP introspection JSON claim key to an Upstream HTTP Header name.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimToHeaderConfig {
    /// Claim name in IdP response (e.g. "sub", "client_id", "scope", "username").
    pub claim: String,
    /// Upstream header name to inject (e.g. "x-user-id", "x-client-id").
    pub header: String,
}

/// Declarative configuration for the OAuth 2.0 Token Introspection plugin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Oauth2IntrospectionConfig {
    /// Full URL of Authorization Server Introspection Endpoint (e.g. "http://127.0.0.1:8080/oauth/introspect").
    pub introspection_url: String,

    /// Client ID of Gateway for authenticating with Authorization Server.
    pub client_id: String,

    /// Client Secret of Gateway for authenticating with Authorization Server.
    pub client_secret: String,

    /// Authentication method: strictly `"client_secret_basic"` or `"client_secret_post"`.
    pub auth_method: String,

    /// Ordered list of exact header names to search for token (e.g. `["x-access-token", "authorization"]`).
    pub token_headers: Vec<String>,

    /// Ordered list of cookie names to search for token (e.g. `["access_token"]` or `[]`).
    pub token_cookies: Vec<String>,

    /// Ordered list of query parameter names to search for token (e.g. `["token"]` or `[]`).
    pub token_query_params: Vec<String>,

    /// Token type hint per RFC 7662: `"access_token"`, `"refresh_token"`, or `"none"`.
    pub token_type_hint: String,

    /// Positive in-memory cache TTL for active tokens in seconds (e.g. 300).
    pub cache_ttl_seconds: u64,

    /// Negative in-memory cache TTL for inactive/invalid tokens in seconds to prevent DoS (e.g. 10).
    pub negative_cache_ttl_seconds: u64,

    /// Maximum number of cache entries stored in RAM (e.g. 10000).
    pub max_cache_entries: usize,

    /// Persistent HTTP Keep-Alive idle connection timeout in seconds (e.g. 60).
    pub keepalive_timeout_secs: u64,

    /// Maximum idle connections maintained in the persistent pool (e.g. 10).
    pub pool_max_idle_connections: usize,

    /// Request timeout for contacting Authorization Server in milliseconds (e.g. 500).
    pub request_timeout_ms: u64,

    /// List of scopes required for this route (e.g. `["read", "orders"]` or `[]`).
    pub required_scopes: Vec<String>,

    /// Claim to header forwarding rules.
    pub claims_to_headers: Vec<ClaimToHeaderConfig>,

    /// Whether to strip the matched token from downstream request before forwarding upstream.
    pub hide_credentials: bool,

    /// Fallback behavior when IdP is unreachable or times out: `"reject"` or `"allow"`.
    pub fallback_behavior: String,
}

/// Validates RFC 9110 token grammar for header names.
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

/// Validates OAuth 2.0 Token Introspection configuration strictly according to production standards.
pub fn validate_oauth2_introspection_config(
    plugin_id: &str,
    config_val: &serde_json::Value,
) -> Result<Oauth2IntrospectionConfig, SyncError> {
    let cfg: Oauth2IntrospectionConfig =
        serde_json::from_value(config_val.clone()).map_err(|e| SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}' has malformed or incomplete oauth2_introspection config: {e}"
            ),
        })?;

    // 1. Validate Introspection URL
    let url_str = cfg.introspection_url.trim();
    if url_str.is_empty() {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!("Plugin '{plugin_id}': introspection_url cannot be empty"),
        });
    }
    if !url_str.starts_with("http://") && !url_str.starts_with("https://") {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': introspection_url must start with 'http://' or 'https://' (got '{url_str}')"
            ),
        });
    }

    // 2. Validate Credentials
    if cfg.client_id.trim().is_empty() {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!("Plugin '{plugin_id}': client_id cannot be empty"),
        });
    }
    if cfg.client_secret.trim().is_empty() {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!("Plugin '{plugin_id}': client_secret cannot be empty"),
        });
    }

    // 3. Validate Auth Method
    let method = cfg.auth_method.trim().to_ascii_lowercase();
    if method != "client_secret_basic" && method != "client_secret_post" {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': auth_method '{}' is invalid; strictly 'client_secret_basic' or 'client_secret_post' are supported",
                cfg.auth_method
            ),
        });
    }

    // 4. Validate Token Sources (At least one non-empty source required)
    if cfg.token_headers.is_empty()
        && cfg.token_cookies.is_empty()
        && cfg.token_query_params.is_empty()
    {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': at least one token extraction source (token_headers, token_cookies, or token_query_params) must be defined"
            ),
        });
    }

    for (idx, hname) in cfg.token_headers.iter().enumerate() {
        if !is_valid_header_name(hname) {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': token_headers[{idx}] contains invalid RFC 9110 header name '{hname}'"
                ),
            });
        }
    }

    for (idx, cname) in cfg.token_cookies.iter().enumerate() {
        if cname.trim().is_empty() {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': token_cookies[{idx}] cannot be empty or whitespace"
                ),
            });
        }
    }

    for (idx, qname) in cfg.token_query_params.iter().enumerate() {
        if qname.trim().is_empty() {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': token_query_params[{idx}] cannot be empty or whitespace"
                ),
            });
        }
    }

    // 5. Validate Token Type Hint
    let hint = cfg.token_type_hint.trim().to_ascii_lowercase();
    if hint != "access_token" && hint != "refresh_token" && hint != "none" {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': token_type_hint '{}' is invalid; strictly 'access_token', 'refresh_token', or 'none' are supported",
                cfg.token_type_hint
            ),
        });
    }

    // 6. Validate Cache TTLs and Capacities
    if cfg.cache_ttl_seconds == 0 || cfg.cache_ttl_seconds > 86400 {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': cache_ttl_seconds must be between 1 and 86400 (got {})",
                cfg.cache_ttl_seconds
            ),
        });
    }

    if cfg.negative_cache_ttl_seconds == 0 || cfg.negative_cache_ttl_seconds > 86400 {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': negative_cache_ttl_seconds must be between 1 and 86400 (got {})",
                cfg.negative_cache_ttl_seconds
            ),
        });
    }

    if cfg.max_cache_entries == 0 || cfg.max_cache_entries > 1_000_000 {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': max_cache_entries must be between 1 and 1000000 (got {})",
                cfg.max_cache_entries
            ),
        });
    }

    // 7. Validate Pool & Request Timeouts
    if cfg.keepalive_timeout_secs == 0 || cfg.keepalive_timeout_secs > 86400 {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': keepalive_timeout_secs must be between 1 and 86400 (got {})",
                cfg.keepalive_timeout_secs
            ),
        });
    }

    if cfg.pool_max_idle_connections == 0 || cfg.pool_max_idle_connections > 1000 {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': pool_max_idle_connections must be between 1 and 1000 (got {})",
                cfg.pool_max_idle_connections
            ),
        });
    }

    if cfg.request_timeout_ms == 0 || cfg.request_timeout_ms > 60_000 {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': request_timeout_ms must be between 1 and 60000 (got {})",
                cfg.request_timeout_ms
            ),
        });
    }

    // 8. Validate Required Scopes
    for (idx, scope) in cfg.required_scopes.iter().enumerate() {
        let trimmed = scope.trim();
        if trimmed.is_empty() {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': required_scopes[{idx}] cannot be empty or whitespace"
                ),
            });
        }
        if trimmed.contains(' ') {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': required_scopes[{idx}] ('{trimmed}') contains whitespace; individual scopes must be single tokens per RFC 6749"
                ),
            });
        }
    }

    // 9. Validate Claims to Headers Mapping
    let mut target_headers = HashSet::with_capacity(cfg.claims_to_headers.len());
    for (idx, mapping) in cfg.claims_to_headers.iter().enumerate() {
        let claim = mapping.claim.trim();
        if claim.is_empty() {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': claims_to_headers[{idx}].claim cannot be empty"
                ),
            });
        }
        let hdr = mapping.header.trim();
        if !is_valid_header_name(hdr) {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': claims_to_headers[{idx}].header ('{hdr}') is not a valid RFC 9110 header name"
                ),
            });
        }
        let hdr_lower = hdr.to_ascii_lowercase();
        if !target_headers.insert(hdr_lower) {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': duplicate target header '{hdr}' in claims_to_headers"
                ),
            });
        }
    }

    // 10. Validate Fallback Behavior
    let fallback = cfg.fallback_behavior.trim().to_ascii_lowercase();
    if fallback != "reject" && fallback != "allow" {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': fallback_behavior '{}' is invalid; strictly 'reject' or 'allow' are supported",
                cfg.fallback_behavior
            ),
        });
    }

    Ok(cfg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn valid_config_json() -> serde_json::Value {
        json!({
            "introspection_url": "http://127.0.0.1:8080/oauth/introspect",
            "client_id": "velda-edge-gateway",
            "client_secret": "sec_gateway_secret_12345",
            "auth_method": "client_secret_basic",
            "token_headers": ["x-access-token", "authorization"],
            "token_cookies": ["access_token"],
            "token_query_params": ["token"],
            "token_type_hint": "access_token",
            "cache_ttl_seconds": 300,
            "negative_cache_ttl_seconds": 10,
            "max_cache_entries": 10000,
            "keepalive_timeout_secs": 60,
            "pool_max_idle_connections": 10,
            "request_timeout_ms": 500,
            "required_scopes": ["read", "orders"],
            "claims_to_headers": [
                { "claim": "sub", "header": "x-user-id" },
                { "claim": "client_id", "header": "x-client-id" },
                { "claim": "scope", "header": "x-authenticated-scope" }
            ],
            "hide_credentials": true,
            "fallback_behavior": "reject"
        })
    }

    #[test]
    fn test_valid_oauth2_introspection_config() {
        let val = valid_config_json();
        let cfg = validate_oauth2_introspection_config("oauth2-1", &val).unwrap();
        assert_eq!(
            cfg.introspection_url,
            "http://127.0.0.1:8080/oauth/introspect"
        );
        assert_eq!(cfg.auth_method, "client_secret_basic");
        assert_eq!(cfg.token_headers.len(), 2);
        assert_eq!(cfg.required_scopes.len(), 2);
        assert_eq!(cfg.claims_to_headers.len(), 3);
        assert!(cfg.hide_credentials);
    }

    #[test]
    fn test_invalid_url_rejected() {
        let mut val = valid_config_json();
        val["introspection_url"] = json!("ftp://auth.server/introspect");
        let err = validate_oauth2_introspection_config("oauth2-1", &val).unwrap_err();
        assert!(err.to_string().contains("must start with 'http://'"));
    }

    #[test]
    fn test_invalid_auth_method_rejected() {
        let mut val = valid_config_json();
        val["auth_method"] = json!("bearer_token");
        let err = validate_oauth2_introspection_config("oauth2-1", &val).unwrap_err();
        assert!(
            err.to_string()
                .contains("auth_method 'bearer_token' is invalid")
        );
    }

    #[test]
    fn test_empty_sources_rejected() {
        let mut val = valid_config_json();
        val["token_headers"] = json!([]);
        val["token_cookies"] = json!([]);
        val["token_query_params"] = json!([]);
        let err = validate_oauth2_introspection_config("oauth2-1", &val).unwrap_err();
        assert!(
            err.to_string()
                .contains("at least one token extraction source")
        );
    }

    #[test]
    fn test_invalid_scope_with_space_rejected() {
        let mut val = valid_config_json();
        val["required_scopes"] = json!(["read write"]);
        let err = validate_oauth2_introspection_config("oauth2-1", &val).unwrap_err();
        assert!(err.to_string().contains("contains whitespace"));
    }

    #[test]
    fn test_duplicate_target_header_rejected() {
        let mut val = valid_config_json();
        val["claims_to_headers"] = json!([
            { "claim": "sub", "header": "x-user-id" },
            { "claim": "client_id", "header": "X-User-Id" }
        ]);
        let err = validate_oauth2_introspection_config("oauth2-1", &val).unwrap_err();
        assert!(err.to_string().contains("duplicate target header"));
    }
}
