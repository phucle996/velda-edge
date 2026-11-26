//! OAuth 2.0 Token Introspection Configuration Schema & Semantic Validation (`oauth2_introspection::validation`).
//!
//! ```text
//!                     OAUTH2 INTROSPECTION SEMANTIC VALIDATION
//!
//!     PluginConfig (JSON, deny_unknown_fields)
//!            │
//!            ▼
//!     validate_oauth2_introspection_config()
//!            ├──► 1. ETP Pool Reference (`etp_id`, optional relative `path` override)
//!            ├──► 2. Client Auth (`client_secret_basic` | `client_secret_post` | `none`)
//!            ├──► 3. Token Sources (`token_headers` / `token_cookies` / `token_query_params`)
//!            ├──► 4. Token Type Hint (`access_token` | `refresh_token` | `none`)
//!            ├──► 5. TTL & Timeout Bounds (`cache_ttl_seconds: 0..=86400`, `request_timeout_ms: 1..=60000`)
//!            └──► 6. Scopes, Claims Projection & Fallback (`reject` | `allow`)
//! ```

use std::collections::HashSet;
use std::str::FromStr;

use http::header::HeaderName;
use serde::{Deserialize, Serialize};

use crate::error::PluginError;

/// Maximum allowable TTL in seconds (24 hours).
pub const MAX_TTL_SECS: u64 = 86_400;

fn default_auth_method() -> String {
    "client_secret_basic".to_string()
}

/// Mapping from an IdP introspection JSON claim path to an upstream HTTP header name.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClaimToHeaderConfig {
    /// Claim key or dot-separated path in IdP JSON response (e.g. `"sub"`, `"client_id"`, `"profile.tenant"`).
    pub claim: String,
    /// Upstream header name to inject when present (e.g. `"x-user-id"`).
    pub header: String,
}

/// Declarative configuration for the OAuth 2.0 Token Introspection plugin.
///
/// Transport connection pooling, TLS/mTLS, and keep-alive mechanics are delegated cleanly
/// to the referenced External Third-Party (`velda-etp`) pool (`etp_id`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Oauth2IntrospectionConfig {
    /// Reference identifier of the ETP pool entry (`velda-etp`) providing IdP/Vault connectivity.
    pub etp_id: String,

    /// Optional URL path override on the attached ETP endpoint (defaults to provider's endpoint path).
    #[serde(default)]
    pub path: Option<String>,

    /// Optional OAuth 2.0 Client ID (overrides ETP-level `client_id` when provided).
    #[serde(default)]
    pub client_id: Option<String>,

    /// Optional OAuth 2.0 Client Secret (overrides ETP-level `client_secret` when provided).
    #[serde(default)]
    pub client_secret: Option<String>,

    /// Authentication method: `"client_secret_basic"`, `"client_secret_post"`, or `"none"`.
    #[serde(default = "default_auth_method")]
    pub auth_method: String,

    /// Ordered list of header names to inspect for incoming tokens.
    pub token_headers: Vec<String>,

    /// Ordered list of cookie names to inspect for incoming tokens.
    pub token_cookies: Vec<String>,

    /// Ordered list of query parameter names to inspect for incoming tokens.
    pub token_query_params: Vec<String>,

    /// Token type hint per RFC 7662: `"access_token"`, `"refresh_token"`, or `"none"`.
    pub token_type_hint: String,

    /// Positive in-memory cache TTL in seconds (`0..=86400`; `0` disables positive cache for instant revocation).
    pub cache_ttl_seconds: u64,

    /// Negative in-memory cache TTL in seconds (`0..=86400`; `0` disables negative cache).
    pub negative_cache_ttl_seconds: u64,

    /// Request timeout for contacting the attached ETP provider in milliseconds (`1..=60000`).
    pub request_timeout_ms: u64,

    /// List of scopes required for this route (`[]` if none).
    pub required_scopes: Vec<String>,

    /// Claim-to-header forwarding rules (`insert_prehashed` when claim exists in IdP response).
    pub claims_to_headers: Vec<ClaimToHeaderConfig>,

    /// Whether to strip the matched token header before forwarding upstream.
    pub hide_credentials: bool,

    /// Fallback behavior when IdP is unreachable or times out: `"reject"` or `"allow"`.
    pub fallback_behavior: String,
}

#[inline]
fn is_valid_header_name(h: &str) -> bool {
    let trimmed = h.trim();
    !trimmed.is_empty() && HeaderName::from_str(trimmed).is_ok()
}

#[inline]
fn is_valid_claim_path(path: &str) -> bool {
    let trimmed = path.trim();
    !trimmed.is_empty() && trimmed.split('.').all(|seg| !seg.trim().is_empty())
}

/// Validates and normalizes `oauth2_introspection` configuration strictly at sync/boot time.
pub fn validate_oauth2_introspection_config(
    plugin_id: &str,
    config_val: &serde_json::Value,
) -> Result<Oauth2IntrospectionConfig, PluginError> {
    let mut cfg: Oauth2IntrospectionConfig =
        serde_json::from_value(config_val.clone()).map_err(|e| {
            PluginError::Config(format!(
                "Plugin '{plugin_id}' has malformed or incomplete oauth2_introspection config: {e}"
            ))
        })?;

    // 1. Validate ETP Pool Reference & Optional Path Override
    cfg.etp_id = cfg.etp_id.trim().to_string();
    if cfg.etp_id.is_empty() {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': etp_id cannot be empty"
        )));
    }
    if let Some(ref mut path) = cfg.path {
        let trimmed = path.trim();
        if trimmed.is_empty() {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': path cannot be empty when specified"
            )));
        }
        if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': path must be a relative endpoint path, not a full URL ('{trimmed}')"
            )));
        }
        if trimmed.chars().any(|c| c.is_whitespace() || c.is_control()) {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': path '{trimmed}' contains invalid whitespace or control characters"
            )));
        }
        *path = trimmed.to_string();
    }

    // 2. Validate Optional Client Credentials Override & Auth Method
    if let Some(ref mut cid) = cfg.client_id {
        let trimmed = cid.trim();
        if trimmed.is_empty() {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': client_id cannot be empty when specified"
            )));
        }
        *cid = trimmed.to_string();
    }
    if let Some(ref csec) = cfg.client_secret
        && csec.trim().is_empty()
    {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': client_secret cannot be empty when specified"
        )));
    }

    let method = cfg.auth_method.trim().to_ascii_lowercase();
    if method != "client_secret_basic" && method != "client_secret_post" && method != "none" {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': auth_method '{}' is invalid; strictly 'client_secret_basic', 'client_secret_post', or 'none' are supported",
            cfg.auth_method
        )));
    }
    cfg.auth_method = method;

    // 3. Validate Token Sources
    if cfg.token_headers.is_empty()
        && cfg.token_cookies.is_empty()
        && cfg.token_query_params.is_empty()
    {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': at least one token extraction source (token_headers, token_cookies, or token_query_params) must be defined"
        )));
    }

    for (idx, hname) in cfg.token_headers.iter_mut().enumerate() {
        let trimmed = hname.trim();
        if !is_valid_header_name(trimmed) {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': token_headers[{idx}] contains invalid RFC 9110 header name '{hname}'"
            )));
        }
        *hname = trimmed.to_ascii_lowercase();
    }

    for (idx, cname) in cfg.token_cookies.iter_mut().enumerate() {
        let trimmed = cname.trim();
        if trimmed.is_empty() {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': token_cookies[{idx}] cannot be empty or whitespace"
            )));
        }
        *cname = trimmed.to_string();
    }

    for (idx, qname) in cfg.token_query_params.iter_mut().enumerate() {
        let trimmed = qname.trim();
        if trimmed.is_empty() {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': token_query_params[{idx}] cannot be empty or whitespace"
            )));
        }
        *qname = trimmed.to_string();
    }

    // 4. Validate & normalize Token Type Hint
    let hint = cfg.token_type_hint.trim().to_ascii_lowercase();
    if hint != "access_token" && hint != "refresh_token" && hint != "none" {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': token_type_hint '{}' is invalid; strictly 'access_token', 'refresh_token', or 'none' are supported",
            cfg.token_type_hint
        )));
    }
    cfg.token_type_hint = hint;

    // 5. Validate Cache TTLs & Timeout
    if cfg.cache_ttl_seconds > MAX_TTL_SECS {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': cache_ttl_seconds must be between 0 and {MAX_TTL_SECS} (got {})",
            cfg.cache_ttl_seconds
        )));
    }

    if cfg.negative_cache_ttl_seconds > MAX_TTL_SECS {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': negative_cache_ttl_seconds must be between 0 and {MAX_TTL_SECS} (got {})",
            cfg.negative_cache_ttl_seconds
        )));
    }

    if cfg.request_timeout_ms == 0 || cfg.request_timeout_ms > 60_000 {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': request_timeout_ms must be between 1 and 60000 (got {})",
            cfg.request_timeout_ms
        )));
    }

    // 6. Validate Required Scopes
    for (idx, scope) in cfg.required_scopes.iter_mut().enumerate() {
        let trimmed = scope.trim();
        if trimmed.is_empty() {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': required_scopes[{idx}] cannot be empty or whitespace"
            )));
        }
        if trimmed.contains(' ') {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': required_scopes[{idx}] ('{trimmed}') contains whitespace; individual scopes must be single tokens per RFC 6749"
            )));
        }
        *scope = trimmed.to_string();
    }

    // 7. Validate Claims to Headers Mapping
    let mut target_headers = HashSet::with_capacity(cfg.claims_to_headers.len());
    for (idx, mapping) in cfg.claims_to_headers.iter_mut().enumerate() {
        let claim = mapping.claim.trim();
        if !is_valid_claim_path(claim) {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': claims_to_headers[{idx}].claim ('{}') is invalid",
                mapping.claim
            )));
        }
        mapping.claim = claim.to_string();

        let hdr = mapping.header.trim();
        if !is_valid_header_name(hdr) {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': claims_to_headers[{idx}].header ('{hdr}') is not a valid RFC 9110 header name"
            )));
        }
        let hdr_lower = hdr.to_ascii_lowercase();
        if !target_headers.insert(hdr_lower.clone()) {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': duplicate target header '{hdr}' in claims_to_headers"
            )));
        }
        mapping.header = hdr_lower;
    }

    // 8. Validate & normalize Fallback Behavior
    let fallback = cfg.fallback_behavior.trim().to_ascii_lowercase();
    if fallback != "reject" && fallback != "allow" {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': fallback_behavior '{}' is invalid; strictly 'reject' or 'allow' are supported",
            cfg.fallback_behavior
        )));
    }
    cfg.fallback_behavior = fallback;

    Ok(cfg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn valid_config_json() -> serde_json::Value {
        json!({
            "etp_id": "kc-prod",
            "path": "/realms/app/protocol/openid-connect/token/introspect",
            "client_id": "velda-edge-gateway",
            "client_secret": "sec_gateway_secret_12345",
            "auth_method": "client_secret_basic",
            "token_headers": ["X-Access-Token", "authorization"],
            "token_cookies": ["access_token"],
            "token_query_params": ["token"],
            "token_type_hint": "access_token",
            "cache_ttl_seconds": 300,
            "negative_cache_ttl_seconds": 10,
            "request_timeout_ms": 500,
            "required_scopes": ["read", "orders"],
            "claims_to_headers": [
                { "claim": "sub", "header": "X-User-ID" },
                { "claim": "client_id", "header": "x-client-id" },
                { "claim": "scope", "header": "x-authenticated-scope" }
            ],
            "hide_credentials": true,
            "fallback_behavior": "reject"
        })
    }

    #[test]
    fn test_valid_oauth2_introspection_config_and_zero_ttl() {
        let mut val = valid_config_json();
        let cfg = validate_oauth2_introspection_config("oauth2-1", &val).unwrap();
        assert_eq!(cfg.etp_id, "kc-prod");
        assert_eq!(
            cfg.path.as_deref(),
            Some("/realms/app/protocol/openid-connect/token/introspect")
        );
        assert_eq!(cfg.token_headers[0], "x-access-token");
        assert_eq!(cfg.claims_to_headers[0].header, "x-user-id");

        // Zero TTL allowed for instant revocation; optional fields omitted with auth_method=none
        val["cache_ttl_seconds"] = json!(0);
        val["negative_cache_ttl_seconds"] = json!(0);
        val["auth_method"] = json!("none");
        val.as_object_mut().unwrap().remove("path");
        val.as_object_mut().unwrap().remove("client_id");
        val.as_object_mut().unwrap().remove("client_secret");
        assert!(validate_oauth2_introspection_config("oauth2-1", &val).is_ok());
    }

    #[test]
    fn test_unknown_and_legacy_pool_fields_rejected() {
        let mut val = valid_config_json();
        val["introspection_url"] = json!("https://kc.local/introspect");
        let err = validate_oauth2_introspection_config("oauth2-1", &val).unwrap_err();
        assert!(err.to_string().contains("unknown field"));

        let mut val2 = valid_config_json();
        val2["keepalive_timeout_secs"] = json!(60);
        assert!(validate_oauth2_introspection_config("oauth2-1", &val2).is_err());

        let mut val3 = valid_config_json();
        val3["pool_max_idle_connections"] = json!(10);
        assert!(validate_oauth2_introspection_config("oauth2-1", &val3).is_err());
    }

    #[test]
    fn test_invalid_etp_id_path_and_auth_method_rejected() {
        let mut val = valid_config_json();
        val["etp_id"] = json!("   ");
        assert!(
            validate_oauth2_introspection_config("oauth2-1", &val)
                .unwrap_err()
                .to_string()
                .contains("etp_id cannot be empty")
        );

        let mut val_path = valid_config_json();
        val_path["path"] = json!("https://kc.local/introspect");
        assert!(
            validate_oauth2_introspection_config("oauth2-1", &val_path)
                .unwrap_err()
                .to_string()
                .contains("relative endpoint path")
        );

        let mut val_auth = valid_config_json();
        val_auth["auth_method"] = json!("bearer_token");
        let err2 = validate_oauth2_introspection_config("oauth2-1", &val_auth).unwrap_err();
        assert!(
            err2.to_string()
                .contains("auth_method 'bearer_token' is invalid")
        );
    }

    #[test]
    fn test_empty_sources_and_duplicate_target_headers_rejected() {
        let mut val = valid_config_json();
        val["token_headers"] = json!([]);
        val["token_cookies"] = json!([]);
        val["token_query_params"] = json!([]);
        assert!(validate_oauth2_introspection_config("oauth2-1", &val).is_err());

        let mut val2 = valid_config_json();
        val2["claims_to_headers"] = json!([
            { "claim": "sub", "header": "x-user-id" },
            { "claim": "client_id", "header": "X-User-Id" }
        ]);
        assert!(
            validate_oauth2_introspection_config("oauth2-1", &val2)
                .unwrap_err()
                .to_string()
                .contains("duplicate target header")
        );
    }
}
