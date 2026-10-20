//! JWT Authentication Configuration Schema & Semantic Validation (`phase: "pre_upstream"`).
//!
//! Enforces a strict declarative contract for JWT token extraction, cryptographic verification,
//! and claims-to-headers mapping.
//!
//! # Strict Declarative Contract (Zero Silent Fallbacks)
//! - **Supported Algorithm**: Must be a standard supported JWT signature algorithm.
//! - **Cryptographic Key Invariant**:
//!   - For symmetric algorithms (`HS256`, `HS384`, `HS512`): `secret_key` is strictly mandatory and must not be empty.
//!   - For asymmetric algorithms (`RS256`, `RS384`, `RS512`, `ES256`, `ES384`, `EdDSA`): `public_key_pem` is strictly mandatory.
//! - **Header Grammar**: Injected claim header names and token source header names must conform to RFC 9110.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::SyncError;

/// Declarative extraction source definition for JWT tokens.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum JwtTokenSourceConfig {
    /// Extract token from an HTTP request header (e.g. `Authorization: Bearer <token>`).
    #[serde(rename = "header")]
    Header {
        /// Header field name (e.g. "authorization").
        name: String,
        /// Optional prefix to strip (e.g. "Bearer ").
        prefix: Option<String>,
    },
    /// Extract token from a Cookie header (e.g. `Cookie: access_token=<token>`).
    #[serde(rename = "cookie")]
    Cookie {
        /// Cookie name (e.g. "access_token").
        name: String,
    },
    /// Extract token from a URL query parameter (e.g. `?token=<token>`).
    #[serde(rename = "query")]
    Query {
        /// Query parameter name (e.g. "token").
        param: String,
    },
}

/// Declarative configuration for the JWT Authentication plugin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JwtAuthConfig {
    /// Cryptographic signing algorithm: HS256, HS384, HS512, RS256, RS384, RS512, ES256, ES384, EdDSA.
    pub algorithm: String,
    /// Shared secret key for HMAC algorithms (HS256/HS384/HS512).
    pub secret_key: Option<String>,
    /// PEM-encoded public key for asymmetric algorithms (RS*/ES*/EdDSA).
    pub public_key_pem: Option<String>,
    /// Locations from which to extract the JWT token.
    pub token_sources: Vec<JwtTokenSourceConfig>,
    /// Whether to enforce expiration (`exp`) claim validation.
    pub validate_exp: bool,
    /// Maximum clock skew allowed in seconds when evaluating `exp` and `nbf` claims.
    pub clock_skew_secs: u64,
    /// Expected issuer (`iss`) claim to verify against.
    pub issuer: Option<String>,
    /// Expected audience (`aud`) claim to verify against.
    pub audience: Option<String>,
    /// Mapping of extracted JWT claim paths to upstream injected request headers.
    pub claims_to_headers: HashMap<String, String>,
    /// Strict assertions requiring specific claim keys to match exact literal values.
    pub assert_claims: HashMap<String, String>,
    /// Cache TTL in seconds for positive/negative validation caching.
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

/// Validates JWT Authentication configuration strictly according to production standards.
pub fn validate_jwt_auth_config(
    plugin_id: &str,
    config_val: &serde_json::Value,
) -> Result<JwtAuthConfig, SyncError> {
    let cfg: JwtAuthConfig =
        serde_json::from_value(config_val.clone()).map_err(|e| SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}' has malformed or incomplete jwt_auth config: {e}"
            ),
        })?;

    // 1. Validate Algorithm
    let alg_upper = cfg.algorithm.trim().to_ascii_uppercase();
    let is_symmetric = matches!(alg_upper.as_str(), "HS256" | "HS384" | "HS512");
    let is_asymmetric = matches!(
        alg_upper.as_str(),
        "RS256" | "RS384" | "RS512" | "ES256" | "ES384" | "EDDSA"
    );

    if !is_symmetric && !is_asymmetric {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': unsupported JWT algorithm '{}' (supported: HS256, HS384, HS512, RS256, RS384, RS512, ES256, ES384, EdDSA)",
                cfg.algorithm
            ),
        });
    }

    // 2. Cryptographic Key Invariant
    if is_symmetric {
        let has_secret = cfg
            .secret_key
            .as_ref()
            .map(|s| s.trim())
            .is_some_and(|s| !s.is_empty());
        if !has_secret {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': algorithm '{alg_upper}' requires a non-empty 'secret_key'"
                ),
            });
        }
    } else {
        let has_pem = cfg
            .public_key_pem
            .as_ref()
            .map(|s| s.trim())
            .is_some_and(|s| s.contains("BEGIN") && s.contains("KEY"));
        if !has_pem {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': algorithm '{alg_upper}' requires a valid PEM-encoded 'public_key_pem'"
                ),
            });
        }
    }

    // 3. Validate Token Sources
    if cfg.token_sources.is_empty() {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!("Plugin '{plugin_id}': token_sources list cannot be empty"),
        });
    }

    for (idx, src) in cfg.token_sources.iter().enumerate() {
        match src {
            JwtTokenSourceConfig::Header { name, .. } => {
                if !is_valid_header_name(name) {
                    return Err(SyncError::Validation {
                        domain: "plugins".into(),
                        reason: format!(
                            "Plugin '{plugin_id}': token source at index {idx} has invalid header name '{name}'"
                        ),
                    });
                }
            }
            JwtTokenSourceConfig::Cookie { name } => {
                if name.trim().is_empty() {
                    return Err(SyncError::Validation {
                        domain: "plugins".into(),
                        reason: format!(
                            "Plugin '{plugin_id}': token source at index {idx} has empty cookie name"
                        ),
                    });
                }
            }
            JwtTokenSourceConfig::Query { param } => {
                if param.trim().is_empty() {
                    return Err(SyncError::Validation {
                        domain: "plugins".into(),
                        reason: format!(
                            "Plugin '{plugin_id}': token source at index {idx} has empty query parameter name"
                        ),
                    });
                }
            }
        }
    }

    // 4. Validate Claims to Headers Mapping
    for (claim, header) in &cfg.claims_to_headers {
        if claim.trim().is_empty() {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': claim path in claims_to_headers cannot be empty"
                ),
            });
        }
        if !is_valid_header_name(header) {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': target header name '{header}' in claims_to_headers is invalid"
                ),
            });
        }
    }

    // 5. Validate Clock Skew
    if cfg.clock_skew_secs > 3600 {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': clock_skew_secs ({}) exceeds maximum allowable 3,600s (1 hour)",
                cfg.clock_skew_secs
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
    fn test_valid_hs256_config() {
        let val = json!({
            "algorithm": "HS256",
            "secret_key": "my-ultra-secure-hmac-secret-string",
            "token_sources": [{ "type": "header", "name": "authorization", "prefix": "Bearer " }],
            "validate_exp": true,
            "clock_skew_secs": 60,
            "issuer": "https://auth.company.com",
            "claims_to_headers": {
                "sub": "X-User-ID",
                "email": "X-User-Email"
            },
            "assert_claims": {},
            "cache_ttl_secs": 300
        });
        let res = validate_jwt_auth_config("jwt_1", &val);
        assert!(res.is_ok());
    }

    #[test]
    fn test_valid_rs256_config() {
        let val = json!({
            "algorithm": "RS256",
            "public_key_pem": "-----BEGIN PUBLIC KEY-----\nMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA...\n-----END PUBLIC KEY-----",
            "token_sources": [{ "type": "cookie", "name": "jwt_token" }],
            "validate_exp": true,
            "clock_skew_secs": 30,
            "claims_to_headers": {},
            "assert_claims": {},
            "cache_ttl_secs": 600
        });
        let res = validate_jwt_auth_config("jwt_2", &val);
        assert!(res.is_ok());
    }

    #[test]
    fn test_hs256_missing_secret_rejected() {
        let val = json!({
            "algorithm": "HS256",
            "token_sources": [{ "type": "header", "name": "authorization" }],
            "validate_exp": true,
            "clock_skew_secs": 60,
            "claims_to_headers": {},
            "assert_claims": {},
            "cache_ttl_secs": 300
        });
        let res = validate_jwt_auth_config("jwt_no_secret", &val);
        assert!(res.is_err());
        assert!(
            res.unwrap_err()
                .to_string()
                .contains("requires a non-empty 'secret_key'")
        );
    }

    #[test]
    fn test_rs256_missing_pem_rejected() {
        let val = json!({
            "algorithm": "RS256",
            "token_sources": [{ "type": "header", "name": "authorization" }],
            "validate_exp": true,
            "clock_skew_secs": 60,
            "claims_to_headers": {},
            "assert_claims": {},
            "cache_ttl_secs": 300
        });
        let res = validate_jwt_auth_config("jwt_no_pem", &val);
        assert!(res.is_err());
        assert!(
            res.unwrap_err()
                .to_string()
                .contains("requires a valid PEM-encoded 'public_key_pem'")
        );
    }

    #[test]
    fn test_invalid_algorithm_rejected() {
        let val = json!({
            "algorithm": "NONE",
            "secret_key": "abc",
            "token_sources": [{ "type": "header", "name": "authorization" }],
            "validate_exp": true,
            "clock_skew_secs": 60,
            "claims_to_headers": {},
            "assert_claims": {},
            "cache_ttl_secs": 300
        });
        let res = validate_jwt_auth_config("jwt_bad_alg", &val);
        assert!(res.is_err());
        assert!(
            res.unwrap_err()
                .to_string()
                .contains("unsupported JWT algorithm")
        );
    }

    #[test]
    fn test_missing_required_fields_rejected() {
        let val = json!({
            "algorithm": "HS256",
            "secret_key": "my-secret"
            // Missing token_sources, validate_exp, clock_skew_secs, claims_to_headers, assert_claims, cache_ttl_secs
        });
        let res = validate_jwt_auth_config("jwt_missing", &val);
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("missing field"));
    }
}
