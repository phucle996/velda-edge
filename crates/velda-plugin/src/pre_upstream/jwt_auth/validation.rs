//! JWT Authentication Configuration Schema & Semantic Validation (`jwt_auth::validation`).
//!
//! ```text
//!                     JWT AUTH SEMANTIC VALIDATION PIPELINE
//!
//!     PluginConfig (JSON, deny_unknown_fields)
//!            │
//!            ▼
//!     validate_jwt_auth_config()
//!            ├──► 1. Algorithm Pinning (HS256/384/512, RS256/384/512, ES256/384, EdDSA)
//!            ├──► 2. Key Material Invariant:
//!            │      • Symmetric  -> requires non-empty `secret_key`, forbids `public_key_pem`
//!            │      • Asymmetric -> forbids `secret_key`; validates `public_key_pem` ASN.1 DER
//!            │                      or defers to `jwks_rotator` when `public_key_pem == null`
//!            ├──► 3. Token Sources (RFC 9110 header grammar / non-empty cookie & query names)
//!            ├──► 4. Claims & Assertions (non-empty paths, valid target HeaderName)
//!            └──► 5. Bounds (`clock_skew_secs <= 3600`, `cache_ttl_secs <= 86400`)
//! ```

use std::collections::HashMap;
use std::str::FromStr;

use http::header::HeaderName;
use serde::{Deserialize, Serialize};

use super::crypto::{JwtAlgorithm, VerifierKey};
use crate::error::PluginError;

/// Maximum allowable clock skew tolerance in seconds (1 hour).
pub const MAX_CLOCK_SKEW_SECS: u64 = 3_600;

/// Maximum allowable authentication cache TTL in seconds (24 hours).
pub const MAX_CACHE_TTL_SECS: u64 = 86_400;

/// Declarative extraction source definition for JWT tokens.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
pub enum JwtTokenSourceConfig {
    /// Extract token from an HTTP request header (e.g. `Authorization: Bearer <token>`).
    #[serde(rename = "header")]
    Header {
        /// Header field name (e.g. `"authorization"`).
        name: String,
        /// Optional prefix to strip (e.g. `"Bearer "`).
        #[serde(default)]
        prefix: Option<String>,
    },
    /// Extract token from a Cookie header (e.g. `Cookie: access_token=<token>`).
    #[serde(rename = "cookie")]
    Cookie {
        /// Cookie name (e.g. `"access_token"`).
        name: String,
    },
    /// Extract token from a URL query parameter (e.g. `?token=<token>`).
    #[serde(rename = "query")]
    Query {
        /// Query parameter name (e.g. `"token"`).
        param: String,
    },
}

/// Declarative configuration for the JWT Authentication plugin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JwtAuthConfig {
    /// Cryptographic signing algorithm: HS256, HS384, HS512, RS256, RS384, RS512, ES256, ES384, EdDSA.
    pub algorithm: String,
    /// Shared secret key for HMAC algorithms (`HS256`/`HS384`/`HS512`). Must be `null` for asymmetric algorithms.
    #[serde(default)]
    pub secret_key: Option<String>,
    /// PEM-encoded public key for asymmetric algorithms (`RS*`/`ES*`/`EdDSA`), or `null` when paired with `jwks_rotator`.
    #[serde(default)]
    pub public_key_pem: Option<String>,
    /// Ordered locations from which to extract the JWT token.
    pub token_sources: Vec<JwtTokenSourceConfig>,
    /// Whether to enforce expiration (`exp`) claim validation.
    pub validate_exp: bool,
    /// Maximum clock skew allowed in seconds (`0..=3600`) when evaluating `exp` and `nbf` claims.
    pub clock_skew_secs: u64,
    /// Expected issuer (`iss`) claim to verify against, or `null` to bypass.
    #[serde(default)]
    pub issuer: Option<String>,
    /// Expected audience (`aud`) claim to verify against, or `null` to bypass.
    #[serde(default)]
    pub audience: Option<String>,
    /// Mapping of extracted JWT claim paths to upstream request headers (insert or overwrite when claim is present).
    pub claims_to_headers: HashMap<String, String>,
    /// Strict assertions requiring specific claim paths to match exact literal values (`403 Forbidden` on mismatch).
    pub assert_claims: HashMap<String, String>,
    /// Cache TTL in seconds (`0..=86400`) for positive/negative validation caching (`0` disables caching).
    pub cache_ttl_secs: u64,
}

#[inline]
fn is_valid_claim_path(path: &str) -> bool {
    let trimmed = path.trim();
    !trimmed.is_empty() && trimmed.split('.').all(|seg| !seg.trim().is_empty())
}

#[inline]
fn is_valid_header_name(h: &str) -> bool {
    let trimmed = h.trim();
    !trimmed.is_empty() && HeaderName::from_str(trimmed).is_ok()
}

/// Validates and normalizes `jwt_auth` configuration strictly at sync/boot time.
pub fn validate_jwt_auth_config(
    plugin_id: &str,
    config_val: &serde_json::Value,
) -> Result<JwtAuthConfig, PluginError> {
    let mut cfg: JwtAuthConfig = serde_json::from_value(config_val.clone()).map_err(|e| {
        PluginError::Config(format!(
            "Plugin '{plugin_id}' has malformed or incomplete jwt_auth config: {e}"
        ))
    })?;

    // 1. Validate & canonicalize algorithm
    let alg = JwtAlgorithm::from_str(&cfg.algorithm)
        .map_err(|e| PluginError::Config(format!("Plugin '{plugin_id}': {e}")))?;
    cfg.algorithm = alg.as_str().to_string();

    // 2. Cryptographic Key Invariant
    if alg.is_symmetric() {
        if cfg
            .public_key_pem
            .as_deref()
            .is_some_and(|s| !s.trim().is_empty())
        {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': symmetric algorithm '{}' must not set 'public_key_pem'",
                alg.as_str()
            )));
        }
        cfg.public_key_pem = None;

        let secret = cfg
            .secret_key
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                PluginError::Config(format!(
                    "Plugin '{plugin_id}': algorithm '{}' requires a non-empty 'secret_key'",
                    alg.as_str()
                ))
            })?
            .to_string();
        VerifierKey::new(alg, Some(&secret), None)
            .map_err(|e| PluginError::Config(format!("Plugin '{plugin_id}': {e}")))?;
        cfg.secret_key = Some(secret);
    } else {
        if cfg
            .secret_key
            .as_deref()
            .is_some_and(|s| !s.trim().is_empty())
        {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': asymmetric algorithm '{}' must not set 'secret_key'",
                alg.as_str()
            )));
        }
        cfg.secret_key = None;

        match cfg.public_key_pem.as_deref().map(str::trim) {
            Some(pem) if !pem.is_empty() => {
                VerifierKey::new(alg, None, Some(pem))
                    .map_err(|e| PluginError::Config(format!("Plugin '{plugin_id}': {e}")))?;
                cfg.public_key_pem = Some(pem.to_string());
            }
            Some(_) => {
                return Err(PluginError::Config(format!(
                    "Plugin '{plugin_id}': 'public_key_pem' cannot be empty whitespace; set null when paired with 'jwks_rotator'"
                )));
            }
            None => {
                // Permitted when paired with a companion `jwks_rotator`; enforced in
                // `validate_plugin_dependencies` and `wire_plugin_dependencies`.
                cfg.public_key_pem = None;
            }
        }
    }

    // 3. Validate Token Sources
    if cfg.token_sources.is_empty() {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': token_sources list cannot be empty"
        )));
    }

    for (idx, src) in cfg.token_sources.iter_mut().enumerate() {
        match src {
            JwtTokenSourceConfig::Header { name, .. } => {
                let trimmed = name.trim();
                if !is_valid_header_name(trimmed) {
                    return Err(PluginError::Config(format!(
                        "Plugin '{plugin_id}': token source at index {idx} has invalid header name '{name}'"
                    )));
                }
                *name = trimmed.to_ascii_lowercase();
            }
            JwtTokenSourceConfig::Cookie { name } => {
                let trimmed = name.trim();
                if trimmed.is_empty() {
                    return Err(PluginError::Config(format!(
                        "Plugin '{plugin_id}': token source at index {idx} has empty cookie name"
                    )));
                }
                *name = trimmed.to_string();
            }
            JwtTokenSourceConfig::Query { param } => {
                let trimmed = param.trim();
                if trimmed.is_empty() {
                    return Err(PluginError::Config(format!(
                        "Plugin '{plugin_id}': token source at index {idx} has empty query parameter name"
                    )));
                }
                *param = trimmed.to_string();
            }
        }
    }

    // 4. Validate Issuer & Audience
    if let Some(iss) = &cfg.issuer {
        let trimmed = iss.trim();
        if trimmed.is_empty() {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': 'issuer' cannot be empty; set null to disable issuer verification"
            )));
        }
        cfg.issuer = Some(trimmed.to_string());
    }
    if let Some(aud) = &cfg.audience {
        let trimmed = aud.trim();
        if trimmed.is_empty() {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': 'audience' cannot be empty; set null to disable audience verification"
            )));
        }
        cfg.audience = Some(trimmed.to_string());
    }

    // 5. Validate Claims to Headers & Assert Claims
    let mut normalized_claims = HashMap::with_capacity(cfg.claims_to_headers.len());
    for (claim, header) in &cfg.claims_to_headers {
        let c_trim = claim.trim();
        if !is_valid_claim_path(c_trim) {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': invalid claim path '{claim}' in claims_to_headers"
            )));
        }
        let h_trim = header.trim();
        if !is_valid_header_name(h_trim) {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': target header name '{header}' in claims_to_headers is invalid"
            )));
        }
        normalized_claims.insert(c_trim.to_string(), h_trim.to_ascii_lowercase());
    }
    cfg.claims_to_headers = normalized_claims;

    let mut normalized_asserts = HashMap::with_capacity(cfg.assert_claims.len());
    for (claim, expected) in &cfg.assert_claims {
        let c_trim = claim.trim();
        if !is_valid_claim_path(c_trim) {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': invalid claim path '{claim}' in assert_claims"
            )));
        }
        normalized_asserts.insert(c_trim.to_string(), expected.clone());
    }
    cfg.assert_claims = normalized_asserts;

    // 6. Validate Bounds
    if cfg.clock_skew_secs > MAX_CLOCK_SKEW_SECS {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': clock_skew_secs ({}) exceeds maximum allowable {MAX_CLOCK_SKEW_SECS}s (1 hour)",
            cfg.clock_skew_secs
        )));
    }

    if cfg.cache_ttl_secs > MAX_CACHE_TTL_SECS {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': cache_ttl_secs ({}) exceeds maximum allowable {MAX_CACHE_TTL_SECS}s (24 hours)",
            cfg.cache_ttl_secs
        )));
    }

    Ok(cfg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::prelude::*;
    use serde_json::json;

    fn valid_rsa_2048_pem() -> String {
        let n = [0x99u8; 256];
        let e = [0x01u8, 0x00, 0x01];
        let pkcs1 = crate::background::jwks_rotator::parser::encode_pkcs1_rsa(&n, &e);
        format!(
            "-----BEGIN RSA PUBLIC KEY-----\n{}\n-----END RSA PUBLIC KEY-----",
            BASE64_STANDARD.encode(&pkcs1)
        )
    }

    #[test]
    fn test_valid_hs256_config() {
        let val = json!({
            "algorithm": "HS256",
            "secret_key": "my-ultra-secure-hmac-secret-string",
            "token_sources": [{ "type": "header", "name": "Authorization", "prefix": "Bearer " }],
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
        let cfg = validate_jwt_auth_config("jwt_auth", &val).unwrap();
        assert_eq!(cfg.claims_to_headers.get("sub").unwrap(), "x-user-id");
    }

    #[test]
    fn test_valid_rs256_static_and_rotator_null_pem() {
        let pem = valid_rsa_2048_pem();
        let static_val = json!({
            "algorithm": "RS256",
            "public_key_pem": pem,
            "token_sources": [{ "type": "cookie", "name": "jwt_token" }],
            "validate_exp": true,
            "clock_skew_secs": 30,
            "claims_to_headers": {},
            "assert_claims": {},
            "cache_ttl_secs": 600
        });
        assert!(validate_jwt_auth_config("jwt_auth", &static_val).is_ok());

        // Companion jwks_rotator mode (`public_key_pem: null`)
        let rotator_val = json!({
            "algorithm": "RS256",
            "public_key_pem": null,
            "token_sources": [{ "type": "header", "name": "authorization", "prefix": "Bearer " }],
            "validate_exp": true,
            "clock_skew_secs": 30,
            "claims_to_headers": {},
            "assert_claims": {},
            "cache_ttl_secs": 300
        });
        assert!(validate_jwt_auth_config("jwt_auth", &rotator_val).is_ok());
    }

    #[test]
    fn test_corrupted_pem_and_unknown_fields_rejected() {
        let bad_pem = json!({
            "algorithm": "RS256",
            "public_key_pem": "-----BEGIN PUBLIC KEY-----\nnot-valid-der\n-----END PUBLIC KEY-----",
            "token_sources": [{ "type": "header", "name": "authorization" }],
            "validate_exp": true,
            "clock_skew_secs": 60,
            "claims_to_headers": {},
            "assert_claims": {},
            "cache_ttl_secs": 300
        });
        assert!(validate_jwt_auth_config("jwt_auth", &bad_pem).is_err());

        let unknown_field = json!({
            "algorithm": "HS256",
            "secret_key": "my-secret-key",
            "token_sources": [{ "type": "header", "name": "authorization" }],
            "validate_exp": true,
            "clock_skew_secs": 60,
            "claims_to_headers": {},
            "assert_claims": {},
            "cache_ttl_secs": 300,
            "cache_shards": 64
        });
        let err = validate_jwt_auth_config("jwt_auth", &unknown_field).unwrap_err();
        assert!(err.to_string().contains("unknown field"));
    }

    #[test]
    fn test_hs256_missing_secret_and_invalid_alg_rejected() {
        let no_secret = json!({
            "algorithm": "HS256",
            "token_sources": [{ "type": "header", "name": "authorization" }],
            "validate_exp": true,
            "clock_skew_secs": 60,
            "claims_to_headers": {},
            "assert_claims": {},
            "cache_ttl_secs": 300
        });
        assert!(validate_jwt_auth_config("jwt_auth", &no_secret).is_err());

        let bad_alg = json!({
            "algorithm": "NONE",
            "secret_key": "abc",
            "token_sources": [{ "type": "header", "name": "authorization" }],
            "validate_exp": true,
            "clock_skew_secs": 60,
            "claims_to_headers": {},
            "assert_claims": {},
            "cache_ttl_secs": 300
        });
        assert!(validate_jwt_auth_config("jwt_auth", &bad_alg).is_err());
    }
}
