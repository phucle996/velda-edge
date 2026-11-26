//! JWT Authentication Configuration Schema & Bit-Mode Validation (`jwt_auth::validation`).
//!
//! ```text
//!                     JWT AUTH SEMANTIC VALIDATION PIPELINE
//!
//!     PluginConfig (JSON, deny_unknown_fields)
//!            │
//!            ├──► 1. `mode` bit encoding:
//!            │      • MODE_INTERNAL (0): static key required, `etp_id` / `path` forbidden, pbww = 0
//!            │      • MODE_EXTERNAL (1): `etp_id` required, static key optional (`algorithm` optional),
//!            │                           when both static key & `etp_id` exist -> ETP takes priority
//!            ├──► 2. Key Material Invariant:
//!            │      • HMAC (`HS*`) requires `secret_key` (forbidden with asymmetric algs)
//!            │      • Asymmetric (`RS*`/`ES*`/`EdDSA`) requires `public_key_pem` ASN.1 DER
//!            ├──► 3. Token Sources (header / cookie / query) & Claim Projections / Assertions
//!            └──► 4. Bounds (`clock_skew_secs <= 3600`, `cache_ttl_secs <= 86400`, `refresh >= 10s`)
//! ```

use std::collections::HashMap;
use std::str::FromStr;

use http::header::HeaderName;
use serde::{Deserialize, Serialize};

use super::crypto::{JwtAlgorithm, VerifierKey};
use crate::error::PluginError;

/// Internal mode bit (`0`): purely RAM-resident static keys (`pbww == 0`).
pub const MODE_INTERNAL: u8 = 0;

/// External mode bit (`1`): pulls & rotates keys asynchronously from an ETP pool (`pbww > 0`).
pub const MODE_EXTERNAL: u8 = 1;

/// Maximum allowable clock skew tolerance in seconds (1 hour).
pub const MAX_CLOCK_SKEW_SECS: u64 = 3_600;

/// Maximum allowable authentication cache TTL in seconds (24 hours).
pub const MAX_CACHE_TTL_SECS: u64 = 86_400;

pub const DEFAULT_REFRESH_INTERVAL_SECS: u64 = 300;
pub const MIN_REFRESH_INTERVAL_SECS: u64 = 10;
pub const DEFAULT_REQUEST_TIMEOUT_SECS: u64 = 10;
pub const DEFAULT_JWT_PBWW: u32 = 20;

fn default_mode() -> String {
    "internal".to_string()
}

fn default_refresh_interval_secs() -> u64 {
    DEFAULT_REFRESH_INTERVAL_SECS
}

fn default_request_timeout_secs() -> u64 {
    DEFAULT_REQUEST_TIMEOUT_SECS
}

const fn default_true() -> bool {
    true
}

const fn default_pbww() -> u32 {
    DEFAULT_JWT_PBWW
}

/// Declarative extraction source definition for JWT tokens.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
pub enum JwtTokenSourceConfig {
    /// Extract token from an HTTP request header (e.g. `Authorization: Bearer <token>`).
    #[serde(rename = "header")]
    Header {
        name: String,
        #[serde(default)]
        prefix: Option<String>,
    },
    /// Extract token from a Cookie header (e.g. `Cookie: access_token=<token>`).
    #[serde(rename = "cookie")]
    Cookie { name: String },
    /// Extract token from a URL query parameter (e.g. `?token=<token>`).
    #[serde(rename = "query")]
    Query { param: String },
}

/// Declarative configuration for unified `jwt_auth` (`internal` static + `external` ETP modes).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JwtAuthConfig {
    /// Key source mode: `"internal"` (static key, bit `0`) or `"external"` (ETP provider, bit `1`).
    #[serde(default = "default_mode")]
    pub mode: String,
    /// Cryptographic algorithm for static key (required in `internal` mode; optional fallback in `external`).
    #[serde(default)]
    pub algorithm: Option<String>,
    /// Shared secret key for HMAC static fallback (`HS256`/`HS384`/`HS512`).
    #[serde(default)]
    pub secret_key: Option<String>,
    /// PEM-encoded public key for asymmetric static fallback (`RS*`/`ES*`/`EdDSA`).
    #[serde(default)]
    pub public_key_pem: Option<String>,
    /// Target ETP connection profile identifier in `etp.json` (required when `mode == "external"`).
    #[serde(default)]
    pub etp_id: Option<String>,
    /// Optional provider resource path / SecretId / object key called over the borrowed ETP connection.
    #[serde(default)]
    pub path: Option<String>,
    /// Background key refresh cadence in seconds (minimum 10s, default 300s) for `external` mode.
    #[serde(default = "default_refresh_interval_secs")]
    pub refresh_interval_secs: u64,
    /// Network read/write timeout in seconds when polling ETP in `external` mode.
    #[serde(default = "default_request_timeout_secs")]
    pub request_timeout_secs: u64,
    /// When `external` mode also specifies a static key, keeps it as standby fallback while prioritizing ETP.
    #[serde(default = "default_true")]
    pub preserve_static_key: bool,
    /// Plugin Background Workload Weight (`1..=100` in `external` mode; forced `0` in `internal`).
    #[serde(default = "default_pbww")]
    pub pbww: u32,
    /// Ordered locations from which to extract the JWT token.
    pub token_sources: Vec<JwtTokenSourceConfig>,
    /// Whether to enforce expiration (`exp`) claim validation.
    pub validate_exp: bool,
    /// Maximum clock skew allowed in seconds (`0..=3600`) when evaluating `exp` and `nbf`.
    pub clock_skew_secs: u64,
    /// Expected issuer (`iss`) claim to verify against, or `null` to bypass.
    #[serde(default)]
    pub issuer: Option<String>,
    /// Expected audience (`aud`) claim to verify against, or `null` to bypass.
    #[serde(default)]
    pub audience: Option<String>,
    /// Mapping of extracted JWT claim paths to upstream request headers.
    pub claims_to_headers: HashMap<String, String>,
    /// Strict assertions requiring specific claim paths to match exact literal values (`403` on mismatch).
    pub assert_claims: HashMap<String, String>,
    /// Cache TTL in seconds (`0..=86400`, `0` disables caching).
    pub cache_ttl_secs: u64,
}

impl JwtAuthConfig {
    /// Returns the compact 1-bit runtime mode (`MODE_INTERNAL = 0`, `MODE_EXTERNAL = 1`).
    #[inline(always)]
    pub fn mode_bit(&self) -> u8 {
        if self.mode.eq_ignore_ascii_case("external") {
            MODE_EXTERNAL
        } else {
            MODE_INTERNAL
        }
    }
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

/// Validates and normalizes `jwt_auth` configuration strictly across both `internal` and `external` modes.
pub fn validate_jwt_auth_config(
    plugin_id: &str,
    config_val: &serde_json::Value,
) -> Result<JwtAuthConfig, PluginError> {
    let mut cfg: JwtAuthConfig = serde_json::from_value(config_val.clone()).map_err(|e| {
        PluginError::Config(format!(
            "Plugin '{plugin_id}' has malformed or incomplete jwt_auth config: {e}"
        ))
    })?;

    // 1. Validate & normalize mode -> 1-bit boundary (`internal` | `external`)
    let mode_norm = cfg.mode.trim().to_ascii_lowercase();
    let mode_bit = match mode_norm.as_str() {
        "internal" => MODE_INTERNAL,
        "external" => MODE_EXTERNAL,
        other => {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': invalid jwt_auth mode '{other}'; expected 'internal' or 'external'"
            )));
        }
    };
    cfg.mode = mode_norm;

    // 2. Normalize & validate ETP fields
    let trimmed_etp = cfg
        .etp_id
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(ToString::to_string);
    if cfg.etp_id.is_some() && trimmed_etp.is_none() {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': 'etp_id' cannot be empty whitespace; omit or set null"
        )));
    }
    cfg.etp_id = trimmed_etp;

    if let Some(raw_path) = cfg.path.as_deref() {
        let p_trim = raw_path.trim();
        if p_trim.is_empty() {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': 'path' cannot be empty whitespace; omit or set null"
            )));
        }
        cfg.path = Some(p_trim.to_string());
    }

    // 3. Validate static key inputs & mode cross-constraints
    let raw_secret = cfg
        .secret_key
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(ToString::to_string);
    if cfg.secret_key.is_some() && raw_secret.is_none() {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': 'secret_key' cannot be empty whitespace; set null if unused"
        )));
    }
    cfg.secret_key = raw_secret;

    let raw_pem = cfg
        .public_key_pem
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(ToString::to_string);
    if cfg.public_key_pem.is_some() && raw_pem.is_none() {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': 'public_key_pem' cannot be empty whitespace; set null if unused"
        )));
    }
    cfg.public_key_pem = raw_pem;

    let has_static_secret = cfg.secret_key.is_some();
    let has_static_pem = cfg.public_key_pem.is_some();
    if has_static_secret && has_static_pem {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': cannot specify both 'secret_key' and 'public_key_pem'"
        )));
    }

    let parsed_alg = if let Some(alg_str) = cfg.algorithm.as_deref().map(str::trim) {
        if alg_str.is_empty() {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': 'algorithm' cannot be empty; omit or set null when using external ETP keys"
            )));
        }
        let alg = JwtAlgorithm::from_str(alg_str)
            .map_err(|e| PluginError::Config(format!("Plugin '{plugin_id}': {e}")))?;
        cfg.algorithm = Some(alg.as_str().to_string());
        Some(alg)
    } else {
        cfg.algorithm = None;
        None
    };

    if has_static_secret || has_static_pem {
        let alg = parsed_alg.ok_or_else(|| {
            PluginError::Config(format!(
                "Plugin '{plugin_id}': static key ('secret_key' / 'public_key_pem') requires 'algorithm' to be pinned"
            ))
        })?;
        if has_static_secret && !alg.is_symmetric() {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': asymmetric algorithm '{}' must not set 'secret_key'",
                alg.as_str()
            )));
        }
        if has_static_pem && alg.is_symmetric() {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': symmetric algorithm '{}' must not set 'public_key_pem'",
                alg.as_str()
            )));
        }
        VerifierKey::new(
            alg,
            cfg.secret_key.as_deref(),
            cfg.public_key_pem.as_deref(),
        )
        .map_err(|e| PluginError::Config(format!("Plugin '{plugin_id}': {e}")))?;
    }

    if mode_bit == MODE_INTERNAL {
        if cfg.etp_id.is_some() {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': mode 'internal' forbids 'etp_id'; use mode 'external' to fetch keys from ETP"
            )));
        }
        if cfg.path.is_some() {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': mode 'internal' forbids 'path'"
            )));
        }
        if parsed_alg.is_none() || (!has_static_secret && !has_static_pem) {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': mode 'internal' requires 'algorithm' and either 'secret_key' or 'public_key_pem'"
            )));
        }
        cfg.pbww = 0;
    } else {
        // MODE_EXTERNAL
        if cfg.etp_id.is_none() {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': mode 'external' requires non-empty 'etp_id'"
            )));
        }
        if cfg.refresh_interval_secs < MIN_REFRESH_INTERVAL_SECS {
            cfg.refresh_interval_secs = MIN_REFRESH_INTERVAL_SECS;
        }
        if cfg.request_timeout_secs == 0 {
            cfg.request_timeout_secs = DEFAULT_REQUEST_TIMEOUT_SECS;
        }
        cfg.pbww = cfg.pbww.clamp(1, 100);
    }

    // 4. Validate Token Sources
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

    // 5. Validate Issuer & Audience
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

    // 6. Validate Claims to Headers & Assert Claims
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

    // 7. Validate Bounds
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

    fn test_pkcs1_rsa_2048_pem() -> String {
        let n = [0x99u8; 256];
        let e = [0x01u8, 0x00, 0x01];
        let mut body = Vec::new();
        for part in [&n[..], &e[..]] {
            body.push(0x02);
            let pad = (part[0] & 0x80) != 0;
            let len = part.len() + usize::from(pad);
            if len < 128 {
                body.push(len as u8);
            } else {
                body.push(0x82);
                body.push((len >> 8) as u8);
                body.push((len & 0xff) as u8);
            }
            if pad {
                body.push(0x00);
            }
            body.extend_from_slice(part);
        }
        let mut der = vec![
            0x30,
            0x82,
            (body.len() >> 8) as u8,
            (body.len() & 0xff) as u8,
        ];
        der.extend_from_slice(&body);
        format!(
            "-----BEGIN RSA PUBLIC KEY-----\n{}\n-----END RSA PUBLIC KEY-----",
            BASE64_STANDARD.encode(&der)
        )
    }

    #[test]
    fn test_internal_mode_hs256_and_rs256() {
        let val = json!({
            "mode": "internal",
            "algorithm": "HS256",
            "secret_key": "my-ultra-secure-hmac-secret-string",
            "token_sources": [{ "type": "header", "name": "Authorization", "prefix": "Bearer " }],
            "validate_exp": true,
            "clock_skew_secs": 60,
            "issuer": "https://auth.company.com",
            "claims_to_headers": { "sub": "X-User-ID", "email": "X-User-Email" },
            "assert_claims": {},
            "cache_ttl_secs": 300
        });
        let cfg = validate_jwt_auth_config("jwt_auth", &val).unwrap();
        assert_eq!(cfg.mode_bit(), MODE_INTERNAL);
        assert_eq!(cfg.pbww, 0);
        assert_eq!(cfg.claims_to_headers.get("sub").unwrap(), "x-user-id");

        let pem = test_pkcs1_rsa_2048_pem();
        let rs_val = json!({
            "algorithm": "RS256",
            "public_key_pem": pem,
            "token_sources": [{ "type": "cookie", "name": "jwt_token" }],
            "validate_exp": true,
            "clock_skew_secs": 30,
            "claims_to_headers": {},
            "assert_claims": {},
            "cache_ttl_secs": 600
        });
        assert!(validate_jwt_auth_config("jwt_auth", &rs_val).is_ok());
    }

    #[test]
    fn test_external_mode_keyless_hmac_and_multi_algo() {
        // Keyless HMAC / ETP-determined algorithm is valid in external mode
        let ext = json!({
            "mode": "external",
            "etp_id": "vault-prod",
            "path": "/v1/secret/data/jwt",
            "refresh_interval_secs": 3,
            "token_sources": [{ "type": "header", "name": "authorization", "prefix": "Bearer " }],
            "validate_exp": true,
            "clock_skew_secs": 30,
            "claims_to_headers": {},
            "assert_claims": {},
            "cache_ttl_secs": 300
        });
        let cfg = validate_jwt_auth_config("jwt_auth", &ext).unwrap();
        assert_eq!(cfg.mode_bit(), MODE_EXTERNAL);
        assert_eq!(cfg.refresh_interval_secs, MIN_REFRESH_INTERVAL_SECS);
        assert!(cfg.pbww > 0);
        assert!(cfg.algorithm.is_none());
    }

    #[test]
    fn test_cross_mode_validation_rejects_mismatches() {
        // Internal forbids etp_id
        let internal_with_etp = json!({
            "mode": "internal",
            "algorithm": "HS256",
            "secret_key": "sec",
            "etp_id": "vault-prod",
            "token_sources": [{ "type": "header", "name": "authorization" }],
            "validate_exp": true,
            "clock_skew_secs": 60,
            "claims_to_headers": {},
            "assert_claims": {},
            "cache_ttl_secs": 300
        });
        assert!(validate_jwt_auth_config("jwt_auth", &internal_with_etp).is_err());

        // External requires etp_id
        let external_no_etp = json!({
            "mode": "external",
            "token_sources": [{ "type": "header", "name": "authorization" }],
            "validate_exp": true,
            "clock_skew_secs": 60,
            "claims_to_headers": {},
            "assert_claims": {},
            "cache_ttl_secs": 300
        });
        assert!(validate_jwt_auth_config("jwt_auth", &external_no_etp).is_err());

        // Static key without pinned algorithm rejected
        let static_no_alg = json!({
            "mode": "external",
            "etp_id": "vault-prod",
            "secret_key": "fallback-secret",
            "token_sources": [{ "type": "header", "name": "authorization" }],
            "validate_exp": true,
            "clock_skew_secs": 60,
            "claims_to_headers": {},
            "assert_claims": {},
            "cache_ttl_secs": 300
        });
        assert!(validate_jwt_auth_config("jwt_auth", &static_no_alg).is_err());

        // Unknown field rejected
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
}
