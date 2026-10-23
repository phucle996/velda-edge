//! HMAC Authentication Configuration Schema & Semantic Validation (`phase: "pre_upstream"`).
//!
//! Enforces a strict declarative contract for Hash-based Message Authentication Code (HMAC)
//! request signing, anti-tampering, and anti-replay verification conforming to RFC 2104.
//!
//! # Strict Declarative Contract (Zero Silent Fallbacks)
//! - **Explicit Credentials**: Non-empty list of access keys and secret keys (min 16 bytes).
//! - **Valid Headers**: All specified header names must conform to RFC 9110 token grammar.
//! - **Strict Algorithms**: Supported algorithms are strictly `"hmac-sha256"` and `"hmac-sha512"`.
//! - **Strict Encodings**: Supported signature encodings are strictly `"hex"` and `"base64"`.
//! - **Anti-Replay Window**: `clock_skew_seconds` must be strictly positive (> 0 and <= 86400).
//! - **Explicit Nonce Control**: If `enforce_nonce` is true, `nonce_header` must be included in `signed_headers`.

use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::SyncError;

/// Declarative credential configuration for an authorized caller.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HmacCredentialConfig {
    /// Public access key identifier (e.g. "partner_vnpay_01").
    pub access_key_id: String,
    /// Shared secret key used for HMAC calculation (minimum 16 bytes).
    pub secret_key: String,
    /// Optional logical consumer or tenant identity.
    pub consumer_id: Option<String>,
}

/// Declarative configuration for the HMAC Authentication plugin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HmacAuthConfig {
    /// Header containing caller's public access key ID (e.g. "x-access-key-id").
    pub access_key_header: String,
    /// Header containing client's signature (e.g. "x-signature").
    pub signature_header: String,
    /// HMAC cryptographic algorithm: `"hmac-sha256"` or `"hmac-sha512"`.
    pub signature_algorithm: String,
    /// Signature wire encoding format: `"hex"` or `"base64"`.
    pub signature_encoding: String,
    /// Header containing request timestamp (e.g. "x-timestamp").
    pub timestamp_header: String,
    /// Maximum allowed clock skew in seconds (e.g. 60).
    pub clock_skew_seconds: u64,
    /// Ordered list of header names required to participate in canonical signature.
    pub signed_headers: Vec<String>,
    /// Whether to enforce SHA-256 digest verification on request payload.
    pub validate_request_body: bool,
    /// Header containing pre-computed SHA-256 payload digest for streaming requests (e.g. "x-content-sha256").
    pub content_sha256_header: String,
    /// Header containing unique single-use request nonce (e.g. "x-nonce").
    pub nonce_header: String,
    /// Whether to strictly enforce and track unique single-use request nonces.
    pub enforce_nonce: bool,
    /// Pre-configured authorized caller credentials.
    pub credentials: Vec<HmacCredentialConfig>,
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

/// Validates HMAC Authentication configuration strictly according to production standards.
pub fn validate_hmac_auth_config(
    plugin_id: &str,
    config_val: &serde_json::Value,
) -> Result<HmacAuthConfig, SyncError> {
    let cfg: HmacAuthConfig =
        serde_json::from_value(config_val.clone()).map_err(|e| SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}' has malformed or incomplete hmac_auth config: {e}"
            ),
        })?;

    // 1. Validate Header Names
    let headers_to_check = [
        ("access_key_header", &cfg.access_key_header),
        ("signature_header", &cfg.signature_header),
        ("timestamp_header", &cfg.timestamp_header),
        ("content_sha256_header", &cfg.content_sha256_header),
        ("nonce_header", &cfg.nonce_header),
    ];

    for (field, hname) in headers_to_check {
        if !is_valid_header_name(hname) {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': field '{field}' contains invalid RFC 9110 header name '{hname}'"
                ),
            });
        }
    }

    // 2. Validate Algorithm
    let algo = cfg.signature_algorithm.trim().to_ascii_lowercase();
    if algo != "hmac-sha256" && algo != "hmac-sha512" {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': signature_algorithm '{}' is invalid; strictly 'hmac-sha256' or 'hmac-sha512' are supported",
                cfg.signature_algorithm
            ),
        });
    }

    // 3. Validate Encoding
    let enc = cfg.signature_encoding.trim().to_ascii_lowercase();
    if enc != "hex" && enc != "base64" {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': signature_encoding '{}' is invalid; strictly 'hex' or 'base64' are supported",
                cfg.signature_encoding
            ),
        });
    }

    // 4. Validate Clock Skew
    if cfg.clock_skew_seconds == 0 || cfg.clock_skew_seconds > 86400 {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': clock_skew_seconds must be between 1 and 86400 (got {})",
                cfg.clock_skew_seconds
            ),
        });
    }

    // 5. Validate Signed Headers
    if cfg.signed_headers.is_empty() {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': signed_headers list cannot be empty (must at least include timestamp_header)"
            ),
        });
    }

    let mut signed_set = HashSet::with_capacity(cfg.signed_headers.len());
    for (idx, h) in cfg.signed_headers.iter().enumerate() {
        let h_lower = h.trim().to_ascii_lowercase();
        if !is_valid_header_name(&h_lower) {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': signed_headers at index {idx} contains invalid header name '{h}'"
                ),
            });
        }
        if !signed_set.insert(h_lower) {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': duplicate header '{h}' detected in signed_headers"
                ),
            });
        }
    }

    let ts_canonical = cfg.timestamp_header.trim().to_ascii_lowercase();
    if !signed_set.contains(&ts_canonical) {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': timestamp_header '{ts_canonical}' must be included in signed_headers to prevent replay attacks"
            ),
        });
    }

    if cfg.enforce_nonce {
        let nonce_canonical = cfg.nonce_header.trim().to_ascii_lowercase();
        if !signed_set.contains(&nonce_canonical) {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': nonce_header '{nonce_canonical}' must be included in signed_headers when enforce_nonce is true"
                ),
            });
        }
    }

    // 6. Validate Credentials
    if cfg.credentials.is_empty() {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': credentials list cannot be empty (at least one credential pair is required)"
            ),
        });
    }

    let mut key_ids = HashSet::with_capacity(cfg.credentials.len());
    for (idx, cred) in cfg.credentials.iter().enumerate() {
        let kid = cred.access_key_id.trim();
        if kid.is_empty() {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': credentials at index {idx} has empty access_key_id"
                ),
            });
        }
        if !key_ids.insert(kid.to_string()) {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': duplicate access_key_id '{kid}' in credentials at index {idx}"
                ),
            });
        }

        if cred.secret_key.trim().len() < 16 {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': secret_key for access_key_id '{kid}' must be at least 16 characters for cryptographic safety"
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
    fn test_valid_hmac_config() {
        let val = json!({
            "access_key_header": "x-access-key-id",
            "signature_header": "x-signature",
            "signature_algorithm": "hmac-sha256",
            "signature_encoding": "hex",
            "timestamp_header": "x-timestamp",
            "clock_skew_seconds": 60,
            "signed_headers": ["host", "x-timestamp", "x-nonce"],
            "validate_request_body": true,
            "content_sha256_header": "x-content-sha256",
            "nonce_header": "x-nonce",
            "enforce_nonce": true,
            "credentials": [
                {
                    "access_key_id": "client_vnpay_01",
                    "secret_key": "sec_0123456789abcdef0123456789",
                    "consumer_id": "vnpay-partner"
                }
            ]
        });

        let res = validate_hmac_auth_config("hmac_test", &val);
        assert!(res.is_ok());
        let cfg = res.unwrap();
        assert_eq!(cfg.clock_skew_seconds, 60);
        assert_eq!(cfg.credentials.len(), 1);
    }

    #[test]
    fn test_missing_timestamp_in_signed_headers() {
        let val = json!({
            "access_key_header": "x-access-key-id",
            "signature_header": "x-signature",
            "signature_algorithm": "hmac-sha256",
            "signature_encoding": "hex",
            "timestamp_header": "x-timestamp",
            "clock_skew_seconds": 60,
            "signed_headers": ["host", "x-nonce"],
            "validate_request_body": true,
            "content_sha256_header": "x-content-sha256",
            "nonce_header": "x-nonce",
            "enforce_nonce": false,
            "credentials": [
                {
                    "access_key_id": "client_01",
                    "secret_key": "sec_0123456789abcdef0123456789"
                }
            ]
        });

        let res = validate_hmac_auth_config("hmac_test", &val);
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("timestamp_header"));
    }

    #[test]
    fn test_secret_key_too_short() {
        let val = json!({
            "access_key_header": "x-access-key-id",
            "signature_header": "x-signature",
            "signature_algorithm": "hmac-sha256",
            "signature_encoding": "hex",
            "timestamp_header": "x-timestamp",
            "clock_skew_seconds": 60,
            "signed_headers": ["x-timestamp"],
            "validate_request_body": false,
            "content_sha256_header": "x-content-sha256",
            "nonce_header": "x-nonce",
            "enforce_nonce": false,
            "credentials": [
                {
                    "access_key_id": "client_01",
                    "secret_key": "short_key"
                }
            ]
        });

        let res = validate_hmac_auth_config("hmac_test", &val);
        assert!(res.is_err());
        assert!(
            res.unwrap_err()
                .to_string()
                .contains("at least 16 characters")
        );
    }
}
