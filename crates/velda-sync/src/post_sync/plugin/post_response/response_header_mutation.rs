//! Response Header Mutation Configuration Schema & Semantic Validation (`phase: "post_response"`).
//!
//! Enforces a strict declarative contract for mutating HTTP response headers before client delivery.
//!
//! # Strict Declarative Contract (Zero Silent Fallbacks)
//! - **RFC 9110 Grammar**: All header names must conform to 1*tchar grammar.
//! - **No CRLF Injection**: Header values must not contain `\r` or `\n`.
//! - **Explicit Control**: `remove_headers`, `set_headers`, `append_headers`, `set_if_missing` are all explicit.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::SyncError;

/// Declarative configuration for the Response Header Mutation plugin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResponseHeaderMutationConfig {
    /// Header names to strip from response before returning to client.
    pub remove_headers: Vec<String>,
    /// Headers to overwrite or set on response.
    pub set_headers: HashMap<String, String>,
    /// Headers to append to on response.
    pub append_headers: HashMap<String, String>,
    /// Headers to set ONLY if not already present in the response.
    pub set_if_missing: HashMap<String, String>,
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

/// Validates Response Header Mutation configuration strictly according to production standards.
pub fn validate_response_header_mutation_config(
    plugin_id: &str,
    config_val: &serde_json::Value,
) -> Result<ResponseHeaderMutationConfig, SyncError> {
    let cfg: ResponseHeaderMutationConfig =
        serde_json::from_value(config_val.clone()).map_err(|e| SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}' has malformed or incomplete response_header_mutation config: {e}"
            ),
        })?;

    // 1. Validate remove_headers
    for h in &cfg.remove_headers {
        if !is_valid_header_name(h) {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': invalid header name '{h}' in remove_headers"
                ),
            });
        }
    }

    // 2. Validate set_headers
    for (k, v) in &cfg.set_headers {
        if !is_valid_header_name(k) {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!("Plugin '{plugin_id}': invalid header name '{k}' in set_headers"),
            });
        }
        if v.bytes().any(|b| b == b'\r' || b == b'\n') {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': header value for '{k}' in set_headers contains CRLF control characters"
                ),
            });
        }
    }

    // 3. Validate append_headers
    for (k, v) in &cfg.append_headers {
        if !is_valid_header_name(k) {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': invalid header name '{k}' in append_headers"
                ),
            });
        }
        if v.bytes().any(|b| b == b'\r' || b == b'\n') {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': header value for '{k}' in append_headers contains CRLF control characters"
                ),
            });
        }
    }

    // 4. Validate set_if_missing
    for (k, v) in &cfg.set_if_missing {
        if !is_valid_header_name(k) {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': invalid header name '{k}' in set_if_missing"
                ),
            });
        }
        if v.bytes().any(|b| b == b'\r' || b == b'\n') {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': header value for '{k}' in set_if_missing contains CRLF control characters"
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
    fn test_valid_response_header_mutation() {
        let val = json!({
            "remove_headers": ["server", "x-powered-by"],
            "set_headers": {
                "server": "velda-edge",
                "x-served-by": "edge-node-1"
            },
            "append_headers": {
                "vary": "accept-encoding"
            },
            "set_if_missing": {
                "cache-control": "no-cache"
            }
        });
        let res = validate_response_header_mutation_config("res_hdr_1", &val);
        assert!(res.is_ok());
    }

    #[test]
    fn test_crlf_injection_rejected() {
        let val = json!({
            "remove_headers": [],
            "set_headers": {
                "x-injected": "val\r\ninjected-header: true"
            },
            "append_headers": {},
            "set_if_missing": {}
        });
        let res = validate_response_header_mutation_config("res_hdr_bad", &val);
        assert!(res.is_err());
        assert!(
            res.unwrap_err()
                .to_string()
                .contains("CRLF control characters")
        );
    }

    #[test]
    fn test_missing_required_fields_rejected() {
        let val = json!({
            "remove_headers": ["server"]
            // Missing set_headers, append_headers, set_if_missing
        });
        let res = validate_response_header_mutation_config("res_hdr_missing", &val);
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("missing field"));
    }
}
