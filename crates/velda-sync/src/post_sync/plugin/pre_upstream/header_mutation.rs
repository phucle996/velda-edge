//! Header Mutation Configuration Schema & Semantic Validation (`phase: "pre_upstream"`).
//!
//! Enforces a strict declarative contract for mutating downstream request headers
//! prior to upstream forwarding.
//!
//! # Strict Declarative Contract (Zero Silent Fallbacks)
//! - **RFC 9110 Grammar**: All header names must conform to 1*tchar grammar.
//! - **No CRLF Injection**: Header values must not contain `\r` or `\n`.
//! - **Explicit Control**: `strip_hop_by_hop`, `remove_headers`, `set_headers`, `append_headers` are all explicit.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::SyncError;

/// Declarative configuration for the Header Mutation plugin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeaderMutationConfig {
    /// Whether to strip hop-by-hop headers according to RFC 9110 (Connection, Keep-Alive, Upgrade, etc.).
    pub strip_hop_by_hop: bool,
    /// Header names to remove before forwarding to upstream.
    pub remove_headers: Vec<String>,
    /// Headers to set/overwrite on the request before forwarding.
    pub set_headers: HashMap<String, String>,
    /// Headers to append comma-separated values to on the request.
    pub append_headers: HashMap<String, String>,
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

/// Validates Header Mutation configuration strictly according to production standards.
pub fn validate_header_mutation_config(
    plugin_id: &str,
    config_val: &serde_json::Value,
) -> Result<HeaderMutationConfig, SyncError> {
    let cfg: HeaderMutationConfig =
        serde_json::from_value(config_val.clone()).map_err(|e| SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}' has malformed or incomplete header_mutation config: {e}"
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

    Ok(cfg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_valid_header_mutation() {
        let val = json!({
            "strip_hop_by_hop": true,
            "remove_headers": ["X-Powered-By", "Server"],
            "set_headers": {
                "X-Forwarded-Proto": "https",
                "X-Edge-Origin": "velda"
            },
            "append_headers": {
                "X-Trace-Hops": "edge-router-1"
            }
        });
        let res = validate_header_mutation_config("mut_1", &val);
        assert!(res.is_ok());
    }

    #[test]
    fn test_invalid_header_name_rejected() {
        let val = json!({
            "strip_hop_by_hop": false,
            "remove_headers": ["Invalid: Header Name"],
            "set_headers": {},
            "append_headers": {}
        });
        let res = validate_header_mutation_config("mut_bad_name", &val);
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("invalid header name"));
    }

    #[test]
    fn test_crlf_injection_rejected() {
        let val = json!({
            "strip_hop_by_hop": false,
            "remove_headers": [],
            "set_headers": {
                "X-Injected": "value\r\nInjected-Header: evil"
            },
            "append_headers": {}
        });
        let res = validate_header_mutation_config("mut_crlf", &val);
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("contains CRLF"));
    }

    #[test]
    fn test_missing_required_fields_rejected() {
        let val = json!({
            "set_headers": { "X-Test": "1" }
            // Missing strip_hop_by_hop, remove_headers, append_headers
        });
        let res = validate_header_mutation_config("mut_missing", &val);
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("missing field"));
    }
}
