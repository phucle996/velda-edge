//! Response Header Mutation Configuration Schema & Semantic Validation (`phase: "post_response"`).
//!
//! Enforces a strict declarative contract for mutating HTTP response headers before client delivery.
//!
//! # Strict Declarative Contract (Zero Silent Fallbacks)
//! - **RFC 9110 Grammar**: All header names must conform to 1*tchar grammar.
//! - **No CRLF Injection**: Header values must not contain `\r` or `\n`.
//! - **Explicit Control**: `remove_headers`, `set_headers`, `append_headers`, `set_if_missing` are all explicit.

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::SyncError;

/// Canonical dynamic variables supported by `response_header_mutation`.
pub const VALID_RESPONSE_DYNAMIC_VARS: &[&str] = &["$request_id", "$status", "$timestamp_epoch"];

/// Declarative configuration for the Response Header Mutation plugin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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

/// Checks if a header is forbidden from being removed from the downstream response.
/// Uses zero-allocation length-bucket dispatch for optimal performance.
#[inline]
pub fn is_forbidden_response_remove(name: &str) -> bool {
    let b = name.as_bytes();
    if b.starts_with(b":") {
        return true;
    }
    match b.len() {
        14 => b.eq_ignore_ascii_case(b"content-length"),
        17 => b.eq_ignore_ascii_case(b"transfer-encoding"),
        _ => false,
    }
}

/// Checks if a header is forbidden from being mutated/injected into the downstream response.
/// Uses zero-allocation length-bucket dispatch for optimal performance.
#[inline]
pub fn is_forbidden_response_mutate(name: &str) -> bool {
    let b = name.as_bytes();
    if b.starts_with(b":") {
        return true;
    }
    match b.len() {
        7 => b.eq_ignore_ascii_case(b"upgrade"),
        10 => b.eq_ignore_ascii_case(b"connection"),
        14 => b.eq_ignore_ascii_case(b"content-length"),
        17 => b.eq_ignore_ascii_case(b"transfer-encoding"),
        _ => false,
    }
}

fn is_valid_header_name(h: &str) -> Result<(), &'static str> {
    if h.is_empty() {
        return Err("header name cannot be empty");
    }
    if h.len() > 256 {
        return Err("header name exceeds maximum length of 256 bytes");
    }
    if h.starts_with(' ') || h.ends_with(' ') || h.starts_with('\t') || h.ends_with('\t') {
        return Err("header name contains leading or trailing whitespace");
    }
    if h.starts_with(':') {
        return Err("pseudo-headers starting with ':' are forbidden");
    }
    let all_tchar = h.bytes().all(|b| {
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
    });
    if !all_tchar {
        return Err("header name contains invalid RFC 9110 tchar characters");
    }
    Ok(())
}

fn is_valid_header_value(v: &str) -> Result<(), &'static str> {
    if v.len() > 8192 {
        return Err("header value exceeds maximum length of 8192 bytes");
    }
    // Dynamic variable validation
    if v.starts_with('$') {
        if v == "$$" {
            return Ok(());
        }
        if !VALID_RESPONSE_DYNAMIC_VARS.contains(&v) {
            return Err(
                "unknown dynamic variable; supported variables are: $request_id, $status, $timestamp_epoch (use '$$' to escape literal '$')",
            );
        }
        return Ok(());
    }

    // RFC 9110 field-vchar + SP/HTAB validation (Strict control character rejection)
    for b in v.bytes() {
        if b == b'\r' || b == b'\n' {
            return Err("header value contains CRLF control characters");
        }
        if b < 0x20 && b != b'\t' {
            return Err("header value contains illegal ASCII control characters");
        }
        if b == 0x7F {
            return Err("header value contains illegal DEL character (0x7F)");
        }
    }
    Ok(())
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

    // Invariant: Non-empty mutation policy
    if cfg.remove_headers.is_empty()
        && cfg.set_headers.is_empty()
        && cfg.append_headers.is_empty()
        && cfg.set_if_missing.is_empty()
    {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': response_header_mutation configuration must declare at least one active mutation rule"
            ),
        });
    }

    // 1. Validate remove_headers
    let mut remove_seen = HashSet::new();
    for h in &cfg.remove_headers {
        if let Err(err) = is_valid_header_name(h) {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': invalid header name '{h}' in remove_headers: {err}"
                ),
            });
        }
        if is_forbidden_response_remove(h) {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': header '{h}' in remove_headers is framing/pseudo and cannot be removed"
                ),
            });
        }
        let lower = h.to_ascii_lowercase();
        if !remove_seen.insert(lower) {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': duplicate case-insensitive header '{h}' in remove_headers"
                ),
            });
        }
    }

    // 2. Validate set_headers
    let mut set_seen = HashSet::new();
    for (k, v) in &cfg.set_headers {
        if let Err(err) = is_valid_header_name(k) {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': invalid header name '{k}' in set_headers: {err}"
                ),
            });
        }
        if is_forbidden_response_mutate(k) {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': header '{k}' in set_headers is framing/pseudo/hop-by-hop and cannot be mutated"
                ),
            });
        }
        if let Err(err) = is_valid_header_value(v) {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': invalid header value for '{k}' in set_headers: {err}"
                ),
            });
        }
        let lower = k.to_ascii_lowercase();
        if !set_seen.insert(lower) {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': duplicate case-insensitive header '{k}' in set_headers"
                ),
            });
        }
    }

    // 3. Validate append_headers
    let mut append_seen = HashSet::new();
    for (k, v) in &cfg.append_headers {
        if let Err(err) = is_valid_header_name(k) {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': invalid header name '{k}' in append_headers: {err}"
                ),
            });
        }
        if is_forbidden_response_mutate(k) {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': header '{k}' in append_headers is framing/pseudo/hop-by-hop and cannot be mutated"
                ),
            });
        }
        if let Err(err) = is_valid_header_value(v) {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': invalid header value for '{k}' in append_headers: {err}"
                ),
            });
        }
        let lower = k.to_ascii_lowercase();
        if !append_seen.insert(lower) {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': duplicate case-insensitive header '{k}' in append_headers"
                ),
            });
        }
    }

    // 4. Validate set_if_missing
    let mut missing_seen = HashSet::new();
    for (k, v) in &cfg.set_if_missing {
        if let Err(err) = is_valid_header_name(k) {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': invalid header name '{k}' in set_if_missing: {err}"
                ),
            });
        }
        if is_forbidden_response_mutate(k) {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': header '{k}' in set_if_missing is framing/pseudo/hop-by-hop and cannot be mutated"
                ),
            });
        }
        if let Err(err) = is_valid_header_value(v) {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': invalid header value for '{k}' in set_if_missing: {err}"
                ),
            });
        }
        let lower = k.to_ascii_lowercase();
        if !missing_seen.insert(lower) {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': duplicate case-insensitive header '{k}' in set_if_missing"
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

    #[test]
    fn test_forbidden_response_headers_rejected() {
        // Removing content-length
        let val_cl = json!({
            "remove_headers": ["content-length"],
            "set_headers": {},
            "append_headers": {},
            "set_if_missing": {}
        });
        assert!(validate_response_header_mutation_config("res_cl", &val_cl).is_err());

        // Mutating transfer-encoding
        let val_te = json!({
            "remove_headers": [],
            "set_headers": {
                "transfer-encoding": "chunked"
            },
            "append_headers": {},
            "set_if_missing": {}
        });
        assert!(validate_response_header_mutation_config("res_te", &val_te).is_err());

        // Mutating connection
        let val_conn = json!({
            "remove_headers": [],
            "set_headers": {
                "connection": "close"
            },
            "append_headers": {},
            "set_if_missing": {}
        });
        assert!(validate_response_header_mutation_config("res_conn", &val_conn).is_err());
    }

    #[test]
    fn test_unknown_response_dynamic_variable_rejected() {
        let val_bad_var = json!({
            "remove_headers": [],
            "set_headers": {
                "X-Trace": "$unknown_var"
            },
            "append_headers": {},
            "set_if_missing": {}
        });
        let res = validate_response_header_mutation_config("res_var", &val_bad_var);
        assert!(res.is_err());
        assert!(
            res.unwrap_err()
                .to_string()
                .contains("unknown dynamic variable")
        );
    }

    #[test]
    fn test_response_noop_rejected() {
        let val_noop = json!({
            "remove_headers": [],
            "set_headers": {},
            "append_headers": {},
            "set_if_missing": {}
        });
        let res = validate_response_header_mutation_config("res_noop", &val_noop);
        assert!(res.is_err());
        assert!(
            res.unwrap_err()
                .to_string()
                .contains("must declare at least one active mutation rule")
        );
    }
}
