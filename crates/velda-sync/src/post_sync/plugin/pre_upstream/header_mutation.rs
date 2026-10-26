//! Header Mutation Configuration Schema & Semantic Validation (`phase: "pre_upstream"`).
//!
//! Enforces a strict declarative contract for mutating downstream request headers
//! prior to upstream forwarding.
//!
//! # Strict Declarative Contract (Zero Silent Fallbacks)
//! - **RFC 9110 Grammar**: All header names must conform to 1*tchar grammar.
//! - **No CRLF Injection**: Header values must not contain `\r` or `\n`.
//! - **Explicit Control**: `strip_hop_by_hop`, `remove_headers`, `set_headers`, `append_headers` are all explicit.

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::SyncError;

/// Canonical dynamic variables supported by `header_mutation`.
pub const VALID_DYNAMIC_VARS: &[&str] = &[
    "$client_ip",
    "$request_id",
    "$scheme",
    "$host",
    "$method",
    "$path",
];

/// Declarative configuration for the Header Mutation plugin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HeaderMutationConfig {
    /// Whether to strip hop-by-hop headers according to RFC 9110 (Connection, Keep-Alive, Upgrade, etc.).
    pub strip_hop_by_hop: bool,
    /// Header names to remove before forwarding to upstream.
    pub remove_headers: Vec<String>,
    /// Header rename mapping from source name to destination name.
    #[serde(default)]
    pub rename_headers: HashMap<String, String>,
    /// Headers to set/overwrite on the request before forwarding.
    pub set_headers: HashMap<String, String>,
    /// Headers to append comma-separated values to on the request.
    pub append_headers: HashMap<String, String>,
}

/// Checks if a header is strictly forbidden from being removed (essential for routing, framing, or tracing).
/// Uses zero-allocation length-bucket dispatch for optimal performance.
#[inline]
pub fn is_forbidden_to_remove(name: &str) -> bool {
    let b = name.as_bytes();
    if b.starts_with(b":") {
        return true;
    }
    match b.len() {
        4 => b.eq_ignore_ascii_case(b"host"),
        12 => b.eq_ignore_ascii_case(b"x-request-id"),
        14 => b.eq_ignore_ascii_case(b"content-length"),
        17 => b.eq_ignore_ascii_case(b"transfer-encoding"),
        _ => false,
    }
}

/// Checks if a header is strictly forbidden from being set, appended, or renamed to (framing, pseudo, hop-by-hop, or tracing).
/// Uses zero-allocation length-bucket dispatch for optimal performance.
#[inline]
pub fn is_forbidden_to_mutate(name: &str) -> bool {
    let b = name.as_bytes();
    if b.starts_with(b":") {
        return true;
    }
    match b.len() {
        2 => b.eq_ignore_ascii_case(b"te"),
        4 => b.eq_ignore_ascii_case(b"host"),
        7 => b.eq_ignore_ascii_case(b"trailer") || b.eq_ignore_ascii_case(b"upgrade"),
        10 => b.eq_ignore_ascii_case(b"connection") || b.eq_ignore_ascii_case(b"keep-alive"),
        12 => b.eq_ignore_ascii_case(b"x-request-id"),
        14 => b.eq_ignore_ascii_case(b"content-length"),
        16 => b.eq_ignore_ascii_case(b"proxy-connection"),
        17 => b.eq_ignore_ascii_case(b"transfer-encoding"),
        18 => b.eq_ignore_ascii_case(b"proxy-authenticate"),
        19 => b.eq_ignore_ascii_case(b"proxy-authorization"),
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
        if !VALID_DYNAMIC_VARS.contains(&v) {
            return Err(
                "unknown dynamic variable; supported variables are: $client_ip, $request_id, $scheme, $host, $method, $path (use '$$' to escape literal '$')",
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

    // Invariant: Non-empty mutation policy
    if !cfg.strip_hop_by_hop
        && cfg.remove_headers.is_empty()
        && cfg.rename_headers.is_empty()
        && cfg.set_headers.is_empty()
        && cfg.append_headers.is_empty()
    {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': header_mutation configuration must declare at least one active mutation rule or enable strip_hop_by_hop"
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
        if is_forbidden_to_remove(h) {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': header '{h}' in remove_headers is protected and cannot be removed"
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

    // 2. Validate rename_headers
    let mut rename_sources = HashSet::new();
    let mut rename_targets = HashSet::new();
    for (from_k, to_v) in &cfg.rename_headers {
        if let Err(err) = is_valid_header_name(from_k) {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': invalid source header name '{from_k}' in rename_headers: {err}"
                ),
            });
        }
        if is_forbidden_to_remove(from_k) {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': source header '{from_k}' in rename_headers is protected and cannot be renamed"
                ),
            });
        }
        if let Err(err) = is_valid_header_name(to_v) {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': invalid target header name '{to_v}' in rename_headers: {err}"
                ),
            });
        }
        if is_forbidden_to_mutate(to_v) {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': target header '{to_v}' in rename_headers is protected/framing/hop-by-hop and cannot be mutated"
                ),
            });
        }
        if from_k.eq_ignore_ascii_case(to_v) {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': source and target header in rename_headers must not be identical ('{from_k}')"
                ),
            });
        }
        let from_lower = from_k.to_ascii_lowercase();
        if !rename_sources.insert(from_lower) {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': duplicate case-insensitive source header '{from_k}' in rename_headers"
                ),
            });
        }
        let to_lower = to_v.to_ascii_lowercase();
        if !rename_targets.insert(to_lower) {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': duplicate case-insensitive target header '{to_v}' in rename_headers"
                ),
            });
        }
    }

    // 3. Validate set_headers
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
        if is_forbidden_to_mutate(k) {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': header '{k}' in set_headers is protected/framing/hop-by-hop and cannot be mutated"
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

    // 4. Validate append_headers
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
        if is_forbidden_to_mutate(k) {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': header '{k}' in append_headers is protected/framing/hop-by-hop and cannot be mutated"
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
            "rename_headers": {
                "X-Old-Header": "X-New-Header"
            },
            "set_headers": {
                "X-Forwarded-Proto": "https",
                "X-Edge-Origin": "velda",
                "X-Client-Ip": "$client_ip"
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
            "strip_hop_by_hop": true,
            "remove_headers": ["Invalid: Header Name"],
            "set_headers": {},
            "append_headers": {}
        });
        let res = validate_header_mutation_config("mut_bad_name", &val);
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("invalid header name"));

        // Header name with leading whitespace must be rejected
        let val_space = json!({
            "strip_hop_by_hop": true,
            "remove_headers": [" X-Spaced"],
            "set_headers": {},
            "append_headers": {}
        });
        let res_space = validate_header_mutation_config("mut_bad_space", &val_space);
        assert!(res_space.is_err());
        assert!(res_space.unwrap_err().to_string().contains("whitespace"));
    }

    #[test]
    fn test_crlf_and_control_chars_rejected() {
        let val = json!({
            "strip_hop_by_hop": true,
            "remove_headers": [],
            "set_headers": {
                "X-Injected": "value\r\nInjected-Header: evil"
            },
            "append_headers": {}
        });
        let res = validate_header_mutation_config("mut_crlf", &val);
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("CRLF"));

        // Null byte
        let val_null = json!({
            "strip_hop_by_hop": true,
            "remove_headers": [],
            "set_headers": {
                "X-Null": "value\u{0000}bad"
            },
            "append_headers": {}
        });
        let res_null = validate_header_mutation_config("mut_null", &val_null);
        assert!(res_null.is_err());
        assert!(
            res_null
                .unwrap_err()
                .to_string()
                .contains("control characters")
        );
    }

    #[test]
    fn test_forbidden_and_protected_headers_rejected() {
        // 1. Removing Host
        let val_host = json!({
            "strip_hop_by_hop": true,
            "remove_headers": ["Host"],
            "set_headers": {},
            "append_headers": {}
        });
        assert!(validate_header_mutation_config("mut_host", &val_host).is_err());

        // 2. Removing Content-Length
        let val_cl = json!({
            "strip_hop_by_hop": true,
            "remove_headers": ["content-length"],
            "set_headers": {},
            "append_headers": {}
        });
        assert!(validate_header_mutation_config("mut_cl", &val_cl).is_err());

        // 3. Mutating Content-Length or Transfer-Encoding (Smuggling attempt)
        let val_smuggle = json!({
            "strip_hop_by_hop": true,
            "remove_headers": [],
            "set_headers": {
                "transfer-encoding": "chunked"
            },
            "append_headers": {}
        });
        assert!(validate_header_mutation_config("mut_smuggle", &val_smuggle).is_err());

        // 4. Mutating x-request-id
        let val_req_id = json!({
            "strip_hop_by_hop": true,
            "remove_headers": [],
            "set_headers": {
                "x-request-id": "spoofed-id"
            },
            "append_headers": {}
        });
        assert!(validate_header_mutation_config("mut_req_id", &val_req_id).is_err());

        // 5. Pseudo-headers forbidden
        let val_pseudo = json!({
            "strip_hop_by_hop": true,
            "remove_headers": [],
            "set_headers": {
                ":path": "/evil"
            },
            "append_headers": {}
        });
        assert!(validate_header_mutation_config("mut_pseudo", &val_pseudo).is_err());
    }

    #[test]
    fn test_dynamic_variable_validation() {
        // Unknown variable must fail
        let val_bad_var = json!({
            "strip_hop_by_hop": true,
            "remove_headers": [],
            "set_headers": {
                "X-Client": "$client_addr" // typo for $client_ip
            },
            "append_headers": {}
        });
        let res = validate_header_mutation_config("mut_var", &val_bad_var);
        assert!(res.is_err());
        assert!(
            res.unwrap_err()
                .to_string()
                .contains("unknown dynamic variable")
        );

        // Valid variable passes
        let val_good_var = json!({
            "strip_hop_by_hop": true,
            "remove_headers": [],
            "set_headers": {
                "X-Client": "$client_ip",
                "X-Req": "$request_id",
                "X-Scheme": "$scheme"
            },
            "append_headers": {}
        });
        assert!(validate_header_mutation_config("mut_good_var", &val_good_var).is_ok());
    }

    #[test]
    fn test_noop_configuration_rejected() {
        let val_noop = json!({
            "strip_hop_by_hop": false,
            "remove_headers": [],
            "rename_headers": {},
            "set_headers": {},
            "append_headers": {}
        });
        let res = validate_header_mutation_config("mut_noop", &val_noop);
        assert!(res.is_err());
        assert!(
            res.unwrap_err()
                .to_string()
                .contains("must declare at least one active mutation rule")
        );
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
