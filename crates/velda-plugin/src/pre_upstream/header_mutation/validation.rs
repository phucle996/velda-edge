//! Header Mutation Configuration Schema & Semantic Validation.
//!
//! Validates explicit static remove, set (overwrite), and append (add) rules
//! targeting `HeaderWorkspace` directly without implicit protections.
//!
//! ```text
//! JSON Config ─▶ serde(deny_unknown_fields) ─▶ Non-Empty Rule Check
//!                                                   │
//!                                                   ├─▶ remove_headers: RFC 9110 tchar + no dup
//!                                                   ├─▶ set_headers / append_headers: tchar + static vchar
//!                                                   └─▶ OK ─▶ Compiled into HeaderMutationPlugin
//! ```

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::error::PluginError;

/// Declarative configuration for the Header Mutation plugin (static values only).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HeaderMutationConfig {
    /// Header names to delete before forwarding to upstream.
    #[serde(default)]
    pub remove_headers: Vec<String>,
    /// Static headers to set/overwrite on the request before forwarding.
    #[serde(default)]
    pub set_headers: HashMap<String, String>,
    /// Static headers to append to the request before forwarding.
    #[serde(default)]
    pub append_headers: HashMap<String, String>,
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

/// Validates Header Mutation configuration syntax and invariants.
pub fn validate_header_mutation_config(
    plugin_id: &str,
    config_val: &serde_json::Value,
) -> Result<HeaderMutationConfig, PluginError> {
    let cfg: HeaderMutationConfig = serde_json::from_value(config_val.clone()).map_err(|e| {
        PluginError::Config(format!(
            "Plugin '{plugin_id}' has malformed or incomplete header_mutation config: {e}"
        ))
    })?;

    if cfg.remove_headers.is_empty() && cfg.set_headers.is_empty() && cfg.append_headers.is_empty()
    {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': header_mutation configuration must declare at least one rule in remove_headers, set_headers, or append_headers"
        )));
    }

    // 1. Validate remove_headers
    let mut remove_seen = HashSet::new();
    for h in &cfg.remove_headers {
        if let Err(err) = is_valid_header_name(h) {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': invalid header name '{h}' in remove_headers: {err}"
            )));
        }
        let lower = h.to_ascii_lowercase();
        if !remove_seen.insert(lower) {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': duplicate case-insensitive header '{h}' in remove_headers"
            )));
        }
    }

    // 2. Validate set_headers
    let mut set_seen = HashSet::new();
    for (k, v) in &cfg.set_headers {
        if let Err(err) = is_valid_header_name(k) {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': invalid header name '{k}' in set_headers: {err}"
            )));
        }
        if let Err(err) = is_valid_header_value(v) {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': invalid header value for '{k}' in set_headers: {err}"
            )));
        }
        let lower = k.to_ascii_lowercase();
        if !set_seen.insert(lower) {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': duplicate case-insensitive header '{k}' in set_headers"
            )));
        }
    }

    // 3. Validate append_headers
    let mut append_seen = HashSet::new();
    for (k, v) in &cfg.append_headers {
        if let Err(err) = is_valid_header_name(k) {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': invalid header name '{k}' in append_headers: {err}"
            )));
        }
        if let Err(err) = is_valid_header_value(v) {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': invalid header value for '{k}' in append_headers: {err}"
            )));
        }
        let lower = k.to_ascii_lowercase();
        if !append_seen.insert(lower) {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': duplicate case-insensitive header '{k}' in append_headers"
            )));
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
            "remove_headers": ["X-Powered-By", "Server", "x-request-id", "Host"],
            "set_headers": {
                "X-Forwarded-Proto": "https",
                "X-Edge-Origin": "velda",
                "x-request-id": "custom-trace-id"
            },
            "append_headers": {
                "X-Trace-Hops": "edge-router-1"
            }
        });
        assert!(validate_header_mutation_config("mut_1", &val).is_ok());
    }

    #[test]
    fn test_partial_config_blocks_allowed() {
        let remove_only = json!({
            "remove_headers": ["x-request-id"]
        });
        assert!(validate_header_mutation_config("mut_remove_only", &remove_only).is_ok());
    }

    #[test]
    fn test_invalid_header_name_rejected() {
        let val = json!({
            "remove_headers": ["Invalid: Header Name"]
        });
        let res = validate_header_mutation_config("mut_bad_name", &val);
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("invalid header name"));

        let val_space = json!({
            "remove_headers": [" X-Spaced"]
        });
        let res_space = validate_header_mutation_config("mut_bad_space", &val_space);
        assert!(res_space.is_err());
        assert!(res_space.unwrap_err().to_string().contains("whitespace"));

        let val_pseudo = json!({
            "set_headers": { ":path": "/evil" }
        });
        assert!(validate_header_mutation_config("mut_pseudo", &val_pseudo).is_err());
    }

    #[test]
    fn test_crlf_and_control_chars_rejected() {
        let val = json!({
            "set_headers": {
                "X-Injected": "value\r\nInjected-Header: evil"
            }
        });
        let res = validate_header_mutation_config("mut_crlf", &val);
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("CRLF"));

        let val_null = json!({
            "set_headers": {
                "X-Null": "value\u{0000}bad"
            }
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
    fn test_literal_dollar_strings_accepted_as_static_values() {
        let val = json!({
            "set_headers": {
                "X-Tag": "$literal-value"
            }
        });
        assert!(validate_header_mutation_config("mut_dollar", &val).is_ok());
    }

    #[test]
    fn test_noop_configuration_rejected() {
        let val_noop = json!({
            "remove_headers": [],
            "set_headers": {},
            "append_headers": {}
        });
        let res = validate_header_mutation_config("mut_noop", &val_noop);
        assert!(res.is_err());
        assert!(
            res.unwrap_err()
                .to_string()
                .contains("must declare at least one rule")
        );

        assert!(validate_header_mutation_config("mut_empty", &json!({})).is_err());
    }

    #[test]
    fn test_unknown_fields_rejected() {
        let val = json!({
            "set_headers": { "X-Test": "1" },
            "unexpected": true
        });
        let res = validate_header_mutation_config("mut_unknown", &val);
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("unknown field"));
    }
}
