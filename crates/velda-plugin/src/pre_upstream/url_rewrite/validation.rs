//! URL Rewrite Configuration Validation (`url_rewrite::validation`).
//!
//! ```text
//!   serde_json::Value
//!     └──► serde(deny_unknown_fields) -> UrlRewriteConfig
//!            ├── non-empty effective policy (path_rules | drop | replace | remove_params | query_append)
//!            ├── path_rules[]: non-empty unique `find`, valid URI path bytes (no '?'/'#'/ctrl)
//!            └── query policy parity:
//!                  ├─ drop    -> forbids query_replace, remove_params, query_append
//!                  ├─ replace -> requires non-empty query_replace; forbids remove_params, query_append
//!                  └─ keep    -> forbids query_replace; validates remove_params keys & query_append
//! ```

use std::collections::HashSet;

use http::uri::PathAndQuery;

pub use super::config::{PathRewriteRule, QueryRewriteMode, UrlRewriteConfig};
use crate::error::PluginError;

#[inline]
fn validate_path_fragment(plugin_id: &str, field: &str, s: &str) -> Result<(), PluginError> {
    for b in s.bytes() {
        if b <= 0x20 || b == 0x7F || b == b'?' || b == b'#' || b == b'\\' {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': '{field}' contains invalid URI path byte 0x{b:02X}"
            )));
        }
    }
    if !s.is_empty() {
        let probe = if s.starts_with('/') {
            s.to_string()
        } else {
            format!("/{s}")
        };
        if probe.parse::<PathAndQuery>().is_err() {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': '{field}' ('{s}') is not valid in an HTTP URI path"
            )));
        }
    }
    Ok(())
}

#[inline]
fn validate_query_payload(plugin_id: &str, field: &str, q: &str) -> Result<(), PluginError> {
    if q.is_empty() {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': '{field}' cannot be empty when specified"
        )));
    }
    if q.starts_with('?') || q.contains('#') {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': '{field}' must not start with '?' or contain '#'"
        )));
    }
    if q.bytes().any(|b| b <= 0x20 || b == 0x7F) {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': '{field}' contains whitespace or ASCII control characters"
        )));
    }
    if format!("/?{q}").parse::<PathAndQuery>().is_err() {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': '{field}' ('{q}') is not a valid URI query string"
        )));
    }
    Ok(())
}

/// Validates a typed [`UrlRewriteConfig`] shared by sync validation and plugin compilation.
pub(crate) fn validate_config_fields(
    plugin_id: &str,
    cfg: &UrlRewriteConfig,
) -> Result<(), PluginError> {
    if cfg.path_rules.is_empty()
        && cfg.query_mode == QueryRewriteMode::Keep
        && cfg.remove_params.is_empty()
        && cfg.query_append.is_none()
    {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': url_rewrite must declare at least one path rule or query transformation"
        )));
    }

    let mut seen_finds = HashSet::with_capacity(cfg.path_rules.len());
    for (idx, rule) in cfg.path_rules.iter().enumerate() {
        if rule.find.is_empty() {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': path_rules[{idx}].find cannot be empty"
            )));
        }
        if !seen_finds.insert(rule.find.as_str()) {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': duplicate path_rules.find '{}'",
                rule.find
            )));
        }
        validate_path_fragment(plugin_id, "path_rules.find", &rule.find)?;
        validate_path_fragment(plugin_id, "path_rules.replace", &rule.replace)?;
    }

    match cfg.query_mode {
        QueryRewriteMode::Drop => {
            if cfg.query_replace.is_some()
                || !cfg.remove_params.is_empty()
                || cfg.query_append.is_some()
            {
                return Err(PluginError::Config(format!(
                    "Plugin '{plugin_id}': query_mode 'drop' forbids query_replace, remove_params, and query_append"
                )));
            }
        }
        QueryRewriteMode::Replace => {
            if !cfg.remove_params.is_empty() || cfg.query_append.is_some() {
                return Err(PluginError::Config(format!(
                    "Plugin '{plugin_id}': query_mode 'replace' forbids remove_params and query_append"
                )));
            }
            let rep = cfg.query_replace.as_deref().ok_or_else(|| {
                PluginError::Config(format!(
                    "Plugin '{plugin_id}': query_mode 'replace' requires query_replace"
                ))
            })?;
            validate_query_payload(plugin_id, "query_replace", rep)?;
        }
        QueryRewriteMode::Keep => {
            if cfg.query_replace.is_some() {
                return Err(PluginError::Config(format!(
                    "Plugin '{plugin_id}': query_mode 'keep' forbids query_replace"
                )));
            }
            let mut seen_params = HashSet::with_capacity(cfg.remove_params.len());
            for key in &cfg.remove_params {
                if key.is_empty() {
                    return Err(PluginError::Config(format!(
                        "Plugin '{plugin_id}': remove_params cannot contain an empty key"
                    )));
                }
                if key
                    .bytes()
                    .any(|b| b <= 0x20 || b == 0x7F || matches!(b, b'&' | b'=' | b'?' | b'#'))
                {
                    return Err(PluginError::Config(format!(
                        "Plugin '{plugin_id}': invalid query parameter name '{key}' in remove_params"
                    )));
                }
                if !seen_params.insert(key.as_str()) {
                    return Err(PluginError::Config(format!(
                        "Plugin '{plugin_id}': duplicate query parameter '{key}' in remove_params"
                    )));
                }
            }
            if let Some(append) = &cfg.query_append {
                if append.starts_with('&') || append.ends_with('&') {
                    return Err(PluginError::Config(format!(
                        "Plugin '{plugin_id}': query_append must not start or end with '&'"
                    )));
                }
                validate_query_payload(plugin_id, "query_append", append)?;
            }
        }
    }

    Ok(())
}

/// Validates and parses JSON config for `url_rewrite`.
pub fn validate_url_rewrite_config(
    plugin_id: &str,
    config_val: &serde_json::Value,
) -> Result<UrlRewriteConfig, PluginError> {
    let cfg: UrlRewriteConfig = serde_json::from_value(config_val.clone()).map_err(|e| {
        PluginError::Config(format!(
            "Plugin '{plugin_id}' has malformed or incomplete url_rewrite config: {e}"
        ))
    })?;

    validate_config_fields(plugin_id, &cfg)?;
    Ok(cfg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_valid_url_rewrite_configs() {
        let val = json!({
            "path_rules": [
                { "find": "/api/v1/", "replace": "/" },
                { "find": "/legacy", "replace": "/v2" }
            ],
            "query_mode": "keep",
            "query_replace": null,
            "remove_params": ["utm_source", "debug"],
            "query_append": "edge=velda"
        });
        assert!(validate_url_rewrite_config("rw_1", &val).is_ok());

        let replace_q = json!({
            "path_rules": [],
            "query_mode": "replace",
            "query_replace": "v=2",
            "remove_params": [],
            "query_append": null
        });
        assert!(validate_url_rewrite_config("rw_2", &replace_q).is_ok());
    }

    #[test]
    fn test_rejects_unknown_fields_and_noop_and_invalid_rules() {
        let unknown = json!({
            "path_rules": [],
            "query_mode": "drop",
            "query_replace": null,
            "remove_params": [],
            "query_append": null,
            "sanitize_path": true
        });
        assert!(validate_url_rewrite_config("rw_unknown", &unknown).is_err());

        let noop = json!({
            "path_rules": [],
            "query_mode": "keep",
            "query_replace": null,
            "remove_params": [],
            "query_append": null
        });
        assert!(validate_url_rewrite_config("rw_noop", &noop).is_err());

        let dup_find = json!({
            "path_rules": [
                { "find": "/a", "replace": "/b" },
                { "find": "/a", "replace": "/c" }
            ],
            "query_mode": "keep",
            "query_replace": null,
            "remove_params": [],
            "query_append": null
        });
        assert!(validate_url_rewrite_config("rw_dup", &dup_find).is_err());

        let bad_drop = json!({
            "path_rules": [],
            "query_mode": "drop",
            "query_replace": null,
            "remove_params": ["a"],
            "query_append": null
        });
        assert!(validate_url_rewrite_config("rw_bad_drop", &bad_drop).is_err());

        let bad_query = json!({
            "path_rules": [],
            "query_mode": "replace",
            "query_replace": "?foo=bar",
            "remove_params": [],
            "query_append": null
        });
        assert!(validate_url_rewrite_config("rw_bad_q", &bad_query).is_err());
    }
}
