//! URL Rewrite Configuration Schema & Semantic Validation (`phase: "pre_upstream"`).
//!
//! Enforces a strict declarative contract for path and query string transformation.
//!
//! # Strict Declarative Contract (Zero Silent Fallbacks)
//! - **Valid Path Mode**: `path_rewrite_mode` must be one of `"prefix"`, `"regex"`, `"exact"`, or `"none"`.
//! - **Valid Query Mode**: `query_rewrite_mode` must be one of `"keep"`, `"strip"`, `"replace"`, or `"append"`.
//! - **Leading Slash Invariant**: `path_replacement` must start with `/` when path rewrite is active.

use serde::{Deserialize, Serialize};

use crate::SyncError;

/// Declarative configuration for the URL Rewrite plugin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UrlRewriteConfig {
    /// Path rewrite strategy: "prefix", "regex", "exact", or "none".
    pub path_rewrite_mode: String,
    /// Path match prefix or regex pattern (empty string if path_rewrite_mode is "none").
    pub path_match: String,
    /// Path replacement string (empty string if path_rewrite_mode is "none").
    pub path_replacement: String,
    /// Query string strategy: "keep", "strip", "replace", or "append".
    pub query_rewrite_mode: String,
    /// Query replacement parameter string when query_rewrite_mode is "replace" or "append".
    pub query_replacement: Option<String>,
    /// Whether to canonicalize and sanitize path (remove redundant slashes, resolve dot segments).
    pub sanitize_path: bool,
}

fn validate_regex_brackets(pattern: &str) -> Result<(), String> {
    let mut paren_count: i32 = 0;
    let mut in_escape = false;

    for c in pattern.chars() {
        if in_escape {
            in_escape = false;
            continue;
        }
        match c {
            '\\' => in_escape = true,
            '(' => paren_count += 1,
            ')' => {
                paren_count -= 1;
                if paren_count < 0 {
                    return Err("unmatched closing parenthesis".to_string());
                }
            }
            _ => {}
        }
    }

    if paren_count != 0 {
        return Err("unclosed parenthesis".to_string());
    }
    Ok(())
}

/// Validates URL Rewrite configuration strictly according to production standards.
pub fn validate_url_rewrite_config(
    plugin_id: &str,
    config_val: &serde_json::Value,
) -> Result<UrlRewriteConfig, SyncError> {
    let cfg: UrlRewriteConfig =
        serde_json::from_value(config_val.clone()).map_err(|e| SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}' has malformed or incomplete url_rewrite config: {e}"
            ),
        })?;

    // 1. Validate path_rewrite_mode
    let path_mode_lower = cfg.path_rewrite_mode.trim().to_ascii_lowercase();
    match path_mode_lower.as_str() {
        "prefix" | "regex" | "exact" | "none" => {}
        other => {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': invalid path_rewrite_mode '{other}' (expected 'prefix', 'regex', 'exact', or 'none')"
                ),
            });
        }
    }

    if path_mode_lower != "none" {
        let rep_trimmed = cfg.path_replacement.trim();
        if !rep_trimmed.starts_with('/') {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': path_replacement '{rep_trimmed}' must start with '/'"
                ),
            });
        }

        if path_mode_lower == "regex"
            && let Err(e) = validate_regex_brackets(&cfg.path_match)
        {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!("Plugin '{plugin_id}': invalid regex pattern in path_match: {e}"),
            });
        }
    }

    // 2. Validate query_rewrite_mode
    let query_mode_lower = cfg.query_rewrite_mode.trim().to_ascii_lowercase();
    match query_mode_lower.as_str() {
        "keep" | "strip" | "replace" | "append" => {}
        other => {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': invalid query_rewrite_mode '{other}' (expected 'keep', 'strip', 'replace', or 'append')"
                ),
            });
        }
    }

    if (query_mode_lower == "replace" || query_mode_lower == "append")
        && cfg
            .query_replacement
            .as_deref()
            .unwrap_or("")
            .trim()
            .is_empty()
    {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': query_rewrite_mode '{query_mode_lower}' requires non-empty query_replacement"
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
    fn test_valid_prefix_rewrite() {
        let val = json!({
            "path_rewrite_mode": "prefix",
            "path_match": "/v1/api",
            "path_replacement": "/internal/api",
            "query_rewrite_mode": "keep",
            "query_replacement": null,
            "sanitize_path": true
        });
        let res = validate_url_rewrite_config("rewrite_1", &val);
        assert!(res.is_ok());
    }

    #[test]
    fn test_invalid_path_replacement_no_slash_rejected() {
        let val = json!({
            "path_rewrite_mode": "exact",
            "path_match": "/old",
            "path_replacement": "new_without_slash",
            "query_rewrite_mode": "strip",
            "query_replacement": null,
            "sanitize_path": true
        });
        let res = validate_url_rewrite_config("rewrite_bad_slash", &val);
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("must start with '/'"));
    }

    #[test]
    fn test_missing_query_replacement_rejected() {
        let val = json!({
            "path_rewrite_mode": "none",
            "path_match": "",
            "path_replacement": "",
            "query_rewrite_mode": "replace",
            "query_replacement": "",
            "sanitize_path": false
        });
        let res = validate_url_rewrite_config("rewrite_bad_query", &val);
        assert!(res.is_err());
        assert!(
            res.unwrap_err()
                .to_string()
                .contains("requires non-empty query_replacement")
        );
    }

    #[test]
    fn test_missing_required_fields_rejected() {
        let val = json!({
            "path_rewrite_mode": "prefix"
            // Missing path_match, path_replacement, query_rewrite_mode, sanitize_path
        });
        let res = validate_url_rewrite_config("rewrite_missing", &val);
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("missing field"));
    }
}
