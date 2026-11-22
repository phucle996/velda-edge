//! WAF Lite Configuration Schema & Semantic Validation.
//!
//! ```text
//!  +─────────────────────────────────────────────────────────────────────────+
//!  |                    WAF_LITE DECLARATIVE VALIDATION                      |
//!  +─────────────────────────────────────────────────────────────────────────+
//!  |                                                                         |
//!  |  JSON Config ─► Deserialize WafLiteConfig { patterns }                  |
//!  |                      │                                                  |
//!  |                      └─ Validate, lowercase & deduplicate patterns      |
//!  |                                                                         |
//!  |  Zero built-in dictionaries: only user-configured patterns are kept.    |
//!  +─────────────────────────────────────────────────────────────────────────+
//! ```

use super::config::WafLiteConfig;
use crate::error::PluginError;

/// Validates WAF Lite configuration strictly and deduplicates configured patterns.
pub fn validate_waf_lite_config(
    plugin_id: &str,
    config_val: &serde_json::Value,
) -> Result<WafLiteConfig, PluginError> {
    let mut cfg: WafLiteConfig = serde_json::from_value(config_val.clone()).map_err(|e| {
        PluginError::Config(format!(
            "Plugin '{plugin_id}' has malformed or incomplete waf_lite config: {e}"
        ))
    })?;

    let mut deduped = Vec::with_capacity(cfg.patterns.len());
    for pat in &cfg.patterns {
        let lower = pat.trim().to_ascii_lowercase();
        if lower.is_empty() {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': patterns cannot contain empty or whitespace-only entries"
            )));
        }
        deduped.push(lower);
    }
    deduped.sort_unstable();
    deduped.dedup();
    cfg.patterns = deduped;

    Ok(cfg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_valid_config_normalizes_and_deduplicates() {
        let raw = json!({
            "patterns": ["../", "  UNION SELECT  ", "union select", "<Script"]
        });

        let cfg = validate_waf_lite_config("waf_lite", &raw).unwrap();
        assert_eq!(cfg.patterns, vec!["../", "<script", "union select"]);
    }

    #[test]
    fn test_rejects_empty_pattern_entry() {
        let raw = json!({
            "patterns": ["../", "   "]
        });
        assert!(validate_waf_lite_config("waf_lite", &raw).is_err());
    }
}
