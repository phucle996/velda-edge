//! Canary Configuration Schema & Strict Validation (`canary::validation`).
//!
//! ```text
//! JSON Config ──► CanarySchemaConfig ──► validate_canary_config()
//!   ├─ canary_upstream : non-empty upstream pool ID
//!   ├─ ratio           : integer 1..=999 (permille: 1 = 0.1%, 250 = 25%, 999 = 99.9%)
//!   ├─ rules           : RFC 9110 header rules (`equals`|`prefix`|`suffix`|`contains`|`exists`)
//!   └─ hash_headers    : non-empty RFC 9110 header names required for multi-header hashing
//! ```

use http::HeaderName;
use serde::{Deserialize, Serialize};

use crate::error::PluginError;

/// Declarative rule matching on request headers to force canary routing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CanaryHeaderRuleConfig {
    /// Header field name in `HeaderWorkspace`.
    pub name: String,
    /// Matching operator: `"equals"`, `"prefix"`, `"suffix"`, `"contains"`, or `"exists"`.
    pub operator: String,
    /// Target byte pattern (ignored when `operator` is `"exists"`).
    pub value: String,
}

/// Declarative JSON schema for the Canary plugin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CanarySchemaConfig {
    /// Target canary upstream pool identifier.
    pub canary_upstream: String,
    /// Traffic share routed to canary in parts-per-thousand (`1..=999`).
    pub ratio: u32,
    /// Header rules forcing immediate canary routing.
    pub rules: Vec<CanaryHeaderRuleConfig>,
    /// Ordered non-empty list of RFC 9110 headers in `HeaderWorkspace` folded together for hashing.
    pub hash_headers: Vec<String>,
}

/// Validates Canary configuration strictly with zero silent fallbacks.
pub fn validate_canary_config(
    plugin_id: &str,
    config_val: &serde_json::Value,
) -> Result<CanarySchemaConfig, PluginError> {
    let cfg: CanarySchemaConfig = serde_json::from_value(config_val.clone()).map_err(|e| {
        PluginError::Config(format!(
            "Plugin '{plugin_id}' has malformed or incomplete canary config: {e}"
        ))
    })?;

    if cfg.canary_upstream.trim().is_empty() {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': canary_upstream cannot be empty"
        )));
    }

    if cfg.ratio == 0 || cfg.ratio >= 1000 {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': ratio ({}) must be between 1 and 999 (disable canary for 0, route directly to target upstream for 1000)",
            cfg.ratio
        )));
    }

    for (idx, rule) in cfg.rules.iter().enumerate() {
        if HeaderName::from_bytes(rule.name.trim().as_bytes()).is_err() {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': rule at index {idx} has invalid header name '{}'",
                rule.name
            )));
        }

        let op_lower = rule.operator.trim().to_ascii_lowercase();
        match op_lower.as_str() {
            "equals" | "prefix" | "suffix" | "contains" => {
                if rule.value.trim().is_empty() {
                    return Err(PluginError::Config(format!(
                        "Plugin '{plugin_id}': rule at index {idx} with operator '{}' requires non-empty value",
                        rule.operator
                    )));
                }
            }
            "exists" => {}
            other => {
                return Err(PluginError::Config(format!(
                    "Plugin '{plugin_id}': rule at index {idx} has invalid operator '{other}'"
                )));
            }
        }
    }

    if cfg.hash_headers.is_empty() {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': hash_headers cannot be empty (at least one RFC 9110 header is required)"
        )));
    }

    for (idx, hdr) in cfg.hash_headers.iter().enumerate() {
        if HeaderName::from_bytes(hdr.trim().as_bytes()).is_err() {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': hash_headers[{idx}] has invalid header name '{hdr}'"
            )));
        }
    }

    Ok(cfg)
}
