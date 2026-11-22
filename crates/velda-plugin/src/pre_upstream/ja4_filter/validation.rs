//! Schema & semantic validation for `ja4_filter` (`pre_upstream`).
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────────────────────┐
//! │                     JA4 FILTER VALIDATION FLOW                                          │
//! ├─────────────────────────────────────────────────────────────────────────────────────────┤
//! │  Raw JSON Value                                                                         │
//! │    ├── Deserialize into Ja4FilterConfig (#[serde(deny_unknown_fields)])                 │
//! │    ├── Validate each deny entry via parse_ja4_entry()                                   │
//! │    ├── Reject empty deny list (a filter with 0 rules is a misconfiguration)             │
//! │    └── Deduplicate & return normalized JSON                                             │
//! └─────────────────────────────────────────────────────────────────────────────────────────┘
//! ```

use serde_json::Value;

use crate::error::PluginError;
use crate::pre_upstream::ja4_filter::config::{Ja4FilterConfig, parse_ja4_entry};

/// Validates and normalizes `ja4_filter` plugin configuration.
pub fn validate_ja4_filter_config(
    plugin_id: &str,
    raw_config: &Value,
) -> Result<Value, PluginError> {
    let mut cfg: Ja4FilterConfig = serde_json::from_value(raw_config.clone()).map_err(|e| {
        PluginError::Config(format!(
            "Invalid configuration for plugin '{plugin_id}' (ja4_filter): {e}"
        ))
    })?;

    if cfg.deny.is_empty() {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}' (ja4_filter) requires at least one entry in 'deny'"
        )));
    }

    for entry in &mut cfg.deny {
        let trimmed = entry.trim().to_string();
        parse_ja4_entry(&trimmed)
            .map_err(|e| PluginError::Config(format!("Plugin '{plugin_id}' (ja4_filter): {e}")))?;
        *entry = trimmed;
    }

    cfg.deny.sort_unstable();
    cfg.deny.dedup();

    serde_json::to_value(&cfg).map_err(|e| {
        PluginError::Config(format!(
            "Failed to serialize normalized config for '{plugin_id}': {e}"
        ))
    })
}
