//! Semantic Configuration Validation for `bot_detection` (`pre_upstream`).

use serde_json::Value;

use crate::error::PluginError;
use crate::pre_upstream::bot_detection::compiler::compile_ruleset;
use crate::pre_upstream::bot_detection::config::{
    BotDetectionConfig, parse_strict_bot_detection_config,
};

/// Upper cap (10 seconds) preventing accidental Slowloris-like socket exhaustion via tarpit delay.
pub const MAX_SAFE_TARPIT_DELAY_MS: u64 = 10_000;

/// Validates `thresholds` and `escalation` invariants cleanly (shared by both `validation` and `from_config`).
pub(crate) fn validate_thresholds_and_escalation(
    plugin_id: &str,
    cfg: &BotDetectionConfig,
) -> Result<(), PluginError> {
    let th = &cfg.thresholds;
    if let Some(block) = th.block_score
        && (block == 0 || block > 100)
    {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}' (bot_detection): 'thresholds.block_score' must be in 1..=100"
        )));
    }

    match (th.delay_score, th.min_delay_ms, th.max_delay_ms) {
        (Some(ds), Some(min_d), Some(max_d)) => {
            if ds == 0 || ds > 100 {
                return Err(PluginError::Config(format!(
                    "Plugin '{plugin_id}' (bot_detection): 'thresholds.delay_score' must be in 1..=100"
                )));
            }
            if let Some(block) = th.block_score
                && ds >= block
            {
                return Err(PluginError::Config(format!(
                    "Plugin '{plugin_id}' (bot_detection): 'thresholds.delay_score' ({ds}) must be < 'thresholds.block_score' ({block})"
                )));
            }
            if min_d == 0 || max_d < min_d || max_d > MAX_SAFE_TARPIT_DELAY_MS {
                return Err(PluginError::Config(format!(
                    "Plugin '{plugin_id}' (bot_detection): 'min_delay_ms'/'max_delay_ms' must satisfy 1 <= min_delay_ms <= max_delay_ms <= {MAX_SAFE_TARPIT_DELAY_MS}"
                )));
            }
        }
        (None, None, None) => {
            if th.block_score.is_none() {
                return Err(PluginError::Config(format!(
                    "Plugin '{plugin_id}' (bot_detection): at least one of 'thresholds.delay_score' or 'thresholds.block_score' must be set"
                )));
            }
        }
        _ => {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}' (bot_detection): 'delay_score', 'min_delay_ms', and 'max_delay_ms' must either all be non-null or all be null"
            )));
        }
    }

    if let Some(esc) = &cfg.escalation {
        if th.delay_score.is_none() {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}' (bot_detection): 'escalation' requires non-null 'thresholds.delay_score'"
            )));
        }
        if esc.window_secs == 0 || esc.window_secs > 3600 {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}' (bot_detection): 'escalation.window_secs' must be in 1..=3600"
            )));
        }
        if esc.decay_secs == 0 || esc.decay_secs > esc.window_secs {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}' (bot_detection): 'escalation.decay_secs' must be in 1..=window_secs"
            )));
        }
        if esc.max_entries < 256 || esc.max_entries > 1_048_576 {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}' (bot_detection): 'escalation.max_entries' must be in 256..=1_048_576"
            )));
        }
        if let Some(sb) = esc.strike_block_threshold
            && (sb == 0 || sb > 15)
        {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}' (bot_detection): 'escalation.strike_block_threshold' must be in 1..=15"
            )));
        }
    }

    Ok(())
}

/// Validates full `bot_detection` configuration for config staging and hot-reload.
pub fn validate_bot_detection_config(
    plugin_id: &str,
    raw_config: &Value,
) -> Result<Value, PluginError> {
    let cfg = parse_strict_bot_detection_config(plugin_id, raw_config)?;
    validate_thresholds_and_escalation(plugin_id, &cfg)?;
    compile_ruleset(plugin_id, &cfg)?;

    serde_json::to_value(&cfg).map_err(|e| {
        PluginError::Config(format!(
            "Failed to serialize validated config for '{plugin_id}' (bot_detection): {e}"
        ))
    })
}
