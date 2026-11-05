//! Configuration Types and Bitmask Operations for Response Header Mutation.
//!
//! Provides single-instruction ALU bitmasks (`op_mask: u8`) and typed rule containers.

use http::header::HeaderName;
use velda_sync::post_sync::plugin::PluginConfig;

use super::var::ResponseHeaderValueSpec;
use crate::error::PluginError;

// ── Operation Bitmask Constants (Single-Instruction ALU Branching) ────

/// Indicates presence of headers to remove from outgoing response.
pub const OP_REMOVE: u8 = 1 << 0;
/// Indicates presence of static header overwrite rules.
pub const OP_STATIC_SET: u8 = 1 << 1;
/// Indicates presence of static multi-value append rules.
pub const OP_STATIC_ADD: u8 = 1 << 2;
/// Indicates presence of static set-if-missing rules.
pub const OP_STATIC_SET_IF_MISSING: u8 = 1 << 3;
/// Indicates presence of dynamic header overwrite rules.
pub const OP_DYNAMIC_SET: u8 = 1 << 4;
/// Indicates presence of dynamic multi-value append rules.
pub const OP_DYNAMIC_ADD: u8 = 1 << 5;
/// Indicates presence of dynamic set-if-missing rules.
pub const OP_DYNAMIC_SET_IF_MISSING: u8 = 1 << 6;

/// Composite bitmask indicating whether any dynamic variables require runtime evaluation.
pub const OP_DYNAMIC_ANY: u8 = OP_DYNAMIC_SET | OP_DYNAMIC_ADD | OP_DYNAMIC_SET_IF_MISSING;

// ── Configuration Schema ─────────────────────────────────────────────

/// Configuration schema for [`ResponseHeaderMutationPlugin`](super::ResponseHeaderMutationPlugin).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ResponseHeaderMutationConfig {
    /// Headers to be explicitly stripped before transmitting to the downstream client.
    pub remove: Vec<HeaderName>,
    /// Headers to be set or overwritten.
    pub set: Vec<(HeaderName, ResponseHeaderValueSpec)>,
    /// Headers to be appended to existing values (multi-value headers).
    pub add: Vec<(HeaderName, ResponseHeaderValueSpec)>,
    /// Headers to be set ONLY if the response does not already contain them.
    pub set_if_missing: Vec<(HeaderName, ResponseHeaderValueSpec)>,
}

impl ResponseHeaderMutationConfig {
    /// Deserializes and constructs an instance directly from a [`PluginConfig`].
    pub fn from_plugin_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let cfg = velda_sync::validate_response_header_mutation_config(&config.id, &config.config)
            .map_err(|e| {
                PluginError::Config(format!(
                    "Invalid response_header_mutation configuration for '{}': {e}",
                    config.id
                ))
            })?;

        let mut remove = Vec::with_capacity(cfg.remove_headers.len());
        for h in &cfg.remove_headers {
            let name = HeaderName::from_bytes(h.as_bytes()).map_err(|e| {
                PluginError::Config(format!(
                    "Invalid remove header name '{h}' in plugin '{}': {e}",
                    config.id
                ))
            })?;
            remove.push(name);
        }

        let mut set = Vec::with_capacity(cfg.set_headers.len());
        for (k, v) in &cfg.set_headers {
            let name = HeaderName::from_bytes(k.as_bytes()).map_err(|e| {
                PluginError::Config(format!(
                    "Invalid set header name '{k}' in plugin '{}': {e}",
                    config.id
                ))
            })?;
            let spec = ResponseHeaderValueSpec::parse(v)?;
            set.push((name, spec));
        }

        let mut add = Vec::with_capacity(cfg.append_headers.len());
        for (k, v) in &cfg.append_headers {
            let name = HeaderName::from_bytes(k.as_bytes()).map_err(|e| {
                PluginError::Config(format!(
                    "Invalid append header name '{k}' in plugin '{}': {e}",
                    config.id
                ))
            })?;
            let spec = ResponseHeaderValueSpec::parse(v)?;
            add.push((name, spec));
        }

        let mut set_if_missing = Vec::with_capacity(cfg.set_if_missing.len());
        for (k, v) in &cfg.set_if_missing {
            let name = HeaderName::from_bytes(k.as_bytes()).map_err(|e| {
                PluginError::Config(format!(
                    "Invalid set_if_missing header name '{k}' in plugin '{}': {e}",
                    config.id
                ))
            })?;
            let spec = ResponseHeaderValueSpec::parse(v)?;
            set_if_missing.push((name, spec));
        }

        Ok(Self {
            remove,
            set,
            add,
            set_if_missing,
        })
    }
}
