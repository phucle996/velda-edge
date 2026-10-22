//! W3C TraceContext Schema & Semantic Validation (`phase: "pre_upstream"`).

use serde::{Deserialize, Serialize};

use crate::SyncError;

/// Declarative configuration for the W3C TraceContext plugin.
///
/// # Strict Invariant: 100% Explicit Declaration
/// Every parameter must be explicitly declared in the configuration JSON.
/// Zero defaults are permitted. Missing fields fail fast at configuration sync time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct W3cTraceContextConfig {
    /// Whether to generate a new W3C `traceparent` header if missing from downstream request.
    pub generate_if_missing: bool,
    /// Whether to forward the optional `tracestate` header unmodified to upstream.
    pub forward_tracestate: bool,
    /// Default sampled flag to apply if generating a new trace (`true` = sampled `01`, `false` = `00`).
    pub sampled: bool,
}

/// Validates W3C TraceContext configuration strictly according to production standards.
pub fn validate_w3c_trace_context_config(
    plugin_id: &str,
    config_val: &serde_json::Value,
) -> Result<W3cTraceContextConfig, SyncError> {
    let cfg: W3cTraceContextConfig =
        serde_json::from_value(config_val.clone()).map_err(|e| SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}' has malformed or incomplete w3c_trace_context config: {e}"
            ),
        })?;

    Ok(cfg)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_w3c_trace_context_config() {
        let val = serde_json::json!({
            "generate_if_missing": true,
            "forward_tracestate": false,
            "sampled": true
        });
        let res = validate_w3c_trace_context_config("w3c", &val);
        assert!(res.is_ok());
    }

    #[test]
    fn test_missing_field_fails() {
        let val = serde_json::json!({
            "generate_if_missing": true,
            "forward_tracestate": false
        });
        let res = validate_w3c_trace_context_config("w3c", &val);
        assert!(res.is_err(), "Must fail when sampled is omitted");
    }
}
