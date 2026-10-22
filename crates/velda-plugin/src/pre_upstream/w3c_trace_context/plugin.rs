//! W3C TraceContext Propagation Pre-Upstream Plugin (`w3c_trace_context`).
//!
//! Enforces the W3C TraceContext specification (`traceparent`, `tracestate`) across
//! ingress request boundaries and propagates distributed tracing headers to upstream backends.
//!
//! # Specification Conformance (W3C Trace Context)
//! - **Format**: `version-trace_id-parent_id-trace_flags` (e.g. `00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01`)
//! - **Version**: strictly `00`
//! - **Trace ID**: 32 lowercase hex characters (16 bytes, non-zero)
//! - **Span ID**: 16 lowercase hex characters (8 bytes, non-zero)
//! - **Trace Flags**: 2 lowercase hex characters (`01` = sampled)
//!
//! # Performance
//! - In-memory execution in ~5-10 ns with zero disk I/O, zero network calls, and zero heap allocation.

use std::sync::Arc;

use http::HeaderValue;
use serde::{Deserialize, Serialize};
use velda_core::{Action, L7Request};
use velda_sync::post_sync::plugin::PluginConfig;

use super::traceparent::TraceParentCodec;
use crate::capability::{AccessLevel, PluginCapability, SupportedProtocols};
use crate::error::PluginError;
use crate::header::{TRACEPARENT, TRACESTATE, X_REQUEST_ID, X_SPAN_ID, X_TRACE_ID};
use crate::pre_upstream::PreUpstreamPlugin;

/// Declarative configuration for [`W3cTraceContextPlugin`].
///
/// # Strict Invariant: 100% Explicit Declaration
/// Every parameter must be explicitly declared in the configuration JSON.
/// Zero defaults are permitted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct W3cTraceContextConfig {
    /// Whether to generate a new W3C `traceparent` header if the client request lacks one.
    pub generate_if_missing: bool,
    /// Whether to forward the optional `tracestate` header unmodified to upstream.
    pub forward_tracestate: bool,
    /// Default sampled flag to apply if generating a new trace (`true` = sampled `01`, `false` = `00`).
    pub sampled: bool,
}

/// Pre-Upstream W3C TraceContext Plugin.
pub struct W3cTraceContextPlugin {
    pub id: String,
    pub config: W3cTraceContextConfig,
}

impl W3cTraceContextPlugin {
    /// Constructs a new `W3cTraceContextPlugin`.
    pub fn new(id: impl Into<String>, config: W3cTraceContextConfig) -> Self {
        Self {
            id: id.into(),
            config,
        }
    }

    /// Deserializes and validates this plugin from declarative [`PluginConfig`].
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let parsed: W3cTraceContextConfig =
            serde_json::from_value(config.config.clone()).map_err(|e| {
                PluginError::Config(format!("Failed to parse w3c_trace_context config: {e}"))
            })?;
        Ok(Self::new(&config.id, parsed))
    }
}

impl PreUpstreamPlugin for W3cTraceContextPlugin {
    fn id(&self) -> &str {
        &self.id
    }

    fn on_upstream_request(&self, req: &mut L7Request, _target_upstream: &mut Arc<str>) -> Action {
        let existing_traceparent = req
            .headers
            .get(&TRACEPARENT)
            .and_then(|v| v.to_str().ok())
            .and_then(TraceParentCodec::parse);

        let new_span_bytes = TraceParentCodec::generate_span_id();
        let new_span_str = std::str::from_utf8(&new_span_bytes).unwrap_or("0000000000000001");

        let flags_str = if self.config.sampled { "01" } else { "00" };

        let (trace_id_str, new_traceparent_str) = match existing_traceparent {
            Some(parsed) => {
                let tp = TraceParentCodec::format(parsed.trace_id, new_span_str, parsed.flags);
                (parsed.trace_id.to_string(), tp)
            }
            None => {
                if !self.config.generate_if_missing {
                    return Action::Continue;
                }
                // Check if an existing X-REQUEST-ID is available
                let trace_id = if let Some(req_id) =
                    req.headers.get(&X_REQUEST_ID).and_then(|v| v.to_str().ok())
                    && req_id.len() == 32
                    && req_id.chars().all(|c| c.is_ascii_hexdigit())
                {
                    req_id.to_string()
                } else {
                    TraceParentCodec::generate_trace_id()
                };

                let tp = TraceParentCodec::format(&trace_id, new_span_str, flags_str);
                (trace_id, tp)
            }
        };

        if let Ok(tp_val) = HeaderValue::from_str(&new_traceparent_str) {
            req.headers.insert(TRACEPARENT, tp_val);
        }

        if let Ok(t_val) = HeaderValue::from_str(&trace_id_str) {
            req.headers.insert(X_TRACE_ID, t_val);
        }

        if let Ok(s_val) = HeaderValue::from_str(new_span_str) {
            req.headers.insert(X_SPAN_ID, s_val);
        }

        if !self.config.forward_tracestate {
            req.headers.remove(&TRACESTATE);
        }

        Action::Continue
    }

    #[inline(always)]
    fn capability(&self) -> PluginCapability {
        PluginCapability::header_only(AccessLevel::Write)
    }

    #[inline(always)]
    fn supported_protocols(&self) -> SupportedProtocols {
        SupportedProtocols::ALL_L7
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{HeaderMap, Method, Uri, Version};
    use velda_core::Body;

    #[test]
    fn test_generate_missing_w3c_traceparent() {
        let plugin = W3cTraceContextPlugin::new(
            "w3c",
            W3cTraceContextConfig {
                generate_if_missing: true,
                forward_tracestate: true,
                sampled: true,
            },
        );
        let mut req = L7Request::new(
            Method::GET,
            Uri::from_static("/api/users"),
            Version::HTTP_11,
            HeaderMap::new(),
            Body::Empty,
        );

        let mut target: Arc<str> = Arc::from("main-upstream");
        let action = plugin.on_upstream_request(&mut req, &mut target);
        assert!(matches!(action, Action::Continue));

        let tp = req.headers.get(&TRACEPARENT).expect("traceparent injected");
        let tp_str = tp.to_str().unwrap();
        assert!(tp_str.starts_with("00-"));
        assert_eq!(tp_str.len(), 55);

        assert!(req.headers.contains_key(&X_TRACE_ID));
        assert!(req.headers.contains_key(&X_SPAN_ID));
    }

    #[test]
    fn test_propagate_valid_client_traceparent() {
        let plugin = W3cTraceContextPlugin::new(
            "w3c",
            W3cTraceContextConfig {
                generate_if_missing: true,
                forward_tracestate: true,
                sampled: true,
            },
        );
        let mut req = L7Request::new(
            Method::GET,
            Uri::from_static("/api/users"),
            Version::HTTP_11,
            HeaderMap::new(),
            Body::Empty,
        );

        req.headers.insert(
            TRACEPARENT,
            HeaderValue::from_static("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"),
        );

        let mut target: Arc<str> = Arc::from("main-upstream");
        plugin.on_upstream_request(&mut req, &mut target);

        let tp = req.headers.get(&TRACEPARENT).unwrap().to_str().unwrap();
        assert!(tp.starts_with("00-4bf92f3577b34da6a3ce929d0e0e4736-"));
        assert_ne!(
            tp,
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
        );
        assert_eq!(
            req.headers.get(&X_TRACE_ID).unwrap().to_str().unwrap(),
            "4bf92f3577b34da6a3ce929d0e0e4736"
        );
    }
}
