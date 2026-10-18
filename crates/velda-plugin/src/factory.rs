//! Plugin Factory and Runtime Compilation Dispatcher.
//!
//! Instantiates concrete plugin instances from compiled [`PluginConfig`] definitions,
//! and builds pre-compiled, stably-ordered [`RoutePluginPlan`] plans for routes.

use std::sync::Arc;

use rustc_hash::FxHashMap;
use velda_core::{ProtocolFamily, StreamingMode};
use velda_sync::post_sync::plugin::PluginConfig;

use crate::capability::{AccessLevel, PluginCapability, SupportedProtocols};
use crate::error::PluginError;
use crate::plan::{RoutePluginPlan, StablyOrderedPlugin, sort_plugins_stable};
use crate::post_response::PostResponsePlugin;
use crate::pre_route::PreRoutePlugin;
use crate::pre_upstream::PreUpstreamPlugin;

/// Type-erased, phase-differentiated container for a loaded plugin instance.
#[derive(Clone)]
pub enum LoadedPlugin {
    PreRoute(Arc<dyn PreRoutePlugin>),
    PreUpstream(Arc<dyn PreUpstreamPlugin>),
    PostResponse(Arc<dyn PostResponsePlugin>),
    Background { id: String },
}

impl LoadedPlugin {
    /// Returns the unique instance identifier of this plugin.
    pub fn id(&self) -> &str {
        match self {
            Self::PreRoute(p) => p.id(),
            Self::PreUpstream(p) => p.id(),
            Self::PostResponse(p) => p.id(),
            Self::Background { id } => id,
        }
    }

    /// Returns the reverse-permission capabilities declared by this plugin.
    pub fn capability(&self) -> PluginCapability {
        match self {
            Self::PreRoute(p) => p.capability(),
            Self::PreUpstream(p) => p.capability(),
            Self::PostResponse(p) => p.capability(),
            Self::Background { .. } => PluginCapability::default(),
        }
    }

    /// Returns the hardcoded network/application protocols supported by this plugin.
    pub fn supported_protocols(&self) -> SupportedProtocols {
        match self {
            Self::PreRoute(p) => p.supported_protocols(),
            Self::PreUpstream(p) => p.supported_protocols(),
            Self::PostResponse(p) => p.supported_protocols(),
            Self::Background { .. } => SupportedProtocols::empty(),
        }
    }
}

/// Returns the expected canonical execution phase for a known plugin identifier.
///
/// Used for fail-fast PhaseMismatch validation prior to sub-dispatcher delegation.
pub fn canonical_phase_for_id(id: &str) -> Option<&'static str> {
    velda_sync::post_sync::plugin::canonical_phase_for_id(id)
}

/// Instantiates a concrete plugin instance by dispatching to the dedicated phase sub-dispatcher.
///
/// # Architecture & Open-Closed Principle (OCP)
/// Instead of a monolithic "God Factory" matching every plugin inline and parsing raw JSON,
/// this dispatcher delegates strictly by `config.phase`:
/// - `pre_route` plugins -> [`crate::pre_route::instantiate`]
/// - `pre_upstream` plugins -> [`crate::pre_upstream::instantiate`]
/// - `post_response` plugins -> [`crate::post_response::instantiate`]
/// - `background` plugins -> [`crate::background::instantiate`]
///
/// Each concrete plugin defines its own `from_config` constructor, enforcing strict module
/// ownership and ensuring `factory.rs` never bloats as new plugins are added.
pub fn instantiate_plugin(config: &PluginConfig) -> Result<LoadedPlugin, PluginError> {
    let effective_phase = if config.phase.is_empty() {
        canonical_phase_for_id(&config.id).unwrap_or("")
    } else {
        config.phase.as_str()
    };

    // 1. Fail-fast PhaseMismatch validation if a known plugin is configured under the wrong phase
    if let Some(expected_phase) = canonical_phase_for_id(&config.id)
        && !config.phase.is_empty()
        && config.phase != expected_phase
    {
        return Err(PluginError::PhaseMismatch {
            plugin_id: config.id.clone(),
            expected: expected_phase.to_string(),
            actual: config.phase.clone(),
        });
    }

    // 2. Delegate to the dedicated phase sub-dispatcher
    match effective_phase {
        "pre_route" => crate::pre_route::instantiate(config).map(LoadedPlugin::PreRoute),
        "pre_upstream" => crate::pre_upstream::instantiate(config).map(LoadedPlugin::PreUpstream),
        "post_response" => {
            crate::post_response::instantiate(config).map(LoadedPlugin::PostResponse)
        }
        "background" => crate::background::instantiate(config),
        other => Err(PluginError::Config(format!(
            "Unknown plugin phase '{other}' for plugin '{}'",
            config.id
        ))),
    }
}

/// Pre-computes and compiles an optimized, stably-ordered [`RoutePluginPlan`] for a route.
///
/// # Validation and Safety Invariants:
/// 1. Unknown plugin IDs are rejected fail-fast.
/// 2. Protocol mismatch between Route and Plugin is rejected fail-fast.
/// 3. Request plugins needing body buffering on a streaming-upload route (`streaming.client == true`) are rejected.
/// 4. Response plugins needing body buffering on a streaming-download route (`streaming.server == true`) are rejected.
/// 5. Within each phase, hooks are stably sorted by descending `priority`, preserving JSON declaration order for ties.
pub fn build_route_plugin_plan(
    route_id: &str,
    route_plugin_ids: &[String],
    available_plugins: &FxHashMap<String, (LoadedPlugin, Option<i32>)>,
    route_protocol: ProtocolFamily,
    route_streaming: StreamingMode,
) -> Result<RoutePluginPlan, PluginError> {
    let mut pre_route_list: Vec<StablyOrderedPlugin<dyn PreRoutePlugin>> = Vec::new();
    let mut pre_upstream_list: Vec<StablyOrderedPlugin<dyn PreUpstreamPlugin>> = Vec::new();
    let mut post_response_list: Vec<StablyOrderedPlugin<dyn PostResponsePlugin>> = Vec::new();

    for (decl_idx, plugin_id) in route_plugin_ids.iter().enumerate() {
        let (plugin, priority) = available_plugins
            .get(plugin_id)
            .ok_or_else(|| PluginError::NotFound(plugin_id.clone()))?;

        // 1. Enforce hardcoded protocol compatibility
        let supported = plugin.supported_protocols();
        if !supported.supports_family(route_protocol) {
            return Err(PluginError::ProtocolMismatch {
                plugin_id: plugin_id.clone(),
                route_id: route_id.to_string(),
                route_protocol,
                supported,
            });
        }

        // 2. Classify by canonical phase
        match plugin {
            LoadedPlugin::PreRoute(p) => {
                pre_route_list.push(StablyOrderedPlugin::new(p.clone(), *priority, decl_idx));
            }
            LoadedPlugin::PreUpstream(p) => {
                pre_upstream_list.push(StablyOrderedPlugin::new(p.clone(), *priority, decl_idx));
            }
            LoadedPlugin::PostResponse(p) => {
                post_response_list.push(StablyOrderedPlugin::new(p.clone(), *priority, decl_idx));
            }
            LoadedPlugin::Background { .. } => {
                return Err(PluginError::PhaseMismatch {
                    plugin_id: plugin_id.clone(),
                    expected: "traffic phase (pre_route, pre_upstream, or post_response)".into(),
                    actual: "background".into(),
                });
            }
        }
    }

    // 3. Stable Sort by priority descending + declaration order
    sort_plugins_stable(&mut pre_route_list);
    sort_plugins_stable(&mut pre_upstream_list);
    sort_plugins_stable(&mut post_response_list);

    // 4. Compute aggregate permissions
    let mut req_header_access = AccessLevel::None;
    let mut req_payload_access = AccessLevel::None;

    for item in &pre_route_list {
        let cap = item.plugin.capability();
        req_header_access = req_header_access.union(cap.header);
        req_payload_access = req_payload_access.union(cap.payload);
    }

    for item in &pre_upstream_list {
        let cap = item.plugin.capability();
        req_header_access = req_header_access.union(cap.header);
        req_payload_access = req_payload_access.union(cap.payload);
    }

    let mut resp_header_access = AccessLevel::None;
    let mut resp_payload_access = AccessLevel::None;

    for item in &post_response_list {
        let cap = item.plugin.capability();
        resp_header_access = resp_header_access.union(cap.header);
        resp_payload_access = resp_payload_access.union(cap.payload);
    }

    // 5. Enforce orthogonal streaming / pipe compatibility
    if !req_payload_access.is_none() && route_streaming.client {
        return Err(PluginError::StreamingConflict {
            plugin_id: "request_phase".into(),
            route_id: route_id.to_string(),
            detail:
                "Request plugin requires payload access, but route has streaming upload enabled"
                    .into(),
        });
    }

    if !resp_payload_access.is_none() && route_streaming.server {
        return Err(PluginError::StreamingConflict {
            plugin_id: "response_phase".into(),
            route_id: route_id.to_string(),
            detail:
                "Response plugin requires payload access, but route has streaming response enabled"
                    .into(),
        });
    }

    // 6. Freeze into final Arc slices
    let pre_route_hooks: Arc<[Arc<dyn PreRoutePlugin>]> =
        pre_route_list.into_iter().map(|p| p.plugin).collect();
    let pre_upstream_hooks: Arc<[Arc<dyn PreUpstreamPlugin>]> =
        pre_upstream_list.into_iter().map(|p| p.plugin).collect();
    let post_response_hooks: Arc<[Arc<dyn PostResponsePlugin>]> =
        post_response_list.into_iter().map(|p| p.plugin).collect();

    Ok(RoutePluginPlan {
        pre_route_hooks,
        pre_upstream_hooks,
        post_response_hooks,
        request_header_access: req_header_access,
        request_payload_access: req_payload_access,
        response_header_access: resp_header_access,
        response_payload_access: resp_payload_access,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_instantiate_plugins() {
        let cfg_jwt = PluginConfig {
            id: "jwt_auth".into(),
            phase: "pre_upstream".into(),
            priority: Some(100),
            config: serde_json::json!({ "secret_key": "secret-12345678901234567890" }),
        };
        let p1 = instantiate_plugin(&cfg_jwt).expect("Should instantiate jwt");
        assert_eq!(p1.id(), "jwt_auth");
        assert!(matches!(p1, LoadedPlugin::PreUpstream(_)));

        let cfg_rate = PluginConfig {
            id: "rate_limit".into(),
            phase: "pre_upstream".into(),
            priority: Some(50),
            config: serde_json::json!({ "rate_per_second": 500 }),
        };
        let p2 = instantiate_plugin(&cfg_rate).expect("Should instantiate rate_limit");
        assert_eq!(p2.id(), "rate_limit");
        assert!(matches!(p2, LoadedPlugin::PreUpstream(_)));

        let cfg_cors = PluginConfig {
            id: "cors".into(),
            phase: "post_response".into(),
            priority: None,
            config: serde_json::json!({ "allow_origin": "https://example.com" }),
        };
        let p3 = instantiate_plugin(&cfg_cors).expect("Should instantiate cors");
        assert_eq!(p3.id(), "cors");
        assert!(matches!(p3, LoadedPlugin::PostResponse(_)));
    }

    #[test]
    fn test_build_route_plugin_plan_success() {
        let mut available = FxHashMap::default();

        let cfg_jwt = PluginConfig {
            id: "jwt_auth".into(),
            phase: "pre_upstream".into(),
            priority: Some(200),
            config: serde_json::json!({ "secret_key": "secret-12345678901234567890" }),
        };
        available.insert(
            "jwt_auth".to_string(),
            (instantiate_plugin(&cfg_jwt).unwrap(), Some(200)),
        );

        let cfg_ip = PluginConfig {
            id: "ip_filter".into(),
            phase: "pre_route".into(),
            priority: Some(100),
            config: serde_json::json!({ "mode": "allow", "allow_cidrs": ["127.0.0.1/32"] }),
        };
        available.insert(
            "ip_filter".to_string(),
            (instantiate_plugin(&cfg_ip).unwrap(), Some(100)),
        );

        let cfg_cors = PluginConfig {
            id: "cors".into(),
            phase: "post_response".into(),
            priority: None,
            config: serde_json::json!({ "allow_origin": "*" }),
        };
        available.insert(
            "cors".to_string(),
            (instantiate_plugin(&cfg_cors).unwrap(), None),
        );

        // Declaration order: jwt_auth, ip_filter, cors
        let route_plugins = vec![
            "jwt_auth".to_string(),
            "ip_filter".to_string(),
            "cors".to_string(),
        ];

        let plan = build_route_plugin_plan(
            "route-test",
            &route_plugins,
            &available,
            ProtocolFamily::Http,
            StreamingMode::DISABLED,
        )
        .expect("Should build plan");

        // pre_route hooks
        assert_eq!(plan.pre_route_hooks.len(), 1);
        assert_eq!(plan.pre_route_hooks[0].id(), "ip_filter");

        // pre_upstream hooks
        assert_eq!(plan.pre_upstream_hooks.len(), 1);
        assert_eq!(plan.pre_upstream_hooks[0].id(), "jwt_auth");

        // post_response hooks
        assert_eq!(plan.post_response_hooks.len(), 1);
        assert_eq!(plan.post_response_hooks[0].id(), "cors");

        // Zero-copy streaming active because payload access is None!
        assert!(plan.is_request_payload_passthrough());
        assert!(plan.is_response_payload_passthrough());
    }

    #[test]
    fn test_build_route_plugin_plan_rejects_missing_plugin() {
        let available = FxHashMap::default();
        let route_plugins = vec!["unknown-plugin".to_string()];

        let res = build_route_plugin_plan(
            "route-test",
            &route_plugins,
            &available,
            ProtocolFamily::Http,
            StreamingMode::DISABLED,
        );

        assert!(matches!(res, Err(PluginError::NotFound(_))));
    }

    #[test]
    fn test_build_route_plugin_plan_rejects_protocol_mismatch() {
        let mut available = FxHashMap::default();

        let cfg_cors = PluginConfig {
            id: "cors".into(),
            phase: "post_response".into(),
            priority: None,
            config: serde_json::json!({ "allow_origin": "*" }),
        };
        available.insert(
            "cors".to_string(),
            (instantiate_plugin(&cfg_cors).unwrap(), None),
        );

        // Attempting to attach HTTP-only CORS plugin to gRPC route!
        let route_plugins = vec!["cors".to_string()];

        let res = build_route_plugin_plan(
            "route-grpc-test",
            &route_plugins,
            &available,
            ProtocolFamily::Grpc,
            StreamingMode::DISABLED,
        );

        match res {
            Err(PluginError::ProtocolMismatch {
                plugin_id,
                route_protocol,
                ..
            }) => {
                assert_eq!(plugin_id, "cors");
                assert_eq!(route_protocol, ProtocolFamily::Grpc);
            }
            _ => panic!("Expected ProtocolMismatch error"),
        }
    }
}
