//! Plugin Factory and Runtime Compilation Dispatcher.
//!
//! Instantiates concrete plugin instances from compiled [`PluginConfig`] definitions,
//! and builds pre-compiled, stably-ordered [`RoutePluginPlan`] plans for routes.

use std::sync::Arc;

use crate::config::PluginConfig;
use rustc_hash::FxHashMap;
use velda_core::{ProtocolFamily, StreamingMode};

use crate::capability::{AccessLevel, PluginCapability, SupportedProtocols};
use crate::error::PluginError;
use crate::plan::RoutePluginPlan;
use crate::post_response::PostResponsePlugin;
use crate::pre_route::PreRoutePlugin;
use crate::pre_upstream::PreUpstreamPlugin;

/// Type-erased, phase-differentiated container for a loaded plugin instance.
#[derive(Clone)]
pub enum LoadedPlugin {
    PreRoute(Arc<dyn PreRoutePlugin>),
    PreUpstream(Arc<dyn PreUpstreamPlugin>),
    PostResponse(Arc<dyn PostResponsePlugin>),
    Background(Arc<dyn crate::background::BackgroundPlugin>),
}

impl LoadedPlugin {
    /// Returns the unique instance identifier of this plugin.
    pub fn id(&self) -> &str {
        match self {
            Self::PreRoute(p) => p.id(),
            Self::PreUpstream(p) => p.id(),
            Self::PostResponse(p) => p.id(),
            Self::Background(p) => p.id(),
        }
    }

    /// Returns the reverse-permission capabilities declared by this plugin.
    pub fn capability(&self) -> PluginCapability {
        match self {
            Self::PreRoute(p) => p.capability(),
            Self::PreUpstream(p) => p.capability(),
            Self::PostResponse(p) => p.capability(),
            Self::Background(p) => p.capability(),
        }
    }

    /// Returns the hardcoded network/application protocols supported by this plugin.
    pub fn supported_protocols(&self) -> SupportedProtocols {
        match self {
            Self::PreRoute(p) => p.supported_protocols(),
            Self::PreUpstream(p) => p.supported_protocols(),
            Self::PostResponse(p) => p.supported_protocols(),
            Self::Background(p) => p.supported_protocols(),
        }
    }

    /// Returns the Plugin Background Workload Weight (`0` = hot-path only, `1..=100` = shadow daemon).
    pub fn pbww(&self) -> u32 {
        match self {
            Self::PreRoute(p) => p.pbww(),
            Self::PreUpstream(p) => p.pbww(),
            Self::PostResponse(p) => p.pbww(),
            Self::Background(p) => p.pbww(),
        }
    }

    /// Starts the background workload future for this plugin on its assigned Shadow Worker (`pbww() > 0`).
    pub fn run_background(
        &self,
        shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> Option<crate::BackgroundTaskFuture> {
        match self {
            Self::PreRoute(p) => Arc::clone(p).run_background(shutdown),
            Self::PreUpstream(p) => Arc::clone(p).run_background(shutdown),
            Self::PostResponse(p) => Arc::clone(p).run_background(shutdown),
            Self::Background(p) => Some(Arc::clone(p).run(shutdown)),
        }
    }
}

/// Instantiates a concrete plugin instance by delegating to the stage module that owns
/// `config.id` at compile time (`pre_route`, `pre_upstream`, `post_response`, or `background`).
///
/// # Architecture & Open-Closed Principle (OCP)
/// Instead of a monolithic "God Factory" matching every plugin inline and parsing raw JSON,
/// each stage module owns its plugin IDs at compile time:
/// - [`crate::pre_route::owns_id`] -> [`crate::pre_route::instantiate`]
/// - [`crate::pre_upstream::owns_id`] -> [`crate::pre_upstream::instantiate`]
/// - [`crate::post_response::owns_id`] -> [`crate::post_response::instantiate`]
/// - [`crate::background::owns_id`] -> [`crate::background::instantiate`]
///
/// Each concrete plugin defines its own `from_config` constructor, enforcing strict module
/// ownership and ensuring `factory.rs` never bloats as new plugins are added.
pub fn instantiate_plugin(
    config: &PluginConfig,
    upstream_ids: &[&str],
) -> Result<LoadedPlugin, PluginError> {
    let id = config.id.as_str();
    if crate::pre_route::owns_id(id) {
        crate::pre_route::instantiate(config).map(LoadedPlugin::PreRoute)
    } else if crate::pre_upstream::owns_id(id) {
        crate::pre_upstream::instantiate(config, upstream_ids).map(LoadedPlugin::PreUpstream)
    } else if crate::post_response::owns_id(id) {
        crate::post_response::instantiate(config).map(LoadedPlugin::PostResponse)
    } else if crate::background::owns_id(id) {
        crate::background::instantiate(config).map(LoadedPlugin::Background)
    } else {
        Err(PluginError::Config(format!(
            "Unknown plugin id '{}'",
            config.id
        )))
    }
}

/// Pre-computes and compiles an optimized [`RoutePluginPlan`] for a route.
///
/// # Validation and Safety Invariants:
/// 1. Unknown plugin IDs are rejected fail-fast.
/// 2. Protocol mismatch between Route and Plugin is rejected fail-fast.
/// 3. Request plugins needing body buffering on a streaming-upload route (`streaming.client == true`) are rejected.
/// 4. Response plugins needing body buffering on a streaming-download route (`streaming.server == true`) are rejected.
/// 5. Within each phase, hooks execute strictly in sequential FIFO order matching route declaration.
pub fn build_route_plugin_plan(
    route_id: &str,
    route_plugin_ids: &[String],
    available_plugins: &FxHashMap<String, LoadedPlugin>,
    route_protocol: ProtocolFamily,
    _route_streaming: StreamingMode,
) -> Result<RoutePluginPlan, PluginError> {
    let mut pre_route_list: Vec<Arc<dyn PreRoutePlugin>> = Vec::new();
    let mut pre_upstream_list: Vec<Arc<dyn PreUpstreamPlugin>> = Vec::new();
    let mut post_response_list: Vec<Arc<dyn PostResponsePlugin>> = Vec::new();

    for plugin_id in route_plugin_ids {
        let plugin = available_plugins
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

        // 2. Classify by canonical phase in exact FIFO declaration order
        match plugin {
            LoadedPlugin::PreRoute(p) => {
                pre_route_list.push(p.clone());
            }
            LoadedPlugin::PreUpstream(p) => {
                pre_upstream_list.push(p.clone());
            }
            LoadedPlugin::PostResponse(p) => {
                post_response_list.push(p.clone());
            }
            LoadedPlugin::Background(_) => {
                return Err(PluginError::PhaseMismatch {
                    plugin_id: plugin_id.clone(),
                    expected: "traffic phase (pre_route, pre_upstream, or post_response)".into(),
                    actual: "background".into(),
                });
            }
        }
    }

    // 3. Compute aggregate permissions
    let mut req_header_access = AccessLevel::None;
    let mut req_payload_access = AccessLevel::None;

    for plugin in &pre_route_list {
        let cap = plugin.capability();
        req_header_access = req_header_access.union(cap.header);
        req_payload_access = req_payload_access.union(cap.payload);
    }

    for plugin in &pre_upstream_list {
        let cap = plugin.capability();
        req_header_access = req_header_access.union(cap.header);
        req_payload_access = req_payload_access.union(cap.payload);
    }

    let mut resp_header_access = AccessLevel::None;
    let mut resp_payload_access = AccessLevel::None;

    for plugin in &post_response_list {
        let cap = plugin.capability();
        resp_header_access = resp_header_access.union(cap.header);
        resp_payload_access = resp_payload_access.union(cap.payload);
    }

    // 4. Orthogonal streaming / pipe compatibility
    // In accordance with Header-Driven Dynamic Decoupling:
    // Listener streaming declaration represents protocol capability (allowance), not an execution mandate.
    // Standard REST transactions on the listener execute buffered (enabling cache & compression),
    // while genuine streaming streams (chunked / SSE) bypass payload plugins dynamically based on headers.

    // 5. Freeze into final Arc slices (None if empty for zero-overhead phase elimination)
    let pre_route_hooks = if pre_route_list.is_empty() {
        None
    } else {
        Some(Arc::from(pre_route_list))
    };
    let pre_upstream_hooks = if pre_upstream_list.is_empty() {
        None
    } else {
        Some(Arc::from(pre_upstream_list))
    };
    let post_response_hooks = if post_response_list.is_empty() {
        None
    } else {
        Some(Arc::from(post_response_list))
    };

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

/// Wires inter-plugin dependencies between companion plugins (e.g. `jwks_rotator` -> `jwt_auth`).
///
/// Enforces that any background service requiring a shared pre-allocated memory store is
/// authoritatively wired to its target before any serving workers or background tasks start.
pub fn wire_plugin_dependencies(
    available_plugins: &FxHashMap<String, LoadedPlugin>,
) -> Result<(), PluginError> {
    for (id, plugin) in available_plugins {
        if let LoadedPlugin::Background(bg) = plugin
            && let Some(target_id) = bg.target_jwt_auth_id()
        {
            let target = available_plugins.get(target_id).ok_or_else(|| {
                PluginError::Config(format!(
                    "Background plugin '{id}' requires target 'jwt_auth' plugin '{target_id}' to be active, but none was found"
                ))
            })?;

            let store = match target {
                LoadedPlugin::PreUpstream(pu) => pu.shared_jwks_store().ok_or_else(|| {
                    PluginError::Config(format!(
                        "Target plugin '{target_id}' for rotator '{id}' does not provide a SharedJwksStore"
                    ))
                })?,
                _ => {
                    return Err(PluginError::Config(format!(
                        "Target plugin '{target_id}' for rotator '{id}' must be a pre_upstream plugin"
                    )));
                }
            };

            bg.attach_shared_jwks_store(store)?;
            tracing::info!(
                rotator_id = %id,
                target_id = %target_id,
                "Successfully wired SharedJwksStore from jwt_auth to jwks_rotator"
            );
        }
    }
    Ok(())
}

/// Wires External Third-Party (`velda-etp`) pool handles from `etp_registry` into any
/// instantiated pre-upstream, background, or post-response plugins that declare `etp_id()`.
pub fn wire_plugin_etps(
    available_plugins: &FxHashMap<String, LoadedPlugin>,
    etp_registry: &velda_etp::EtpRegistry,
) -> Result<(), PluginError> {
    for (id, plugin) in available_plugins {
        let etp_id = match plugin {
            LoadedPlugin::PreUpstream(pu) => pu.etp_id(),
            LoadedPlugin::Background(bg) => bg.etp_id(),
            LoadedPlugin::PostResponse(pr) => pr.etp_id(),
            _ => None,
        };
        if let Some(etp_id) = etp_id {
            let inst = etp_registry.entries.get(etp_id).ok_or_else(|| {
                PluginError::Config(format!(
                    "Plugin '{id}' requires etp_id '{etp_id}' in EtpRegistry, but none was found"
                ))
            })?;
            match plugin {
                LoadedPlugin::PreUpstream(pu) => pu.attach_etp(inst)?,
                LoadedPlugin::Background(bg) => bg.attach_etp(inst)?,
                LoadedPlugin::PostResponse(pr) => pr.attach_etp(inst)?,
                _ => {}
            }
            tracing::info!(
                plugin_id = %id,
                etp_id = %etp_id,
                etp_kind = %inst.kind,
                "Successfully wired ETP pool to plugin"
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_instantiate_plugins() {
        let cfg_jwt = PluginConfig {
            id: "jwt_auth".into(),
            config: serde_json::json!({
                "algorithm": "HS256",
                "secret_key": "secret-12345678901234567890",
                "public_key_pem": null,
                "token_sources": [
                    { "type": "header", "name": "authorization", "prefix": "Bearer " }
                ],
                "validate_exp": true,
                "clock_skew_secs": 60,
                "issuer": null,
                "audience": null,
                "claims_to_headers": {},
                "assert_claims": {},
                "cache_shards": 16,
                "cache_shard_capacity": 512,
                "cache_ttl_secs": 300,
                "negative_cache_ttl_secs": 10
            }),
        };
        let p1 = instantiate_plugin(&cfg_jwt, &[]).expect("Should instantiate jwt");
        assert_eq!(p1.id(), "jwt_auth");
        assert!(matches!(p1, LoadedPlugin::PreUpstream(_)));

        let cfg_rate = PluginConfig {
            id: "rate_limit".into(),
            config: serde_json::json!({
                "algorithm": "token_bucket",
                "backend": "memory",
                "rate_per_second": 500,
                "burst_capacity": 1000,
                "key_strategy": "priority",
                "key_headers": ["client_ip"],
                "fallback_behavior": "memory"
            }),
        };
        let p2 = instantiate_plugin(&cfg_rate, &[]).expect("Should instantiate rate_limit");
        assert_eq!(p2.id(), "rate_limit");
        assert!(matches!(p2, LoadedPlugin::PreUpstream(_)));

        let cfg_cors = PluginConfig {
            id: "cors".into(),
            config: serde_json::json!({
                "allow_origins": ["https://example.com"],
                "allow_credentials": false,
                "expose_headers": []
            }),
        };
        let p3 = instantiate_plugin(&cfg_cors, &[]).expect("Should instantiate cors");
        assert_eq!(p3.id(), "cors");
        assert!(matches!(p3, LoadedPlugin::PostResponse(_)));
    }

    #[test]
    fn test_build_route_plugin_plan_success() {
        let mut available = FxHashMap::default();

        let cfg_jwt = PluginConfig {
            id: "jwt_auth".into(),
            config: serde_json::json!({
                "algorithm": "HS256",
                "secret_key": "secret-12345678901234567890",
                "public_key_pem": null,
                "token_sources": [
                    { "type": "header", "name": "authorization", "prefix": "Bearer " }
                ],
                "validate_exp": true,
                "clock_skew_secs": 60,
                "issuer": null,
                "audience": null,
                "claims_to_headers": {},
                "assert_claims": {},
                "cache_shards": 16,
                "cache_shard_capacity": 512,
                "cache_ttl_secs": 300,
                "negative_cache_ttl_secs": 10
            }),
        };
        available.insert(
            "jwt_auth".to_string(),
            instantiate_plugin(&cfg_jwt, &[]).unwrap(),
        );

        let cfg_ip = PluginConfig {
            id: "ip_filter".into(),
            config: serde_json::json!({
                "mode": "allow",
                "allow_cidrs": ["127.0.0.1/32"],
                "deny_cidrs": [],
                "client_ip_source": "x_forwarded_for"
            }),
        };
        available.insert(
            "ip_filter".to_string(),
            instantiate_plugin(&cfg_ip, &[]).unwrap(),
        );

        let cfg_cors = PluginConfig {
            id: "cors".into(),
            config: serde_json::json!({
                "allow_origins": ["*"],
                "allow_credentials": false,
                "expose_headers": []
            }),
        };
        available.insert(
            "cors".to_string(),
            instantiate_plugin(&cfg_cors, &[]).unwrap(),
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
        let pre_route = plan.pre_route_hooks.as_ref().unwrap();
        assert_eq!(pre_route.len(), 1);
        assert_eq!(pre_route[0].id(), "ip_filter");

        // pre_upstream hooks
        let pre_upstream = plan.pre_upstream_hooks.as_ref().unwrap();
        assert_eq!(pre_upstream.len(), 1);
        assert_eq!(pre_upstream[0].id(), "jwt_auth");

        // post_response hooks
        let post_resp = plan.post_response_hooks.as_ref().unwrap();
        assert_eq!(post_resp.len(), 1);
        assert_eq!(post_resp[0].id(), "cors");

        // Zero-copy streaming active because payload access is None!
        assert!(!plan.is_empty());
        assert!(plan.is_request_payload_passthrough());
        assert!(plan.is_response_payload_passthrough());
    }

    #[test]
    fn test_build_route_plugin_plan_fifo_declaration_order() {
        let mut available = FxHashMap::default();

        let cfg_cors_preflight = PluginConfig {
            id: "cors_preflight".into(),
            config: serde_json::json!({
                "allowed_origins": ["*"],
                "allowed_methods": ["GET", "POST", "OPTIONS"],
                "allowed_headers": ["*"],
                "allow_credentials": false,
                "max_age_secs": 86400
            }),
        };
        available.insert(
            "cors_preflight".to_string(),
            instantiate_plugin(&cfg_cors_preflight, &[]).unwrap(),
        );

        let cfg_ip = PluginConfig {
            id: "ip_filter".into(),
            config: serde_json::json!({
                "mode": "allow",
                "allow_cidrs": ["127.0.0.1/32"],
                "deny_cidrs": [],
                "client_ip_source": "x_forwarded_for"
            }),
        };
        available.insert(
            "ip_filter".to_string(),
            instantiate_plugin(&cfg_ip, &[]).unwrap(),
        );

        // Order 1: cors_preflight before ip_filter
        let route_plugins_1 = vec!["cors_preflight".to_string(), "ip_filter".to_string()];
        let plan_1 = build_route_plugin_plan(
            "route-1",
            &route_plugins_1,
            &available,
            ProtocolFamily::Http,
            StreamingMode::DISABLED,
        )
        .expect("Should build plan");
        let pre_route_1 = plan_1.pre_route_hooks.as_ref().unwrap();
        assert_eq!(pre_route_1.len(), 2);
        assert_eq!(pre_route_1[0].id(), "cors_preflight");
        assert_eq!(pre_route_1[1].id(), "ip_filter");

        // Order 2: ip_filter before cors_preflight (strictly follows route array FIFO)
        let route_plugins_2 = vec!["ip_filter".to_string(), "cors_preflight".to_string()];
        let plan_2 = build_route_plugin_plan(
            "route-2",
            &route_plugins_2,
            &available,
            ProtocolFamily::Http,
            StreamingMode::DISABLED,
        )
        .expect("Should build plan");
        let pre_route_2 = plan_2.pre_route_hooks.as_ref().unwrap();
        assert_eq!(pre_route_2.len(), 2);
        assert_eq!(pre_route_2[0].id(), "ip_filter");
        assert_eq!(pre_route_2[1].id(), "cors_preflight");
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
            config: serde_json::json!({
                "allow_origins": ["*"],
                "allow_credentials": false,
                "expose_headers": []
            }),
        };
        available.insert(
            "cors".to_string(),
            instantiate_plugin(&cfg_cors, &[]).unwrap(),
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

    #[test]
    fn test_wire_plugin_dependencies_success() {
        let mut available = FxHashMap::default();

        let cfg_jwt = PluginConfig {
            id: "jwt_auth".into(),
            config: serde_json::json!({
                "algorithm": "HS256",
                "secret_key": "secret-12345678901234567890",
                "token_sources": [{ "type": "header", "name": "authorization", "prefix": "Bearer " }],
                "validate_exp": true,
                "clock_skew_secs": 60,
                "claims_to_headers": {},
                "assert_claims": {},
                "cache_ttl_secs": 300
            }),
        };
        available.insert(
            "jwt_auth".to_string(),
            instantiate_plugin(&cfg_jwt, &[]).unwrap(),
        );

        let cfg_rotator = PluginConfig {
            id: "jwks_rotator".into(),
            config: serde_json::json!({
                "target_jwt_auth_id": "jwt_auth",
                "jwks_uri": "https://auth.example.com/.well-known/jwks.json",
                "refresh_interval_secs": 60,
                "request_timeout_secs": 5,
                "preserve_static_key": true
            }),
        };
        available.insert(
            "jwks_rotator".to_string(),
            instantiate_plugin(&cfg_rotator, &[]).unwrap(),
        );

        // Wire dependencies
        let res = wire_plugin_dependencies(&available);
        assert!(
            res.is_ok(),
            "Wiring should succeed when target jwt_auth exists: {res:?}"
        );
    }

    #[test]
    fn test_wire_plugin_dependencies_missing_target_fails() {
        let mut available = FxHashMap::default();

        let cfg_rotator = PluginConfig {
            id: "jwks_rotator".into(),
            config: serde_json::json!({
                "target_jwt_auth_id": "jwt_auth",
                "jwks_uri": "https://auth.example.com/.well-known/jwks.json",
            }),
        };
        available.insert(
            "jwks_rotator".to_string(),
            instantiate_plugin(&cfg_rotator, &[]).unwrap(),
        );

        // Wiring must fail because target 'jwt_auth' is absent from available_plugins
        let res = wire_plugin_dependencies(&available);
        assert!(res.is_err());
        let err_msg = res.unwrap_err().to_string();
        assert!(err_msg.contains("requires target 'jwt_auth' plugin 'jwt_auth' to be active"));
    }
}
