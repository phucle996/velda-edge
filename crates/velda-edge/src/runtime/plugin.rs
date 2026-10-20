//! In-memory Route Plugin Plan table and compilation dispatcher.
//!
//! Pre-compiles declarative [`PluginConfig`] definitions into typed [`LoadedPlugin`] instances,
//! and compiles route-specific [`RoutePluginPlan`] plans in RAM ahead of time.
//! Provides O(1) lock-free plan lookup on the request hot path with total bypass when empty.

use std::sync::Arc;

use rustc_hash::FxHashMap;
use velda_core::{ProtocolFamily, RouteId, StreamingMode};
use velda_plugin::{RoutePluginPlan, build_route_plugin_plan, instantiate_plugin};
use velda_sync::post_sync::listener::ListenerConfig;
use velda_sync::post_sync::plugin::PluginConfig;
use velda_sync::post_sync::route::RouteConfig;

use crate::error::EdgeError;

/// Active in-memory plugin table holding pre-compiled [`RoutePluginPlan`] execution plans.
#[derive(Clone, Default)]
pub struct PluginTable {
    /// O(1) integer lookup table: RouteId -> RoutePluginPlan.
    pub routes: FxHashMap<RouteId, Arc<RoutePluginPlan>>,
    /// Canonical empty plan singleton used for routes without any declared plugins.
    pub empty_plan: Arc<RoutePluginPlan>,
}

impl std::fmt::Debug for PluginTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PluginTable")
            .field("routes_count", &self.routes.len())
            .finish()
    }
}

impl PluginTable {
    /// Creates a new empty plugin table.
    pub fn empty() -> Self {
        Self {
            routes: FxHashMap::default(),
            empty_plan: Arc::new(RoutePluginPlan::default()),
        }
    }

    /// Returns the pre-compiled plugin execution plan for the given route identifier.
    ///
    /// If the route has no plugins or route_id is unknown, returns the canonical empty plan.
    #[inline(always)]
    pub fn get_plan(&self, route_id: RouteId) -> &Arc<RoutePluginPlan> {
        self.routes.get(&route_id).unwrap_or(&self.empty_plan)
    }

    /// Returns `true` if the route has at least one active plugin hook.
    #[inline(always)]
    pub fn has_plugins(&self, route_id: RouteId) -> bool {
        self.routes.get(&route_id).is_some_and(|p| !p.is_empty())
    }
}

/// Pre-compiles all plugin instances and constructs the static [`PluginTable`].
pub fn build_plugin_table(
    plugins: &[PluginConfig],
    routes: &[RouteConfig],
    listeners: &[ListenerConfig],
) -> Result<PluginTable, EdgeError> {
    // 1. Instantiate all available plugins into typed LoadedPlugin instances
    let mut available_plugins = FxHashMap::default();
    for plugin_cfg in plugins {
        let loaded = instantiate_plugin(plugin_cfg).map_err(|e| EdgeError::InvalidConfig {
            detail: format!("Failed to instantiate plugin '{}': {e}", plugin_cfg.id),
        })?;
        available_plugins.insert(plugin_cfg.id.clone(), loaded);
    }

    // 2. Pre-index listeners by listener_id for O(1) protocol & streaming lookup
    let listener_map: FxHashMap<&str, &ListenerConfig> =
        listeners.iter().map(|l| (l.id.as_str(), l)).collect();

    // 3. Pre-compile RoutePluginPlan for each route that specifies plugins
    let mut route_plans = FxHashMap::default();
    let empty_plan = Arc::new(RoutePluginPlan::default());

    for route in routes {
        if route.plugins.is_empty() {
            continue; // Zero overhead: do not populate routes map for empty routes!
        }

        let listener = listener_map.get(route.listener.as_str()).copied();
        let (protocol_family, streaming_mode) = if let Some(l) = listener {
            let family =
                ProtocolFamily::parse(&l.application.protocol).unwrap_or(ProtocolFamily::Http);
            let streaming = l.application.streaming;
            (family, streaming)
        } else {
            (ProtocolFamily::Http, StreamingMode::DISABLED)
        };

        let plan = build_route_plugin_plan(
            &route.id,
            &route.plugins,
            &available_plugins,
            protocol_family,
            streaming_mode,
        )
        .map_err(|e| EdgeError::InvalidConfig {
            detail: format!("Failed to build plugin plan for route '{}': {e}", route.id),
        })?;

        if !plan.is_empty() {
            let route_id = RouteId::new(crate::runtime::router::hash_id_to_u32(&route.id));
            route_plans.insert(route_id, Arc::new(plan));
        }
    }

    Ok(PluginTable {
        routes: route_plans,
        empty_plan,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_plugin_table() {
        let table = PluginTable::empty();
        let non_existent = RouteId::new(999);
        assert!(!table.has_plugins(non_existent));
        assert!(table.get_plan(non_existent).is_empty());
    }
}
