//! Route Plugin Execution Plan (`RoutePluginPlan`).
//!
//! # Architecture and Invariants
//! 1. **Phase Isolation**:
//!    - Plugins execute in strictly non-overlapping phases:
//!      `pre_route` -> `Router Match` -> `pre_upstream` -> `Upstream Dispatch` -> `post_response`.
//! 2. **Hybrid Stable Ordering**:
//!    - Within each phase, plugins are sorted by descending `priority` (highest priority runs first).
//!    - Ties are resolved by the 0-based declaration order from configuration (Stable Sort).
//! 3. **Aggregate Reverse Permissions**:
//!    - The plan pre-computes aggregate [`AccessLevel`] masks across all attached plugins.
//!    - If `request_payload_access == None`, the data plane enables Zero-Copy Direct Streaming
//!      with 0 bytes of RAM allocated for request bodies.

use std::sync::Arc;

use crate::capability::AccessLevel;
use crate::post_response::PostResponsePlugin;
use crate::pre_route::PreRoutePlugin;
use crate::pre_upstream::PreUpstreamPlugin;

/// Pre-compiled execution plan containing ordered plugin chains and aggregate permissions for a route.
#[derive(Clone, Default)]
pub struct RoutePluginPlan {
    /// Ordered hooks evaluated before route lookup (IP filter, JWT auth, etc.).
    pub pre_route_hooks: Arc<[Arc<dyn PreRoutePlugin>]>,
    /// Ordered hooks evaluated after route lookup, before upstream dispatch (Rate limit, Header mutation, etc.).
    pub pre_upstream_hooks: Arc<[Arc<dyn PreUpstreamPlugin>]>,
    /// Ordered hooks evaluated on returning response before downstream encoding.
    pub post_response_hooks: Arc<[Arc<dyn PostResponsePlugin>]>,

    /// Aggregate permission required for request headers across all request hooks.
    pub request_header_access: AccessLevel,
    /// Aggregate permission required for request payload across all request hooks.
    pub request_payload_access: AccessLevel,
    /// Aggregate permission required for response headers across all response hooks.
    pub response_header_access: AccessLevel,
    /// Aggregate permission required for response payload across all response hooks.
    pub response_payload_access: AccessLevel,
}

impl RoutePluginPlan {
    /// Returns `true` if request payload body is completely bypassed (enables Zero-Copy streaming).
    #[inline(always)]
    pub fn is_request_payload_passthrough(&self) -> bool {
        self.request_payload_access.is_none()
    }

    /// Returns `true` if response payload body is completely bypassed (enables Zero-Copy streaming).
    #[inline(always)]
    pub fn is_response_payload_passthrough(&self) -> bool {
        self.response_payload_access.is_none()
    }
}

/// Helper container used during cold-start compilation to sort plugins stably.
pub struct StablyOrderedPlugin<T: ?Sized> {
    pub plugin: Arc<T>,
    pub priority: i32,
    pub declaration_index: usize,
}

impl<T: ?Sized> StablyOrderedPlugin<T> {
    #[inline]
    pub fn new(plugin: Arc<T>, priority: Option<i32>, declaration_index: usize) -> Self {
        Self {
            plugin,
            priority: priority.unwrap_or(0),
            declaration_index,
        }
    }
}

/// Sorts a slice of plugins using the Hybrid Stable Sort formula:
/// 1. Descending priority (higher priority number runs first).
/// 2. Ascending declaration index in JSON (earlier declared runs first on tie).
pub fn sort_plugins_stable<T: ?Sized>(plugins: &mut [StablyOrderedPlugin<T>]) {
    plugins.sort_by(|a, b| {
        b.priority
            .cmp(&a.priority)
            .then_with(|| a.declaration_index.cmp(&b.declaration_index))
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    struct DummyPlugin {
        name: &'static str,
    }

    #[test]
    fn test_hybrid_stable_sorting() {
        let mut plugins = vec![
            StablyOrderedPlugin::new(
                Arc::new(DummyPlugin {
                    name: "default_0_idx0",
                }),
                None,
                0,
            ),
            StablyOrderedPlugin::new(
                Arc::new(DummyPlugin {
                    name: "priority_100_idx1",
                }),
                Some(100),
                1,
            ),
            StablyOrderedPlugin::new(
                Arc::new(DummyPlugin {
                    name: "priority_50_idx2",
                }),
                Some(50),
                2,
            ),
            StablyOrderedPlugin::new(
                Arc::new(DummyPlugin {
                    name: "default_0_idx3",
                }),
                Some(0),
                3,
            ),
            StablyOrderedPlugin::new(
                Arc::new(DummyPlugin {
                    name: "priority_100_idx4",
                }),
                Some(100),
                4,
            ),
        ];

        sort_plugins_stable(&mut plugins);

        let names: Vec<&'static str> = plugins.iter().map(|p| p.plugin.name).collect();
        assert_eq!(
            names,
            vec![
                "priority_100_idx1", // priority 100, idx 1
                "priority_100_idx4", // priority 100, idx 4
                "priority_50_idx2",  // priority 50, idx 2
                "default_0_idx0",    // priority 0, idx 0
                "default_0_idx3",    // priority 0, idx 3
            ]
        );
    }

    #[test]
    fn test_route_plugin_plan_passthrough_defaults() {
        let plan = RoutePluginPlan::default();
        assert!(plan.is_request_payload_passthrough());
        assert!(plan.is_response_payload_passthrough());
        assert_eq!(plan.request_header_access, AccessLevel::None);
        assert_eq!(plan.request_payload_access, AccessLevel::None);
        assert_eq!(plan.response_header_access, AccessLevel::None);
        assert_eq!(plan.response_payload_access, AccessLevel::None);
    }
}
