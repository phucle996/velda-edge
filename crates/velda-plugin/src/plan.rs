//! Route Plugin Execution Plan (`RoutePluginPlan`).
//!
//! # Architecture and Invariants
//! 1. **Phase Isolation**:
//!    - Plugins execute in strictly non-overlapping phases:
//!      `pre_route` -> `Router Match` -> `pre_upstream` -> `Upstream Dispatch` -> `post_response`.
//! 2. **Sequential FIFO Declaration Order**:
//!    - Within each phase, hooks execute strictly in the sequential order they are declared
//!      in the route's `plugins` array (First declared, first executed).
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
    /// Ordered hooks evaluated before route lookup (IP filter, JWT auth, etc.), or `None` if phase is empty.
    pub pre_route_hooks: Option<Arc<[Arc<dyn PreRoutePlugin>]>>,
    /// Ordered hooks evaluated after route lookup, before upstream dispatch (Rate limit, Header mutation, etc.), or `None` if phase is empty.
    pub pre_upstream_hooks: Option<Arc<[Arc<dyn PreUpstreamPlugin>]>>,
    /// Ordered hooks evaluated on returning response before downstream encoding, or `None` if phase is empty.
    pub post_response_hooks: Option<Arc<[Arc<dyn PostResponsePlugin>]>>,

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
    /// Returns `true` if this plan has no plugins in any phase (enables total route bypass).
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.pre_route_hooks.is_none()
            && self.pre_upstream_hooks.is_none()
            && self.post_response_hooks.is_none()
    }

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_route_plugin_plan_passthrough_defaults() {
        let plan = RoutePluginPlan::default();
        assert!(plan.is_empty());
        assert!(plan.pre_route_hooks.is_none());
        assert!(plan.pre_upstream_hooks.is_none());
        assert!(plan.post_response_hooks.is_none());
        assert!(plan.is_request_payload_passthrough());
        assert!(plan.is_response_payload_passthrough());
        assert_eq!(plan.request_header_access, AccessLevel::None);
        assert_eq!(plan.request_payload_access, AccessLevel::None);
        assert_eq!(plan.response_header_access, AccessLevel::None);
        assert_eq!(plan.response_payload_access, AccessLevel::None);
    }
}
