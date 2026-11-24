//! Velda Plugin Engine and Edge Extensions (`velda-plugin`).
//!
//! Provides extensible edge capabilities divided strictly across canonical pipeline phases:
//! - **Traffic Phase Plugins**: Evaluated linearly within the request pipeline:
//!   - [`pre_route`]: Evaluated immediately after downstream L7 decode, before route lookup.
//!   - [`pre_upstream`]: Evaluated after route and endpoint selection, before upstream request dispatch.
//!   - [`post_response`]: Evaluated on upstream responses before downstream frame encoding.
//! - **Background Plugins**:
//!   - [`background`]: Executed on Shadow Logistics Worker runtimes isolated from Data Plane workers
//!     (e.g., Prometheus Exporter, Health Probes).

pub mod background;
pub mod capability;
pub mod config;
pub mod error;
pub mod factory;
pub mod hook;
pub mod http_cache;
pub mod plan;
pub mod post_response;
pub mod pre_route;
pub mod pre_upstream;
pub mod validation;

// Root exports are limited to the cross-phase orchestration surface.
// Plugin types, configs and internal state are reached through their phase module
// (e.g. `velda_plugin::pre_route::WafLitePlugin`) so the phase stays visible at call sites.
pub use background::{BackgroundPlugin, BackgroundTaskFuture};
pub use capability::{AccessLevel, Direction, PluginCapability, SupportedProtocols};
pub use config::{OutboundTlsConfig, PluginConfig};
pub use error::PluginError;
pub use factory::{
    LoadedPlugin, build_route_plugin_plan, instantiate_plugin, wire_plugin_dependencies,
    wire_plugin_etps, wire_plugin_upstreams,
};
pub use hook::{ShadowTask, set_core_shadow_sender, try_dispatch_shadow};
pub use plan::RoutePluginPlan;
pub use post_response::run_post_response_plugins;
pub use pre_route::run_pre_route_plugins;
pub use pre_upstream::run_pre_upstream_plugins;
pub use validation::*;
