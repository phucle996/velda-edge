//! Velda Plugin Engine and Auxiliary Extensions (`velda-plugin`).
//!
//! Provides extensible edge capabilities divided strictly across canonical pipeline phases:
//! - **Traffic Phase Plugins**: Evaluated linearly within the request pipeline:
//!   - [`pre_route`]: Evaluated immediately after downstream L7 decode, before route lookup.
//!   - [`pre_upstream`]: Evaluated after route and endpoint selection, before upstream request dispatch.
//!   - [`post_response`]: Evaluated on upstream responses before downstream frame encoding.
//! - **Auxiliary Background Plugins**:
//!   - [`background`]: Executed on auxiliary supervisory runtimes isolated from Data Plane workers
//!     (e.g., Prometheus Exporter, Health Probes).

pub mod background;
pub mod error;
pub mod post_response;
pub mod pre_route;
pub mod pre_upstream;

pub use background::prometheus::{PrometheusExporter, format_snapshot};
pub use background::{BackgroundPlugin, HealthProbePlugin};
pub use error::PluginError;
pub use post_response::{
    CorsResponsePlugin, PostResponsePlugin, SecurityHeadersPlugin, run_post_response_plugins,
};
pub use pre_route::{
    IpFilterMode, IpFilterPlugin, JwtAuthPlugin, PreRoutePlugin, run_pre_route_plugins,
};
pub use pre_upstream::{
    HeaderMutationPlugin, PreUpstreamPlugin, RateLimitPlugin, run_pre_upstream_plugins,
};
