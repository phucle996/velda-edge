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
pub mod capability;
pub mod error;
pub mod factory;
pub mod header;
pub mod hook;
pub mod http_cache;
pub mod plan;
pub mod post_response;
pub mod pre_route;
pub mod pre_upstream;

pub use hook::{
    ShadowTask, has_shadow_hook, set_core_shadow_sender, set_global_shadow_sender,
    try_dispatch_shadow,
};

pub use background::prometheus::{PrometheusExporter, format_snapshot};
pub use background::{
    BackgroundPlugin, HealthProbeConfig, HealthProbePlugin, HealthState, OtlpMetricsExporterConfig,
    OtlpMetricsExporterPlugin, spawn_background_plugins,
};
pub use capability::{AccessLevel, Direction, PluginCapability, SupportedProtocols};
pub use error::PluginError;
pub use factory::{LoadedPlugin, build_route_plugin_plan, instantiate_plugin};
pub use header::*;
pub use http_cache::{
    CacheEntry, HttpCacheZone, HttpCacheZoneRegistry, filter_cacheable_headers, normalize_query,
};
pub use plan::RoutePluginPlan;
pub use post_response::{
    AccessLogConfig, AccessLogPlugin, CompressionAlgorithm, CorsConfig, CorsResponsePlugin,
    HttpCacheStoreConfig, HttpCacheStorePlugin, LogFormat, LogRecord, OpentelemetryLoggerConfig,
    OpentelemetryLoggerPlugin, OpentelemetryTracerConfig, OpentelemetryTracerPlugin,
    PostResponsePlugin, ResponseCompressionConfig, ResponseCompressionPlugin,
    ResponseHeaderMutationConfig, ResponseHeaderMutationPlugin, SecurityHeadersPlugin,
    run_post_response_plugins,
};
pub use pre_route::{
    CorsPreflightPlugin, IpFilterMode, IpFilterPlugin, OriginPattern, PreRoutePlugin,
    ResourceLimitPlugin, ResourceLimits, ThreatVector, WafLitePlugin, WafMode, WafScope,
    run_pre_route_plugins,
};
pub use pre_upstream::{
    ApiKeyAuthPlugin, ApiKeyCache, ApiKeyEntry, CanaryConfig, CanaryPlugin, GrpcAbortConfig,
    GrpcDelayConfig, GrpcFaultCondition, GrpcFaultInjectionPlugin, GrpcTargetMatcher, HashSource,
    HeaderMatchRule, HeaderMutationConfig, HeaderMutationPlugin, HeaderRule, HttpAbortConfig,
    HttpCacheLookupConfig, HttpCacheLookupPlugin, HttpDelayConfig, HttpFaultCondition,
    HttpFaultInjectionPlugin, IpRangeMatcher, JwtAlgorithm, JwtAuthPlugin, KeySource,
    MirroredRequest, PathRewriteStrategy, PreUpstreamPlugin, QueryStrategy, RateLimitPlugin,
    RequestMirrorConfig, RequestMirrorPlugin, RequestTerminationConfig, RequestTerminationPlugin,
    TokenSource, UrlRewriteConfig, UrlRewritePlugin, W3cTraceContextConfig, W3cTraceContextPlugin,
    get_mirror_sender, init_mirror_queue, run_pre_upstream_plugins,
};
