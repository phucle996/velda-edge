//! Post-Response Execution Phase Plugins (`phase: "post_response"`).
//!
//! Evaluated on upstream responses immediately before encoding and transmitting
//! frames back to the downstream client (e.g., security response headers, compression, response body masking).
//!
//! # Execution Invariants
//! - **Response Mutability**: Plugins in this phase operate directly on outgoing [`L7Response`] headers and status.
//! - **Non-failing**: At this phase, the upstream interaction has completed. Plugins mutate headers or status in-place.

use http::HeaderMap;
use std::sync::Arc;
use velda_core::L7Response;

use crate::capability::{Direction, PluginCapability, SupportedProtocols};

pub mod access_log;
pub mod cors;
pub mod http_cache_store;
pub mod opentelemetry_logger;
pub mod opentelemetry_tracer;
pub mod response_compression;
pub mod response_header_mutation;
pub mod security_headers;

pub use access_log::{AccessLogConfig, AccessLogPlugin, LogFormat, LogRecord};
pub use cors::{CorsConfig, CorsResponsePlugin};
pub use http_cache_store::{HttpCacheStoreConfig, HttpCacheStorePlugin};
pub use opentelemetry_logger::{OpentelemetryLoggerConfig, OpentelemetryLoggerPlugin};
pub use opentelemetry_tracer::{OpentelemetryTracerConfig, OpentelemetryTracerPlugin};
pub use response_compression::{
    CompressionAlgorithm, ResponseCompressionConfig, ResponseCompressionPlugin,
};
pub use response_header_mutation::{
    ResponseDynamicVar, ResponseHeaderMutationConfig, ResponseHeaderMutationPlugin,
    ResponseHeaderValueSpec,
};
pub use security_headers::SecurityHeadersPlugin;

/// Contract for plugins evaluated on outgoing upstream responses.
pub trait PostResponsePlugin: Send + Sync {
    /// Unique identifier of this plugin instance.
    fn id(&self) -> &str;

    /// Evaluated on outgoing response before downstream encoding.
    ///
    /// The borrowed `req_headers` workspace strictly belongs to THIS specific in-flight
    /// request (enforcing zero cross-request contamination in pipelined/multiplexed streams).
    fn on_response(&self, req_headers: &HeaderMap, resp: &mut L7Response);

    /// Declares the reverse-permission capabilities required by this plugin.
    fn capability(&self) -> PluginCapability;

    /// Hardcoded network/application protocols supported by this plugin.
    fn supported_protocols(&self) -> SupportedProtocols;

    /// Execution direction. Always [`Direction::Response`] for post-response plugins.
    #[inline(always)]
    fn direction(&self) -> Direction {
        Direction::Response
    }
}

/// Executes a linear chain of post-response plugins on the outgoing response.
#[inline]
pub fn run_post_response_plugins(
    chain: &[Arc<dyn PostResponsePlugin>],
    req_headers: &HeaderMap,
    resp: &mut L7Response,
) {
    for plugin in chain {
        plugin.on_response(req_headers, resp);
    }
}

/// Sub-dispatcher: instantiates concrete [`PostResponsePlugin`] instances for this phase.
///
/// # Open-Closed Principle (OCP)
/// Adding a new post-response plugin requires adding exactly 1 match arm here, delegating
/// config extraction and validation to the plugin's own `from_config` implementation.
pub fn instantiate(
    config: &velda_sync::post_sync::plugin::PluginConfig,
) -> Result<Arc<dyn PostResponsePlugin>, crate::error::PluginError> {
    match config.id.as_str() {
        "security_headers" => Ok(Arc::new(SecurityHeadersPlugin::from_config(config)?)),
        "cors" => Ok(Arc::new(CorsResponsePlugin::from_config(config)?)),
        "access_log" => Ok(Arc::new(AccessLogPlugin::from_config(config)?)),
        "http_cache_store" => Ok(Arc::new(HttpCacheStorePlugin::from_config(config)?)),
        "response_header_mutation" => {
            Ok(Arc::new(ResponseHeaderMutationPlugin::from_config(config)?))
        }
        "response_compression" => Ok(Arc::new(ResponseCompressionPlugin::from_config(config)?)),
        "opentelemetry_logger" => Ok(Arc::new(OpentelemetryLoggerPlugin::from_config(config)?)),
        "opentelemetry_tracer" => Ok(Arc::new(OpentelemetryTracerPlugin::from_config(config)?)),
        other => Err(crate::error::PluginError::Config(format!(
            "Unsupported post_response plugin '{other}' for plugin '{}'",
            config.id
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::StatusCode;

    #[test]
    fn test_post_response_security_headers_injection() {
        let plugin = Arc::new(SecurityHeadersPlugin::new("sec-1"));
        let chain: Vec<Arc<dyn PostResponsePlugin>> = vec![plugin];

        let mut resp = L7Response::from_bytes(StatusCode::OK, "{\"status\":\"ok\"}");
        let req_headers = HeaderMap::new();

        run_post_response_plugins(&chain, &req_headers, &mut resp);

        assert!(resp.headers.contains_key("strict-transport-security"));
        assert_eq!(
            resp.headers.get("x-content-type-options").unwrap(),
            "nosniff"
        );
        assert_eq!(resp.headers.get("x-frame-options").unwrap(), "DENY");
        assert_eq!(
            resp.headers.get("referrer-policy").unwrap(),
            "strict-origin-when-cross-origin"
        );
    }

    #[test]
    fn test_post_response_cors_injection() {
        let plugin = Arc::new(CorsResponsePlugin::new(
            "cors-1",
            "https://app.example.com",
            true,
        ));
        let chain: Vec<Arc<dyn PostResponsePlugin>> = vec![plugin];

        let mut resp = L7Response::empty(StatusCode::NO_CONTENT);
        let mut req_headers = HeaderMap::new();
        req_headers.insert(
            "origin",
            http::HeaderValue::from_static("https://app.example.com"),
        );

        run_post_response_plugins(&chain, &req_headers, &mut resp);

        assert_eq!(
            resp.headers.get("access-control-allow-origin").unwrap(),
            "https://app.example.com"
        );
        assert_eq!(
            resp.headers
                .get("access-control-allow-credentials")
                .unwrap(),
            "true"
        );
        assert_eq!(resp.headers.get("vary").unwrap(), "Origin");
    }
}
