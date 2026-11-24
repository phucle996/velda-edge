//! Post-Response Execution Stage Plugins (`PostResponsePlugin`).
//!
//! Evaluated on upstream responses immediately before encoding and transmitting
//! frames back to the downstream client (e.g., security response headers, compression, response body masking).
//!
//! # Execution Invariants
//! - **Response Mutability**: Plugins in this phase operate directly on outgoing [`L7Response`] headers and status.
//! - **Non-failing**: At this phase, the upstream interaction has completed. Plugins mutate headers or status in-place.

use std::sync::Arc;
use velda_core::L7Response;

use crate::capability::{Direction, PluginCapability, SupportedProtocols};

pub mod cors;
pub mod http_cache_store;
pub mod openai_token_quota;
pub mod response_compression;
pub mod response_header_mutation;
pub mod security_headers;

pub use cors::{CorsConfig, CorsOriginPattern, CorsResponsePlugin};
pub use http_cache_store::{HttpCacheStoreConfig, HttpCacheStorePlugin};
pub use openai_token_quota::{
    OpenAiTokenQuotaConfig, OpenAiTokenQuotaPlugin, SharedUsageArena, TokenUsage,
};
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
    fn on_response(&self, req_headers: &velda_core::HeaderWorkspace, resp: &mut L7Response);

    /// Declares the reverse-permission capabilities required by this plugin.
    fn capability(&self) -> PluginCapability;

    /// Hardcoded network/application protocols supported by this plugin.
    fn supported_protocols(&self) -> SupportedProtocols;

    /// Execution direction. Always [`Direction::Response`] for post-response plugins.
    #[inline(always)]
    fn direction(&self) -> Direction {
        Direction::Response
    }

    /// Plugin Background Workload Weight (`0` = hot-path only, `1..=100` = shadow daemon weight).
    fn pbww(&self) -> u32;

    /// Optional `etp_id` referencing an external third-party pool in `EtpRegistry`.
    #[inline(always)]
    fn etp_id(&self) -> Option<&str> {
        None
    }

    /// Attaches the resolved [`velda_etp::EtpInstance`] from `EtpRegistry` before
    /// `run_background()` starts.
    fn attach_etp(
        &self,
        _instance: &velda_etp::EtpInstance,
    ) -> Result<(), crate::error::PluginError> {
        Ok(())
    }

    /// Starts optional background workload daemon on the assigned Shadow Worker (`pbww() > 0`).
    #[inline]
    fn run_background(
        self: Arc<Self>,
        _shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> Option<crate::BackgroundTaskFuture> {
        None
    }
}

/// Executes a linear chain of post-response plugins on the outgoing response
/// and batch-renders all arithmetic header mutations into `resp.headers` in a single pass.
#[inline]
pub fn run_post_response_plugins(
    chain: &[Arc<dyn PostResponsePlugin>],
    req_headers: &velda_core::HeaderWorkspace,
    resp: &mut L7Response,
) {
    for plugin in chain {
        plugin.on_response(req_headers, resp);
    }
}

/// Compile-time registry check for plugins owned by the `post_response` stage.
#[inline]
pub fn owns_id(id: &str) -> bool {
    matches!(
        id,
        "security_headers"
            | "cors"
            | "http_cache_store"
            | "response_header_mutation"
            | "response_compression"
            | "openai_token_quota"
    )
}

/// Sub-dispatcher: instantiates concrete [`PostResponsePlugin`] instances for this stage.
///
/// # Open-Closed Principle (OCP)
/// Adding a new post-response plugin requires adding its ID to [`owns_id`] and 1 match arm here,
/// delegating config extraction and validation to the plugin's own `from_config` implementation.
pub fn instantiate(
    config: &crate::config::PluginConfig,
) -> Result<Arc<dyn PostResponsePlugin>, crate::error::PluginError> {
    match config.id.as_str() {
        "security_headers" => Ok(Arc::new(SecurityHeadersPlugin::from_config(config)?)),
        "cors" => Ok(Arc::new(CorsResponsePlugin::from_config(config)?)),
        "http_cache_store" => Ok(Arc::new(HttpCacheStorePlugin::from_config(config)?)),
        "response_header_mutation" => {
            Ok(Arc::new(ResponseHeaderMutationPlugin::from_config(config)?))
        }
        "response_compression" => Ok(Arc::new(ResponseCompressionPlugin::from_config(config)?)),
        "openai_token_quota" => Ok(Arc::new(OpenAiTokenQuotaPlugin::from_config(config)?)),
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
        let req_headers = velda_core::HeaderWorkspace::default();

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
        let plugin =
            Arc::new(CorsResponsePlugin::new("cors-1", "https://app.example.com", true).unwrap());
        let chain: Vec<Arc<dyn PostResponsePlugin>> = vec![plugin];

        let mut resp = L7Response::empty(StatusCode::NO_CONTENT);
        let mut req_headers = velda_core::HeaderWorkspace::default();
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
