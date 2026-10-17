//! Header Mutation Pre-Upstream Plugin Skeleton (`phase: "pre_upstream"`).
//!
//! Adds, mutates, or strips HTTP headers before proxying requests to upstream backends
//! (e.g., adding `X-Request-Id`, passing tracing contexts, stripping sensitive hop-by-hop headers).

use http::HeaderValue;
use http::header::HeaderName;
use velda_core::{Action, L7Request};

use super::PreUpstreamPlugin;

/// Production skeleton for mutating request headers before upstream dispatch.
#[derive(Debug, Clone)]
pub struct HeaderMutationPlugin {
    pub id: String,
    pub set_headers: Vec<(HeaderName, HeaderValue)>,
    pub remove_headers: Vec<HeaderName>,
}

impl HeaderMutationPlugin {
    /// Constructs a new header mutation plugin.
    pub fn new(
        id: impl Into<String>,
        set_headers: Vec<(HeaderName, HeaderValue)>,
        remove_headers: Vec<HeaderName>,
    ) -> Self {
        Self {
            id: id.into(),
            set_headers,
            remove_headers,
        }
    }

    /// Deserializes and constructs an instance directly from a [`PluginConfig`].
    pub fn from_config(
        config: &velda_sync::post_sync::plugin::PluginConfig,
    ) -> Result<Self, crate::error::PluginError> {
        let mut set_headers = Vec::new();
        if let Some(set_obj) = config.config.get("set").and_then(|v| v.as_object()) {
            for (k, v) in set_obj {
                if let (Ok(name), Some(val)) = (
                    HeaderName::from_bytes(k.as_bytes()),
                    v.as_str().and_then(|s| HeaderValue::from_str(s).ok()),
                ) {
                    set_headers.push((name, val));
                }
            }
        }

        let mut remove_headers = Vec::new();
        if let Some(rem_arr) = config.config.get("remove").and_then(|v| v.as_array()) {
            for item in rem_arr {
                if let Some(name) = item
                    .as_str()
                    .and_then(|s| HeaderName::from_bytes(s.as_bytes()).ok())
                {
                    remove_headers.push(name);
                }
            }
        }

        Ok(Self::new(&config.id, set_headers, remove_headers))
    }
}

impl PreUpstreamPlugin for HeaderMutationPlugin {
    #[inline]
    fn id(&self) -> &str {
        &self.id
    }

    fn on_upstream_request(&self, req: &mut L7Request) -> Action {
        for header in &self.remove_headers {
            req.headers.remove(header);
        }

        for (name, val) in &self.set_headers {
            req.headers.insert(name.clone(), val.clone());
        }

        Action::Continue
    }

    #[inline]
    fn capability(&self) -> crate::capability::PluginCapability {
        crate::capability::PluginCapability::header_only(crate::capability::AccessLevel::Write)
    }

    #[inline]
    fn supported_protocols(&self) -> crate::capability::SupportedProtocols {
        crate::capability::SupportedProtocols::ALL_L7
    }
}
