//! Cross-Origin Resource Sharing (CORS) Post-Response Plugin Skeleton (`phase: "post_response"`).
//!
//! Injects standard CORS response headers on outgoing HTTP API responses.

use http::HeaderValue;
use http::header::{ACCESS_CONTROL_ALLOW_CREDENTIALS, ACCESS_CONTROL_ALLOW_ORIGIN, VARY};
use velda_core::L7Response;

use super::PostResponsePlugin;

/// Production skeleton for injecting CORS response headers.
#[derive(Debug, Clone)]
pub struct CorsResponsePlugin {
    pub id: String,
    pub allow_origin: HeaderValue,
    pub allow_credentials: bool,
}

impl CorsResponsePlugin {
    /// Constructs a new CORS response plugin.
    pub fn new(
        id: impl Into<String>,
        allow_origin: impl AsRef<str>,
        allow_credentials: bool,
    ) -> Self {
        let val = HeaderValue::from_str(allow_origin.as_ref())
            .unwrap_or_else(|_| HeaderValue::from_static("*"));
        Self {
            id: id.into(),
            allow_origin: val,
            allow_credentials,
        }
    }

    /// Deserializes and constructs an instance directly from a [`PluginConfig`].
    pub fn from_config(
        config: &velda_sync::post_sync::plugin::PluginConfig,
    ) -> Result<Self, crate::error::PluginError> {
        let allow_origin = config
            .config
            .get("allow_origin")
            .and_then(|v| v.as_str())
            .unwrap_or("*");

        let allow_credentials = config
            .config
            .get("allow_credentials")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        Ok(Self::new(&config.id, allow_origin, allow_credentials))
    }
}

impl PostResponsePlugin for CorsResponsePlugin {
    #[inline]
    fn id(&self) -> &str {
        &self.id
    }

    fn on_response(&self, resp: &mut L7Response) {
        resp.headers
            .insert(ACCESS_CONTROL_ALLOW_ORIGIN, self.allow_origin.clone());

        if self.allow_credentials {
            resp.headers.insert(
                ACCESS_CONTROL_ALLOW_CREDENTIALS,
                HeaderValue::from_static("true"),
            );
        }

        resp.headers
            .insert(VARY, HeaderValue::from_static("Origin"));
    }

    #[inline]
    fn capability(&self) -> crate::capability::PluginCapability {
        crate::capability::PluginCapability::header_only(crate::capability::AccessLevel::Write)
    }

    #[inline]
    fn supported_protocols(&self) -> crate::capability::SupportedProtocols {
        crate::capability::SupportedProtocols::ALL_HTTP
    }
}
