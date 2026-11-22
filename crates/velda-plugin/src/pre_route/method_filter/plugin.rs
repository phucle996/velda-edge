//! Pre-Route HTTP Method Filter Plugin (`method_filter`).
//!
//! ```text
//!  +─────────────────────────────────────────────────────────────────────────+
//!  |                    METHOD_FILTER BITMASK EXECUTION                      |
//!  +─────────────────────────────────────────────────────────────────────────+
//!  |                                                                         |
//!  |  Inbound L7Request (HTTP/1.1, HTTP/2, HTTP/3)                           |
//!  |        │                                                                |
//!  |        ▼  HttpMethod::from_bytes(req.method.as_str().as_bytes())        |
//!  |  id: u8 (0 = Unknown, 1..=9 = GET..TRACE)                               |
//!  |        │                                                                |
//!  |        ▼  bit = 1u16 << id                                              |
//!  |  [(self.allowed_mask & bit) != 0 ?]                                     |
//!  |        │                                                                |
//!  |        ├──► TRUE (1 cycle bitwise AND) ──► Action::Continue             |
//!  |        │                                                                |
//!  |        └──► FALSE: [id == 0 && !custom_methods.is_empty()?]             |
//!  |                      /                              \                   |
//!  |                    YES                               NO                 |
//!  |                    /                                   \                |
//!  |         Scan custom_methods                   Action::Reject            |
//!  |         (PROPFIND, MKCOL, ...)                (MethodNotAllowed / 405)  |
//!  +─────────────────────────────────────────────────────────────────────────+
//! ```

use velda_core::lut::HttpMethod;
use velda_core::{Action, L7Request, RejectReason};

use super::config::MethodFilterConfig;
use crate::capability::{AccessLevel, PluginCapability, SupportedProtocols};
use crate::config::PluginConfig;
use crate::error::PluginError;
use crate::pre_route::PreRoutePlugin;

/// Pre-Route HTTP Method Filter Plugin enforcing 1-cycle bitmask allowlists.
pub struct MethodFilterPlugin {
    pub id: String,
    pub allowed_mask: u16,
    pub custom_methods: Vec<String>,
}

impl MethodFilterPlugin {
    /// Constructs a new `MethodFilterPlugin` from a list of allowed HTTP method names.
    ///
    /// Standard HTTP methods (`GET`..`TRACE`, discriminants `1..=9`) are packed into a
    /// 16-bit integer bitmask. Non-standard methods (e.g. WebDAV `PROPFIND`) are stored
    /// in `custom_methods` for fallback comparison when `HttpMethod::Unknown` is encountered.
    pub fn try_new(
        id: impl Into<String>,
        allowed_methods: Vec<String>,
    ) -> Result<Self, PluginError> {
        let id = id.into();
        if allowed_methods.is_empty() {
            return Err(PluginError::Config(format!(
                "Plugin '{id}': allowed_methods cannot be empty"
            )));
        }

        let mut allowed_mask: u16 = 0;
        let mut custom_methods: Vec<String> = Vec::new();

        for raw in allowed_methods {
            let upper = raw.trim().to_ascii_uppercase();
            if upper.is_empty() {
                return Err(PluginError::Config(format!(
                    "Plugin '{id}': allowed_methods cannot contain empty entries"
                )));
            }
            let method = HttpMethod::from_bytes(upper.as_bytes());
            if method != HttpMethod::Unknown {
                allowed_mask |= 1u16 << (method as u8);
            } else {
                custom_methods.push(upper);
            }
        }

        custom_methods.sort_unstable();
        custom_methods.dedup();

        Ok(Self {
            id,
            allowed_mask,
            custom_methods,
        })
    }

    /// Instantiates `MethodFilterPlugin` from a declarative [`PluginConfig`].
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let cfg: MethodFilterConfig =
            serde_json::from_value(config.config.clone()).map_err(|e| {
                PluginError::Config(format!(
                    "Invalid method_filter config for '{}': {e}",
                    config.id
                ))
            })?;

        Self::try_new(&config.id, cfg.allowed_methods)
    }
}

impl PreRoutePlugin for MethodFilterPlugin {
    #[inline(always)]
    fn id(&self) -> &str {
        &self.id
    }

    #[inline(always)]
    fn pbww(&self) -> u32 {
        0
    }

    #[inline]
    fn on_request(&self, req: &mut L7Request) -> Action {
        let method_id: u8 = match req.method {
            http::Method::GET => HttpMethod::Get as u8,
            http::Method::POST => HttpMethod::Post as u8,
            http::Method::PUT => HttpMethod::Put as u8,
            http::Method::DELETE => HttpMethod::Delete as u8,
            http::Method::HEAD => HttpMethod::Head as u8,
            http::Method::OPTIONS => HttpMethod::Options as u8,
            http::Method::PATCH => HttpMethod::Patch as u8,
            http::Method::CONNECT => HttpMethod::Connect as u8,
            http::Method::TRACE => HttpMethod::Trace as u8,
            _ => HttpMethod::Unknown as u8,
        };
        let bit = 1u16 << method_id;

        if (self.allowed_mask & bit) != 0 {
            return Action::Continue;
        }

        if method_id == 0 && !self.custom_methods.is_empty() {
            let raw = req.method.as_str();
            if self
                .custom_methods
                .iter()
                .any(|m| m.eq_ignore_ascii_case(raw))
            {
                return Action::Continue;
            }
        }

        Action::Reject(RejectReason::MethodNotAllowed)
    }

    fn capability(&self) -> PluginCapability {
        PluginCapability::header_only(AccessLevel::Read)
    }

    fn supported_protocols(&self) -> SupportedProtocols {
        SupportedProtocols::ALL_HTTP
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{Method, Uri, Version};
    use serde_json::json;
    use velda_core::{Body, HeaderWorkspace};

    fn make_req(method: Method) -> L7Request {
        L7Request::new(
            method,
            "/api/v1/resource".parse::<Uri>().unwrap(),
            Version::HTTP_11,
            HeaderWorkspace::new(0),
            Body::Empty,
        )
    }

    #[test]
    fn test_allows_configured_standard_methods_and_blocks_others() {
        let plugin = MethodFilterPlugin::try_new(
            "method_filter",
            vec!["GET".to_string(), "POST".to_string(), "OPTIONS".to_string()],
        )
        .unwrap();

        assert!(matches!(
            plugin.on_request(&mut make_req(Method::GET)),
            Action::Continue
        ));
        assert!(matches!(
            plugin.on_request(&mut make_req(Method::POST)),
            Action::Continue
        ));
        assert!(matches!(
            plugin.on_request(&mut make_req(Method::OPTIONS)),
            Action::Continue
        ));

        assert!(matches!(
            plugin.on_request(&mut make_req(Method::DELETE)),
            Action::Reject(RejectReason::MethodNotAllowed)
        ));
        assert!(matches!(
            plugin.on_request(&mut make_req(Method::TRACE)),
            Action::Reject(RejectReason::MethodNotAllowed)
        ));
        assert!(matches!(
            plugin.on_request(&mut make_req(Method::CONNECT)),
            Action::Reject(RejectReason::MethodNotAllowed)
        ));
    }

    #[test]
    fn test_blocks_unknown_custom_verbs_when_only_standard_methods_configured() {
        let plugin = MethodFilterPlugin::try_new("method_filter", vec!["GET".to_string()]).unwrap();
        let propfind = Method::from_bytes(b"PROPFIND").unwrap();
        assert!(matches!(
            plugin.on_request(&mut make_req(propfind)),
            Action::Reject(RejectReason::MethodNotAllowed)
        ));
    }

    #[test]
    fn test_allows_explicit_custom_webdav_methods() {
        let plugin = MethodFilterPlugin::try_new(
            "method_filter",
            vec!["GET".to_string(), "PROPFIND".to_string()],
        )
        .unwrap();

        let propfind = Method::from_bytes(b"PROPFIND").unwrap();
        let mkcol = Method::from_bytes(b"MKCOL").unwrap();
        assert!(matches!(
            plugin.on_request(&mut make_req(propfind)),
            Action::Continue
        ));
        assert!(matches!(
            plugin.on_request(&mut make_req(mkcol)),
            Action::Reject(RejectReason::MethodNotAllowed)
        ));
    }

    #[test]
    fn test_from_config_deserialization() {
        let cfg = PluginConfig {
            id: "method_filter".to_string(),
            config: json!({
                "allowed_methods": ["GET", "HEAD"]
            }),
        };
        let plugin = MethodFilterPlugin::from_config(&cfg).unwrap();
        assert!(matches!(
            plugin.on_request(&mut make_req(Method::HEAD)),
            Action::Continue
        ));
        assert!(matches!(
            plugin.on_request(&mut make_req(Method::POST)),
            Action::Reject(RejectReason::MethodNotAllowed)
        ));
    }
}
