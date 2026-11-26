//! Request Termination Pre-Upstream Plugin (`request_termination::plugin`).
//!
//! ```text
//!   Sync / Config Compile
//!     RequestTerminationConfig ──► validate_config_fields()
//!       └──► HeaderWorkspace::empty(cap) + Body::Empty|Bytes ──► base_response: L7Response
//!
//!   Frontline Worker Hot Path (SupportedProtocols::ALL_HTTP, AccessLevel::None)
//!     on_upstream_request(_req, _targets)
//!       └──► Action::Respond(self.base_response.clone())
//!              └─► velda-edge cancels targets, injects x-request-id, runs post_response_hooks, sends wire response
//! ```

use bytes::Bytes;
use http::header::{CONTENT_LENGTH, CONTENT_TYPE, HeaderName, HeaderValue};
use http::{StatusCode, Version};
use velda_core::{Action, Body, HeaderWorkspace, L7Request, L7Response};

use super::config::{RequestTerminationConfig, forbids_content_length};
use super::validation::{validate_config_fields, validate_request_termination_config};
use crate::capability::{PluginCapability, SupportedProtocols};
use crate::config::PluginConfig;
use crate::error::PluginError;
use crate::pre_upstream::PreUpstreamPlugin;

/// Lightweight pre-upstream plugin returning a pre-compiled synthetic [`L7Response`].
#[repr(C, align(64))]
#[derive(Debug, Clone)]
pub struct RequestTerminationPlugin {
    base_response: L7Response,
    pub id: String,
    pub config: RequestTerminationConfig,
}

impl RequestTerminationPlugin {
    /// Compiles and validates a [`RequestTerminationPlugin`] from declarative configuration.
    pub fn new(
        id: impl Into<String>,
        config: RequestTerminationConfig,
    ) -> Result<Self, PluginError> {
        let id = id.into();
        validate_config_fields(&id, &config)?;

        let status = StatusCode::from_u16(config.status_code).map_err(|e| {
            PluginError::Config(format!(
                "Plugin '{id}': invalid status_code {}: {e}",
                config.status_code
            ))
        })?;

        let has_body = !config.body.is_empty();
        let emit_cl = !forbids_content_length(config.status_code);
        let slot_cap = config.headers.len() + usize::from(has_body) + usize::from(emit_cl);

        // Allocate only custom/framing slots with zero startup clock syscall or dummy x-request-id.
        let mut header_ws = HeaderWorkspace::empty(slot_cap);

        let mut custom_headers: Vec<(HeaderName, HeaderValue)> =
            Vec::with_capacity(config.headers.len());
        for (k, v) in &config.headers {
            let hname = HeaderName::from_bytes(k.as_bytes()).map_err(|e| {
                PluginError::Config(format!("Plugin '{id}': invalid header name '{k}': {e}"))
            })?;
            let hval = HeaderValue::from_str(v).map_err(|e| {
                PluginError::Config(format!(
                    "Plugin '{id}': invalid header value for '{k}': {e}"
                ))
            })?;
            custom_headers.push((hname, hval));
        }
        custom_headers.sort_unstable_by(|a, b| a.0.as_str().cmp(b.0.as_str()));

        for (hname, hval) in custom_headers {
            header_ws.insert(hname, hval);
        }

        if has_body {
            let ct = HeaderValue::from_str(&config.content_type).map_err(|e| {
                PluginError::Config(format!("Plugin '{id}': invalid content_type: {e}"))
            })?;
            header_ws.insert(CONTENT_TYPE, ct);
        }

        if emit_cl {
            let mut itoa_buf = itoa::Buffer::new();
            let cl = HeaderValue::from_str(itoa_buf.format(config.body.len()))
                .unwrap_or_else(|_| HeaderValue::from_static("0"));
            header_ws.insert(CONTENT_LENGTH, cl);
        }

        let body = if has_body {
            Body::Bytes(Bytes::copy_from_slice(config.body.as_bytes()))
        } else {
            Body::Empty
        };

        let base_response = L7Response::new(status, Version::HTTP_11, header_ws, body);

        Ok(Self {
            base_response,
            id,
            config,
        })
    }

    /// Parses, validates, and constructs an instance from raw [`PluginConfig`].
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let cfg = validate_request_termination_config(&config.id, &config.config)?;
        Self::new(&config.id, cfg)
    }

    /// Returns a reference to the pre-compiled synthetic [`L7Response`].
    #[inline(always)]
    pub fn base_response(&self) -> &L7Response {
        &self.base_response
    }
}

impl PreUpstreamPlugin for RequestTerminationPlugin {
    #[inline(always)]
    fn id(&self) -> &str {
        &self.id
    }

    #[inline(always)]
    fn pbww(&self) -> u32 {
        0
    }

    #[inline(always)]
    fn on_upstream_request(&self, _req: &mut L7Request, _targets: &mut u32) -> Action {
        Action::Respond(self.base_response.clone())
    }

    #[inline(always)]
    fn capability(&self) -> PluginCapability {
        PluginCapability::default()
    }

    #[inline(always)]
    fn supported_protocols(&self) -> SupportedProtocols {
        SupportedProtocols::ALL_HTTP
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use http::{Method, Uri};

    fn empty_req() -> L7Request {
        L7Request::new(
            Method::GET,
            Uri::from_static("/test"),
            Version::HTTP_11,
            HeaderWorkspace::empty(0),
            Body::Empty,
        )
    }

    #[test]
    fn test_exact_body_and_no_dummy_request_id() {
        let cfg = RequestTerminationConfig {
            status_code: 503,
            body: r#"{"error":"maintenance"}"#.to_string(),
            content_type: "application/json".to_string(),
            headers: HashMap::from([("Retry-After".into(), "300".into())]),
        };
        let plugin = RequestTerminationPlugin::new("maint", cfg).unwrap();

        let mut req = empty_req();
        let mut target = 1;
        match plugin.on_upstream_request(&mut req, &mut target) {
            Action::Respond(resp) => {
                assert_eq!(resp.status, StatusCode::SERVICE_UNAVAILABLE);
                assert!(resp.headers.get("x-request-id").is_none());
                assert_eq!(resp.headers.get("retry-after").unwrap(), "300");
                assert_eq!(
                    resp.headers.get("content-type").unwrap(),
                    "application/json"
                );
                assert_eq!(resp.headers.get("content-length").unwrap(), "23");
                assert_eq!(resp.body.as_slice(), br#"{"error":"maintenance"}"#);
            }
            _ => panic!("Expected Action::Respond"),
        }
    }

    #[test]
    fn test_empty_body_200_and_301_redirect_emit_zero_length_without_content_type() {
        let redirect = RequestTerminationPlugin::new(
            "redir",
            RequestTerminationConfig {
                status_code: 301,
                body: String::new(),
                content_type: String::new(),
                headers: HashMap::from([("location".into(), "https://example.com/v2".into())]),
            },
        )
        .unwrap();

        let mut req = empty_req();
        let mut target = 1;
        match redirect.on_upstream_request(&mut req, &mut target) {
            Action::Respond(resp) => {
                assert_eq!(resp.status, StatusCode::MOVED_PERMANENTLY);
                assert_eq!(resp.body, Body::Empty);
                assert!(resp.headers.get("content-type").is_none());
                assert_eq!(resp.headers.get("content-length").unwrap(), "0");
                assert_eq!(
                    resp.headers.get("location").unwrap(),
                    "https://example.com/v2"
                );
            }
            _ => panic!("Expected Action::Respond"),
        }
    }

    #[test]
    fn test_204_and_304_omit_content_length_and_reject_body() {
        for status in [204, 304] {
            let ok = RequestTerminationPlugin::new(
                "no-body",
                RequestTerminationConfig {
                    status_code: status,
                    body: String::new(),
                    content_type: String::new(),
                    headers: HashMap::new(),
                },
            )
            .unwrap();
            let resp = ok.base_response();
            assert_eq!(resp.status.as_u16(), status);
            assert_eq!(resp.body, Body::Empty);
            assert!(resp.headers.get("content-length").is_none());
            assert!(resp.headers.get("content-type").is_none());

            let err = RequestTerminationPlugin::new(
                "bad-body",
                RequestTerminationConfig {
                    status_code: status,
                    body: "illegal".into(),
                    content_type: "text/plain".into(),
                    headers: HashMap::new(),
                },
            );
            assert!(err.is_err());
        }
    }

    #[test]
    fn test_from_config_json() {
        let plugin_cfg = PluginConfig {
            id: "request_termination".into(),
            config: serde_json::json!({
                "status_code": 418,
                "body": "I'm a teapot\n",
                "content_type": "text/plain",
                "headers": {
                    "x-tea-type": "EarlGrey"
                }
            }),
        };

        let plugin = RequestTerminationPlugin::from_config(&plugin_cfg).unwrap();
        assert_eq!(plugin.base_response().status.as_u16(), 418);
        assert_eq!(
            plugin.base_response().headers.get("x-tea-type").unwrap(),
            "EarlGrey"
        );
    }
}
