//! Header Mutation Pre-Upstream Plugin (`header_mutation`).
//!
//! Adds, mutates, renames, or strips HTTP headers before proxying requests to upstream backends.
//! Provides support for:
//! - Stripping RFC 9110 / RFC 7230 hop-by-hop headers via [`super::rule::strip_hop_by_hop_headers`].
//! - Removing sensitive or spoofable headers.
//! - Renaming headers to match upstream expectations.
//! - Setting static or dynamic variable headers via [`super::variable::DynamicVar`].
//! - Appending values to existing headers (e.g. `X-Forwarded-For`).

use http::header::{HeaderName, HeaderValue};
use velda_core::{Action, L7Request};
use velda_sync::post_sync::plugin::PluginConfig;

use super::rule::strip_hop_by_hop_headers;
use super::variable::HeaderValueSpec;
use crate::capability::{AccessLevel, PluginCapability, SupportedProtocols};
use crate::error::PluginError;
use crate::pre_upstream::PreUpstreamPlugin;

/// Configuration settings for the header mutation plugin.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeaderMutationConfig {
    /// Strips RFC 9110 / RFC 7230 hop-by-hop headers before upstream forwarding.
    pub strip_hop_by_hop: bool,
    /// Headers to be explicitly stripped before upstream dispatch.
    pub remove: Vec<HeaderName>,
    /// Headers to be renamed from `(source_name, target_name)`.
    pub rename: Vec<(HeaderName, HeaderName)>,
    /// Headers to be set or overwritten.
    pub set: Vec<(HeaderName, HeaderValueSpec)>,
    /// Headers to be appended to existing values.
    pub append: Vec<(HeaderName, HeaderValueSpec)>,
}

impl Default for HeaderMutationConfig {
    fn default() -> Self {
        Self {
            strip_hop_by_hop: true,
            remove: Vec::new(),
            rename: Vec::new(),
            set: Vec::new(),
            append: Vec::new(),
        }
    }
}

/// Pre-Upstream Header Mutation Plugin.
#[derive(Debug, Clone)]
pub struct HeaderMutationPlugin {
    pub id: String,
    pub config: HeaderMutationConfig,
}

impl HeaderMutationPlugin {
    /// Constructs a new header mutation plugin with set and remove lists (backward compatibility).
    pub fn new(
        id: impl Into<String>,
        set_headers: Vec<(HeaderName, HeaderValue)>,
        remove_headers: Vec<HeaderName>,
    ) -> Self {
        let set = set_headers
            .into_iter()
            .map(|(k, v)| (k, HeaderValueSpec::Static(v)))
            .collect();
        Self::with_config(
            id,
            HeaderMutationConfig {
                strip_hop_by_hop: true,
                remove: remove_headers,
                rename: Vec::new(),
                set,
                append: Vec::new(),
            },
        )
    }

    /// Constructs a new header mutation plugin with complete configuration.
    pub fn with_config(id: impl Into<String>, config: HeaderMutationConfig) -> Self {
        Self {
            id: id.into(),
            config,
        }
    }

    /// Deserializes and constructs an instance directly from a [`PluginConfig`].
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let defaults = HeaderMutationConfig::default();

        if config.config.is_null() {
            return Ok(Self::with_config(&config.id, defaults));
        }

        let raw = config.config.as_object().ok_or_else(|| {
            PluginError::Config(format!("Plugin '{}' config must be an object", config.id))
        })?;

        // 1. strip_hop_by_hop (defaults to true)
        let strip_hop_by_hop = raw
            .get("strip_hop_by_hop")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);

        // 2. remove list
        let mut remove = Vec::new();
        if let Some(rem_arr) = raw.get("remove").and_then(|v| v.as_array()) {
            for item in rem_arr {
                if let Some(s) = item.as_str() {
                    let name = HeaderName::from_bytes(s.as_bytes()).map_err(|e| {
                        PluginError::Config(format!("Invalid remove header name '{s}': {e}"))
                    })?;
                    remove.push(name);
                }
            }
        }

        // 3. rename map
        let mut rename = Vec::new();
        if let Some(rename_obj) = raw.get("rename").and_then(|v| v.as_object()) {
            for (from_k, to_v) in rename_obj {
                let from_name = HeaderName::from_bytes(from_k.as_bytes()).map_err(|e| {
                    PluginError::Config(format!("Invalid rename source header '{from_k}': {e}"))
                })?;
                let to_str = to_v.as_str().ok_or_else(|| {
                    PluginError::Config(format!(
                        "Invalid rename target header for '{from_k}': must be string"
                    ))
                })?;
                let to_name = HeaderName::from_bytes(to_str.as_bytes()).map_err(|e| {
                    PluginError::Config(format!("Invalid rename target header '{to_str}': {e}"))
                })?;
                rename.push((from_name, to_name));
            }
        }

        // 4. set map
        let mut set = Vec::new();
        if let Some(set_obj) = raw.get("set").and_then(|v| v.as_object()) {
            for (k, v) in set_obj {
                let name = HeaderName::from_bytes(k.as_bytes()).map_err(|e| {
                    PluginError::Config(format!("Invalid set header name '{k}': {e}"))
                })?;
                let spec_str = v.as_str().ok_or_else(|| {
                    PluginError::Config(format!("Set header value for '{k}' must be string"))
                })?;
                let spec = HeaderValueSpec::parse(spec_str)?;
                set.push((name, spec));
            }
        }

        // 5. append map
        let mut append = Vec::new();
        if let Some(append_obj) = raw.get("append").and_then(|v| v.as_object()) {
            for (k, v) in append_obj {
                let name = HeaderName::from_bytes(k.as_bytes()).map_err(|e| {
                    PluginError::Config(format!("Invalid append header name '{k}': {e}"))
                })?;
                let spec_str = v.as_str().ok_or_else(|| {
                    PluginError::Config(format!("Append header value for '{k}' must be string"))
                })?;
                let spec = HeaderValueSpec::parse(spec_str)?;
                append.push((name, spec));
            }
        }

        Ok(Self::with_config(
            &config.id,
            HeaderMutationConfig {
                strip_hop_by_hop,
                remove,
                rename,
                set,
                append,
            },
        ))
    }
}

impl PreUpstreamPlugin for HeaderMutationPlugin {
    #[inline(always)]
    fn id(&self) -> &str {
        &self.id
    }

    fn on_upstream_request(&self, req: &mut L7Request) -> Action {
        // Step 1: Strip RFC Hop-by-hop headers if configured
        if self.config.strip_hop_by_hop {
            strip_hop_by_hop_headers(&mut req.headers);
        }

        // Step 2: Remove blacklisted / unneeded headers
        for name in &self.config.remove {
            req.headers.remove(name);
        }

        // Step 3: Rename headers
        for (from_name, to_name) in &self.config.rename {
            if from_name != to_name {
                let values: Vec<HeaderValue> =
                    req.headers.get_all(from_name).iter().cloned().collect();
                if !values.is_empty() {
                    req.headers.remove(from_name);
                    for v in values {
                        req.headers.append(to_name.clone(), v);
                    }
                }
            }
        }

        // Step 4: Set / overwrite headers
        for (name, spec) in &self.config.set {
            if let Some(val) = spec.resolve(req) {
                req.headers.insert(name.clone(), val);
            }
        }

        // Step 5: Append headers
        for (name, spec) in &self.config.append {
            if let Some(val) = spec.resolve(req) {
                req.headers.append(name.clone(), val);
            }
        }

        Action::Continue
    }

    #[inline(always)]
    fn capability(&self) -> PluginCapability {
        PluginCapability::header_only(AccessLevel::Write)
    }

    #[inline(always)]
    fn supported_protocols(&self) -> SupportedProtocols {
        SupportedProtocols::ALL_L7
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{HeaderMap, Method, Uri, Version};
    use velda_core::Body;

    #[test]
    fn test_backward_compatibility_new() {
        let set = vec![(
            HeaderName::from_static("x-custom-gateway"),
            HeaderValue::from_static("velda"),
        )];
        let remove = vec![HeaderName::from_static("x-sensitive")];
        let plugin = HeaderMutationPlugin::new("hm-1", set, remove);

        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("x-sensitive"),
            HeaderValue::from_static("secret"),
        );
        let mut req = L7Request::new(
            Method::GET,
            Uri::from_static("/test"),
            Version::HTTP_11,
            headers,
            Body::Empty,
        );

        let action = plugin.on_upstream_request(&mut req);
        assert!(matches!(action, Action::Continue));
        assert!(!req.headers.contains_key("x-sensitive"));
        assert_eq!(req.headers.get("x-custom-gateway").unwrap(), "velda");
    }

    #[test]
    fn test_header_rename() {
        let config = HeaderMutationConfig {
            strip_hop_by_hop: false,
            remove: Vec::new(),
            rename: vec![(
                HeaderName::from_static("x-legacy-token"),
                HeaderName::from_static("authorization"),
            )],
            set: Vec::new(),
            append: Vec::new(),
        };
        let plugin = HeaderMutationPlugin::with_config("hm-rename", config);

        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("x-legacy-token"),
            HeaderValue::from_static("Bearer abc123xyz"),
        );
        let mut req = L7Request::new(
            Method::GET,
            Uri::from_static("/test"),
            Version::HTTP_11,
            headers,
            Body::Empty,
        );

        plugin.on_upstream_request(&mut req);
        assert!(!req.headers.contains_key("x-legacy-token"));
        assert_eq!(
            req.headers.get("authorization").unwrap(),
            "Bearer abc123xyz"
        );
    }

    #[test]
    fn test_dynamic_variables_integration() {
        let config = HeaderMutationConfig {
            strip_hop_by_hop: false,
            remove: Vec::new(),
            rename: Vec::new(),
            set: vec![
                (
                    HeaderName::from_static("x-client-ip"),
                    HeaderValueSpec::parse("$client_ip").unwrap(),
                ),
                (
                    HeaderName::from_static("x-req-id"),
                    HeaderValueSpec::parse("$request_id").unwrap(),
                ),
                (
                    HeaderName::from_static("x-method"),
                    HeaderValueSpec::parse("$method").unwrap(),
                ),
                (
                    HeaderName::from_static("x-path"),
                    HeaderValueSpec::parse("$path").unwrap(),
                ),
            ],
            append: vec![(
                HeaderName::from_static("x-forwarded-for"),
                HeaderValueSpec::parse("$client_ip").unwrap(),
            )],
        };
        let plugin = HeaderMutationPlugin::with_config("hm-dynamic", config);

        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("x-real-ip"),
            HeaderValue::from_static("198.51.100.25"),
        );
        headers.insert(
            HeaderName::from_static("x-forwarded-for"),
            HeaderValue::from_static("192.0.2.1"),
        );

        let mut req = L7Request::new(
            Method::PUT,
            Uri::from_static("/v1/resources/99"),
            Version::HTTP_11,
            headers,
            Body::Empty,
        );

        plugin.on_upstream_request(&mut req);

        assert_eq!(req.headers.get("x-client-ip").unwrap(), "192.0.2.1");
        assert_eq!(req.headers.get("x-method").unwrap(), "PUT");
        assert_eq!(req.headers.get("x-path").unwrap(), "/v1/resources/99");
        assert!(req.headers.contains_key("x-req-id"));

        let xff: Vec<_> = req.headers.get_all("x-forwarded-for").iter().collect();
        assert_eq!(xff.len(), 2);
        assert_eq!(xff[0], "192.0.2.1");
        assert_eq!(xff[1], "192.0.2.1");
    }
}
