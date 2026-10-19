//! Canary Traffic Splitting Pre-Upstream Plugin (`canary`).
//!
//! Provides controlled, progressive traffic steering between baseline and canary upstreams
//! based on:
//! 1. **Rule-Based Overrides**: Direct routing for internal testers, specific roles (e.g. `X-User-Role: beta`),
//!    or explicit canary headers (e.g. `X-Canary: always`).
//! 2. **Consistent Session Hashing**: Ensures the same user or client (hashed by `User-Id`, Cookie, or Client IP)
//!    remains pinned to the same version throughout their session, preventing split-session checkout/auth errors.
//! 3. **Weight-Based Percentage**: Configurable percentage (0% to 100%) for gradual rollout.
//! 4. **Backend Canary Marker Injection**: Injects an informational header (default `X-Velda-Canary: true`)
//!    so the target backend microservice can tag logs, trigger feature flags, or activate sandbox modes.

use http::header::{HeaderName, HeaderValue};
use velda_core::{Action, L7Request};
use velda_sync::post_sync::plugin::PluginConfig;

use super::strategy::{HashSource, HeaderRule};
use crate::capability::{AccessLevel, PluginCapability, SupportedProtocols};
use crate::error::PluginError;
use crate::pre_upstream::PreUpstreamPlugin;

/// Configuration settings for the Canary Traffic Splitting plugin.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanaryConfig {
    /// Name of the target canary upstream (e.g., `"orders-v2"`).
    pub canary_upstream: String,
    /// Percentage of traffic directed to the canary upstream (0 to 100).
    pub weight: u32,
    /// Explicit header rules that force canary routing when matched.
    pub rules: Vec<HeaderRule>,
    /// Key source used for consistent hashing.
    pub hash_by: HashSource,
    /// Header name injected into upstream request to mark canary status.
    pub canary_header: String,
    /// Header value injected (defaults to `"true"`).
    pub canary_header_value: String,
}

impl Default for CanaryConfig {
    fn default() -> Self {
        Self {
            canary_upstream: String::new(),
            weight: 0,
            rules: Vec::new(),
            hash_by: HashSource::default(),
            canary_header: "x-velda-canary".into(),
            canary_header_value: "true".into(),
        }
    }
}

/// Canary Traffic Splitting Pre-Upstream Plugin.
#[derive(Debug, Clone)]
pub struct CanaryPlugin {
    pub id: String,
    pub config: CanaryConfig,
    canary_header_name: HeaderName,
    canary_header_val: HeaderValue,
    canary_upstream_val: HeaderValue,
    upstream_override_name: HeaderName,
}

impl CanaryPlugin {
    /// Constructs a new canary plugin with full configuration.
    pub fn new(id: impl Into<String>, config: CanaryConfig) -> Result<Self, PluginError> {
        let canary_header_name = HeaderName::from_bytes(config.canary_header.as_bytes())
            .map_err(|e| PluginError::Config(format!("Invalid canary_header: {e}")))?;

        let canary_header_val = HeaderValue::from_str(&config.canary_header_value)
            .map_err(|e| PluginError::Config(format!("Invalid canary_header_value: {e}")))?;

        let canary_upstream_val = HeaderValue::from_str(&config.canary_upstream)
            .map_err(|e| PluginError::Config(format!("Invalid canary_upstream: {e}")))?;

        let upstream_override_name = HeaderName::from_static("x-velda-upstream");

        Ok(Self {
            id: id.into(),
            config,
            canary_header_name,
            canary_header_val,
            canary_upstream_val,
            upstream_override_name,
        })
    }

    /// Deserializes and constructs an instance directly from a [`PluginConfig`].
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let raw = config.config.as_object().ok_or_else(|| {
            PluginError::Config(format!("Plugin '{}' config must be an object", config.id))
        })?;

        // 1. canary_upstream (required)
        let canary_upstream = raw
            .get("canary_upstream")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                PluginError::Config(format!(
                    "Plugin '{}' requires 'canary_upstream' string parameter",
                    config.id
                ))
            })?
            .to_string();

        // 2. weight (0 to 100, defaults to 0)
        let weight = raw
            .get("weight")
            .and_then(|v| v.as_u64())
            .map(|w| (w as u32).min(100))
            .unwrap_or(0);

        // 3. rules
        let mut rules = Vec::new();
        if let Some(rules_arr) = raw.get("rules").and_then(|v| v.as_array()) {
            for item in rules_arr {
                if let Some(r_obj) = item.as_object()
                    && let Some(hdr) = r_obj.get("header").and_then(|v| v.as_str())
                {
                    let exact = r_obj
                        .get("exact")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string());
                    let prefix = r_obj
                        .get("prefix")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string());
                    let exists = r_obj
                        .get("exists")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(exact.is_none() && prefix.is_none());

                    rules.push(HeaderRule {
                        header: hdr.to_ascii_lowercase(),
                        exact,
                        prefix,
                        exists,
                    });
                }
            }
        }

        // 4. hash_by
        let hash_by = if let Some(hash_obj) = raw.get("hash_by").and_then(|v| v.as_object()) {
            let src = hash_obj
                .get("source")
                .and_then(|v| v.as_str())
                .unwrap_or("random");
            let key = hash_obj.get("key").and_then(|v| v.as_str());
            HashSource::parse(src, key)
        } else if let Some(src_str) = raw.get("hash_by").and_then(|v| v.as_str()) {
            HashSource::parse(src_str, None)
        } else {
            HashSource::Random
        };

        // 5. canary_header
        let canary_header = raw
            .get("canary_header")
            .and_then(|v| v.as_str())
            .unwrap_or("x-velda-canary")
            .to_string();

        // 6. canary_header_value
        let canary_header_value = raw
            .get("canary_header_value")
            .and_then(|v| v.as_str())
            .unwrap_or("true")
            .to_string();

        Self::new(
            &config.id,
            CanaryConfig {
                canary_upstream,
                weight,
                rules,
                hash_by,
                canary_header,
                canary_header_value,
            },
        )
    }

    /// Evaluates whether the incoming request should be routed to the canary upstream.
    #[inline]
    pub fn should_route_to_canary(&self, req: &L7Request) -> bool {
        // Step 1: Rule-based override (highest priority)
        for rule in &self.config.rules {
            if rule.matches(req) {
                return true;
            }
        }

        // Step 2: Weight boundaries
        if self.config.weight == 0 {
            return false;
        }
        if self.config.weight >= 100 {
            return true;
        }

        // Step 3: Consistent Hashing / Weight Distribution
        self.config.hash_by.evaluate_percentage(req) < self.config.weight
    }
}

impl PreUpstreamPlugin for CanaryPlugin {
    #[inline(always)]
    fn id(&self) -> &str {
        &self.id
    }

    fn on_upstream_request(&self, req: &mut L7Request) -> Action {
        if self.should_route_to_canary(req) {
            // 1. Inject informational marker header for backend telemetry/sandbox
            req.headers.insert(
                self.canary_header_name.clone(),
                self.canary_header_val.clone(),
            );

            // 2. Set upstream override target for Data Plane leasing
            req.headers.insert(
                self.upstream_override_name.clone(),
                self.canary_upstream_val.clone(),
            );
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
    fn test_canary_rule_based_override() {
        let config = CanaryConfig {
            canary_upstream: "payments-v2".into(),
            weight: 0, // 0% random weight
            rules: vec![HeaderRule {
                header: "x-canary".into(),
                exact: Some("always".into()),
                prefix: None,
                exists: false,
            }],
            hash_by: HashSource::Random,
            canary_header: "x-velda-canary".into(),
            canary_header_value: "true".into(),
        };

        let plugin = CanaryPlugin::new("canary-test", config).unwrap();

        // 1. Request with matching rule
        let mut headers = HeaderMap::new();
        headers.insert("x-canary", HeaderValue::from_static("always"));
        let mut req1 = L7Request::new(
            Method::GET,
            Uri::from_static("/v1/pay"),
            Version::HTTP_11,
            headers,
            Body::Empty,
        );
        let action1 = plugin.on_upstream_request(&mut req1);
        assert!(matches!(action1, Action::Continue));
        assert_eq!(req1.headers.get("x-velda-canary").unwrap(), "true");
        assert_eq!(req1.headers.get("x-velda-upstream").unwrap(), "payments-v2");

        // 2. Request without matching rule
        let mut req2 = L7Request::new(
            Method::GET,
            Uri::from_static("/v1/pay"),
            Version::HTTP_11,
            HeaderMap::new(),
            Body::Empty,
        );
        let action2 = plugin.on_upstream_request(&mut req2);
        assert!(matches!(action2, Action::Continue));
        assert!(!req2.headers.contains_key("x-velda-canary"));
        assert!(!req2.headers.contains_key("x-velda-upstream"));
    }

    #[test]
    fn test_canary_full_weight() {
        let config = CanaryConfig {
            canary_upstream: "catalog-v2".into(),
            weight: 100, // 100% canary
            rules: Vec::new(),
            hash_by: HashSource::Random,
            canary_header: "x-velda-canary".into(),
            canary_header_value: "true".into(),
        };

        let plugin = CanaryPlugin::new("canary-100", config).unwrap();
        let mut req = L7Request::new(
            Method::GET,
            Uri::from_static("/items"),
            Version::HTTP_11,
            HeaderMap::new(),
            Body::Empty,
        );
        plugin.on_upstream_request(&mut req);
        assert_eq!(req.headers.get("x-velda-canary").unwrap(), "true");
        assert_eq!(req.headers.get("x-velda-upstream").unwrap(), "catalog-v2");
    }
}
