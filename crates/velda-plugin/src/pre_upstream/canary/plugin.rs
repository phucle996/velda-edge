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
use std::sync::Arc;
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
    canary_upstream: Arc<str>,
}

impl CanaryPlugin {
    /// Constructs a new canary plugin with full configuration.
    pub fn new(id: impl Into<String>, config: CanaryConfig) -> Result<Self, PluginError> {
        let canary_header_name = HeaderName::from_bytes(config.canary_header.as_bytes())
            .map_err(|e| PluginError::Config(format!("Invalid canary_header: {e}")))?;

        let canary_header_val = HeaderValue::from_str(&config.canary_header_value)
            .map_err(|e| PluginError::Config(format!("Invalid canary_header_value: {e}")))?;

        let canary_upstream: Arc<str> = Arc::from(config.canary_upstream.as_str());

        Ok(Self {
            id: id.into(),
            config,
            canary_header_name,
            canary_header_val,
            canary_upstream,
        })
    }

    /// Deserializes and constructs an instance directly from a [`PluginConfig`].
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let cfg = velda_sync::validate_canary_config(&config.id, &config.config).map_err(|e| {
            PluginError::Config(format!(
                "Invalid canary configuration for '{}': {e}",
                config.id
            ))
        })?;

        let mut rules = Vec::with_capacity(cfg.rules.len());
        for r in &cfg.rules {
            let (exact, prefix, exists) = match r.operator.to_ascii_lowercase().as_str() {
                "equals" => (Some(r.value.clone()), None, false),
                "prefix" => (None, Some(r.value.clone()), false),
                "exists" => (None, None, true),
                _ => (Some(r.value.clone()), None, false),
            };
            rules.push(HeaderRule {
                header: r.name.to_ascii_lowercase(),
                exact,
                prefix,
                exists,
            });
        }

        let hash_by = HashSource::parse(&cfg.hash_by, None);

        Self::new(
            &config.id,
            CanaryConfig {
                canary_upstream: cfg.canary_upstream,
                weight: cfg.weight,
                rules,
                hash_by,
                canary_header: cfg.canary_header,
                canary_header_value: cfg.canary_header_value,
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

    fn on_upstream_request(&self, req: &mut L7Request, target_upstream: &mut Arc<str>) -> Action {
        if self.should_route_to_canary(req) {
            // 1. Inject informational marker header for backend telemetry/sandbox
            req.headers.insert(
                self.canary_header_name.clone(),
                self.canary_header_val.clone(),
            );

            // 2. Direct in-memory target update without touching HTTP headers
            *target_upstream = Arc::clone(&self.canary_upstream);
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
        let mut target1 = Arc::from("payments-v1");
        let action1 = plugin.on_upstream_request(&mut req1, &mut target1);
        assert!(matches!(action1, Action::Continue));
        assert_eq!(req1.headers.get("x-velda-canary").unwrap(), "true");
        assert_eq!(target1.as_ref(), "payments-v2");
        assert!(!req1.headers.contains_key("x-velda-upstream"));

        // 2. Request without matching rule
        let mut req2 = L7Request::new(
            Method::GET,
            Uri::from_static("/v1/pay"),
            Version::HTTP_11,
            HeaderMap::new(),
            Body::Empty,
        );
        let mut target2 = Arc::from("payments-v1");
        let action2 = plugin.on_upstream_request(&mut req2, &mut target2);
        assert!(matches!(action2, Action::Continue));
        assert!(!req2.headers.contains_key("x-velda-canary"));
        assert_eq!(target2.as_ref(), "payments-v1");
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
        let mut target = Arc::from("catalog-v1");
        plugin.on_upstream_request(&mut req, &mut target);
        assert_eq!(req.headers.get("x-velda-canary").unwrap(), "true");
        assert_eq!(target.as_ref(), "catalog-v2");
        assert!(!req.headers.contains_key("x-velda-upstream"));
    }

    #[test]
    fn test_canary_from_config() {
        let json_cfg = serde_json::json!({
            "canary_upstream": "catalog-v2",
            "weight": 20,
            "rules": [
                {
                    "name": "x-canary",
                    "operator": "equals",
                    "value": "always"
                }
            ],
            "hash_by": "client_ip",
            "canary_header": "x-velda-canary",
            "canary_header_value": "true"
        });
        let config = PluginConfig {
            id: "canary".into(),
            phase: String::new(),
            config: json_cfg,
        };
        let plugin = CanaryPlugin::from_config(&config).expect("valid config");
        assert_eq!(plugin.id, "canary");
        assert_eq!(plugin.config.canary_upstream, "catalog-v2");
        assert_eq!(plugin.config.weight, 20);
        assert_eq!(plugin.config.rules.len(), 1);

        // Empty config must fail validation
        let bad_config = PluginConfig {
            id: "canary".into(),
            phase: String::new(),
            config: serde_json::json!({}),
        };
        assert!(CanaryPlugin::from_config(&bad_config).is_err());
    }
}
