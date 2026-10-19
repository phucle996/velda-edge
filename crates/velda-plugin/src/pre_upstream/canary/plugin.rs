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

use std::hash::Hasher;
use std::sync::atomic::{AtomicU64, Ordering};

use http::header::{COOKIE, HeaderName, HeaderValue};
use rustc_hash::FxHasher;
use velda_core::{Action, L7Request};
use velda_sync::post_sync::plugin::PluginConfig;

use crate::capability::{AccessLevel, PluginCapability, SupportedProtocols};
use crate::error::PluginError;
use crate::pre_upstream::PreUpstreamPlugin;

static CANARY_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Key source used for consistent hashing to ensure session stickiness.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum HashSource {
    /// Uniform pseudo-random distribution per request (default).
    #[default]
    Random,
    /// Hashes client IP address (resolved from `x-forwarded-for` or `x-real-ip`).
    ClientIp,
    /// Hashes a specific request header (e.g., `x-user-id`, `authorization`, `x-account-id`).
    Header(String),
    /// Hashes a specific cookie value (e.g., `session_id`, `user_token`).
    Cookie(String),
}

impl HashSource {
    /// Parses a hash source definition from configuration format.
    pub fn parse(source: &str, key: Option<&str>) -> Self {
        match source.to_ascii_lowercase().as_str() {
            "client_ip" | "ip" => Self::ClientIp,
            "header" => Self::Header(key.unwrap_or("x-user-id").to_string()),
            "cookie" => Self::Cookie(key.unwrap_or("session_id").to_string()),
            _ => Self::Random,
        }
    }
}

/// Conditional rule matching against request headers to force canary routing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeaderRule {
    /// Target header name to evaluate (case-insensitive).
    pub header: String,
    /// Exact value match (e.g., `exact: "beta"` or `exact: "always"`).
    pub exact: Option<String>,
    /// Prefix value match (e.g., `prefix: "internal-"`).
    pub prefix: Option<String>,
    /// Triggers if the header exists regardless of value.
    pub exists: bool,
}

impl HeaderRule {
    /// Evaluates this rule against the request headers.
    #[inline]
    pub fn matches(&self, req: &L7Request) -> bool {
        let val = req.headers.get(&self.header).and_then(|v| v.to_str().ok());

        match val {
            Some(v) => {
                if self.exists {
                    return true;
                }
                if let Some(ref exact_val) = self.exact
                    && v.eq_ignore_ascii_case(exact_val)
                {
                    return true;
                }
                if let Some(ref prefix_val) = self.prefix
                    && v.starts_with(prefix_val)
                {
                    return true;
                }
                false
            }
            None => false,
        }
    }
}

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

    /// Extracts cookie value by name from the standard `Cookie` header.
    #[inline]
    fn extract_cookie<'a>(req: &'a L7Request, cookie_name: &str) -> Option<&'a str> {
        let cookie_header = req.headers.get(COOKIE)?.to_str().ok()?;
        for pair in cookie_header.split(';') {
            let mut parts = pair.splitn(2, '=');
            if let Some(k) = parts.next()
                && k.trim().eq_ignore_ascii_case(cookie_name)
            {
                return parts.next().map(|v| v.trim());
            }
        }
        None
    }

    /// Computes a stable hash modulo 100 (0..=99) from a key byte slice.
    #[inline]
    fn hash_to_percentage(key: &[u8]) -> u32 {
        let mut hasher = FxHasher::default();
        hasher.write(key);
        (hasher.finish() % 100) as u32
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
        match &self.config.hash_by {
            HashSource::Random => {
                let counter = CANARY_COUNTER.fetch_add(1, Ordering::Relaxed);
                (counter % 100) < (self.config.weight as u64)
            }
            HashSource::ClientIp => {
                let ip_str = req
                    .headers
                    .get("x-forwarded-for")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|s| s.split(',').next())
                    .map(|s| s.trim())
                    .or_else(|| {
                        req.headers
                            .get("x-real-ip")
                            .and_then(|v| v.to_str().ok())
                            .map(|s| s.trim())
                    })
                    .unwrap_or("127.0.0.1");

                Self::hash_to_percentage(ip_str.as_bytes()) < self.config.weight
            }
            HashSource::Header(hdr_name) => {
                if let Some(val) = req.headers.get(hdr_name).and_then(|v| v.to_str().ok()) {
                    Self::hash_to_percentage(val.as_bytes()) < self.config.weight
                } else {
                    // Fallback to random if expected header is absent
                    let counter = CANARY_COUNTER.fetch_add(1, Ordering::Relaxed);
                    (counter % 100) < (self.config.weight as u64)
                }
            }
            HashSource::Cookie(cookie_name) => {
                if let Some(val) = Self::extract_cookie(req, cookie_name) {
                    Self::hash_to_percentage(val.as_bytes()) < self.config.weight
                } else {
                    // Fallback to random if expected cookie is absent
                    let counter = CANARY_COUNTER.fetch_add(1, Ordering::Relaxed);
                    (counter % 100) < (self.config.weight as u64)
                }
            }
        }
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

    fn build_test_request(headers: HeaderMap) -> L7Request {
        L7Request::new(
            Method::GET,
            Uri::from_static("/api/orders"),
            Version::HTTP_11,
            headers,
            Body::Empty,
        )
    }

    #[test]
    fn test_canary_rule_match_overrides_zero_weight() {
        let config = CanaryConfig {
            canary_upstream: "orders-v2".into(),
            weight: 0, // 0% weight
            rules: vec![HeaderRule {
                header: "x-user-role".into(),
                exact: Some("beta".into()),
                prefix: None,
                exists: false,
            }],
            hash_by: HashSource::Random,
            canary_header: "x-velda-canary".into(),
            canary_header_value: "true".into(),
        };

        let plugin = CanaryPlugin::new("canary-test", config).unwrap();

        // 1. Regular user: weight is 0 -> should NOT route to canary
        let mut req_regular = build_test_request(HeaderMap::new());
        plugin.on_upstream_request(&mut req_regular);
        assert!(!req_regular.headers.contains_key("x-velda-canary"));

        // 2. Beta user: rule matches -> MUST route to canary
        let mut headers_beta = HeaderMap::new();
        headers_beta.insert("x-user-role", HeaderValue::from_static("beta"));
        let mut req_beta = build_test_request(headers_beta);
        plugin.on_upstream_request(&mut req_beta);

        assert_eq!(req_beta.headers.get("x-velda-canary").unwrap(), "true");
        assert_eq!(
            req_beta.headers.get("x-velda-upstream").unwrap(),
            "orders-v2"
        );
    }

    #[test]
    fn test_canary_consistent_hashing_by_user_id() {
        let config = CanaryConfig {
            canary_upstream: "orders-v2".into(),
            weight: 50, // 50% split
            rules: Vec::new(),
            hash_by: HashSource::Header("x-user-id".into()),
            canary_header: "x-velda-canary".into(),
            canary_header_value: "true".into(),
        };

        let plugin = CanaryPlugin::new("canary-hash", config).unwrap();

        // Consistent stickiness check: 10 calls for user "usr-100" must ALWAYS give the identical result!
        let mut headers = HeaderMap::new();
        headers.insert("x-user-id", HeaderValue::from_static("usr-100"));
        let req1 = build_test_request(headers.clone());
        let initial_decision = plugin.should_route_to_canary(&req1);

        for _ in 0..10 {
            let req = build_test_request(headers.clone());
            assert_eq!(plugin.should_route_to_canary(&req), initial_decision);
        }
    }

    #[test]
    fn test_canary_cookie_extraction() {
        let config = CanaryConfig {
            canary_upstream: "orders-v2".into(),
            weight: 50,
            rules: Vec::new(),
            hash_by: HashSource::Cookie("session_id".into()),
            canary_header: "x-velda-canary".into(),
            canary_header_value: "true".into(),
        };

        let plugin = CanaryPlugin::new("canary-cookie", config).unwrap();

        let mut headers = HeaderMap::new();
        headers.insert(
            COOKIE,
            HeaderValue::from_static("theme=dark; session_id=sess_abc12345; lang=vi"),
        );
        let req = build_test_request(headers);

        let cookie_val = CanaryPlugin::extract_cookie(&req, "session_id");
        assert_eq!(cookie_val, Some("sess_abc12345"));

        let initial_decision = plugin.should_route_to_canary(&req);
        for _ in 0..10 {
            assert_eq!(plugin.should_route_to_canary(&req), initial_decision);
        }
    }

    #[test]
    fn test_canary_from_config_complete() {
        let json_cfg = serde_json::json!({
            "canary_upstream": "catalog-v2",
            "weight": 25,
            "rules": [
                { "header": "x-canary", "exact": "always" },
                { "header": "x-env", "prefix": "staging" }
            ],
            "hash_by": {
                "source": "header",
                "key": "x-account-id"
            },
            "canary_header": "x-app-canary",
            "canary_header_value": "1"
        });

        let config = PluginConfig {
            id: "canary".into(),
            phase: String::new(),
            config: json_cfg,
        };

        let plugin = CanaryPlugin::from_config(&config).expect("valid config");
        assert_eq!(plugin.id, "canary");
        assert_eq!(plugin.config.canary_upstream, "catalog-v2");
        assert_eq!(plugin.config.weight, 25);
        assert_eq!(plugin.config.rules.len(), 2);
        assert_eq!(
            plugin.config.hash_by,
            HashSource::Header("x-account-id".into())
        );

        let mut headers = HeaderMap::new();
        headers.insert("x-canary", HeaderValue::from_static("always"));
        let mut req = build_test_request(headers);

        plugin.on_upstream_request(&mut req);

        assert_eq!(req.headers.get("x-app-canary").unwrap(), "1");
        assert_eq!(req.headers.get("x-velda-upstream").unwrap(), "catalog-v2");
    }
}
