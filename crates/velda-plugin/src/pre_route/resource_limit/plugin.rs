//! Pre-Route Resource Limit Plugin (`resource_limit`).
//!
//! Provides ultra-low-latency ingress boundary defense before routing, body streaming,
//! and upstream connection leasing by enforcing strict thresholds on URI length,
//! header counts, and cumulative header bytes.

use http::header::HeaderName;
use velda_core::{Action, L7Request, RejectReason};
use velda_sync::post_sync::plugin::PluginConfig;

use crate::capability::{AccessLevel, PluginCapability, SupportedProtocols};
use crate::error::PluginError;
use crate::pre_route::PreRoutePlugin;

/// Configurable thresholds for request URI and header resource consumption.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResourceLimits {
    /// Maximum allowed total URI length in bytes (Authority + Path + Query string).
    pub max_uri_length: usize,
    /// Maximum allowed number of distinct header entries.
    pub max_headers_count: usize,
    /// Maximum allowed cumulative size of all headers in bytes (names + values + wire framing).
    pub max_header_bytes: usize,
    /// Maximum allowed size for any single header name, value, or multi-value aggregate in bytes.
    pub max_single_header_bytes: usize,
}

impl Default for ResourceLimits {
    fn default() -> Self {
        Self {
            max_uri_length: 4096,
            max_headers_count: 64,
            max_header_bytes: 16384,
            max_single_header_bytes: 8192,
        }
    }
}

/// Pre-Route Resource Limit Plugin.
#[derive(Debug, Clone)]
pub struct ResourceLimitPlugin {
    pub id: String,
    pub limits: ResourceLimits,
}

impl ResourceLimitPlugin {
    /// Constructs a new `ResourceLimitPlugin`.
    pub fn new(id: impl Into<String>, limits: ResourceLimits) -> Self {
        Self {
            id: id.into(),
            limits,
        }
    }

    /// Deserializes and constructs an instance directly from a [`PluginConfig`].
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let cfg = velda_sync::validate_resource_limit_config(&config.id, &config.config).map_err(
            |e| {
                PluginError::Config(format!(
                    "Invalid resource_limit configuration for '{}': {e}",
                    config.id
                ))
            },
        )?;

        Ok(Self::new(
            &config.id,
            ResourceLimits {
                max_uri_length: cfg.max_uri_length,
                max_headers_count: cfg.max_headers_count,
                max_header_bytes: cfg.max_header_bytes,
                max_single_header_bytes: cfg.max_single_header_bytes,
            },
        ))
    }
}

impl PreRoutePlugin for ResourceLimitPlugin {
    #[inline(always)]
    fn id(&self) -> &str {
        &self.id
    }

    fn on_request(&self, req: &mut L7Request) -> Action {
        // 1. Inspect Full URI Length: Authority + Path + Query String
        // Fully evaluates absolute-form URIs (RFC 9112 §3.2.2) to prevent authority smuggling
        let uri_len = req
            .uri
            .path_and_query()
            .map_or_else(|| req.uri.path().len(), |pq| pq.as_str().len())
            + req.uri.authority().map_or(0, |a| a.as_str().len());

        if uri_len > self.limits.max_uri_length {
            return Action::Reject(RejectReason::UriTooLong);
        }

        // 2. Inspect Header Count
        // Prevents header bombing and guarantees bounded execution time in subsequent loop
        let header_count = req.headers.len();
        if header_count > self.limits.max_headers_count {
            return Action::Reject(RejectReason::HeaderFieldsTooLarge);
        }

        // 3. Branch-Minimized Single-Pass Inspection of Headers:
        // - Tracks total wire framing bytes (name + ": " + val + "\r\n")
        // - Defends against Header Name Bombing (name_len > max_single_header_bytes)
        // - Defends against Multi-Value Header Fragmentation Bypass by aggregating consecutive values of the same key
        // - Employs branchless CMOV (.max) inside loop, deferring threshold check to a single bitwise OR test
        let mut prev_name: Option<&HeaderName> = None;
        let mut current_key_val_bytes = 0usize;
        let mut max_single_header_seen = 0usize;
        let mut total_header_bytes = 0usize;

        for (name, val) in &req.headers {
            let name_len = name.as_str().len();
            let val_len = val.as_bytes().len();

            // Wire framing estimate: name + ": " (2 bytes) + value + "\r\n" (2 bytes)
            total_header_bytes += name_len + val_len + 4;

            // Header name itself cannot exceed single header threshold (Anti-Name-Bombing)
            max_single_header_seen = max_single_header_seen.max(name_len);

            if prev_name == Some(name) {
                // Fragmented header split across multiple lines: accumulate combined logical value
                current_key_val_bytes += val_len + 2; // +2 for ", " delimiter in logical join
            } else {
                prev_name = Some(name);
                current_key_val_bytes = val_len;
            }
            max_single_header_seen = max_single_header_seen.max(current_key_val_bytes);
        }

        if (max_single_header_seen > self.limits.max_single_header_bytes)
            | (total_header_bytes > self.limits.max_header_bytes)
        {
            return Action::Reject(RejectReason::HeaderFieldsTooLarge);
        }

        Action::Continue
    }

    #[inline(always)]
    fn capability(&self) -> PluginCapability {
        PluginCapability::header_only(AccessLevel::Read)
    }

    #[inline(always)]
    fn supported_protocols(&self) -> SupportedProtocols {
        SupportedProtocols::ALL_HTTP
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{HeaderMap, Method, Uri, Version};
    use serde_json::json;
    use velda_core::Body;

    fn build_test_request(path_and_query: &str, headers: HeaderMap) -> L7Request {
        L7Request::new(
            Method::GET,
            path_and_query.parse::<Uri>().unwrap(),
            Version::HTTP_11,
            headers,
            Body::Empty,
        )
    }

    #[test]
    fn test_normal_request_within_limits_passes() {
        let plugin = ResourceLimitPlugin::new("resource_limit", ResourceLimits::default());
        let mut headers = HeaderMap::new();
        headers.insert("accept", "application/json".parse().unwrap());
        headers.insert("user-agent", "Mozilla/5.0".parse().unwrap());

        let mut req = build_test_request("/api/v1/users?page=1&limit=20", headers);
        let action = plugin.on_request(&mut req);

        assert!(matches!(action, Action::Continue));
    }

    #[test]
    fn test_uri_too_long_returns_414_with_cpdos_defense() {
        let limits = ResourceLimits {
            max_uri_length: 30,
            ..Default::default()
        };
        let plugin = ResourceLimitPlugin::new("resource_limit", limits);

        // Path (14) + '?' + Query (20) = 35 bytes > 30 bytes
        let mut req = build_test_request("/api/v1/orders?filter_date=2026-10-19", HeaderMap::new());
        let action = plugin.on_request(&mut req);
        assert!(matches!(action, Action::Reject(RejectReason::UriTooLong)));
    }

    #[test]
    fn test_absolute_form_uri_authority_overflow_returns_414() {
        let limits = ResourceLimits {
            max_uri_length: 50,
            ..Default::default()
        };
        let plugin = ResourceLimitPlugin::new("resource_limit", limits);

        // Absolute-form URI with huge authority string: http://very-long-gateway-domain-name-exceeding-limit.example.com/api
        let mut req = build_test_request(
            "http://very-long-gateway-domain-name-exceeding-limit.example.com/api",
            HeaderMap::new(),
        );
        let action = plugin.on_request(&mut req);
        assert!(matches!(action, Action::Reject(RejectReason::UriTooLong)));
    }

    #[test]
    fn test_headers_count_exceeded_returns_431() {
        let limits = ResourceLimits {
            max_headers_count: 2,
            ..Default::default()
        };
        let plugin = ResourceLimitPlugin::new("resource_limit", limits);

        let mut headers = HeaderMap::new();
        headers.insert("h1", "v1".parse().unwrap());
        headers.insert("h2", "v2".parse().unwrap());
        headers.insert("h3", "v3".parse().unwrap());

        let mut req = build_test_request("/api/test", headers);
        let action = plugin.on_request(&mut req);
        assert!(matches!(
            action,
            Action::Reject(RejectReason::HeaderFieldsTooLarge)
        ));
    }

    #[test]
    fn test_total_header_bytes_exceeded_returns_431() {
        let limits = ResourceLimits {
            max_header_bytes: 40,
            ..Default::default()
        };
        let plugin = ResourceLimitPlugin::new("resource_limit", limits);

        let mut headers = HeaderMap::new();
        // name (11) + ": " (2) + val (20) + "\r\n" (2) = 35
        headers.insert("x-custom-h1", "01234567890123456789".parse().unwrap());
        // Second header pushes it well beyond 40 bytes
        headers.insert("x-custom-h2", "01234567890123456789".parse().unwrap());

        let mut req = build_test_request("/api/test", headers);
        let action = plugin.on_request(&mut req);
        assert!(matches!(
            action,
            Action::Reject(RejectReason::HeaderFieldsTooLarge)
        ));
    }

    #[test]
    fn test_single_header_bytes_exceeded_returns_431() {
        let limits = ResourceLimits {
            max_single_header_bytes: 20,
            ..Default::default()
        };
        let plugin = ResourceLimitPlugin::new("resource_limit", limits);

        let mut headers = HeaderMap::new();
        // Single value of 25 bytes > 20 bytes threshold
        headers.insert(
            "x-large-cookie",
            "session_id=123456789012345".parse().unwrap(),
        );

        let mut req = build_test_request("/api/test", headers);
        let action = plugin.on_request(&mut req);
        assert!(matches!(
            action,
            Action::Reject(RejectReason::HeaderFieldsTooLarge)
        ));
    }

    #[test]
    fn test_header_name_bombing_defense_returns_431() {
        let limits = ResourceLimits {
            max_single_header_bytes: 30,
            max_header_bytes: 1024,
            ..Default::default()
        };
        let plugin = ResourceLimitPlugin::new("resource_limit", limits);

        let mut headers = HeaderMap::new();
        // Header name of 35 bytes > 30 byte max_single_header_bytes, even though value is only 1 byte
        let long_name = "x-an-extremely-long-header-name-bomb";
        headers.insert(
            HeaderName::from_bytes(long_name.as_bytes()).unwrap(),
            "1".parse().unwrap(),
        );

        let mut req = build_test_request("/api/test", headers);
        let action = plugin.on_request(&mut req);
        assert!(matches!(
            action,
            Action::Reject(RejectReason::HeaderFieldsTooLarge)
        ));
    }

    #[test]
    fn test_multi_value_header_fragmentation_bypass_returns_431() {
        let limits = ResourceLimits {
            max_single_header_bytes: 30,
            max_header_bytes: 1024,
            ..Default::default()
        };
        let plugin = ResourceLimitPlugin::new("resource_limit", limits);

        let mut headers = HeaderMap::new();
        // Attacker sends 2 separate Cookie headers, each 18 bytes (<= 30 byte threshold),
        // but combined logical header is 18 + 2 + 18 = 38 bytes (> 30 byte threshold)
        headers.append("cookie", "session_token=1234".parse().unwrap());
        headers.append("cookie", "session_token=5678".parse().unwrap());

        let mut req = build_test_request("/api/test", headers);
        let action = plugin.on_request(&mut req);
        assert!(matches!(
            action,
            Action::Reject(RejectReason::HeaderFieldsTooLarge)
        ));
    }

    #[test]
    fn test_from_config_custom_and_defaults() {
        let config = PluginConfig {
            id: "resource_limit".into(),
            phase: String::new(),
            config: json!({
                "max_uri_length": 8192,
                "max_headers_count": 32,
                "max_header_bytes": 32768,
                "max_single_header_bytes": 4096
            }),
        };

        let plugin = ResourceLimitPlugin::from_config(&config).expect("valid config");
        assert_eq!(plugin.id, "resource_limit");
        assert_eq!(plugin.limits.max_uri_length, 8192);
        assert_eq!(plugin.limits.max_headers_count, 32);
        assert_eq!(plugin.limits.max_header_bytes, 32768);
        assert_eq!(plugin.limits.max_single_header_bytes, 4096);

        // Incomplete/empty config must fail validation (100% explicit configuration required)
        let empty_config = PluginConfig {
            id: "resource_limit".into(),
            phase: String::new(),
            config: json!({}),
        };
        assert!(ResourceLimitPlugin::from_config(&empty_config).is_err());
    }
}
