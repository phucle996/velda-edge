//! Pre-Route Web Application Firewall Lite Plugin (`waf_lite`).
//!
//! Provides ultra-low-latency L7 ingress perimeter defense before route lookup
//! and body streaming by inspecting request URI paths, query parameters, User-Agents,
//! and headers against known attack signatures in single-digit nanoseconds.

use std::str::FromStr;

use http::StatusCode;
use http::header::{HeaderName, HeaderValue};
use velda_core::{Action, L7Request, L7Response};
use velda_sync::post_sync::plugin::PluginConfig;

use super::scanner::{ThreatVector, WafMatcher, with_normalized};
use crate::capability::{AccessLevel, PluginCapability, SupportedProtocols};
use crate::error::PluginError;
use crate::pre_route::PreRoutePlugin;

/// Operational mode of the WAF Lite inspection engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WafMode {
    /// Rejects malicious requests immediately with HTTP 403 Forbidden (Default).
    #[default]
    Block,
    /// Bypasses blocking and injects audit headers for observability and backend inspection.
    DetectOnly,
}

impl WafMode {
    pub fn parse(s: &str) -> Self {
        match s.trim().to_ascii_lowercase().as_str() {
            "detect" | "detect_only" | "monitor" => Self::DetectOnly,
            _ => Self::Block,
        }
    }
}

/// Inspection scope configuring which parts of the incoming HTTP request are evaluated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WafScope {
    pub check_path: bool,
    pub check_query: bool,
    pub check_user_agent: bool,
    pub check_headers: Vec<HeaderName>,
}

impl Default for WafScope {
    fn default() -> Self {
        Self {
            check_path: true,
            check_query: true,
            check_user_agent: true,
            check_headers: Vec::new(),
        }
    }
}

/// Pre-Route Web Application Firewall Lite Plugin.
pub struct WafLitePlugin {
    pub id: String,
    pub mode: WafMode,
    pub scope: WafScope,
    pub custom_blocked_patterns: Vec<String>,
    pub custom_blocked_user_agents: Vec<String>,
    pub matcher: WafMatcher,
    forbidden_response: L7Response,
}

impl WafLitePlugin {
    /// Constructs a new `WafLitePlugin` from raw configuration parameters.
    pub fn new(
        id: impl Into<String>,
        mode: WafMode,
        scope: WafScope,
        custom_blocked_patterns: Vec<String>,
        custom_blocked_user_agents: Vec<String>,
    ) -> Self {
        let forbidden_response = L7Response::from_bytes(
            StatusCode::FORBIDDEN,
            "{\"error\":\"Forbidden: Malicious payload detected\"}",
        )
        .with_header(
            http::header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );

        let custom_patterns_lower: Vec<String> = custom_blocked_patterns
            .into_iter()
            .map(|p| p.trim().to_ascii_lowercase())
            .filter(|p| !p.is_empty())
            .collect();

        let custom_ua_lower: Vec<String> = custom_blocked_user_agents
            .into_iter()
            .map(|ua| ua.trim().to_ascii_lowercase())
            .filter(|ua| !ua.is_empty())
            .collect();

        let matcher = WafMatcher::new(&custom_patterns_lower, &custom_ua_lower)
            .expect("Failed to initialize WafMatcher");

        Self {
            id: id.into(),
            mode,
            scope,
            custom_blocked_patterns: custom_patterns_lower,
            custom_blocked_user_agents: custom_ua_lower,
            matcher,
            forbidden_response,
        }
    }

    /// Deserializes and constructs an instance directly from a [`PluginConfig`].
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let raw = config.config.as_object().ok_or_else(|| {
            PluginError::Config(format!("Plugin '{}' config must be an object", config.id))
        })?;

        let mode = raw
            .get("mode")
            .and_then(|v| v.as_str())
            .map(WafMode::parse)
            .unwrap_or(WafMode::Block);

        let check_path = raw
            .get("check_path")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);

        let check_query = raw
            .get("check_query")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);

        let check_user_agent = raw
            .get("check_user_agent")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);

        let mut check_headers = Vec::new();
        if let Some(hdrs_arr) = raw.get("check_headers").and_then(|v| v.as_array()) {
            for item in hdrs_arr {
                if let Some(s) = item.as_str() {
                    let hname = HeaderName::from_str(s).map_err(|e| {
                        PluginError::Config(format!(
                            "Invalid check_header '{s}' in plugin '{}': {e}",
                            config.id
                        ))
                    })?;
                    check_headers.push(hname);
                }
            }
        }

        let mut custom_patterns = Vec::new();
        if let Some(arr) = raw
            .get("custom_blocked_patterns")
            .and_then(|v| v.as_array())
        {
            for item in arr {
                if let Some(s) = item.as_str() {
                    custom_patterns.push(s.to_string());
                }
            }
        }

        let mut custom_user_agents = Vec::new();
        if let Some(arr) = raw
            .get("custom_blocked_user_agents")
            .and_then(|v| v.as_array())
        {
            for item in arr {
                if let Some(s) = item.as_str() {
                    custom_user_agents.push(s.to_string());
                }
            }
        }

        let scope = WafScope {
            check_path,
            check_query,
            check_user_agent,
            check_headers,
        };

        Ok(Self::new(
            &config.id,
            mode,
            scope,
            custom_patterns,
            custom_user_agents,
        ))
    }

    #[inline]
    fn handle_threat(&self, req: &mut L7Request, vector: ThreatVector) -> Action {
        match self.mode {
            WafMode::Block => Action::Respond(self.forbidden_response.clone()),
            WafMode::DetectOnly => {
                req.headers
                    .insert("x-waf-threat-detected", HeaderValue::from_static("true"));
                if let Ok(val) = HeaderValue::from_str(vector.as_str()) {
                    req.headers.insert("x-waf-threat-type", val);
                }
                Action::Continue
            }
        }
    }
}

impl PreRoutePlugin for WafLitePlugin {
    #[inline(always)]
    fn id(&self) -> &str {
        &self.id
    }

    fn on_request(&self, req: &mut L7Request) -> Action {
        // 1. Inspect URI Path (zero-allocation + SIMD Aho-Corasick)
        if self.scope.check_path {
            let path = req.uri.path();
            let threat = with_normalized(path, |norm| self.matcher.check_patterns(norm));
            if let Some((_, vector)) = threat {
                return self.handle_threat(req, vector);
            }
        }

        // 2. Inspect Query String (zero-allocation + SIMD Aho-Corasick)
        if self.scope.check_query
            && let Some(query) = req.uri.query()
        {
            let threat = with_normalized(query, |norm| self.matcher.check_patterns(norm));
            if let Some((_, vector)) = threat {
                return self.handle_threat(req, vector);
            }
        }

        // 3. Inspect User-Agent Header (zero-allocation + SIMD Aho-Corasick)
        if self.scope.check_user_agent
            && let Some(ua) = req.headers.get(http::header::USER_AGENT)
            && let Ok(ua_str) = ua.to_str()
        {
            let matched = with_normalized(ua_str, |norm| self.matcher.check_user_agent(norm));
            if matched.is_some() {
                return self.handle_threat(req, ThreatVector::MaliciousScanner);
            }
        }

        // 4. Inspect Custom Configured Headers (zero-allocation + SIMD Aho-Corasick)
        for hdr_name in &self.scope.check_headers {
            if let Some(val) = req.headers.get(hdr_name)
                && let Ok(s) = val.to_str()
            {
                let threat = with_normalized(s, |norm| self.matcher.check_patterns(norm));
                if let Some((_, vector)) = threat {
                    return self.handle_threat(req, vector);
                }
            }
        }

        Action::Continue
    }

    #[inline(always)]
    fn capability(&self) -> PluginCapability {
        match self.mode {
            WafMode::Block => PluginCapability::header_only(AccessLevel::Read),
            WafMode::DetectOnly => PluginCapability::header_only(AccessLevel::Write),
        }
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
    fn test_waf_blocks_path_traversal() {
        let plugin = WafLitePlugin::new(
            "waf_lite",
            WafMode::Block,
            WafScope::default(),
            vec![],
            vec![],
        );

        let mut req = build_test_request(
            "/api/v1/download?file=..%2f..%2fetc/passwd",
            HeaderMap::new(),
        );
        let action = plugin.on_request(&mut req);

        match action {
            Action::Respond(resp) => {
                assert_eq!(resp.status, StatusCode::FORBIDDEN);
                assert_eq!(
                    resp.headers.get("content-type").unwrap(),
                    "application/json"
                );
            }
            _ => panic!("Expected Action::Respond(403)"),
        }
    }

    #[test]
    fn test_waf_blocks_sql_injection_in_query() {
        let plugin = WafLitePlugin::new(
            "waf_lite",
            WafMode::Block,
            WafScope::default(),
            vec![],
            vec![],
        );

        let mut req = build_test_request(
            "/api/v1/items?id=1%27%20UNION%20SELECT%20*%20FROM%20users--",
            HeaderMap::new(),
        );
        let action = plugin.on_request(&mut req);

        match action {
            Action::Respond(resp) => assert_eq!(resp.status, StatusCode::FORBIDDEN),
            _ => panic!("Expected Action::Respond(403)"),
        }
    }

    #[test]
    fn test_waf_blocks_xss_probe() {
        let plugin = WafLitePlugin::new(
            "waf_lite",
            WafMode::Block,
            WafScope::default(),
            vec![],
            vec![],
        );

        let mut req = build_test_request(
            "/search?query=%3Cscript%3Ealert(1)%3C/script%3E",
            HeaderMap::new(),
        );
        let action = plugin.on_request(&mut req);

        match action {
            Action::Respond(resp) => assert_eq!(resp.status, StatusCode::FORBIDDEN),
            _ => panic!("Expected Action::Respond(403)"),
        }
    }

    #[test]
    fn test_waf_blocks_scanner_user_agent() {
        let plugin = WafLitePlugin::new(
            "waf_lite",
            WafMode::Block,
            WafScope::default(),
            vec![],
            vec![],
        );

        let mut headers = HeaderMap::new();
        headers.insert(
            http::header::USER_AGENT,
            "sqlmap/1.6.1#stable".parse().unwrap(),
        );

        let mut req = build_test_request("/api/v1/users", headers);
        let action = plugin.on_request(&mut req);

        match action {
            Action::Respond(resp) => assert_eq!(resp.status, StatusCode::FORBIDDEN),
            _ => panic!("Expected Action::Respond(403)"),
        }
    }

    #[test]
    fn test_waf_detect_only_mode_allows_request_and_injects_headers() {
        let plugin = WafLitePlugin::new(
            "waf_lite",
            WafMode::DetectOnly,
            WafScope::default(),
            vec![],
            vec![],
        );

        let mut req = build_test_request("/api/v1/files?path=..%2fboot.ini", HeaderMap::new());
        let action = plugin.on_request(&mut req);

        assert!(matches!(action, Action::Continue));
        assert_eq!(req.headers.get("x-waf-threat-detected").unwrap(), "true");
        assert_eq!(
            req.headers.get("x-waf-threat-type").unwrap(),
            "path_traversal"
        );
    }

    #[test]
    fn test_waf_allows_clean_request() {
        let plugin = WafLitePlugin::new(
            "waf_lite",
            WafMode::Block,
            WafScope::default(),
            vec![],
            vec![],
        );

        let mut headers = HeaderMap::new();
        headers.insert(
            http::header::USER_AGENT,
            "Mozilla/5.0 Chrome/120.0".parse().unwrap(),
        );

        let mut req = build_test_request("/api/v1/orders/12345?format=json", headers);
        let action = plugin.on_request(&mut req);

        assert!(matches!(action, Action::Continue));
        assert!(!req.headers.contains_key("x-waf-threat-detected"));
    }

    #[test]
    fn test_from_config_complete_deserialization() {
        let config = PluginConfig {
            id: "waf_lite".into(),
            phase: String::new(),
            config: json!({
                "mode": "detect_only",
                "check_path": true,
                "check_query": true,
                "check_user_agent": true,
                "check_headers": ["referer"],
                "custom_blocked_patterns": ["${jndi:"],
                "custom_blocked_user_agents": ["nmap-scanner"]
            }),
        };

        let plugin = WafLitePlugin::from_config(&config).expect("valid config");
        assert_eq!(plugin.id, "waf_lite");
        assert_eq!(plugin.mode, WafMode::DetectOnly);
        assert_eq!(plugin.scope.check_headers.len(), 1);
        assert!(
            plugin
                .custom_blocked_patterns
                .contains(&"${jndi:".to_string())
        );
        assert!(
            plugin
                .custom_blocked_user_agents
                .contains(&"nmap-scanner".to_string())
        );

        let mut headers = HeaderMap::new();
        headers.insert(
            "referer",
            "http://attacker.com?payload=${jndi:ldap}".parse().unwrap(),
        );
        let mut req = build_test_request("/api/login", headers);

        let action = plugin.on_request(&mut req);
        assert!(matches!(action, Action::Continue));
        assert_eq!(req.headers.get("x-waf-threat-detected").unwrap(), "true");
    }
}
