//! Pre-Route Web Application Firewall Lite Plugin (`waf_lite`).
//!
//! ```text
//!  +─────────────────────────────────────────────────────────────────────────+
//!  |                      WAF_LITE EXECUTION PIPELINE                        |
//!  +─────────────────────────────────────────────────────────────────────────+
//!  |                                                                         |
//!  |  Inbound L7Request                                                      |
//!  |        │                                                                |
//!  |        ├──► [Empty Patterns?] ──► YES ──► Action::Continue              |
//!  |        │                                                                |
//!  |        ├──► 1. URI Path  (req.uri.path().as_bytes(), plus = false)      |
//!  |        ├──► 2. URI Query (req.uri.query().as_bytes(), plus = true)      |
//!  |        └──► 3. Body Slice (req.body[..min(len, 32KB)], plus = false)    |
//!  |                                │                                        |
//!  |                       [Any Pattern Matched?]                            |
//!  |                         /                 \                             |
//!  |                       YES                  NO                           |
//!  |                       /                     \                           |
//!  |     Action::Reject(RejectReason::WafBlocked)   Action::Continue         |
//!  |     (Wire: HTTP 403 Forbidden generic,                                  |
//!  |      Internal Telemetry: waf_blocked)                                   |
//!  +─────────────────────────────────────────────────────────────────────────+
//! ```

use velda_core::{Action, L7Request, RejectReason};

use super::config::{MAX_BODY_SCAN_BYTES, WafLiteConfig};
use super::scanner::WafMatcher;
use crate::capability::{AccessLevel, PluginCapability, SupportedProtocols};
use crate::config::PluginConfig;
use crate::error::PluginError;
use crate::pre_route::PreRoutePlugin;

/// Pre-Route Web Application Firewall Lite Plugin.
pub struct WafLitePlugin {
    pub id: String,
    pub patterns: Vec<String>,
    pub matcher: WafMatcher,
}

impl WafLitePlugin {
    /// Constructs a new `WafLitePlugin` from explicit attack patterns.
    ///
    /// Normalizes (`trim` + ASCII lowercase) and deduplicates patterns before compiling
    /// the SIMD Aho-Corasick state machine.
    pub fn try_new(id: impl Into<String>, patterns: Vec<String>) -> Result<Self, PluginError> {
        let mut deduped = Vec::with_capacity(patterns.len());
        for p in patterns {
            let lower = p.trim().to_ascii_lowercase();
            if !lower.is_empty() {
                deduped.push(lower);
            }
        }
        deduped.sort_unstable();
        deduped.dedup();

        let matcher = WafMatcher::new(&deduped)?;
        Ok(Self {
            id: id.into(),
            patterns: deduped,
            matcher,
        })
    }

    /// Instantiates `WafLitePlugin` from a declarative [`PluginConfig`].
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let cfg: WafLiteConfig = serde_json::from_value(config.config.clone()).map_err(|e| {
            PluginError::Config(format!("Invalid waf_lite config for '{}': {e}", config.id))
        })?;

        Self::try_new(&config.id, cfg.patterns)
    }
}

impl PreRoutePlugin for WafLitePlugin {
    #[inline(always)]
    fn id(&self) -> &str {
        &self.id
    }

    #[inline(always)]
    fn pbww(&self) -> u32 {
        0
    }

    fn on_request(&self, req: &mut L7Request) -> Action {
        if self.matcher.is_empty() {
            return Action::Continue;
        }

        // 1. Inspect URI Path and Query String
        if self.matcher.check_bytes(req.uri.path().as_bytes(), false) {
            return Action::Reject(RejectReason::WafBlocked);
        }
        if let Some(query) = req.uri.query()
            && self.matcher.check_bytes(query.as_bytes(), true)
        {
            return Action::Reject(RejectReason::WafBlocked);
        }

        // 2. Inspect leading Payload bytes up to MAX_BODY_SCAN_BYTES (32 KB)
        if !req.body.is_empty() {
            let body_slice = req.body.as_slice();
            let cap = body_slice.len().min(MAX_BODY_SCAN_BYTES);
            if self.matcher.check_bytes(&body_slice[..cap], false) {
                return Action::Reject(RejectReason::WafBlocked);
            }
        }

        Action::Continue
    }

    fn capability(&self) -> PluginCapability {
        PluginCapability::new(AccessLevel::Read, AccessLevel::Read)
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

    fn make_req(uri: &str, body: Body) -> L7Request {
        L7Request::new(
            Method::GET,
            uri.parse::<Uri>().unwrap(),
            Version::HTTP_11,
            HeaderWorkspace::new(0),
            body,
        )
    }

    fn default_plugin(patterns: Vec<&str>) -> WafLitePlugin {
        WafLitePlugin::try_new("waf_lite", patterns.into_iter().map(String::from).collect())
            .unwrap()
    }

    #[test]
    fn test_waf_blocks_path_traversal_with_waf_blocked_reason() {
        let plugin = default_plugin(vec!["../", "/etc/passwd"]);
        let mut req = make_req("/static/../../etc/passwd", Body::Empty);
        let action = plugin.on_request(&mut req);
        assert!(matches!(action, Action::Reject(RejectReason::WafBlocked)));
    }

    #[test]
    fn test_waf_blocks_double_percent_encoded_traversal() {
        let plugin = default_plugin(vec!["../"]);
        let mut req = make_req("/assets/%252e%252e%252fconfig.json", Body::Empty);
        let action = plugin.on_request(&mut req);
        assert!(matches!(action, Action::Reject(RejectReason::WafBlocked)));
    }

    #[test]
    fn test_waf_blocks_sqli_in_query_with_plus_and_comments() {
        let plugin = default_plugin(vec!["union select"]);
        let mut req = make_req("/items?id=1+UNION%2F*evasion*%2FSELECT+1,2", Body::Empty);
        let action = plugin.on_request(&mut req);
        assert!(matches!(action, Action::Reject(RejectReason::WafBlocked)));
    }

    #[test]
    fn test_waf_blocks_xss_in_query() {
        let plugin = default_plugin(vec!["<script"]);
        let mut req = make_req("/search?q=%3Cscript%3Ealert(1)%3C/script%3E", Body::Empty);
        let action = plugin.on_request(&mut req);
        assert!(matches!(action, Action::Reject(RejectReason::WafBlocked)));
    }

    #[test]
    fn test_waf_scans_body_within_32kb_window() {
        let plugin = default_plugin(vec!["' or '1'='1", "<script"]);
        let payload = br#"{"username": "admin' or '1'='1", "password": "123"}"#;
        let mut req = make_req("/api/login", Body::from_bytes(&payload[..]));
        let action = plugin.on_request(&mut req);
        assert!(matches!(action, Action::Reject(RejectReason::WafBlocked)));
    }

    #[test]
    fn test_waf_caps_body_scan_at_32kb() {
        let plugin = default_plugin(vec!["<script"]);
        let mut payload = vec![b'a'; 40_000];
        // Place signature past the 32 KB (32768 byte) scan window
        payload[35_000..35_007].copy_from_slice(b"<script");
        let mut req = make_req("/api/upload", Body::from_bytes(payload));
        assert!(matches!(plugin.on_request(&mut req), Action::Continue));
    }

    #[test]
    fn test_waf_allows_clean_request_and_ignores_unconfigured_patterns() {
        let plugin = default_plugin(vec!["../"]);
        // Contains <script, but <script is NOT in configured patterns (WYCIWIS)
        let mut req = make_req("/search?q=%3Cscript%3E", Body::Empty);
        assert!(matches!(plugin.on_request(&mut req), Action::Continue));
    }

    #[test]
    fn test_waf_empty_patterns_always_continues() {
        let plugin = default_plugin(vec![]);
        let mut req = make_req("/../../etc/passwd?q=%3Cscript%3E", Body::Empty);
        assert!(matches!(plugin.on_request(&mut req), Action::Continue));
    }

    #[test]
    fn test_from_config_deserialization() {
        let cfg = PluginConfig {
            id: "waf_lite".to_string(),
            phase: "pre_route".to_string(),
            config: json!({
                "patterns": ["../", "union select", "<script"]
            }),
        };
        let plugin = WafLitePlugin::from_config(&cfg).unwrap();
        assert_eq!(plugin.patterns.len(), 3);
    }
}
