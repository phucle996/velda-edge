//! Security Response Headers Plugin (`security_headers`).
//!
//! Evaluated during `post_response` phase immediately before encoding downstream frames.
//! Injects pre-computed browser hardening headers (HSTS, nosniff, X-Frame-Options,
//! Referrer-Policy, CSP, Permissions-Policy) with zero heap allocations on the hot path.
//!
//! Full schema specification, allowed values, and configuration examples:
//! See [`README.md`](./README.md).

use http::header::{
    HeaderName, HeaderValue, REFERRER_POLICY, STRICT_TRANSPORT_SECURITY, X_CONTENT_TYPE_OPTIONS,
    X_FRAME_OPTIONS,
};
use velda_core::L7Response;
use velda_sync::post_sync::plugin::PluginConfig;

use crate::capability::{AccessLevel, PluginCapability, SupportedProtocols};
use crate::error::PluginError;
use crate::post_response::PostResponsePlugin;

/// Custom header name for Content-Security-Policy.
static CONTENT_SECURITY_POLICY: HeaderName = HeaderName::from_static("content-security-policy");

/// Custom header name for Permissions-Policy.
static PERMISSIONS_POLICY: HeaderName = HeaderName::from_static("permissions-policy");

/// Granular HSTS configuration options.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HstsConfig {
    pub enable: bool,
    pub max_age_secs: u64,
    pub include_subdomains: bool,
    pub preload: bool,
}

impl Default for HstsConfig {
    fn default() -> Self {
        Self {
            enable: true,
            max_age_secs: 31_536_000, // 1 year standard
            include_subdomains: true,
            preload: false,
        }
    }
}

/// Comprehensive browser hardening and security headers configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecurityHeadersConfig {
    pub hsts: HstsConfig,
    pub frame_options: Option<String>,
    pub content_type_nosniff: bool,
    pub referrer_policy: Option<String>,
    pub csp: Option<String>,
    pub permissions_policy: Option<String>,
}

impl Default for SecurityHeadersConfig {
    fn default() -> Self {
        Self {
            hsts: HstsConfig::default(),
            frame_options: Some("DENY".to_string()),
            content_type_nosniff: true,
            referrer_policy: Some("strict-origin-when-cross-origin".to_string()),
            csp: None,
            permissions_policy: None,
        }
    }
}

/// Production implementation of the Security Headers plugin.
///
/// Pre-computes all HTTP header values at initialization to guarantee Zero-Allocation
/// on the response serving hot path.
#[derive(Debug, Clone)]
pub struct SecurityHeadersPlugin {
    pub id: String,
    precomputed_headers: Vec<(HeaderName, HeaderValue)>,
}

impl SecurityHeadersPlugin {
    /// Constructs a new [`SecurityHeadersPlugin`] with default OWASP hardening headers.
    pub fn new(id: impl Into<String>) -> Self {
        Self::with_config(id, SecurityHeadersConfig::default())
    }

    /// Constructs a new [`SecurityHeadersPlugin`] with explicit granular configuration.
    pub fn with_config(id: impl Into<String>, config: SecurityHeadersConfig) -> Self {
        let mut headers = Vec::with_capacity(6);

        // 1. Pre-compute HSTS header
        if config.hsts.enable {
            let mut hsts_str = format!("max-age={}", config.hsts.max_age_secs);
            if config.hsts.include_subdomains {
                hsts_str.push_str("; includeSubDomains");
            }
            if config.hsts.preload {
                hsts_str.push_str("; preload");
            }

            if let Ok(val) = HeaderValue::from_str(&hsts_str) {
                headers.push((STRICT_TRANSPORT_SECURITY, val));
            }
        }

        // 2. Pre-compute X-Content-Type-Options
        if config.content_type_nosniff {
            headers.push((X_CONTENT_TYPE_OPTIONS, HeaderValue::from_static("nosniff")));
        }

        // 3. Pre-compute X-Frame-Options
        if let Some(ref fo) = config.frame_options {
            let fo_trimmed = fo.trim();
            if !fo_trimmed.is_empty()
                && !fo_trimmed.eq_ignore_ascii_case("none")
                && let Ok(val) = HeaderValue::from_str(fo_trimmed)
            {
                headers.push((X_FRAME_OPTIONS, val));
            }
        }

        // 4. Pre-compute Referrer-Policy
        if let Some(ref rp) = config.referrer_policy {
            let rp_trimmed = rp.trim();
            if !rp_trimmed.is_empty()
                && !rp_trimmed.eq_ignore_ascii_case("none")
                && let Ok(val) = HeaderValue::from_str(rp_trimmed)
            {
                headers.push((REFERRER_POLICY, val));
            }
        }

        // 5. Pre-compute Content-Security-Policy
        if let Some(ref csp) = config.csp {
            let csp_trimmed = csp.trim();
            if !csp_trimmed.is_empty()
                && !csp_trimmed.eq_ignore_ascii_case("none")
                && let Ok(val) = HeaderValue::from_str(csp_trimmed)
            {
                headers.push((CONTENT_SECURITY_POLICY.clone(), val));
            }
        }

        // 6. Pre-compute Permissions-Policy
        if let Some(ref pp) = config.permissions_policy {
            let pp_trimmed = pp.trim();
            if !pp_trimmed.is_empty()
                && !pp_trimmed.eq_ignore_ascii_case("none")
                && let Ok(val) = HeaderValue::from_str(pp_trimmed)
            {
                headers.push((PERMISSIONS_POLICY.clone(), val));
            }
        }

        Self {
            id: id.into(),
            precomputed_headers: headers,
        }
    }

    /// Deserializes and constructs an instance directly from a [`PluginConfig`].
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let cfg = velda_sync::validate_security_headers_config(&config.id, &config.config)
            .map_err(|e| {
                PluginError::Config(format!(
                    "Invalid security_headers configuration for '{}': {e}",
                    config.id
                ))
            })?;

        let frame_options = if cfg.frame_options.trim().eq_ignore_ascii_case("none") {
            None
        } else {
            Some(cfg.frame_options)
        };

        let referrer_policy = if cfg.referrer_policy.trim().eq_ignore_ascii_case("none") {
            None
        } else {
            Some(cfg.referrer_policy)
        };

        let csp = match cfg.content_security_policy {
            Some(ref s) if s.trim().eq_ignore_ascii_case("none") => None,
            other => other,
        };

        let permissions_policy = match cfg.permissions_policy {
            Some(ref s) if s.trim().eq_ignore_ascii_case("none") => None,
            other => other,
        };

        let sec_config = SecurityHeadersConfig {
            hsts: HstsConfig {
                enable: cfg.hsts.enable,
                max_age_secs: cfg.hsts.max_age_secs,
                include_subdomains: cfg.hsts.include_subdomains,
                preload: cfg.hsts.preload,
            },
            frame_options,
            content_type_nosniff: cfg.content_type_nosniff,
            referrer_policy,
            csp,
            permissions_policy,
        };

        Ok(Self::with_config(&config.id, sec_config))
    }

    /// Returns a slice of pre-computed header pairs applied by this plugin.
    #[inline(always)]
    pub fn precomputed_headers(&self) -> &[(HeaderName, HeaderValue)] {
        &self.precomputed_headers
    }
}

impl PostResponsePlugin for SecurityHeadersPlugin {
    #[inline(always)]
    fn id(&self) -> &str {
        &self.id
    }

    /// Injects pre-computed browser security headers on outgoing response.
    ///
    /// # Performance Contract
    /// Executes purely by cloning pre-computed `HeaderValue` handles (O(1) atomic ref-count
    /// increments). **Zero heap allocations** are performed on this hot path.
    #[inline(always)]
    fn on_response(&self, _req_headers: &velda_core::HeaderWorkspace, resp: &mut L7Response) {
        for (name, val) in &self.precomputed_headers {
            resp.headers.insert(name.clone(), val.clone());
        }
    }

    #[inline(always)]
    fn capability(&self) -> PluginCapability {
        PluginCapability::header_only(AccessLevel::Write)
    }

    #[inline(always)]
    fn supported_protocols(&self) -> SupportedProtocols {
        SupportedProtocols::ALL_HTTP
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::StatusCode;

    #[test]
    fn test_default_security_headers_injection() {
        let plugin = SecurityHeadersPlugin::new("sec-default");
        let mut resp = L7Response::from_bytes(StatusCode::OK, "{\"status\":\"ok\"}");
        let req_headers = velda_core::HeaderWorkspace::empty();

        plugin.on_response(&req_headers, &mut resp);

        assert_eq!(
            resp.headers.get("strict-transport-security").unwrap(),
            "max-age=31536000; includeSubDomains"
        );
        assert_eq!(
            resp.headers.get("x-content-type-options").unwrap(),
            "nosniff"
        );
        assert_eq!(resp.headers.get("x-frame-options").unwrap(), "DENY");
        assert_eq!(
            resp.headers.get("referrer-policy").unwrap(),
            "strict-origin-when-cross-origin"
        );
        assert!(!resp.headers.contains_key("content-security-policy"));
        assert!(!resp.headers.contains_key("permissions-policy"));
    }

    #[test]
    fn test_custom_hsts_preload_and_csp() {
        let config = SecurityHeadersConfig {
            hsts: HstsConfig {
                enable: true,
                max_age_secs: 63_072_000,
                include_subdomains: true,
                preload: true,
            },
            frame_options: Some("SAMEORIGIN".to_string()),
            content_type_nosniff: true,
            referrer_policy: Some("no-referrer".to_string()),
            csp: Some("default-src 'self'; script-src 'self' cdn.example.com".to_string()),
            permissions_policy: Some("camera=(), microphone=()".to_string()),
        };

        let plugin = SecurityHeadersPlugin::with_config("sec-custom", config);
        let mut resp = L7Response::from_bytes(StatusCode::OK, "");
        let req_headers = velda_core::HeaderWorkspace::empty();

        plugin.on_response(&req_headers, &mut resp);

        assert_eq!(
            resp.headers.get("strict-transport-security").unwrap(),
            "max-age=63072000; includeSubDomains; preload"
        );
        assert_eq!(resp.headers.get("x-frame-options").unwrap(), "SAMEORIGIN");
        assert_eq!(resp.headers.get("referrer-policy").unwrap(), "no-referrer");
        assert_eq!(
            resp.headers.get("content-security-policy").unwrap(),
            "default-src 'self'; script-src 'self' cdn.example.com"
        );
        assert_eq!(
            resp.headers.get("permissions-policy").unwrap(),
            "camera=(), microphone=()"
        );
    }

    #[test]
    fn test_selective_disabling() {
        let config = SecurityHeadersConfig {
            hsts: HstsConfig {
                enable: false,
                ..Default::default()
            },
            frame_options: Some("none".to_string()),
            content_type_nosniff: false,
            referrer_policy: None,
            csp: None,
            permissions_policy: None,
        };

        let plugin = SecurityHeadersPlugin::with_config("sec-none", config);
        let mut resp = L7Response::from_bytes(StatusCode::OK, "");
        let req_headers = velda_core::HeaderWorkspace::empty();

        plugin.on_response(&req_headers, &mut resp);

        // Verify no security headers were injected (only the pre-loaded x-request-id is present)
        assert_eq!(resp.headers.len(), 1);
        assert!(resp.headers.contains_key(&velda_core::X_REQUEST_ID));
        assert!(!resp.headers.contains_key("x-frame-options"));
        assert!(!resp.headers.contains_key("strict-transport-security"));
        assert!(!resp.headers.contains_key("x-content-type-options"));
    }

    #[test]
    fn test_from_config_json_deserialization() {
        let json = serde_json::json!({
            "hsts": {
                "enable": true,
                "max_age_secs": 15768000,
                "include_subdomains": false,
                "preload": false
            },
            "frame_options": "SAMEORIGIN",
            "content_type_nosniff": true,
            "referrer_policy": "strict-origin",
            "content_security_policy": "frame-ancestors 'none'",
            "permissions_policy": "none"
        });

        let plugin_config = PluginConfig {
            id: "sec-json".to_string(),
            phase: "post_response".to_string(),
            config: json,
        };

        let plugin = SecurityHeadersPlugin::from_config(&plugin_config).unwrap();
        let mut resp = L7Response::from_bytes(StatusCode::OK, "");
        let req_headers = velda_core::HeaderWorkspace::empty();
        plugin.on_response(&req_headers, &mut resp);

        assert_eq!(
            resp.headers.get("strict-transport-security").unwrap(),
            "max-age=15768000"
        );
        assert_eq!(resp.headers.get("x-frame-options").unwrap(), "SAMEORIGIN");
        assert_eq!(
            resp.headers.get("referrer-policy").unwrap(),
            "strict-origin"
        );
        assert_eq!(
            resp.headers.get("content-security-policy").unwrap(),
            "frame-ancestors 'none'"
        );
    }

    #[test]
    fn test_explicit_none_values_are_omitted() {
        let json = serde_json::json!({
            "hsts": {
                "enable": true,
                "max_age_secs": 31536000,
                "include_subdomains": true,
                "preload": false
            },
            "frame_options": "DENY",
            "content_type_nosniff": true,
            "referrer_policy": "strict-origin-when-cross-origin",
            "content_security_policy": "none",
            "permissions_policy": "none"
        });

        let plugin_config = PluginConfig {
            id: "sec-none".to_string(),
            phase: "post_response".to_string(),
            config: json,
        };

        let plugin = SecurityHeadersPlugin::from_config(&plugin_config).unwrap();
        let mut resp = L7Response::from_bytes(StatusCode::OK, "");
        let req_headers = velda_core::HeaderWorkspace::empty();
        plugin.on_response(&req_headers, &mut resp);

        assert_eq!(
            resp.headers.get("strict-transport-security").unwrap(),
            "max-age=31536000; includeSubDomains"
        );
        assert_eq!(resp.headers.get("x-frame-options").unwrap(), "DENY");
        assert_eq!(
            resp.headers.get("x-content-type-options").unwrap(),
            "nosniff"
        );
        assert_eq!(
            resp.headers.get("referrer-policy").unwrap(),
            "strict-origin-when-cross-origin"
        );
        // Specifically verify "none" fields are omitted, NOT injected as literal "none" headers
        assert!(!resp.headers.contains_key("content-security-policy"));
        assert!(!resp.headers.contains_key("permissions-policy"));
    }
}
