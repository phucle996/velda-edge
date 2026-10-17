//! ============================================================================
//! VELDA EDGE PLUGIN MANIFEST & HUMAN SPECIFICATION
//! ============================================================================
//!
//! - **PLUGIN NAME**: `SECURITY_HEADERS`
//! - **CANONICAL PHASE**: `POST_RESPONSE`
//! - **EXECUTION ORDER**: STABLY SORTED DESCENDING BY ROUTE PRIORITY (DEFAULT: STANDARD POST-RESPONSE ORDER)
//! - **REVERSE CAPABILITIES**: HEADER-ONLY WRITE (`PluginCapability::header_only(AccessLevel::Write)`)
//! - **SUPPORTED PROTOCOLS**: `ALL_HTTP` (`SupportedProtocols::ALL_HTTP`: HTTP/1.1, HTTP/2, HTTP/3)
//!
//! ----------------------------------------------------------------------------
//! ### 1. PURPOSE & WHAT IT PROTECTS ("Phục vụ cái gì?")
//! ----------------------------------------------------------------------------
//! Enforces client-side browser hardening and OWASP L7 hygiene on outgoing HTTP responses.
//! This plugin instructs user-agents (browsers) to activate internal sandbox defenses against
//! standard client-side attack vectors:
//!
//! 1. **Strict-Transport-Security (HSTS - RFC 6797)**:
//!    Forces browsers to communicate strictly over HTTPS for a defined duration, permanently
//!    neutralizing SSL-Stripping Man-In-The-Middle (MITM) downgrade attacks.
//! 2. **X-Content-Type-Options (RFC 7231 / MIME Sniffing Standard)**:
//!    Sets `nosniff`, disabling browser MIME-type guessing. Prevents drive-by download and
//!    MIME-confusion attacks where malicious scripts are disguised as benign assets (e.g. image files).
//! 3. **X-Frame-Options (RFC 7034)**:
//!    Enforces `DENY` or `SAMEORIGIN` to forbid or restrict embedding within `<iframe>` tags,
//!    neutralizing UI Redressing and Clickjacking exploits.
//! 4. **Referrer-Policy (W3C Specification)**:
//!    Enforces `strict-origin-when-cross-origin` (or custom policy) to prevent query parameters,
//!    session IDs, or PII tokens from leaking to external domains via the HTTP `Referer` header.
//! 5. **Content-Security-Policy (CSP - W3C Level 3)**:
//!    Optionally declares authorized domains for scripts, styles, images, and child frames,
//!    mitigating Cross-Site Scripting (XSS) and covert data exfiltration.
//! 6. **Permissions-Policy (W3C Specification)**:
//!    Optionally restricts unneeded browser hardware capabilities (camera, microphone, geolocation).
//!
//! ----------------------------------------------------------------------------
//! ### 2. WHAT IS OUT OF SCOPE ("KHÔNG phục vụ cái gì? Ranh giới cấm")
//! ----------------------------------------------------------------------------
//! - **DOES NOT protect backend servers**: It does not inspect payloads, block SQL Injection,
//!   prevent Remote Code Execution (RCE), or absorb L7 DDoS floods.
//! - **DOES NOT handle CORS**: Cross-Origin Resource Sharing is owned exclusively by the `cors`
//!   plugin, which manages Preflight `OPTIONS` and dynamic Origin reflection.
//! - **DOES NOT perform arbitrary header transformations**: Dynamic variable substitutions
//!   (`$request_id`, `$upstream_addr`) are owned strictly by the `header_mutation` plugin.
//! - **DOES NOT buffer or inspect response payloads**: Purely operates on response headers,
//!   preserving zero-alloc streaming pipe invariants across all HTTP protocols.
//!
//! ----------------------------------------------------------------------------
//! ### 3. ARCHITECTURAL & HOT-PATH INVARIANTS ("Notes & Performance Contracts")
//! ----------------------------------------------------------------------------
//! - **Zero-Allocation Hot Path**:
//!   All `HeaderName` and `HeaderValue` pairs are pre-computed, pre-formatted, and validated
//!   once during initialization (`from_config`). Serving a response executes a tight contiguous
//!   iteration cloning `HeaderValue` (O(1) atomic ref-count increment on internal `Bytes`,
//!   strictly **0 heap allocations** on the request hot path).
//! - **Granular Toggles**:
//!   Every single security header can be enabled, customized, or disabled individually,
//!   ensuring applications requiring iframe embedding or custom staging configs are never broken.
//!
//! ----------------------------------------------------------------------------
//! ### 4. CONFIGURATION SCHEMA EXAMPLE
//! ----------------------------------------------------------------------------
//! ```json
//! {
//!   "id": "security_headers",
//!   "priority": 100,
//!   "config": {
//!     "hsts": {
//!       "enable": true,
//!       "max_age_secs": 31536000,
//!       "include_subdomains": true,
//!       "preload": false
//!     },
//!     "frame_options": "DENY",
//!     "content_type_nosniff": true,
//!     "referrer_policy": "strict-origin-when-cross-origin",
//!     "csp": "default-src 'self'",
//!     "permissions_policy": "camera=(), microphone=(), geolocation=()"
//!   }
//! }
//! ```
//! ============================================================================

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
    ///
    /// Supports both granular nested objects (`hsts: { ... }`) and legacy flat properties
    /// (`hsts_max_age_secs: ...`) for seamless backward compatibility.
    ///
    /// # Explicit 'none' Handling Invariant
    /// Operators must explicitly specify `"none"` for any unused or omitted header field
    /// (e.g. `"csp": "none"`, `"permissions_policy": "none"`). The header is recognized
    /// as omitted rather than treated as a literal header value.
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let mut sec_config = SecurityHeadersConfig::default();

        // 1. Parse HSTS (supports nested object, boolean, or string "none")
        if let Some(hsts_val) = config
            .config
            .get("hsts")
            .or_else(|| config.config.get("strict_transport_security"))
        {
            if let Some(hsts_obj) = hsts_val.as_object() {
                sec_config.hsts.enable = hsts_obj
                    .get("enable")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(true);
                sec_config.hsts.max_age_secs = hsts_obj
                    .get("max_age_secs")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(31_536_000);
                sec_config.hsts.include_subdomains = hsts_obj
                    .get("include_subdomains")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(true);
                sec_config.hsts.preload = hsts_obj
                    .get("preload")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
            } else if let Some(hsts_bool) = hsts_val.as_bool() {
                sec_config.hsts.enable = hsts_bool;
            } else if let Some(hsts_str) = hsts_val.as_str()
                && hsts_str.trim().eq_ignore_ascii_case("none")
            {
                sec_config.hsts.enable = false;
            }
        } else {
            // Flat backward compatibility
            if let Some(max_age) = config
                .config
                .get("hsts_max_age_secs")
                .and_then(|v| v.as_u64())
            {
                sec_config.hsts.max_age_secs = max_age;
            }
            if let Some(inc_sub) = config
                .config
                .get("include_subdomains")
                .and_then(|v| v.as_bool())
            {
                sec_config.hsts.include_subdomains = inc_sub;
            }
            if let Some(preload) = config.config.get("preload").and_then(|v| v.as_bool()) {
                sec_config.hsts.preload = preload;
            }
        }

        // 2. Parse X-Frame-Options (strictly "none" to omit)
        if let Some(fo) = config
            .config
            .get("frame_options")
            .or_else(|| config.config.get("x_frame_options"))
            .and_then(|v| v.as_str())
        {
            if fo.trim().eq_ignore_ascii_case("none") {
                sec_config.frame_options = None;
            } else {
                sec_config.frame_options = Some(fo.to_string());
            }
        }

        // 3. Parse X-Content-Type-Options (strictly "none" to omit)
        if let Some(nosniff_val) = config
            .config
            .get("content_type_nosniff")
            .or_else(|| config.config.get("x_content_type_options"))
        {
            if let Some(b) = nosniff_val.as_bool() {
                sec_config.content_type_nosniff = b;
            } else if let Some(s) = nosniff_val.as_str() {
                if s.trim().eq_ignore_ascii_case("none") {
                    sec_config.content_type_nosniff = false;
                } else if s.trim().eq_ignore_ascii_case("nosniff") {
                    sec_config.content_type_nosniff = true;
                }
            }
        }

        // 4. Parse Referrer-Policy (strictly "none" to omit)
        if let Some(rp) = config
            .config
            .get("referrer_policy")
            .and_then(|v| v.as_str())
        {
            if rp.trim().eq_ignore_ascii_case("none") {
                sec_config.referrer_policy = None;
            } else {
                sec_config.referrer_policy = Some(rp.to_string());
            }
        }

        // 5. Parse Content-Security-Policy (strictly "none" to omit)
        if let Some(csp) = config
            .config
            .get("csp")
            .or_else(|| config.config.get("content_security_policy"))
            .and_then(|v| v.as_str())
        {
            if csp.trim().eq_ignore_ascii_case("none") {
                sec_config.csp = None;
            } else {
                sec_config.csp = Some(csp.to_string());
            }
        }

        // 6. Parse Permissions-Policy (strictly "none" to omit)
        if let Some(pp) = config
            .config
            .get("permissions_policy")
            .and_then(|v| v.as_str())
        {
            if pp.trim().eq_ignore_ascii_case("none") {
                sec_config.permissions_policy = None;
            } else {
                sec_config.permissions_policy = Some(pp.to_string());
            }
        }

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
    fn on_response(&self, resp: &mut L7Response) {
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

        plugin.on_response(&mut resp);

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

        plugin.on_response(&mut resp);

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

        plugin.on_response(&mut resp);

        assert!(resp.headers.is_empty());
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
            "csp": "frame-ancestors 'none'"
        });

        let plugin_config = PluginConfig {
            id: "sec-json".to_string(),
            phase: "post_response".to_string(),
            priority: Some(50),
            config: json,
        };

        let plugin = SecurityHeadersPlugin::from_config(&plugin_config).unwrap();
        let mut resp = L7Response::from_bytes(StatusCode::OK, "");
        plugin.on_response(&mut resp);

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
            "csp": "none",
            "permissions_policy": "none"
        });

        let plugin_config = PluginConfig {
            id: "sec-none".to_string(),
            phase: "post_response".to_string(),
            priority: Some(100),
            config: json,
        };

        let plugin = SecurityHeadersPlugin::from_config(&plugin_config).unwrap();
        let mut resp = L7Response::from_bytes(StatusCode::OK, "");
        plugin.on_response(&mut resp);

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
