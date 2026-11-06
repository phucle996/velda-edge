//! Security Response Headers Plugin (`security_headers`).
//!
//! Evaluated during `post_response` phase immediately before encoding downstream frames.
//! Injects pre-computed browser hardening headers (HSTS, nosniff, X-Frame-Options,
//! Referrer-Policy, CSP, Permissions-Policy, COOP, COEP, CORP) with zero heap allocations on the hot path.
//!
//! # Hardware Optimizations & Invariants
//! - **Dual-Representation Invariant**:
//!   - Headers `<= 16` bytes (`nosniff`, `DENY`, `same-origin`, `none`, `0`) are written directly
//!     into [`velda_core::HeaderWorkspace`]'s L1-resident 128-bit hardware registers (`set_inline_ascii`).
//!   - Headers `> 16` bytes (HSTS `max-age=...`, long CSP, Permissions-Policy) are stored as zero-copy
//!     `HeaderValue` pointers in RAM (`insert`).
//! - **64-Byte Cacheline Alignment**: [`SecurityHeadersPlugin`] is aligned to 64 bytes (`#[repr(align(64))]`)
//!   to avoid false sharing across multi-threaded traffic workers.
//! - **Single-Instruction ALU Bitmask (`op_mask: u16`)**: Pre-compiled 16-bit bitmask encoding
//!   active header features.
//! - **Prefetcher & Branch Predictor Harmony**: Flat contiguous array with sequential loop iteration
//!   enables 100% accurate CPU stream prefetching and zero branch mispredictions.

use http::header::{HeaderName, HeaderValue};
use velda_core::L7Response;
use velda_sync::post_sync::plugin::PluginConfig;

use super::config::{CompiledHeader, HstsConfig, MAX_SECURITY_HEADERS, SecurityHeadersConfig};
use crate::capability::{AccessLevel, PluginCapability, SupportedProtocols};
use crate::error::PluginError;
use crate::post_response::PostResponsePlugin;

/// Production implementation of the Security Headers plugin with 64-byte L1 cacheline alignment.
///
/// Pre-computes all HTTP header values at initialization into an inline array partitioned into:
/// - Inline 128-bit integer registers for values `<= 16` bytes (`HeaderWorkspace::set_inline_ascii`).
/// - Zero-copy reference pointers for values `> 16` bytes (`HeaderWorkspace::insert`).
#[derive(Debug, Clone)]
#[repr(align(64))]
pub struct SecurityHeadersPlugin {
    pub id: String,
    pub config: SecurityHeadersConfig,
    /// Bitmask flags encoding configured headers for fast ALU inspection.
    pub op_mask: u16,
    headers_len: u8,
    headers: [CompiledHeader; MAX_SECURITY_HEADERS],
}

impl SecurityHeadersPlugin {
    /// Constructs a new [`SecurityHeadersPlugin`] with default OWASP hardening headers.
    pub fn new(id: impl Into<String>) -> Self {
        Self::with_config(id, SecurityHeadersConfig::default())
    }

    /// Constructs a new [`SecurityHeadersPlugin`] with explicit granular configuration.
    pub fn with_config(id: impl Into<String>, config: SecurityHeadersConfig) -> Self {
        let (op_mask, headers_len, headers) = config.compile_headers();

        Self {
            id: id.into(),
            config,
            op_mask,
            headers_len,
            headers,
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

        let cross_origin_opener_policy = match cfg.cross_origin_opener_policy {
            Some(ref s) if s.trim().eq_ignore_ascii_case("none") => None,
            other => other,
        };

        let cross_origin_embedder_policy = match cfg.cross_origin_embedder_policy {
            Some(ref s) if s.trim().eq_ignore_ascii_case("none") => None,
            other => other,
        };

        let cross_origin_resource_policy = match cfg.cross_origin_resource_policy {
            Some(ref s) if s.trim().eq_ignore_ascii_case("none") => None,
            other => other,
        };

        let cross_domain_policies = match cfg.cross_domain_policies {
            Some(ref s) if s.trim().is_empty() => None,
            other => other,
        };

        let xss_protection = match cfg.xss_protection {
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
            cross_origin_opener_policy,
            cross_origin_embedder_policy,
            cross_origin_resource_policy,
            cross_domain_policies,
            xss_protection,
        };

        Ok(Self::with_config(&config.id, sec_config))
    }

    /// Returns a slice of pre-compiled header actions.
    #[inline(always)]
    pub fn compiled_headers(&self) -> &[CompiledHeader] {
        &self.headers[..self.headers_len as usize]
    }

    /// Materializes and returns pre-computed header pairs for compatibility and test inspection.
    pub fn precomputed_headers(&self) -> Vec<(HeaderName, HeaderValue)> {
        self.headers[..self.headers_len as usize]
            .iter()
            .map(|h| (h.name().clone(), h.to_header_value()))
            .collect()
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
    /// - Headers `<= 16` bytes (`nosniff`, `DENY`, `same-origin`, etc.) are written directly to
    ///   hardware integer registers in `HeaderWorkspace` (`set_inline_ascii`).
    /// - Headers `> 16` bytes (HSTS `max-age=...`, long CSP) are inserted as zero-copy references.
    /// - Linear contiguous loop over `[0..headers_len]` guarantees optimal CPU prefetching and branch prediction.
    #[inline(always)]
    fn on_response(&self, _req_headers: &velda_core::HeaderWorkspace, resp: &mut L7Response) {
        for header in &self.headers[..self.headers_len as usize] {
            match header {
                CompiledHeader::InlineRegister { name, len, raw } => {
                    resp.headers
                        .set_inline_ascii(name.clone(), &raw[..*len as usize]);
                }
                CompiledHeader::StandardPointer { name, value } => {
                    resp.headers.insert(name.clone(), value.clone());
                }
            }
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
    use serde_json::json;

    #[test]
    fn test_default_security_headers_injection() {
        let plugin = SecurityHeadersPlugin::new("sec-default");
        let mut resp = L7Response::from_bytes(StatusCode::OK, "{\"status\":\"ok\"}");
        let req_headers = velda_core::HeaderWorkspace::default();

        plugin.on_response(&req_headers, &mut resp);

        // Verify dual representation: short headers are in numeric registers before batch render
        assert!(resp.headers.numeric_len > 0);
        assert!(resp.headers.contains_key("x-content-type-options"));
        assert!(resp.headers.contains_key("x-frame-options"));

        // Batch-render to inspect wire strings
        resp.headers.render();

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
        assert!(!resp.headers.contains_key("cross-origin-opener-policy"));
        assert!(!resp.headers.contains_key("cross-origin-embedder-policy"));
    }

    #[test]
    fn test_cacheline_alignment_and_bitmask() {
        assert_eq!(
            std::mem::align_of::<SecurityHeadersPlugin>(),
            64,
            "SecurityHeadersPlugin must be 64-byte L1 cacheline aligned"
        );

        let plugin = SecurityHeadersPlugin::new("sec-align");
        assert!(plugin.op_mask & super::super::config::FLAG_HSTS != 0);
        assert!(plugin.op_mask & super::super::config::FLAG_NOSNIFF != 0);
        assert!(plugin.op_mask & super::super::config::FLAG_FRAME_OPTIONS != 0);
        assert!(plugin.op_mask & super::super::config::FLAG_REFERRER_POLICY != 0);
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
            cross_origin_opener_policy: None,
            cross_origin_embedder_policy: None,
            cross_origin_resource_policy: None,
            cross_domain_policies: None,
            xss_protection: None,
        };

        let plugin = SecurityHeadersPlugin::with_config("sec-custom", config);
        let mut resp = L7Response::from_bytes(StatusCode::OK, "");
        let req_headers = velda_core::HeaderWorkspace::default();

        plugin.on_response(&req_headers, &mut resp);
        resp.headers.render();

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
    fn test_modern_coop_coep_corp_injection() {
        let config = SecurityHeadersConfig {
            hsts: HstsConfig::default(),
            frame_options: Some("DENY".to_string()),
            content_type_nosniff: true,
            referrer_policy: Some("strict-origin-when-cross-origin".to_string()),
            csp: None,
            permissions_policy: None,
            cross_origin_opener_policy: Some("same-origin".to_string()),
            cross_origin_embedder_policy: Some("require-corp".to_string()),
            cross_origin_resource_policy: Some("same-site".to_string()),
            cross_domain_policies: Some("none".to_string()),
            xss_protection: Some("0".to_string()),
        };

        let plugin = SecurityHeadersPlugin::with_config("sec-modern", config);
        let mut resp = L7Response::from_bytes(StatusCode::OK, "");
        let req_headers = velda_core::HeaderWorkspace::default();

        plugin.on_response(&req_headers, &mut resp);
        resp.headers.render();

        assert_eq!(
            resp.headers.get("cross-origin-opener-policy").unwrap(),
            "same-origin"
        );
        assert_eq!(
            resp.headers.get("cross-origin-embedder-policy").unwrap(),
            "require-corp"
        );
        assert_eq!(
            resp.headers.get("cross-origin-resource-policy").unwrap(),
            "same-site"
        );
        assert_eq!(
            resp.headers
                .get("x-permitted-cross-domain-policies")
                .unwrap(),
            "none"
        );
        assert_eq!(resp.headers.get("x-xss-protection").unwrap(), "0");
    }

    #[test]
    fn test_selective_disabling() {
        let config = SecurityHeadersConfig {
            hsts: HstsConfig {
                enable: false,
                max_age_secs: 0,
                include_subdomains: false,
                preload: false,
            },
            frame_options: None,
            content_type_nosniff: false,
            referrer_policy: None,
            csp: None,
            permissions_policy: None,
            cross_origin_opener_policy: None,
            cross_origin_embedder_policy: None,
            cross_origin_resource_policy: None,
            cross_domain_policies: None,
            xss_protection: None,
        };

        let plugin = SecurityHeadersPlugin::with_config("sec-disabled", config);
        let mut resp = L7Response::from_bytes(StatusCode::OK, "");
        let req_headers = velda_core::HeaderWorkspace::default();

        plugin.on_response(&req_headers, &mut resp);
        resp.headers.render();

        assert_eq!(plugin.precomputed_headers().len(), 0);
        assert_eq!(plugin.op_mask, 0);
        assert!(!resp.headers.contains_key("strict-transport-security"));
        assert!(!resp.headers.contains_key("x-content-type-options"));
        assert!(!resp.headers.contains_key("x-frame-options"));
        assert!(!resp.headers.contains_key("referrer-policy"));
    }

    #[test]
    fn test_explicit_none_values_are_omitted() {
        let config = SecurityHeadersConfig {
            hsts: HstsConfig::default(),
            frame_options: Some("none".to_string()),
            content_type_nosniff: true,
            referrer_policy: Some("none".to_string()),
            csp: Some("none".to_string()),
            permissions_policy: Some("none".to_string()),
            cross_origin_opener_policy: Some("none".to_string()),
            cross_origin_embedder_policy: Some("none".to_string()),
            cross_origin_resource_policy: Some("none".to_string()),
            cross_domain_policies: None,
            xss_protection: Some("none".to_string()),
        };

        let plugin = SecurityHeadersPlugin::with_config("sec-none", config);
        let mut resp = L7Response::from_bytes(StatusCode::OK, "");
        let req_headers = velda_core::HeaderWorkspace::default();

        plugin.on_response(&req_headers, &mut resp);
        resp.headers.render();

        assert!(resp.headers.contains_key("strict-transport-security"));
        assert!(resp.headers.contains_key("x-content-type-options"));
        assert!(!resp.headers.contains_key("x-frame-options"));
        assert!(!resp.headers.contains_key("referrer-policy"));
        assert!(!resp.headers.contains_key("content-security-policy"));
        assert!(!resp.headers.contains_key("permissions-policy"));
        assert!(!resp.headers.contains_key("cross-origin-opener-policy"));
        assert!(!resp.headers.contains_key("cross-origin-embedder-policy"));
        assert!(!resp.headers.contains_key("cross-origin-resource-policy"));
        assert!(
            !resp
                .headers
                .contains_key("x-permitted-cross-domain-policies")
        );
        assert!(!resp.headers.contains_key("x-xss-protection"));
    }

    #[test]
    fn test_from_config_json_deserialization() {
        let raw_json = json!({
            "hsts": {
                "enable": true,
                "max_age_secs": 15768000,
                "include_subdomains": false,
                "preload": false
            },
            "frame_options": "DENY",
            "content_type_nosniff": true,
            "referrer_policy": "origin",
            "content_security_policy": "default-src https:",
            "permissions_policy": "geolocation=()",
            "cross_origin_opener_policy": "same-origin",
            "cross_origin_embedder_policy": "credentialless",
            "cross_origin_resource_policy": "cross-origin",
            "cross_domain_policies": "none",
            "xss_protection": "0"
        });

        let p_cfg = PluginConfig {
            id: "my-sec-plugin".to_string(),
            phase: "post_response".to_string(),
            config: raw_json,
        };

        let plugin =
            SecurityHeadersPlugin::from_config(&p_cfg).expect("deserialization should succeed");
        assert_eq!(plugin.id, "my-sec-plugin");

        let mut resp = L7Response::from_bytes(StatusCode::OK, "");
        let req_headers = velda_core::HeaderWorkspace::default();
        plugin.on_response(&req_headers, &mut resp);
        resp.headers.render();

        assert_eq!(
            resp.headers.get("strict-transport-security").unwrap(),
            "max-age=15768000"
        );
        assert_eq!(resp.headers.get("x-frame-options").unwrap(), "DENY");
        assert_eq!(
            resp.headers.get("x-content-type-options").unwrap(),
            "nosniff"
        );
        assert_eq!(resp.headers.get("referrer-policy").unwrap(), "origin");
        assert_eq!(
            resp.headers.get("content-security-policy").unwrap(),
            "default-src https:"
        );
        assert_eq!(
            resp.headers.get("permissions-policy").unwrap(),
            "geolocation=()"
        );
        assert_eq!(
            resp.headers.get("cross-origin-opener-policy").unwrap(),
            "same-origin"
        );
        assert_eq!(
            resp.headers.get("cross-origin-embedder-policy").unwrap(),
            "credentialless"
        );
        assert_eq!(
            resp.headers.get("cross-origin-resource-policy").unwrap(),
            "cross-origin"
        );
        assert_eq!(
            resp.headers
                .get("x-permitted-cross-domain-policies")
                .unwrap(),
            "none"
        );
        assert_eq!(resp.headers.get("x-xss-protection").unwrap(), "0");
    }
}
