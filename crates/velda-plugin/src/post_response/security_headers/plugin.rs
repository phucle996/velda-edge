//! Security Response Headers Plugin (`security_headers`).
//!
//! Evaluated during `post_response` phase immediately before encoding downstream frames.
//! Injects pre-computed browser hardening headers with zero heap allocations on the hot path.
//!
//! ```text
//!   on_response(_req_headers, resp)
//!     │
//!     ▼ headers.get(..headers_len as usize).unwrap_or(&[])
//!   ┌────────────────────────────────────────────────────────────┐
//!   │ for header in precompiled_slice (0..=12 entries)           │
//!   │   ├─ InlineRegister { name, len, raw }                     │
//!   │   │    ──► resp.headers.set_inline_ascii(name, slice)      │
//!   │   └─ StandardPointer { name, value }                       │
//!   │        ──► resp.headers.insert(name, value.clone())        │
//!   └────────────────────────────────────────────────────────────┘
//! ```
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

use crate::config::PluginConfig;
use velda_core::L7Response;

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
        let cfg = super::validation::validate_security_headers_config(&config.id, &config.config)
            .map_err(|e| {
            PluginError::Config(format!(
                "Invalid security_headers configuration for '{}': {e}",
                config.id
            ))
        })?;

        let non_none = |val: Option<String>| -> Option<String> {
            match val {
                Some(ref s) if s.trim().eq_ignore_ascii_case("none") => None,
                other => other,
            }
        };

        let frame_options = non_none(Some(cfg.frame_options));
        let referrer_policy = non_none(Some(cfg.referrer_policy));

        let cross_domain_policies = match cfg.cross_domain_policies {
            Some(ref s) if s.trim().is_empty() => None,
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
            csp: non_none(cfg.content_security_policy),
            permissions_policy: non_none(cfg.permissions_policy),
            cross_origin_opener_policy: non_none(cfg.cross_origin_opener_policy),
            cross_origin_embedder_policy: non_none(cfg.cross_origin_embedder_policy),
            cross_origin_resource_policy: non_none(cfg.cross_origin_resource_policy),
            cross_domain_policies,
            xss_protection: non_none(cfg.xss_protection),
        };

        Ok(Self::with_config(&config.id, sec_config))
    }

    /// Returns a slice of pre-compiled header actions.
    #[inline(always)]
    pub fn compiled_headers(&self) -> &[CompiledHeader] {
        self.headers.get(..self.headers_len as usize).unwrap_or(&[])
    }
}

impl PostResponsePlugin for SecurityHeadersPlugin {
    #[inline(always)]
    fn id(&self) -> &str {
        &self.id
    }

    #[inline(always)]
    fn pbww(&self) -> u32 {
        0
    }

    /// Injects pre-computed browser security headers on outgoing response.
    ///
    /// # Performance Contract
    /// - Headers `<= 16` bytes (`nosniff`, `DENY`, `same-origin`, etc.) are written directly to
    ///   hardware integer registers in `HeaderWorkspace` (`set_inline_ascii`).
    /// - Headers `> 16` bytes (HSTS `max-age=...`, long CSP) are inserted as zero-copy references.
    #[inline(always)]
    fn on_response(&self, _req_headers: &velda_core::HeaderWorkspace, resp: &mut L7Response) {
        let active = self.headers.get(..self.headers_len as usize).unwrap_or(&[]);
        for header in active {
            match header {
                CompiledHeader::InlineRegister { name, len, raw } => {
                    let slice = raw.get(..*len as usize).unwrap_or(&[]);
                    resp.headers.set_inline_ascii(name.clone(), slice);
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
