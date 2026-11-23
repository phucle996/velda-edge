//! Security Headers Configuration Schema & Pre-baked State.
//!
//! Encapsulates declarative configuration structures, bitmask flags, and compile-time
//! header construction partitioned into dual representations:
//!
//! ```text
//!   SecurityHeadersConfig (init / reload)
//!     ├─► HSTS (max-age + includeSubDomains + preload)
//!     ├─► X-Content-Type-Options: nosniff
//!     └─► 9 optional policy directives (skip "none" except X-Permitted-Cross-Domain-Policies)
//!           │
//!           ▼ compile_header_entry(name, trimmed)
//!     ┌──────────────────────────────────────────────────────────┐
//!     │ bytes.len() <= 16                                        │
//!     │   ──► CompiledHeader::InlineRegister { name, len, raw }  │
//!     │ bytes.len() > 16                                         │
//!     │   ──► CompiledHeader::StandardPointer { name, value }    │
//!     └──────────────────────────────────────────────────────────┘
//! ```

use http::header::{
    HeaderName, HeaderValue, REFERRER_POLICY, STRICT_TRANSPORT_SECURITY, X_CONTENT_TYPE_OPTIONS,
    X_FRAME_OPTIONS,
};

/// Maximum pre-computed headers stored inline per plugin instance.
pub const MAX_SECURITY_HEADERS: usize = 12;

/// Bitmask flags for single-instruction ALU configuration checks.
pub const FLAG_HSTS: u16 = 1 << 0;
pub const FLAG_NOSNIFF: u16 = 1 << 1;
pub const FLAG_FRAME_OPTIONS: u16 = 1 << 2;
pub const FLAG_REFERRER_POLICY: u16 = 1 << 3;
pub const FLAG_CSP: u16 = 1 << 4;
pub const FLAG_PERMISSIONS_POLICY: u16 = 1 << 5;
pub const FLAG_COOP: u16 = 1 << 6;
pub const FLAG_COEP: u16 = 1 << 7;
pub const FLAG_CORP: u16 = 1 << 8;
pub const FLAG_CROSS_DOMAIN: u16 = 1 << 9;
pub const FLAG_XSS_PROTECTION: u16 = 1 << 10;

/// Custom header name for Content-Security-Policy.
pub static CONTENT_SECURITY_POLICY: HeaderName = HeaderName::from_static("content-security-policy");

/// Custom header name for Permissions-Policy.
pub static PERMISSIONS_POLICY: HeaderName = HeaderName::from_static("permissions-policy");

/// Custom header name for Cross-Origin-Opener-Policy (COOP).
pub static CROSS_ORIGIN_OPENER_POLICY: HeaderName =
    HeaderName::from_static("cross-origin-opener-policy");

/// Custom header name for Cross-Origin-Embedder-Policy (COEP).
pub static CROSS_ORIGIN_EMBEDDER_POLICY: HeaderName =
    HeaderName::from_static("cross-origin-embedder-policy");

/// Custom header name for Cross-Origin-Resource-Policy (CORP).
pub static CROSS_ORIGIN_RESOURCE_POLICY: HeaderName =
    HeaderName::from_static("cross-origin-resource-policy");

/// Custom header name for X-Permitted-Cross-Domain-Policies.
pub static X_PERMITTED_CROSS_DOMAIN_POLICIES: HeaderName =
    HeaderName::from_static("x-permitted-cross-domain-policies");

/// Custom header name for X-XSS-Protection.
pub static X_XSS_PROTECTION: HeaderName = HeaderName::from_static("x-xss-protection");

/// Pre-compiled header representation partitioned by size:
/// - `<= 16` bytes: directly stored as inline byte array for 128-bit CPU integer register write.
/// - `> 16` bytes: stored as zero-copy [`HeaderValue`] handle referencing RAM.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompiledHeader {
    /// Values `<= 16` bytes stored inline in L1 cache for single-MOV integer register insertion.
    InlineRegister {
        name: HeaderName,
        len: u8,
        raw: [u8; 16],
    },
    /// Values `> 16` bytes stored as reference pointers in RAM.
    StandardPointer {
        name: HeaderName,
        value: HeaderValue,
    },
}

impl Default for CompiledHeader {
    fn default() -> Self {
        CompiledHeader::InlineRegister {
            name: HeaderName::from_static("x-empty"),
            len: 0,
            raw: [0u8; 16],
        }
    }
}

impl CompiledHeader {
    #[inline(always)]
    pub fn name(&self) -> &HeaderName {
        match self {
            CompiledHeader::InlineRegister { name, .. } => name,
            CompiledHeader::StandardPointer { name, .. } => name,
        }
    }

    #[inline]
    pub fn to_header_value(&self) -> HeaderValue {
        match self {
            CompiledHeader::InlineRegister { len, raw, .. } => {
                let slice = raw.get(..*len as usize).unwrap_or(&[]);
                HeaderValue::from_bytes(slice).unwrap_or_else(|_| HeaderValue::from_static(""))
            }
            CompiledHeader::StandardPointer { value, .. } => value.clone(),
        }
    }
}

/// Compiles a header name and string value into an [`InlineRegister`] (if `<= 16` bytes)
/// or [`StandardPointer`] (if `> 16` bytes).
#[inline]
pub fn compile_header_entry(name: HeaderName, val_str: &str) -> Option<CompiledHeader> {
    let trimmed = val_str.trim();
    if trimmed.is_empty() {
        return None;
    }
    let bytes = trimmed.as_bytes();
    if bytes.len() <= 16 {
        let mut raw = [0u8; 16];
        if let Some(dst) = raw.get_mut(..bytes.len()) {
            dst.copy_from_slice(bytes);
        }
        Some(CompiledHeader::InlineRegister {
            name,
            len: bytes.len() as u8,
            raw,
        })
    } else {
        HeaderValue::from_str(trimmed)
            .ok()
            .map(|value| CompiledHeader::StandardPointer { name, value })
    }
}

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
    pub cross_origin_opener_policy: Option<String>,
    pub cross_origin_embedder_policy: Option<String>,
    pub cross_origin_resource_policy: Option<String>,
    pub cross_domain_policies: Option<String>,
    pub xss_protection: Option<String>,
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
            cross_origin_opener_policy: None,
            cross_origin_embedder_policy: None,
            cross_origin_resource_policy: None,
            cross_domain_policies: None,
            xss_protection: None,
        }
    }
}

impl SecurityHeadersConfig {
    /// Compiles configuration into an inline flat array partitioned by size and ALU bitmask flags.
    pub fn compile_headers(&self) -> (u16, u8, [CompiledHeader; MAX_SECURITY_HEADERS]) {
        let mut op_mask = 0u16;
        let mut count = 0usize;
        let mut array: [CompiledHeader; MAX_SECURITY_HEADERS] =
            std::array::from_fn(|_| CompiledHeader::default());

        let mut push_entry = |entry: CompiledHeader, flag: u16| {
            if let Some(slot) = array.get_mut(count) {
                *slot = entry;
                count += 1;
                op_mask |= flag;
            }
        };

        // 1. Pre-compute HSTS header
        if self.hsts.enable {
            let mut hsts_str = format!("max-age={}", self.hsts.max_age_secs);
            if self.hsts.include_subdomains {
                hsts_str.push_str("; includeSubDomains");
            }
            if self.hsts.preload {
                hsts_str.push_str("; preload");
            }

            if let Some(entry) = compile_header_entry(STRICT_TRANSPORT_SECURITY, &hsts_str) {
                push_entry(entry, FLAG_HSTS);
            }
        }

        // 2. Pre-compute X-Content-Type-Options (nosniff = 7 bytes <= 16 -> InlineRegister)
        if self.content_type_nosniff
            && let Some(entry) = compile_header_entry(X_CONTENT_TYPE_OPTIONS, "nosniff")
        {
            push_entry(entry, FLAG_NOSNIFF);
        }

        // 3..11. Pre-compute optional string directives uniformly
        let directives: [(&Option<String>, HeaderName, u16, bool); 9] = [
            (
                &self.frame_options,
                X_FRAME_OPTIONS,
                FLAG_FRAME_OPTIONS,
                true,
            ),
            (
                &self.referrer_policy,
                REFERRER_POLICY,
                FLAG_REFERRER_POLICY,
                true,
            ),
            (&self.csp, CONTENT_SECURITY_POLICY.clone(), FLAG_CSP, true),
            (
                &self.permissions_policy,
                PERMISSIONS_POLICY.clone(),
                FLAG_PERMISSIONS_POLICY,
                true,
            ),
            (
                &self.cross_origin_opener_policy,
                CROSS_ORIGIN_OPENER_POLICY.clone(),
                FLAG_COOP,
                true,
            ),
            (
                &self.cross_origin_embedder_policy,
                CROSS_ORIGIN_EMBEDDER_POLICY.clone(),
                FLAG_COEP,
                true,
            ),
            (
                &self.cross_origin_resource_policy,
                CROSS_ORIGIN_RESOURCE_POLICY.clone(),
                FLAG_CORP,
                true,
            ),
            (
                &self.cross_domain_policies,
                X_PERMITTED_CROSS_DOMAIN_POLICIES.clone(),
                FLAG_CROSS_DOMAIN,
                false,
            ),
            (
                &self.xss_protection,
                X_XSS_PROTECTION.clone(),
                FLAG_XSS_PROTECTION,
                true,
            ),
        ];

        for (raw_val, header_name, flag, skip_none) in directives {
            let Some(val) = raw_val.as_deref() else {
                continue;
            };
            let trimmed = val.trim();
            if trimmed.is_empty() || (skip_none && trimmed.eq_ignore_ascii_case("none")) {
                continue;
            }
            if let Some(entry) = compile_header_entry(header_name, trimmed) {
                push_entry(entry, flag);
            }
        }

        (op_mask, count as u8, array)
    }
}
