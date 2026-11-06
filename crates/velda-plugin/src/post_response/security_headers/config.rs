//! Security Headers Configuration Schema & Pre-baked State.
//!
//! Encapsulates declarative configuration structures, bitmask flags for single-instruction ALU
//! dispatch, and compile-time header construction partitioned into:
//! - **Inline Registers (`<= 16` bytes)**: Written directly into [`velda_core::HeaderWorkspace`]'s
//!   128-bit hardware registers (`set_inline_ascii`).
//! - **Standard Pointers (`> 16` bytes)**: Stored as zero-copy [`HeaderValue`] references to RAM.

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
                HeaderValue::from_bytes(&raw[..*len as usize])
                    .unwrap_or_else(|_| HeaderValue::from_static(""))
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
        raw[..bytes.len()].copy_from_slice(bytes);
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

        let dummy = CompiledHeader::default();
        let mut array: [CompiledHeader; MAX_SECURITY_HEADERS] = [
            dummy.clone(),
            dummy.clone(),
            dummy.clone(),
            dummy.clone(),
            dummy.clone(),
            dummy.clone(),
            dummy.clone(),
            dummy.clone(),
            dummy.clone(),
            dummy.clone(),
            dummy.clone(),
            dummy,
        ];

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
                array[count] = entry;
                count += 1;
                op_mask |= FLAG_HSTS;
            }
        }

        // 2. Pre-compute X-Content-Type-Options (nosniff = 7 bytes <= 16 -> InlineRegister)
        if self.content_type_nosniff
            && let Some(entry) = compile_header_entry(X_CONTENT_TYPE_OPTIONS, "nosniff")
        {
            array[count] = entry;
            count += 1;
            op_mask |= FLAG_NOSNIFF;
        }

        // 3. Pre-compute X-Frame-Options (DENY = 4 bytes, SAMEORIGIN = 10 bytes <= 16 -> InlineRegister)
        if let Some(ref fo) = self.frame_options {
            let fo_trimmed = fo.trim();
            if !fo_trimmed.is_empty()
                && !fo_trimmed.eq_ignore_ascii_case("none")
                && let Some(entry) = compile_header_entry(X_FRAME_OPTIONS, fo_trimmed)
            {
                array[count] = entry;
                count += 1;
                op_mask |= FLAG_FRAME_OPTIONS;
            }
        }

        // 4. Pre-compute Referrer-Policy
        if let Some(ref rp) = self.referrer_policy {
            let rp_trimmed = rp.trim();
            if !rp_trimmed.is_empty()
                && !rp_trimmed.eq_ignore_ascii_case("none")
                && let Some(entry) = compile_header_entry(REFERRER_POLICY, rp_trimmed)
            {
                array[count] = entry;
                count += 1;
                op_mask |= FLAG_REFERRER_POLICY;
            }
        }

        // 5. Pre-compute Content-Security-Policy
        if let Some(ref csp) = self.csp {
            let csp_trimmed = csp.trim();
            if !csp_trimmed.is_empty()
                && !csp_trimmed.eq_ignore_ascii_case("none")
                && let Some(entry) =
                    compile_header_entry(CONTENT_SECURITY_POLICY.clone(), csp_trimmed)
            {
                array[count] = entry;
                count += 1;
                op_mask |= FLAG_CSP;
            }
        }

        // 6. Pre-compute Permissions-Policy
        if let Some(ref pp) = self.permissions_policy {
            let pp_trimmed = pp.trim();
            if !pp_trimmed.is_empty()
                && !pp_trimmed.eq_ignore_ascii_case("none")
                && let Some(entry) = compile_header_entry(PERMISSIONS_POLICY.clone(), pp_trimmed)
            {
                array[count] = entry;
                count += 1;
                op_mask |= FLAG_PERMISSIONS_POLICY;
            }
        }

        // 7. Pre-compute Cross-Origin-Opener-Policy (COOP)
        if let Some(ref coop) = self.cross_origin_opener_policy {
            let coop_trimmed = coop.trim();
            if !coop_trimmed.is_empty()
                && !coop_trimmed.eq_ignore_ascii_case("none")
                && let Some(entry) =
                    compile_header_entry(CROSS_ORIGIN_OPENER_POLICY.clone(), coop_trimmed)
            {
                array[count] = entry;
                count += 1;
                op_mask |= FLAG_COOP;
            }
        }

        // 8. Pre-compute Cross-Origin-Embedder-Policy (COEP)
        if let Some(ref coep) = self.cross_origin_embedder_policy {
            let coep_trimmed = coep.trim();
            if !coep_trimmed.is_empty()
                && !coep_trimmed.eq_ignore_ascii_case("none")
                && let Some(entry) =
                    compile_header_entry(CROSS_ORIGIN_EMBEDDER_POLICY.clone(), coep_trimmed)
            {
                array[count] = entry;
                count += 1;
                op_mask |= FLAG_COEP;
            }
        }

        // 9. Pre-compute Cross-Origin-Resource-Policy (CORP)
        if let Some(ref corp) = self.cross_origin_resource_policy {
            let corp_trimmed = corp.trim();
            if !corp_trimmed.is_empty()
                && !corp_trimmed.eq_ignore_ascii_case("none")
                && let Some(entry) =
                    compile_header_entry(CROSS_ORIGIN_RESOURCE_POLICY.clone(), corp_trimmed)
            {
                array[count] = entry;
                count += 1;
                op_mask |= FLAG_CORP;
            }
        }

        // 10. Pre-compute X-Permitted-Cross-Domain-Policies (none = 4 bytes <= 16 -> InlineRegister)
        if let Some(ref xpd) = self.cross_domain_policies {
            let xpd_trimmed = xpd.trim();
            if !xpd_trimmed.is_empty()
                && let Some(entry) =
                    compile_header_entry(X_PERMITTED_CROSS_DOMAIN_POLICIES.clone(), xpd_trimmed)
            {
                array[count] = entry;
                count += 1;
                op_mask |= FLAG_CROSS_DOMAIN;
            }
        }

        // 11. Pre-compute X-XSS-Protection (0 = 1 byte <= 16 -> InlineRegister)
        if let Some(ref xxss) = self.xss_protection {
            let xxss_trimmed = xxss.trim();
            if !xxss_trimmed.is_empty()
                && !xxss_trimmed.eq_ignore_ascii_case("none")
                && let Some(entry) = compile_header_entry(X_XSS_PROTECTION.clone(), xxss_trimmed)
            {
                array[count] = entry;
                count += 1;
                op_mask |= FLAG_XSS_PROTECTION;
            }
        }

        (op_mask, count as u8, array)
    }
}
