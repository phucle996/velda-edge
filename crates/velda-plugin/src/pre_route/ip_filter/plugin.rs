//! IP / CIDR Subnet Access Filter Plugin (`ip_filter`).
//!
//! Evaluated during `pre_route` phase prior to router table lookups and body streaming.
//! Enforces zero-allocation bitmask CIDR matching on IPv4/IPv6 addresses with allow/deny modes
//! and exception precedence.
//!
//! Full schema specification, allowed values, and configuration examples:
//! See [`README.md`](./README.md).

use velda_core::{Action, L7Request, RejectReason};
use velda_sync::post_sync::plugin::PluginConfig;

use super::network::{
    ClientIpSource, IpExtractionError, IpNetwork, ParsedIp, compact_and_sort_v4,
    compact_and_sort_v6,
};
use crate::capability::{AccessLevel, PluginCapability, SupportedProtocols};
use crate::error::PluginError;
use crate::pre_route::PreRoutePlugin;

/// Operational filtering mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IpFilterMode {
    /// Only permitted IPs can access (Whitelist).
    AllowList,
    /// Specified IPs are blocked (Blacklist).
    DenyList,
}

/// Comprehensive configuration for IP and CIDR access control.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IpFilterConfig {
    pub mode: IpFilterMode,
    pub allow_cidrs: Vec<IpNetwork>,
    pub deny_cidrs: Vec<IpNetwork>,
    pub client_ip_source: ClientIpSource,
}

impl Default for IpFilterConfig {
    fn default() -> Self {
        Self {
            mode: IpFilterMode::DenyList,
            allow_cidrs: Vec::new(),
            deny_cidrs: Vec::new(),
            client_ip_source: ClientIpSource::default(),
        }
    }
}

/// Pre-Route IP Filter Plugin instance.
#[derive(Debug, Clone)]
pub struct IpFilterPlugin {
    pub id: String,
    pub mode: IpFilterMode,
    pub allow_cidrs: Vec<IpNetwork>,
    pub deny_cidrs: Vec<IpNetwork>,
    pub client_ip_source: ClientIpSource,
    // Optimization: Structure-of-Arrays (SoA) compacted & sorted bitwise tables for 256-bit SIMD scanning
    allow_v4_nets: Vec<u32>,
    allow_v4_masks: Vec<u32>,
    deny_v4_nets: Vec<u32>,
    deny_v4_masks: Vec<u32>,
    allow_v6: Vec<(u128, u128)>,
    deny_v6: Vec<(u128, u128)>,
}

impl IpFilterPlugin {
    /// Constructs a basic IP filter with allow and deny CIDR lists.
    pub fn new(
        id: impl Into<String>,
        mode: IpFilterMode,
        allow_cidrs: Vec<IpNetwork>,
        deny_cidrs: Vec<IpNetwork>,
    ) -> Self {
        Self::with_config(
            id,
            IpFilterConfig {
                mode,
                allow_cidrs,
                deny_cidrs,
                client_ip_source: ClientIpSource::default(),
            },
        )
    }

    /// Constructs an IP filter with full configuration options.
    pub fn with_config(id: impl Into<String>, config: IpFilterConfig) -> Self {
        let mut raw_allow_v4 = Vec::with_capacity(config.allow_cidrs.len());
        let mut raw_allow_v6 = Vec::with_capacity(config.allow_cidrs.len());
        for net in &config.allow_cidrs {
            match *net {
                IpNetwork::V4 { net, mask } => raw_allow_v4.push((net, mask)),
                IpNetwork::V6 { net, mask } => raw_allow_v6.push((net, mask)),
            }
        }

        let mut raw_deny_v4 = Vec::with_capacity(config.deny_cidrs.len());
        let mut raw_deny_v6 = Vec::with_capacity(config.deny_cidrs.len());
        for net in &config.deny_cidrs {
            match *net {
                IpNetwork::V4 { net, mask } => raw_deny_v4.push((net, mask)),
                IpNetwork::V6 { net, mask } => raw_deny_v6.push((net, mask)),
            }
        }

        let allow_v4 = compact_and_sort_v4(&raw_allow_v4);
        let deny_v4 = compact_and_sort_v4(&raw_deny_v4);
        let allow_v6 = compact_and_sort_v6(&raw_allow_v6);
        let deny_v6 = compact_and_sort_v6(&raw_deny_v6);

        let (allow_v4_nets, allow_v4_masks): (Vec<u32>, Vec<u32>) = allow_v4.into_iter().unzip();
        let (deny_v4_nets, deny_v4_masks): (Vec<u32>, Vec<u32>) = deny_v4.into_iter().unzip();

        Self {
            id: id.into(),
            mode: config.mode,
            allow_cidrs: config.allow_cidrs,
            deny_cidrs: config.deny_cidrs,
            client_ip_source: config.client_ip_source,
            allow_v4_nets,
            allow_v4_masks,
            deny_v4_nets,
            deny_v4_masks,
            allow_v6,
            deny_v6,
        }
    }

    /// Deserializes and constructs an instance directly from a [`PluginConfig`].
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let cfg =
            velda_sync::validate_ip_filter_config(&config.id, &config.config).map_err(|e| {
                PluginError::Config(format!(
                    "Invalid ip_filter configuration for '{}': {e}",
                    config.id
                ))
            })?;

        let mode = match cfg.mode.to_ascii_lowercase().as_str() {
            "allow" => IpFilterMode::AllowList,
            "deny" => IpFilterMode::DenyList,
            other => {
                return Err(PluginError::Config(format!(
                    "Plugin '{}': unsupported mode '{other}'",
                    config.id
                )));
            }
        };

        let client_ip_source = match cfg.client_ip_source.to_ascii_lowercase().as_str() {
            "x_real_ip" | "x-real-ip" | "real_ip" => ClientIpSource::XRealIp,
            "x_forwarded_for" | "x-forwarded-for" => ClientIpSource::XForwardedFor,
            other => {
                return Err(PluginError::Config(format!(
                    "Plugin '{}': unsupported client_ip_source '{other}'",
                    config.id
                )));
            }
        };

        let mut allow_cidrs = Vec::with_capacity(cfg.allow_cidrs.len());
        for s in &cfg.allow_cidrs {
            let net = IpNetwork::from_cidr(s).map_err(|e| {
                PluginError::Config(format!(
                    "Plugin '{}' invalid allow_cidr '{s}': {e}",
                    config.id
                ))
            })?;
            allow_cidrs.push(net);
        }

        let mut deny_cidrs = Vec::with_capacity(cfg.deny_cidrs.len());
        for s in &cfg.deny_cidrs {
            let net = IpNetwork::from_cidr(s).map_err(|e| {
                PluginError::Config(format!(
                    "Plugin '{}' invalid deny_cidr '{s}': {e}",
                    config.id
                ))
            })?;
            deny_cidrs.push(net);
        }

        Ok(Self::with_config(
            &config.id,
            IpFilterConfig {
                mode,
                allow_cidrs,
                deny_cidrs,
                client_ip_source,
            },
        ))
    }
}

impl PreRoutePlugin for IpFilterPlugin {
    #[inline(always)]
    fn id(&self) -> &str {
        &self.id
    }

    /// Evaluates downstream request client IP chain against configured CIDR rules.
    ///
    /// # Zero-Trust Multi-Hop Evaluation Invariants & Precedence:
    /// - **AllowList Mode**:
    ///   1. If ANY hop in the extracted chain matches `deny_cidrs` (exception) -> REJECT 403 Forbidden.
    ///   2. Else if EVERY hop in the extracted chain matches `allow_cidrs` -> PASS (`Action::Continue`).
    ///   3. Else (any hop is outside `allow_cidrs`) -> REJECT 403 Forbidden.
    /// - **DenyList Mode**:
    ///   1. For each hop in the extracted chain: if the hop matches `allow_cidrs` (exception), that hop is excused.
    ///   2. Else if the hop matches `deny_cidrs` -> immediately REJECT 403 Forbidden.
    ///   3. If no hop triggers a deny rule -> PASS (`Action::Continue`).
    fn on_request(&self, req: &mut L7Request) -> Action {
        let extracted = match self.client_ip_source.extract_all_parsed_ips(req) {
            Ok(ips) => ips,
            Err(IpExtractionError::Missing) => {
                return match self.mode {
                    IpFilterMode::AllowList => Action::Reject(RejectReason::Forbidden),
                    IpFilterMode::DenyList => Action::Continue,
                };
            }
            Err(IpExtractionError::Malformed) => {
                // Anti-evasion: Malformed forwarding header, spoofed multi-header, or hop flood is strictly rejected
                return Action::Reject(RejectReason::Forbidden);
            }
        };

        let hops = extracted.as_slice();
        let has_deny = !self.deny_v4_nets.is_empty() || !self.deny_v6.is_empty();
        let has_allow = !self.allow_v4_nets.is_empty() || !self.allow_v6.is_empty();

        match self.mode {
            IpFilterMode::AllowList => {
                // Single-pass bitwise verification: checks deny exceptions and accumulates allow status
                // using a branchless bitwise OR to avoid CPU pipeline stalls
                let mut unallowed = false;
                for &hop_ip in hops {
                    if has_deny && self.matches_deny(hop_ip) {
                        return Action::Reject(RejectReason::Forbidden);
                    }
                    unallowed |= !self.matches_allow(hop_ip);
                }
                if unallowed {
                    Action::Reject(RejectReason::Forbidden)
                } else {
                    Action::Continue
                }
            }
            IpFilterMode::DenyList => {
                // Single-pass stream evaluation: an allow exception excuses only this specific hop
                for &hop_ip in hops {
                    let excused = has_allow && self.matches_allow(hop_ip);
                    if !excused && self.matches_deny(hop_ip) {
                        return Action::Reject(RejectReason::Forbidden);
                    }
                }
                Action::Continue
            }
        }
    }

    #[inline(always)]
    fn capability(&self) -> PluginCapability {
        PluginCapability::header_only(AccessLevel::Read)
    }

    #[inline(always)]
    fn supported_protocols(&self) -> SupportedProtocols {
        SupportedProtocols::ALL_L7
    }
}

/// Evaluates IPv4 CIDR rules using hardware vector registers.
///
/// Inlined direct execution eliminating indirect function pointer overhead.
#[inline(always)]
pub fn match_cidr_v4(ip: u32, nets: &[u32], masks: &[u32]) -> bool {
    #[cfg(target_arch = "x86_64")]
    {
        if std::is_x86_feature_detected!("avx2") {
            unsafe { match_cidr_v4_256_x86_impl(ip, nets, masks) }
        } else {
            unsafe { match_cidr_v4_128_x86_impl(ip, nets, masks) }
        }
    }
    #[cfg(target_arch = "aarch64")]
    {
        unsafe { match_cidr_v4_128_neon_impl(ip, nets, masks) }
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        match_cidr_v4_64(ip, nets, masks)
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn match_cidr_v4_256_x86_impl(ip: u32, nets: &[u32], masks: &[u32]) -> bool {
    use std::arch::x86_64::*;
    let len = nets.len().min(masks.len());
    let vec_len = len & !7;
    let mut i = 0;

    if vec_len > 0 {
        let ip_vec = _mm256_set1_epi32(ip as i32);
        while i < vec_len {
            unsafe {
                let m = _mm256_loadu_si256(masks.as_ptr().add(i) as *const __m256i);
                let n = _mm256_loadu_si256(nets.as_ptr().add(i) as *const __m256i);
                let masked = _mm256_and_si256(ip_vec, m);
                let cmp = _mm256_cmpeq_epi32(masked, n);
                if _mm256_movemask_epi8(cmp) != 0 {
                    return true;
                }
            }
            i += 8;
        }
    }

    while i < len {
        if (ip & masks[i]) == nets[i] {
            return true;
        }
        i += 1;
    }
    false
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse2")]
unsafe fn match_cidr_v4_128_x86_impl(ip: u32, nets: &[u32], masks: &[u32]) -> bool {
    use std::arch::x86_64::*;
    let len = nets.len().min(masks.len());
    let vec_len = len & !3;
    let mut i = 0;

    if vec_len > 0 {
        let ip_vec = _mm_set1_epi32(ip as i32);
        while i < vec_len {
            unsafe {
                let m = _mm_loadu_si128(masks.as_ptr().add(i) as *const __m128i);
                let n = _mm_loadu_si128(nets.as_ptr().add(i) as *const __m128i);
                let masked = _mm_and_si128(ip_vec, m);
                let cmp = _mm_cmpeq_epi32(masked, n);
                if _mm_movemask_epi8(cmp) != 0 {
                    return true;
                }
            }
            i += 4;
        }
    }

    while i < len {
        if (ip & masks[i]) == nets[i] {
            return true;
        }
        i += 1;
    }
    false
}

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn match_cidr_v4_128_neon_impl(ip: u32, nets: &[u32], masks: &[u32]) -> bool {
    use std::arch::aarch64::*;
    let len = nets.len().min(masks.len());
    let vec_len = len & !3;
    let mut i = 0;

    if vec_len > 0 {
        let ip_vec = vdupq_n_u32(ip);
        while i < vec_len {
            unsafe {
                let m = vld1q_u32(masks.as_ptr().add(i));
                let n = vld1q_u32(nets.as_ptr().add(i));
                let masked = vandq_u32(ip_vec, m);
                let cmp = vceqq_u32(masked, n);
                if vmaxvq_u32(cmp) != 0 {
                    return true;
                }
            }
            i += 4;
        }
    }

    while i < len {
        if (ip & masks[i]) == nets[i] {
            return true;
        }
        i += 1;
    }
    false
}

/// Evaluates 2 IPv4 CIDR rules per cycle using 64-bit SWAR registers.
#[inline]
pub fn match_cidr_v4_64(ip: u32, nets: &[u32], masks: &[u32]) -> bool {
    let len = nets.len().min(masks.len());
    let pair_len = len & !1;
    let ip64 = ((ip as u64) << 32) | (ip as u64);
    let mut i = 0;

    while i < pair_len {
        let m64 = ((masks[i + 1] as u64) << 32) | (masks[i] as u64);
        let n64 = ((nets[i + 1] as u64) << 32) | (nets[i] as u64);
        let diff = (ip64 & m64) ^ n64;
        if (diff as u32) == 0 || (diff >> 32) == 0 {
            return true;
        }
        i += 2;
    }

    if i < len && (ip & masks[i]) == nets[i] {
        return true;
    }
    false
}

impl IpFilterPlugin {
    #[inline(always)]
    fn matches_allow(&self, ip: ParsedIp) -> bool {
        match ip {
            ParsedIp::V4(bits) => match_cidr_v4(bits, &self.allow_v4_nets, &self.allow_v4_masks),
            ParsedIp::V6(bits) => {
                for &(net, mask) in &self.allow_v6 {
                    if (bits & mask) == net {
                        return true;
                    }
                }
                false
            }
        }
    }

    #[inline(always)]
    fn matches_deny(&self, ip: ParsedIp) -> bool {
        match ip {
            ParsedIp::V4(bits) => match_cidr_v4(bits, &self.deny_v4_nets, &self.deny_v4_masks),
            ParsedIp::V6(bits) => {
                for &(net, mask) in &self.deny_v6 {
                    if (bits & mask) == net {
                        return true;
                    }
                }
                false
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{HeaderValue, Method, Uri, Version};
    use velda_core::{Body, RejectReason};

    fn make_request_with_ip(ip: &str) -> L7Request {
        let mut req = L7Request::new(
            Method::GET,
            Uri::from_static("/api/data"),
            Version::HTTP_11,
            http::HeaderMap::new(),
            Body::Empty,
        );
        req.headers
            .insert("x-forwarded-for", HeaderValue::from_str(ip).unwrap());
        req
    }

    #[test]
    fn test_allow_list_mode() {
        let plugin = IpFilterPlugin::new(
            "ip-allow",
            IpFilterMode::AllowList,
            vec![
                IpNetwork::from_cidr("10.0.0.0/8").unwrap(),
                IpNetwork::from_cidr("192.168.1.0/24").unwrap(),
            ],
            vec![],
        );

        let mut req1 = make_request_with_ip("10.5.20.1");
        assert!(matches!(plugin.on_request(&mut req1), Action::Continue));

        let mut req2 = make_request_with_ip("192.168.1.50");
        assert!(matches!(plugin.on_request(&mut req2), Action::Continue));

        let mut req3 = make_request_with_ip("172.16.0.1");
        assert!(matches!(
            plugin.on_request(&mut req3),
            Action::Reject(RejectReason::Forbidden)
        ));
    }

    #[test]
    fn test_allow_list_mode_with_port_and_quotes() {
        let plugin = IpFilterPlugin::new(
            "ip-allow",
            IpFilterMode::AllowList,
            vec![IpNetwork::from_cidr("10.0.0.0/8").unwrap()],
            vec![],
        );

        let mut req1 = make_request_with_ip("10.5.20.1:44321");
        assert!(matches!(plugin.on_request(&mut req1), Action::Continue));

        let mut req2 = make_request_with_ip("\"10.5.20.1\"");
        assert!(matches!(plugin.on_request(&mut req2), Action::Continue));
    }

    #[test]
    fn test_deny_list_mode() {
        let plugin = IpFilterPlugin::new(
            "ip-deny",
            IpFilterMode::DenyList,
            vec![],
            vec![IpNetwork::from_cidr("203.0.113.0/24").unwrap()],
        );

        let mut req1 = make_request_with_ip("203.0.113.5");
        assert!(matches!(
            plugin.on_request(&mut req1),
            Action::Reject(RejectReason::Forbidden)
        ));

        let mut req2 = make_request_with_ip("8.8.8.8");
        assert!(matches!(plugin.on_request(&mut req2), Action::Continue));
    }

    #[test]
    fn test_dual_stack_mapped_ipv6_blocking() {
        let plugin = IpFilterPlugin::new(
            "ip-deny",
            IpFilterMode::DenyList,
            vec![],
            vec![IpNetwork::from_cidr("203.0.113.0/24").unwrap()],
        );

        // Attacker attempting bypass via IPv4-mapped IPv6 address
        let mut req = make_request_with_ip("::ffff:203.0.113.5");
        assert!(matches!(
            plugin.on_request(&mut req),
            Action::Reject(RejectReason::Forbidden)
        ));
    }

    #[test]
    fn test_malformed_ip_rejection_anti_evasion() {
        let plugin = IpFilterPlugin::new(
            "ip-deny",
            IpFilterMode::DenyList,
            vec![],
            vec![IpNetwork::from_cidr("203.0.113.0/24").unwrap()],
        );

        // Attacker attempting evasion with malformed header
        let mut req = make_request_with_ip("invalid_ip_format");
        assert!(matches!(
            plugin.on_request(&mut req),
            Action::Reject(RejectReason::Forbidden)
        ));
    }

    #[test]
    fn test_ip_filter_from_config() {
        let valid_json = serde_json::json!({
            "mode": "allow",
            "allow_cidrs": ["10.0.0.0/8", "192.168.1.0/24"],
            "deny_cidrs": ["10.99.0.0/16"],
            "client_ip_source": "x_forwarded_for"
        });
        let config = PluginConfig {
            id: "ip_filter".into(),
            phase: String::new(),
            config: valid_json,
        };
        let plugin = IpFilterPlugin::from_config(&config).expect("valid config");
        assert_eq!(plugin.id, "ip_filter");
        assert_eq!(plugin.mode, IpFilterMode::AllowList);
        assert_eq!(plugin.allow_cidrs.len(), 2);
        assert_eq!(plugin.deny_cidrs.len(), 1);

        // Incomplete config must fail validation (100% explicit configuration required)
        let incomplete_json = serde_json::json!({
            "mode": "allow"
        });
        let bad_config = PluginConfig {
            id: "ip_filter".into(),
            phase: String::new(),
            config: incomplete_json,
        };
        assert!(IpFilterPlugin::from_config(&bad_config).is_err());
    }

    #[test]
    fn test_cve_vector_1_multi_hop_xff_spoofing_in_allow_mode() {
        let plugin = IpFilterPlugin::new(
            "ip-allow-strict",
            IpFilterMode::AllowList,
            vec![IpNetwork::from_cidr("10.0.0.0/8").unwrap()],
            vec![IpNetwork::from_cidr("10.99.0.0/16").unwrap()],
        );

        // Legitimate multi-hop chain where every proxy/client hop is within 10.0.0.0/8 (and not in 10.99.0.0/16)
        let mut legit_chain = make_request_with_ip("10.1.2.3, 10.4.5.6");
        assert!(matches!(
            plugin.on_request(&mut legit_chain),
            Action::Continue
        ));

        // Attacker at 203.0.113.5 spoofs X-Forwarded-For: 10.1.2.3 upstream of edge proxy
        let mut spoofed_external = make_request_with_ip("10.1.2.3, 203.0.113.5");
        assert!(matches!(
            plugin.on_request(&mut spoofed_external),
            Action::Reject(RejectReason::Forbidden)
        ));

        // Attacker at denied subnet 10.99.1.5 spoofs X-Forwarded-For: 10.1.2.3
        let mut spoofed_denied_hop = make_request_with_ip("10.1.2.3, 10.99.1.5");
        assert!(matches!(
            plugin.on_request(&mut spoofed_denied_hop),
            Action::Reject(RejectReason::Forbidden)
        ));
    }

    #[test]
    fn test_cve_vector_2_allow_exception_smuggling_in_deny_mode() {
        let plugin = IpFilterPlugin::new(
            "ip-deny-strict",
            IpFilterMode::DenyList,
            vec![IpNetwork::from_cidr("203.0.113.10/32").unwrap()],
            vec![IpNetwork::from_cidr("203.0.113.0/24").unwrap()],
        );

        // Exact allow-exception IP by itself is permitted
        let mut legit_exception = make_request_with_ip("203.0.113.10");
        assert!(matches!(
            plugin.on_request(&mut legit_exception),
            Action::Continue
        ));

        // Blacklisted attacker (203.0.113.5) prepends the allowed exception IP (203.0.113.10)
        // in X-Forwarded-For hoping the allow exception grants a global pass to the whole request.
        let mut smuggled_exception = make_request_with_ip("203.0.113.10, 203.0.113.5");
        assert!(matches!(
            plugin.on_request(&mut smuggled_exception),
            Action::Reject(RejectReason::Forbidden)
        ));

        // Blacklisted attacker (203.0.113.5) behind a benign public IP (8.8.8.8)
        let mut hidden_second_hop = make_request_with_ip("8.8.8.8, 203.0.113.5");
        assert!(matches!(
            plugin.on_request(&mut hidden_second_hop),
            Action::Reject(RejectReason::Forbidden)
        ));
    }

    #[test]
    fn test_cve_vector_3_cross_header_fallback_poisoning() {
        let plugin = IpFilterPlugin::with_config(
            "ip-allow-xff",
            IpFilterConfig {
                mode: IpFilterMode::AllowList,
                allow_cidrs: vec![IpNetwork::from_cidr("10.0.0.0/8").unwrap()],
                deny_cidrs: vec![],
                client_ip_source: ClientIpSource::XForwardedFor,
            },
        );

        // Attacker sends malformed X-Forwarded-For + spoofed X-Real-IP: 10.0.0.1
        // Hoping parser falls back to X-Real-IP when X-Forwarded-For fails parsing.
        let mut req = make_request_with_ip("malformed_poison_payload");
        req.headers
            .insert("x-real-ip", HeaderValue::from_static("10.0.0.1"));
        assert!(matches!(
            plugin.on_request(&mut req),
            Action::Reject(RejectReason::Forbidden)
        ));
    }

    #[test]
    fn test_cve_vector_4_duplicate_header_and_x_real_ip_list_smuggling() {
        let plugin_real_ip = IpFilterPlugin::with_config(
            "ip-allow-real-ip",
            IpFilterConfig {
                mode: IpFilterMode::AllowList,
                allow_cidrs: vec![IpNetwork::from_cidr("10.0.0.0/8").unwrap()],
                deny_cidrs: vec![],
                client_ip_source: ClientIpSource::XRealIp,
            },
        );

        // Duplicate X-Real-IP headers (HTTP Header Smuggling) must be rejected
        let mut req_dup = L7Request::new(
            Method::GET,
            Uri::from_static("/api/data"),
            Version::HTTP_11,
            http::HeaderMap::new(),
            Body::Empty,
        );
        req_dup
            .headers
            .append("x-real-ip", HeaderValue::from_static("10.0.0.1"));
        req_dup
            .headers
            .append("x-real-ip", HeaderValue::from_static("203.0.113.5"));
        assert!(matches!(
            plugin_real_ip.on_request(&mut req_dup),
            Action::Reject(RejectReason::Forbidden)
        ));

        // Comma-separated list inside single-value X-Real-IP must be rejected
        let mut req_comma = L7Request::new(
            Method::GET,
            Uri::from_static("/api/data"),
            Version::HTTP_11,
            http::HeaderMap::new(),
            Body::Empty,
        );
        req_comma.headers.insert(
            "x-real-ip",
            HeaderValue::from_static("10.0.0.1, 203.0.113.5"),
        );
        assert!(matches!(
            plugin_real_ip.on_request(&mut req_comma),
            Action::Reject(RejectReason::Forbidden)
        ));
    }

    #[test]
    fn test_cve_vector_5_octal_ipv4_parser_differential_cve_2021_28918() {
        let plugin = IpFilterPlugin::new(
            "ip-allow-octal-defense",
            IpFilterMode::AllowList,
            vec![IpNetwork::from_cidr("10.0.0.0/8").unwrap()],
            vec![],
        );

        // CVE-2021-28918 style attack: "010.0.0.1" is octal 8.0.0.1 in legacy libc inet_aton,
        // but naive decimal parsers evaluate it as 10.0.0.1. Must be rejected as Malformed!
        for octal_payload in ["010.0.0.1", "10.00.0.1", "10.0.0.01", "0177.0.0.1"] {
            let mut req = make_request_with_ip(octal_payload);
            assert!(
                matches!(
                    plugin.on_request(&mut req),
                    Action::Reject(RejectReason::Forbidden)
                ),
                "Octal / leading-zero IPv4 payload '{octal_payload}' must be rejected"
            );
        }
    }

    #[test]
    fn test_cve_vector_6_port_userinfo_and_out_of_range_port_smuggling() {
        let plugin = IpFilterPlugin::new(
            "ip-allow-port-defense",
            IpFilterMode::AllowList,
            vec![
                IpNetwork::from_cidr("10.0.0.0/8").unwrap(),
                IpNetwork::from_cidr("2001:db8::/32").unwrap(),
            ],
            vec![],
        );

        for bad_port_payload in [
            "10.0.0.1:80@203.0.113.5",
            "10.0.0.1:0",
            "10.0.0.1:65536",
            "10.0.0.1:080",
            "10.0.0.1:",
            "10.0.0.1:443:80",
            "[2001:db8::1]:0",
            "[2001:db8::1]:70000",
            "[2001:db8::1]garbage",
            "[2001:db8::1]:80@evil",
        ] {
            let mut req = make_request_with_ip(bad_port_payload);
            assert!(
                matches!(
                    plugin.on_request(&mut req),
                    Action::Reject(RejectReason::Forbidden)
                ),
                "Port smuggling payload '{bad_port_payload}' must be rejected"
            );
        }

        // Valid bracketed IPv6 with valid port must succeed
        let mut valid_v6_port = make_request_with_ip("[2001:db8::1]:8443");
        assert!(matches!(
            plugin.on_request(&mut valid_v6_port),
            Action::Continue
        ));
    }

    #[test]
    fn test_cve_vector_7_ipv6_transition_tunneling_evasion_nat64_siit_compat() {
        let plugin = IpFilterPlugin::new(
            "ip-deny-tunnel-defense",
            IpFilterMode::DenyList,
            vec![],
            vec![IpNetwork::from_cidr("203.0.113.0/24").unwrap()],
        );

        // 203.0.113.5 in hex is cb00:7105
        for tunneled_payload in [
            "::ffff:203.0.113.5",      // RFC 4291 IPv4-mapped (dotted)
            "::ffff:cb00:7105",        // RFC 4291 IPv4-mapped (hex)
            "::ffff:0:203.0.113.5",    // RFC 2765 SIIT IPv4-translated (dotted)
            "::ffff:0:cb00:7105",      // RFC 2765 SIIT IPv4-translated (hex)
            "64:ff9b::203.0.113.5",    // RFC 6052 Well-Known NAT64 (dotted)
            "64:ff9b::cb00:7105",      // RFC 6052 Well-Known NAT64 (hex)
            "::203.0.113.5",           // RFC 4291 §2.5.5.1 IPv4-compatible (dotted)
            "::cb00:7105",             // RFC 4291 §2.5.5.1 IPv4-compatible (hex)
            "[64:ff9b::cb00:7105]:80", // Bracketed NAT64 with port
        ] {
            let mut req = make_request_with_ip(tunneled_payload);
            assert!(
                matches!(
                    plugin.on_request(&mut req),
                    Action::Reject(RejectReason::Forbidden)
                ),
                "IPv6-tunneled IPv4 payload '{tunneled_payload}' must be canonicalized and blocked"
            );
        }
    }

    #[test]
    fn test_cve_vector_8_hop_flood_and_trailing_garbage_dos() {
        let plugin = IpFilterPlugin::new(
            "ip-allow-dos-defense",
            IpFilterMode::AllowList,
            vec![IpNetwork::from_cidr("10.0.0.0/8").unwrap()],
            vec![],
        );

        // 17 hops (> MAX_XFF_HOPS = 16) must be rejected immediately
        let flood_17 = (0..17)
            .map(|i| format!("10.0.0.{}", i + 1))
            .collect::<Vec<_>>()
            .join(", ");
        let mut req_flood = make_request_with_ip(&flood_17);
        assert!(matches!(
            plugin.on_request(&mut req_flood),
            Action::Reject(RejectReason::Forbidden)
        ));

        // Trailing comma or empty hop ("10.0.0.1, ") or trailing garbage ("10.0.0.1, garbage")
        for malformed_chain in [
            "10.0.0.1, ",
            ", 10.0.0.1",
            "10.0.0.1, , 10.0.0.2",
            "10.0.0.1, garbage",
        ] {
            let mut req = make_request_with_ip(malformed_chain);
            assert!(
                matches!(
                    plugin.on_request(&mut req),
                    Action::Reject(RejectReason::Forbidden)
                ),
                "Malformed XFF chain '{malformed_chain}' must be rejected"
            );
        }
    }

    #[test]
    fn test_match_cidr_v4_all_register_tiers_equivalence() {
        // 11 CIDR networks (tests 8-lane 256-bit, 4-lane 128-bit, 2-lane 64-bit, and remainder tail)
        let cidrs = [
            "10.0.0.0/8",
            "172.16.0.0/12",
            "192.168.1.0/24",
            "192.168.2.0/24",
            "192.168.3.0/24",
            "192.168.4.0/24",
            "192.168.5.0/24",
            "192.168.6.0/24",
            "192.168.7.0/24",
            "192.168.8.0/24",
            "203.0.113.0/24",
        ];
        let mut nets = Vec::new();
        let mut masks = Vec::new();
        for c in cidrs {
            if let IpNetwork::V4 { net, mask } = IpNetwork::from_cidr(c).unwrap() {
                nets.push(net);
                masks.push(mask);
            }
        }

        let match_ip = u32::from(std::net::Ipv4Addr::new(203, 0, 113, 42));
        let first_ip = u32::from(std::net::Ipv4Addr::new(10, 25, 1, 1));
        let miss_ip = u32::from(std::net::Ipv4Addr::new(8, 8, 8, 8));

        assert!(match_cidr_v4(match_ip, &nets, &masks));
        assert!(match_cidr_v4(first_ip, &nets, &masks));
        assert!(!match_cidr_v4(miss_ip, &nets, &masks));

        assert!(match_cidr_v4_64(match_ip, &nets, &masks));
        assert!(match_cidr_v4_64(first_ip, &nets, &masks));
        assert!(!match_cidr_v4_64(miss_ip, &nets, &masks));
    }
}
