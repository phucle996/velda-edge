//! ============================================================================
//! VELDA EDGE PLUGIN MANIFEST & HUMAN SPECIFICATION
//! ============================================================================
//!
//! - **PLUGIN NAME**: `IP_FILTER`
//! - **CANONICAL PHASE**: `PRE_ROUTE`
//! - **EXECUTION ORDER**: STABLY SORTED DESCENDING BY ROUTE PRIORITY (FAIL-FAST GATEKEEPER)
//! - **REVERSE CAPABILITIES**: HEADER-ONLY READ (`PluginCapability::header_only(AccessLevel::Read)`)
//! - **SUPPORTED PROTOCOLS**: `ALL_L7` (`SupportedProtocols::ALL_L7`: HTTP/1.1, HTTP/2, HTTP/3, gRPC)
//!
//! ----------------------------------------------------------------------------
//! ### 1. PURPOSE & WHAT IT PROTECTS ("Phục vụ cái gì?")
//! ----------------------------------------------------------------------------
//! Acts as an L7 edge gatekeeper enforcing IP address and CIDR subnet access control
//! before route lookup, body streaming, or upstream leasing.
//!
//! 1. **Fail-Fast Access Control**:
//!    Rejects unauthorized clients immediately with 403 Forbidden, saving 100% of CPU cycles
//!    that would otherwise be wasted on routing trie lookups, body streaming, or upstream pool leases.
//! 2. **CIDR Subnet Filtering (IPv4 & IPv6)**:
//!    Supports both single IP addresses (`192.168.1.50`, `::1`) and broad network prefixes
//!    (`10.0.0.0/8`, `172.16.0.0/12`, `2001:db8::/32`) using zero-allocation bitmask math.
//! 3. **Operational Mode Switching (AllowList vs DenyList)**:
//!    - `AllowList` (Whitelist): Only authorized networks can access; all others are rejected.
//!    - `DenyList` (Blacklist): Specific malicious or abusive networks are blocked; all others pass.
//! 4. **Exception Handling & Incident Lockdown**:
//!    Allows maintaining both `allow_cidrs` and `deny_cidrs` simultaneously.
//!    Deny rules take precedence over allow rules (e.g. allow entire VPC `10.0.0.0/8` except compromised host `10.0.5.99`).
//!
//! ----------------------------------------------------------------------------
//! ### 2. WHAT IS OUT OF SCOPE ("KHÔNG phục vụ cái gì? Ranh giới cấm")
//! ----------------------------------------------------------------------------
//! - **DOES NOT inspect or mutate request headers/body**: Operates purely on client IP detection.
//! - **DOES NOT perform cryptographic token validation**: Token/Subject authentication is owned by `jwt_auth`.
//! - **DOES NOT enforce traffic rate pacing**: Request pacing and burst limiting is owned by `rate_limit`.
//! - **DOES NOT manage L4 TCP firewalling**: L4 raw socket drops belong to kernel/iptables/eBPF;
//!   this plugin operates at L7 HTTP/gRPC ingress.
//!
//! ----------------------------------------------------------------------------
//! ### 3. ARCHITECTURAL & HOT-PATH INVARIANTS ("Notes & Performance Contracts")
//! ----------------------------------------------------------------------------
//! - **Zero-Allocation Hot Path**:
//!   All CIDR strings are parsed, normalized, and pre-compiled at configuration time into
//!   strongly-typed [`IpNetwork`] structures with pre-calculated integer bitmasks (`u32` for IPv4,
//!   `u128` for IPv6). Matching on hot path executes in single-digit nanoseconds using pure bitwise operations
//!   (`(ip_bits & mask) == net_bits`), performing strictly **0 heap allocations**.
//! - **Strict "none" Sentinel Value**:
//!   In JSON configuration, any unused list must be explicitly declared as `"none"`
//!   (e.g. `"deny_cidrs": "none"`).
//!
//! ----------------------------------------------------------------------------
//! ### 4. CONFIGURATION SCHEMA EXAMPLE
//! ----------------------------------------------------------------------------
//! ```json
//! {
//!   "id": "ip_filter",
//!   "priority": 100,
//!   "config": {
//!     "mode": "allow",
//!     "allow_cidrs": [
//!       "10.0.0.0/8",
//!       "192.168.1.0/24",
//!       "2001:db8::/32"
//!     ],
//!     "deny_cidrs": "none",
//!     "client_ip_source": "x_forwarded_for"
//!   }
//! }
//! ```
//! ============================================================================

use std::net::IpAddr;

use http::StatusCode;
use velda_core::{Action, L7Request, L7Response};
use velda_sync::post_sync::plugin::PluginConfig;

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

/// Preferred source for determining the downstream client's IP address.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ClientIpSource {
    /// Inspects the `X-Forwarded-For` header first (first client IP), falling back to `X-Real-IP`.
    #[default]
    XForwardedFor,
    /// Inspects the `X-Real-IP` header first, falling back to `X-Forwarded-For`.
    XRealIp,
}

/// Strongly typed, zero-allocation CIDR network prefix matcher for IPv4 and IPv6.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IpNetwork {
    V4 { net: u32, mask: u32 },
    V6 { net: u128, mask: u128 },
}

impl IpNetwork {
    /// Parses an exact IP address or CIDR notation (e.g. `10.0.0.0/8`, `192.168.1.5`, `2001:db8::/32`).
    pub fn from_cidr(s: &str) -> Result<Self, String> {
        let s = s.trim();
        if let Some((ip_str, prefix_str)) = s.split_once('/') {
            let ip: IpAddr = ip_str
                .trim()
                .parse()
                .map_err(|e| format!("Invalid IP address '{ip_str}': {e}"))?;
            let prefix: u8 = prefix_str
                .trim()
                .parse()
                .map_err(|e| format!("Invalid CIDR prefix '{prefix_str}': {e}"))?;

            match ip {
                IpAddr::V4(v4) => {
                    if prefix > 32 {
                        return Err(format!("IPv4 prefix cannot exceed 32, got {prefix}"));
                    }
                    let mask = if prefix == 0 {
                        0u32
                    } else {
                        !0u32 << (32 - prefix)
                    };
                    let net = v4.to_bits() & mask;
                    Ok(Self::V4 { net, mask })
                }
                IpAddr::V6(v6) => {
                    if prefix > 128 {
                        return Err(format!("IPv6 prefix cannot exceed 128, got {prefix}"));
                    }
                    let mask = if prefix == 0 {
                        0u128
                    } else {
                        !0u128 << (128 - prefix)
                    };
                    let net = v6.to_bits() & mask;
                    Ok(Self::V6 { net, mask })
                }
            }
        } else {
            // Single exact IP address
            let ip: IpAddr = s
                .parse()
                .map_err(|e| format!("Invalid IP address '{s}': {e}"))?;
            match ip {
                IpAddr::V4(v4) => Ok(Self::V4 {
                    net: v4.to_bits(),
                    mask: !0u32,
                }),
                IpAddr::V6(v6) => Ok(Self::V6 {
                    net: v6.to_bits(),
                    mask: !0u128,
                }),
            }
        }
    }

    /// Tests whether an [`IpAddr`] falls within this network prefix.
    ///
    /// # Performance Contract
    /// Executes in 1-2 CPU clock cycles using pure bitwise operations:
    /// `(ip_bits & mask) == net_bits`. **Zero heap allocations**.
    #[inline(always)]
    pub fn contains(&self, ip: IpAddr) -> bool {
        match (self, ip) {
            (Self::V4 { net, mask }, IpAddr::V4(v4)) => (v4.to_bits() & mask) == *net,
            (Self::V6 { net, mask }, IpAddr::V6(v6)) => (v6.to_bits() & mask) == *net,
            _ => false, // IPv4 network cannot match IPv6 address and vice versa
        }
    }
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
            mode: IpFilterMode::AllowList,
            allow_cidrs: Vec::new(),
            deny_cidrs: Vec::new(),
            client_ip_source: ClientIpSource::XForwardedFor,
        }
    }
}

/// Production implementation of the IP & CIDR Subnet Filter Pre-Route plugin.
#[derive(Debug, Clone)]
pub struct IpFilterPlugin {
    pub id: String,
    pub mode: IpFilterMode,
    pub allow_cidrs: Vec<IpNetwork>,
    pub deny_cidrs: Vec<IpNetwork>,
    pub client_ip_source: ClientIpSource,
}

impl IpFilterPlugin {
    /// Constructs a new IP filter plugin with explicit lists.
    pub fn new(
        id: impl Into<String>,
        mode: IpFilterMode,
        allow_cidrs: impl IntoIterator<Item = IpNetwork>,
        deny_cidrs: impl IntoIterator<Item = IpNetwork>,
    ) -> Self {
        Self {
            id: id.into(),
            mode,
            allow_cidrs: allow_cidrs.into_iter().collect(),
            deny_cidrs: deny_cidrs.into_iter().collect(),
            client_ip_source: ClientIpSource::XForwardedFor,
        }
    }

    /// Constructs an [`IpFilterPlugin`] from a typed [`IpFilterConfig`].
    pub fn with_config(id: impl Into<String>, config: IpFilterConfig) -> Self {
        Self {
            id: id.into(),
            mode: config.mode,
            allow_cidrs: config.allow_cidrs,
            deny_cidrs: config.deny_cidrs,
            client_ip_source: config.client_ip_source,
        }
    }

    /// Deserializes and constructs an instance directly from a [`PluginConfig`].
    ///
    /// # Strict 'none' Sentinel Invariant
    /// In JSON configuration, unused lists must explicitly specify `"none"`.
    /// (e.g. `"allow_cidrs": ["10.0.0.0/8"], "deny_cidrs": "none"`).
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let mode_str = config
            .config
            .get("mode")
            .and_then(|v| v.as_str())
            .unwrap_or("allow");

        let mode = match mode_str.to_ascii_lowercase().as_str() {
            "deny" | "denylist" | "blacklist" => IpFilterMode::DenyList,
            _ => IpFilterMode::AllowList,
        };

        let client_ip_source = match config
            .config
            .get("client_ip_source")
            .and_then(|v| v.as_str())
            .unwrap_or("x_forwarded_for")
            .to_ascii_lowercase()
            .as_str()
        {
            "x_real_ip" | "x-real-ip" | "real_ip" => ClientIpSource::XRealIp,
            _ => ClientIpSource::XForwardedFor,
        };

        let mut allow_cidrs = Vec::new();
        let mut deny_cidrs = Vec::new();

        // 1. Parse allow_cidrs (strictly "none" to omit)
        if let Some(allow_val) = config.config.get("allow_cidrs") {
            if let Some(arr) = allow_val.as_array() {
                for item in arr {
                    if let Some(s) = item.as_str() {
                        let net = IpNetwork::from_cidr(s).map_err(|e| {
                            PluginError::Config(format!(
                                "Plugin '{}' invalid allow_cidr: {e}",
                                config.id
                            ))
                        })?;
                        allow_cidrs.push(net);
                    }
                }
            } else if let Some(s) = allow_val.as_str()
                && !s.trim().eq_ignore_ascii_case("none")
            {
                let net = IpNetwork::from_cidr(s).map_err(|e| {
                    PluginError::Config(format!("Plugin '{}' invalid allow_cidr: {e}", config.id))
                })?;
                allow_cidrs.push(net);
            }
        }

        // 2. Parse deny_cidrs (strictly "none" to omit)
        if let Some(deny_val) = config.config.get("deny_cidrs") {
            if let Some(arr) = deny_val.as_array() {
                for item in arr {
                    if let Some(s) = item.as_str() {
                        let net = IpNetwork::from_cidr(s).map_err(|e| {
                            PluginError::Config(format!(
                                "Plugin '{}' invalid deny_cidr: {e}",
                                config.id
                            ))
                        })?;
                        deny_cidrs.push(net);
                    }
                }
            } else if let Some(s) = deny_val.as_str()
                && !s.trim().eq_ignore_ascii_case("none")
            {
                let net = IpNetwork::from_cidr(s).map_err(|e| {
                    PluginError::Config(format!("Plugin '{}' invalid deny_cidr: {e}", config.id))
                })?;
                deny_cidrs.push(net);
            }
        }

        // 3. Backward compatibility: if "ips" array is present and specific lists were not provided
        if allow_cidrs.is_empty()
            && deny_cidrs.is_empty()
            && let Some(ips_arr) = config.config.get("ips").and_then(|v| v.as_array())
        {
            for item in ips_arr {
                if let Some(s) = item.as_str() {
                    let net = IpNetwork::from_cidr(s).map_err(|e| {
                        PluginError::Config(format!("Plugin '{}' invalid ip: {e}", config.id))
                    })?;
                    match mode {
                        IpFilterMode::AllowList => allow_cidrs.push(net),
                        IpFilterMode::DenyList => deny_cidrs.push(net),
                    }
                }
            }
        }

        Ok(Self {
            id: config.id.clone(),
            mode,
            allow_cidrs,
            deny_cidrs,
            client_ip_source,
        })
    }

    /// Extracts the candidate downstream client IP address from request headers.
    #[inline]
    fn extract_client_ip(&self, req: &L7Request) -> Option<IpAddr> {
        match self.client_ip_source {
            ClientIpSource::XForwardedFor => {
                self.extract_xff(req).or_else(|| self.extract_real_ip(req))
            }
            ClientIpSource::XRealIp => self.extract_real_ip(req).or_else(|| self.extract_xff(req)),
        }
    }

    #[inline]
    fn extract_xff(&self, req: &L7Request) -> Option<IpAddr> {
        req.headers
            .get("x-forwarded-for")
            .and_then(|val| val.to_str().ok())
            .and_then(|raw| raw.split(',').next())
            .and_then(|first| first.trim().parse::<IpAddr>().ok())
    }

    #[inline]
    fn extract_real_ip(&self, req: &L7Request) -> Option<IpAddr> {
        req.headers
            .get("x-real-ip")
            .and_then(|val| val.to_str().ok())
            .and_then(|raw| raw.trim().parse::<IpAddr>().ok())
    }
}

impl PreRoutePlugin for IpFilterPlugin {
    #[inline(always)]
    fn id(&self) -> &str {
        &self.id
    }

    /// Evaluates downstream request client IP against configured CIDR rules.
    ///
    /// # Evaluation Invariants & Precedence:
    /// - **AllowList Mode**:
    ///   1. If matched in `deny_cidrs` (exception) -> REJECT 403 Forbidden.
    ///   2. Else if matched in `allow_cidrs` -> PASS (Action::Continue).
    ///   3. Else -> REJECT 403 Forbidden.
    /// - **DenyList Mode**:
    ///   1. If matched in `allow_cidrs` (exception) -> PASS (Action::Continue).
    ///   2. Else if matched in `deny_cidrs` -> REJECT 403 Forbidden.
    ///   3. Else -> PASS (Action::Continue).
    fn on_request(&self, req: &mut L7Request) -> Action {
        let client_ip = match self.extract_client_ip(req) {
            Some(ip) => ip,
            None => {
                // If client IP cannot be extracted from headers:
                // In AllowList mode, fail-safe requires rejection (unknown IP cannot be on whitelist).
                // In DenyList mode, pass through since IP is not matched on blacklist.
                return match self.mode {
                    IpFilterMode::AllowList => Action::Respond(L7Response::from_bytes(
                        StatusCode::FORBIDDEN,
                        "Forbidden: unable to verify client IP address for protected resource",
                    )),
                    IpFilterMode::DenyList => Action::Continue,
                };
            }
        };

        let is_denied = self.deny_cidrs.iter().any(|net| net.contains(client_ip));
        let is_allowed = self.allow_cidrs.iter().any(|net| net.contains(client_ip));

        match self.mode {
            IpFilterMode::AllowList => {
                // Deny exception takes precedence
                if is_denied {
                    Action::Respond(L7Response::from_bytes(
                        StatusCode::FORBIDDEN,
                        "Forbidden: client IP address rejected by edge policy",
                    ))
                } else if is_allowed {
                    Action::Continue
                } else {
                    Action::Respond(L7Response::from_bytes(
                        StatusCode::FORBIDDEN,
                        "Forbidden: client IP address is not permitted by edge policy",
                    ))
                }
            }
            IpFilterMode::DenyList => {
                // Allow exception takes precedence
                if is_allowed {
                    Action::Continue
                } else if is_denied {
                    Action::Respond(L7Response::from_bytes(
                        StatusCode::FORBIDDEN,
                        "Forbidden: client IP address rejected by edge policy",
                    ))
                } else {
                    Action::Continue
                }
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

#[cfg(test)]
mod tests {
    use super::*;
    use http::{HeaderValue, Method, Uri, Version};
    use velda_core::Body;

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
    fn test_ip_network_v4_contains() {
        let net = IpNetwork::from_cidr("192.168.1.0/24").unwrap();
        assert!(net.contains("192.168.1.1".parse().unwrap()));
        assert!(net.contains("192.168.1.254".parse().unwrap()));
        assert!(!net.contains("192.168.2.1".parse().unwrap()));
        assert!(!net.contains("10.0.0.1".parse().unwrap()));

        // Single exact IP
        let single = IpNetwork::from_cidr("10.0.5.99").unwrap();
        assert!(single.contains("10.0.5.99".parse().unwrap()));
        assert!(!single.contains("10.0.5.100".parse().unwrap()));
    }

    #[test]
    fn test_ip_network_v6_contains() {
        let net = IpNetwork::from_cidr("2001:db8::/32").unwrap();
        assert!(net.contains("2001:db8::1".parse().unwrap()));
        assert!(net.contains("2001:db8:ffff:ffff::1".parse().unwrap()));
        assert!(!net.contains("2001:db9::1".parse().unwrap()));

        let single = IpNetwork::from_cidr("::1").unwrap();
        assert!(single.contains("::1".parse().unwrap()));
        assert!(!single.contains("::2".parse().unwrap()));
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

        // Permitted IPs
        let mut req1 = make_request_with_ip("10.1.2.3");
        assert!(matches!(plugin.on_request(&mut req1), Action::Continue));

        let mut req2 = make_request_with_ip("192.168.1.50");
        assert!(matches!(plugin.on_request(&mut req2), Action::Continue));

        // Rejected IP
        let mut req3 = make_request_with_ip("203.0.113.1");
        assert!(matches!(plugin.on_request(&mut req3), Action::Respond(_)));
    }

    #[test]
    fn test_deny_list_mode() {
        let plugin = IpFilterPlugin::new(
            "ip-deny",
            IpFilterMode::DenyList,
            vec![],
            vec![IpNetwork::from_cidr("203.0.113.0/24").unwrap()],
        );

        // Denied IP
        let mut req1 = make_request_with_ip("203.0.113.5");
        assert!(matches!(plugin.on_request(&mut req1), Action::Respond(_)));

        // Permitted IP
        let mut req2 = make_request_with_ip("198.51.100.1");
        assert!(matches!(plugin.on_request(&mut req2), Action::Continue));
    }

    #[test]
    fn test_hybrid_exceptions_in_allow_mode() {
        // Allow entire 10.0.0.0/8 EXCEPT 10.0.5.99
        let plugin = IpFilterPlugin::new(
            "ip-hybrid",
            IpFilterMode::AllowList,
            vec![IpNetwork::from_cidr("10.0.0.0/8").unwrap()],
            vec![IpNetwork::from_cidr("10.0.5.99").unwrap()],
        );

        let mut req_normal = make_request_with_ip("10.0.5.1");
        assert!(matches!(
            plugin.on_request(&mut req_normal),
            Action::Continue
        ));

        let mut req_exception = make_request_with_ip("10.0.5.99");
        assert!(matches!(
            plugin.on_request(&mut req_exception),
            Action::Respond(_)
        ));
    }

    #[test]
    fn test_from_config_with_none_sentinel() {
        let json = serde_json::json!({
            "mode": "allow",
            "allow_cidrs": [
                "10.0.0.0/8",
                "192.168.1.0/24"
            ],
            "deny_cidrs": "none",
            "client_ip_source": "x_forwarded_for"
        });

        let config = PluginConfig {
            id: "ip-none-test".to_string(),
            phase: "pre_route".to_string(),
            priority: Some(100),
            config: json,
        };

        let plugin = IpFilterPlugin::from_config(&config).unwrap();
        assert_eq!(plugin.mode, IpFilterMode::AllowList);
        assert_eq!(plugin.allow_cidrs.len(), 2);
        assert!(plugin.deny_cidrs.is_empty());

        let mut req = make_request_with_ip("10.20.30.40");
        assert!(matches!(plugin.on_request(&mut req), Action::Continue));
    }

    #[test]
    fn test_multi_hop_x_forwarded_for() {
        let plugin = IpFilterPlugin::new(
            "ip-xff",
            IpFilterMode::AllowList,
            vec![IpNetwork::from_cidr("203.0.113.195").unwrap()],
            vec![],
        );

        // Multi-hop XFF: client, proxy1, proxy2
        let mut req = make_request_with_ip("203.0.113.195, 70.41.3.18, 150.172.238.178");
        assert!(matches!(plugin.on_request(&mut req), Action::Continue));
    }
}
