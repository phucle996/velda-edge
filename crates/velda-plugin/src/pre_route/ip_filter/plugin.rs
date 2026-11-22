//! IP / CIDR Subnet Access Filter Plugin (`ip_filter`).
//!
//! Evaluated during `pre_route` phase prior to router table lookups and body streaming.
//! Enforces zero-allocation bitmask CIDR matching on IPv4/IPv6 addresses with allow/deny modes
//! and exception precedence.
//!
//! ```text
//!  PreRoutePlugin Flow (ip_filter::on_request):
//!
//!             Inbound L7Request
//!                     │
//!                     ▼
//!      +──────────────────────────────+
//!      | Check client_ip_source       |
//!      | - XForwardedFor vs XRealIp   |
//!      +──────────────────────────────+
//!                     │
//!                     ▼
//!      +──────────────────────────────+
//!      | stream::evaluate_stream      |
//!      |   (Streaming on Registers)   |
//!      +──────────────────────────────+
//!          │                      │
//!   (AllowList Mode)       (DenyList Mode)
//!          │                      │
//!          ▼                      ▼
//!   Any hop in deny?       Hop in allow?
//!     ├─► Yes: REJECT 403    ├─► Yes: Excused (skip deny)
//!     └─► No:                └─► No:
//!   Every hop in allow?         Hop in deny?
//!     ├─► Yes: CONTINUE           ├─► Yes: REJECT 403
//!     └─► No:  REJECT 403         └─► No:  CONTINUE
//! ```
//!
//! Full schema specification, allowed values, and configuration examples:
//! See [`README.md`](./README.md).

use crate::capability::{AccessLevel, PluginCapability, SupportedProtocols};
use crate::config::PluginConfig;
use crate::error::PluginError;
use crate::pre_route::PreRoutePlugin;
use velda_core::{Action, L7Request};

use super::config::{ClientIpSource, IpFilterConfig, IpFilterMode, IpNetwork};
use super::engine::IpFilterEngine;
pub use super::engine::{match_cidr_v4, match_cidr_v4_64};
use super::stream::evaluate_stream;

/// Pre-Route IP Filter Plugin instance.
#[derive(Debug, Clone)]
pub struct IpFilterPlugin {
    pub id: String,
    pub mode: IpFilterMode,
    pub allow_cidrs: Vec<IpNetwork>,
    pub deny_cidrs: Vec<IpNetwork>,
    pub client_ip_source: ClientIpSource,
    pub allow_engine: IpFilterEngine,
    pub deny_engine: IpFilterEngine,
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
        let allow_engine = IpFilterEngine::from_cidrs(&config.allow_cidrs);
        let deny_engine = IpFilterEngine::from_cidrs(&config.deny_cidrs);

        Self {
            id: id.into(),
            mode: config.mode,
            allow_cidrs: config.allow_cidrs,
            deny_cidrs: config.deny_cidrs,
            client_ip_source: config.client_ip_source,
            allow_engine,
            deny_engine,
        }
    }

    /// Deserializes and constructs an instance directly from a [`PluginConfig`].
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let cfg = super::validation::validate_ip_filter_config(&config.id, &config.config)
            .map_err(|e| {
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

    #[inline(always)]
    pub fn matches_allow(&self, ip: super::stream::ParsedIp) -> bool {
        self.allow_engine.matches_ip(ip)
    }

    #[inline(always)]
    pub fn matches_deny(&self, ip: super::stream::ParsedIp) -> bool {
        self.deny_engine.matches_ip(ip)
    }
}

impl PreRoutePlugin for IpFilterPlugin {
    #[inline(always)]
    fn id(&self) -> &str {
        &self.id
    }

    #[inline(always)]
    fn pbww(&self) -> u32 {
        0
    }

    /// Evaluates downstream request client IP chain against configured CIDR rules on the fly.
    ///
    /// Delegates directly to the streaming evaluator for zero-allocation, register-only matching.
    #[inline(always)]
    fn on_request(&self, req: &mut L7Request) -> Action {
        evaluate_stream(
            req,
            self.client_ip_source,
            self.mode,
            &self.allow_engine,
            &self.deny_engine,
        )
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
            Action::Reject(RejectReason::IpBlocked)
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
            Action::Reject(RejectReason::IpBlocked)
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
            Action::Reject(RejectReason::IpBlocked)
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
            Action::Reject(RejectReason::IpBlocked)
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
            Action::Reject(RejectReason::IpBlocked)
        ));

        // Attacker at denied subnet 10.99.1.5 spoofs X-Forwarded-For: 10.1.2.3
        let mut spoofed_denied_hop = make_request_with_ip("10.1.2.3, 10.99.1.5");
        assert!(matches!(
            plugin.on_request(&mut spoofed_denied_hop),
            Action::Reject(RejectReason::IpBlocked)
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
            Action::Reject(RejectReason::IpBlocked)
        ));

        // Blacklisted attacker (203.0.113.5) behind a benign public IP (8.8.8.8)
        let mut hidden_second_hop = make_request_with_ip("8.8.8.8, 203.0.113.5");
        assert!(matches!(
            plugin.on_request(&mut hidden_second_hop),
            Action::Reject(RejectReason::IpBlocked)
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
            Action::Reject(RejectReason::IpBlocked)
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
            Action::Reject(RejectReason::IpBlocked)
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
            Action::Reject(RejectReason::IpBlocked)
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
                    Action::Reject(RejectReason::IpBlocked)
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
                    Action::Reject(RejectReason::IpBlocked)
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
                    Action::Reject(RejectReason::IpBlocked)
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
            Action::Reject(RejectReason::IpBlocked)
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
                    Action::Reject(RejectReason::IpBlocked)
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
