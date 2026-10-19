//! IP / CIDR Subnet Access Filter Plugin (`ip_filter`).
//!
//! Evaluated during `pre_route` phase prior to routing trie lookups and body streaming.
//! Enforces zero-allocation bitmask CIDR matching on IPv4/IPv6 addresses with allow/deny modes
//! and exception precedence.
//!
//! Full schema specification, allowed values, and configuration examples:
//! See [`README.md`](./README.md).

use std::net::IpAddr;

use http::StatusCode;
use velda_core::{Action, L7Request, L7Response};
use velda_sync::post_sync::plugin::PluginConfig;

use super::network::{ClientIpSource, IpNetwork};
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
        Self {
            id: id.into(),
            mode: config.mode,
            allow_cidrs: config.allow_cidrs,
            deny_cidrs: config.deny_cidrs,
            client_ip_source: config.client_ip_source,
        }
    }

    /// Deserializes and constructs an instance directly from a [`PluginConfig`].
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let mode = match config
            .config
            .get("mode")
            .and_then(|v| v.as_str())
            .unwrap_or("deny")
        {
            "allow" | "whitelist" => IpFilterMode::AllowList,
            _ => IpFilterMode::DenyList,
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
        self.client_ip_source.extract_client_ip(req)
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

        let mut req1 = make_request_with_ip("203.0.113.5");
        assert!(matches!(plugin.on_request(&mut req1), Action::Respond(_)));

        let mut req2 = make_request_with_ip("8.8.8.8");
        assert!(matches!(plugin.on_request(&mut req2), Action::Continue));
    }
}
