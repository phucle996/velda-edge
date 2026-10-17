//! IP Whitelist / Blacklist Pre-Route Plugin Skeleton (`phase: "pre_route"`).
//!
//! Inspects client IP address or proxy headers (`X-Forwarded-For`) before routing,
//! rejecting disallowed clients fast with 403 Forbidden.

use std::collections::HashSet;
use std::net::IpAddr;

use http::StatusCode;
use velda_core::{Action, L7Request, L7Response};

use super::PreRoutePlugin;

/// Operational filtering mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IpFilterMode {
    /// Only permitted IPs can access (Whitelist).
    AllowList,
    /// Specified IPs are blocked (Blacklist).
    DenyList,
}

/// Production skeleton for IP-based network filtering.
#[derive(Debug, Clone)]
pub struct IpFilterPlugin {
    pub id: String,
    pub mode: IpFilterMode,
    pub ip_set: HashSet<IpAddr>,
}

impl IpFilterPlugin {
    /// Constructs a new IP filter plugin.
    pub fn new(
        id: impl Into<String>,
        mode: IpFilterMode,
        ips: impl IntoIterator<Item = IpAddr>,
    ) -> Self {
        Self {
            id: id.into(),
            mode,
            ip_set: ips.into_iter().collect(),
        }
    }

    /// Deserializes and constructs an instance directly from a [`PluginConfig`].
    ///
    /// # Module Ownership Invariant
    /// Parsing logic, string-to-IP conversion, and whitelist/blacklist mode resolution
    /// belong strictly to the plugin itself.
    pub fn from_config(
        config: &velda_sync::post_sync::plugin::PluginConfig,
    ) -> Result<Self, crate::error::PluginError> {
        let mode_str = config
            .config
            .get("mode")
            .and_then(|v| v.as_str())
            .unwrap_or("allow");

        let mode = match mode_str.to_ascii_lowercase().as_str() {
            "deny" | "denylist" | "blacklist" => IpFilterMode::DenyList,
            _ => IpFilterMode::AllowList,
        };

        let mut ip_set = HashSet::new();
        if let Some(ips_arr) = config.config.get("ips").and_then(|v| v.as_array()) {
            for item in ips_arr {
                if let Some(ip) = item.as_str().and_then(|s| s.trim().parse::<IpAddr>().ok()) {
                    ip_set.insert(ip);
                }
            }
        }

        Ok(Self::new(&config.id, mode, ip_set))
    }
}

impl PreRoutePlugin for IpFilterPlugin {
    #[inline]
    fn id(&self) -> &str {
        &self.id
    }

    fn on_request(&self, req: &mut L7Request) -> Action {
        // Extract client IP candidate from X-Forwarded-For or X-Real-IP header
        let client_ip = req
            .headers
            .get("x-forwarded-for")
            .or_else(|| req.headers.get("x-real-ip"))
            .and_then(|val| val.to_str().ok())
            .and_then(|raw| raw.split(',').next())
            .and_then(|first| first.trim().parse::<IpAddr>().ok());

        let matched = match client_ip {
            Some(ip) => self.ip_set.contains(&ip),
            None => false,
        };

        let is_allowed = match self.mode {
            IpFilterMode::AllowList => matched,
            IpFilterMode::DenyList => !matched,
        };

        if is_allowed {
            Action::Continue
        } else {
            Action::Respond(L7Response::from_bytes(
                StatusCode::FORBIDDEN,
                "Forbidden: client IP address rejected by edge policy",
            ))
        }
    }

    #[inline]
    fn capability(&self) -> crate::capability::PluginCapability {
        crate::capability::PluginCapability::header_only(crate::capability::AccessLevel::Read)
    }

    #[inline]
    fn supported_protocols(&self) -> crate::capability::SupportedProtocols {
        crate::capability::SupportedProtocols::ALL_L7
    }
}
