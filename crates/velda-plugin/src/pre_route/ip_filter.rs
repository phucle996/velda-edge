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
}
