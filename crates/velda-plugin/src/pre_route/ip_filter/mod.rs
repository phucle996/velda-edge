//! IP Whitelist / Blacklist Pre-Route Plugin Module (`phase: "pre_route"`).
//!
//! Provides zero-allocation IP address and CIDR subnet access control for edge traffic.

pub mod plugin;

pub use plugin::{ClientIpSource, IpFilterConfig, IpFilterMode, IpFilterPlugin, IpNetwork};
