//! IP Access Control Plugin Module (`ip_filter`).

pub mod network;
pub mod plugin;

pub use network::{ClientIpSource, IpNetwork};
pub use plugin::{IpFilterConfig, IpFilterMode, IpFilterPlugin};
