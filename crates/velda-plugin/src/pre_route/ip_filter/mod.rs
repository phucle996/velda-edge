//! IP Access Control Plugin Module (`ip_filter`).

pub mod network;
pub mod plugin;

pub use network::{
    ClientIpSource, ExtractedIps, IpExtractionError, IpNetwork, MAX_XFF_HEADERS, MAX_XFF_HOPS,
    ParsedIp,
};
pub use plugin::{IpFilterConfig, IpFilterMode, IpFilterPlugin};
