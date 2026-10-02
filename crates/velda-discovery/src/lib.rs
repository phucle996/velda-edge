//! # velda-discovery
//!
//! Stage 1 — Backend Topology Discovery for the Velda Edge Data Plane.
//!
//! Answers the fundamental question: **"Where are the backends located?"**
//!
//! Strict Invariants:
//! - **Topology Only**: Discovery discovers IP addresses and ports; it has no awareness of health, draining, or routing.
//! - **Zero DNS on Request Hot Path**: Lookups occur exclusively in background tasks; request serving reads lock-free in-memory `EndpointSet` snapshots.
//! - **Unified Cache as Single Source of Truth**: All resolved addresses (from static `/etc/hosts` or upstream DNS queries) are cached in `DnsCache`.
//! - **Zero Hardcoded Nameservers**: Upstream nameservers are resolved dynamically from `/etc/resolv.conf` or user configuration manifests.
//! - **Resilient LKG Fallback**: Network or nameserver failures preserve the Last-Known-Good endpoint snapshot.
//! - **Negative Caching**: Protects against query storms when names do not exist (NXDOMAIN).

pub mod dns;
pub mod endpoint;
pub mod error;
pub mod service;

pub use dns::{
    CacheLookup, DEFAULT_DNS_PACKET_BUFFER_SIZE, DnsCache, DnsResolverConfig, DnsResolverProvider,
    DnsServer, DnsServerProvider, DnsServerTarget, DnsTransport, HostsFileSource,
    ResolvConfServerProvider, StaticServerProvider, SystemDnsTransport, UdpDnsTransport,
    build_query_packet, parse_response_packet, skip_name,
};

pub use endpoint::{Endpoint, EndpointId, EndpointSet};
pub use error::{DiscoveryError, Result};
pub use service::{Discovery, DiscoveryMode};
