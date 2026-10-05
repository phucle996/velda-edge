//! # velda-upstream
//!
//! Protocol-agnostic upstream backend lifecycle, discovery, health tracking,
//! and load balancing for the Velda Edge Data Plane.
//!
//! Core invariant:
//! **Router decides $\rightarrow$ Upstream resolves $\rightarrow$ Protocol connects & executes.**

pub mod connection;
pub mod error;
pub mod health;
pub mod upstream;

// Re-exports from velda-core
pub use velda_core::{Endpoint, EndpointId};

// Re-exports from velda-discovery
pub use velda_discovery::{Discovery, DiscoveryMode};
pub use velda_discovery::{
    DnsResolverConfig, DnsResolverProvider, DnsServerProvider, DnsTransport, EndpointSet,
    HostsFileSource, ResolvConfServerProvider, SystemDnsTransport, UdpDnsTransport,
};

// Re-exports from velda-lb
pub use velda_lb::{
    EndpointMetrics, GenericHash, IpHash, LeastConnections, LeastRequests, LoadBalancer, Maglev,
    PeakEwma, PowerOfTwoChoices, Random, RingHash, RoundRobin, SelectionContext,
    WeightedLeastRequests, WeightedRandom, WeightedRoundRobin, fnv1a_hash,
};

// Re-exports for clean, ergonomic usage within upstream
pub use connection::SocketAccelerationPath;
pub use error::{Result, UpstreamError};
pub use health::{ActiveHealthConfig, HealthConfig, HealthTracker, PassiveHealthConfig};
pub use upstream::{Upstream, UpstreamTimeouts};
