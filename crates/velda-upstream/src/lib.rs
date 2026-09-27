//! # velda-upstream
//!
//! Protocol-agnostic upstream backend lifecycle, discovery, health tracking,
//! load balancing, and connection lease acquisition for the Velda Edge Data Plane.
//!
//! Core invariant:
//! **Router decides $\rightarrow$ Upstream resolves $\rightarrow$ Pool reuses $\rightarrow$ Execution executes.**

pub mod connection;
pub mod endpoint;
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

// Re-exports from velda-connection-pool
pub use velda_connection_pool::{ConnectionKey, PoolManager, PoolStats, PoolableResource};

// Re-exports for clean, ergonomic usage within upstream
pub use connection::{BackendConnection, Connector, RealTcpConnection, TcpConnector};
pub use endpoint::EndpointState;
pub use error::{Result, UpstreamError};
pub use health::{ActiveHealthConfig, HealthConfig, HealthTracker, PassiveHealthConfig};
pub use upstream::{AcquireTarget, BackendLease, Upstream, UpstreamPoolManager, UpstreamTimeouts};

// Test utilities
#[cfg(any(test, feature = "test-utils"))]
pub use connection::mock::{MockConnection, MockConnector};
