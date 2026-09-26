//! Request context and runtime state passed to load balancer selections.

use std::collections::HashMap;
use std::net::SocketAddr;

use crate::metrics::EndpointMetrics;

/// Contextual attributes passed into [`crate::LoadBalancer::select`] during request serving.
///
/// Designed for zero heap allocations on the hot path.
#[derive(Clone, Copy, Default)]
pub struct SelectionContext<'a> {
    /// Deterministic 64-bit hash key extracted from request attributes
    /// (e.g. Client IP hash, Header value hash, Cookie hash, Request Path hash).
    pub hash_key: Option<u64>,

    /// Client peer socket address for direct IP-based hashing.
    pub client_ip: Option<SocketAddr>,

    /// Raw byte slice for direct on-the-fly hashing without heap allocation.
    pub hash_bytes: Option<&'a [u8]>,

    /// Optional reference to the dynamic metrics table for state-aware balancers.
    pub metrics_map: Option<&'a HashMap<SocketAddr, EndpointMetrics>>,

    // OPTIMIZATION: Contiguous metrics slice eliminates SipHash lookup overhead in HashMap.
    pub metrics_slice: Option<&'a [EndpointMetrics]>,

    // OPTIMIZATION: Upstream topology version check eliminates O(N) FNV1a hashing on hot path.
    pub topology_version: Option<u64>,
}

impl<'a> SelectionContext<'a> {
    /// An empty context with no hash key or metrics.
    pub const NONE: Self = Self {
        hash_key: None,
        client_ip: None,
        hash_bytes: None,
        metrics_map: None,
        metrics_slice: None,
        topology_version: None,
    };

    /// Creates a context with a precomputed 64-bit hash key.
    #[inline]
    pub const fn with_hash(hash_key: u64) -> Self {
        Self {
            hash_key: Some(hash_key),
            client_ip: None,
            hash_bytes: None,
            metrics_map: None,
            metrics_slice: None,
            topology_version: None,
        }
    }

    /// Creates a context with the client IP address.
    #[inline]
    pub const fn with_client_ip(client_ip: SocketAddr) -> Self {
        Self {
            hash_key: None,
            client_ip: Some(client_ip),
            hash_bytes: None,
            metrics_map: None,
            metrics_slice: None,
            topology_version: None,
        }
    }

    /// Creates a context with raw byte slice.
    #[inline]
    pub const fn with_hash_bytes(hash_bytes: &'a [u8]) -> Self {
        Self {
            hash_key: None,
            client_ip: None,
            hash_bytes: Some(hash_bytes),
            metrics_map: None,
            metrics_slice: None,
            topology_version: None,
        }
    }

    /// Attaches the dynamic metrics map to this context.
    #[inline]
    pub fn with_metrics(mut self, metrics_map: &'a HashMap<SocketAddr, EndpointMetrics>) -> Self {
        self.metrics_map = Some(metrics_map);
        self
    }

    // OPTIMIZATION: Attaches contiguous metrics slice for direct O(1) array index access without hashing.
    #[inline]
    pub fn with_metrics_slice(mut self, metrics_slice: &'a [EndpointMetrics]) -> Self {
        self.metrics_slice = Some(metrics_slice);
        self
    }

    // OPTIMIZATION: Attaches upstream revision version for instant O(1) version check on Maglev/RingHash.
    #[inline]
    pub const fn with_topology_version(mut self, version: u64) -> Self {
        self.topology_version = Some(version);
        self
    }
}
