//! Pure topology endpoint representation and immutable generation sets.
//!
//! Strict Invariant: Discovery represents **WHERE** backends exist (Topology),
//! not whether they are healthy or draining (State). Health and lifecycle state
//! are managed downstream by `velda-upstream`.

use std::collections::HashMap;
use std::net::SocketAddr;

pub use velda_core::{Endpoint, EndpointId};

/// Immutable snapshot of discovered endpoints for a given generation.
#[derive(Debug, Clone)]
pub struct EndpointSet {
    endpoints: Vec<Endpoint>,
    by_addr: HashMap<SocketAddr, usize>,
    generation: u64,
}

impl EndpointSet {
    /// Creates a new endpoint set with a specific generation counter.
    pub fn new(endpoints: Vec<Endpoint>, generation: u64) -> Self {
        let mut by_addr = HashMap::with_capacity(endpoints.len());
        for (idx, ep) in endpoints.iter().enumerate() {
            by_addr.insert(ep.address, idx);
        }
        Self {
            endpoints,
            by_addr,
            generation,
        }
    }

    /// Creates an empty endpoint set at generation 0.
    pub fn empty() -> Self {
        Self {
            endpoints: Vec::new(),
            by_addr: HashMap::new(),
            generation: 0,
        }
    }

    /// Returns a slice of all discovered endpoints.
    #[inline]
    pub fn all_endpoints(&self) -> &[Endpoint] {
        &self.endpoints
    }

    /// Returns an iterator over all discovered socket addresses.
    #[inline]
    pub fn addresses(&self) -> impl Iterator<Item = SocketAddr> + '_ {
        self.endpoints.iter().map(|ep| ep.address)
    }

    /// Returns an iterator over all discovered IP addresses.
    #[inline]
    pub fn ips(&self) -> impl Iterator<Item = std::net::IpAddr> + '_ {
        self.endpoints.iter().map(|ep| ep.address.ip())
    }

    /// Finds an endpoint by its socket address.
    #[inline]
    pub fn find_by_addr(&self, addr: &SocketAddr) -> Option<&Endpoint> {
        self.by_addr.get(addr).map(|&idx| &self.endpoints[idx])
    }

    /// Returns `true` if the given address is present in this set.
    #[inline]
    pub fn contains_addr(&self, addr: &SocketAddr) -> bool {
        self.by_addr.contains_key(addr)
    }

    /// Returns the number of endpoints in this set.
    #[inline]
    pub fn len(&self) -> usize {
        self.endpoints.len()
    }

    /// Returns `true` if there are no endpoints in this set.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.endpoints.is_empty()
    }

    /// Returns the generation counter of this snapshot.
    #[inline]
    pub fn generation(&self) -> u64 {
        self.generation
    }
}
