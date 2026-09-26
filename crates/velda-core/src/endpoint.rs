//! Shared backend destination endpoint definitions for Velda Edge.
//!
//! # Monorepo Invariant:
//! `Endpoint` is strictly reserved across the entire codebase to denote
//! a physical backend destination target (IP address + port + weight).
//! Subsystems MUST NOT use `Endpoint` for listeners, routes, or client sockets.

use std::fmt;
use std::net::SocketAddr;

/// Strongly typed identifier for an individual backend endpoint.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EndpointId(pub String);

impl From<&str> for EndpointId {
    #[inline]
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

impl From<String> for EndpointId {
    #[inline]
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl fmt::Display for EndpointId {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Represents a concrete backend destination target.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Endpoint {
    /// Unique identifier within the discovery / upstream scope.
    pub id: EndpointId,
    /// Destination IP socket address and port.
    pub address: SocketAddr,
    /// Load balancing weight (minimum 1, default 1).
    pub weight: u32,
}

impl Endpoint {
    /// Creates a new backend destination endpoint.
    pub fn new(id: impl Into<EndpointId>, address: SocketAddr, weight: u32) -> Self {
        Self {
            id: id.into(),
            address,
            weight: weight.max(1),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_endpoint_creation() {
        let addr: SocketAddr = "10.0.0.1:8080".parse().unwrap();
        let ep = Endpoint::new("ep-1", addr, 10);
        assert_eq!(ep.id.to_string(), "ep-1");
        assert_eq!(ep.address, addr);
        assert_eq!(ep.weight, 10);
    }

    #[test]
    fn test_endpoint_min_weight() {
        let addr: SocketAddr = "10.0.0.1:8080".parse().unwrap();
        let ep = Endpoint::new("ep-1", addr, 0);
        assert_eq!(ep.weight, 1);
    }
}
