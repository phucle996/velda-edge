//! Modulo-based IP Hash and Generic Request Attribute Hash load balancing.

use std::net::SocketAddr;

use crate::LoadBalancer;
use crate::context::SelectionContext;
use velda_core::Endpoint;

/// 64-bit FNV-1a hash function with zero external dependencies.
#[inline]
pub fn fnv1a_hash(data: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf29ce484222325;
    for &byte in data {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

/// Hashes the client's IP address to map them consistently to a backend.
#[derive(Debug, Default)]
pub struct IpHash;

impl IpHash {
    /// Creates a new IP Hash balancer.
    pub const fn new() -> Self {
        Self
    }

    /// Computes a 64-bit hash from a [`SocketAddr`].
    pub fn hash_socket_addr(addr: SocketAddr) -> u64 {
        match addr {
            SocketAddr::V4(v4) => fnv1a_hash(&v4.ip().octets()),
            SocketAddr::V6(v6) => fnv1a_hash(&v6.ip().octets()),
        }
    }
}

impl LoadBalancer for IpHash {
    fn select_index(&self, endpoints: &[Endpoint], ctx: &SelectionContext<'_>) -> Option<usize> {
        let n = endpoints.len();
        if n == 0 {
            return None;
        }
        if n == 1 {
            return Some(0);
        }

        let hash_val = if let Some(key) = ctx.hash_key {
            key
        } else if let Some(client_ip) = ctx.client_ip {
            Self::hash_socket_addr(client_ip)
        } else if let Some(bytes) = ctx.hash_bytes {
            fnv1a_hash(bytes)
        } else {
            0
        };

        if n & (n - 1) == 0 {
            Some((hash_val as usize) & (n - 1))
        } else {
            Some(super::random::fast_reduce(hash_val, n))
        }
    }
}

/// Hashes a generic request attribute (URI, cookie, header value, etc.).
#[derive(Debug, Default)]
pub struct GenericHash;

impl GenericHash {
    /// Creates a new generic hash balancer.
    pub const fn new() -> Self {
        Self
    }
}

impl LoadBalancer for GenericHash {
    fn select_index(&self, endpoints: &[Endpoint], ctx: &SelectionContext<'_>) -> Option<usize> {
        let n = endpoints.len();
        if n == 0 {
            return None;
        }
        if n == 1 {
            return Some(0);
        }

        let hash_val = if let Some(key) = ctx.hash_key {
            key
        } else if let Some(bytes) = ctx.hash_bytes {
            fnv1a_hash(bytes)
        } else {
            0
        };

        if n & (n - 1) == 0 {
            Some((hash_val as usize) & (n - 1))
        } else {
            Some(super::random::fast_reduce(hash_val, n))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ip_hash_consistency() {
        let ep1: SocketAddr = "10.0.0.1:8080".parse().unwrap();
        let ep2: SocketAddr = "10.0.0.2:8080".parse().unwrap();
        let ep3: SocketAddr = "10.0.0.3:8080".parse().unwrap();

        let endpoints = vec![
            Endpoint::new("e1", ep1, 1),
            Endpoint::new("e2", ep2, 1),
            Endpoint::new("e3", ep3, 1),
        ];

        let balancer = IpHash::new();
        let client_ip: SocketAddr = "192.168.1.100:44321".parse().unwrap();
        let ctx = SelectionContext::with_client_ip(client_ip);

        let initial_choice = balancer.select(&endpoints, &ctx).unwrap().address;

        for _ in 0..100 {
            let choice = balancer.select(&endpoints, &ctx).unwrap().address;
            assert_eq!(choice, initial_choice);
        }
    }
}
