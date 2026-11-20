//! Modulo-based IP Hash and Generic Request Attribute Hash load balancing.

use crate::LoadBalancer;
use crate::context::SelectionContext;
use velda_core::Endpoint;

const FNV_OFFSET: u64 = 0xcbf29ce484222325;
const FNV_PRIME: u64 = 0x100000001b3;

/// 64-bit register word-accelerated FNV-1a mixer (processes 8 bytes per multiplication).
#[inline]
fn hash_bytes_64(data: &[u8]) -> u64 {
    let mut hash: u64 = FNV_OFFSET;
    let len = data.len();
    let aligned = len & !7;
    let ptr = data.as_ptr();
    let mut i = 0;

    while i < aligned {
        let word = unsafe { (ptr.add(i) as *const u64).read_unaligned() }.to_le();
        hash ^= word;
        hash = hash.wrapping_mul(FNV_PRIME).rotate_left(31);
        i += 8;
    }

    while i < len {
        hash ^= data[i] as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
        i += 1;
    }
    hash ^ (hash >> 32)
}

/// High-speed 64-bit hash function with zero external dependencies.
///
/// Inlined direct execution eliminating indirect function pointer overhead.
#[inline(always)]
pub fn fnv1a_hash(data: &[u8]) -> u64 {
    hash_bytes_64(data)
}

/// Hashes the client's IP address to map them consistently to a backend.
#[derive(Debug, Default)]
pub struct IpHash;

impl IpHash {
    /// Creates a new IP Hash balancer.
    pub const fn new() -> Self {
        Self
    }

    /// Computes a 64-bit hash from an RFC 4291 `u128` IP address directly in integer registers.
    #[inline]
    pub fn hash_ip_u128(ip: u128) -> u64 {
        let hi = (ip >> 64) as u64;
        let lo = ip as u64;
        let h = (FNV_OFFSET ^ hi).wrapping_mul(FNV_PRIME) ^ lo;
        let h = h.wrapping_mul(FNV_PRIME);
        h ^ (h >> 32)
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
            Self::hash_ip_u128(client_ip)
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
        let endpoints = vec![
            Endpoint::from_v4([10, 0, 0, 1], 8080, 1),
            Endpoint::from_v4([10, 0, 0, 2], 8080, 1),
            Endpoint::from_v4([10, 0, 0, 3], 8080, 1),
        ];

        let balancer = IpHash::new();
        let client_ip = velda_core::lut::ip::encode_ipv4_u128(0xC0A8_0164);
        let ctx = SelectionContext::with_client_ip(client_ip);

        let initial_choice = *balancer.select(&endpoints, &ctx).unwrap();

        for _ in 0..100 {
            let choice = *balancer.select(&endpoints, &ctx).unwrap();
            assert_eq!(choice, initial_choice);
        }
    }
}
