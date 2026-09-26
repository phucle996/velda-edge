//! Consistent Hash Ring (Ketama-style) load balancing.
//!
//! Maps each backend to multiple virtual nodes along a 64-bit integer ring.
//! Replicas per backend scale proportionally with endpoint weight (1..=100).
//! Lookup runs in $O(log n ) time via binary search with zero heap allocation.
//! Uses lock-free atomic pointer swaps (`ArcSwap`) to eliminate read contention.

use std::sync::{Arc, Mutex};

use arc_swap::ArcSwap;

use super::hash::fnv1a_hash;
use crate::LoadBalancer;
use crate::context::SelectionContext;
use velda_core::Endpoint;

const VNODES_PER_WEIGHT_UNIT: u32 = 4; // Weight 1 -> 4 vnodes, Weight 100 -> 400 vnodes

#[derive(Clone, Copy, Debug)]
struct Vnode {
    hash: u64,
    endpoint_idx: usize,
}

#[derive(Default)]
struct RingState {
    ring: Vec<Vnode>,
    fingerprint: u64,
}

/// Consistent Hash Ring balancer.
pub struct RingHash {
    state: ArcSwap<RingState>,
    rebuild_lock: Mutex<()>,
}

impl Default for RingHash {
    fn default() -> Self {
        Self::new()
    }
}

impl RingHash {
    /// Creates a new empty Ring Hash balancer.
    pub fn new() -> Self {
        Self {
            state: ArcSwap::from_pointee(RingState::default()),
            rebuild_lock: Mutex::new(()),
        }
    }

    fn compute_fingerprint(endpoints: &[Endpoint]) -> u64 {
        let mut fp = 0xcbf29ce484222325u64;
        for ep in endpoints {
            let addr_hash = match ep.address {
                std::net::SocketAddr::V4(v4) => fnv1a_hash(&v4.ip().octets()) ^ (v4.port() as u64),
                std::net::SocketAddr::V6(v6) => fnv1a_hash(&v6.ip().octets()) ^ (v6.port() as u64),
            };
            fp ^= addr_hash ^ (ep.weight as u64);
            fp = fp.wrapping_mul(0x100000001b3);
        }
        fp
    }

    fn rebuild_ring(endpoints: &[Endpoint]) -> Vec<Vnode> {
        let total_vnodes: usize = endpoints
            .iter()
            .map(|ep| (ep.weight.clamp(1, 100) * VNODES_PER_WEIGHT_UNIT) as usize)
            .sum();
        let mut vnodes = Vec::with_capacity(total_vnodes);

        for (idx, ep) in endpoints.iter().enumerate() {
            let num_vnodes = ep.weight.clamp(1, 100) * VNODES_PER_WEIGHT_UNIT;
            let (ip_bytes, ip_len) = match ep.address {
                std::net::SocketAddr::V4(v4) => {
                    let mut b = [0u8; 24];
                    b[..4].copy_from_slice(&v4.ip().octets());
                    (b, 4)
                }
                std::net::SocketAddr::V6(v6) => {
                    let mut b = [0u8; 24];
                    b[..16].copy_from_slice(&v6.ip().octets());
                    (b, 16)
                }
            };

            for v in 0..num_vnodes {
                let mut seed = ip_bytes;
                seed[ip_len..ip_len + 4].copy_from_slice(&v.to_be_bytes());
                let h = fnv1a_hash(&seed[..ip_len + 4]);
                vnodes.push(Vnode {
                    hash: h,
                    endpoint_idx: idx,
                });
            }
        }

        vnodes.sort_by_key(|v| v.hash);
        vnodes
    }
}

impl LoadBalancer for RingHash {
    fn select_index(&self, endpoints: &[Endpoint], ctx: &SelectionContext<'_>) -> Option<usize> {
        if endpoints.is_empty() {
            return None;
        }
        if endpoints.len() == 1 {
            return Some(0);
        }

        // OPTIMIZATION: Ultra-fast O(log V) path. If caller provides upstream topology_version,
        // we compare it directly against the compiled state, completely bypassing O(N) FNV1a hashing.
        if let Some(version) = ctx.topology_version {
            let current = self.state.load();
            if current.fingerprint == version && !current.ring.is_empty() {
                let key = ctx.hash_key.unwrap_or(0);
                let idx = match current.ring.binary_search_by_key(&key, |v| v.hash) {
                    Ok(i) => i,
                    Err(i) => {
                        if i >= current.ring.len() {
                            0
                        } else {
                            i
                        }
                    }
                };
                let ep_idx = current.ring[idx].endpoint_idx;
                return if ep_idx < endpoints.len() {
                    Some(ep_idx)
                } else {
                    Some(0)
                };
            }
        }

        // Fallback: compute FNV1a fingerprint over endpoints slice
        let fp = ctx
            .topology_version
            .unwrap_or_else(|| Self::compute_fingerprint(endpoints));

        // Fast path: lock-free binary search on ring
        {
            let current = self.state.load();
            if current.fingerprint == fp && !current.ring.is_empty() {
                let key = ctx.hash_key.unwrap_or(0);
                let idx = match current.ring.binary_search_by_key(&key, |v| v.hash) {
                    Ok(i) => i,
                    Err(i) => {
                        if i >= current.ring.len() {
                            0
                        } else {
                            i
                        }
                    }
                };
                let ep_idx = current.ring[idx].endpoint_idx;
                return if ep_idx < endpoints.len() {
                    Some(ep_idx)
                } else {
                    Some(0)
                };
            }
        }

        // BLOCKER FIX: Prevent poisoned mutex cascade panic on ring rebuild.
        // If a thread panics during ring construction, recover the lock guard instead of taking down the upstream.
        let _guard = self
            .rebuild_lock
            .lock()
            .unwrap_or_else(|poison_err| poison_err.into_inner());

        let current = self.state.load();
        if current.fingerprint == fp && !current.ring.is_empty() {
            let key = ctx.hash_key.unwrap_or(0);
            let idx = match current.ring.binary_search_by_key(&key, |v| v.hash) {
                Ok(i) => i,
                Err(i) => {
                    if i >= current.ring.len() {
                        0
                    } else {
                        i
                    }
                }
            };
            let ep_idx = current.ring[idx].endpoint_idx;
            return if ep_idx < endpoints.len() {
                Some(ep_idx)
            } else {
                Some(0)
            };
        }

        let new_ring = Self::rebuild_ring(endpoints);
        let new_state = RingState {
            ring: new_ring,
            fingerprint: fp,
        };

        let key = ctx.hash_key.unwrap_or(0);
        let idx = match new_state.ring.binary_search_by_key(&key, |v| v.hash) {
            Ok(i) => i,
            Err(i) => {
                if i >= new_state.ring.len() {
                    0
                } else {
                    i
                }
            }
        };
        let ep_idx = new_state.ring[idx].endpoint_idx;

        self.state.store(Arc::new(new_state));

        if ep_idx < endpoints.len() {
            Some(ep_idx)
        } else {
            Some(0)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    #[test]
    fn test_ring_hash_consistency() {
        let ep1: SocketAddr = "10.0.0.1:8080".parse().unwrap();
        let ep2: SocketAddr = "10.0.0.2:8080".parse().unwrap();
        let ep3: SocketAddr = "10.0.0.3:8080".parse().unwrap();

        let endpoints = vec![
            Endpoint::new("e1", ep1, 1),
            Endpoint::new("e2", ep2, 2),
            Endpoint::new("e3", ep3, 1),
        ];

        let balancer = RingHash::new();
        let key = 987654321u64;
        let ctx = SelectionContext::with_hash(key);

        let initial_choice = balancer.select(&endpoints, &ctx).unwrap().address;

        for _ in 0..1_000 {
            let choice = balancer.select(&endpoints, &ctx).unwrap().address;
            assert_eq!(choice, initial_choice);
        }
    }
}
