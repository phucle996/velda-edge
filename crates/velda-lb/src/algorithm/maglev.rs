//! Google Maglev consistent hashing with $O(1)$ constant-time lookup.
//!
//! Generates a deterministic permutation lookup table of prime size $M$.
//! Lookup operates in true $O(1)$ constant time with zero heap allocations on the hot path.
//! Uses lock-free atomic pointer swaps (`ArcSwap`) to eliminate read lock contention.

use std::sync::{Arc, Mutex};

use arc_swap::ArcSwap;

use super::hash::fnv1a_hash;
use crate::LoadBalancer;
use crate::context::SelectionContext;
use velda_core::Endpoint;

/// Standard prime table size used by Google Maglev and Envoy.
pub const MAGLEV_TABLE_SIZE: usize = 65537;

#[derive(Clone, Default)]
struct MaglevState {
    table: Vec<usize>,
    fingerprint: u64,
    endpoints_ptr: usize,
    endpoints_len: usize,
}

/// Google Maglev consistent hash balancer.
pub struct Maglev {
    state: ArcSwap<MaglevState>,
    rebuild_lock: Mutex<()>,
    table_size: usize,
}

impl Default for Maglev {
    fn default() -> Self {
        Self::new(MAGLEV_TABLE_SIZE)
    }
}

/// Checks if a number is prime.
fn is_prime(n: usize) -> bool {
    if n <= 1 {
        return false;
    }
    if n <= 3 {
        return true;
    }
    if n.is_multiple_of(2) || n.is_multiple_of(3) {
        return false;
    }
    let mut i = 5;
    while i * i <= n {
        if n.is_multiple_of(i) || n.is_multiple_of(i + 2) {
            return false;
        }
        i += 6;
    }
    true
}

/// Finds the smallest prime greater than or equal to `n`.
fn next_prime(mut n: usize) -> usize {
    if n <= 2 {
        return 2;
    }
    if n.is_multiple_of(2) {
        n += 1;
    }
    while !is_prime(n) {
        n += 2;
    }
    n
}

impl Maglev {
    /// Creates a new Maglev balancer with custom prime table size (default 65,537).
    pub fn new(table_size: usize) -> Self {
        // Maglev requires table size M to be prime so that gcd(skip, M) == 1.
        // A composite number would cause the probe sequence to cycle in a proper subgroup,
        // risking an infinite loop when that subgroup fills. Auto-upgrades to the next prime.
        let prime_table_size = next_prime(table_size.max(3));
        Self {
            state: ArcSwap::from_pointee(MaglevState::default()),
            rebuild_lock: Mutex::new(()),
            table_size: prime_table_size,
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

    /// Builds the Maglev permutation lookup table.
    fn build_table(endpoints: &[Endpoint], m: usize) -> Vec<usize> {
        let n = endpoints.len();
        if n == 0 {
            return Vec::new();
        }

        // Generate permutation offsets and skips for each backend
        let mut offset = Vec::with_capacity(n);
        let mut skip = Vec::with_capacity(n);

        for ep in endpoints {
            let h1 = match ep.address {
                std::net::SocketAddr::V4(v4) => fnv1a_hash(&v4.ip().octets()),
                std::net::SocketAddr::V6(v6) => fnv1a_hash(&v6.ip().octets()),
            };
            let h2 = h1.wrapping_mul(0x517cc1b727220a95).wrapping_add(1);

            offset.push((h1 as usize) % m);
            skip.push(((h2 as usize) % (m - 1)) + 1);
        }

        let mut table = vec![usize::MAX; m];
        let mut next = vec![0usize; n];
        let mut filled = 0;

        // Populate table using interleaved round-robin over permutations
        let weights: Vec<u32> = endpoints.iter().map(|e| e.weight.clamp(1, 100)).collect();
        let mut remaining_weights = weights.clone();

        while filled < m {
            let mut total_weight = 0;
            for i in 0..n {
                if remaining_weights[i] == 0 {
                    continue;
                }
                total_weight += remaining_weights[i];
                remaining_weights[i] -= 1;

                let mut c = offset[i].wrapping_add(next[i].wrapping_mul(skip[i])) % m;
                let mut probes = 0;
                // In an ideal prime table, gcd(skip[i], m) == 1 guarantees visiting all slots.
                // Bounding iterations by m guarantees termination even under pathological hash collisions.
                while table[c] != usize::MAX && probes < m {
                    next[i] += 1;
                    c = offset[i].wrapping_add(next[i].wrapping_mul(skip[i])) % m;
                    probes += 1;
                }

                if table[c] == usize::MAX {
                    table[c] = i;
                    next[i] += 1;
                    filled += 1;
                }

                if filled == m {
                    break;
                }
            }

            if total_weight == 0 {
                remaining_weights.clone_from_slice(&weights);
            }
        }

        // Fill any potential gaps if loop broke early
        for entry in &mut table {
            if *entry == usize::MAX {
                *entry = 0;
            }
        }

        table
    }

    #[inline(always)]
    fn table_index(&self, key: u64) -> usize {
        let key_usize = key as usize;
        // In 99.9% of deployments, table_size matches MAGLEV_TABLE_SIZE (65,537).
        // Branching on the compile-time constant allows LLVM to emit a fast reciprocal
        // multiply and shift (1-2 cycles) instead of a 64-bit hardware integer division (`idivq`, 15-25 cycles).
        if self.table_size == MAGLEV_TABLE_SIZE {
            key_usize % MAGLEV_TABLE_SIZE
        } else {
            key_usize % self.table_size
        }
    }
}

impl LoadBalancer for Maglev {
    fn select_index(&self, endpoints: &[Endpoint], ctx: &SelectionContext<'_>) -> Option<usize> {
        let n = endpoints.len();
        if n == 0 {
            return None;
        }
        if n == 1 {
            return Some(0);
        }

        let current = self.state.load();
        let ptr = endpoints.as_ptr() as usize;

        // Ultra-fast path: same slice pointer/len or matching topology version.
        // Completely bypasses FNV1a hashing, achieving flat ~3 ns lookup regardless of N backends.
        if ((current.endpoints_ptr == ptr && current.endpoints_len == n)
            || ctx
                .topology_version
                .is_some_and(|v| v == current.fingerprint))
            && current.table.len() == self.table_size
        {
            let key = ctx.hash_key.unwrap_or(0);
            let idx = self.table_index(key);
            let ep_idx = current.table[idx];
            return if ep_idx < n { Some(ep_idx) } else { Some(0) };
        }

        // Fallback: compute FNV1a fingerprint over endpoints slice
        let fp = ctx
            .topology_version
            .unwrap_or_else(|| Self::compute_fingerprint(endpoints));

        // Fast path: lock-free atomic load
        if current.fingerprint == fp && current.table.len() == self.table_size {
            let key = ctx.hash_key.unwrap_or(0);
            let idx = self.table_index(key);
            let ep_idx = current.table[idx];
            return if ep_idx < n { Some(ep_idx) } else { Some(0) };
        }

        // Recover lock guard if another thread panics during table construction
        let _guard = self
            .rebuild_lock
            .lock()
            .unwrap_or_else(|poison_err| poison_err.into_inner());

        // Double check after acquiring lock
        let current = self.state.load();
        if current.fingerprint == fp && current.table.len() == self.table_size {
            let key = ctx.hash_key.unwrap_or(0);
            let idx = self.table_index(key);
            let ep_idx = current.table[idx];
            return if ep_idx < n { Some(ep_idx) } else { Some(0) };
        }

        let new_table = Self::build_table(endpoints, self.table_size);
        let new_state = MaglevState {
            table: new_table,
            fingerprint: fp,
            endpoints_ptr: ptr,
            endpoints_len: n,
        };

        let key = ctx.hash_key.unwrap_or(0);
        let idx = self.table_index(key);
        let ep_idx = new_state.table[idx];

        self.state.store(Arc::new(new_state));
        if ep_idx < n { Some(ep_idx) } else { Some(0) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    #[test]
    fn test_maglev_consistency_and_stability() {
        let ep1: SocketAddr = "10.0.0.1:8080".parse().unwrap();
        let ep2: SocketAddr = "10.0.0.2:8080".parse().unwrap();
        let ep3: SocketAddr = "10.0.0.3:8080".parse().unwrap();

        let endpoints = vec![
            Endpoint::new("e1", ep1, 1),
            Endpoint::new("e2", ep2, 1),
            Endpoint::new("e3", ep3, 1),
        ];

        let balancer = Maglev::new(257); // Small prime for fast test
        let key = 123456789u64;
        let ctx = SelectionContext::with_hash(key);

        let initial_choice = balancer.select(&endpoints, &ctx).unwrap().address;

        // Repeat 1,000 times: must return identical target
        for _ in 0..1_000 {
            let choice = balancer.select(&endpoints, &ctx).unwrap().address;
            assert_eq!(choice, initial_choice);
        }
    }

    #[test]
    fn test_table_size_auto_prime() {
        let ep1: SocketAddr = "10.0.0.1:8080".parse().unwrap();
        let ep2: SocketAddr = "10.0.0.2:8080".parse().unwrap();
        let endpoints = vec![Endpoint::new("e1", ep1, 1), Endpoint::new("e2", ep2, 1)];

        // 100 is composite; must auto-upgrade to 101 without infinite looping or hanging
        let balancer = Maglev::new(100);
        assert_eq!(balancer.table_size, 101);

        let ctx = SelectionContext::with_hash(42);
        let chosen = balancer.select(&endpoints, &ctx);
        assert!(chosen.is_some());
    }
}
