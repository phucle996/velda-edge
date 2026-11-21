//! Layer 4 UDP routing rules and in-memory lookup table.
//!
//! ### Architectural Note: Protocol Isolation Invariant (AGENTS.md §2.1 & §2.7)
//! UDP routing operates strictly on L4 datagram flows, managing optional idle session timeouts
//! for bidirectional stateful proxying or unidirectional fire-and-forget.

use std::time::Duration;

use crate::error::RouterError;

/// Concrete compiled Layer 4 UDP routing rule (`Copy`, zero heap strings).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UdpRoute {
    /// Unique identifier of this route (`u16`).
    pub id: u16,
    /// Ingress listener identifier (`u32` FNV-1a) this route attaches to.
    pub listener_id: u32,
    /// Logical upstream destination for forwarding traffic (`u16`).
    pub upstream_id: u16,
    /// Optional idle timeout for stateful bidirectional L4 UDP sessions.
    /// If `None`, UDP is unidirectional (1 chiều fire-and-forget).
    /// If `Some`, UDP is bidirectional (2 chiều stateful proxying).
    pub udp_idle_timeout: Option<Duration>,
    /// Session affinity strategy (pure L4 client peer address vs opt-in payload token).
    pub session_affinity: UdpSessionAffinity,
}

/// Session affinity strategy for L4 UDP stateful routing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum UdpSessionAffinity {
    /// Pure L4 default: Session key is (listener_id, client_addr). Zero payload inspection.
    #[default]
    ClientAddr,
    /// Opt-in Token Affinity (Game Ticket / IoT / QUIC Connection ID):
    /// Extracts a token of `len` bytes (1..=16) at byte `offset` in the UDP payload.
    /// Used as the session table key (supporting NAT rebinding) and deterministic LB hash key.
    PayloadToken { offset: u16, len: u8 },
}

impl UdpSessionAffinity {
    /// Extracts a 64-bit session token from the UDP payload if enabled and data is sufficient.
    #[inline]
    pub fn extract_token(&self, payload: &[u8]) -> Option<u64> {
        match self {
            Self::ClientAddr => None,
            Self::PayloadToken { offset, len } => {
                let start = *offset as usize;
                let end = start.checked_add(*len as usize)?;
                if payload.len() >= end && *len > 0 && *len <= 16 {
                    let slice = &payload[start..end];
                    match slice.len() {
                        8 => {
                            let buf: [u8; 8] = slice.try_into().ok()?;
                            Some(u64::from_le_bytes(buf))
                        }
                        4 => {
                            let buf: [u8; 4] = slice.try_into().ok()?;
                            Some(u32::from_le_bytes(buf) as u64)
                        }
                        1..=7 => {
                            let mut buf = [0u8; 8];
                            buf[..slice.len()].copy_from_slice(slice);
                            Some(u64::from_le_bytes(buf))
                        }
                        9..=16 => {
                            // Fast 64-bit FNV-1a hash
                            let mut hash: u64 = 0xcbf29ce484222325;
                            for &byte in slice {
                                hash = (hash ^ (byte as u64)).wrapping_mul(0x100000001b3);
                            }
                            Some(hash)
                        }
                        _ => None,
                    }
                } else {
                    None
                }
            }
        }
    }
}

impl UdpRoute {
    /// Creates a new L4 UDP route rule.
    #[inline]
    pub const fn new(id: u16, listener_id: u32, upstream_id: u16) -> Self {
        Self {
            id,
            listener_id,
            upstream_id,
            udp_idle_timeout: None,
            session_affinity: UdpSessionAffinity::ClientAddr,
        }
    }

    /// Sets the optional idle timeout for stateful bidirectional L4 UDP sessions.
    #[inline]
    pub const fn with_udp_idle_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.udp_idle_timeout = timeout;
        self
    }

    /// Sets the session affinity strategy (pure L4 peer address vs opt-in payload token).
    #[inline]
    pub const fn with_session_affinity(mut self, affinity: UdpSessionAffinity) -> Self {
        self.session_affinity = affinity;
        self
    }

    /// Returns whether this route is configured for unidirectional (fire-and-forget) UDP.
    #[inline]
    pub const fn is_unidirectional(&self) -> bool {
        self.udp_idle_timeout.is_none()
    }
}

/// High-performance, in-memory L4 UDP route lookup table backed by a flat contiguous `Vec<UdpRoute>`.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct UdpRouter {
    routes: Vec<UdpRoute>,
}

impl UdpRouter {
    /// Builds a new [`UdpRouter`] from a list of compiled [`UdpRoute`] rules.
    pub fn new(routes: impl IntoIterator<Item = UdpRoute>) -> Result<Self, RouterError> {
        let mut vec: Vec<UdpRoute> = routes.into_iter().collect();
        vec.sort_by_key(|r| r.listener_id);
        for w in vec.windows(2) {
            if w[0].listener_id == w[1].listener_id {
                return Err(RouterError::DuplicateRoute {
                    listener: w[0].listener_id.to_string(),
                    protocol: "Udp".to_string(),
                });
            }
        }
        Ok(Self { routes: vec })
    }

    /// Resolves a UDP route for an incoming datagram on a listener.
    #[inline]
    pub fn route(&self, listener_id: u32) -> Option<&UdpRoute> {
        if self.routes.len() <= 16 {
            self.routes.iter().find(|r| r.listener_id == listener_id)
        } else {
            self.routes
                .binary_search_by_key(&listener_id, |r| r.listener_id)
                .ok()
                .map(|idx| &self.routes[idx])
        }
    }

    /// Returns the total number of configured UDP routes.
    #[inline]
    pub fn len(&self) -> usize {
        self.routes.len()
    }

    /// Returns whether the router has no configured routes.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.routes.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use velda_core::fnv1a_hash_32;

    #[test]
    fn test_udp_router_lookup() {
        let l1 = fnv1a_hash_32("dns-in");
        let l2 = fnv1a_hash_32("syslog-in");
        let routes = vec![
            UdpRoute::new(1, l1, 10).with_udp_idle_timeout(Some(Duration::from_secs(30))),
            UdpRoute::new(2, l2, 20),
        ];

        let router = UdpRouter::new(routes).unwrap();
        assert_eq!(router.len(), 2);
        assert!(!router.is_empty());

        let r1 = router.route(l1).unwrap();
        assert_eq!(r1.id, 1);
        assert_eq!(r1.upstream_id, 10);
        assert!(!r1.is_unidirectional());

        let r2 = router.route(l2).unwrap();
        assert_eq!(r2.id, 2);
        assert_eq!(r2.upstream_id, 20);
        assert!(r2.is_unidirectional());

        assert!(router.route(fnv1a_hash_32("unknown")).is_none());
    }

    #[test]
    fn test_udp_router_duplicate_fails() {
        let l1 = fnv1a_hash_32("dns");
        let routes = vec![UdpRoute::new(1, l1, 10), UdpRoute::new(2, l1, 20)];

        let err = UdpRouter::new(routes).unwrap_err();
        assert!(matches!(err, RouterError::DuplicateRoute { .. }));
    }
}
