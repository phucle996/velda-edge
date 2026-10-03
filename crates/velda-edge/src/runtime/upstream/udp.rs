//! Layer 4 UDP Upstream managing physical backend endpoint targets.

use std::net::SocketAddr;

use super::lb::EdgeUpstream;

/// Layer 4 UDP Upstream managing physical backend endpoint targets.
///
/// Pre-compiled with static load balancer and physical discovery endpoints.
pub struct UdpUpstream {
    /// [PRE-COMPILED]: Pre-assembled upstream core holding discovery, health tracker,
    /// timeouts, and the selected `LbAlgorithm` enum variant.
    inner: EdgeUpstream,
}

impl std::fmt::Debug for UdpUpstream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UdpUpstream")
            .field("id", &self.inner.id())
            .finish()
    }
}

impl UdpUpstream {
    /// Creates a new [`UdpUpstream`] instance.
    pub fn new(inner: EdgeUpstream) -> Self {
        Self { inner }
    }

    /// Returns the upstream unique identifier.
    #[inline]
    pub fn id(&self) -> &str {
        self.inner.id()
    }

    /// Selects an eligible backend endpoint via Round-Robin load balancing.
    #[inline]
    pub fn select_target(&self) -> Option<SocketAddr> {
        self.inner.select_endpoint().ok()
    }
}
