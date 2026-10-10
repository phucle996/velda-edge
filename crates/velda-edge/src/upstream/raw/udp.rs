//! Layer 4 UDP Upstream managing backend endpoints and load-balanced target selection.
//!
//! Enforces Rule 2.8 by delegating all raw datagram socket creation and Linux setsockopt
//! offload flags to `super::udp_connector`. Upstream strictly owns endpoint topology
//! and health tracking.

use std::net::SocketAddr;

use super::udp_connector::{UdpAccelerationPath, connect_udp_socket};
use crate::upstream::lb::EdgeUpstream;

/// Layer 4 UDP Upstream managing physical backend endpoint targets.
///
/// Pre-compiled with static load balancer, physical discovery endpoints, and socket acceleration profile.
pub struct UdpUpstream {
    /// [PRE-COMPILED]: Pre-assembled upstream core holding discovery, health tracker,
    /// timeouts, and the selected `LbAlgorithm` enum variant.
    inner: EdgeUpstream,
    acceleration: UdpAccelerationPath,
}

impl std::fmt::Debug for UdpUpstream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UdpUpstream")
            .field("id", &self.inner.id())
            .field("acceleration", &self.acceleration)
            .finish()
    }
}

impl UdpUpstream {
    /// Creates a new [`UdpUpstream`] instance.
    pub fn new(inner: EdgeUpstream, acceleration: UdpAccelerationPath) -> Self {
        Self {
            inner,
            acceleration,
        }
    }

    /// Returns the upstream unique identifier.
    #[inline]
    pub fn id(&self) -> &str {
        self.inner.id()
    }

    /// Returns the pre-compiled socket acceleration path.
    #[inline]
    pub fn acceleration(&self) -> &UdpAccelerationPath {
        &self.acceleration
    }

    /// Selects an eligible backend endpoint via configured load balancing.
    #[inline]
    pub fn select_target(&self) -> Option<SocketAddr> {
        self.inner.select_endpoint().ok()
    }

    /// Connects an ephemeral UDP socket to the given target endpoint using pre-compiled acceleration.
    #[inline]
    pub fn connect_socket(
        &self,
        target: SocketAddr,
    ) -> Result<tokio::net::UdpSocket, std::io::Error> {
        connect_udp_socket(target, &self.acceleration)
    }
}
