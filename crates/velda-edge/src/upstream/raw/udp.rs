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
    last_probe_sec: std::sync::atomic::AtomicU64,
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
            last_probe_sec: std::sync::atomic::AtomicU64::new(0),
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

    /// Selects an eligible backend endpoint via configured load balancing with contextual affinity attributes.
    #[inline]
    pub fn select_target_with_context(
        &self,
        ctx: &velda_lb::SelectionContext<'_>,
    ) -> Option<SocketAddr> {
        self.inner.select_endpoint_with_context(ctx).ok()
    }

    /// Connects an ephemeral UDP socket to the given target endpoint using pre-compiled acceleration.
    #[inline]
    pub fn connect_socket(
        &self,
        target: SocketAddr,
    ) -> Result<tokio::net::UdpSocket, std::io::Error> {
        connect_udp_socket(target, &self.acceleration)
    }

    /// Performs periodic active health check across all physical endpoints for `lane`.
    pub async fn check_active_health(
        &self,
        _lane: usize,
        heal_sock: Option<&velda_sock::UdpSocket>,
    ) {
        let health = self.inner.health();
        let active_cfg = match health.config().active.as_ref() {
            Some(a) if a.enabled => a,
            _ => return,
        };

        let now_sec = velda_core::coarse_now_sec();
        let interval_sec = active_cfg.interval.as_secs().max(1);
        let last = self
            .last_probe_sec
            .load(std::sync::atomic::Ordering::Relaxed);
        if now_sec < last + interval_sec {
            return;
        }
        self.last_probe_sec
            .store(now_sec, std::sync::atomic::Ordering::Relaxed);

        let endpoints = self.inner.current_endpoints();
        let all_eps = endpoints.all_endpoints();
        if all_eps.is_empty() {
            return;
        }

        for ep in all_eps {
            let addr = ep.address;
            let success = if let Some(sock) = heal_sock {
                sock.inner().try_send_to(b"ping", addr).is_ok()
            } else {
                false
            };
            health.record_active_result(&addr, success);
        }
    }
}
