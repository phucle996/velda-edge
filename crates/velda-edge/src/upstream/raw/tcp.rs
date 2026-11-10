//! Layer 4 TCP Upstream managing connection lifecycle and stream dispatch.
//!
//! Enforces Rule 2.8 by delegating all raw socket creation, Linux setsockopt flags,
//! and transport handshakes to `super::tcp_connector`. Upstream strictly owns
//! target endpoint selection, health state, and stream dispatch.

use tokio::net::TcpStream;

use super::tcp_connector::{TcpAccelerationPath, connect_tcp_stream};
use crate::error::EdgeError;
use crate::upstream::lb::EdgeUpstream;

/// Layer 4 TCP Upstream managing connection acquisition and stream forwarding.
///
/// Pre-compiled with static load balancer, physical discovery endpoints, and pre-computed socket acceleration.
pub struct TcpUpstream {
    /// [PRE-COMPILED]: Pre-assembled upstream core holding discovery, health tracker,
    /// timeouts, and the selected `LbAlgorithm` enum variant.
    inner: EdgeUpstream,
    acceleration: TcpAccelerationPath,
    last_probe_sec: std::sync::atomic::AtomicU64,
}

impl std::fmt::Debug for TcpUpstream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TcpUpstream")
            .field("id", &self.inner.id())
            .field("acceleration", &self.acceleration)
            .finish()
    }
}

impl TcpUpstream {
    /// Creates a new [`TcpUpstream`] instance.
    pub fn new(inner: EdgeUpstream, acceleration: TcpAccelerationPath) -> Self {
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

    /// Returns the upstream timeouts.
    #[inline]
    pub fn timeouts(&self) -> velda_upstream::UpstreamTimeouts {
        *self.inner.timeouts()
    }

    /// Hands off downstream byte forwarding to an acquired upstream backend connection.
    pub async fn dispatch_stream<F, Fut, T, E>(&self, pipe: F) -> Result<T, EdgeError>
    where
        F: FnOnce(TcpStream) -> Fut,
        Fut: std::future::Future<Output = Result<T, E>>,
        E: std::fmt::Display,
    {
        let socket_accel = self.acceleration;
        let connect_timeout = self.inner.timeouts().connect;

        let stream = self
            .inner
            .execute(|endpoint| async move {
                connect_tcp_stream(endpoint, &socket_accel, connect_timeout)
                    .await
                    .map_err(|e| e.to_string())
            })
            .await
            .map_err(EdgeError::Upstream)?;

        pipe(stream).await.map_err(|e| {
            EdgeError::Upstream(velda_upstream::UpstreamError::Protocol(e.to_string()))
        })
    }

    /// Performs periodic active health check across all physical endpoints for `lane`.
    pub async fn check_active_health(&self, _lane: usize) {
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

        let timeout = active_cfg.timeout;
        for ep in all_eps {
            let addr = ep.address;
            let success =
                match tokio::time::timeout(timeout, tokio::net::TcpStream::connect(addr)).await {
                    Ok(Ok(stream)) => {
                        let _ = stream
                            .into_std()
                            .and_then(|s| s.shutdown(std::net::Shutdown::Both));
                        true
                    }
                    _ => false,
                };
            health.record_active_result(&addr, success);
        }
    }
}
