//! # velda-upstream: Logical Upstream Pipeline Root
//!
//! Owns logical backends, discovery, health tracking, and load balancing:
//! ```text
//! [1. Discovery] ──> [2. Usable Health Filter] ──> [3. Load Balancer] ──> [4. Candidate Execution / Failover]
//! ```

use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use velda_core::Endpoint;
use velda_discovery::{Discovery, EndpointSet};
use velda_lb::{LoadBalancer, RoundRobin, SelectionContext};

use crate::error::{Result, UpstreamError};
use crate::health::{HealthConfig, HealthTracker};

#[derive(Debug)]
struct DegradedSnapshot {
    discovery_gen: u64,
    health_epoch: u64,
    usable_endpoints: Arc<[Endpoint]>,
}

// ============================================================================
// 1. Upstream Timeouts Configuration
// ============================================================================

/// Timeout configuration for upstream connections.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UpstreamTimeouts {
    /// Timeout for establishing a new physical backend connection (TCP/TLS handshake).
    pub connect: Duration,

    /// Maximum idle duration before a pooled connection is considered expired and evicted.
    pub idle: Duration,

    /// Optional request/response transaction timeout (applies to L7 protocols; None for L4 raw streams).
    pub request: Option<Duration>,
}

impl UpstreamTimeouts {
    /// Creates timeout configuration for unbounded L4 streams (no request timeout).
    pub fn tcp(connect: Duration, idle: Duration) -> Self {
        Self {
            connect,
            idle,
            request: None,
        }
    }

    /// Creates timeout configuration for L7 request/response transactions.
    pub fn http(connect: Duration, idle: Duration, request: Duration) -> Self {
        Self {
            connect,
            idle,
            request: Some(request),
        }
    }
}

// ============================================================================
// 2. Logical Upstream Entity
// ============================================================================

/// Represents an active logical upstream managing backend discovery, health, and load balancing.
pub struct Upstream<LB: LoadBalancer = RoundRobin> {
    id: String,
    protocol: Arc<str>,
    discovery: Arc<Discovery>,
    health: Arc<HealthTracker>,
    balancer: LB,
    timeouts: UpstreamTimeouts,
    degraded_cache: ArcSwap<DegradedSnapshot>,
}

impl<LB: LoadBalancer> Upstream<LB> {
    /// Primary constructor for a fully configured upstream entity.
    pub fn new(
        id: impl Into<String>,
        protocol: impl Into<Arc<str>>,
        discovery: Arc<Discovery>,
        balancer: LB,
        timeouts: UpstreamTimeouts,
    ) -> Self {
        Self {
            id: id.into(),
            protocol: protocol.into(),
            discovery,
            health: Arc::new(HealthTracker::new(HealthConfig::default())),
            balancer,
            timeouts,
            degraded_cache: ArcSwap::from_pointee(DegradedSnapshot {
                discovery_gen: u64::MAX,
                health_epoch: u64::MAX,
                usable_endpoints: Arc::from([]),
            }),
        }
    }

    /// Attaches custom health tracker configuration.
    pub fn with_health_config(mut self, config: HealthConfig) -> Self {
        self.health = Arc::new(HealthTracker::new(config));
        self
    }

    /// Attaches an existing shared health tracker.
    pub fn with_health_tracker(mut self, tracker: Arc<HealthTracker>) -> Self {
        self.health = tracker;
        self
    }

    // ------------------------------------------------------------------------
    // Inspection & Metrics Getters
    // ------------------------------------------------------------------------

    /// Returns the logical upstream identifier.
    #[inline]
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Returns the upstream configured default protocol.
    #[inline]
    pub fn protocol(&self) -> &str {
        &self.protocol
    }

    /// Returns the upstream configured timeouts.
    #[inline]
    pub fn timeouts(&self) -> &UpstreamTimeouts {
        &self.timeouts
    }

    /// Returns the current active [`EndpointSet`] from discovery.
    #[inline]
    pub fn current_endpoints(&self) -> Arc<EndpointSet> {
        self.discovery.current_endpoints()
    }

    /// Accesses the underlying health tracker.
    #[inline]
    pub fn health(&self) -> &Arc<HealthTracker> {
        &self.health
    }

    /// Prunes health tracker state for endpoints that are no longer part of active discovery.
    pub fn prune_retired_endpoints(&self) {
        let current = self.discovery.current_endpoints();
        let active_addrs: HashSet<SocketAddr> = current
            .all_endpoints()
            .iter()
            .map(|ep| ep.address)
            .collect();
        self.health.prune_unregistered(&active_addrs);
    }

    /// Spawns a background maintenance worker that periodically prunes retired endpoint health state.
    pub fn spawn_maintenance_task(
        self: &Arc<Self>,
        interval: Duration,
    ) -> tokio::task::JoinHandle<()>
    where
        LB: 'static,
    {
        let upstream = Arc::clone(self);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                ticker.tick().await;
                upstream.prune_retired_endpoints();
            }
        })
    }

    // ------------------------------------------------------------------------
    // Public Acquisition & Execution Entrypoints
    // ------------------------------------------------------------------------

    /// Selects a healthy endpoint address using the configured load balancer.
    pub fn select_endpoint(&self) -> Result<SocketAddr> {
        let endpoints = self.discovery.current_endpoints();
        let all_eps = endpoints.all_endpoints();
        if all_eps.is_empty() {
            return Err(UpstreamError::NoEndpointsAvailable(self.id.clone()));
        }

        let cached_degraded;
        let usable: &[Endpoint] = if !self.health.has_unhealthy() {
            all_eps
        } else {
            let current_gen = endpoints.generation();
            let current_epoch = self.health.epoch();
            cached_degraded = self.get_or_compile_degraded(&endpoints, current_gen, current_epoch);
            if cached_degraded.is_empty() {
                return Err(UpstreamError::NoEndpointsAvailable(self.id.clone()));
            }
            &cached_degraded[..]
        };

        let selected = self
            .balancer
            .select(usable, &SelectionContext::NONE)
            .ok_or_else(|| UpstreamError::NoEndpointsAvailable(self.id.clone()))?;

        Ok(selected.address)
    }

    /// Executes a protocol-specific asynchronous operation against a healthy target endpoint.
    ///
    /// Manages healthy candidate selection, health success/failure tracking, and automatic
    /// failover if the operation encounters a network/connection error.
    pub async fn execute<F, Fut, T, E>(&self, mut action: F) -> Result<T>
    where
        F: FnMut(SocketAddr) -> Fut,
        Fut: std::future::Future<Output = std::result::Result<T, E>>,
        E: std::fmt::Display,
    {
        let endpoints = self.discovery.current_endpoints();
        let all_eps = endpoints.all_endpoints();
        if all_eps.is_empty() {
            return Err(UpstreamError::NoEndpointsAvailable(self.id.clone()));
        }

        let cached_degraded;
        let usable: &[Endpoint] = if !self.health.has_unhealthy() {
            all_eps
        } else {
            let current_gen = endpoints.generation();
            let current_epoch = self.health.epoch();
            cached_degraded = self.get_or_compile_degraded(&endpoints, current_gen, current_epoch);
            if cached_degraded.is_empty() {
                return Err(UpstreamError::NoEndpointsAvailable(self.id.clone()));
            }
            &cached_degraded[..]
        };

        let max_attempts = usable.len().min(2);
        let mut last_err = None;
        let mut candidate_slice = usable;
        let mut fallback_endpoints: Vec<Endpoint>;

        for _ in 0..max_attempts {
            let Some(selected) = self
                .balancer
                .select(candidate_slice, &SelectionContext::NONE)
            else {
                break;
            };

            let target_addr = selected.address;
            match action(target_addr).await {
                Ok(val) => {
                    self.health.record_success(&target_addr);
                    return Ok(val);
                }
                Err(e) => {
                    self.health.record_failure(&target_addr);
                    last_err = Some(e.to_string());

                    fallback_endpoints = candidate_slice
                        .iter()
                        .filter(|ep| ep.address != target_addr)
                        .cloned()
                        .collect();
                    candidate_slice = &fallback_endpoints[..];
                }
            }
        }

        Err(UpstreamError::ConnectionFailed {
            endpoint: usable[0].address,
            reason: last_err.unwrap_or_else(|| "all candidate endpoints failed".into()),
        })
    }

    fn get_or_compile_degraded(
        &self,
        endpoints: &Arc<EndpointSet>,
        current_gen: u64,
        current_epoch: u64,
    ) -> Arc<[Endpoint]> {
        let snapshot = self.degraded_cache.load();
        if snapshot.discovery_gen == current_gen && snapshot.health_epoch == current_epoch {
            return Arc::clone(&snapshot.usable_endpoints);
        }

        let compiled: Arc<[Endpoint]> = endpoints
            .all_endpoints()
            .iter()
            .filter(|ep| self.health.is_healthy(&ep.address))
            .cloned()
            .collect();

        self.degraded_cache.store(Arc::new(DegradedSnapshot {
            discovery_gen: current_gen,
            health_epoch: current_epoch,
            usable_endpoints: Arc::clone(&compiled),
        }));

        compiled
    }
}

// ============================================================================
// 3. Unit Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    #[tokio::test]
    async fn test_upstream_round_robin_selection() {
        let ep1: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let ep2: SocketAddr = "127.0.0.1:8081".parse().unwrap();
        let discovery = Discovery::new_explicit(vec![
            Endpoint::new("e1", ep1, 1),
            Endpoint::new("e2", ep2, 1),
        ]);

        let timeouts = UpstreamTimeouts::http(
            Duration::from_millis(500),
            Duration::from_secs(30),
            Duration::from_secs(5),
        );
        let upstream = Upstream::new(
            "users-service",
            "http1",
            discovery,
            RoundRobin::new(),
            timeouts,
        );

        assert_eq!(upstream.id(), "users-service");
        assert_eq!(upstream.protocol(), "http1");

        let s1 = upstream.select_endpoint().unwrap();
        let s2 = upstream.select_endpoint().unwrap();
        assert_ne!(s1, s2);
    }

    #[tokio::test]
    async fn test_upstream_execute_failover() {
        let ep1: SocketAddr = "10.0.0.1:8080".parse().unwrap();
        let ep2: SocketAddr = "10.0.0.2:8080".parse().unwrap();

        let discovery = Discovery::new_explicit(vec![
            Endpoint::new("e1", ep1, 1),
            Endpoint::new("e2", ep2, 1),
        ]);

        let timeouts = UpstreamTimeouts::tcp(Duration::from_millis(500), Duration::from_secs(30));
        let upstream = Upstream::new("test-up", "tcp", discovery, RoundRobin::new(), timeouts)
            .with_health_config(HealthConfig::passive_only(1, Duration::from_secs(10)));

        // Execute action where ep1 fails but ep2 succeeds
        let res = upstream
            .execute(|ep| async move {
                if ep == ep1 {
                    Err("connection refused")
                } else {
                    Ok("success")
                }
            })
            .await;

        assert_eq!(res.unwrap(), "success");
        // ep1 should be recorded as failed, ep2 as succeeded
        assert!(!upstream.health().is_healthy(&ep1));
        assert!(upstream.health().is_healthy(&ep2));
    }

    #[tokio::test]
    async fn test_upstream_maintenance_and_prune() {
        let ep1: SocketAddr = "10.0.0.1:8080".parse().unwrap();
        let ep2: SocketAddr = "10.0.0.2:8080".parse().unwrap();

        let discovery = Discovery::new_explicit(vec![
            Endpoint::new("e1", ep1, 1),
            Endpoint::new("e2", ep2, 1),
        ]);
        let timeouts = UpstreamTimeouts::tcp(Duration::from_millis(500), Duration::from_millis(10));
        let health_config = HealthConfig::passive_only(1, Duration::from_secs(10));
        let upstream = Arc::new(
            Upstream::new(
                "sweep-up",
                "tcp",
                discovery.clone(),
                RoundRobin::new(),
                timeouts,
            )
            .with_health_config(health_config),
        );

        // Populate health record for ep2
        upstream.health().record_failure(&ep2);
        assert!(upstream.health().has_record(&ep2));

        // Prune retired endpoints
        discovery.update_endpoints(vec![Endpoint::new("e1", ep1, 1)], 2);
        upstream.prune_retired_endpoints();
        assert!(!upstream.health().has_record(&ep2));
    }
}
