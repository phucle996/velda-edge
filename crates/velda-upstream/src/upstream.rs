//! # velda-upstream: Logical Upstream Pipeline Root
//!
//! Owns logical backends, discovery, health tracking, and load balancing:
//! ```text
//! [1. Discovery] ──> [2. Usable Health Filter] ──> [3. Load Balancer] ──> [4. Candidate Execution / Failover]
//! ```

use std::cell::Cell;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use arc_swap::ArcSwap;
use velda_core::Endpoint;
use velda_discovery::{Discovery, EndpointSet};
use velda_lb::{LoadBalancer, RoundRobin, SelectionContext};

use crate::error::{Result, UpstreamError};
use crate::health::{HealthConfig, HealthTracker};

thread_local! {
    static UPSTREAM_LANE_ID: Cell<usize> = const { Cell::new(usize::MAX) };
}

#[inline(always)]
fn current_lane_id() -> usize {
    UPSTREAM_LANE_ID.with(|cell| {
        let mut id = cell.get();
        if id == usize::MAX {
            static NEXT_LANE: AtomicUsize = AtomicUsize::new(0);
            id = NEXT_LANE.fetch_add(1, Ordering::Relaxed);
            cell.set(id);
        }
        id
    })
}

/// Immutable per-lane endpoint snapshot pushed by discovery on topology changes.
#[derive(Debug)]
pub struct LaneEndpointSnapshot {
    pub endpoints: Arc<[Endpoint]>,
    pub generation: u64,
}

#[repr(align(64))]
struct LaneEndpointShard {
    snapshot: ArcSwap<LaneEndpointSnapshot>,
}

struct LaneEndpointShards {
    shards: Box<[LaneEndpointShard]>,
    mask: usize,
}

impl LaneEndpointShards {
    fn push(&self, endpoints: &[Endpoint], generation: u64) {
        let snap = Arc::new(LaneEndpointSnapshot {
            endpoints: Arc::from(endpoints),
            generation,
        });
        for shard in &self.shards {
            shard.snapshot.store(Arc::clone(&snap));
        }
    }
}

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
    id: Arc<str>,
    protocol: Arc<str>,
    discovery: Arc<Discovery>,
    health: Arc<HealthTracker>,
    balancer: LB,
    timeouts: UpstreamTimeouts,
    lanes: Arc<LaneEndpointShards>,
    degraded_cache: ArcSwap<DegradedSnapshot>,
}

impl<LB: LoadBalancer> Upstream<LB> {
    /// Primary constructor for a fully configured upstream entity.
    pub fn new(
        id: impl AsRef<str>,
        protocol: impl Into<Arc<str>>,
        discovery: Arc<Discovery>,
        balancer: LB,
        timeouts: UpstreamTimeouts,
    ) -> Self {
        let lane_count = velda_core::hardware::global_hardware_topology()
            .worker_threads
            .max(1)
            .next_power_of_two();
        let initial_set = discovery.current_endpoints();
        let initial_snap = Arc::new(LaneEndpointSnapshot {
            endpoints: Arc::from(initial_set.all_endpoints()),
            generation: initial_set.generation(),
        });
        let shards: Vec<LaneEndpointShard> = (0..lane_count)
            .map(|_| LaneEndpointShard {
                snapshot: ArcSwap::from(Arc::clone(&initial_snap)),
            })
            .collect();
        let lanes = Arc::new(LaneEndpointShards {
            shards: shards.into_boxed_slice(),
            mask: lane_count - 1,
        });

        let weak_lanes = Arc::downgrade(&lanes);
        discovery.set_on_change(Arc::new(move |endpoints, gen_id| {
            if let Some(l) = weak_lanes.upgrade() {
                l.push(endpoints, gen_id);
            }
        }));

        Self {
            id: Arc::from(id.as_ref()),
            protocol: protocol.into(),
            discovery,
            health: Arc::new(HealthTracker::new(HealthConfig::default())),
            balancer,
            timeouts,
            lanes,
            degraded_cache: ArcSwap::from_pointee(DegradedSnapshot {
                discovery_gen: u64::MAX,
                health_epoch: u64::MAX,
                usable_endpoints: Arc::from([]),
            }),
        }
    }

    /// Event-driven push: updates all lane shards with the latest sorted endpoint slice.
    pub fn push_endpoints(&self, endpoints: &[Endpoint], generation: u64) {
        self.lanes.push(endpoints, generation);
    }

    /// Returns the pre-computed lane-local endpoint snapshot without touching `Discovery::current`.
    #[inline(always)]
    pub fn current_lane_endpoints(&self) -> arc_swap::Guard<Arc<LaneEndpointSnapshot>> {
        let idx = current_lane_id() & self.lanes.mask;
        self.lanes.shards[idx].snapshot.load()
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
        let active_addrs: HashSet<(u128, u16)> = current
            .all_endpoints()
            .iter()
            .map(|ep| (ep.ip, ep.port_be))
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

    /// Selects a healthy endpoint address using the configured load balancer and selection context.
    pub fn select_endpoint_with_context(&self, ctx: &SelectionContext<'_>) -> Result<(u128, u16)> {
        let lane_snap = self.current_lane_endpoints();
        let all_eps: &[Endpoint] = &lane_snap.endpoints;
        if all_eps.is_empty() {
            return Err(UpstreamError::NoEndpointsAvailable(Arc::clone(&self.id)));
        }

        let cached_degraded;
        let usable: &[Endpoint] = if !self.health.has_unhealthy() {
            all_eps
        } else {
            let current_gen = lane_snap.generation;
            let current_epoch = self.health.epoch();
            cached_degraded = self.get_or_compile_degraded(all_eps, current_gen, current_epoch);
            if cached_degraded.is_empty() {
                return Err(UpstreamError::NoEndpointsAvailable(Arc::clone(&self.id)));
            }
            &cached_degraded[..]
        };

        let selected = self
            .balancer
            .select(usable, ctx)
            .ok_or_else(|| UpstreamError::NoEndpointsAvailable(Arc::clone(&self.id)))?;

        Ok((selected.ip, selected.port_be))
    }

    /// Selects a healthy endpoint address using the configured load balancer.
    #[inline]
    pub fn select_endpoint(&self) -> Result<(u128, u16)> {
        self.select_endpoint_with_context(&SelectionContext::NONE)
    }

    /// Executes a protocol-specific asynchronous operation against a healthy target endpoint.
    ///
    /// Manages healthy candidate selection, health success/failure tracking, and automatic
    /// failover if the operation encounters a network/connection error.
    pub async fn execute<F, Fut, T, E>(&self, mut action: F) -> Result<T>
    where
        F: FnMut((u128, u16)) -> Fut,
        Fut: std::future::Future<Output = std::result::Result<T, E>>,
        E: std::fmt::Display,
    {
        let lane_snap = self.current_lane_endpoints();
        let all_eps: &[Endpoint] = &lane_snap.endpoints;
        if all_eps.is_empty() {
            return Err(UpstreamError::NoEndpointsAvailable(Arc::clone(&self.id)));
        }

        let cached_degraded;
        let usable: &[Endpoint] = if !self.health.has_unhealthy() {
            all_eps
        } else {
            let current_gen = lane_snap.generation;
            let current_epoch = self.health.epoch();
            cached_degraded = self.get_or_compile_degraded(all_eps, current_gen, current_epoch);
            if cached_degraded.is_empty() {
                return Err(UpstreamError::NoEndpointsAvailable(Arc::clone(&self.id)));
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

            let target_addr = (selected.ip, selected.port_be);
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
                        .filter(|ep| (ep.ip, ep.port_be) != target_addr)
                        .copied()
                        .collect();
                    candidate_slice = &fallback_endpoints[..];
                }
            }
        }

        Err(UpstreamError::ConnectionFailed {
            ip: usable[0].ip,
            port_be: usable[0].port_be,
            reason: last_err.unwrap_or_else(|| "all candidate endpoints failed".into()),
        })
    }

    fn get_or_compile_degraded(
        &self,
        endpoints: &[Endpoint],
        current_gen: u64,
        current_epoch: u64,
    ) -> Arc<[Endpoint]> {
        let snapshot = self.degraded_cache.load();
        if snapshot.discovery_gen == current_gen && snapshot.health_epoch == current_epoch {
            return Arc::clone(&snapshot.usable_endpoints);
        }

        let compiled: Arc<[Endpoint]> = endpoints
            .iter()
            .filter(|ep| self.health.is_healthy(&(ep.ip, ep.port_be)))
            .copied()
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

    #[tokio::test]
    async fn test_upstream_round_robin_selection() {
        let discovery = Discovery::new_explicit(vec![
            Endpoint::from_v4([127, 0, 0, 1], 8080, 1),
            Endpoint::from_v4([127, 0, 0, 1], 8081, 1),
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
        let ep1_raw = Endpoint::from_v4([10, 0, 0, 1], 8080, 1);
        let ep2_raw = Endpoint::from_v4([10, 0, 0, 2], 8080, 1);
        let ep1 = (ep1_raw.ip, ep1_raw.port_be);
        let ep2 = (ep2_raw.ip, ep2_raw.port_be);

        let discovery = Discovery::new_explicit(vec![ep1_raw, ep2_raw]);

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
        let ep1_raw = Endpoint::from_v4([10, 0, 0, 1], 8080, 1);
        let ep2_raw = Endpoint::from_v4([10, 0, 0, 2], 8080, 1);
        let ep2 = (ep2_raw.ip, ep2_raw.port_be);

        let discovery = Discovery::new_explicit(vec![ep1_raw, ep2_raw]);
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
        discovery.update_endpoints(vec![ep1_raw], 2);
        upstream.prune_retired_endpoints();
        assert!(!upstream.health().has_record(&ep2));
    }
}
