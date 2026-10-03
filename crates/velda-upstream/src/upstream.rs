//! # velda-upstream: Logical Upstream Pipeline Root
//!
//! Owns the complete backend request lifecycle:
//! ```text
//! [1. Discovery] ──> [2. Usable Health Filter] ──> [3. Load Balancer]
//!                                                         │
//!                            ┌────────────────────────────┴───────────────────────────┐
//!                            ▼                                                        ▼
//!                 [4a. Pool Reuse (HIT)]                                   [4b. Network Connect (MISS)]
//!                            │                                                        │
//!                            └────────────────────────────┬───────────────────────────┘
//!                                                         ▼
//!                                              [5. RAII BackendLease]
//! ```

use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use velda_connection_pool::{PoolManager, PoolStats};
use velda_core::Endpoint;
use velda_discovery::{Discovery, EndpointSet};
use velda_lb::{LoadBalancer, RoundRobin, SelectionContext};

use crate::connection::{BackendConnection, ConnectionKey, Connector, TcpConnector};
use crate::error::{Result, UpstreamError};
use crate::health::{HealthConfig, HealthTracker};

/// Strongly typed pool manager for upstream backend connections.
pub type UpstreamPoolManager = PoolManager<ConnectionKey, Box<dyn BackendConnection>>;

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
///
/// NOTE: Timeouts must be explicitly configured from sync/JSON. Zero default guessing.
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
// 2. RAII Backend Lease Guard
// ============================================================================

/// An active, exclusive lease of a backend connection.
///
/// When finished with the request:
/// - Call [`BackendLease::release`] with `reusable: true` to return healthy connections to the pool.
/// - Call [`BackendLease::release`] with `reusable: false` if connection had protocol or I/O errors.
/// - If dropped without explicit release, RAII will safely inspect and return or close the connection.
pub struct BackendLease {
    connection: Option<Box<dyn BackendConnection>>,
    key: ConnectionKey,
    pool: Arc<UpstreamPoolManager>,
    health: Arc<HealthTracker>,
    is_draining: bool,
}

impl BackendLease {
    /// Creates a new backend lease wrapping an established connection.
    pub fn new(
        connection: Box<dyn BackendConnection>,
        key: ConnectionKey,
        pool: Arc<UpstreamPoolManager>,
        health: Arc<HealthTracker>,
        is_draining: bool,
    ) -> Self {
        Self {
            connection: Some(connection),
            key,
            pool,
            health,
            is_draining,
        }
    }

    /// Accesses the underlying backend connection.
    #[inline]
    pub fn connection(&self) -> &dyn BackendConnection {
        self.connection.as_ref().unwrap().as_ref()
    }

    /// Mutably accesses the underlying backend connection.
    #[inline]
    pub fn connection_mut(&mut self) -> &mut (dyn BackendConnection + 'static) {
        self.connection.as_mut().unwrap().as_mut()
    }

    /// Returns the target endpoint address of this connection.
    #[inline]
    pub fn endpoint(&self) -> SocketAddr {
        self.key.target_addr
    }

    /// Explicitly releases the connection back to the pool manager.
    ///
    /// - If `reusable` is true and endpoint is not draining: returns connection to pool and records health success.
    /// - If `reusable` is false or endpoint is draining: closes connection immediately.
    pub fn release(mut self, reusable: bool) {
        if let Some(conn) = self.connection.take() {
            if reusable {
                self.health.record_success(&self.key.target_addr);
            }
            self.pool
                .release(&self.key, conn, reusable, self.is_draining);
        }
    }

    /// Consumes this lease and extracts the raw underlying connection.
    pub fn into_inner(mut self) -> Option<Box<dyn BackendConnection>> {
        self.connection.take()
    }

    /// Consumes this lease and returns the underlying raw [`tokio::net::TcpStream`] if available.
    pub fn into_tcp_stream(mut self) -> Option<tokio::net::TcpStream> {
        self.connection.take().and_then(|c| c.into_tcp_stream())
    }
}

impl Drop for BackendLease {
    fn drop(&mut self) {
        if let Some(conn) = self.connection.take() {
            // Guard against leaked file descriptors on task cancellation / panic
            let healthy = conn.is_healthy();
            self.pool
                .release(&self.key, conn, healthy, self.is_draining);
        }
    }
}

// ============================================================================
// 3. Acquisition Target Parameters
// ============================================================================

/// Protocol and transport parameters passed into backend connection acquisition.
#[derive(Clone, Default)]
pub struct AcquireTarget<'a> {
    /// Optional protocol override (e.g. "http2", "http1", "tcp"). If None, uses upstream default protocol.
    pub protocol: Option<Arc<str>>,

    /// Optional TLS Server Name Indication (SNI).
    pub sni: Option<Arc<str>>,

    /// Optional ALPN negotiation token.
    pub alpn: Option<Arc<str>>,

    /// Contextual hints for advanced load balancing (e.g. hashing key, IP, client attributes).
    pub selection_context: SelectionContext<'a>,
}

impl<'a> AcquireTarget<'a> {
    /// Creates a default acquisition target using the upstream's configured protocol.
    pub fn default_target() -> Self {
        Self {
            protocol: None,
            sni: None,
            alpn: None,
            selection_context: SelectionContext::NONE,
        }
    }

    /// Creates an acquisition target specifying an explicit protocol override.
    pub fn new(protocol: impl Into<Arc<str>>) -> Self {
        Self {
            protocol: Some(protocol.into()),
            sni: None,
            alpn: None,
            selection_context: SelectionContext::NONE,
        }
    }

    pub fn with_sni(mut self, sni: impl Into<Arc<str>>) -> Self {
        self.sni = Some(sni.into());
        self
    }

    pub fn with_alpn(mut self, alpn: impl Into<Arc<str>>) -> Self {
        self.alpn = Some(alpn.into());
        self
    }

    pub fn with_selection_context(mut self, ctx: SelectionContext<'a>) -> Self {
        self.selection_context = ctx;
        self
    }

    pub fn with_hash_key(mut self, hash_key: u64) -> Self {
        self.selection_context.hash_key = Some(hash_key);
        self
    }
}

// ============================================================================
// 4. Logical Upstream Entity
// ============================================================================

/// Represents an active logical upstream managing backend discovery, health, and pooling.
pub struct Upstream<C: Connector = TcpConnector, LB: LoadBalancer = RoundRobin> {
    id: String,
    protocol: Arc<str>,
    discovery: Arc<Discovery>,
    health: Arc<HealthTracker>,
    balancer: LB,
    pool: Arc<UpstreamPoolManager>,
    connector: Arc<C>,
    timeouts: UpstreamTimeouts,
    degraded_cache: ArcSwap<DegradedSnapshot>,
}

// ============================================================================
// 5. Core Methods & Flat Workflow Pipeline
// ============================================================================

impl<C: Connector, LB: LoadBalancer> Upstream<C, LB> {
    /// Primary constructor for a fully configured upstream entity.
    pub fn new(
        id: impl Into<String>,
        protocol: impl Into<Arc<str>>,
        discovery: Arc<Discovery>,
        balancer: LB,
        timeouts: UpstreamTimeouts,
        connector: Arc<C>,
    ) -> Self {
        Self {
            id: id.into(),
            protocol: protocol.into(),
            discovery,
            health: Arc::new(HealthTracker::new(HealthConfig::default())),
            balancer,
            pool: Arc::new(UpstreamPoolManager::new()),
            connector,
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

    /// Returns the current active [`EndpointSet`] from discovery.
    #[inline]
    pub fn current_endpoints(&self) -> Arc<EndpointSet> {
        self.discovery.current_endpoints()
    }

    /// Returns the current pool statistics (hits, misses, evictions).
    #[inline]
    pub fn pool_stats(&self) -> PoolStats {
        self.pool.stats()
    }

    /// Accesses the underlying health tracker.
    #[inline]
    pub fn health(&self) -> &Arc<HealthTracker> {
        &self.health
    }

    /// Performs an active sweep of expired idle connections and empty subpools.
    ///
    /// Returns `(connections_evicted, empty_subpools_pruned)`.
    pub fn sweep_idle(&self) -> (usize, usize) {
        let evicted = self.pool.evict_expired(self.timeouts.idle);
        let pruned = self.pool.prune_empty_pools(self.timeouts.idle);
        (evicted, pruned)
    }

    /// Closes all idle connections across all pool shards and clears containers.
    pub fn clear_pool(&self) {
        self.pool.clear();
    }

    /// Prunes health tracker state for endpoints that are no longer part of active discovery.
    ///
    /// Prevents unbounded memory growth in long-running clusters with high discovery churn.
    pub fn prune_retired_endpoints(&self) {
        let current = self.discovery.current_endpoints();
        let active_addrs: HashSet<SocketAddr> = current
            .all_endpoints()
            .iter()
            .map(|ep| ep.address)
            .collect();
        self.health.prune_unregistered(&active_addrs);
    }

    /// Spawns a background maintenance worker that periodically sweeps idle connections
    /// and prunes retired endpoint health state.
    pub fn spawn_maintenance_task(
        self: &Arc<Self>,
        interval: Duration,
    ) -> tokio::task::JoinHandle<()>
    where
        C: 'static,
        LB: 'static,
    {
        let upstream = Arc::clone(self);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                ticker.tick().await;
                upstream.sweep_idle();
                upstream.prune_retired_endpoints();
            }
        })
    }

    // ------------------------------------------------------------------------
    // Public Acquisition Entrypoints
    // ------------------------------------------------------------------------

    /// Acquires a backend connection lease using the default protocol configured for this upstream.
    pub async fn acquire(&self) -> Result<BackendLease> {
        self.acquire_with_target(AcquireTarget::default_target())
            .await
    }

    /// Acquires a backend connection lease with an explicit protocol override.
    pub async fn acquire_protocol(&self, protocol: &str) -> Result<BackendLease> {
        self.acquire_with_target(AcquireTarget::new(protocol)).await
    }

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

    /// Executes a closure against an acquired backend lease, automatically releasing the lease
    /// on completion with health metrics tracking.
    pub async fn acquire_and_pipe<F, Fut, T, E>(
        &self,
        target: AcquireTarget<'_>,
        mut pipe: F,
    ) -> Result<T>
    where
        F: FnMut(&mut BackendLease) -> Fut,
        Fut: std::future::Future<Output = std::result::Result<T, E>>,
        E: std::fmt::Display,
    {
        let mut lease = self.acquire_with_target(target).await?;
        let addr = lease.endpoint();
        match pipe(&mut lease).await {
            Ok(res) => {
                lease.release(true);
                Ok(res)
            }
            Err(e) => {
                lease.release(false);
                Err(UpstreamError::ConnectionFailed {
                    endpoint: addr,
                    reason: e.to_string(),
                })
            }
        }
    }

    /// The core Flat Workflow Pipeline:
    ///
    /// 1. Query Discovery -> Filter healthy endpoints.
    /// 2. Pick candidate via Load Balancer.
    /// 3. Try Pool HIT (reuse idle connection).
    /// 4. On Pool MISS -> Connect via network with timeout.
    /// 5. On connection error -> Record health failure & fallback to next candidate.
    pub async fn acquire_with_target(&self, target: AcquireTarget<'_>) -> Result<BackendLease> {
        // Step 1: Query Discovery snapshot
        let endpoints = self.discovery.current_endpoints();
        let all_eps = endpoints.all_endpoints();

        if all_eps.is_empty() {
            return Err(UpstreamError::NoEndpointsAvailable(self.id.clone()));
        }

        // Step 2: Determine usable healthy endpoints
        // Zero-Alloc Fast Path: If all endpoints are healthy (normal hot path), bypass filtering!
        // Degraded Snapshot: If endpoints are degraded, read compiled snapshot from ArcSwap (O(1), zero-alloc).
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

        // Step 3: Attempt selection and connection (primary + 1 fallback on connection failure)
        let max_attempts = usable.len().min(2);
        let mut last_err = None;
        let mut candidate_slice = usable;
        let mut fallback_endpoints: Vec<Endpoint>;

        for _ in 0..max_attempts {
            // Step 3a: Run Load Balancer algorithm on healthy pool
            let Some(selected) = self
                .balancer
                .select(candidate_slice, &target.selection_context)
            else {
                break;
            };

            let is_draining = !endpoints.contains_addr(&selected.address);

            // Step 3b: Try acquiring from pool or establishing new connection
            match self
                .try_acquire_endpoint(selected, &target, is_draining)
                .await
            {
                Ok(lease) => return Ok(lease),
                Err(err) => {
                    let failed_addr = selected.address;
                    last_err = Some(err);

                    // Ensure subsequent retry falls back to another endpoint, even with deterministic hash balancers (IpHash)
                    if candidate_slice.len() > 1 {
                        fallback_endpoints = candidate_slice
                            .iter()
                            .filter(|ep| ep.address != failed_addr)
                            .cloned()
                            .collect();
                        candidate_slice = &fallback_endpoints;
                    }
                }
            }
        }

        Err(last_err.unwrap_or_else(|| UpstreamError::NoEndpointsAvailable(self.id.clone())))
    }

    // ------------------------------------------------------------------------
    // Internal Pipeline Helpers
    // ------------------------------------------------------------------------

    /// Retrieves the cached degraded healthy endpoints or compiles and swaps a fresh snapshot.
    fn get_or_compile_degraded(
        &self,
        endpoints: &EndpointSet,
        discovery_gen: u64,
        health_epoch: u64,
    ) -> Arc<[Endpoint]> {
        let cached = self.degraded_cache.load();
        if cached.discovery_gen == discovery_gen && cached.health_epoch == health_epoch {
            return Arc::clone(&cached.usable_endpoints);
        }

        let filtered: Vec<Endpoint> = endpoints
            .all_endpoints()
            .iter()
            .filter(|ep| self.health.is_healthy(&ep.address))
            .cloned()
            .collect();

        let new_usable: Arc<[Endpoint]> = Arc::from(filtered);
        self.degraded_cache.store(Arc::new(DegradedSnapshot {
            discovery_gen,
            health_epoch,
            usable_endpoints: Arc::clone(&new_usable),
        }));
        new_usable
    }

    /// Attempts to acquire a connection for a specific endpoint (Pool HIT first, then Network Connect on MISS).
    async fn try_acquire_endpoint(
        &self,
        endpoint: &Endpoint,
        target: &AcquireTarget<'_>,
        is_draining: bool,
    ) -> Result<BackendLease> {
        let proto = match &target.protocol {
            Some(p) => Arc::clone(p),
            None => Arc::clone(&self.protocol),
        };
        let key = ConnectionKey::http(
            endpoint.address,
            proto,
            target.sni.clone(),
            target.alpn.clone(),
        );

        // Sub-step A: Check Connection Pool (HIT)
        if let Some(pooled) = self.pool.acquire(&key, self.timeouts.idle) {
            return Ok(BackendLease::new(
                pooled,
                key,
                Arc::clone(&self.pool),
                Arc::clone(&self.health),
                is_draining,
            ));
        }

        // Sub-step B: Pool MISS -> Establish new connection with timeout
        match tokio::time::timeout(
            self.timeouts.connect,
            self.connector.connect(endpoint.address),
        )
        .await
        {
            Ok(Ok(new_conn)) => {
                self.health.record_success(&endpoint.address);
                Ok(BackendLease::new(
                    new_conn,
                    key,
                    Arc::clone(&self.pool),
                    Arc::clone(&self.health),
                    is_draining,
                ))
            }
            Ok(Err(err)) => {
                self.health.record_failure(&endpoint.address);
                Err(err)
            }
            Err(_) => {
                self.health.record_failure(&endpoint.address);
                Err(UpstreamError::AcquisitionTimeout(self.timeouts.connect))
            }
        }
    }
}

// ============================================================================
// 6. Testing & Custom Connector Constructors
// ============================================================================

#[cfg(any(test, feature = "test-utils"))]
impl<C: Connector> Upstream<C, RoundRobin> {
    /// Creates an upstream with a custom connector (useful for tests and non-TCP backends).
    pub fn with_connector(
        id: impl Into<String>,
        protocol: impl Into<Arc<str>>,
        discovery: Arc<Discovery>,
        timeouts: UpstreamTimeouts,
        connector: Arc<C>,
    ) -> Self {
        Self::new(
            id,
            protocol,
            discovery,
            RoundRobin::new(),
            timeouts,
            connector,
        )
    }
}

// ============================================================================
// 8. Unit Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::MockConnector;
    use std::net::SocketAddr;

    #[tokio::test]
    async fn test_upstream_acquire_and_pool_lifecycle() {
        let ep1: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let discovery = Discovery::new_explicit(vec![Endpoint::new("e1", ep1, 1)]);
        let connector = Arc::new(MockConnector::new());

        let timeouts = UpstreamTimeouts::http(
            Duration::from_millis(500),
            Duration::from_secs(30),
            Duration::from_secs(5),
        );
        let upstream =
            Upstream::with_connector("users-service", "http1", discovery, timeouts, connector);

        assert_eq!(upstream.id(), "users-service");
        assert_eq!(upstream.protocol(), "http1");

        // 1. First acquire using default protocol -> Miss
        let lease1 = upstream.acquire().await.unwrap();
        assert_eq!(lease1.endpoint(), ep1);
        assert_eq!(upstream.pool_stats().misses, 1);
        assert_eq!(upstream.pool_stats().hits, 0);

        // Release connection into pool
        lease1.release(true);
        assert_eq!(upstream.pool_stats().releases, 1);

        // 2. Second acquire -> Hit!
        let lease2 = upstream.acquire().await.unwrap();
        assert_eq!(lease2.endpoint(), ep1);
        assert_eq!(upstream.pool_stats().hits, 1);
        lease2.release(true);
    }

    #[tokio::test]
    async fn test_upstream_connection_failure_fallback() {
        let ep1: SocketAddr = "10.0.0.1:8080".parse().unwrap();
        let ep2: SocketAddr = "10.0.0.2:8080".parse().unwrap();

        let discovery = Discovery::new_explicit(vec![
            Endpoint::new("e1", ep1, 1),
            Endpoint::new("e2", ep2, 1),
        ]);
        let connector = Arc::new(MockConnector::new());
        connector.set_failing(ep1);

        let timeouts = UpstreamTimeouts::tcp(Duration::from_millis(500), Duration::from_secs(30));
        let upstream = Upstream::with_connector("test-up", "tcp", discovery, timeouts, connector);

        // Should attempt ep1, fail, and fallback to ep2!
        let lease = upstream.acquire().await.unwrap();
        assert_eq!(lease.endpoint(), ep2);
    }

    #[tokio::test]
    async fn test_upstream_iphash_deterministic_fallback() {
        use velda_lb::IpHash;

        let ep1: SocketAddr = "10.0.0.1:8080".parse().unwrap();
        let ep2: SocketAddr = "10.0.0.2:8080".parse().unwrap();

        let discovery = Discovery::new_explicit(vec![
            Endpoint::new("e1", ep1, 1),
            Endpoint::new("e2", ep2, 1),
        ]);
        let connector = Arc::new(MockConnector::new());

        // We determine which endpoint IpHash selects first for client IP 192.168.1.100
        let client_ip: SocketAddr = "192.168.1.100:50000".parse().unwrap();
        let target = AcquireTarget::default_target()
            .with_selection_context(SelectionContext::with_client_ip(client_ip));

        let timeouts = UpstreamTimeouts::tcp(Duration::from_millis(500), Duration::from_secs(30));
        let upstream = Upstream::new(
            "iphash-up",
            "tcp",
            discovery,
            IpHash,
            timeouts,
            Arc::clone(&connector),
        );

        // Check which one IpHash picks when healthy
        let lease = upstream.acquire_with_target(target.clone()).await.unwrap();
        let first_chosen = lease.endpoint();
        let fallback_expected = if first_chosen == ep1 { ep2 } else { ep1 };
        lease.release(false); // Do not pool connection, forcing fresh connect attempt next time

        // Now set the primary chosen endpoint to fail
        connector.set_failing(first_chosen);

        // Acquire must NOT fail or retry the dead node blindly; it must fall back to the survivor!
        let lease2 = upstream.acquire_with_target(target).await.unwrap();
        assert_eq!(lease2.endpoint(), fallback_expected);
    }

    #[tokio::test]
    async fn test_upstream_sweep_idle_and_maintenance() {
        let ep1: SocketAddr = "10.0.0.1:8080".parse().unwrap();
        let ep2: SocketAddr = "10.0.0.2:8080".parse().unwrap();

        let discovery = Discovery::new_explicit(vec![
            Endpoint::new("e1", ep1, 1),
            Endpoint::new("e2", ep2, 1),
        ]);
        let connector = Arc::new(MockConnector::new());
        let timeouts = UpstreamTimeouts::tcp(Duration::from_millis(500), Duration::from_millis(10));
        let health_config = HealthConfig::passive_only(1, Duration::from_secs(10));
        let upstream = Arc::new(
            Upstream::with_connector("sweep-up", "tcp", discovery.clone(), timeouts, connector)
                .with_health_config(health_config),
        );

        // Acquire and release into pool
        let lease = upstream.acquire().await.unwrap();
        lease.release(true);

        // Sleep to exceed idle timeout (10ms)
        tokio::time::sleep(Duration::from_millis(15)).await;

        // Sweep idle connections
        let (evicted, _) = upstream.sweep_idle();
        assert_eq!(evicted, 1);

        // Populate health record for ep2
        upstream.health().record_failure(&ep2);
        assert!(upstream.health().has_record(&ep2));

        // Prune retired endpoints
        discovery.update_endpoints(vec![Endpoint::new("e1", ep1, 1)], 2);
        upstream.prune_retired_endpoints();
        assert!(!upstream.health().has_record(&ep2));
    }
}
