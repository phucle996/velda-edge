//! # Pre-Compiled RDMA Upstream Processor
//!
//! Owns connection lifecycle, endpoint topology, and core lane pre-warming ("WHEN & WHO").

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Instant;

use velda_connection_pool::{PoolConfig, PoolManager};
use velda_rdma::{
    RdmaChannel, RdmaConfig, RdmaConnector, RdmaError, RdmaSlabArena, RdmaSlabClass,
    RdmaSlabConfig, RdmaSlabLease, RdmaStream,
};

use crate::error::EdgeError;
use crate::upstream::lb::EdgeUpstream;
use crate::upstream::rdma::lease::{RdmaClientResource, RdmaLease};

/// Pre-compiled RDMA Upstream managing kernel-bypass connections, pooling, and core lane pre-warming.
pub struct RdmaUpstream {
    /// Pre-assembled upstream core holding discovery, health tracking, timeouts, and LB algorithm.
    pub inner: EdgeUpstream,
    /// RDMA hardware and transport configuration.
    pub config: RdmaConfig,
    /// Core-lane sharded connection pool.
    pub pool: Arc<PoolManager<SocketAddr, RdmaClientResource>>,
    /// Pre-registered zero-copy slab memory arena.
    pub arena: Arc<RdmaSlabArena>,
}

impl std::fmt::Debug for RdmaUpstream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RdmaUpstream")
            .field("id", &self.id())
            .finish()
    }
}

impl RdmaUpstream {
    /// Constructs a new pre-compiled RDMA upstream processor.
    pub fn new(inner: EdgeUpstream, config: RdmaConfig, pool_config: PoolConfig) -> Self {
        let pool = Arc::new(PoolManager::with_config(pool_config));
        let arena = Arc::new(
            RdmaSlabArena::new_auto(&config, RdmaSlabConfig::default()).unwrap_or_else(|e| {
                if config.force_mock || cfg!(test) {
                    RdmaSlabArena::new_mock(RdmaSlabConfig::default())
                        .expect("Failed to initialize mock RDMA slab arena")
                } else {
                    panic!(
                        "FATAL: Failed to initialize hardware RDMA slab arena in production: {e}"
                    );
                }
            }),
        );
        Self {
            inner,
            config,
            pool,
            arena,
        }
    }

    /// Constructs an RDMA upstream with an explicitly configured slab arena.
    pub fn with_arena(
        inner: EdgeUpstream,
        config: RdmaConfig,
        pool_config: PoolConfig,
        arena: Arc<RdmaSlabArena>,
    ) -> Self {
        let pool = Arc::new(PoolManager::with_config(pool_config));
        Self {
            inner,
            config,
            pool,
            arena,
        }
    }

    /// Leases a zero-copy slot directly from the upstream RDMA slab arena.
    #[inline(always)]
    pub fn lease_slab(&self, class: RdmaSlabClass) -> Result<RdmaSlabLease, RdmaError> {
        self.arena.acquire(class)
    }

    /// Leases a zero-copy slot sized for `size` bytes from the upstream RDMA slab arena.
    #[inline(always)]
    pub fn lease_slab_for_size(&self, size: usize) -> Result<RdmaSlabLease, RdmaError> {
        self.arena.acquire_for_size(size)
    }

    /// Upstream unique identifier.
    #[inline(always)]
    pub fn id(&self) -> &str {
        self.inner.id()
    }

    /// Hot-Path: Acquires an RDMA channel lease with O(1) local core lane reuse without cross-thread synchronization.
    pub async fn acquire(&self) -> Result<RdmaLease, EdgeError> {
        let pool = Arc::clone(&self.pool);
        let config = self.config.clone();
        let idle_timeout = self.inner.timeouts().idle;
        let arena_pd = self.arena.pd();

        let (channel, endpoint, created_at, is_reused) = self
            .inner
            .execute(move |endpoint| {
                let pool = Arc::clone(&pool);
                let config = config.clone();
                let arena_pd = arena_pd.clone();
                async move {
                    if let Some(res) = pool.acquire_with_lifetime(&endpoint, idle_timeout, None) {
                        return Ok::<_, String>((res.channel, endpoint, res.created_at, true));
                    }

                    // Share the Protection Domain from RdmaSlabArena so zero-copy slab sends
                    // and QP share identical memory protection credentials (eliminating IBV_WC_LOC_PROT_ERR).
                    let fresh_channel = RdmaConnector::connect_with_pd(endpoint, &config, arena_pd)
                        .await
                        .map_err(|e| e.to_string())?;

                    Ok::<_, String>((fresh_channel, endpoint, Instant::now(), false))
                }
            })
            .await
            .map_err(EdgeError::Upstream)?;

        Ok(RdmaLease {
            channel: Some(channel),
            endpoint,
            pool: Arc::clone(&self.pool),
            reusable: AtomicBool::new(true),
            is_reused,
            created_at,
        })
    }

    /// Returns the number of idle RDMA connections currently cached in a specific core lane.
    #[inline(always)]
    pub fn idle_conns_for_lane(&self, endpoint: &SocketAddr, lane: usize) -> usize {
        self.pool.idle_conns_for_lane(endpoint, lane)
    }

    /// Acquires a fresh, non-pooled RDMA channel for transparent 1-shot self-healing.
    pub async fn acquire_fresh(&self) -> Result<RdmaChannel, EdgeError> {
        let config = self.config.clone();
        let arena_pd = self.arena.pd();

        let (channel, _) = self
            .inner
            .execute(move |endpoint| {
                let config = config.clone();
                let arena_pd = arena_pd.clone();
                async move {
                    let fresh = RdmaConnector::connect_with_pd(endpoint, &config, arena_pd)
                        .await
                        .map_err(|e| e.to_string())?;

                    Ok::<_, String>((fresh, endpoint))
                }
            })
            .await
            .map_err(EdgeError::Upstream)?;

        Ok(channel)
    }

    /// Helper to acquire an RDMA channel and wrap it as an `AsyncRead + AsyncWrite` stream.
    pub async fn acquire_stream(&self) -> Result<RdmaStream, EdgeError> {
        let lease = self.acquire().await?;
        Ok(lease.into_stream())
    }

    /// Autonomous background pre-warming for a specific worker core lane (`lane = worker_id`).
    ///
    /// Invoked under `SCHED_IDLE` by the core-local Shadow Logistics Worker (`AGENTS.md` Sec 2.9).
    pub async fn prewarm_lane(&self, lane: usize, count: usize) {
        if let Ok(endpoint) = self.inner.select_endpoint() {
            let config = self.config.clone();
            let arena_pd = self.arena.pd();
            for _ in 0..count {
                if let Ok(channel) =
                    RdmaConnector::connect_with_pd(endpoint, &config, arena_pd.clone()).await
                {
                    self.pool.release_to_lane(
                        &endpoint,
                        lane,
                        RdmaClientResource {
                            channel,
                            created_at: Instant::now(),
                            last_used_at: Instant::now(),
                        },
                    );
                }
            }
        }
    }
}
