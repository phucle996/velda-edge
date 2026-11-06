//! Layer 4/7 RDMA Upstream managing hardware connection lifecycle, pooling, and core lane pre-warming.
//!
//! Conforms to `AGENTS.md` Section 2.8:
//! - `velda-upstream` owns connection lifecycle, pool state, and endpoint selection ("WHEN & WHO").
//! - `velda-rdma` owns verbs, QP mechanics, memory registration, and wire execution ("HOW").
//! - Section 2.9: Frontline Traffic Worker vs Shadow Logistics Invariant.

use std::net::SocketAddr;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use velda_connection_pool::{PoolConfig, PoolManager, PoolableResource};
use velda_rdma::{
    RdmaChannel, RdmaConfig, RdmaConnector, RdmaError, RdmaSlabArena, RdmaSlabClass,
    RdmaSlabConfig, RdmaSlabLease, RdmaStream,
};

use crate::error::EdgeError;
use crate::upstream::lb::EdgeUpstream;

/// Pooled RDMA client connection resource.
#[derive(Clone)]
pub struct RdmaClientResource {
    pub channel: RdmaChannel,
    pub created_at: Instant,
    pub last_used_at: Instant,
}

impl PoolableResource for RdmaClientResource {
    fn is_healthy(&self) -> bool {
        true
    }

    fn close(&mut self) {}

    fn created_at(&self) -> Instant {
        self.created_at
    }

    fn last_used_at(&self) -> Instant {
        self.last_used_at
    }

    fn touch(&mut self) {
        self.last_used_at = Instant::now();
    }

    fn touch_at(&mut self, now: Instant) {
        self.last_used_at = now;
    }
}

impl std::fmt::Debug for RdmaClientResource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RdmaClientResource")
            .field("created_at", &self.created_at)
            .finish()
    }
}

/// RAII lease guard for an acquired RDMA channel.
///
/// Automatically returns healthy channels to the connection pool on drop.
pub struct RdmaLease {
    channel: Option<RdmaChannel>,
    endpoint: SocketAddr,
    pool: Arc<PoolManager<SocketAddr, RdmaClientResource>>,
    reusable: AtomicBool,
    pub is_reused: bool,
    created_at: Instant,
}

impl Deref for RdmaLease {
    type Target = RdmaChannel;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        self.channel.as_ref().expect("RdmaLease channel missing")
    }
}

impl DerefMut for RdmaLease {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.channel.as_mut().expect("RdmaLease channel missing")
    }
}

impl Drop for RdmaLease {
    fn drop(&mut self) {
        if self.reusable.load(Ordering::Acquire)
            && let Some(channel) = self.channel.take()
        {
            self.pool.release(
                &self.endpoint,
                RdmaClientResource {
                    channel,
                    created_at: self.created_at,
                    last_used_at: Instant::now(),
                },
                true,
                false,
            );
        }
    }
}

impl RdmaLease {
    /// Marks this channel as non-reusable (e.g. on fatal hardware error).
    pub fn mark_broken(&self) {
        self.reusable.store(false, Ordering::Release);
    }

    /// Converts this lease into a streaming adapter implementing `AsyncRead + AsyncWrite`.
    pub fn into_stream(mut self) -> RdmaStream {
        let channel = self.channel.take().expect("RdmaLease channel missing");
        self.reusable.store(false, Ordering::Release); // Owned by stream
        RdmaStream::new(channel)
    }
}

/// Pre-compiled RDMA Upstream managing kernel-bypass connections, pooling, and core lane pre-warming.
pub struct RdmaUpstream {
    /// Pre-assembled upstream core holding discovery, health tracking, timeouts, and LB algorithm.
    pub inner: EdgeUpstream,
    /// RDMA hardware and transport configuration.
    pub config: RdmaConfig,
    /// Core-lane sharded connection pool.
    pool: Arc<PoolManager<SocketAddr, RdmaClientResource>>,
    /// Pre-registered zero-copy slab memory arena.
    arena: Arc<RdmaSlabArena>,
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
            RdmaSlabArena::new_auto(&config, RdmaSlabConfig::default()).unwrap_or_else(|_| {
                RdmaSlabArena::new_mock(RdmaSlabConfig::default())
                    .expect("Failed to initialize mock RDMA slab arena")
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

    /// Access to the pre-registered zero-copy RDMA slab memory arena.
    #[inline(always)]
    pub fn arena(&self) -> &Arc<RdmaSlabArena> {
        &self.arena
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

        let (channel, endpoint, created_at, is_reused) = self
            .inner
            .execute(move |endpoint| {
                let pool = Arc::clone(&pool);
                let config = config.clone();
                async move {
                    if let Some(res) = pool.acquire_with_lifetime(&endpoint, idle_timeout, None) {
                        return Ok::<_, String>((res.channel, endpoint, res.created_at, true));
                    }

                    let fresh_channel = RdmaConnector::connect(endpoint, &config)
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

        let (channel, _) = self
            .inner
            .execute(move |endpoint| {
                let config = config.clone();
                async move {
                    let fresh = RdmaConnector::connect(endpoint, &config)
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
            for _ in 0..count {
                if let Ok(channel) = RdmaConnector::connect(endpoint, &config).await {
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
