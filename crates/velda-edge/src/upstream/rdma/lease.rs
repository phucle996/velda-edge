//! # RDMA Pooled Client Connection Resource and RAII Lease Guard
//!
//! Provides `RdmaClientResource` for core-lane pooling and `RdmaLease` for RAII lease management.

use std::net::SocketAddr;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use velda_connection_pool::{PoolManager, PoolableResource};
use velda_rdma::channel::{RdmaReadHalf, RdmaReceiver, RdmaSender, RdmaWriteHalf};
use velda_rdma::{RdmaChannel, RdmaStream};

/// Pooled RDMA client connection resource.
#[derive(Clone)]
pub struct RdmaClientResource {
    pub channel: RdmaChannel,
    pub created_at: Instant,
    pub last_used_at: Instant,
}

impl PoolableResource for RdmaClientResource {
    fn is_healthy(&self) -> bool {
        self.channel.is_healthy()
    }

    fn close(&mut self) {
        self.channel.mark_broken();
    }

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
    pub(crate) channel: Option<RdmaChannel>,
    pub(crate) endpoint: SocketAddr,
    pub(crate) pool: Arc<PoolManager<SocketAddr, RdmaClientResource>>,
    pub(crate) reusable: AtomicBool,
    pub is_reused: bool,
    pub(crate) created_at: Instant,
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
            if !channel.is_healthy() {
                // Drop without returning to pool if channel experienced errors or timeouts
                return;
            }
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
    /// Constructs a new `RdmaLease`.
    pub fn new(
        channel: RdmaChannel,
        endpoint: SocketAddr,
        pool: Arc<PoolManager<SocketAddr, RdmaClientResource>>,
        is_reused: bool,
        created_at: Instant,
    ) -> Self {
        Self {
            channel: Some(channel),
            endpoint,
            pool,
            reusable: AtomicBool::new(true),
            is_reused,
            created_at,
        }
    }

    /// Marks this channel as non-reusable (e.g. on fatal hardware error).
    pub fn mark_broken(&self) {
        self.reusable.store(false, Ordering::Release);
    }

    /// Splits the leased RDMA channel into decoupled sender and receiver handles.
    pub fn split(&self) -> (RdmaSender, RdmaReceiver) {
        self.channel
            .as_ref()
            .expect("RdmaLease channel missing")
            .split()
    }

    /// Converts this lease into a streaming adapter implementing `AsyncRead + AsyncWrite`.
    pub fn into_stream(mut self) -> RdmaStream {
        let channel = self.channel.take().expect("RdmaLease channel missing");
        self.reusable.store(false, Ordering::Release); // Owned by stream
        RdmaStream::new(channel)
    }

    /// Converts this lease into decoupled streaming read and write halves.
    pub fn into_split_stream(self) -> (RdmaReadHalf, RdmaWriteHalf) {
        self.into_stream().into_split()
    }
}
