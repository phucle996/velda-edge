//! Layer 7 HTTP/1.1 Upstream managing endpoint selection, connection establishment, and pipe forwarding.

use std::net::SocketAddr;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::time::Instant;

use velda_connection_pool::{PoolConfig, PoolManager, PoolableResource};
use velda_core::StreamingMode;
pub use velda_http1::UpstreamHttp1Stream;
use velda_http1::pipe::Http1PipeStrategy;
use velda_tls::TlsClientEngine;

use super::lb::EdgeUpstream;
use crate::error::EdgeError;

/// Pooled HTTP/1.1 client connection resource.
#[derive(Debug)]
pub struct Http1ClientResource {
    pub stream: UpstreamHttp1Stream,
    pub created_at: Instant,
    pub last_used_at: Instant,
}

impl PoolableResource for Http1ClientResource {
    fn is_healthy(&self) -> bool {
        self.stream.is_healthy()
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

/// RAII lease guard for an acquired HTTP/1.1 upstream stream.
///
/// Automatically returns healthy connections to the pool on drop,
/// or closes them if marked as non-reusable (e.g. after stream errors or `Connection: close`).
pub struct Http1Lease {
    stream: Option<UpstreamHttp1Stream>,
    pub endpoint: SocketAddr,
    pool: Arc<PoolManager<SocketAddr, Http1ClientResource>>,
    reusable: bool,
    created_at: Instant,
}

impl Http1Lease {
    /// Marks the leased connection as dirty/closed, preventing its return to the pool.
    #[inline]
    pub fn mark_closed(&mut self) {
        self.reusable = false;
    }
}

impl Deref for Http1Lease {
    type Target = UpstreamHttp1Stream;

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.stream.as_ref().expect("stream must be active")
    }
}

impl DerefMut for Http1Lease {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.stream.as_mut().expect("stream must be active")
    }
}

impl Drop for Http1Lease {
    fn drop(&mut self) {
        if !self.reusable {
            return;
        }
        if let Some(stream) = self.stream.take() {
            let res = Http1ClientResource {
                stream,
                created_at: self.created_at,
                last_used_at: Instant::now(),
            };
            self.pool.release(&self.endpoint, res, true, false);
        }
    }
}

/// Layer 7 HTTP/1.1 Upstream managing endpoint selection, connection establishment, and pipe forwarding.
///
/// Pre-compiled with static TLS engine, load balancer, sequential keep-alive pool, and streaming strategy.
pub struct Http1Upstream {
    /// [PRE-COMPILED]: Pre-assembled upstream core holding discovery, health tracker,
    /// timeouts, and the selected `LbAlgorithm` enum variant.
    inner: EdgeUpstream,
    /// Lock-sharded persistent HTTP/1.1 sequential client connection pool for keep-alive reuse.
    pool: Arc<PoolManager<SocketAddr, Http1ClientResource>>,
    /// [PRE-COMPILED]: Upstream TLS identity: client engine plus the static target SNI.
    /// `None` for plaintext upstreams. A TLS upstream without SNI is rejected at compile time.
    tls: Option<(Arc<TlsClientEngine>, Arc<str>)>,
    /// Declarative streaming mode from upstream configuration.
    pub streaming: StreamingMode,
    /// [PRE-COMPILED]: Pre-computed wire forwarding strategy (`Http1PipeStrategy`) derived from
    /// `streaming` at compile time; enables $O(1)$ zero-cost dispatch on the hot path.
    pub strategy: Http1PipeStrategy,
    /// [PRE-COMPILED]: Protocol-specific socket acceleration path.
    pub acceleration: velda_http1::Http1AccelerationPath,
}

impl std::fmt::Debug for Http1Upstream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Http1Upstream")
            .field("id", &self.inner.id())
            .field("tls_sni", &self.tls.as_ref().map(|(_, sni)| sni))
            .field("streaming", &self.streaming)
            .field("strategy", &self.strategy)
            .field("acceleration", &self.acceleration)
            .finish()
    }
}

impl Http1Upstream {
    /// Creates a new pre-compiled [`Http1Upstream`].
    pub fn new(
        inner: EdgeUpstream,
        tls: Option<(Arc<TlsClientEngine>, Arc<str>)>,
        streaming: StreamingMode,
        pool_config: PoolConfig,
        shard_count: usize,
        acceleration: velda_http1::Http1AccelerationPath,
    ) -> Self {
        let strategy = Http1PipeStrategy::from_streaming(streaming);
        let pool = Arc::new(PoolManager::with_config_and_shards(
            pool_config,
            shard_count,
        ));
        Self {
            inner,
            pool,
            tls,
            streaming,
            strategy,
            acceleration,
        }
    }

    /// Returns the upstream unique identifier.
    #[inline]
    pub fn id(&self) -> &str {
        self.inner.id()
    }

    /// Acquires a pooled or fresh HTTP/1.1 upstream stream wrapped in an RAII [`Http1Lease`].
    ///
    /// Manages single-round Load Balancer selection, idle connection reuse (HIT),
    /// fresh TCP/TLS connection establishment (MISS), and automatic candidate failover.
    pub async fn acquire(&self) -> Result<Http1Lease, EdgeError> {
        let tls = self
            .tls
            .as_ref()
            .map(|(engine, sni)| (engine.as_ref(), sni.as_ref()));
        let acceleration = self.acceleration;
        let connect_timeout = self.inner.timeouts().connect;
        let idle_timeout = self.inner.timeouts().idle;
        let pool = &self.pool;

        let (stream, endpoint, created_at) = self
            .inner
            .execute(|endpoint| async move {
                while let Some(res) = pool.acquire_with_lifetime(&endpoint, idle_timeout, None) {
                    if res.is_healthy() {
                        return Ok::<_, String>((res.stream, endpoint, res.created_at));
                    }
                }

                let stream = velda_http1::client::connect_stream(
                    endpoint,
                    tls,
                    Some(&acceleration),
                    Some(connect_timeout),
                )
                .await
                .map_err(|e| e.to_string())?;

                Ok::<_, String>((stream, endpoint, Instant::now()))
            })
            .await
            .map_err(EdgeError::Upstream)?;

        Ok(Http1Lease {
            stream: Some(stream),
            endpoint,
            pool: Arc::clone(&self.pool),
            reusable: true,
            created_at,
        })
    }
}
