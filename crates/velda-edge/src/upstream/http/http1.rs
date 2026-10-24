//! Layer 7 HTTP/1.1 Upstream managing endpoint selection, connection establishment, and pipe forwarding.

use std::net::SocketAddr;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::time::{Duration, Instant};

use velda_connection_pool::{PoolConfig, PoolManager, PoolableResource};
use velda_core::StreamingMode;
use velda_http1::Http1PipeStrategy;
pub use velda_http1::UpstreamHttp1Stream;
use velda_tls::TlsClientEngine;

use crate::error::EdgeError;
use crate::upstream::lb::EdgeUpstream;

/// Pooled HTTP/1.1 client connection resource with pre-allocated wire buffers.
#[derive(Debug)]
pub struct Http1ClientResource {
    pub stream: UpstreamHttp1Stream,
    pub read_buf: bytes::BytesMut,
    pub write_buf: bytes::BytesMut,
    pub created_at: Instant,
    pub last_used_at: Instant,
}

impl PoolableResource for Http1ClientResource {
    fn is_healthy(&self) -> bool {
        // Fast-path: Sockets utilized within the last 500ms are actively hot in the kernel TCP stack.
        // Bypasses the try_read() syscall completely (0 syscalls, pure RAM < 10ns).
        // Only probes sockets that have sat idle for >= 500ms.
        if self.last_used_at.elapsed() < Duration::from_millis(500) {
            return true;
        }
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
/// Automatically returns healthy connections and pre-warmed buffers to the pool on drop,
/// or closes them if marked as non-reusable (e.g. after stream errors or `Connection: close`).
pub struct Http1Lease {
    stream: Option<UpstreamHttp1Stream>,
    pub read_buf: bytes::BytesMut,
    pub write_buf: bytes::BytesMut,
    pub endpoint: SocketAddr,
    pool: Arc<PoolManager<SocketAddr, Http1ClientResource>>,
    reusable: bool,
    pub is_reused: bool,
    created_at: Instant,
    pub proxy_buffer_size: usize,
}

impl Http1Lease {
    /// Marks the leased connection as dirty/closed, preventing its return to the pool.
    #[inline]
    pub fn mark_closed(&mut self) {
        self.reusable = false;
    }

    /// Borrows the active stream handle and pooled read buffer simultaneously.
    #[inline]
    pub fn split_read(&mut self) -> (&mut UpstreamHttp1Stream, &mut bytes::BytesMut) {
        (
            self.stream.as_mut().expect("stream must be active"),
            &mut self.read_buf,
        )
    }

    /// Borrows the active stream handle and pooled write buffer simultaneously.
    #[inline]
    pub fn split_write(&mut self) -> (&mut UpstreamHttp1Stream, &mut bytes::BytesMut) {
        (
            self.stream.as_mut().expect("stream must be active"),
            &mut self.write_buf,
        )
    }

    /// Borrows the active stream handle, pooled read buffer, and write buffer simultaneously.
    #[inline]
    pub fn split_all(
        &mut self,
    ) -> (
        &mut UpstreamHttp1Stream,
        &mut bytes::BytesMut,
        &mut bytes::BytesMut,
    ) {
        (
            self.stream.as_mut().expect("stream must be active"),
            &mut self.read_buf,
            &mut self.write_buf,
        )
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
            let mut read_buf = std::mem::take(&mut self.read_buf);
            let mut write_buf = std::mem::take(&mut self.write_buf);
            read_buf.clear();
            if read_buf.capacity() > 16384 {
                read_buf = bytes::BytesMut::with_capacity(4096);
            }
            write_buf.clear();
            if write_buf.capacity() < 4096 || write_buf.capacity() > 16384 {
                write_buf = bytes::BytesMut::with_capacity(4096);
            }
            let res = Http1ClientResource {
                stream,
                read_buf,
                write_buf,
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
    /// [PRE-COMPILED]: Pre-computed HTTP/1.1 protocol engine configuration.
    pub config: velda_http1::config::Http1Config,
    /// Configured buffer capacity for reading upstream responses.
    pub proxy_buffer_size: usize,
}

impl std::fmt::Debug for Http1Upstream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Http1Upstream")
            .field("id", &self.inner.id())
            .field("tls_sni", &self.tls.as_ref().map(|(_, sni)| sni))
            .field("streaming", &self.streaming)
            .field("strategy", &self.strategy)
            .field("acceleration", &self.acceleration)
            .field("proxy_buffer_size", &self.proxy_buffer_size)
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
        buffer_config: Option<&velda_sync::post_sync::upstream::UpstreamBufferConfig>,
    ) -> Self {
        let strategy = if let Some(buf) = buffer_config
            && let Some(buffering) = buf.proxy_buffering
        {
            if buffering {
                Http1PipeStrategy::Buffered
            } else {
                Http1PipeStrategy::from_streaming(streaming)
            }
        } else {
            Http1PipeStrategy::from_streaming(streaming)
        };
        let mut config = velda_http1::config::Http1Config::auto();
        let mut proxy_buffer_size = 32768;
        if let Some(buf) = buffer_config {
            if let Some(pbs) = buf.proxy_buffer_size {
                proxy_buffer_size = pbs;
                config.initial_buffer_capacity = pbs;
            }
            if let Some(cbbs) = buf.client_body_buffer_size {
                config.client_body_buffer_size = cbbs;
            }
            if let Some(mbs) = buf.max_body_size {
                config.max_body_size = mbs;
            }
        }
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
            config,
            proxy_buffer_size,
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

        let (stream, read_buf, write_buf, endpoint, created_at, is_reused) = self
            .inner
            .execute(|endpoint| async move {
                if let Some(res) = pool.acquire_with_lifetime(&endpoint, idle_timeout, None) {
                    return Ok::<_, String>((
                        res.stream,
                        res.read_buf,
                        res.write_buf,
                        endpoint,
                        res.created_at,
                        true,
                    ));
                }

                // Under high burst concurrency, in-flight upstream requests typically return
                // their leased connection within 0.5-1ms. Yield once to let completed tasks
                // recycle connections before initiating an expensive TCP handshake.
                tokio::task::yield_now().await;

                if let Some(res) = pool.acquire_with_lifetime(&endpoint, idle_timeout, None) {
                    return Ok::<_, String>((
                        res.stream,
                        res.read_buf,
                        res.write_buf,
                        endpoint,
                        res.created_at,
                        true,
                    ));
                }

                let stream = velda_http1::client::connect(
                    endpoint,
                    tls,
                    Some(&acceleration),
                    Some(connect_timeout),
                )
                .await
                .map_err(|e| e.to_string())?;

                Ok::<_, String>((
                    stream,
                    bytes::BytesMut::with_capacity(4096),
                    bytes::BytesMut::with_capacity(4096),
                    endpoint,
                    Instant::now(),
                    false,
                ))
            })
            .await
            .map_err(EdgeError::Upstream)?;

        Ok(Http1Lease {
            stream: Some(stream),
            read_buf,
            write_buf,
            endpoint,
            pool: Arc::clone(&self.pool),
            reusable: true,
            is_reused,
            created_at,
            proxy_buffer_size: self.proxy_buffer_size,
        })
    }

    /// Connects a brand new HTTP/1.1 upstream stream directly, bypassing the keep-alive pool.
    ///
    /// Used for transparent 1-shot self-healing when a reused connection encounters a stale keep-alive race.
    pub async fn acquire_fresh(&self) -> Result<Http1Lease, EdgeError> {
        let tls = self
            .tls
            .as_ref()
            .map(|(engine, sni)| (engine.as_ref(), sni.as_ref()));
        let acceleration = self.acceleration;
        let connect_timeout = self.inner.timeouts().connect;

        let (stream, endpoint) = self
            .inner
            .execute(|endpoint| async move {
                let stream = velda_http1::client::connect(
                    endpoint,
                    tls,
                    Some(&acceleration),
                    Some(connect_timeout),
                )
                .await
                .map_err(|e| e.to_string())?;

                Ok::<_, String>((stream, endpoint))
            })
            .await
            .map_err(EdgeError::Upstream)?;

        Ok(Http1Lease {
            stream: Some(stream),
            read_buf: bytes::BytesMut::with_capacity(4096),
            write_buf: bytes::BytesMut::with_capacity(4096),
            endpoint,
            pool: Arc::clone(&self.pool),
            reusable: true,
            is_reused: false,
            created_at: Instant::now(),
            proxy_buffer_size: self.proxy_buffer_size,
        })
    }

    /// Returns the number of idle keep-alive connections for `endpoint` in `lane`.
    #[inline]
    pub fn idle_conns_for_lane(&self, endpoint: &SocketAddr, lane: usize) -> usize {
        self.pool.idle_conns_for_lane(endpoint, lane)
    }

    /// Pre-warms idle keep-alive connections for the specified worker lane.
    ///
    /// Executed autonomously by the core-local Shadow Worker (`SCHED_IDLE`) to ensure
    /// the frontline traffic worker always encounters ready connections in its lane.
    pub async fn prewarm_lane(&self, lane: usize, min_idle: usize) {
        let endpoints = self.inner.current_endpoints();
        let all_eps = endpoints.all_endpoints();
        if all_eps.is_empty() {
            return;
        }

        for ep in all_eps {
            let addr = ep.address;
            if !self.inner.health().is_healthy(&addr) {
                continue;
            }

            let idle = self.pool.idle_conns_for_lane(&addr, lane);
            if idle < min_idle {
                let needed = min_idle - idle;
                for _ in 0..needed {
                    let tls_ref = self
                        .tls
                        .as_ref()
                        .map(|(engine, sni)| (engine.as_ref(), sni.as_ref()));
                    let connect_timeout = self.inner.timeouts().connect;

                    match velda_http1::client::connect(
                        addr,
                        tls_ref,
                        Some(&self.acceleration),
                        Some(connect_timeout),
                    )
                    .await
                    {
                        Ok(stream) => {
                            let res = Http1ClientResource {
                                stream,
                                read_buf: bytes::BytesMut::with_capacity(4096),
                                write_buf: bytes::BytesMut::with_capacity(4096),
                                created_at: Instant::now(),
                                last_used_at: Instant::now(),
                            };
                            self.pool.release_to_lane(&addr, lane, res);
                            tracing::debug!(endpoint = %addr, lane, "Shadow Worker pre-warmed HTTP/1.1 connection");
                        }
                        Err(e) => {
                            tracing::debug!(endpoint = %addr, lane, error = %e, "Shadow Worker pre-warm connect failed for HTTP/1.1");
                            break;
                        }
                    }
                }
            }
        }
    }
}
