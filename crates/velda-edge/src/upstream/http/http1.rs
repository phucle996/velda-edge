//! Layer 7 HTTP/1.1 Upstream managing endpoint selection, connection establishment, and pipe forwarding.

use std::net::SocketAddr;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};

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

/// Idle age threshold below which a recently used socket is considered healthy without probing syscalls.
const RECENT_USAGE_HEALTHY_THRESHOLD: Duration = Duration::from_millis(500);

impl PoolableResource for Http1ClientResource {
    fn is_healthy(&self) -> bool {
        // Fast-path: Sockets utilized within the last 500ms are actively hot in the kernel TCP stack.
        // Bypasses the try_read() syscall completely (0 syscalls, pure RAM lookup).
        // Only probes sockets that have sat idle for >= 500ms.
        if self.last_used_at.elapsed() < RECENT_USAGE_HEALTHY_THRESHOLD {
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

/// RAII guard that automatically marks an [`Http1Lease`] closed if dropped before explicit commit.
pub struct LeaseGuard(Option<Http1Lease>);

impl LeaseGuard {
    /// Wraps an active lease in an auto-closing cancellation guard.
    #[inline]
    pub fn new(lease: Http1Lease) -> Self {
        Self(Some(lease))
    }

    /// Returns whether this lease was acquired from the pool as a reused connection.
    #[inline]
    pub fn is_reused(&self) -> bool {
        self.0.as_ref().expect("lease must be active").is_reused
    }

    /// Replaces the current lease with a fresh one, safely marking the old lease closed.
    #[inline]
    pub fn replace(&mut self, fresh: Http1Lease) {
        if let Some(mut old) = self.0.take() {
            old.mark_closed();
        }
        self.0 = Some(fresh);
    }

    /// Commits the transaction, disarming the guard and returning the clean, reusable lease.
    #[inline]
    pub fn commit(mut self) -> Http1Lease {
        self.0.take().expect("lease must be active")
    }
}

impl Deref for LeaseGuard {
    type Target = UpstreamHttp1Stream;

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.0.as_ref().expect("lease must be active").deref()
    }
}

impl DerefMut for LeaseGuard {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0.as_mut().expect("lease must be active").deref_mut()
    }
}

impl Drop for LeaseGuard {
    fn drop(&mut self) {
        if let Some(mut lease) = self.0.take() {
            lease.mark_closed();
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
    /// Last active health probe timestamp in seconds from coarse integer clock.
    last_probe_sec: AtomicU64,
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
            last_probe_sec: AtomicU64::new(0),
        }
    }

    /// Returns the upstream unique identifier.
    #[inline]
    pub fn id(&self) -> &str {
        self.inner.id()
    }

    /// Returns whether the specified backend endpoint is healthy.
    #[inline]
    pub fn is_healthy(&self, addr: &SocketAddr) -> bool {
        self.inner.health().is_healthy(addr)
    }

    /// Returns the request timeout for upstream HTTP/1.1 exchanges (defaults to 60s if unspecified).
    #[inline]
    pub fn request_timeout(&self) -> std::time::Duration {
        self.inner
            .timeouts()
            .request
            .unwrap_or(std::time::Duration::from_secs(60))
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

    /// Performs periodic active health check across all physical endpoints for `lane`.
    ///
    /// Evaluates interval using the lock-free coarse clock (`coarse_now_sec`) in ~0.5ns.
    /// Employs non-biased random sampling (Lemire's FastRange) on idle connections in pool
    /// to avoid zombie connections, bounds I/O with strict hard timeouts, and binds outbound
    /// probe sockets to the dedicated 60k port range (`60000 + (lane & 0x3FF)`).
    pub async fn check_active_health(&self, lane: usize, seed: u32) {
        let health = self.inner.health();
        let active_cfg = match health.config().active.as_ref() {
            Some(a) if a.enabled => a,
            _ => return,
        };

        let now_sec = velda_core::coarse_now_sec();
        let interval_sec = active_cfg.interval.as_secs().max(1);
        let last = self.last_probe_sec.load(Ordering::Relaxed);
        if now_sec < last + interval_sec {
            return;
        }
        self.last_probe_sec.store(now_sec, Ordering::Relaxed);

        let endpoints = self.inner.current_endpoints();
        let all_eps = endpoints.all_endpoints();
        if all_eps.is_empty() {
            return;
        }

        for ep in all_eps {
            let addr = ep.address;
            let success = self.probe_endpoint(&addr, active_cfg, lane, seed).await;
            health.record_active_result(&addr, success);
        }
    }

    async fn probe_endpoint(
        &self,
        addr: &SocketAddr,
        active_cfg: &velda_upstream::ActiveHealthConfig,
        lane: usize,
        seed: u32,
    ) -> bool {
        let timeout = active_cfg.timeout;

        // 1. Lightweight L4 TCP probe if configured
        if active_cfg.check_type.eq_ignore_ascii_case("tcp") {
            let socket_res = if addr.is_ipv4() {
                tokio::net::TcpSocket::new_v4()
            } else {
                tokio::net::TcpSocket::new_v6()
            };
            let Ok(socket) = socket_res else { return false };
            let _ = socket.set_reuseport(true);
            let _ = socket.set_reuseaddr(true);
            let local_port = 60000 + (lane as u16 & 0x3FF);
            let bind_addr = if addr.is_ipv4() {
                SocketAddr::new(
                    std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED),
                    local_port,
                )
            } else {
                SocketAddr::new(
                    std::net::IpAddr::V6(std::net::Ipv6Addr::UNSPECIFIED),
                    local_port,
                )
            };
            let _ = socket.bind(bind_addr);

            let connect_fut = socket.connect(*addr);
            match tokio::time::timeout(timeout, connect_fut).await {
                Ok(Ok(stream)) => {
                    // Safe teardown: only shutdown when zero client bytes transferred
                    let _ = stream
                        .into_std()
                        .and_then(|s| s.shutdown(std::net::Shutdown::Both));
                    true
                }
                _ => false,
            }
        } else {
            // 2. L7 HTTP/1.1 Probe
            let idle_timeout = self.inner.timeouts().idle;
            let sampled = self
                .pool
                .acquire_random_from_lane(addr, lane, idle_timeout, seed);

            let (mut stream, was_reused) = match sampled {
                Some(res) => (res.stream, true),
                None => {
                    let local_port = 60000 + (lane as u16 & 0x3FF);
                    let tls_ref = self.tls.as_ref().map(|(e, s)| (e.as_ref(), s.as_ref()));
                    match velda_http1::client::connect_with_bind(
                        *addr,
                        Some(local_port),
                        tls_ref,
                        Some(&self.acceleration),
                        Some(timeout),
                    )
                    .await
                    {
                        Ok(s) => (s, false),
                        Err(_) => return false,
                    }
                }
            };

            let path = active_cfg.path.as_deref().unwrap_or("/healthz");
            let host = self
                .tls
                .as_ref()
                .map(|(_, sni)| sni.as_ref())
                .unwrap_or_else(|| self.inner.id());
            let probe_req = format!(
                "GET {path} HTTP/1.1\r\nHost: {host}\r\nUser-Agent: Velda-HealthCheck\r\nConnection: keep-alive\r\n\r\n"
            );

            let io_fut = async {
                stream.write_all(probe_req.as_bytes()).await.ok()?;
                let mut buf = [0u8; 64];
                let n = stream.read(&mut buf).await.ok()?;
                if n >= 12 && &buf[..8] == b"HTTP/1.1" {
                    let status = std::str::from_utf8(&buf[9..12]).ok()?.parse::<u16>().ok()?;
                    Some(status)
                } else {
                    None
                }
            };

            match tokio::time::timeout(timeout, io_fut).await {
                Ok(Some(status))
                    if active_cfg.expected_statuses.is_empty()
                        || active_cfg.expected_statuses.contains(&status) =>
                {
                    // Success: Deposit healthy connection into pool as pre-warmed resource
                    let res = Http1ClientResource {
                        stream,
                        read_buf: bytes::BytesMut::with_capacity(4096),
                        write_buf: bytes::BytesMut::with_capacity(4096),
                        created_at: Instant::now(),
                        last_used_at: Instant::now(),
                    };
                    self.pool.release_to_lane(addr, lane, res);
                    true
                }
                _ => {
                    // Safe teardown: only shutdown when zero client bytes are transferred
                    let _ = stream.shutdown().await;
                    if was_reused {
                        self.pool.drain_endpoint_from_lane(addr, lane);
                    }
                    false
                }
            }
        }
    }
}
