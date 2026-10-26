//! Layer 7 HTTP/2 Upstream managing persistent client multiplexing (RFC 9113) and pipe forwarding.

use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use velda_connection_pool::{MultiplexedPool, PoolableResource};
use velda_core::StreamingMode;
use velda_http2::Http2PipeStrategy;
use velda_tls::TlsClientEngine;

use crate::error::EdgeError;
use crate::upstream::lb::EdgeUpstream;

/// Pooled HTTP/2 client resource wrapping [`h2::client::SendRequest<Bytes>`].
#[derive(Clone, Debug)]
pub struct Http2ClientResource {
    pub client: h2::client::SendRequest<Bytes>,
}

impl Http2ClientResource {
    pub fn new(client: h2::client::SendRequest<Bytes>) -> Self {
        Self { client }
    }
}

impl PoolableResource for Http2ClientResource {
    fn is_healthy(&self) -> bool {
        true
    }

    fn close(&mut self) {}
}

/// Layer 7 HTTP/2 Upstream managing persistent client multiplexing (RFC 9113) and pipe forwarding.
///
/// Pre-compiled with static load balancer, wire pipe strategy, and sharded multiplexed client pool.
pub struct Http2Upstream {
    /// [PRE-COMPILED]: Pre-assembled upstream core holding discovery, health tracker,
    /// timeouts, and the selected `LbAlgorithm` enum variant.
    inner: EdgeUpstream,
    /// [PRE-COMPILED]: Upstream TLS identity: client engine plus the static target SNI.
    /// `None` for cleartext (h2c) upstreams. TLS without SNI is rejected at compile time.
    tls: Option<(Arc<TlsClientEngine>, Arc<str>)>,
    /// Declarative streaming mode from upstream configuration.
    pub streaming: StreamingMode,
    /// [PRE-COMPILED]: Pre-computed wire forwarding strategy (`Http2PipeStrategy`) derived from
    /// `streaming` at compile time; eliminates runtime enum branching.
    pub strategy: Http2PipeStrategy,
    /// Lock-sharded persistent HTTP/2 multiplexed client connection pool.
    pool: Arc<MultiplexedPool<SocketAddr, Http2ClientResource>>,
    /// Maximum parallel TCP connections maintained per physical endpoint.
    pub max_connections_per_key: usize,
    /// Maximum concurrent streams per multiplexed connection.
    pub max_concurrent_streams: u32,
    /// [PRE-COMPILED]: Protocol-specific socket acceleration path.
    pub acceleration: velda_http2::Http2AccelerationPath,
    /// [PRE-COMPILED]: Pre-computed HTTP/2 protocol configuration.
    pub config: velda_http2::Http2Config,
    /// Cold-start / saturation synchronization lock preventing thundering-herd connection storms.
    connect_lock: tokio::sync::Mutex<()>,
}

impl std::fmt::Debug for Http2Upstream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Http2Upstream")
            .field("id", &self.inner.id())
            .field("streaming", &self.streaming)
            .field("strategy", &self.strategy)
            .field("max_connections_per_key", &self.max_connections_per_key)
            .field("max_concurrent_streams", &self.max_concurrent_streams)
            .field("acceleration", &self.acceleration)
            .finish()
    }
}

impl Http2Upstream {
    /// Creates a new pre-compiled [`Http2Upstream`].
    pub fn new(
        inner: EdgeUpstream,
        tls: Option<(Arc<TlsClientEngine>, Arc<str>)>,
        streaming: StreamingMode,
        shard_count: usize,
        max_connections_per_key: usize,
        max_concurrent_streams: u32,
        acceleration: velda_http2::Http2AccelerationPath,
    ) -> Self {
        let strategy = Http2PipeStrategy::from_streaming(streaming);
        Self {
            inner,
            tls,
            streaming,
            strategy,
            pool: Arc::new(
                MultiplexedPool::with_shards(shard_count)
                    .with_max_connections_per_key(max_connections_per_key),
            ),
            max_connections_per_key: max_connections_per_key.max(1),
            max_concurrent_streams,
            acceleration,
            config: velda_http2::Http2Config::auto(),
            connect_lock: tokio::sync::Mutex::new(()),
        }
    }

    /// Returns the upstream unique identifier.
    #[inline]
    pub fn id(&self) -> &str {
        self.inner.id()
    }

    /// Returns the request timeout for upstream HTTP/2 exchanges (defaults to 60s if unspecified).
    #[inline]
    pub fn request_timeout(&self) -> std::time::Duration {
        self.inner
            .timeouts()
            .request
            .unwrap_or(std::time::Duration::from_secs(60))
    }

    /// Returns whether this upstream uses TLS.
    #[inline]
    pub fn is_tls(&self) -> bool {
        self.tls.is_some()
    }

    /// Returns the target host or SNI for HTTP/2 :authority routing.
    #[inline]
    pub fn target_host(&self) -> &str {
        if let Some((_, ref sni)) = self.tls {
            sni.as_ref()
        } else {
            self.inner.id()
        }
    }

    /// Acquires an active, ready multiplexed HTTP/2 client connection and lease from the pool or connects a fresh one.
    ///
    /// Distributes streams across parallel TCP connections and Tokio cores while keeping active stream
    /// accounting precise via the returned RAII lease guard.
    pub async fn acquire(
        &self,
        config: &velda_http2::Http2Config,
    ) -> Result<
        (
            h2::client::SendRequest<Bytes>,
            velda_connection_pool::StreamLease<Http2ClientResource>,
        ),
        EdgeError,
    > {
        let max_streams = self.max_concurrent_streams;
        let acceleration = self.acceleration;
        let connect_timeout = self.inner.timeouts().connect;
        let idle_timeout = self.inner.timeouts().idle;
        let tls = self
            .tls
            .as_ref()
            .map(|(engine, sni)| (engine.as_ref(), sni.as_ref()));

        let (client_and_lease, _endpoint) = self
            .inner
            .execute(|endpoint| async move {
                // 1. Fast-path: lease a stream slot from an existing healthy connection in the pool.
                if let Some(lease) = self.pool.acquire_stream(&endpoint, idle_timeout) {
                    let mut client = lease.client.clone();
                    let ready_res = std::future::poll_fn(|cx| match client.poll_ready(cx) {
                        std::task::Poll::Ready(Ok(())) => std::task::Poll::Ready(Some(true)),
                        std::task::Poll::Ready(Err(_)) => std::task::Poll::Ready(Some(false)),
                        std::task::Poll::Pending => std::task::Poll::Ready(None),
                    })
                    .await;

                    match ready_res {
                        Some(true) => return Ok::<_, String>(((client, lease), endpoint)),
                        Some(false) => {
                            lease.mark_goaway();
                        }
                        None => {
                            if let Ok(Ok(ready_client)) =
                                tokio::time::timeout(connect_timeout, client.ready()).await
                            {
                                return Ok::<_, String>(((ready_client, lease), endpoint));
                            } else {
                                lease.mark_goaway();
                            }
                        }
                    }
                }

                // 2. Slow-path: acquire connect lock to serialize fresh connection establishment
                let _guard = self.connect_lock.lock().await;

                // Double-check: did a concurrent request finish connecting while we waited for the lock?
                if let Some(lease) = self.pool.acquire_stream(&endpoint, idle_timeout) {
                    let mut client = lease.client.clone();
                    let ready_res = std::future::poll_fn(|cx| match client.poll_ready(cx) {
                        std::task::Poll::Ready(Ok(())) => std::task::Poll::Ready(Some(true)),
                        std::task::Poll::Ready(Err(_)) => std::task::Poll::Ready(Some(false)),
                        std::task::Poll::Pending => std::task::Poll::Ready(None),
                    })
                    .await;

                    match ready_res {
                        Some(true) => return Ok::<_, String>(((client, lease), endpoint)),
                        Some(false) => {
                            lease.mark_goaway();
                        }
                        None => {
                            if let Ok(Ok(ready_client)) =
                                tokio::time::timeout(connect_timeout, client.ready()).await
                            {
                                return Ok::<_, String>(((ready_client, lease), endpoint));
                            } else {
                                lease.mark_goaway();
                            }
                        }
                    }
                }

                // Establish a single fresh HTTP/2 client connection
                let fresh = velda_http2::client::connect(
                    endpoint,
                    tls,
                    config,
                    Some(&acceleration),
                    Some(connect_timeout),
                )
                .await
                .map_err(|e| e.to_string())?;

                let res = Http2ClientResource::new(fresh);
                let lease = self.pool.register(endpoint, res, max_streams);

                let ready_client = lease.client.clone();
                let ready_client = tokio::time::timeout(connect_timeout, ready_client.ready())
                    .await
                    .map_err(|_| {
                        "Timeout waiting for client ready after fresh connect".to_string()
                    })?
                    .map_err(|e| format!("Client not ready after fresh connect: {e}"))?;

                Ok::<_, String>(((ready_client, lease), endpoint))
            })
            .await
            .map_err(EdgeError::Upstream)?;

        Ok(client_and_lease)
    }

    /// Acquires a fresh, dedicated HTTP/2 client connection, bypassing existing connections in the pool.
    ///
    /// Used for transparent 1-shot self-healing when a pooled connection fails on stream open (RFC 9113 §8.1.4).
    pub async fn acquire_fresh(
        &self,
        config: &velda_http2::Http2Config,
    ) -> Result<
        (
            h2::client::SendRequest<Bytes>,
            velda_connection_pool::StreamLease<Http2ClientResource>,
        ),
        EdgeError,
    > {
        let max_streams = self.max_concurrent_streams;
        let acceleration = self.acceleration;
        let connect_timeout = self.inner.timeouts().connect;
        let tls = self
            .tls
            .as_ref()
            .map(|(engine, sni)| (engine.as_ref(), sni.as_ref()));

        let (client_and_lease, _endpoint) = self
            .inner
            .execute(|endpoint| async move {
                let fresh = velda_http2::client::connect(
                    endpoint,
                    tls,
                    config,
                    Some(&acceleration),
                    Some(connect_timeout),
                )
                .await
                .map_err(|e| e.to_string())?;

                let res = Http2ClientResource::new(fresh);
                let lease = self.pool.register(endpoint, res, max_streams);

                let ready_client = lease.client.clone();
                let ready_client = tokio::time::timeout(connect_timeout, ready_client.ready())
                    .await
                    .map_err(|_| {
                        "Timeout waiting for client ready after fresh connect".to_string()
                    })?
                    .map_err(|e| format!("Client not ready after fresh connect: {e}"))?;

                Ok::<_, String>(((ready_client, lease), endpoint))
            })
            .await
            .map_err(EdgeError::Upstream)?;

        Ok(client_and_lease)
    }

    /// Returns the number of physical multiplexed connections for `endpoint` in `lane`.
    #[inline]
    pub fn connection_count_for_lane(&self, endpoint: &SocketAddr, lane: usize) -> usize {
        self.pool.connection_count_for_lane(endpoint, lane)
    }

    /// Pre-warms multiplexed client connections for the specified worker lane.
    ///
    /// Executed autonomously by the core-local Shadow Worker (`SCHED_IDLE`) whenever
    /// lane capacity drops below available headroom.
    pub async fn prewarm_lane(&self, lane: usize, config: &velda_http2::Http2Config) {
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

            let existing = self.pool.connection_count_for_lane(&addr, lane);
            let min_warm = 1;
            let to_establish = if existing < min_warm {
                min_warm - existing
            } else if existing < self.max_connections_per_key
                && !self
                    .pool
                    .has_headroom_for_lane(&addr, lane, self.max_concurrent_streams)
            {
                1
            } else {
                0
            };

            for _ in 0..to_establish {
                let tls = self
                    .tls
                    .as_ref()
                    .map(|(engine, sni)| (engine.as_ref(), sni.as_ref()));
                let acceleration = self.acceleration;
                let connect_timeout = self.inner.timeouts().connect;

                match velda_http2::client::connect(
                    addr,
                    tls,
                    config,
                    Some(&acceleration),
                    Some(connect_timeout),
                )
                .await
                {
                    Ok(fresh) => {
                        let ready_res = tokio::time::timeout(connect_timeout, fresh.ready()).await;
                        if let Ok(Ok(ready_client)) = ready_res {
                            let res = Http2ClientResource::new(ready_client);
                            self.pool.register_warmed_to_lane(
                                addr,
                                lane,
                                res,
                                self.max_concurrent_streams,
                            );
                            tracing::debug!(endpoint = %addr, lane, "Shadow Worker pre-warmed HTTP/2 connection");
                        }
                    }
                    Err(e) => {
                        tracing::debug!(endpoint = %addr, lane, error = %e, "Shadow Worker pre-warm connect failed for HTTP/2");
                        break;
                    }
                }
            }
        }
    }
}
