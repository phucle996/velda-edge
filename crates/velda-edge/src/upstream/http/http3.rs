//! Layer 7 HTTP/3 Upstream managing persistent QUIC client multiplexing and request forwarding (RFC 9114).

use std::net::SocketAddr;
use std::sync::Arc;

use velda_connection_pool::{MultiplexedPool, PoolableResource};
use velda_core::StreamingMode;
use velda_http3::Http3Config;
use velda_http3::Http3PipeStrategy;
use velda_http3::client::Http3Client;
use velda_http3::quinn_proto;

use crate::error::EdgeError;
use crate::upstream::lb::EdgeUpstream;

/// Pooled HTTP/3 client resource wrapping [`Http3Client`].
#[derive(Clone)]
pub struct Http3ClientResource {
    pub client: Http3Client,
}

impl PoolableResource for Http3ClientResource {
    fn is_healthy(&self) -> bool {
        !self.client.is_closed()
    }

    fn close(&mut self) {}
}

impl std::fmt::Debug for Http3ClientResource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Http3ClientResource")
            .field("target", &self.client.target())
            .field("is_closed", &self.client.is_closed())
            .finish()
    }
}

/// Layer 7 HTTP/3 Upstream managing persistent QUIC client multiplexing and pipe forwarding.
///
/// Pre-compiled with static load balancer, streaming strategy, target SNI, and sharded multiplexed client pool.
pub struct Http3Upstream {
    /// [PRE-COMPILED]: Pre-assembled upstream core holding discovery, health tracker,
    /// timeouts, and the selected `LbAlgorithm` enum variant.
    inner: EdgeUpstream,
    /// [PRE-COMPILED]: Target TLS SNI for upstream QUIC connection handshake.
    pub target_sni: Arc<str>,
    /// Declarative streaming mode from upstream configuration.
    pub streaming: StreamingMode,
    /// [PRE-COMPILED]: Pre-computed wire forwarding strategy (`Http3PipeStrategy`) derived from
    /// `streaming` at compile time; eliminates runtime enum branching.
    pub strategy: Http3PipeStrategy,
    /// Lock-sharded persistent HTTP/3 QUIC client connection pool.
    pool: Arc<MultiplexedPool<SocketAddr, Http3ClientResource>>,
    /// Maximum concurrent connections permitted per physical endpoint.
    pub max_connections_per_key: usize,
    /// Maximum concurrent streams per multiplexed connection.
    pub max_concurrent_streams: u32,
    /// Serialization lock for establishing new QUIC connections to prevent thundering herd storms.
    connect_lock: tokio::sync::Mutex<()>,
    /// [PRE-COMPILED]: Pre-computed HTTP/3 protocol configuration.
    pub config: Http3Config,
    /// [PRE-COMPILED]: Pre-compiled QUIC client TLS configuration compiled directly
    /// from upstream `tls` config (custom CA roots, client mTLS cert, verify settings).
    pub quic_config: Arc<quinn_proto::ClientConfig>,
}

impl std::fmt::Debug for Http3Upstream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Http3Upstream")
            .field("id", &self.inner.id())
            .field("target_sni", &self.target_sni)
            .field("streaming", &self.streaming)
            .field("strategy", &self.strategy)
            .field("max_connections_per_key", &self.max_connections_per_key)
            .field("max_concurrent_streams", &self.max_concurrent_streams)
            .finish()
    }
}

impl Http3Upstream {
    /// Creates a new pre-compiled [`Http3Upstream`].
    pub fn new(
        inner: EdgeUpstream,
        target_sni: Arc<str>,
        streaming: StreamingMode,
        shard_count: usize,
        max_connections_per_key: usize,
        max_concurrent_streams: u32,
        quic_config: Arc<quinn_proto::ClientConfig>,
    ) -> Self {
        let strategy = Http3PipeStrategy::from_streaming(streaming);
        Self {
            inner,
            target_sni,
            streaming,
            strategy,
            pool: Arc::new(
                MultiplexedPool::with_shards(shard_count)
                    .with_max_connections_per_key(max_connections_per_key),
            ),
            max_connections_per_key: max_connections_per_key.max(1),
            max_concurrent_streams,
            connect_lock: tokio::sync::Mutex::new(()),
            config: Http3Config::auto(),
            quic_config,
        }
    }

    /// Returns the upstream unique identifier.
    #[inline]
    pub fn id(&self) -> &str {
        self.inner.id()
    }

    /// Returns the request timeout for upstream HTTP/3 exchanges (defaults to 60s if unspecified).
    #[inline]
    pub fn request_timeout(&self) -> std::time::Duration {
        self.inner
            .timeouts()
            .request
            .unwrap_or(std::time::Duration::from_secs(60))
    }

    /// Acquires an active multiplexed HTTP/3 client from the pool or establishes a new QUIC connection.
    ///
    /// Distributes streams across parallel QUIC connections when active streams exceed the scale-out threshold.
    pub async fn acquire(
        &self,
        config: &Http3Config,
    ) -> Result<
        (
            Http3Client,
            velda_connection_pool::StreamLease<Http3ClientResource>,
        ),
        EdgeError,
    > {
        let max_streams = self.max_concurrent_streams;
        let sni = Arc::clone(&self.target_sni);
        let idle_timeout = self.inner.timeouts().idle;
        let quic_tls = Arc::clone(&self.quic_config);

        let ((client, lease), _endpoint) = self
            .inner
            .execute(|endpoint| {
                let sni = Arc::clone(&sni);
                let quic_tls = Arc::clone(&quic_tls);
                async move {
                    // 1. Fast-path: lease a stream slot from an existing healthy QUIC connection in the pool.
                    if let Some(lease) = self.pool.acquire_stream(&endpoint, idle_timeout) {
                        return Ok::<_, String>(((lease.client.clone(), lease), endpoint));
                    }

                    // 2. Slow-path: acquire connect lock to serialize fresh QUIC connection establishment
                    // and eliminate thundering herd packet storms
                    let _guard = self.connect_lock.lock().await;

                    // Double-check: did a concurrent request finish connecting while we waited for the lock?
                    if let Some(lease) = self.pool.acquire_stream(&endpoint, idle_timeout) {
                        return Ok::<_, String>(((lease.client.clone(), lease), endpoint));
                    }

                    let client =
                        velda_http3::client::connect(endpoint, &sni, config, (*quic_tls).clone())
                            .await
                            .map_err(|e| e.to_string())?;

                    let res = Http3ClientResource {
                        client: client.clone(),
                    };
                    let lease = self.pool.register(endpoint, res, max_streams);

                    Ok::<_, String>(((client, lease), endpoint))
                }
            })
            .await
            .map_err(EdgeError::Upstream)?;

        Ok((client, lease))
    }

    /// Forcefully establishes a fresh HTTP/3 client connection directly, bypassing existing connections in the pool.
    pub async fn acquire_fresh(
        &self,
        config: &Http3Config,
    ) -> Result<
        (
            Http3Client,
            velda_connection_pool::StreamLease<Http3ClientResource>,
        ),
        EdgeError,
    > {
        let max_streams = self.max_concurrent_streams;
        let sni = Arc::clone(&self.target_sni);
        let idle_timeout = self.inner.timeouts().idle;
        let quic_tls = Arc::clone(&self.quic_config);

        let ((client, lease), _endpoint) = self
            .inner
            .execute(|endpoint| {
                let sni = Arc::clone(&sni);
                let quic_tls = Arc::clone(&quic_tls);
                async move {
                    let _guard = self.connect_lock.lock().await;
                    if let Some(lease) = self.pool.acquire_stream(&endpoint, idle_timeout) {
                        return Ok::<_, String>(((lease.client.clone(), lease), endpoint));
                    }

                    let client =
                        velda_http3::client::connect(endpoint, &sni, config, (*quic_tls).clone())
                            .await
                            .map_err(|e| e.to_string())?;

                    let res = Http3ClientResource {
                        client: client.clone(),
                    };
                    let lease = self.pool.register(endpoint, res, max_streams);

                    Ok::<_, String>(((client, lease), endpoint))
                }
            })
            .await
            .map_err(EdgeError::Upstream)?;

        Ok((client, lease))
    }

    /// Pre-warms QUIC client connections for the specified worker lane.
    ///
    /// Executed autonomously by the core-local Shadow Worker (`SCHED_IDLE`) whenever
    /// lane capacity drops below available headroom.
    pub async fn prewarm_lane(&self, lane: usize, config: &Http3Config) {
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

            if !self
                .pool
                .has_headroom_for_lane(&addr, lane, self.max_concurrent_streams)
            {
                let sni = Arc::clone(&self.target_sni);
                let quic_tls = Arc::clone(&self.quic_config);

                match velda_http3::client::connect(addr, &sni, config, (*quic_tls).clone()).await {
                    Ok(client) => {
                        let res = Http3ClientResource { client };
                        self.pool.register_warmed_to_lane(
                            addr,
                            lane,
                            res,
                            self.max_concurrent_streams,
                        );
                        tracing::debug!(endpoint = %addr, lane, "Shadow Worker pre-warmed HTTP/3 connection");
                    }
                    Err(e) => {
                        tracing::debug!(endpoint = %addr, lane, error = %e, "Shadow Worker pre-warm connect failed for HTTP/3");
                    }
                }
            }
        }
    }
}
