//! Layer 7 gRPC over TCP Upstream managing connection lifecycle, multiplexing, and endpoint health tracking.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use velda_connection_pool::{MultiplexedPool, PoolableResource};
use velda_core::StreamingMode;
use velda_grpc::GrpcConfig;
use velda_grpc::tcp::GrpcPipeStrategy;
use velda_grpc::tcp::client::{GrpcAccelerationPath, GrpcUpstreamConnector};
use velda_tls::TlsClientEngine;

use super::super::lb::EdgeUpstream;
use crate::error::EdgeError;

/// Pooled gRPC over TCP client resource wrapping [`GrpcUpstreamConnector`].
#[derive(Clone)]
pub struct GrpcTcpClientResource {
    pub client: GrpcUpstreamConnector,
}

impl PoolableResource for GrpcTcpClientResource {
    fn is_healthy(&self) -> bool {
        true
    }

    fn close(&mut self) {}
}

impl std::fmt::Debug for GrpcTcpClientResource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GrpcTcpClientResource").finish()
    }
}

/// Layer 7 gRPC over TCP Upstream managing persistent HTTP/2 multiplexing and endpoint health tracking.
///
/// Pre-compiled with static load balancer, streaming strategy, socket acceleration, and sharded multiplexed client pool.
pub struct GrpcTcpUpstream {
    /// [PRE-COMPILED]: Pre-assembled upstream core holding discovery, health tracker,
    /// timeouts, and the selected `LbAlgorithm` enum variant.
    inner: EdgeUpstream,
    /// [PRE-COMPILED]: Upstream TLS identity: client engine plus the static target SNI.
    /// `None` for cleartext upstreams. TLS without SNI is rejected at compile time.
    tls: Option<(Arc<TlsClientEngine>, Arc<str>)>,
    /// Declarative streaming mode from upstream configuration.
    pub streaming: StreamingMode,
    /// [PRE-COMPILED]: Pre-computed wire forwarding strategy (`GrpcPipeStrategy`) derived from
    /// `streaming` at compile time; eliminates runtime enum branching.
    pub strategy: GrpcPipeStrategy,
    /// Lock-sharded persistent gRPC HTTP/2 multiplexed client connection pool.
    pool: Arc<MultiplexedPool<SocketAddr, GrpcTcpClientResource>>,
    /// Maximum concurrent streams per multiplexed connection.
    pub max_concurrent_streams: u32,
    /// [PRE-COMPILED]: Protocol-specific socket acceleration path.
    pub acceleration: GrpcAccelerationPath,
    /// Last active health probe timestamp in seconds from coarse integer clock.
    last_probe_sec: AtomicU64,
}

impl std::fmt::Debug for GrpcTcpUpstream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GrpcTcpUpstream")
            .field("id", &self.inner.id())
            .field("streaming", &self.streaming)
            .field("strategy", &self.strategy)
            .field("max_concurrent_streams", &self.max_concurrent_streams)
            .field("acceleration", &self.acceleration)
            .finish()
    }
}

impl GrpcTcpUpstream {
    /// Creates a new [`GrpcTcpUpstream`] instance.
    pub fn new(
        inner: EdgeUpstream,
        tls: Option<(Arc<TlsClientEngine>, Arc<str>)>,
        streaming: StreamingMode,
        shard_count: usize,
        max_concurrent_streams: u32,
        acceleration: GrpcAccelerationPath,
    ) -> Self {
        let strategy = GrpcPipeStrategy::from_streaming(streaming);
        Self {
            inner,
            tls,
            streaming,
            strategy,
            pool: Arc::new(MultiplexedPool::with_shards(shard_count)),
            max_concurrent_streams,
            acceleration,
            last_probe_sec: AtomicU64::new(0),
        }
    }

    /// Returns the upstream unique identifier.
    #[inline]
    pub fn id(&self) -> &str {
        self.inner.id()
    }

    /// Acquires an active, ready multiplexed gRPC over TCP client connection from the pool or connects a fresh one.
    pub async fn acquire(&self, config: &GrpcConfig) -> Result<GrpcUpstreamConnector, EdgeError> {
        let max_streams = self.max_concurrent_streams;
        let acceleration = self.acceleration;
        let connect_timeout = self.inner.timeouts().connect;
        let idle_timeout = self.inner.timeouts().idle;
        let tls = self
            .tls
            .as_ref()
            .map(|(engine, sni)| (engine.as_ref(), sni.as_ref()));

        let (connector, _endpoint) = self
            .inner
            .execute(|endpoint| async move {
                if let Some(lease) = self.pool.acquire_stream(&endpoint, idle_timeout) {
                    let mut connector = lease.client.clone();
                    match connector.ready().await {
                        Ok(()) => return Ok::<_, String>((connector, endpoint)),
                        Err(_) => {
                            lease.mark_goaway();
                        }
                    }
                }

                let mut fresh = GrpcUpstreamConnector::connect(
                    endpoint,
                    tls,
                    config,
                    Some(&acceleration),
                    Some(connect_timeout),
                )
                .await
                .map_err(|e| e.to_string())?;

                fresh
                    .ready()
                    .await
                    .map_err(|err| format!("Reconnected gRPC TCP client not ready: {err}"))?;

                let _ = self.pool.register(
                    endpoint,
                    GrpcTcpClientResource {
                        client: fresh.clone(),
                    },
                    max_streams,
                );

                Ok::<_, String>((fresh, endpoint))
            })
            .await
            .map_err(EdgeError::Upstream)?;

        Ok(connector)
    }

    /// Connects a brand new gRPC over TCP client connector directly, bypassing pool leases.
    ///
    /// Used for transparent 1-shot self-healing when an existing multiplexed connection encounters
    /// GOAWAY, REFUSED_STREAM, or silent connection drop.
    pub async fn acquire_fresh(
        &self,
        config: &GrpcConfig,
    ) -> Result<GrpcUpstreamConnector, EdgeError> {
        let max_streams = self.max_concurrent_streams;
        let acceleration = self.acceleration;
        let connect_timeout = self.inner.timeouts().connect;
        let tls = self
            .tls
            .as_ref()
            .map(|(engine, sni)| (engine.as_ref(), sni.as_ref()));

        let (connector, _endpoint) = self
            .inner
            .execute(|endpoint| async move {
                let mut fresh = GrpcUpstreamConnector::connect(
                    endpoint,
                    tls,
                    config,
                    Some(&acceleration),
                    Some(connect_timeout),
                )
                .await
                .map_err(|e| e.to_string())?;

                fresh
                    .ready()
                    .await
                    .map_err(|err| format!("Fresh gRPC TCP client not ready: {err}"))?;

                let _ = self.pool.register(
                    endpoint,
                    GrpcTcpClientResource {
                        client: fresh.clone(),
                    },
                    max_streams,
                );

                Ok::<_, String>((fresh, endpoint))
            })
            .await
            .map_err(EdgeError::Upstream)?;

        Ok(connector)
    }

    /// Pre-warms gRPC TCP client connections for the specified worker lane.
    ///
    /// Executed autonomously by the core-local Shadow Worker (`SCHED_IDLE`) whenever
    /// lane capacity drops below available headroom.
    pub async fn prewarm_lane(&self, lane: usize, config: &GrpcConfig) {
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
                let tls = self
                    .tls
                    .as_ref()
                    .map(|(engine, sni)| (engine.as_ref(), sni.as_ref()));
                let acceleration = self.acceleration;
                let connect_timeout = self.inner.timeouts().connect;

                match GrpcUpstreamConnector::connect(
                    addr,
                    tls,
                    config,
                    Some(&acceleration),
                    Some(connect_timeout),
                )
                .await
                {
                    Ok(mut fresh) => {
                        let ready_res = tokio::time::timeout(connect_timeout, fresh.ready()).await;
                        if let Ok(Ok(_)) = ready_res {
                            let res = GrpcTcpClientResource { client: fresh };
                            self.pool.register_warmed_to_lane(
                                addr,
                                lane,
                                res,
                                self.max_concurrent_streams,
                            );
                            tracing::debug!(endpoint = %addr, lane, "Shadow Worker pre-warmed gRPC TCP connection");
                        }
                    }
                    Err(e) => {
                        tracing::debug!(endpoint = %addr, lane, error = %e, "Shadow Worker pre-warm connect failed for gRPC TCP");
                    }
                }
            }
        }
    }

    /// Performs periodic active health check across all physical endpoints for `lane`.
    pub async fn check_active_health(&self, lane: usize, _seed: u32) {
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
            let success = self.probe_endpoint(&addr, active_cfg, lane).await;
            health.record_active_result(&addr, success);
        }
    }

    async fn probe_endpoint(
        &self,
        addr: &SocketAddr,
        active_cfg: &velda_upstream::ActiveHealthConfig,
        lane: usize,
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
                    let _ = stream
                        .into_std()
                        .and_then(|s| s.shutdown(std::net::Shutdown::Both));
                    true
                }
                _ => false,
            }
        } else {
            // 2. gRPC Multiplexed probe
            let idle_timeout = self.inner.timeouts().idle;
            let leased = self.pool.acquire_stream(addr, idle_timeout);

            if let Some(lease) = leased {
                let mut client = lease.client.clone();
                let ready_res = tokio::time::timeout(timeout, client.ready()).await;
                if let Ok(Ok(_)) = ready_res {
                    return true;
                }
                lease.mark_goaway();
            }

            let tls = self
                .tls
                .as_ref()
                .map(|(engine, sni)| (engine.as_ref(), sni.as_ref()));
            let acceleration = self.acceleration;
            let config = GrpcConfig::default();

            match GrpcUpstreamConnector::connect(
                *addr,
                tls,
                &config,
                Some(&acceleration),
                Some(timeout),
            )
            .await
            {
                Ok(mut fresh) => {
                    let ready_res = tokio::time::timeout(timeout, fresh.ready()).await;
                    if let Ok(Ok(_)) = ready_res {
                        let res = GrpcTcpClientResource { client: fresh };
                        self.pool.register_warmed_to_lane(
                            *addr,
                            lane,
                            res,
                            self.max_concurrent_streams,
                        );
                        true
                    } else {
                        false
                    }
                }
                Err(_) => false,
            }
        }
    }
}
