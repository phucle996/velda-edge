//! Layer 7 HTTP/3 Upstream managing persistent QUIC client multiplexing (RFC 9114) and request handoff.

use std::net::SocketAddr;

use velda_connection_pool::{MultiplexedPool, PoolableResource};
use velda_core::{L7Request, L7Response, StreamingMode};
use velda_http3::pipe::Http3PipeStrategy;

use super::lb::EdgeUpstream;
use crate::error::EdgeError;

/// Pooled HTTP/3 client resource wrapping [`velda_http3::Http3Client`].
#[derive(Clone)]
pub struct Http3ClientResource {
    pub client: velda_http3::Http3Client,
}

impl Http3ClientResource {
    pub fn new(client: velda_http3::Http3Client) -> Self {
        Self { client }
    }
}

impl std::fmt::Debug for Http3ClientResource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Http3ClientResource").finish()
    }
}

impl PoolableResource for Http3ClientResource {
    fn is_healthy(&self) -> bool {
        !self.client.is_closed()
    }

    fn close(&mut self) {}
}

/// Layer 7 HTTP/3 Upstream managing persistent QUIC client multiplexing (RFC 9114) and request handoff.
///
/// Pre-compiled with static load balancer, target SNI, streaming strategy, and sharded multiplexed client pool.
pub struct Http3Upstream {
    /// [PRE-COMPILED]: Pre-assembled upstream core holding discovery, health tracker,
    /// timeouts, and the selected `LbAlgorithm` enum variant.
    inner: EdgeUpstream,
    /// [PRE-COMPILED]: Target SNI hostname validated and pre-resolved from configuration.
    target_sni: Option<String>,
    /// Declarative streaming mode from upstream configuration.
    pub streaming: StreamingMode,
    /// [PRE-COMPILED]: Pre-computed wire forwarding strategy (`Http3PipeStrategy`) derived from
    /// `streaming` at compile time; eliminates runtime enum branching.
    pub strategy: Http3PipeStrategy,
    /// Lock-sharded persistent HTTP/3 (QUIC) multiplexed client connection pool.
    pool: MultiplexedPool<SocketAddr, Http3ClientResource>,
    /// Maximum concurrent streams per multiplexed connection.
    pub max_concurrent_streams: u32,
}

impl std::fmt::Debug for Http3Upstream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Http3Upstream")
            .field("id", &self.inner.id())
            .field("target_sni", &self.target_sni)
            .field("streaming", &self.streaming)
            .field("strategy", &self.strategy)
            .field("max_concurrent_streams", &self.max_concurrent_streams)
            .finish()
    }
}

impl Http3Upstream {
    /// Creates a new pre-compiled [`Http3Upstream`].
    pub fn new(
        inner: EdgeUpstream,
        target_sni: Option<String>,
        streaming: StreamingMode,
        shard_count: usize,
        max_concurrent_streams: u32,
    ) -> Self {
        let strategy = Http3PipeStrategy::from_streaming(streaming);
        Self {
            inner,
            target_sni,
            streaming,
            strategy,
            pool: MultiplexedPool::with_shards(shard_count),
            max_concurrent_streams,
        }
    }

    /// Returns the upstream unique identifier.
    #[inline]
    pub fn id(&self) -> &str {
        self.inner.id()
    }

    /// Returns the configured target SNI name, if any.
    #[inline]
    pub fn target_sni(&self) -> Option<&str> {
        self.target_sni.as_deref()
    }

    /// Resolves SNI domain for HTTP/3 QUIC connection.
    pub fn resolve_sni(&self, req: &L7Request, host_header: Option<&str>) -> String {
        if let Some(ref sni) = self.target_sni {
            sni.clone()
        } else if let Some(h) = host_header {
            velda_http3::strip_port(h).to_string()
        } else if let Some(h) = req.headers.get("host").and_then(|v| v.to_str().ok()) {
            velda_http3::strip_port(h).to_string()
        } else if let Some(authority) = req.uri.authority() {
            velda_http3::strip_port(authority.as_str()).to_string()
        } else {
            "localhost".to_string()
        }
    }

    /// Hands off downstream HTTP/3 request execution to a persistent multiplexed QUIC client.
    ///
    /// Requests stream concurrently over UDP datagrams without Head-of-Line blocking.
    pub async fn dispatch_request(
        &self,
        req: L7Request,
        server_name: &str,
        config: &velda_http3::Http3Config,
    ) -> Result<L7Response, EdgeError> {
        let req_cell = std::sync::Mutex::new(Some(req));
        let server_name_cloned = server_name.to_string();
        let max_streams = self.max_concurrent_streams;

        self.inner
            .execute(|endpoint| {
                let cell = &req_cell;
                let server_name = server_name_cloned.clone();
                async move {
                    let (client, _lease) = if let Some(lease) = self.pool.acquire_stream(&endpoint)
                    {
                        if !lease.client.is_closed() {
                            (lease.client.clone(), Some(lease))
                        } else {
                            lease.mark_goaway();
                            let fresh = velda_http3::connect(endpoint, &server_name, config)
                                .await
                                .map_err(|e| e.to_string())?;
                            let stream_lease = self.pool.register(
                                endpoint,
                                Http3ClientResource::new(fresh.clone()),
                                max_streams,
                            );
                            (fresh, Some(stream_lease))
                        }
                    } else {
                        let fresh = velda_http3::connect(endpoint, &server_name, config)
                            .await
                            .map_err(|e| e.to_string())?;
                        let stream_lease = self.pool.register(
                            endpoint,
                            Http3ClientResource::new(fresh.clone()),
                            max_streams,
                        );
                        (fresh, Some(stream_lease))
                    };

                    let req = cell
                        .lock()
                        .unwrap()
                        .take()
                        .ok_or_else(|| "Request payload already consumed".to_string())?;

                    client.send_request(req).await.map_err(|e| e.to_string())
                }
            })
            .await
            .map_err(EdgeError::Upstream)
    }
}
