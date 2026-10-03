//! Layer 7 HTTP/2 Upstream managing persistent client multiplexing (RFC 9113) and pipe handoff.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::RwLock;
use velda_core::StreamingMode;
use velda_http2::pipe::Http2PipeStrategy;

use super::lb::EdgeUpstream;
use crate::error::EdgeError;

/// Layer 7 HTTP/2 Upstream managing persistent client multiplexing (RFC 9113) and pipe handoff.
///
/// Pre-compiled with static load balancer, wire pipe strategy, and persistent multiplexed client cache.
pub struct Http2Upstream {
    /// [PRE-COMPILED]: Pre-assembled upstream core holding discovery, health tracker,
    /// timeouts, and the selected `LbAlgorithm` enum variant.
    inner: EdgeUpstream,
    /// Declarative streaming mode from upstream configuration.
    pub streaming: StreamingMode,
    /// [PRE-COMPILED]: Pre-computed wire forwarding strategy (`Http2PipeStrategy`) derived from
    /// `streaming` at compile time; eliminates runtime enum branching.
    pub strategy: Http2PipeStrategy,
    /// [PRE-COMPILED STATE]: Lock-free persistent HTTP/2 multiplexed client connection cache.
    /// Reuses existing established H2 streams across hundreds of concurrent requests.
    clients: RwLock<HashMap<SocketAddr, h2::client::SendRequest<Bytes>>>,
}

impl std::fmt::Debug for Http2Upstream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Http2Upstream")
            .field("id", &self.inner.id())
            .field("streaming", &self.streaming)
            .field("strategy", &self.strategy)
            .finish()
    }
}

impl Http2Upstream {
    /// Creates a new pre-compiled [`Http2Upstream`].
    pub fn new(inner: EdgeUpstream, streaming: StreamingMode) -> Self {
        let strategy = Http2PipeStrategy::from_streaming(streaming);
        Self {
            inner,
            streaming,
            strategy,
            clients: RwLock::new(HashMap::new()),
        }
    }

    /// Returns the upstream unique identifier.
    #[inline]
    pub fn id(&self) -> &str {
        self.inner.id()
    }

    /// Hands off downstream HTTP/2 pipe execution to a persistent multiplexed client connection.
    ///
    /// Hundreds of concurrent downstream streams share the same underlying TCP connection
    /// without repeated handshakes. Dead connections are automatically replaced.
    pub async fn dispatch_pipe<F, Fut, T, E>(
        &self,
        config: &velda_http2::Http2Config,
        pipe: F,
    ) -> Result<T, EdgeError>
    where
        F: FnOnce(h2::client::SendRequest<Bytes>) -> Fut,
        Fut: std::future::Future<Output = Result<T, E>>,
        E: std::fmt::Display,
    {
        let pipe_cell = Arc::new(std::sync::Mutex::new(Some(pipe)));

        self.inner
            .execute(|endpoint| {
                let cell = Arc::clone(&pipe_cell);
                async move {
                    let existing_client = {
                        let guard = self.clients.read().await;
                        guard.get(&endpoint).cloned()
                    };

                    let mut client = match existing_client {
                        Some(c) => c,
                        None => {
                            let mut guard = self.clients.write().await;
                            if let Some(c) = guard.get(&endpoint) {
                                c.clone()
                            } else {
                                let c = velda_http2::client::connect(endpoint, config)
                                    .await
                                    .map_err(|e| e.to_string())?;
                                guard.insert(endpoint, c.clone());
                                c
                            }
                        }
                    };

                    client = match client.ready().await {
                        Ok(ready_client) => ready_client,
                        Err(_) => {
                            let mut guard = self.clients.write().await;
                            guard.remove(&endpoint);
                            let fresh = velda_http2::client::connect(endpoint, config)
                                .await
                                .map_err(|e| e.to_string())?;
                            guard.insert(endpoint, fresh.clone());
                            fresh
                                .ready()
                                .await
                                .map_err(|err| format!("Reconnected H2 client not ready: {err}"))?
                        }
                    };

                    let pipe_fn = cell
                        .lock()
                        .unwrap()
                        .take()
                        .ok_or_else(|| "Pipe closure already consumed".to_string())?;

                    pipe_fn(client).await.map_err(|e| e.to_string())
                }
            })
            .await
            .map_err(EdgeError::Upstream)
    }
}
