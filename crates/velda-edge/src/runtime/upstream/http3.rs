//! Layer 7 HTTP/3 Upstream managing persistent QUIC client multiplexing (RFC 9114) and request handoff.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use tokio::sync::RwLock;
use velda_core::{L7Request, L7Response, StreamingMode};
use velda_http3::pipe::Http3PipeStrategy;

use super::lb::EdgeUpstream;
use crate::error::EdgeError;

/// Layer 7 HTTP/3 Upstream managing persistent QUIC client multiplexing (RFC 9114) and request handoff.
///
/// Pre-compiled with static load balancer, target SNI, streaming strategy, and persistent QUIC client connection cache.
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
    /// [PRE-COMPILED STATE]: Lock-free persistent HTTP/3 (QUIC) multiplexed client connection cache.
    /// Manages persistent UDP datagram streams to eliminate handshake delays.
    clients: RwLock<HashMap<SocketAddr, velda_http3::Http3Client>>,
}

impl std::fmt::Debug for Http3Upstream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Http3Upstream")
            .field("id", &self.inner.id())
            .field("target_sni", &self.target_sni)
            .field("streaming", &self.streaming)
            .field("strategy", &self.strategy)
            .finish()
    }
}

impl Http3Upstream {
    /// Creates a new pre-compiled [`Http3Upstream`].
    pub fn new(inner: EdgeUpstream, target_sni: Option<String>, streaming: StreamingMode) -> Self {
        let strategy = Http3PipeStrategy::from_streaming(streaming);
        Self {
            inner,
            target_sni,
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
        let req_cell = Arc::new(std::sync::Mutex::new(Some(req)));

        self.inner
            .execute(|endpoint| {
                let cell = Arc::clone(&req_cell);
                async move {
                    let existing_client = {
                        let guard = self.clients.read().await;
                        guard.get(&endpoint).cloned()
                    };

                    let client = match existing_client {
                        Some(c) if !c.is_closed() => c,
                        _ => {
                            let mut guard = self.clients.write().await;
                            if let Some(c) = guard.get(&endpoint) {
                                if !c.is_closed() {
                                    c.clone()
                                } else {
                                    let fresh = velda_http3::connect(endpoint, server_name, config)
                                        .await
                                        .map_err(|e| e.to_string())?;
                                    guard.insert(endpoint, fresh.clone());
                                    fresh
                                }
                            } else {
                                let fresh = velda_http3::connect(endpoint, server_name, config)
                                    .await
                                    .map_err(|e| e.to_string())?;
                                guard.insert(endpoint, fresh.clone());
                                fresh
                            }
                        }
                    };

                    let req = cell
                        .lock()
                        .unwrap()
                        .as_ref()
                        .cloned()
                        .ok_or_else(|| "Request payload already consumed".to_string())?;

                    match client.send_request(req).await {
                        Ok(resp) => Ok(resp),
                        Err(e) => {
                            let mut guard = self.clients.write().await;
                            guard.remove(&endpoint);
                            Err(e.to_string())
                        }
                    }
                }
            })
            .await
            .map_err(EdgeError::Upstream)
    }
}
