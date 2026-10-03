//! Layer 7 HTTP/1.1 Upstream managing endpoint selection, connection establishment, and pipe handoff.

use std::sync::Arc;

use velda_core::StreamingMode;
pub use velda_http1::UpstreamHttp1Stream;
use velda_http1::pipe::Http1PipeStrategy;
use velda_tls::TlsClientEngine;
use velda_upstream::UpstreamError;

use super::lb::EdgeUpstream;
use crate::error::EdgeError;

/// Layer 7 HTTP/1.1 Upstream managing endpoint selection, connection establishment, and pipe handoff.
///
/// Pre-compiled with static TLS engine, load balancer, and streaming strategy to eliminate hot-path lookups.
pub struct Http1Upstream {
    /// [PRE-COMPILED]: Pre-assembled upstream core holding discovery, health tracker,
    /// timeouts, connection pool, and the selected `LbAlgorithm` enum variant.
    inner: EdgeUpstream,
    /// [PRE-COMPILED]: Pre-compiled TLS client engine holding TLS connectors and root CAs.
    /// Baked at snapshot compilation; eliminates runtime lookup of TLS context.
    tls_engine: Option<Arc<TlsClientEngine>>,
    /// [PRE-COMPILED]: Target SNI hostname validated and pre-resolved from configuration.
    target_sni: Option<String>,
    /// [PRE-COMPILED]: Static boolean flag indicating whether outbound connection requires TLS.
    is_tls: bool,
    /// Declarative streaming mode from upstream configuration.
    pub streaming: StreamingMode,
    /// [PRE-COMPILED]: Pre-computed wire forwarding strategy (`Http1PipeStrategy`) derived from
    /// `streaming` at compile time; enables $O(1)$ zero-cost dispatch on the hot path.
    pub strategy: Http1PipeStrategy,
}

impl std::fmt::Debug for Http1Upstream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Http1Upstream")
            .field("id", &self.inner.id())
            .field("target_sni", &self.target_sni)
            .field("is_tls", &self.is_tls)
            .field("streaming", &self.streaming)
            .field("strategy", &self.strategy)
            .finish()
    }
}

impl Http1Upstream {
    /// Creates a new pre-compiled [`Http1Upstream`].
    pub fn new(
        inner: EdgeUpstream,
        target_sni: Option<String>,
        is_tls: bool,
        tls_engine: Option<Arc<TlsClientEngine>>,
        streaming: StreamingMode,
    ) -> Self {
        let strategy = Http1PipeStrategy::from_streaming(streaming);
        Self {
            inner,
            tls_engine,
            target_sni,
            is_tls,
            streaming,
            strategy,
        }
    }

    /// Returns the upstream unique identifier.
    #[inline]
    pub fn id(&self) -> &str {
        self.inner.id()
    }

    /// Returns the configured SNI DNS name, if any.
    #[inline]
    pub fn target_sni(&self) -> Option<&str> {
        self.target_sni.as_deref()
    }

    /// Returns whether this upstream target requires TLS encryption.
    #[inline]
    pub fn is_tls(&self) -> bool {
        self.is_tls
    }

    /// Hands off downstream HTTP/1.1 pipe execution to an acquired upstream connection.
    ///
    /// Manages single-round Load Balancer selection, TCP/TLS connection establishment,
    /// automatic endpoint failover, and streaming pipe execution.
    ///
    /// Hot-Path Invariant: Zero dynamic TLS lookup or protocol sniffing. The TLS client engine
    /// is pre-compiled and baked directly into this upstream.
    pub async fn dispatch_pipe<F, Fut, T, E>(
        &self,
        host_override: Option<&str>,
        pipe: F,
    ) -> Result<T, EdgeError>
    where
        F: FnOnce(UpstreamHttp1Stream) -> Fut,
        Fut: std::future::Future<Output = Result<T, E>>,
        E: std::fmt::Display,
    {
        let target_sni = self.target_sni.clone();
        let is_tls = self.is_tls;
        let host = host_override.map(|s| s.to_string());
        let tls_engine = self.tls_engine.clone();

        let stream = self
            .inner
            .execute(|endpoint| {
                let target_sni = target_sni.clone();
                let host = host.clone();
                let tls_engine = tls_engine.clone();
                async move {
                    velda_http1::client::connect_stream(
                        endpoint,
                        is_tls,
                        target_sni.as_deref(),
                        tls_engine.as_deref(),
                        host.as_deref(),
                    )
                    .await
                    .map_err(|e| e.to_string())
                }
            })
            .await
            .map_err(EdgeError::Upstream)?;

        pipe(stream)
            .await
            .map_err(|e| EdgeError::Upstream(UpstreamError::Protocol(e.to_string())))
    }
}
