//! Protocol-isolated in-memory upstream tables.
//!
//! Separates backend upstreams into 6 dedicated, typed, protocol-isolated tables:
//! - `tcp`: L4 raw TCP connection pool
//! - `udp`: L4 raw UDP endpoints & session tracking
//! - `http1`: L7 HTTP/1.1 backend endpoints & pooling
//! - `http2`: L7 HTTP/2 backend endpoints & multiplexing
//! - `http3`: L7 HTTP/3 backend endpoints & QUIC
//! - `grpc`: L7 gRPC backend endpoints & streaming
//!
//! Eliminates cross-protocol pollution and provides O(1) lock-free lookups.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use velda_sync::post_sync::upstream::UpstreamConfig;
use velda_upstream::{Endpoint, RoundRobin, TcpConnector, Upstream, UpstreamTimeouts};

pub use velda_http1::UpstreamHttp1Stream;

/// Runtime upstream wrapper delegating to protocol-agnostic [`velda_upstream::Upstream`].
pub struct RuntimeUpstream {
    inner: Upstream<TcpConnector, RoundRobin>,
    target_sni: Option<String>,
    is_tls: bool,
    pub streaming: velda_core::StreamingMode,
}

impl std::fmt::Debug for RuntimeUpstream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RuntimeUpstream")
            .field("id", &self.inner.id())
            .field("target_sni", &self.target_sni)
            .field("is_tls", &self.is_tls)
            .field("streaming", &self.streaming)
            .finish()
    }
}

impl std::ops::Deref for RuntimeUpstream {
    type Target = Upstream<TcpConnector, RoundRobin>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl RuntimeUpstream {
    pub fn new(
        inner: Upstream<TcpConnector, RoundRobin>,
        target_sni: Option<String>,
        is_tls: bool,
        streaming: velda_core::StreamingMode,
    ) -> Self {
        Self {
            inner,
            target_sni,
            is_tls,
            streaming,
        }
    }

    #[inline]
    pub fn target_sni(&self) -> Option<&str> {
        self.target_sni.as_deref()
    }

    #[inline]
    pub fn is_tls(&self) -> bool {
        self.is_tls
    }

    #[inline]
    pub fn select_target(&self) -> Option<SocketAddr> {
        self.inner.select_endpoint().ok()
    }
}

pub type TcpUpstream = RuntimeUpstream;
pub type UdpUpstream = RuntimeUpstream;
pub type Http1Upstream = RuntimeUpstream;
pub type Http2Upstream = RuntimeUpstream;
pub type Http3Upstream = RuntimeUpstream;
pub type GrpcUpstream = RuntimeUpstream;
pub type L4Upstream = RuntimeUpstream;

/// Generic single-protocol lookup table.
pub struct SubUpstreamTable<T> {
    entries: HashMap<String, Arc<T>>,
}

impl<T> Clone for SubUpstreamTable<T> {
    fn clone(&self) -> Self {
        Self {
            entries: self.entries.clone(),
        }
    }
}

impl<T> Default for SubUpstreamTable<T> {
    fn default() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }
}

impl<T> std::fmt::Debug for SubUpstreamTable<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SubUpstreamTable")
            .field("keys", &self.entries.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl<T> SubUpstreamTable<T> {
    pub fn new(entries: HashMap<String, Arc<T>>) -> Self {
        Self { entries }
    }

    #[inline]
    pub fn get(&self, id: &str) -> Option<&Arc<T>> {
        self.entries.get(id)
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

pub type TcpUpstreamTable = SubUpstreamTable<TcpUpstream>;
pub type UdpUpstreamTable = SubUpstreamTable<UdpUpstream>;
pub type Http1UpstreamTable = SubUpstreamTable<Http1Upstream>;
pub type Http2UpstreamTable = SubUpstreamTable<Http2Upstream>;
pub type Http3UpstreamTable = SubUpstreamTable<Http3Upstream>;
pub type GrpcUpstreamTable = SubUpstreamTable<GrpcUpstream>;

/// Pre-compiled upstream table divided strictly into 6 protocol-isolated tables.
#[derive(Clone, Default, Debug)]
pub struct UpstreamTable {
    pub tcp: TcpUpstreamTable,
    pub udp: UdpUpstreamTable,
    pub http1: Http1UpstreamTable,
    pub http2: Http2UpstreamTable,
    pub http3: Http3UpstreamTable,
    pub grpc: GrpcUpstreamTable,
}

impl UpstreamTable {
    /// Returns total number of upstreams across all protocol tables.
    pub fn len(&self) -> usize {
        self.tcp.len()
            + self.udp.len()
            + self.http1.len()
            + self.http2.len()
            + self.http3.len()
            + self.grpc.len()
    }

    /// Returns true if all protocol tables are empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Compiles declarative UpstreamConfig entries into protocol-isolated [`UpstreamTable`].
pub fn build_upstreams(configs: &[UpstreamConfig]) -> UpstreamTable {
    let mut tcp_map = HashMap::new();
    let mut udp_map = HashMap::new();
    let mut http1_map = HashMap::new();
    let mut http2_map = HashMap::new();
    let mut http3_map = HashMap::new();
    let mut grpc_map = HashMap::new();

    for config in configs {
        let mut endpoints = Vec::new();
        for (i, ep) in config.endpoints.iter().enumerate() {
            if let Ok(addr) = ep.address.parse::<SocketAddr>() {
                let id = format!("{}-ep-{}", config.id, i);
                endpoints.push(Endpoint::new(id, addr, ep.weight));
            }
        }

        if endpoints.is_empty()
            && let Some(ref target) = config.target
            && let Ok(ip) = target.host.parse::<std::net::IpAddr>()
        {
            let addr = SocketAddr::new(ip, target.port);
            let id = format!("{}-target", config.id);
            endpoints.push(Endpoint::new(id, addr, 1));
        }

        let timeouts = UpstreamTimeouts {
            connect: std::time::Duration::from_millis(config.timeouts.connect_ms),
            idle: std::time::Duration::from_millis(config.timeouts.idle_ms),
            request: config
                .timeouts
                .request_ms
                .map(std::time::Duration::from_millis),
        };

        let target_sni = config.tls.as_ref().and_then(|t| t.sni.first().cloned());
        let is_tls = config.tls.is_some();
        let protocol_str = Arc::from(config.protocol.transport.as_str());
        let inner = Upstream::new_explicit(&config.id, protocol_str, endpoints, timeouts);
        let runtime_upstream = Arc::new(RuntimeUpstream::new(
            inner,
            target_sni,
            is_tls,
            config.protocol.streaming,
        ));

        let transport = config.protocol.transport.to_ascii_lowercase();
        let app = config.protocol.application.to_ascii_lowercase();

        match app.as_str() {
            "grpc" => {
                grpc_map.insert(config.id.clone(), runtime_upstream);
            }
            "http3" => {
                http3_map.insert(config.id.clone(), runtime_upstream);
            }
            "http2" => {
                http2_map.insert(config.id.clone(), runtime_upstream);
            }
            "http1" => {
                http1_map.insert(config.id.clone(), runtime_upstream);
            }
            "raw" if transport == "udp" => {
                udp_map.insert(config.id.clone(), runtime_upstream);
            }
            "raw" => {
                tcp_map.insert(config.id.clone(), runtime_upstream);
            }
            other => {
                tracing::warn!(
                    upstream = %config.id,
                    application = %other,
                    "Unknown or unsupported upstream protocol; skipped"
                );
            }
        }
    }

    UpstreamTable {
        tcp: SubUpstreamTable::new(tcp_map),
        udp: SubUpstreamTable::new(udp_map),
        http1: SubUpstreamTable::new(http1_map),
        http2: SubUpstreamTable::new(http2_map),
        http3: SubUpstreamTable::new(http3_map),
        grpc: SubUpstreamTable::new(grpc_map),
    }
}
