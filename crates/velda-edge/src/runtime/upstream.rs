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
use std::sync::atomic::{AtomicUsize, Ordering};

use velda_sync::post_sync::upstream::{UpstreamConfig, UpstreamTlsConfig};
use velda_upstream::{Endpoint, RoundRobin, TcpConnector, Upstream, UpstreamTimeouts};

/// Concrete compiled L4 TCP upstream managing socket endpoints, health, and pooling without TLS.
pub type TcpUpstream = Upstream<TcpConnector, RoundRobin>;
pub type L4Upstream = TcpUpstream;

/// Represents a compiled target endpoint resource for protocol-isolated routing.
#[derive(Debug)]
pub struct ProtocolUpstream {
    pub id: String,
    pub endpoints: Vec<SocketAddr>,
    pub timeouts: UpstreamTimeouts,
    pub tls: Option<UpstreamTlsConfig>,
    pub target_sni: Option<String>,
    rr_index: AtomicUsize,
}

impl ProtocolUpstream {
    pub fn new(
        id: impl Into<String>,
        endpoints: Vec<SocketAddr>,
        timeouts: UpstreamTimeouts,
        tls: Option<UpstreamTlsConfig>,
    ) -> Self {
        let target_sni = tls.as_ref().and_then(|t| t.sni.first().cloned());
        Self {
            id: id.into(),
            endpoints,
            timeouts,
            tls,
            target_sni,
            rr_index: AtomicUsize::new(0),
        }
    }

    /// Returns the primary target SNI if TLS is configured for this upstream.
    #[inline]
    pub fn target_sni(&self) -> Option<&str> {
        self.target_sni.as_deref()
    }

    /// Returns whether this upstream requires a TLS connection.
    #[inline]
    pub fn is_tls(&self) -> bool {
        self.tls.is_some()
    }

    /// Selects an eligible target backend address using round-robin.
    #[inline]
    pub fn select_target(&self) -> Option<SocketAddr> {
        if self.endpoints.is_empty() {
            return None;
        }
        if self.endpoints.len() == 1 {
            return Some(self.endpoints[0]);
        }
        let idx = self.rr_index.fetch_add(1, Ordering::Relaxed);
        Some(self.endpoints[idx % self.endpoints.len()])
    }
}

pub type UdpUpstream = ProtocolUpstream;
pub type Http1Upstream = ProtocolUpstream;
pub type Http2Upstream = ProtocolUpstream;
pub type Http3Upstream = ProtocolUpstream;
pub type GrpcUpstream = ProtocolUpstream;

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
        let mut addrs = Vec::new();
        for (i, ep) in config.endpoints.iter().enumerate() {
            if let Ok(addr) = ep.address.parse::<SocketAddr>() {
                let id = format!("{}-ep-{}", config.id, i);
                endpoints.push(Endpoint::new(id, addr, ep.weight));
                addrs.push(addr);
            }
        }

        if endpoints.is_empty()
            && let Some(ref target) = config.target
            && let Ok(ip) = target.host.parse::<std::net::IpAddr>()
        {
            let addr = SocketAddr::new(ip, target.port);
            let id = format!("{}-target", config.id);
            endpoints.push(Endpoint::new(id, addr, 1));
            addrs.push(addr);
        }

        let timeouts = UpstreamTimeouts {
            connect: std::time::Duration::from_millis(config.timeouts.connect_ms),
            idle: std::time::Duration::from_millis(config.timeouts.idle_ms),
            request: config
                .timeouts
                .request_ms
                .map(std::time::Duration::from_millis),
        };

        let transport = config.protocol.transport.to_ascii_lowercase();
        let app = config.protocol.application.to_ascii_lowercase();

        match app.as_str() {
            "grpc" => {
                grpc_map.insert(
                    config.id.clone(),
                    Arc::new(GrpcUpstream::new(
                        &config.id,
                        addrs,
                        timeouts,
                        config.tls.clone(),
                    )),
                );
            }
            "http3" => {
                http3_map.insert(
                    config.id.clone(),
                    Arc::new(Http3Upstream::new(
                        &config.id,
                        addrs,
                        timeouts,
                        config.tls.clone(),
                    )),
                );
            }
            "http2" => {
                http2_map.insert(
                    config.id.clone(),
                    Arc::new(Http2Upstream::new(
                        &config.id,
                        addrs,
                        timeouts,
                        config.tls.clone(),
                    )),
                );
            }
            "http1" => {
                http1_map.insert(
                    config.id.clone(),
                    Arc::new(Http1Upstream::new(
                        &config.id,
                        addrs,
                        timeouts,
                        config.tls.clone(),
                    )),
                );
            }
            "raw" if transport == "udp" => {
                udp_map.insert(
                    config.id.clone(),
                    Arc::new(UdpUpstream::new(&config.id, addrs, timeouts, None)),
                );
            }
            "raw" => {
                let protocol: Arc<str> = Arc::from(config.protocol.transport.as_str());
                let upstream = Arc::new(TcpUpstream::new_explicit(
                    &config.id, protocol, endpoints, timeouts,
                ));
                tcp_map.insert(config.id.clone(), upstream);
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
