//! Protocol-isolated in-memory upstream tables and snapshot builder.

use std::net::SocketAddr;
use std::sync::Arc;

use rustc_hash::FxHashMap;
use velda_core::Endpoint;
use velda_discovery::Discovery;
use velda_sync::post_sync::upstream::UpstreamConfig;
use velda_tls::TlsClientEngine;
use velda_upstream::{TcpConnector, Upstream, UpstreamTimeouts};

use super::grpc::GrpcUpstream;
use super::http1::Http1Upstream;
use super::http2::Http2Upstream;
use super::http3::Http3Upstream;
use super::lb::LbAlgorithm;
use super::tcp::TcpUpstream;
use super::udp::UdpUpstream;

/// Pre-compiled single-protocol lookup table providing lock-free O(1) reads.
///
/// Pre-constructed during snapshot build; maps upstream_id -> Arc<T>.
pub struct SubUpstreamTable<T> {
    /// [PRE-COMPILED]: In-memory mapping of upstream identifiers to protocol processors.
    entries: FxHashMap<String, Arc<T>>,
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
            entries: FxHashMap::default(),
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
    /// Creates a new protocol sub-table from an owned entry map.
    pub fn new(entries: FxHashMap<String, Arc<T>>) -> Self {
        Self { entries }
    }

    /// Retrieves an upstream processor by identifier.
    #[inline]
    pub fn get(&self, id: &str) -> Option<&Arc<T>> {
        self.entries.get(id)
    }

    /// Returns the number of upstreams in this sub-table.
    #[inline]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns whether this sub-table is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// Pre-compiled upstream table divided strictly into 6 protocol-isolated tables.
///
/// Guaranteed zero cross-protocol lookup overhead and zero dynamic casting on the request serving hot path.
#[derive(Clone, Default, Debug)]
pub struct UpstreamTable {
    /// [PRE-COMPILED]: Protocol table for L4 raw TCP stream forwarding.
    pub tcp: SubUpstreamTable<TcpUpstream>,
    /// [PRE-COMPILED]: Protocol table for L4 raw UDP datagram forwarding.
    pub udp: SubUpstreamTable<UdpUpstream>,
    /// [PRE-COMPILED]: Protocol table for L7 HTTP/1.1 backend pipelines.
    pub http1: SubUpstreamTable<Http1Upstream>,
    /// [PRE-COMPILED]: Protocol table for L7 HTTP/2 multiplexed streams.
    pub http2: SubUpstreamTable<Http2Upstream>,
    /// [PRE-COMPILED]: Protocol table for L7 HTTP/3 QUIC streams.
    pub http3: SubUpstreamTable<Http3Upstream>,
    /// [PRE-COMPILED]: Protocol table for L7 gRPC RPC endpoints.
    pub grpc: SubUpstreamTable<GrpcUpstream>,
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
///
/// Pre-bakes upstream TLS client engines and streaming strategies into each Upstream instance
/// to guarantee zero runtime lookups on the request serving hot path.
pub fn build_upstreams(
    configs: &[UpstreamConfig],
    tls_client: Option<&TlsClientEngine>,
) -> UpstreamTable {
    let mut tcp_map = FxHashMap::default();
    let mut udp_map = FxHashMap::default();
    let mut http1_map = FxHashMap::default();
    let mut http2_map = FxHashMap::default();
    let mut http3_map = FxHashMap::default();
    let mut grpc_map = FxHashMap::default();

    let shared_tls_client = tls_client.map(|c| Arc::new(c.clone()));

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
        let balancer = LbAlgorithm::from_name(&config.load_balancer.algorithm);
        let discovery = Discovery::new_explicit(endpoints);
        let inner = Upstream::new(
            &config.id,
            protocol_str,
            discovery,
            balancer,
            timeouts,
            Arc::new(TcpConnector),
        );

        let transport = config.protocol.transport.to_ascii_lowercase();
        let app = config.protocol.application.to_ascii_lowercase();

        match app.as_str() {
            "grpc" => {
                grpc_map.insert(
                    config.id.clone(),
                    Arc::new(GrpcUpstream::new(inner, config.protocol.streaming)),
                );
            }
            "http3" => {
                http3_map.insert(
                    config.id.clone(),
                    Arc::new(Http3Upstream::new(
                        inner,
                        target_sni,
                        config.protocol.streaming,
                    )),
                );
            }
            "http2" => {
                http2_map.insert(
                    config.id.clone(),
                    Arc::new(Http2Upstream::new(inner, config.protocol.streaming)),
                );
            }
            "http1" => {
                let tls_engine = if is_tls {
                    shared_tls_client.clone()
                } else {
                    None
                };
                http1_map.insert(
                    config.id.clone(),
                    Arc::new(Http1Upstream::new(
                        inner,
                        target_sni,
                        is_tls,
                        tls_engine,
                        config.protocol.streaming,
                    )),
                );
            }
            "raw" if transport == "udp" => {
                udp_map.insert(config.id.clone(), Arc::new(UdpUpstream::new(inner)));
            }
            "raw" => {
                tcp_map.insert(config.id.clone(), Arc::new(TcpUpstream::new(inner)));
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
