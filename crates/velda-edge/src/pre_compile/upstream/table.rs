//! Protocol-isolated in-memory upstream tables and snapshot builder.

use std::net::SocketAddr;
use std::sync::Arc;

use rustc_hash::FxHashMap;
use velda_core::Endpoint;
use velda_discovery::{
    Discovery, DnsResolverConfig, DnsResolverProvider, HostsFileSource, ResolvConfServerProvider,
    StaticServerProvider, UdpDnsTransport,
};
use velda_sync::post_sync::upstream::UpstreamConfig;
use velda_tls::TlsClientEngine;
use velda_upstream::{Upstream, UpstreamTimeouts};

use super::grpc::{GrpcTcpUpstream, GrpcUdpUpstream};
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

/// Upstream variant in the Layer 4 Raw family (TCP byte stream / UDP datagram).
#[derive(Clone, Debug)]
pub enum RawUpstream {
    Tcp(Arc<TcpUpstream>),
    Udp(Arc<UdpUpstream>),
}

impl RawUpstream {
    #[inline]
    pub fn as_tcp(&self) -> Option<&Arc<TcpUpstream>> {
        match self {
            Self::Tcp(u) => Some(u),
            _ => None,
        }
    }

    #[inline]
    pub fn as_udp(&self) -> Option<&Arc<UdpUpstream>> {
        match self {
            Self::Udp(u) => Some(u),
            _ => None,
        }
    }
}

/// Upstream variant in the Layer 7 HTTP family (HTTP/1.1, HTTP/2, HTTP/3).
#[derive(Clone, Debug)]
pub enum HttpUpstream {
    Http1(Arc<Http1Upstream>),
    Http2(Arc<Http2Upstream>),
    Http3(Arc<Http3Upstream>),
}

impl HttpUpstream {
    #[inline]
    pub fn as_http1(&self) -> Option<&Arc<Http1Upstream>> {
        match self {
            Self::Http1(u) => Some(u),
            _ => None,
        }
    }

    #[inline]
    pub fn as_http2(&self) -> Option<&Arc<Http2Upstream>> {
        match self {
            Self::Http2(u) => Some(u),
            _ => None,
        }
    }

    #[inline]
    pub fn as_http3(&self) -> Option<&Arc<Http3Upstream>> {
        match self {
            Self::Http3(u) => Some(u),
            _ => None,
        }
    }
}

/// Upstream variant in the Layer 7 gRPC family (gRPC over TCP / gRPC over UDP).
#[derive(Clone, Debug)]
pub enum GrpcUpstream {
    Tcp(Arc<GrpcTcpUpstream>),
    Udp(Arc<GrpcUdpUpstream>),
}

impl GrpcUpstream {
    #[inline]
    pub fn as_tcp(&self) -> Option<&Arc<GrpcTcpUpstream>> {
        match self {
            Self::Tcp(u) => Some(u),
            _ => None,
        }
    }

    #[inline]
    pub fn as_udp(&self) -> Option<&Arc<GrpcUdpUpstream>> {
        match self {
            Self::Udp(u) => Some(u),
            _ => None,
        }
    }
}

/// Pre-compiled upstream table grouped strictly by ProtocolFamily.
///
/// Guaranteed zero cross-protocol lookup overhead and zero dynamic casting on the request serving hot path.
#[derive(Clone, Default, Debug)]
pub struct UpstreamTable {
    /// [PRE-COMPILED]: Protocol table for L4 Raw streams and datagrams (ProtocolFamily::Raw).
    pub raw: SubUpstreamTable<RawUpstream>,
    /// [PRE-COMPILED]: Protocol table for L7 HTTP web traffic (ProtocolFamily::Http).
    pub http: SubUpstreamTable<HttpUpstream>,
    /// [PRE-COMPILED]: Protocol table for L7 gRPC RPC endpoints (ProtocolFamily::Grpc).
    pub grpc: SubUpstreamTable<GrpcUpstream>,
}

impl UpstreamTable {
    /// Returns total number of upstreams across all protocol families.
    #[inline]
    pub fn len(&self) -> usize {
        self.raw.len() + self.http.len() + self.grpc.len()
    }

    /// Returns true if all protocol tables are empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Retrieves an upstream processor in the Raw family.
    #[inline]
    pub fn get_raw(&self, id: &str) -> Option<&Arc<RawUpstream>> {
        self.raw.get(id)
    }

    /// Retrieves an upstream processor in the HTTP family.
    #[inline]
    pub fn get_http(&self, id: &str) -> Option<&Arc<HttpUpstream>> {
        self.http.get(id)
    }

    /// Retrieves an upstream processor in the gRPC family.
    #[inline]
    pub fn get_grpc(&self, id: &str) -> Option<&Arc<GrpcUpstream>> {
        self.grpc.get(id)
    }
}

/// Compiles declarative UpstreamConfig entries into protocol-family partitioned [`UpstreamTable`].
///
/// Pre-bakes upstream TLS client engines and streaming strategies into each Upstream instance
/// to guarantee zero runtime lookups on the request serving hot path.
///
/// Upstreams declared with `mode: "dns"` or hostname targets are bound to dynamic background
/// DNS discovery sized according to [`DnsResolverConfig`].
pub fn build_upstreams(
    configs: &[UpstreamConfig],
    tls_client: Option<&TlsClientEngine>,
    dns_config: &DnsResolverConfig,
) -> UpstreamTable {
    let mut raw_map = FxHashMap::default();
    let mut http_map = FxHashMap::default();
    let mut grpc_map = FxHashMap::default();

    let shared_tls_client = tls_client.map(|c| Arc::new(c.clone()));

    // Check if any upstream requires dynamic DNS resolution
    let has_dns_upstreams = configs.iter().any(|c| {
        c.mode.eq_ignore_ascii_case("dns")
            || c.target
                .as_ref()
                .is_some_and(|t| t.host.parse::<std::net::IpAddr>().is_err())
    });

    // Lazily initialize shared DNS dependencies only when dynamic resolution is declared
    let dns_env = if has_dns_upstreams {
        let system_hosts = HostsFileSource::load_system();
        let transport = UdpDnsTransport::new();
        let default_servers = ResolvConfServerProvider::load_system();
        let default_resolver = Arc::new(DnsResolverProvider::with_hosts_and_config(
            default_servers,
            system_hosts.clone(),
            transport,
            *dns_config,
        ));
        Some((system_hosts, transport, default_resolver))
    } else {
        None
    };

    for config in configs {
        let mut endpoints = Vec::new();
        for (i, ep) in config.endpoints.iter().enumerate() {
            if let Ok(addr) = ep.address.parse::<SocketAddr>() {
                let id = format!("{}-ep-{}", config.id, i);
                endpoints.push(Endpoint::new(id, addr, ep.weight));
            }
        }

        let is_dns_mode = config.mode.eq_ignore_ascii_case("dns");
        let mut dns_hostname_target: Option<(String, u16)> = None;

        if let Some(ref target) = config.target {
            if let Ok(ip) = target.host.parse::<std::net::IpAddr>() {
                if endpoints.is_empty() {
                    let addr = SocketAddr::new(ip, target.port);
                    let id = format!("{}-target", config.id);
                    endpoints.push(Endpoint::new(id, addr, 1));
                }
            } else {
                dns_hostname_target = Some((target.host.clone(), target.port));
            }
        }

        let discovery = if let Some((ref system_hosts, transport, ref default_resolver)) = dns_env {
            if (is_dns_mode || dns_hostname_target.is_some())
                && let Some((host, port)) = dns_hostname_target
            {
                let refresh_interval = config
                    .resolver
                    .as_ref()
                    .and_then(|r| r.refresh_interval_ms)
                    .map(std::time::Duration::from_millis)
                    .unwrap_or(dns_config.positive_ttl);

                let custom_nameservers: Vec<SocketAddr> = config
                    .resolver
                    .as_ref()
                    .map(|r| {
                        r.nameservers
                            .iter()
                            .filter_map(|ns| {
                                if let Ok(sa) = ns.parse::<SocketAddr>() {
                                    Some(sa)
                                } else if let Ok(ip) = ns.parse::<std::net::IpAddr>() {
                                    Some(SocketAddr::new(ip, 53))
                                } else {
                                    None
                                }
                            })
                            .collect()
                    })
                    .unwrap_or_default();

                if !custom_nameservers.is_empty() {
                    let server_provider = StaticServerProvider::from_addresses(custom_nameservers);
                    let resolver = Arc::new(DnsResolverProvider::with_hosts_and_config(
                        server_provider,
                        system_hosts.clone(),
                        transport,
                        *dns_config,
                    ));
                    Discovery::new_dns_with_initial(
                        host,
                        port,
                        refresh_interval,
                        resolver,
                        endpoints,
                    )
                } else {
                    let resolver = Arc::clone(default_resolver);
                    Discovery::new_dns_with_initial(
                        host,
                        port,
                        refresh_interval,
                        resolver,
                        endpoints,
                    )
                }
            } else {
                Discovery::new_explicit(endpoints)
            }
        } else {
            Discovery::new_explicit(endpoints)
        };

        let timeouts = UpstreamTimeouts {
            connect: std::time::Duration::from_millis(config.timeouts.connect_ms),
            idle: std::time::Duration::from_millis(config.timeouts.idle_ms),
            request: config
                .timeouts
                .request_ms
                .map(std::time::Duration::from_millis),
        };

        let target_sni: Option<Arc<str>> = config
            .tls
            .as_ref()
            .and_then(|t| t.sni.first())
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(Arc::from);
        let is_tls = config.tls.is_some();
        let protocol_str = Arc::from(config.protocol.transport.as_str());
        let balancer = LbAlgorithm::from_name(&config.load_balancer.algorithm);

        // Tier-aware Pool Sizing:
        // - CpuTier dictates concurrency_shards (lock distribution)
        // - MemoryTier dictates baseline capacity (max_idle_per_key and max_concurrent_streams)
        // - UpstreamConfig::pool takes precedence if explicitly specified by operator in upstream.json
        let topology = velda_core::global_hardware_topology();
        let cpu_tier = topology.cpu_tier();
        let mem_tier = topology.memory_tier();

        let default_shards = velda_connection_pool::concurrency_shards_for_cpu_tier(cpu_tier);
        let default_max_idle = velda_connection_pool::max_idle_per_key_for_mem_tier(mem_tier);
        let default_max_streams =
            velda_connection_pool::max_concurrent_streams_for_mem_tier(mem_tier);

        let pool_opt = config.pool.as_ref();
        let shard_count = pool_opt
            .and_then(|p| p.concurrency_shards)
            .map(|s| s.max(1).next_power_of_two())
            .unwrap_or(default_shards);
        let max_idle = pool_opt
            .and_then(|p| p.max_idle_per_key)
            .unwrap_or(default_max_idle);
        let max_streams = pool_opt
            .and_then(|p| p.max_concurrent_streams)
            .unwrap_or(default_max_streams);
        let idle_timeout = pool_opt
            .and_then(|p| p.idle_timeout_ms.map(std::time::Duration::from_millis))
            .unwrap_or(timeouts.idle);
        let max_lifetime = pool_opt
            .and_then(|p| p.max_lifetime_ms.map(std::time::Duration::from_millis))
            .or(Some(velda_connection_pool::DEFAULT_MAX_LIFETIME));

        let pool_config = velda_connection_pool::PoolConfig {
            max_idle_per_key: max_idle,
            max_concurrent_streams: max_streams,
            idle_timeout,
            max_lifetime,
        };

        let inner = Upstream::new(&config.id, protocol_str, discovery, balancer, timeouts);

        let transport = config.protocol.transport.to_ascii_lowercase();
        let app = config.protocol.application.to_ascii_lowercase();

        // Compile-time TLS identity validation (never accept TLS without an explicit SNI,
        // never fall back to guessed names, never silently downgrade to cleartext):
        // - QUIC upstreams (http3, grpc/udp) always handshake TLS and require an SNI.
        // - TCP upstreams (http1, http2, grpc/tcp) with TLS require an SNI and a client engine.
        // - Declared ALPN, if any, must contain the protocol id the upstream speaks. ALPN never
        //   selects the protocol; the connector only validates the negotiated value.
        let is_quic = app == "http3" || (app == "grpc" && transport == "udp");
        let expected_alpn = match app.as_str() {
            "http1" => Some("http/1.1"),
            "http2" => Some("h2"),
            "grpc" if !is_quic => Some("h2"),
            "http3" => Some("h3"),
            _ => None,
        };
        let alpn_mismatch = match (config.tls.as_ref(), expected_alpn) {
            (Some(t), Some(expected)) => {
                !t.alpn.is_empty()
                    && !t
                        .alpn
                        .iter()
                        .any(|a| a.trim().eq_ignore_ascii_case(expected))
            }
            _ => false,
        };
        let needs_tls_identity = is_tls && (is_quic || expected_alpn.is_some());
        let tls_reject = if is_quic && target_sni.is_none() {
            Some("QUIC upstream requires tls.sni")
        } else if needs_tls_identity && target_sni.is_none() {
            Some("TLS upstream requires a non-empty tls.sni")
        } else if needs_tls_identity && !is_quic && shared_tls_client.is_none() {
            Some("TLS upstream declared but no TLS client engine compiled")
        } else if alpn_mismatch {
            Some("tls.alpn does not contain the protocol id spoken by this upstream")
        } else {
            None
        };
        if let Some(reason) = tls_reject {
            tracing::error!(upstream = %config.id, application = %app, reason, "Upstream rejected at compile time");
            continue;
        }

        // TLS identity for TCP-based upstreams; `None` means cleartext.
        let tls = match (is_tls, shared_tls_client.as_ref(), target_sni.clone()) {
            (true, Some(engine), Some(sni)) => Some((Arc::clone(engine), sni)),
            _ => None,
        };

        match app.as_str() {
            "grpc" => {
                if transport == "udp" {
                    let Some(sni) = target_sni else { continue };
                    grpc_map.insert(
                        config.id.clone(),
                        Arc::new(GrpcUpstream::Udp(Arc::new(GrpcUdpUpstream::new(
                            inner,
                            sni,
                            config.protocol.streaming,
                            shard_count,
                            max_streams,
                        )))),
                    );
                } else {
                    let grpc_acceleration =
                        velda_grpc::tcp::client::GrpcAccelerationPath::for_topology(
                            topology,
                            timeouts.connect,
                            timeouts.idle,
                            is_tls,
                        );
                    grpc_map.insert(
                        config.id.clone(),
                        Arc::new(GrpcUpstream::Tcp(Arc::new(GrpcTcpUpstream::new(
                            inner,
                            tls,
                            config.protocol.streaming,
                            shard_count,
                            max_streams,
                            grpc_acceleration,
                        )))),
                    );
                }
            }
            "http3" => {
                let Some(sni) = target_sni else { continue };
                http_map.insert(
                    config.id.clone(),
                    Arc::new(HttpUpstream::Http3(Arc::new(Http3Upstream::new(
                        inner,
                        sni,
                        config.protocol.streaming,
                        shard_count,
                        max_streams,
                    )))),
                );
            }
            "http2" => {
                let http2_acceleration = velda_http2::Http2AccelerationPath::for_topology(
                    topology,
                    timeouts.connect,
                    timeouts.idle,
                    is_tls,
                );
                http_map.insert(
                    config.id.clone(),
                    Arc::new(HttpUpstream::Http2(Arc::new(Http2Upstream::new(
                        inner,
                        tls,
                        config.protocol.streaming,
                        shard_count,
                        max_idle,
                        max_streams,
                        http2_acceleration,
                    )))),
                );
            }
            "http" | "http1" => {
                let http1_acceleration = velda_http1::Http1AccelerationPath::for_topology(
                    topology,
                    timeouts.connect,
                    timeouts.idle,
                    is_tls,
                );
                http_map.insert(
                    config.id.clone(),
                    Arc::new(HttpUpstream::Http1(Arc::new(Http1Upstream::new(
                        inner,
                        tls,
                        config.protocol.streaming,
                        pool_config,
                        shard_count,
                        http1_acceleration,
                    )))),
                );
            }
            "raw" | "udp" if transport == "udp" => {
                let udp_acceleration = super::udp::UdpAccelerationPath::for_topology(topology);
                raw_map.insert(
                    config.id.clone(),
                    Arc::new(RawUpstream::Udp(Arc::new(UdpUpstream::new(
                        inner,
                        udp_acceleration,
                    )))),
                );
            }
            "raw" | "tcp" => {
                let tcp_acceleration =
                    super::tcp::TcpAccelerationPath::for_topology(topology, &timeouts, is_tls);
                raw_map.insert(
                    config.id.clone(),
                    Arc::new(RawUpstream::Tcp(Arc::new(TcpUpstream::new(
                        inner,
                        tcp_acceleration,
                    )))),
                );
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
        raw: SubUpstreamTable::new(raw_map),
        http: SubUpstreamTable::new(http_map),
        grpc: SubUpstreamTable::new(grpc_map),
    }
}

/// Compiles declarative UpstreamConfig entries using default hardware topology DNS configuration.
pub fn build_upstreams_default(
    configs: &[UpstreamConfig],
    tls_client: Option<&TlsClientEngine>,
) -> UpstreamTable {
    let tier = velda_core::global_hardware_topology().memory_tier();
    let dns_config = DnsResolverConfig::for_tier(tier);
    build_upstreams(configs, tls_client, &dns_config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use velda_sync::post_sync::upstream::{
        DnsTarget, EndpointConfig, LoadBalancerConfig, ResolverConfig, UpstreamProtocolConfig,
        UpstreamTimeouts as SyncTimeouts,
    };

    fn make_test_upstream(id: &str, app: &str, mode: &str) -> UpstreamConfig {
        UpstreamConfig {
            id: id.to_string(),
            mode: mode.to_string(),
            protocol: UpstreamProtocolConfig {
                transport: "tcp".to_string(),
                application: app.to_string(),
                streaming: velda_core::StreamingMode::disabled(),
            },
            target: None,
            resolver: None,
            endpoints: vec![EndpointConfig {
                address: "127.0.0.1:8080".to_string(),
                weight: 1,
            }],
            load_balancer: LoadBalancerConfig {
                algorithm: "round_robin".to_string(),
            },
            timeouts: SyncTimeouts {
                connect_ms: 1000,
                idle_ms: 5000,
                request_ms: Some(3000),
            },
            health_check: None,
            tls: None,
            pool: None,
        }
    }

    fn test_tls(sni: &[&str]) -> Option<velda_sync::post_sync::upstream::UpstreamTlsConfig> {
        Some(velda_sync::post_sync::upstream::UpstreamTlsConfig {
            ca_pem: None,
            client_cert_pem: None,
            client_key_pem: None,
            versions: vec!["tls1.3".into()],
            alpn: vec![],
            sni: sni.iter().map(|s| s.to_string()).collect(),
            insecure_skip_verify: false,
        })
    }

    #[test]
    fn test_explicit_endpoints() {
        let mut u_udp = make_test_upstream("u_udp", "raw", "endpoints");
        u_udp.protocol.transport = "udp".to_string();

        let u_tcp = make_test_upstream("u_tcp", "raw", "endpoints");

        let mut u_h3 = make_test_upstream("u_h3", "http3", "endpoints");
        u_h3.protocol.transport = "quic".to_string();
        u_h3.tls = test_tls(&["h3.internal"]);

        let mut u_grpc_udp = make_test_upstream("u_grpc_udp", "grpc", "endpoints");
        u_grpc_udp.protocol.transport = "udp".to_string();
        u_grpc_udp.tls = test_tls(&["grpc.internal"]);

        let configs = vec![
            make_test_upstream("u1", "http1", "endpoints"),
            make_test_upstream("u2", "http2", "endpoints"),
            make_test_upstream("u3", "grpc", "endpoints"),
            u_udp,
            u_tcp,
            u_h3,
            u_grpc_udp,
        ];

        let table = build_upstreams_default(&configs, None);
        assert_eq!(table.len(), 7);
        assert!(table.http.get("u1").and_then(|u| u.as_http1()).is_some());
        assert!(table.http.get("u2").and_then(|u| u.as_http2()).is_some());
        assert!(table.grpc.get("u3").and_then(|u| u.as_tcp()).is_some());
        assert!(
            table
                .grpc
                .get("u_grpc_udp")
                .and_then(|u| u.as_udp())
                .is_some()
        );
        assert!(table.raw.get("u_udp").and_then(|u| u.as_udp()).is_some());
        assert!(table.raw.get("u_tcp").and_then(|u| u.as_tcp()).is_some());
        assert!(table.http.get("u_h3").and_then(|u| u.as_http3()).is_some());
        assert_eq!(
            &*table
                .http
                .get("u_h3")
                .and_then(|u| u.as_http3())
                .unwrap()
                .target_sni,
            "h3.internal"
        );
    }

    #[test]
    fn test_reject_invalid_tls_identity() {
        // QUIC upstream without SNI: no fallback to upstream id.
        let mut h3_no_sni = make_test_upstream("h3_no_sni", "http3", "endpoints");
        h3_no_sni.protocol.transport = "quic".to_string();

        // QUIC upstream with blank SNI.
        let mut h3_blank = make_test_upstream("h3_blank", "http3", "endpoints");
        h3_blank.tls = test_tls(&["  "]);

        // TLS http1 without SNI.
        let mut h1_no_sni = make_test_upstream("h1_no_sni", "http1", "endpoints");
        h1_no_sni.tls = test_tls(&[]);

        // TLS http2 without a compiled client engine must not silently downgrade to h2c.
        let mut h2_tls = make_test_upstream("h2_tls", "http2", "endpoints");
        h2_tls.tls = test_tls(&["h2.internal"]);

        let table = build_upstreams_default(&[h3_no_sni, h3_blank, h1_no_sni, h2_tls], None);
        assert_eq!(table.len(), 0);
    }

    fn test_engine(sni: &str) -> TlsClientEngine {
        TlsClientEngine::new(&[velda_tls::ClientTlsConfig {
            sni: vec![sni.to_string()],
            versions: vec!["tls1.3".into()],
            insecure_skip_verify: true,
            ..Default::default()
        }])
        .unwrap()
    }

    #[test]
    fn test_tls_http2_grpc() {
        let mut h2 = make_test_upstream("h2", "http2", "endpoints");
        h2.tls = test_tls(&["h2.internal"]);
        let mut grpc = make_test_upstream("grpc", "grpc", "endpoints");
        grpc.tls = test_tls(&["grpc.internal"]);

        let engine = test_engine("h2.internal");
        let table = build_upstreams_default(&[h2, grpc], Some(&engine));
        assert!(table.http.get("h2").and_then(|u| u.as_http2()).is_some());
        assert!(table.grpc.get("grpc").and_then(|u| u.as_tcp()).is_some());
    }

    #[test]
    fn test_reject_alpn_mismatch() {
        let engine = test_engine("h2.internal");

        // ALPN is validation only: declaring h3-only ALPN on an http2 upstream is a config error.
        let mut h2 = make_test_upstream("h2", "http2", "endpoints");
        h2.tls = test_tls(&["h2.internal"]);
        h2.tls.as_mut().unwrap().alpn = vec!["http/1.1".into()];

        let mut h1 = make_test_upstream("h1", "http1", "endpoints");
        h1.tls = test_tls(&["h2.internal"]);
        h1.tls.as_mut().unwrap().alpn = vec!["h2".into()];

        let table = build_upstreams_default(&[h2, h1], Some(&engine));
        assert_eq!(table.len(), 0);
    }

    #[tokio::test]
    async fn test_dns_resolver() {
        let mut u_dns = make_test_upstream("u_dns", "http1", "dns");
        u_dns.endpoints.clear();
        u_dns.target = Some(DnsTarget {
            host: "api.internal.service".to_string(),
            port: 9000,
        });
        u_dns.resolver = Some(ResolverConfig {
            mode: Some("custom".to_string()),
            nameservers: vec!["1.1.1.1:53".to_string(), "8.8.8.8:53".to_string()],
            refresh_interval_ms: Some(15000),
        });

        let dns_config = DnsResolverConfig {
            positive_ttl: std::time::Duration::from_secs(10),
            ..Default::default()
        };

        let table = build_upstreams(&[u_dns], None, &dns_config);
        assert_eq!(table.len(), 1);
        let http1 = table
            .http
            .get("u_dns")
            .and_then(|u| u.as_http1())
            .expect("u_dns registered");
        assert_eq!(http1.id(), "u_dns");
    }

    #[test]
    fn test_operator_pool_override() {
        use velda_sync::post_sync::upstream::UpstreamPoolConfig;

        // Upstream with operator-specified pool overrides
        let mut u_override = make_test_upstream("u_override", "http2", "endpoints");
        u_override.pool = Some(UpstreamPoolConfig {
            concurrency_shards: Some(16),
            max_idle_per_key: Some(77),
            max_concurrent_streams: Some(333),
            idle_timeout_ms: Some(40000),
            max_lifetime_ms: Some(7200000),
        });

        // Upstream without pool config (relies on hardware tier fallback)
        let u_default = make_test_upstream("u_default", "http2", "endpoints");

        let table = build_upstreams_default(&[u_override, u_default], None);
        let h2_override = table
            .http
            .get("u_override")
            .and_then(|u| u.as_http2())
            .unwrap();
        assert_eq!(h2_override.max_concurrent_streams, 333);

        let h2_default = table
            .http
            .get("u_default")
            .and_then(|u| u.as_http2())
            .unwrap();
        let mem_tier = velda_core::global_hardware_topology().memory_tier();
        let expected_streams = velda_connection_pool::max_concurrent_streams_for_mem_tier(mem_tier);
        assert_eq!(h2_default.max_concurrent_streams, expected_streams);
    }
}
