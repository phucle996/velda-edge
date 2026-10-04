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
///
/// Upstreams declared with `mode: "dns"` or hostname targets are bound to dynamic background
/// DNS discovery sized according to [`DnsResolverConfig`].
pub fn build_upstreams(
    configs: &[UpstreamConfig],
    tls_client: Option<&TlsClientEngine>,
    dns_config: &DnsResolverConfig,
) -> UpstreamTable {
    let mut tcp_map = FxHashMap::default();
    let mut udp_map = FxHashMap::default();
    let mut http1_map = FxHashMap::default();
    let mut http2_map = FxHashMap::default();
    let mut http3_map = FxHashMap::default();
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

        let target_sni = config.tls.as_ref().and_then(|t| t.sni.first().cloned());
        let is_tls = config.tls.is_some();
        let protocol_str = Arc::from(config.protocol.transport.as_str());
        let balancer = LbAlgorithm::from_name(&config.load_balancer.algorithm);
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
        }
    }

    #[test]
    fn test_build_upstreams_explicit_endpoints() {
        let configs = vec![
            make_test_upstream("u1", "http1", "endpoints"),
            make_test_upstream("u2", "http2", "endpoints"),
            make_test_upstream("u3", "grpc", "endpoints"),
        ];

        let table = build_upstreams_default(&configs, None);
        assert_eq!(table.len(), 3);
        assert!(table.http1.get("u1").is_some());
        assert!(table.http2.get("u2").is_some());
        assert!(table.grpc.get("u3").is_some());
    }

    #[tokio::test]
    async fn test_build_upstreams_dns_mode_and_custom_resolver() {
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
        let http1 = table.http1.get("u_dns").expect("u_dns registered");
        assert_eq!(http1.id(), "u_dns");
    }
}
