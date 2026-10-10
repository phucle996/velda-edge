//! Upstream snapshot builder compiling declarative configuration into in-memory tables.

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
use super::http::{Http1Upstream, Http2Upstream, Http3Upstream};
use super::lb::LbAlgorithm;
use super::raw::{TcpAccelerationPath, TcpUpstream, UdpAccelerationPath, UdpUpstream};
use super::table::{GrpcUpstream, HttpUpstream, RawUpstream, SubUpstreamTable, UpstreamTable};

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

        // Compile-time TLS identity validation
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
                        max_idle,
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
                let udp_acceleration = UdpAccelerationPath::for_topology(topology);
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
                    TcpAccelerationPath::for_topology(topology, &timeouts, is_tls);
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
    fn test_build_upstreams_empty() {
        let table = build_upstreams_default(&[], None);
        assert!(table.is_empty());
        assert_eq!(table.len(), 0);
    }

    #[test]
    fn test_build_upstreams_protocols() {
        let configs = vec![
            make_test_upstream("u-http1", "http1", "static"),
            make_test_upstream("u-http2", "http2", "static"),
            make_test_upstream("u-tcp", "raw", "static"),
            {
                let mut u = make_test_upstream("u-udp", "raw", "static");
                u.protocol.transport = "udp".to_string();
                u
            },
            make_test_upstream("u-grpc-tcp", "grpc", "static"),
            {
                let mut u = make_test_upstream("u-grpc-udp", "grpc", "static");
                u.protocol.transport = "udp".to_string();
                u.tls = test_tls(&["example.com"]);
                u
            },
            {
                let mut u = make_test_upstream("u-http3", "http3", "static");
                u.protocol.transport = "udp".to_string();
                u.tls = test_tls(&["example.com"]);
                u
            },
        ];

        let table = build_upstreams_default(&configs, None);
        assert_eq!(table.raw.len(), 2);
        assert_eq!(table.http.len(), 3);
        assert_eq!(table.grpc.len(), 2);
        assert_eq!(table.len(), 7);

        assert!(table.get_raw("u-tcp").is_some());
        assert!(table.get_raw("u-udp").is_some());
        assert!(table.get_http("u-http1").is_some());
        assert!(table.get_http("u-http2").is_some());
        assert!(table.get_http("u-http3").is_some());
        assert!(table.get_grpc("u-grpc-tcp").is_some());
        assert!(table.get_grpc("u-grpc-udp").is_some());
    }

    #[test]
    fn test_build_upstreams_dns_hostname_target() {
        let mut cfg = make_test_upstream("u-dns", "http1", "dns");
        cfg.target = Some(DnsTarget {
            host: "backend.local".to_string(),
            port: 8080,
        });
        cfg.resolver = Some(ResolverConfig {
            mode: None,
            nameservers: vec!["1.1.1.1:53".to_string()],
            refresh_interval_ms: Some(1000),
        });

        let table = build_upstreams_default(&[cfg], None);
        assert_eq!(table.http.len(), 1);
        let u = table.get_http("u-dns").unwrap();
        assert_eq!(u.id(), "u-dns");
    }

    #[test]
    fn test_build_upstreams_rejections() {
        let mut no_sni_quic = make_test_upstream("u-h3-bad", "http3", "static");
        no_sni_quic.tls = test_tls(&[]);

        let mut tls_no_engine = make_test_upstream("u-h1-tls", "http1", "static");
        tls_no_engine.tls = test_tls(&["example.com"]);

        let table = build_upstreams_default(&[no_sni_quic, tls_no_engine], None);
        assert!(table.is_empty());
    }
}
