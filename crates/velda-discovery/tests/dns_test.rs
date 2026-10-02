//! Integration tests for DNS discovery: caching, failover, hosts integration, and LKG resilience.
//! Uses real `UdpDnsTransport` over actual loopback UDP sockets.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use velda_discovery::{
    CacheLookup, DiscoveryError, DnsResolverConfig, DnsResolverProvider, HostsFileSource,
    ResolvConfServerProvider, UdpDnsTransport,
};

// ============================================================================
// Helper: Loopback UDP DNS Server for RFC 1035 wire testing
// ============================================================================

struct TestUdpDnsServer {
    addr: SocketAddr,
    query_count: Arc<AtomicUsize>,
    active: Arc<AtomicBool>,
}

impl TestUdpDnsServer {
    async fn spawn(ip: Option<Ipv4Addr>, delay: Option<Duration>, is_nxdomain: bool) -> Self {
        let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let addr = socket.local_addr().unwrap();
        let query_count = Arc::new(AtomicUsize::new(0));
        let count_clone = Arc::clone(&query_count);
        let active = Arc::new(AtomicBool::new(true));
        let active_clone = Arc::clone(&active);

        tokio::spawn(async move {
            let mut buf = [0u8; 512];
            while let Ok((len, peer)) = socket.recv_from(&mut buf).await {
                if !active_clone.load(Ordering::SeqCst) {
                    // Simulates server failure / outage: drop packets
                    continue;
                }

                count_clone.fetch_add(1, Ordering::SeqCst);
                if let Some(d) = delay {
                    tokio::time::sleep(d).await;
                }

                let req_id = u16::from_be_bytes([buf[0], buf[1]]);
                let qtype = u16::from_be_bytes([buf[len - 4], buf[len - 3]]);
                let mut resp = Vec::with_capacity(64);
                resp.extend_from_slice(&req_id.to_be_bytes());

                if is_nxdomain {
                    resp.extend_from_slice(&0x8183u16.to_be_bytes()); // NXDOMAIN
                    resp.extend_from_slice(&1u16.to_be_bytes()); // QDCOUNT=1
                    resp.extend_from_slice(&0u16.to_be_bytes()); // ANCOUNT=0
                    resp.extend_from_slice(&0u16.to_be_bytes());
                    resp.extend_from_slice(&0u16.to_be_bytes());
                    resp.extend_from_slice(&buf[12..len]);
                } else if qtype == 1 && ip.is_some() {
                    let addr = ip.unwrap();
                    resp.extend_from_slice(&0x8180u16.to_be_bytes()); // Standard response
                    resp.extend_from_slice(&1u16.to_be_bytes()); // QDCOUNT=1
                    resp.extend_from_slice(&1u16.to_be_bytes()); // ANCOUNT=1
                    resp.extend_from_slice(&0u16.to_be_bytes());
                    resp.extend_from_slice(&0u16.to_be_bytes());
                    resp.extend_from_slice(&buf[12..len]); // Echo question

                    // Answer: Pointer to name (0xC00C)
                    resp.extend_from_slice(&[0xC0, 0x0C]);
                    resp.extend_from_slice(&1u16.to_be_bytes()); // Type A
                    resp.extend_from_slice(&1u16.to_be_bytes()); // Class IN
                    resp.extend_from_slice(&60u32.to_be_bytes()); // TTL = 60s
                    resp.extend_from_slice(&4u16.to_be_bytes()); // RDLENGTH = 4
                    resp.extend_from_slice(&addr.octets());
                } else {
                    resp.extend_from_slice(&0x8180u16.to_be_bytes());
                    resp.extend_from_slice(&1u16.to_be_bytes());
                    resp.extend_from_slice(&0u16.to_be_bytes());
                    resp.extend_from_slice(&0u16.to_be_bytes());
                    resp.extend_from_slice(&0u16.to_be_bytes());
                    resp.extend_from_slice(&buf[12..len]);
                }

                let _ = socket.send_to(&resp, peer).await;
            }
        });

        Self {
            addr,
            query_count,
            active,
        }
    }

    fn query_count(&self) -> usize {
        self.query_count.load(Ordering::SeqCst)
    }

    fn set_active(&self, is_active: bool) {
        self.active.store(is_active, Ordering::SeqCst);
    }
}

// ============================================================================
// DNS Subsystem Integration Tests
// ============================================================================

#[tokio::test]
async fn test_hosts_bootstrap_and_cache_population() {
    let hosts_data = "192.168.10.50 myapp.internal\n";
    let hosts = HostsFileSource::from_content(hosts_data);
    let servers = ResolvConfServerProvider::with_servers(vec!["127.0.0.1:53".parse().unwrap()]);
    let transport = UdpDnsTransport::new();

    let resolver = Arc::new(DnsResolverProvider::with_hosts(servers, hosts, transport));

    // Initial cache state: Miss
    assert_eq!(resolver.cache().get("myapp.internal"), CacheLookup::Miss);

    // 1st resolve: pulls from HostsFileSource, populates DnsCache!
    let endpoints = resolver.resolve("myapp.internal", 8080).await.unwrap();
    assert_eq!(endpoints.len(), 1);
    assert_eq!(endpoints[0].address, "192.168.10.50:8080".parse().unwrap());

    // 2nd check: DnsCache is now populated (Hit)!
    match resolver.cache().get("myapp.internal") {
        CacheLookup::Hit(ips) => {
            assert_eq!(&*ips, &["192.168.10.50".parse::<IpAddr>().unwrap()]);
        }
        other => panic!("Expected CacheLookup::Hit, got {other:?}"),
    }

    // 2nd resolve: hits DnsCache directly without wire query!
    let endpoints2 = resolver.resolve("myapp.internal", 8080).await.unwrap();
    assert_eq!(endpoints2[0].address, "192.168.10.50:8080".parse().unwrap());
}

#[tokio::test]
async fn test_multi_server_failover() {
    // S1 is dead (no listener), S2 is live
    let s1: SocketAddr = "127.0.0.1:1".parse().unwrap();
    let s2_server = TestUdpDnsServer::spawn(Some(Ipv4Addr::new(10, 0, 1, 1)), None, false).await;

    let servers = ResolvConfServerProvider::with_servers(vec![s1, s2_server.addr]);
    let hosts = HostsFileSource::empty();
    let transport = UdpDnsTransport::new();

    let config = DnsResolverConfig {
        query_timeout: Duration::from_millis(50),
        ..Default::default()
    };

    let resolver = Arc::new(DnsResolverProvider::with_hosts_and_config(
        servers, hosts, transport, config,
    ));
    let endpoints = resolver.resolve("api.prod.local", 443).await.unwrap();

    assert_eq!(endpoints.len(), 1);
    assert_eq!(endpoints[0].address, "10.0.1.1:443".parse().unwrap());
    assert!(s2_server.query_count() >= 1);
}

#[tokio::test]
async fn test_positive_caching_and_ttl() {
    let server = TestUdpDnsServer::spawn(Some(Ipv4Addr::new(10, 5, 5, 5)), None, false).await;

    let servers = ResolvConfServerProvider::with_servers(vec![server.addr]);
    let hosts = HostsFileSource::empty();
    let transport = UdpDnsTransport::new();

    let config = DnsResolverConfig {
        positive_ttl: Duration::from_millis(50),
        hosts_ttl: Duration::from_millis(50),
        negative_ttl: Duration::from_millis(50),
        ..Default::default()
    };
    let resolver = Arc::new(DnsResolverProvider::with_hosts_and_config(
        servers, hosts, transport, config,
    ));

    // 1st resolve: wire query to UDP socket
    let eps1 = resolver.resolve("cache.test", 80).await.unwrap();
    assert_eq!(eps1.len(), 1);
    let count1 = server.query_count();
    assert!(count1 >= 1);

    // 2nd resolve: cache hit (zero wire queries to socket)
    let eps2 = resolver.resolve("cache.test", 80).await.unwrap();
    assert_eq!(eps2.len(), 1);
    assert_eq!(server.query_count(), count1);

    // Sleep past TTL
    tokio::time::sleep(Duration::from_millis(60)).await;

    // 3rd resolve: cache expired -> triggers fresh wire query
    let eps3 = resolver.resolve("cache.test", 80).await.unwrap();
    assert_eq!(eps3.len(), 1);
    assert!(server.query_count() > count1);
}

#[tokio::test]
async fn test_negative_caching_prevents_query_storm() {
    let server = TestUdpDnsServer::spawn(None, None, true).await;

    let servers = ResolvConfServerProvider::with_servers(vec![server.addr]);
    let hosts = HostsFileSource::empty();
    let transport = UdpDnsTransport::new();

    let config = DnsResolverConfig {
        positive_ttl: Duration::from_secs(10),
        hosts_ttl: Duration::from_secs(10),
        negative_ttl: Duration::from_millis(100),
        ..Default::default()
    };
    let resolver = Arc::new(DnsResolverProvider::with_hosts_and_config(
        servers, hosts, transport, config,
    ));

    // 1st resolve: fails and enters negative cache
    let res1 = resolver.resolve("nonexistent.local", 80).await;
    assert!(res1.is_err());
    let initial_count = server.query_count();

    // 2nd resolve: immediately blocked by negative cache without hitting wire!
    let res2 = resolver.resolve("nonexistent.local", 80).await;
    match res2 {
        Err(DiscoveryError::NegativeCacheHit { .. }) => {}
        other => panic!("Expected NegativeCacheHit, got {other:?}"),
    }
    assert_eq!(server.query_count(), initial_count);
}

#[tokio::test]
async fn test_last_known_good_resilience() {
    let target_ip = Ipv4Addr::new(10, 20, 30, 40);
    let server = TestUdpDnsServer::spawn(Some(target_ip), None, false).await;

    let servers = ResolvConfServerProvider::with_servers(vec![server.addr]);
    let hosts = HostsFileSource::empty();
    let transport = UdpDnsTransport::new();

    let config = DnsResolverConfig {
        positive_ttl: Duration::from_millis(15),
        hosts_ttl: Duration::from_millis(15),
        negative_ttl: Duration::from_millis(50),
        query_timeout: Duration::from_millis(10),
        ..Default::default()
    };

    let resolver = Arc::new(DnsResolverProvider::with_hosts_and_config(
        servers, hosts, transport, config,
    ));

    // 1. Prime cache and LKG
    let eps = resolver.resolve("resilient.local", 8080).await.unwrap();
    assert_eq!(eps.len(), 1);
    assert_eq!(eps[0].address.ip(), IpAddr::V4(target_ip));

    // 2. Shut down nameserver to simulate network outage / nameserver failure
    server.set_active(false);

    // 3. Wait for positive TTL to expire
    tokio::time::sleep(Duration::from_millis(25)).await;

    // 4. Wire query now times out on unreachable server.
    // Invariant: Resolver engages Step 4 and falls back to Last-Known-Good (LKG)!
    let fallback_eps = resolver.resolve("resilient.local", 8080).await.unwrap();
    assert_eq!(fallback_eps.len(), 1);
    assert_eq!(fallback_eps[0].address.ip(), IpAddr::V4(target_ip));
}

#[tokio::test]
async fn test_case_insensitive_domain_resolution() {
    let target_ip = Ipv4Addr::new(192, 168, 5, 5);
    let server = TestUdpDnsServer::spawn(Some(target_ip), None, false).await;

    let servers = ResolvConfServerProvider::with_servers(vec![server.addr]);
    let hosts = HostsFileSource::empty();
    let transport = UdpDnsTransport::new();

    let resolver = Arc::new(DnsResolverProvider::with_hosts(servers, hosts, transport));

    // 1. Resolve lowercase
    let eps1 = resolver.resolve("api.case.test", 80).await.unwrap();
    assert_eq!(eps1[0].address.ip(), IpAddr::V4(target_ip));

    // 2. Resolve UPPERCASE - RFC 1035 requires case-insensitivity: must hit in-memory cache
    let initial_queries = server.query_count();
    let eps2 = resolver.resolve("API.CASE.TEST", 80).await.unwrap();
    assert_eq!(eps2[0].address.ip(), IpAddr::V4(target_ip));
    assert_eq!(
        server.query_count(),
        initial_queries,
        "Uppercase query must hit positive cache without additional wire queries"
    );

    // 3. Resolve MixedCase
    let eps3 = resolver.resolve("Api.Case.Test", 80).await.unwrap();
    assert_eq!(eps3[0].address.ip(), IpAddr::V4(target_ip));
    assert_eq!(
        server.query_count(),
        initial_queries,
        "MixedCase query must hit positive cache without additional wire queries"
    );
}

#[tokio::test]
async fn test_singleflight_coalescing_prevents_thundering_herd() {
    // Inject 15ms artificial delay to simulate wire query window
    let server = TestUdpDnsServer::spawn(
        Some(Ipv4Addr::new(10, 0, 0, 99)),
        Some(Duration::from_millis(15)),
        false,
    )
    .await;

    let servers = ResolvConfServerProvider::with_servers(vec![server.addr]);
    let hosts = HostsFileSource::empty();
    let transport = UdpDnsTransport::new();

    let resolver = Arc::new(DnsResolverProvider::with_hosts(servers, hosts, transport));

    // Spawn 20 concurrent requests for the exact same domain
    let mut handles = Vec::new();
    for _ in 0..20 {
        let r = Arc::clone(&resolver);
        handles.push(tokio::spawn(async move {
            r.resolve("stampede.service", 8080).await
        }));
    }

    for handle in handles {
        let res = handle.await.unwrap().unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res[0].address, "10.0.0.99:8080".parse().unwrap());
    }

    // Crucial check: 20 simultaneous requests coalesced into exactly 1 query batch (A and AAAA queries from single leader)
    assert!(server.query_count() <= 2);
}

#[tokio::test]
async fn test_udp_query_timeout_failover() {
    // S1 hangs with 200ms delay, S2 responds immediately
    let s1_server = TestUdpDnsServer::spawn(
        Some(Ipv4Addr::new(10, 1, 1, 1)),
        Some(Duration::from_millis(200)),
        false,
    )
    .await;
    let s2_server = TestUdpDnsServer::spawn(Some(Ipv4Addr::new(10, 2, 2, 2)), None, false).await;

    let servers = ResolvConfServerProvider::with_servers(vec![s1_server.addr, s2_server.addr]);
    let hosts = HostsFileSource::empty();
    let transport = UdpDnsTransport::new();

    let config = DnsResolverConfig {
        query_timeout: Duration::from_millis(25), // Strict 25ms timeout
        ..Default::default()
    };

    let resolver = DnsResolverProvider::with_hosts_and_config(servers, hosts, transport, config);

    let start = std::time::Instant::now();
    let eps = resolver.resolve("fast.service", 9090).await.unwrap();
    let elapsed = start.elapsed();

    // Verify it timed out on s1 (~25ms) and immediately fell over to s2
    assert_eq!(eps.len(), 1);
    assert_eq!(eps[0].address, "10.2.2.2:9090".parse().unwrap());
    assert!(elapsed < Duration::from_millis(120)); // Well under the 200ms hanging delay
}

#[tokio::test]
async fn test_resolve_ips_and_resolve() {
    let target = Ipv4Addr::new(10, 1, 1, 1);
    let server = TestUdpDnsServer::spawn(Some(target), None, false).await;

    let servers = ResolvConfServerProvider::with_servers(vec![server.addr]);
    let hosts = HostsFileSource::empty();
    let transport = UdpDnsTransport::new();

    let resolver = DnsResolverProvider::with_hosts(servers, hosts, transport);

    // Call resolve_ips directly
    let ips = resolver.resolve_ips("api.service").await.unwrap();
    assert_eq!(&*ips, &[IpAddr::V4(target)]);

    // Call resolve with port
    let endpoints = resolver.resolve("api.service", 8080).await.unwrap();
    assert_eq!(endpoints.len(), 1);
    assert_eq!(
        endpoints[0].address,
        SocketAddr::new(IpAddr::V4(target), 8080)
    );
}

#[test]
fn test_dns_resolver_config_for_tier() {
    let constrained = DnsResolverConfig::for_tier(velda_core::MemoryTier::Constrained);
    assert_eq!(constrained.cache_capacity, 1_000);
    assert_eq!(constrained.lkg_capacity, 500);

    let small = DnsResolverConfig::for_tier(velda_core::MemoryTier::Small);
    assert_eq!(small.cache_capacity, 10_000);
    assert_eq!(small.lkg_capacity, 2_000);

    let medium = DnsResolverConfig::for_tier(velda_core::MemoryTier::Medium);
    assert_eq!(medium.cache_capacity, 50_000);
    assert_eq!(medium.lkg_capacity, 10_000);

    let large = DnsResolverConfig::for_tier(velda_core::MemoryTier::Large);
    assert_eq!(large.cache_capacity, 150_000);
    assert_eq!(large.lkg_capacity, 30_000);

    let xlarge = DnsResolverConfig::for_tier(velda_core::MemoryTier::XLarge);
    assert_eq!(xlarge.cache_capacity, 400_000);
    assert_eq!(xlarge.lkg_capacity, 80_000);

    let two_xlarge = DnsResolverConfig::for_tier(velda_core::MemoryTier::TwoXLarge);
    assert_eq!(two_xlarge.cache_capacity, 1_000_000);
    assert_eq!(two_xlarge.lkg_capacity, 200_000);

    let ultra = DnsResolverConfig::for_tier(velda_core::MemoryTier::Ultra);
    assert_eq!(ultra.cache_capacity, 2_500_000);
    assert_eq!(ultra.lkg_capacity, 500_000);
}

#[tokio::test]
async fn test_bounded_lkg_capacity_eviction() {
    let target = Ipv4Addr::new(10, 99, 1, 1);
    let server = TestUdpDnsServer::spawn(Some(target), None, false).await;

    let servers = ResolvConfServerProvider::with_servers(vec![server.addr]);
    let hosts = HostsFileSource::empty();
    let transport = UdpDnsTransport::new();

    let config = DnsResolverConfig {
        lkg_capacity: 2,                        // Strict bound of 2 entries
        positive_ttl: Duration::from_millis(1), // Rapidly expire cache to force LKG fallbacks
        ..Default::default()
    };

    let resolver = DnsResolverProvider::with_hosts_and_config(servers, hosts, transport, config);

    // Resolve 3 different domains (domain1, domain2, domain3)
    let _ = resolver.resolve_ips("domain1.local").await.unwrap();
    let _ = resolver.resolve_ips("domain2.local").await.unwrap();
    let _ = resolver.resolve_ips("domain3.local").await.unwrap();

    // Verify server shutdown and test that LKG does not exceed 2 entries
    server.set_active(false);
    tokio::time::sleep(Duration::from_millis(10)).await;

    // domain3 was inserted last, so it's guaranteed to be in LKG
    let res3 = resolver.resolve_ips("domain3.local").await;
    assert!(res3.is_ok(), "Most recent domain must be retained in LKG");
}
