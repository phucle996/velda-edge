//! Integration tests for DNS discovery: caching, failover, hosts integration, and LKG resilience.

mod common;

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use common::MockDnsTransport;
use velda_discovery::{
    CacheLookup, Discovery, DiscoveryError, DnsResolverConfig, DnsResolverProvider,
    HostsFileSource, ResolvConfServerProvider,
};

#[tokio::test]
async fn test_hosts_bootstrap_and_cache_population() {
    let hosts_data = "192.168.10.50 myapp.internal\n";
    let hosts = HostsFileSource::from_content(hosts_data);
    let servers = ResolvConfServerProvider::with_servers(vec!["127.0.0.1:53".parse().unwrap()]);
    let transport = Arc::new(MockDnsTransport::new());

    let resolver = Arc::new(DnsResolverProvider::with_hosts(
        servers,
        hosts,
        transport.clone(),
    ));

    // Initial cache state: Miss
    assert_eq!(resolver.cache().get("myapp.internal"), CacheLookup::Miss);

    // 1st resolve: pulls from HostsFileSource, populates DnsCache!
    let endpoints = resolver.resolve("myapp.internal", 8080).await.unwrap();
    assert_eq!(endpoints.len(), 1);
    assert_eq!(endpoints[0].address, "192.168.10.50:8080".parse().unwrap());
    assert_eq!(transport.query_count(), 0);

    // 2nd check: DnsCache is now populated (Hit)!
    match resolver.cache().get("myapp.internal") {
        CacheLookup::Hit(ips) => {
            assert_eq!(ips, vec!["192.168.10.50".parse::<IpAddr>().unwrap()]);
        }
        other => panic!("Expected CacheLookup::Hit, got {other:?}"),
    }

    // 2nd resolve: hits DnsCache directly!
    let endpoints2 = resolver.resolve("myapp.internal", 8080).await.unwrap();
    assert_eq!(endpoints2[0].address, "192.168.10.50:8080".parse().unwrap());
    assert_eq!(transport.query_count(), 0);
}

#[tokio::test]
async fn test_multi_server_failover() {
    let s1: SocketAddr = "10.99.0.1:53".parse().unwrap();
    let s2: SocketAddr = "10.99.0.2:53".parse().unwrap();

    let servers = ResolvConfServerProvider::with_servers(vec![s1, s2]);
    let hosts = HostsFileSource::empty();
    let transport = Arc::new(MockDnsTransport::new());

    // S1 fails, S2 succeeds
    transport.set_failing(s1);
    transport.set_response(
        s2,
        "api.prod.local",
        vec!["10.0.1.1".parse().unwrap(), "10.0.1.2".parse().unwrap()],
    );

    let resolver = Arc::new(DnsResolverProvider::with_hosts(
        servers,
        hosts,
        transport.clone(),
    ));
    let endpoints = resolver.resolve("api.prod.local", 443).await.unwrap();

    assert_eq!(endpoints.len(), 2);
    assert_eq!(endpoints[0].address, "10.0.1.1:443".parse().unwrap());
    assert_eq!(endpoints[1].address, "10.0.1.2:443".parse().unwrap());
    // Tried S1 (failed), then S2 (succeeded)
    assert_eq!(transport.query_count(), 2);
}

#[tokio::test]
async fn test_positive_caching_and_ttl() {
    let s1: SocketAddr = "10.99.0.1:53".parse().unwrap();
    let servers = ResolvConfServerProvider::with_servers(vec![s1]);
    let hosts = HostsFileSource::empty();
    let transport = Arc::new(MockDnsTransport::new());

    transport.set_response(s1, "cache.test", vec!["10.5.5.5".parse().unwrap()]);

    let config = DnsResolverConfig {
        positive_ttl: Duration::from_millis(50),
        hosts_ttl: Duration::from_millis(50),
        negative_ttl: Duration::from_millis(50),
        ..Default::default()
    };
    let resolver = Arc::new(DnsResolverProvider::with_hosts_and_config(
        servers,
        hosts,
        transport.clone(),
        config,
    ));

    // 1st resolve: wire query
    let eps1 = resolver.resolve("cache.test", 80).await.unwrap();
    assert_eq!(eps1.len(), 1);
    assert_eq!(transport.query_count(), 1);

    // 2nd resolve: cache hit (no extra wire query)
    let eps2 = resolver.resolve("cache.test", 80).await.unwrap();
    assert_eq!(eps2.len(), 1);
    assert_eq!(transport.query_count(), 1);

    // Sleep past TTL
    tokio::time::sleep(Duration::from_millis(60)).await;

    // 3rd resolve: cache expired -> triggers fresh wire query
    let eps3 = resolver.resolve("cache.test", 80).await.unwrap();
    assert_eq!(eps3.len(), 1);
    assert_eq!(transport.query_count(), 2);
}

#[tokio::test]
async fn test_negative_caching_prevents_query_storm() {
    let s1: SocketAddr = "10.99.0.1:53".parse().unwrap();
    let servers = ResolvConfServerProvider::with_servers(vec![s1]);
    let hosts = HostsFileSource::empty();
    let transport = Arc::new(MockDnsTransport::new());

    // No response configured -> NXDOMAIN
    let config = DnsResolverConfig {
        positive_ttl: Duration::from_secs(10),
        hosts_ttl: Duration::from_secs(10),
        negative_ttl: Duration::from_millis(100),
        ..Default::default()
    };
    let resolver = Arc::new(DnsResolverProvider::with_hosts_and_config(
        servers,
        hosts,
        transport.clone(),
        config,
    ));

    // 1st resolve: fails and enters negative cache
    let res1 = resolver.resolve("nonexistent.local", 80).await;
    assert!(res1.is_err());
    assert_eq!(transport.query_count(), 1);

    // 2nd resolve: immediately blocked by negative cache without hitting wire!
    let res2 = resolver.resolve("nonexistent.local", 80).await;
    match res2 {
        Err(DiscoveryError::NegativeCacheHit { .. }) => {}
        other => panic!("Expected NegativeCacheHit, got {other:?}"),
    }
    assert_eq!(transport.query_count(), 1);
}

#[tokio::test]
async fn test_last_known_good_resilience() {
    let s1: SocketAddr = "10.99.0.1:53".parse().unwrap();
    let servers = ResolvConfServerProvider::with_servers(vec![s1]);
    let hosts = HostsFileSource::empty();
    let transport = Arc::new(MockDnsTransport::new());

    let target_ip: IpAddr = "10.20.30.40".parse().unwrap();
    transport.set_response(s1, "resilient.local", vec![target_ip]);

    let config = DnsResolverConfig {
        positive_ttl: Duration::from_millis(10),
        hosts_ttl: Duration::from_millis(10),
        negative_ttl: Duration::from_millis(10),
        ..Default::default()
    };

    let resolver = Arc::new(DnsResolverProvider::with_hosts_and_config(
        servers,
        hosts,
        transport.clone(),
        config,
    ));

    // 1. Initial success establishes LKG
    let eps = resolver.resolve("resilient.local", 8080).await.unwrap();
    assert_eq!(eps[0].address.ip(), target_ip);

    // 2. DNS server dies
    tokio::time::sleep(Duration::from_millis(20)).await;
    transport.set_failing(s1);

    // 3. Next resolve fails wire, but returns LKG!
    let fallback_eps = resolver.resolve("resilient.local", 8080).await.unwrap();
    assert_eq!(fallback_eps.len(), 1);
    assert_eq!(fallback_eps[0].address.ip(), target_ip);
}

#[tokio::test]
async fn test_background_dns_discovery_service() {
    let s1: SocketAddr = "10.99.0.1:53".parse().unwrap();
    let servers = ResolvConfServerProvider::with_servers(vec![s1]);
    let hosts = HostsFileSource::empty();
    let transport = Arc::new(MockDnsTransport::new());

    transport.set_response(
        s1,
        "dynamic.service",
        vec!["10.0.0.1".parse().unwrap(), "10.0.0.2".parse().unwrap()],
    );

    let resolver = Arc::new(DnsResolverProvider::with_hosts(
        servers,
        hosts,
        transport.clone(),
    ));
    let disc = Discovery::new_dns(
        "dynamic.service".into(),
        8080,
        Duration::from_millis(20),
        resolver,
    );

    // Wait for first background tick
    tokio::time::sleep(Duration::from_millis(50)).await;

    let snapshot = disc.current_endpoints();
    assert_eq!(snapshot.len(), 2);
    assert!(snapshot.generation() >= 1);

    disc.shutdown();
}

#[tokio::test]
async fn test_singleflight_coalescing_prevents_thundering_herd() {
    // FIX (Blocker 3 - Cache Stampede / Thundering Herd): Verify singleflight deduplication
    let s1: SocketAddr = "10.99.0.1:53".parse().unwrap();
    let servers = ResolvConfServerProvider::with_servers(vec![s1]);
    let hosts = HostsFileSource::empty();
    let transport = Arc::new(MockDnsTransport::new());

    // Inject 10ms artificial delay to simulate wire query window
    transport.set_delay(s1, Duration::from_millis(10));
    transport.set_response(s1, "stampede.service", vec!["10.0.0.99".parse().unwrap()]);

    let resolver = Arc::new(DnsResolverProvider::with_hosts(
        servers,
        hosts,
        transport.clone(),
    ));

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

    // Crucial check: 20 simultaneous requests coalesced into exactly 1 wire query!
    assert_eq!(transport.query_count(), 1);
}

#[tokio::test]
async fn test_udp_query_timeout_failover() {
    // FIX (Blocker 4 - UDP Hang / Timeout): Verify timeout-triggered failover
    let s1: SocketAddr = "10.0.0.1:53".parse().unwrap();
    let s2: SocketAddr = "10.0.0.2:53".parse().unwrap();

    let servers = ResolvConfServerProvider::with_servers(vec![s1, s2]);
    let hosts = HostsFileSource::empty();
    let transport = Arc::new(MockDnsTransport::new());

    // Server 1 hangs with 200ms delay
    transport.set_delay(s1, Duration::from_millis(200));

    // Server 2 responds immediately
    transport.set_response(s2, "fast.service", vec!["10.2.2.2".parse().unwrap()]);

    let config = DnsResolverConfig {
        query_timeout: Duration::from_millis(25), // Strict 25ms timeout
        ..Default::default()
    };

    let resolver =
        DnsResolverProvider::with_hosts_and_config(servers, hosts, transport.clone(), config);

    let start = std::time::Instant::now();
    let eps = resolver.resolve("fast.service", 9090).await.unwrap();
    let elapsed = start.elapsed();

    // Verify it timed out on s1 (~25ms) and immediately fell over to s2
    assert_eq!(eps.len(), 1);
    assert_eq!(eps[0].address, "10.2.2.2:9090".parse().unwrap());
    assert!(elapsed < Duration::from_millis(100)); // Well under the 200ms hanging delay
}
