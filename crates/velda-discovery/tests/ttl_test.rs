//! Resolver honors the TTL reported by the transport, clamped to `[1s, positive_ttl]`.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use velda_discovery::{
    DnsAnswer, DnsResolverConfig, DnsResolverProvider, DnsTransport, HostsFileSource, Result,
    StaticServerProvider,
};

/// Always answers with one IP and a fixed record TTL, counting wire queries.
struct FixedTtlTransport {
    ttl: Option<Duration>,
    queries: Arc<AtomicUsize>,
}

impl DnsTransport for FixedTtlTransport {
    async fn query(&self, _server: SocketAddr, _host: &str) -> Result<DnsAnswer> {
        self.queries.fetch_add(1, Ordering::Relaxed);
        Ok(DnsAnswer {
            ips: vec![IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))],
            ttl: self.ttl,
        })
    }
}

fn resolver(
    ttl: Option<Duration>,
    positive_ttl: Duration,
) -> (
    DnsResolverProvider<StaticServerProvider, FixedTtlTransport>,
    Arc<AtomicUsize>,
) {
    let queries = Arc::new(AtomicUsize::new(0));
    let servers = StaticServerProvider::from_addresses(vec!["127.0.0.1:53".parse().unwrap()]);
    let config = DnsResolverConfig {
        positive_ttl,
        ..DnsResolverConfig::default()
    };
    let r = DnsResolverProvider::with_hosts_and_config(
        servers,
        HostsFileSource::default(),
        FixedTtlTransport {
            ttl,
            queries: Arc::clone(&queries),
        },
        config,
    );
    (r, queries)
}

#[tokio::test]
async fn record_ttl_expires_cache_entry_even_when_cap_is_large() {
    // Floor of 1s applies, so a 0s record TTL still caches for ~1s then re-resolves.
    let (r, queries) = resolver(Some(Duration::ZERO), Duration::from_secs(30));
    r.resolve_ips("svc.internal").await.unwrap();
    r.resolve_ips("svc.internal").await.unwrap();
    assert_eq!(
        queries.load(Ordering::Relaxed),
        1,
        "TTL=0 is floored, not uncached"
    );

    tokio::time::sleep(Duration::from_millis(1100)).await;
    r.resolve_ips("svc.internal").await.unwrap();
    assert_eq!(
        queries.load(Ordering::Relaxed),
        2,
        "expired after the 1s floor"
    );
}

#[tokio::test]
async fn positive_ttl_caps_a_long_record_ttl() {
    // Record says 1h, cap is 50ms: the cap wins.
    let (r, queries) = resolver(Some(Duration::from_secs(3600)), Duration::from_millis(50));
    r.resolve_ips("svc.internal").await.unwrap();
    tokio::time::sleep(Duration::from_millis(80)).await;
    r.resolve_ips("svc.internal").await.unwrap();
    assert_eq!(queries.load(Ordering::Relaxed), 2);
}

#[tokio::test]
async fn transport_without_ttl_uses_positive_ttl() {
    let (r, queries) = resolver(None, Duration::from_millis(50));
    r.resolve_ips("svc.internal").await.unwrap();
    tokio::time::sleep(Duration::from_millis(80)).await;
    r.resolve_ips("svc.internal").await.unwrap();
    assert_eq!(queries.load(Ordering::Relaxed), 2);
}
