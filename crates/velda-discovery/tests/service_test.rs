//! Integration tests for Discovery service lifecycle, background sync, and EndpointSet snapshots.
//! Uses real `UdpDnsTransport` over actual loopback UDP sockets.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use velda_discovery::{
    Discovery, DiscoveryError, DiscoveryMode, DnsResolverProvider, Endpoint, EndpointSet,
    HostsFileSource, ResolvConfServerProvider, UdpDnsTransport,
};

// ============================================================================
// Helper: Loopback UDP DNS Server with Dynamic IP and Outage Simulation
// ============================================================================

struct TestDnsHandle {
    addr: SocketAddr,
    ip: Arc<RwLock<Ipv4Addr>>,
    active: Arc<AtomicBool>,
}

impl TestDnsHandle {
    fn set_ip(&self, new_ip: Ipv4Addr) {
        *self.ip.write().unwrap() = new_ip;
    }

    fn set_active(&self, is_active: bool) {
        self.active.store(is_active, Ordering::SeqCst);
    }
}

async fn spawn_test_udp_server(ip: Ipv4Addr) -> TestDnsHandle {
    let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let addr = socket.local_addr().unwrap();
    let ip_holder = Arc::new(RwLock::new(ip));
    let ip_clone = Arc::clone(&ip_holder);
    let active = Arc::new(AtomicBool::new(true));
    let active_clone = Arc::clone(&active);

    tokio::spawn(async move {
        let mut buf = [0u8; 512];
        while let Ok((len, peer)) = socket.recv_from(&mut buf).await {
            if !active_clone.load(Ordering::SeqCst) {
                // Drop packet to simulate network outage / nameserver crash
                continue;
            }

            let current_ip = *ip_clone.read().unwrap();
            let req_id = u16::from_be_bytes([buf[0], buf[1]]);
            let len = question_end(&buf, len);
            let qtype = u16::from_be_bytes([buf[len - 4], buf[len - 3]]);
            let mut resp = Vec::with_capacity(64);
            resp.extend_from_slice(&req_id.to_be_bytes());

            if qtype == 1 {
                resp.extend_from_slice(&0x8180u16.to_be_bytes()); // QR=1, RD=1, RA=1
                resp.extend_from_slice(&1u16.to_be_bytes()); // QDCOUNT=1
                resp.extend_from_slice(&1u16.to_be_bytes()); // ANCOUNT=1
                resp.extend_from_slice(&0u16.to_be_bytes());
                resp.extend_from_slice(&0u16.to_be_bytes());
                resp.extend_from_slice(&buf[12..len]); // Echo question

                // Answer: Pointer to name (0xC00C), Type A (1), Class IN (1), TTL (60s), RDLENGTH (4)
                resp.extend_from_slice(&[0xC0, 0x0C]);
                resp.extend_from_slice(&1u16.to_be_bytes());
                resp.extend_from_slice(&1u16.to_be_bytes());
                resp.extend_from_slice(&60u32.to_be_bytes());
                resp.extend_from_slice(&4u16.to_be_bytes());
                resp.extend_from_slice(&current_ip.octets());
            } else {
                resp.extend_from_slice(&0x8180u16.to_be_bytes());
                resp.extend_from_slice(&1u16.to_be_bytes());
                resp.extend_from_slice(&0u16.to_be_bytes()); // ANCOUNT=0
                resp.extend_from_slice(&0u16.to_be_bytes());
                resp.extend_from_slice(&0u16.to_be_bytes());
                resp.extend_from_slice(&buf[12..len]);
            }

            let _ = socket.send_to(&resp, peer).await;
        }
    });

    TestDnsHandle {
        addr,
        ip: ip_holder,
        active,
    }
}

// ============================================================================
// Tests
// ============================================================================

#[test]
fn test_explicit_discovery_lifecycle() {
    let ep1: SocketAddr = "10.0.0.1:8080".parse().unwrap();
    let ep2: SocketAddr = "10.0.0.2:8080".parse().unwrap();

    let endpoints = vec![
        Endpoint::new("ep-1", ep1, 10),
        Endpoint::new("ep-2", ep2, 20),
    ];

    let disc = Discovery::new_explicit(endpoints);
    let snapshot = disc.current_endpoints();

    assert_eq!(snapshot.len(), 2);
    assert_eq!(snapshot.generation(), 1);
    assert!(snapshot.contains_addr(&ep1));
    assert!(snapshot.contains_addr(&ep2));

    let found = snapshot.find_by_addr(&ep1).unwrap();
    assert_eq!(found.id.0, "ep-1");
    assert_eq!(found.weight, 10);
}

#[test]
fn test_discovery_from_mode_explicit() {
    let ep: SocketAddr = "192.168.1.1:80".parse().unwrap();
    let endpoints = vec![Endpoint::new("srv", ep, 1)];

    let disc = Discovery::from_mode::<ResolvConfServerProvider, UdpDnsTransport>(
        DiscoveryMode::Explicit(endpoints),
        None,
    )
    .unwrap();

    let snapshot = disc.current_endpoints();
    assert_eq!(snapshot.len(), 1);
    assert!(snapshot.contains_addr(&ep));
}

#[test]
fn test_dns_missing_resolver_error() {
    let res = Discovery::from_mode::<ResolvConfServerProvider, UdpDnsTransport>(
        DiscoveryMode::Dns {
            host: "unconfigured.service".into(),
            port: 80,
            refresh_interval: Duration::from_millis(50),
        },
        None,
    );

    assert!(matches!(
        res,
        Err(DiscoveryError::DnsResolutionFailed { ref host, ref reason })
            if host == "unconfigured.service" && reason.contains("DNS resolver required")
    ));
}

#[tokio::test]
async fn test_discovery_from_mode_dns() {
    let target_ip = Ipv4Addr::new(10, 0, 2, 1);
    let server = spawn_test_udp_server(target_ip).await;

    let servers = ResolvConfServerProvider::with_servers(vec![server.addr]);
    let hosts = HostsFileSource::empty();
    let transport = UdpDnsTransport::new();

    let resolver = Arc::new(DnsResolverProvider::with_hosts(servers, hosts, transport));

    let disc = Discovery::from_mode(
        DiscoveryMode::Dns {
            host: "mode.service".into(),
            port: 9000,
            refresh_interval: Duration::from_millis(20),
        },
        Some(resolver),
    )
    .unwrap();

    // Wait for first background resolution
    tokio::time::sleep(Duration::from_millis(60)).await;

    let snapshot = disc.current_endpoints();
    assert_eq!(snapshot.len(), 1);
    assert!(snapshot.contains_addr(&SocketAddr::new(IpAddr::V4(target_ip), 9000)));

    disc.shutdown();
}

#[tokio::test]
async fn test_discovery_from_mode_async_eager_resolution() {
    let target_ip = Ipv4Addr::new(10, 0, 5, 1);
    let server = spawn_test_udp_server(target_ip).await;

    let servers = ResolvConfServerProvider::with_servers(vec![server.addr]);
    let hosts = HostsFileSource::empty();
    let transport = UdpDnsTransport::new();

    let resolver = Arc::new(DnsResolverProvider::with_hosts(servers, hosts, transport));

    // Invariant: from_mode_async resolves eagerly so that immediately after return (0ms sleep),
    // current_endpoints() is already primed with discovered endpoints, eliminating cold-start 503s!
    let disc = Discovery::from_mode_async(
        DiscoveryMode::Dns {
            host: "eager.service".into(),
            port: 8443,
            refresh_interval: Duration::from_millis(50),
        },
        Some(resolver),
    )
    .await
    .unwrap();

    let snapshot = disc.current_endpoints();
    assert_eq!(
        snapshot.len(),
        1,
        "Snapshot must be eagerly primed without sleeping!"
    );
    assert!(snapshot.contains_addr(&SocketAddr::new(IpAddr::V4(target_ip), 8443)));
    assert_eq!(snapshot.generation(), 1);

    disc.shutdown();
}

#[tokio::test]
async fn test_topology_evolution_and_lkg_outage() {
    let initial_ip = Ipv4Addr::new(10, 0, 3, 1);
    let server = spawn_test_udp_server(initial_ip).await;

    let servers = ResolvConfServerProvider::with_servers(vec![server.addr]);
    let hosts = HostsFileSource::empty();
    let transport = UdpDnsTransport::new();

    let config = velda_discovery::DnsResolverConfig {
        positive_ttl: Duration::from_millis(15),
        hosts_ttl: Duration::from_millis(15),
        negative_ttl: Duration::from_millis(50),
        query_timeout: Duration::from_millis(10),
        ..Default::default()
    };

    let resolver = Arc::new(velda_discovery::DnsResolverProvider::with_hosts_and_config(
        servers, hosts, transport, config,
    ));

    let disc = Discovery::new_dns(
        "evolving.service".into(),
        8080,
        Duration::from_millis(25),
        resolver,
    );

    // 1. Initial resolution (Generation >= 1)
    tokio::time::sleep(Duration::from_millis(50)).await;
    let snap1 = disc.current_endpoints();
    let initial_gen = snap1.generation();
    assert!(
        initial_gen >= 1,
        "Must complete at least initial resolution"
    );
    assert!(snap1.contains_addr(&SocketAddr::new(IpAddr::V4(initial_ip), 8080)));

    // 2. Rolling update: backend changes IP from 10.0.3.1 to 10.0.3.99
    let updated_ip = Ipv4Addr::new(10, 0, 3, 99);
    server.set_ip(updated_ip);

    // Wait for background ticker to refresh with new IP
    tokio::time::sleep(Duration::from_millis(60)).await;
    let snap2 = disc.current_endpoints();
    let updated_gen = snap2.generation();
    assert!(
        updated_gen > initial_gen,
        "Generation counter must increment on topology evolution"
    );
    assert!(
        snap2.contains_addr(&SocketAddr::new(IpAddr::V4(updated_ip), 8080)),
        "Endpoints must update to new backend IP"
    );

    // 3. Upstream Outage: DNS server crashes / goes down
    server.set_active(false);

    // Wait through multiple refresh cycles during outage
    tokio::time::sleep(Duration::from_millis(75)).await;
    let snap3 = disc.current_endpoints();

    // Invariant: Background discovery MUST retain the Last-Known-Good endpoints!
    assert_eq!(
        snap3.len(),
        1,
        "Endpoint set must NOT become empty during upstream DNS outages"
    );
    assert!(
        snap3.contains_addr(&SocketAddr::new(IpAddr::V4(updated_ip), 8080)),
        "Endpoints must preserve Last-Known-Good address during outage"
    );

    disc.shutdown();
}

#[test]
fn test_discovery_atomic_update_endpoints() {
    let ep1: SocketAddr = "10.0.0.1:8080".parse().unwrap();
    let endpoints = vec![Endpoint::new("ep-1", ep1, 1)];

    let disc = Discovery::new_explicit(endpoints);
    assert_eq!(disc.current_endpoints().len(), 1);
    assert_eq!(disc.current_endpoints().generation(), 1);

    // Atomically swap in new generation
    let ep2: SocketAddr = "10.0.0.2:8080".parse().unwrap();
    let new_endpoints = vec![Endpoint::new("ep-1", ep1, 1), Endpoint::new("ep-2", ep2, 5)];
    disc.update_endpoints(new_endpoints, 2);

    let updated = disc.current_endpoints();
    assert_eq!(updated.len(), 2);
    assert_eq!(updated.generation(), 2);
    assert!(updated.contains_addr(&ep2));
}

#[test]
fn test_endpoint_set_empty_and_iterators() {
    let empty = EndpointSet::empty();
    assert!(empty.is_empty());
    assert_eq!(empty.len(), 0);
    assert_eq!(empty.generation(), 0);

    let ep1: SocketAddr = "10.1.1.1:80".parse().unwrap();
    let ep2: SocketAddr = "10.1.1.2:80".parse().unwrap();
    let set = EndpointSet::new(
        vec![Endpoint::new("a", ep1, 1), Endpoint::new("b", ep2, 2)],
        42,
    );

    assert_eq!(set.generation(), 42);
    assert_eq!(set.len(), 2);
    assert!(!set.is_empty());

    let ips: Vec<IpAddr> = set.ips().collect();
    assert_eq!(
        ips,
        vec![
            "10.1.1.1".parse::<IpAddr>().unwrap(),
            "10.1.1.2".parse::<IpAddr>().unwrap()
        ]
    );

    let addrs: Vec<SocketAddr> = set.addresses().collect();
    assert_eq!(addrs, vec![ep1, ep2]);

    assert_eq!(set.all_endpoints().len(), 2);
}

#[tokio::test]
async fn test_discovery_graceful_shutdown() {
    let s1: SocketAddr = "127.0.0.1:53".parse().unwrap();
    let servers = ResolvConfServerProvider::with_servers(vec![s1]);
    let hosts = HostsFileSource::empty();
    let transport = UdpDnsTransport::new();
    let resolver = Arc::new(DnsResolverProvider::with_hosts(servers, hosts, transport));

    let disc = Discovery::new_dns(
        "shutdown.service".into(),
        80,
        Duration::from_millis(50),
        resolver,
    );

    // Call shutdown cleanly
    disc.shutdown();
}

/// Offset just past the (single) question section, so mock servers echo the
/// question without the EDNS0 OPT record appended by the client.
fn question_end(buf: &[u8], len: usize) -> usize {
    let mut p = 12;
    while buf[p] != 0 {
        p += 1 + buf[p] as usize;
    }
    (p + 5).min(len)
}
