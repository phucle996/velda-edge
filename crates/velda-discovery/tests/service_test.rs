//! Integration tests for Discovery service lifecycle, background sync, and EndpointSet snapshots.
//! Uses real `UdpDnsTransport` over actual loopback UDP sockets.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use velda_discovery::{
    Discovery, DiscoveryMode, DnsResolverProvider, Endpoint, EndpointSet, HostsFileSource,
    ResolvConfServerProvider, UdpDnsTransport,
};

// ============================================================================
// Helper: Loopback UDP DNS Server
// ============================================================================

async fn spawn_test_udp_server(ip: Ipv4Addr) -> SocketAddr {
    let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let addr = socket.local_addr().unwrap();

    tokio::spawn(async move {
        let mut buf = [0u8; 512];
        while let Ok((len, peer)) = socket.recv_from(&mut buf).await {
            let req_id = u16::from_be_bytes([buf[0], buf[1]]);
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
                resp.extend_from_slice(&ip.octets());
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

    addr
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

#[tokio::test]
async fn test_discovery_from_mode_dns() {
    let target_ip = Ipv4Addr::new(10, 0, 2, 1);
    let srv_addr = spawn_test_udp_server(target_ip).await;

    let servers = ResolvConfServerProvider::with_servers(vec![srv_addr]);
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
