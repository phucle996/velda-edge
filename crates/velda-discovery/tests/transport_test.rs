//! Integration tests for production wire transports: UdpDnsTransport and SystemDnsTransport.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use velda_discovery::{DiscoveryError, DnsTransport, SystemDnsTransport, UdpDnsTransport};

// ============================================================================
// Helper: Mock UDP DNS Server for RFC 1035 wire protocol
// ============================================================================

async fn spawn_mock_udp_server(
    a_records: Vec<Ipv4Addr>,
    aaaa_records: Vec<Ipv6Addr>,
    is_nxdomain: bool,
) -> SocketAddr {
    let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let addr = socket.local_addr().unwrap();

    tokio::spawn(async move {
        let mut buf = [0u8; 512];
        loop {
            let (len, peer) = match socket.recv_from(&mut buf).await {
                Ok(res) => res,
                Err(_) => break,
            };

            let req_id = u16::from_be_bytes([buf[0], buf[1]]);
            let qtype = u16::from_be_bytes([buf[len - 4], buf[len - 3]]);

            let mut resp = Vec::new();
            resp.extend_from_slice(&req_id.to_be_bytes());

            if is_nxdomain {
                // Response with RCODE=3 (NXDOMAIN)
                resp.extend_from_slice(&0x8183u16.to_be_bytes());
                resp.extend_from_slice(&1u16.to_be_bytes()); // QDCOUNT=1
                resp.extend_from_slice(&0u16.to_be_bytes()); // ANCOUNT=0
                resp.extend_from_slice(&0u16.to_be_bytes());
                resp.extend_from_slice(&0u16.to_be_bytes());
                resp.extend_from_slice(&buf[12..len]); // Echo question
            } else {
                resp.extend_from_slice(&0x8180u16.to_be_bytes()); // QR=1, RD=1, RA=1, RCODE=0
                resp.extend_from_slice(&1u16.to_be_bytes()); // QDCOUNT=1

                if qtype == 1 && !a_records.is_empty() {
                    // Type A
                    resp.extend_from_slice(&(a_records.len() as u16).to_be_bytes()); // ANCOUNT
                    resp.extend_from_slice(&0u16.to_be_bytes());
                    resp.extend_from_slice(&0u16.to_be_bytes());
                    resp.extend_from_slice(&buf[12..len]); // Question

                    for ip in &a_records {
                        resp.extend_from_slice(&[0xC0, 0x0C]); // Compression pointer
                        resp.extend_from_slice(&1u16.to_be_bytes()); // TYPE A
                        resp.extend_from_slice(&1u16.to_be_bytes()); // CLASS IN
                        resp.extend_from_slice(&300u32.to_be_bytes()); // TTL
                        resp.extend_from_slice(&4u16.to_be_bytes()); // RDLENGTH = 4
                        resp.extend_from_slice(&ip.octets());
                    }
                } else if qtype == 28 && !aaaa_records.is_empty() {
                    // Type AAAA
                    resp.extend_from_slice(&(aaaa_records.len() as u16).to_be_bytes()); // ANCOUNT
                    resp.extend_from_slice(&0u16.to_be_bytes());
                    resp.extend_from_slice(&0u16.to_be_bytes());
                    resp.extend_from_slice(&buf[12..len]); // Question

                    for ip in &aaaa_records {
                        resp.extend_from_slice(&[0xC0, 0x0C]); // Compression pointer
                        resp.extend_from_slice(&28u16.to_be_bytes()); // TYPE AAAA
                        resp.extend_from_slice(&1u16.to_be_bytes()); // CLASS IN
                        resp.extend_from_slice(&300u32.to_be_bytes()); // TTL
                        resp.extend_from_slice(&16u16.to_be_bytes()); // RDLENGTH = 16
                        resp.extend_from_slice(&ip.octets());
                    }
                } else {
                    // Empty answer
                    resp.extend_from_slice(&0u16.to_be_bytes()); // ANCOUNT=0
                    resp.extend_from_slice(&0u16.to_be_bytes());
                    resp.extend_from_slice(&0u16.to_be_bytes());
                    resp.extend_from_slice(&buf[12..len]); // Question
                }
            }

            let _ = socket.send_to(&resp, peer).await;
        }
    });

    addr
}

// ============================================================================
// UdpDnsTransport Tests
// ============================================================================

#[tokio::test]
async fn test_udp_dns_transport_a_records() {
    let srv_addr = spawn_mock_udp_server(
        vec![Ipv4Addr::new(10, 0, 1, 1), Ipv4Addr::new(10, 0, 1, 2)],
        vec![],
        false,
    )
    .await;

    let transport = UdpDnsTransport::new();
    let ips = transport.query(srv_addr, "api.service").await.unwrap();

    assert_eq!(
        ips,
        vec![
            IpAddr::V4(Ipv4Addr::new(10, 0, 1, 1)),
            IpAddr::V4(Ipv4Addr::new(10, 0, 1, 2)),
        ]
    );
}

#[tokio::test]
async fn test_udp_dns_transport_dual_stack_a_and_aaaa() {
    let ipv4 = Ipv4Addr::new(192, 168, 1, 50);
    let ipv6: Ipv6Addr = "2001:db8::1".parse().unwrap();

    let srv_addr = spawn_mock_udp_server(vec![ipv4], vec![ipv6], false).await;

    let transport = UdpDnsTransport::new();
    let ips = transport
        .query(srv_addr, "dualstack.internal")
        .await
        .unwrap();

    assert!(ips.contains(&IpAddr::V4(ipv4)));
    assert!(ips.contains(&IpAddr::V6(ipv6)));
    assert_eq!(ips.len(), 2);
}

#[tokio::test]
async fn test_udp_dns_transport_nxdomain_error() {
    let srv_addr = spawn_mock_udp_server(vec![], vec![], true).await;

    let transport = UdpDnsTransport::new();
    let res = transport.query(srv_addr, "nonexistent.domain").await;

    match res {
        Err(DiscoveryError::DnsResolutionFailed { host, reason }) => {
            assert_eq!(host, "nonexistent.domain");
            assert_eq!(reason, "NXDOMAIN");
        }
        other => panic!("Expected DnsResolutionFailed with NXDOMAIN, got {other:?}"),
    }
}

// ============================================================================
// SystemDnsTransport Tests
// ============================================================================

#[tokio::test]
async fn test_system_dns_transport_localhost() {
    let transport = SystemDnsTransport::new();
    let dummy_server: SocketAddr = "127.0.0.1:53".parse().unwrap();

    let ips = transport.query(dummy_server, "localhost").await.unwrap();
    assert!(!ips.is_empty());
    assert!(ips.contains(&"127.0.0.1".parse().unwrap()) || ips.contains(&"::1".parse().unwrap()));
}

#[tokio::test]
async fn test_system_dns_transport_invalid_domain() {
    let transport = SystemDnsTransport::new();
    let dummy_server: SocketAddr = "127.0.0.1:53".parse().unwrap();

    let res = transport
        .query(dummy_server, "this-domain-does-not-exist.invalid")
        .await;

    assert!(res.is_err());
    match res {
        Err(DiscoveryError::DnsResolutionFailed { host, .. }) => {
            assert_eq!(host, "this-domain-does-not-exist.invalid");
        }
        other => panic!("Expected DnsResolutionFailed, got {other:?}"),
    }
}
