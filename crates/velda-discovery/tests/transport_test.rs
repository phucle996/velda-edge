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
            let len = question_end(&buf, len);
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
    let ips = transport.query(srv_addr, "api.service").await.unwrap().ips;

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
        .unwrap()
        .ips;

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
        Err(DiscoveryError::NameNotFound { host, .. }) => {
            assert_eq!(host, "nonexistent.domain");
        }
        other => panic!("Expected NameNotFound, got {other:?}"),
    }
}

// ============================================================================
// SystemDnsTransport Tests
// ============================================================================

#[tokio::test]
async fn test_system_dns_transport_localhost() {
    let transport = SystemDnsTransport::new();
    let dummy_server: SocketAddr = "127.0.0.1:53".parse().unwrap();

    let ips = transport
        .query(dummy_server, "localhost")
        .await
        .unwrap()
        .ips;
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

/// Builds a response for `req` echoing its question (without EDNS0), with an
/// optional single A record and explicit flags.
fn mock_response(req: &[u8], flags: u16, a: Option<Ipv4Addr>, id_xor: u16) -> Vec<u8> {
    let qend = question_end(req, req.len());
    let qtype = u16::from_be_bytes([req[qend - 4], req[qend - 3]]);
    let id = u16::from_be_bytes([req[0], req[1]]) ^ id_xor;
    let answer = a.filter(|_| qtype == 1);
    let mut resp = Vec::new();
    resp.extend_from_slice(&id.to_be_bytes());
    resp.extend_from_slice(&flags.to_be_bytes());
    resp.extend_from_slice(&1u16.to_be_bytes());
    resp.extend_from_slice(&(answer.is_some() as u16).to_be_bytes());
    resp.extend_from_slice(&[0, 0, 0, 0]);
    resp.extend_from_slice(&req[12..qend]);
    if let Some(ip) = answer {
        resp.extend_from_slice(&[0xC0, 0x0C, 0, 1, 0, 1, 0, 0, 1, 0x2C, 0, 4]);
        resp.extend_from_slice(&ip.octets());
    }
    resp
}

#[tokio::test]
async fn test_udp_dns_transport_ignores_forged_response_ids() {
    let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let srv_addr = socket.local_addr().unwrap();
    tokio::spawn(async move {
        let mut buf = [0u8; 512];
        while let Ok((len, peer)) = socket.recv_from(&mut buf).await {
            let req = &buf[..len];
            // Forged answer first (wrong ID, attacker IP), then the genuine one.
            let forged = mock_response(req, 0x8180, Some(Ipv4Addr::new(6, 6, 6, 6)), 0x5A5A);
            let _ = socket.send_to(&forged, peer).await;
            let genuine = mock_response(req, 0x8180, Some(Ipv4Addr::new(10, 9, 8, 7)), 0);
            let _ = socket.send_to(&genuine, peer).await;
        }
    });

    let ips = UdpDnsTransport::new()
        .query(srv_addr, "api.internal")
        .await
        .unwrap()
        .ips;
    assert_eq!(ips, vec![IpAddr::V4(Ipv4Addr::new(10, 9, 8, 7))]);
}

#[tokio::test]
async fn test_udp_dns_transport_ignores_mismatched_question() {
    let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let srv_addr = socket.local_addr().unwrap();
    tokio::spawn(async move {
        let mut buf = [0u8; 512];
        while let Ok((len, peer)) = socket.recv_from(&mut buf).await {
            let req = &buf[..len];
            // Correct ID but the question names a different host.
            let mut other = req.to_vec();
            other[13] = b'x';
            let forged = mock_response(&other, 0x8180, Some(Ipv4Addr::new(6, 6, 6, 6)), 0);
            let _ = socket.send_to(&forged, peer).await;
            let genuine = mock_response(req, 0x8180, Some(Ipv4Addr::new(10, 9, 8, 7)), 0);
            let _ = socket.send_to(&genuine, peer).await;
        }
    });

    let ips = UdpDnsTransport::new()
        .query(srv_addr, "api.internal")
        .await
        .unwrap()
        .ips;
    assert_eq!(ips, vec![IpAddr::V4(Ipv4Addr::new(10, 9, 8, 7))]);
}

#[tokio::test]
async fn test_udp_dns_transport_truncated_falls_back_to_tcp() {
    let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let srv_addr = socket.local_addr().unwrap();
    let listener = tokio::net::TcpListener::bind(srv_addr).await.unwrap();

    tokio::spawn(async move {
        let mut buf = [0u8; 512];
        while let Ok((len, peer)) = socket.recv_from(&mut buf).await {
            // TC=1 and no answers: forces the client onto TCP.
            let resp = mock_response(&buf[..len], 0x8380, None, 0);
            let _ = socket.send_to(&resp, peer).await;
        }
    });
    tokio::spawn(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        while let Ok((mut stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                let mut len_buf = [0u8; 2];
                stream.read_exact(&mut len_buf).await.unwrap();
                let mut req = vec![0u8; u16::from_be_bytes(len_buf) as usize];
                stream.read_exact(&mut req).await.unwrap();
                let resp = mock_response(&req, 0x8180, Some(Ipv4Addr::new(10, 1, 2, 3)), 0);
                stream
                    .write_all(&(resp.len() as u16).to_be_bytes())
                    .await
                    .unwrap();
                stream.write_all(&resp).await.unwrap();
            });
        }
    });

    let ips = UdpDnsTransport::new()
        .query(srv_addr, "big.internal")
        .await
        .unwrap()
        .ips;
    assert_eq!(ips, vec![IpAddr::V4(Ipv4Addr::new(10, 1, 2, 3))]);
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
