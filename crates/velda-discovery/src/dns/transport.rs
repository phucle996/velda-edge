//! Production DNS wire transports for `velda-discovery`.
//!
//! Provides:
//! - [`UdpDnsTransport`]: Pure RFC 1035 wire query implementation over UDP using `tokio::net::UdpSocket`.
//! - [`SystemDnsTransport`]: Standard OS-level fallback resolution via `tokio::net::lookup_host`.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicU16, Ordering};

use crate::dns::resolver::DnsTransport;
use crate::error::{DiscoveryError, Result};

// ============================================================================
// 1. RFC 1035 UDP Wire Transport
// ============================================================================

/// Asynchronous RFC 1035 wire-format DNS transport operating directly over UDP sockets.
///
/// Features:
/// - Pure zero-dependency wire protocol serialization and parsing.
/// - Atomic query transaction ID generation.
/// - Parallel A (IPv4) and AAAA (IPv6) queries.
/// - Automatic skipping of DNS compression pointers (`0xC0`).
/// - Zero heap allocations during packet decoding beyond the resulting IP vector.
#[derive(Debug, Clone)]
pub struct UdpDnsTransport {
    next_id: Arc<AtomicU16>,
}

impl Default for UdpDnsTransport {
    fn default() -> Self {
        Self {
            next_id: Arc::new(AtomicU16::new(1)),
        }
    }
}

impl UdpDnsTransport {
    /// Creates a new `UdpDnsTransport` instance.
    pub fn new() -> Self {
        Self::default()
    }

    /// Queries a single record type (A = 1, AAAA = 28) against `server`.
    async fn query_record(
        &self,
        server: SocketAddr,
        host: &str,
        qtype: u16,
    ) -> Result<Vec<IpAddr>> {
        let bind_addr = if server.is_ipv6() {
            "[::]:0"
        } else {
            "0.0.0.0:0"
        };

        let socket = tokio::net::UdpSocket::bind(bind_addr).await.map_err(|e| {
            DiscoveryError::ServerUnreachable {
                address: server,
                reason: format!("failed to bind local UDP socket: {e}"),
            }
        })?;

        socket
            .connect(server)
            .await
            .map_err(|e| DiscoveryError::ServerUnreachable {
                address: server,
                reason: format!("failed to connect to DNS server: {e}"),
            })?;

        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let packet = build_query_packet(id, host, qtype).ok_or_else(|| {
            DiscoveryError::DnsResolutionFailed {
                host: host.to_string(),
                reason: "invalid domain name for DNS query".into(),
            }
        })?;

        socket
            .send(&packet)
            .await
            .map_err(|e| DiscoveryError::ServerUnreachable {
                address: server,
                reason: format!("failed to send DNS query packet: {e}"),
            })?;

        let mut buf = [0u8; 1024];
        let len = socket
            .recv(&mut buf)
            .await
            .map_err(|e| DiscoveryError::ServerUnreachable {
                address: server,
                reason: format!("failed to receive DNS response: {e}"),
            })?;

        parse_response_packet(id, host, &buf[..len])
    }
}

impl DnsTransport for UdpDnsTransport {
    async fn query(&self, server: SocketAddr, host: &str) -> Result<Vec<IpAddr>> {
        let (res_a, res_aaaa) = tokio::join!(
            self.query_record(server, host, 1),
            self.query_record(server, host, 28)
        );

        let mut ips = Vec::new();
        let mut last_err = None;

        match res_a {
            Ok(records) => ips.extend(records),
            Err(e) => last_err = Some(e),
        }

        match res_aaaa {
            Ok(records) => ips.extend(records),
            Err(e) => {
                if last_err.is_none() {
                    last_err = Some(e);
                }
            }
        }

        if !ips.is_empty() {
            Ok(ips)
        } else if let Some(err) = last_err {
            Err(err)
        } else {
            Err(DiscoveryError::DnsResolutionFailed {
                host: host.to_string(),
                reason: "no A or AAAA records returned".into(),
            })
        }
    }
}

// ============================================================================
// 2. System OS DNS Transport
// ============================================================================

/// DNS transport that delegates domain resolution to the operating system resolver
/// via `tokio::net::lookup_host`.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemDnsTransport;

impl SystemDnsTransport {
    /// Creates a new `SystemDnsTransport`.
    pub fn new() -> Self {
        Self
    }
}

impl DnsTransport for SystemDnsTransport {
    async fn query(&self, _server: SocketAddr, host: &str) -> Result<Vec<IpAddr>> {
        let socket_addr_str = format!("{host}:0");
        match tokio::net::lookup_host(&socket_addr_str).await {
            Ok(iter) => {
                let ips: Vec<IpAddr> = iter.map(|sa| sa.ip()).collect();
                if ips.is_empty() {
                    Err(DiscoveryError::DnsResolutionFailed {
                        host: host.to_string(),
                        reason: "system resolver returned 0 addresses".into(),
                    })
                } else {
                    Ok(ips)
                }
            }
            Err(e) => Err(DiscoveryError::DnsResolutionFailed {
                host: host.to_string(),
                reason: e.to_string(),
            }),
        }
    }
}

// ============================================================================
// 3. RFC 1035 Packet Encoding & Decoding Logic
// ============================================================================

/// Builds a 12-byte header + question section for a standard DNS query.
pub(crate) fn build_query_packet(id: u16, host: &str, qtype: u16) -> Option<Vec<u8>> {
    let host = host.trim_end_matches('.');
    if host.is_empty() || host.len() > 253 {
        return None;
    }

    let mut packet = Vec::with_capacity(12 + host.len() + 2 + 4);

    // RFC 1035 12-byte Header
    packet.extend_from_slice(&id.to_be_bytes()); // ID
    packet.extend_from_slice(&0x0100u16.to_be_bytes()); // Flags: RD=1 (Recursion Desired)
    packet.extend_from_slice(&1u16.to_be_bytes()); // QDCOUNT = 1
    packet.extend_from_slice(&0u16.to_be_bytes()); // ANCOUNT = 0
    packet.extend_from_slice(&0u16.to_be_bytes()); // NSCOUNT = 0
    packet.extend_from_slice(&0u16.to_be_bytes()); // ARCOUNT = 0

    // Question Section: Labels
    for label in host.split('.') {
        if label.is_empty() || label.len() > 63 {
            return None;
        }
        packet.push(label.len() as u8);
        packet.extend_from_slice(label.as_bytes());
    }
    packet.push(0); // Root label null terminator

    // QTYPE and QCLASS
    packet.extend_from_slice(&qtype.to_be_bytes()); // QTYPE (1 = A, 28 = AAAA)
    packet.extend_from_slice(&1u16.to_be_bytes()); // QCLASS (1 = IN)

    Some(packet)
}

/// Skips a DNS name in wire representation, safely advancing across label sequences
/// and RFC 1035 compression pointers (`0xC0`).
pub(crate) fn skip_name(buf: &[u8], mut pos: usize) -> Option<usize> {
    loop {
        if pos >= buf.len() {
            return None;
        }
        let b = buf[pos];
        if b == 0 {
            return Some(pos + 1);
        }
        if (b & 0xC0) == 0xC0 {
            // 2-byte compression pointer: terminates name
            if pos + 2 > buf.len() {
                return None;
            }
            return Some(pos + 2);
        }
        let len = b as usize;
        pos += 1;
        if pos + len > buf.len() {
            return None;
        }
        pos += len;
    }
}

/// Parses an RFC 1035 DNS response buffer and extracts IP addresses.
pub(crate) fn parse_response_packet(query_id: u16, host: &str, buf: &[u8]) -> Result<Vec<IpAddr>> {
    if buf.len() < 12 {
        return Err(DiscoveryError::DnsResolutionFailed {
            host: host.to_string(),
            reason: "DNS response packet too short (< 12 bytes)".into(),
        });
    }

    let resp_id = u16::from_be_bytes([buf[0], buf[1]]);
    if resp_id != query_id {
        return Err(DiscoveryError::DnsResolutionFailed {
            host: host.to_string(),
            reason: format!("DNS response ID mismatch (expected {query_id}, got {resp_id})"),
        });
    }

    let flags = u16::from_be_bytes([buf[2], buf[3]]);
    let is_response = (flags & 0x8000) != 0;
    if !is_response {
        return Err(DiscoveryError::DnsResolutionFailed {
            host: host.to_string(),
            reason: "DNS packet is not a response".into(),
        });
    }

    let rcode = flags & 0x000F;
    if rcode == 3 {
        return Err(DiscoveryError::DnsResolutionFailed {
            host: host.to_string(),
            reason: "NXDOMAIN".into(),
        });
    } else if rcode != 0 {
        return Err(DiscoveryError::DnsResolutionFailed {
            host: host.to_string(),
            reason: format!("DNS server returned error code (RCODE={rcode})"),
        });
    }

    let qdcount = u16::from_be_bytes([buf[4], buf[5]]) as usize;
    let ancount = u16::from_be_bytes([buf[6], buf[7]]) as usize;

    let mut pos = 12;

    // Skip question section
    for _ in 0..qdcount {
        pos = skip_name(buf, pos).ok_or_else(|| DiscoveryError::DnsResolutionFailed {
            host: host.to_string(),
            reason: "malformed question section in DNS response".into(),
        })?;
        if pos + 4 > buf.len() {
            return Err(DiscoveryError::DnsResolutionFailed {
                host: host.to_string(),
                reason: "truncated question section in DNS response".into(),
            });
        }
        pos += 4; // QTYPE (2) + QCLASS (2)
    }

    // Parse answer section
    let mut ips = Vec::new();
    for _ in 0..ancount {
        pos = skip_name(buf, pos).ok_or_else(|| DiscoveryError::DnsResolutionFailed {
            host: host.to_string(),
            reason: "malformed answer section in DNS response".into(),
        })?;

        if pos + 10 > buf.len() {
            return Err(DiscoveryError::DnsResolutionFailed {
                host: host.to_string(),
                reason: "truncated answer header in DNS response".into(),
            });
        }

        let rtype = u16::from_be_bytes([buf[pos], buf[pos + 1]]);
        let rclass = u16::from_be_bytes([buf[pos + 2], buf[pos + 3]]);
        let rdlength = u16::from_be_bytes([buf[pos + 8], buf[pos + 9]]) as usize;
        pos += 10;

        if pos + rdlength > buf.len() {
            return Err(DiscoveryError::DnsResolutionFailed {
                host: host.to_string(),
                reason: "truncated answer data in DNS response".into(),
            });
        }

        // Only parse Internet Class (IN = 1)
        if rclass == 1 {
            if rtype == 1 && rdlength == 4 {
                // Type A (IPv4)
                let ip = Ipv4Addr::new(buf[pos], buf[pos + 1], buf[pos + 2], buf[pos + 3]);
                ips.push(IpAddr::V4(ip));
            } else if rtype == 28 && rdlength == 16 {
                // Type AAAA (IPv6)
                let mut octets = [0u8; 16];
                octets.copy_from_slice(&buf[pos..pos + 16]);
                let ip = Ipv6Addr::from(octets);
                ips.push(IpAddr::V6(ip));
            }
        }

        pos += rdlength;
    }

    Ok(ips)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_query_packet() {
        let packet = build_query_packet(0x1234, "api.internal", 1).unwrap();
        assert_eq!(&packet[0..2], &0x1234u16.to_be_bytes());
        assert_eq!(&packet[2..4], &0x0100u16.to_be_bytes()); // RD=1
        assert_eq!(&packet[4..6], &1u16.to_be_bytes()); // QDCOUNT=1
        assert_eq!(&packet[6..8], &0u16.to_be_bytes()); // ANCOUNT=0

        // Label: 3 'a' 'p' 'i' 8 'i' 'n' 't' 'e' 'r' 'n' 'a' 'l' 0
        assert_eq!(packet[12], 3);
        assert_eq!(&packet[13..16], b"api");
        assert_eq!(packet[16], 8);
        assert_eq!(&packet[17..25], b"internal");
        assert_eq!(packet[25], 0);

        // QTYPE=1, QCLASS=1
        assert_eq!(&packet[26..28], &1u16.to_be_bytes());
        assert_eq!(&packet[28..30], &1u16.to_be_bytes());
    }

    #[test]
    fn test_parse_response_packet_valid_a_record() {
        let query_id = 0xABCDu16;
        let mut resp = Vec::new();

        // Header: ID=0xABCD, Flags=0x8180 (Response, Recursion Available), QDCOUNT=1, ANCOUNT=1
        resp.extend_from_slice(&query_id.to_be_bytes());
        resp.extend_from_slice(&0x8180u16.to_be_bytes());
        resp.extend_from_slice(&1u16.to_be_bytes());
        resp.extend_from_slice(&1u16.to_be_bytes());
        resp.extend_from_slice(&0u16.to_be_bytes());
        resp.extend_from_slice(&0u16.to_be_bytes());

        // Question: api.internal, QTYPE=1, QCLASS=1
        resp.push(3);
        resp.extend_from_slice(b"api");
        resp.push(8);
        resp.extend_from_slice(b"internal");
        resp.push(0);
        resp.extend_from_slice(&1u16.to_be_bytes());
        resp.extend_from_slice(&1u16.to_be_bytes());

        // Answer: Compression pointer to offset 12 (0xC00C)
        resp.extend_from_slice(&[0xC0, 0x0C]);
        resp.extend_from_slice(&1u16.to_be_bytes()); // TYPE A
        resp.extend_from_slice(&1u16.to_be_bytes()); // CLASS IN
        resp.extend_from_slice(&300u32.to_be_bytes()); // TTL
        resp.extend_from_slice(&4u16.to_be_bytes()); // RDLENGTH = 4
        resp.extend_from_slice(&[192, 168, 1, 100]); // 192.168.1.100

        let ips = parse_response_packet(query_id, "api.internal", &resp).unwrap();
        assert_eq!(ips, vec!["192.168.1.100".parse::<IpAddr>().unwrap()]);
    }

    #[test]
    fn test_parse_response_packet_nxdomain() {
        let query_id = 0x5555u16;
        let mut resp = Vec::new();

        // Header: Flags with RCODE=3 (0x8183)
        resp.extend_from_slice(&query_id.to_be_bytes());
        resp.extend_from_slice(&0x8183u16.to_be_bytes());
        resp.extend_from_slice(&1u16.to_be_bytes());
        resp.extend_from_slice(&0u16.to_be_bytes());
        resp.extend_from_slice(&0u16.to_be_bytes());
        resp.extend_from_slice(&0u16.to_be_bytes());

        // Question
        resp.push(4);
        resp.extend_from_slice(b"none");
        resp.push(0);
        resp.extend_from_slice(&1u16.to_be_bytes());
        resp.extend_from_slice(&1u16.to_be_bytes());

        let err = parse_response_packet(query_id, "none", &resp).unwrap_err();
        match err {
            DiscoveryError::DnsResolutionFailed { reason, .. } => {
                assert_eq!(reason, "NXDOMAIN");
            }
            _ => panic!("Expected DnsResolutionFailed with NXDOMAIN"),
        }
    }

    #[tokio::test]
    async fn test_udp_dns_transport_local_mock_server() {
        let mock_server = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let server_addr = mock_server.local_addr().unwrap();

        let transport = UdpDnsTransport::new();

        // Spawn mock DNS server task that handles incoming queries
        tokio::spawn(async move {
            let mut buf = [0u8; 512];
            for _ in 0..2 {
                if let Ok((len, peer)) = mock_server.recv_from(&mut buf).await {
                    let req_id = u16::from_be_bytes([buf[0], buf[1]]);
                    let qtype = u16::from_be_bytes([buf[len - 4], buf[len - 3]]);

                    let mut resp = Vec::new();
                    resp.extend_from_slice(&req_id.to_be_bytes());
                    resp.extend_from_slice(&0x8180u16.to_be_bytes()); // QR=1, RD=1, RA=1
                    resp.extend_from_slice(&1u16.to_be_bytes()); // QDCOUNT=1
                    if qtype == 1 {
                        // Answer with A record
                        resp.extend_from_slice(&1u16.to_be_bytes()); // ANCOUNT=1
                        resp.extend_from_slice(&0u16.to_be_bytes());
                        resp.extend_from_slice(&0u16.to_be_bytes());
                        // Echo question
                        resp.extend_from_slice(&buf[12..len]);
                        // Answer
                        resp.extend_from_slice(&[0xC0, 0x0C]); // Pointer
                        resp.extend_from_slice(&1u16.to_be_bytes()); // A
                        resp.extend_from_slice(&1u16.to_be_bytes()); // IN
                        resp.extend_from_slice(&60u32.to_be_bytes()); // TTL
                        resp.extend_from_slice(&4u16.to_be_bytes());
                        resp.extend_from_slice(&[10, 0, 0, 99]);
                    } else {
                        // Return empty answer for AAAA
                        resp.extend_from_slice(&0u16.to_be_bytes()); // ANCOUNT=0
                        resp.extend_from_slice(&0u16.to_be_bytes());
                        resp.extend_from_slice(&0u16.to_be_bytes());
                        resp.extend_from_slice(&buf[12..len]);
                    }

                    let _ = mock_server.send_to(&resp, peer).await;
                }
            }
        });

        let ips = transport.query(server_addr, "mock.local").await.unwrap();
        assert_eq!(ips, vec!["10.0.0.99".parse::<IpAddr>().unwrap()]);
    }
}
