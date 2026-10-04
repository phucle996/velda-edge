//! Production DNS wire transports for `velda-discovery`.
//!
//! Provides:
//! - [`UdpDnsTransport`]: Pure RFC 1035 wire query implementation over UDP using `tokio::net::UdpSocket`,
//!   with EDNS0 (RFC 6891) and TCP fallback (RFC 7766) when the answer does not fit in a datagram.
//! - [`SystemDnsTransport`]: Standard OS-level fallback resolution via `tokio::net::lookup_host`.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::dns::resolver::{DnsAnswer, DnsTransport};
use crate::error::{DiscoveryError, Result};

// ============================================================================
// 1. RFC 1035 UDP Wire Transport
// ============================================================================

/// UDP receive buffer size and the payload size advertised through EDNS0.
///
/// 1232 is the DNS Flag Day 2020 value: the largest payload that fits an IPv6
/// minimum-MTU packet without fragmentation, so answers are not lost to
/// fragment filtering and there is no need to probe path MTU.
pub const DEFAULT_DNS_PACKET_BUFFER_SIZE: usize = 1232;

/// Asynchronous RFC 1035 wire-format DNS transport operating directly over UDP sockets.
///
/// Stateless: every query draws fresh random transaction IDs, so the struct
/// holds nothing that needs sharing across tasks.
///
/// Per query it:
/// - uses one connected UDP socket for both A and AAAA (one bind/connect, not two);
/// - draws unpredictable transaction IDs, on top of the OS-randomized source port,
///   so an off-path attacker must guess ~32 bits to forge an answer;
/// - discards datagrams whose ID or question section do not match what was asked,
///   instead of failing, so a spoofed packet cannot poison or abort the lookup;
/// - falls back to TCP when the server sets TC (truncated) or rejects EDNS0 (FORMERR).
#[derive(Debug, Default, Clone, Copy)]
pub struct UdpDnsTransport;

impl UdpDnsTransport {
    /// Creates a new `UdpDnsTransport` instance.
    pub fn new() -> Self {
        Self
    }
}

/// Retries one record type over TCP (2-byte length-prefixed framing, RFC 1035 §4.2.2).
///
/// Plain query without EDNS0: TCP has no datagram size limit, and this also
/// serves servers that answered the EDNS0 query with FORMERR.
async fn query_tcp(server: SocketAddr, host: &str, qtype: u16) -> Result<DnsAnswer> {
    let unreachable = |what: &str, e: std::io::Error| DiscoveryError::ServerUnreachable {
        address: server,
        reason: format!("{what}: {e}"),
    };

    let id = getrandom::u32().map_err(|e| DiscoveryError::DnsResolutionFailed {
        host: host.to_string(),
        reason: format!("failed to draw random transaction id: {e}"),
    })? as u16;
    let packet =
        build_query_packet(id, host, qtype).ok_or_else(|| DiscoveryError::DnsResolutionFailed {
            host: host.to_string(),
            reason: "invalid domain name for DNS query".into(),
        })?;

    let mut stream = tokio::net::TcpStream::connect(server)
        .await
        .map_err(|e| unreachable("failed to connect to DNS server over TCP", e))?;

    let mut framed = Vec::with_capacity(2 + packet.len());
    framed.extend_from_slice(&(packet.len() as u16).to_be_bytes());
    framed.extend_from_slice(&packet);
    stream
        .write_all(&framed)
        .await
        .map_err(|e| unreachable("failed to send DNS query over TCP", e))?;

    let mut len_buf = [0u8; 2];
    stream
        .read_exact(&mut len_buf)
        .await
        .map_err(|e| unreachable("failed to read DNS response length over TCP", e))?;
    let mut body = vec![0u8; u16::from_be_bytes(len_buf) as usize];
    stream
        .read_exact(&mut body)
        .await
        .map_err(|e| unreachable("failed to read DNS response over TCP", e))?;

    // TCP is not spoofable off-path, but a confused server could still answer another question.
    if body.len() < 12 || !question_matches(&body, host, qtype) {
        return Err(DiscoveryError::DnsResolutionFailed {
            host: host.to_string(),
            reason: "DNS TCP response does not match the question".into(),
        });
    }
    parse_response_packet(id, host, &body)
}

impl DnsTransport for UdpDnsTransport {
    async fn query(&self, server: SocketAddr, host: &str) -> Result<DnsAnswer> {
        let unreachable = |what: &str, e: std::io::Error| DiscoveryError::ServerUnreachable {
            address: server,
            reason: format!("{what}: {e}"),
        };

        let bind_addr = if server.is_ipv6() {
            "[::]:0"
        } else {
            "0.0.0.0:0"
        };
        let socket = tokio::net::UdpSocket::bind(bind_addr)
            .await
            .map_err(|e| unreachable("failed to bind local UDP socket", e))?;
        // `connect` makes the kernel drop datagrams from any other source address.
        socket
            .connect(server)
            .await
            .map_err(|e| unreachable("failed to connect to DNS server", e))?;

        let rand = getrandom::u32().map_err(|e| DiscoveryError::DnsResolutionFailed {
            host: host.to_string(),
            reason: format!("failed to draw random transaction id: {e}"),
        })?;
        let id_a = (rand >> 16) as u16;
        let mut id_aaaa = rand as u16;
        if id_aaaa == id_a {
            // Responses are demultiplexed by ID; the two queries must not collide.
            id_aaaa = id_a.wrapping_add(1);
        }

        // Both queries go out back-to-back on the same socket; the resolver's
        // `query_timeout` bounds the whole exchange.
        for (id, qtype) in [(id_a, 1u16), (id_aaaa, 28u16)] {
            let mut packet = build_query_packet(id, host, qtype).ok_or_else(|| {
                DiscoveryError::DnsResolutionFailed {
                    host: host.to_string(),
                    reason: "invalid domain name for DNS query".into(),
                }
            })?;
            // EDNS0 OPT pseudo-record (RFC 6891): ARCOUNT=1, root name, TYPE=41,
            // CLASS=advertised UDP payload size, TTL=0 (no flags), RDLEN=0.
            packet[10..12].copy_from_slice(&1u16.to_be_bytes());
            packet.push(0);
            packet.extend_from_slice(&41u16.to_be_bytes());
            packet.extend_from_slice(&(DEFAULT_DNS_PACKET_BUFFER_SIZE as u16).to_be_bytes());
            packet.extend_from_slice(&0u32.to_be_bytes());
            packet.extend_from_slice(&0u16.to_be_bytes());
            socket
                .send(&packet)
                .await
                .map_err(|e| unreachable("failed to send DNS query packet", e))?;
        }

        let mut res_a: Option<Result<DnsAnswer>> = None;
        let mut res_aaaa: Option<Result<DnsAnswer>> = None;
        let mut buf = [0u8; DEFAULT_DNS_PACKET_BUFFER_SIZE];

        while res_a.is_none() || res_aaaa.is_none() {
            let len = socket
                .recv(&mut buf)
                .await
                .map_err(|e| unreachable("failed to receive DNS response", e))?;
            let pkt = &buf[..len];
            if len < 12 {
                continue;
            }

            let rid = u16::from_be_bytes([pkt[0], pkt[1]]);
            let (qtype, slot) = if rid == id_a && res_a.is_none() {
                (1u16, &mut res_a)
            } else if rid == id_aaaa && res_aaaa.is_none() {
                (28u16, &mut res_aaaa)
            } else {
                // Unknown, duplicate or forged ID: ignore and keep waiting.
                continue;
            };
            if !question_matches(pkt, host, qtype) {
                continue;
            }

            let flags = u16::from_be_bytes([pkt[2], pkt[3]]);
            let truncated = flags & 0x0200 != 0;
            let formerr = flags & 0x000F == 1;
            *slot = Some(if truncated || formerr {
                query_tcp(server, host, qtype).await
            } else {
                parse_response_packet(rid, host, pkt)
            });
        }

        let mut ips = Vec::new();
        let mut ttl: Option<Duration> = None;
        let mut last_err = None;
        for res in [res_a, res_aaaa].into_iter().flatten() {
            match res {
                Ok(answer) => {
                    ips.extend(answer.ips);
                    // The A and AAAA sets are cached together, so the shorter TTL wins.
                    ttl = match (ttl, answer.ttl) {
                        (Some(a), Some(b)) => Some(a.min(b)),
                        (a, b) => a.or(b),
                    };
                }
                Err(e) => {
                    if last_err.is_none() {
                        last_err = Some(e);
                    }
                }
            }
        }

        if !ips.is_empty() {
            Ok(DnsAnswer { ips, ttl })
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

/// Returns true when the response carries exactly one question equal to
/// (`host`, `qtype`, IN). Names are compared case-insensitively because
/// resolvers may apply 0x20 case randomization or normalize case.
fn question_matches(buf: &[u8], host: &str, qtype: u16) -> bool {
    if buf.len() < 12 || u16::from_be_bytes([buf[4], buf[5]]) != 1 {
        return false;
    }
    let mut pos = 12;
    for label in host.trim_end_matches('.').split('.') {
        let end = pos + 1 + label.len();
        if end > buf.len()
            || buf[pos] as usize != label.len()
            || !buf[pos + 1..end].eq_ignore_ascii_case(label.as_bytes())
        {
            return false;
        }
        pos = end;
    }
    // Root terminator, QTYPE, QCLASS=IN.
    pos + 5 <= buf.len()
        && buf[pos] == 0
        && u16::from_be_bytes([buf[pos + 1], buf[pos + 2]]) == qtype
        && u16::from_be_bytes([buf[pos + 3], buf[pos + 4]]) == 1
}

// ============================================================================
// 2. System OS DNS Transport
// ============================================================================

/// DNS transport that delegates domain resolution to the operating system resolver
/// via `tokio::net::lookup_host`.
///
/// The OS resolver picks its own nameservers, so the `server` argument is ignored:
/// per-server failover in the resolver has no effect with this transport.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemDnsTransport;

impl SystemDnsTransport {
    /// Creates a new `SystemDnsTransport`.
    pub fn new() -> Self {
        Self
    }
}

impl DnsTransport for SystemDnsTransport {
    async fn query(&self, _server: SocketAddr, host: &str) -> Result<DnsAnswer> {
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
                    Ok(DnsAnswer { ips, ttl: None })
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
pub fn build_query_packet(id: u16, host: &str, qtype: u16) -> Option<Vec<u8>> {
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
pub fn skip_name(buf: &[u8], mut pos: usize) -> Option<usize> {
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
        // RFC 1035 §4.1.4: labels must be 63 octets or less
        if len > 63 || pos + 1 + len > buf.len() {
            return None;
        }
        pos += 1 + len;
    }
}

/// Parses an RFC 1035 DNS response buffer and extracts IP addresses.
pub fn parse_response_packet(query_id: u16, host: &str, buf: &[u8]) -> Result<DnsAnswer> {
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
        // RFC 2308 §5: the negative-caching TTL is min(SOA record TTL, SOA MINIMUM).
        // Any malformed section simply yields `None`; the NXDOMAIN verdict still stands.
        let negative_ttl = (|| -> Option<Duration> {
            let qdcount = u16::from_be_bytes([buf[4], buf[5]]) as usize;
            let rr_count = u16::from_be_bytes([buf[6], buf[7]]) as usize
                + u16::from_be_bytes([buf[8], buf[9]]) as usize;
            let mut pos = 12;
            for _ in 0..qdcount {
                pos = skip_name(buf, pos)? + 4;
                if pos > buf.len() {
                    return None;
                }
            }
            let mut soa_ttl: Option<u32> = None;
            for _ in 0..rr_count {
                pos = skip_name(buf, pos)?;
                if pos + 10 > buf.len() {
                    return None;
                }
                let rtype = u16::from_be_bytes([buf[pos], buf[pos + 1]]);
                let rr_ttl =
                    u32::from_be_bytes([buf[pos + 4], buf[pos + 5], buf[pos + 6], buf[pos + 7]]);
                let rdlength = u16::from_be_bytes([buf[pos + 8], buf[pos + 9]]) as usize;
                pos += 10;
                if pos + rdlength > buf.len() {
                    return None;
                }
                if rtype == 6 {
                    // SOA RDATA: MNAME, RNAME, SERIAL, REFRESH, RETRY, EXPIRE, MINIMUM.
                    let rname_end = skip_name(buf, skip_name(buf, pos)?)?;
                    if rname_end + 20 > pos + rdlength {
                        return None;
                    }
                    let minimum = u32::from_be_bytes([
                        buf[rname_end + 16],
                        buf[rname_end + 17],
                        buf[rname_end + 18],
                        buf[rname_end + 19],
                    ]);
                    soa_ttl = Some(rr_ttl.min(minimum));
                }
                pos += rdlength;
            }
            soa_ttl.map(|s| Duration::from_secs(u64::from(s)))
        })();
        return Err(DiscoveryError::NameNotFound {
            host: host.to_string(),
            negative_ttl,
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
    let mut min_ttl: Option<u32> = None;
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
        let ttl = u32::from_be_bytes([buf[pos + 4], buf[pos + 5], buf[pos + 6], buf[pos + 7]]);
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
                min_ttl = Some(min_ttl.map_or(ttl, |m| m.min(ttl)));
            } else if rtype == 28 && rdlength == 16 {
                // Type AAAA (IPv6)
                let mut octets = [0u8; 16];
                octets.copy_from_slice(&buf[pos..pos + 16]);
                let ip = Ipv6Addr::from(octets);
                ips.push(IpAddr::V6(ip));
                min_ttl = Some(min_ttl.map_or(ttl, |m| m.min(ttl)));
            }
        }

        pos += rdlength;
    }

    Ok(DnsAnswer {
        ips,
        ttl: min_ttl.map(|s| Duration::from_secs(u64::from(s))),
    })
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
        assert_eq!(ips.ips, vec!["192.168.1.100".parse::<IpAddr>().unwrap()]);
        assert_eq!(ips.ttl, Some(Duration::from_secs(300)));
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
            DiscoveryError::NameNotFound { negative_ttl, .. } => {
                assert_eq!(negative_ttl, None, "no SOA in response");
            }
            _ => panic!("Expected NameNotFound"),
        }
    }

    #[test]
    fn test_parse_response_packet_nxdomain_soa_negative_ttl() {
        let query_id = 0x7777u16;
        let mut resp = Vec::new();
        resp.extend_from_slice(&query_id.to_be_bytes());
        resp.extend_from_slice(&0x8183u16.to_be_bytes()); // NXDOMAIN
        resp.extend_from_slice(&1u16.to_be_bytes()); // QDCOUNT
        resp.extend_from_slice(&0u16.to_be_bytes()); // ANCOUNT
        resp.extend_from_slice(&1u16.to_be_bytes()); // NSCOUNT
        resp.extend_from_slice(&0u16.to_be_bytes());

        resp.push(4);
        resp.extend_from_slice(b"none");
        resp.push(0);
        resp.extend_from_slice(&1u16.to_be_bytes());
        resp.extend_from_slice(&1u16.to_be_bytes());

        // Authority: SOA for the root, RR TTL 900 and MINIMUM 120 -> negative TTL 120.
        resp.push(0);
        resp.extend_from_slice(&6u16.to_be_bytes());
        resp.extend_from_slice(&1u16.to_be_bytes());
        resp.extend_from_slice(&900u32.to_be_bytes());
        let rdata: Vec<u8> = {
            let mut r = vec![0u8, 0u8]; // MNAME=root, RNAME=root
            for v in [1u32, 3600, 600, 86400, 120] {
                r.extend_from_slice(&v.to_be_bytes());
            }
            r
        };
        resp.extend_from_slice(&(rdata.len() as u16).to_be_bytes());
        resp.extend_from_slice(&rdata);

        match parse_response_packet(query_id, "none", &resp).unwrap_err() {
            DiscoveryError::NameNotFound { negative_ttl, .. } => {
                assert_eq!(negative_ttl, Some(Duration::from_secs(120)));
            }
            other => panic!("Expected NameNotFound, got {other:?}"),
        }
    }
}
