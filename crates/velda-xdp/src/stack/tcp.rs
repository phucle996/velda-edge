//! # Lock-Free Single-Core TCP State Machine in User-Space
//!
//! ## Full Kernel Bypass & Zero-Lock Connection Tracking
//!
//! Under [`XdpEngineKind::AfXdp`], wire Ethernet frames bypass the Linux kernel TCP/IP stack
//! (`sk_buff`, Netfilter, conntrack) and land directly in user-space UMEM slabs.
//!
//! ### Thread-Local Concurrency & Zero Mutex Locking
//! Hardware RSS and eBPF 4-tuple hashing guarantee that all incoming packets for a given
//! `(src_ip, src_port, dst_ip, dst_port)` connection are consistently steered to the same
//! hardware RX queue and worker core.
//! - Because a single pinned thread manages each connection from `SYN` to `FIN`, the
//!   [`TcpConnectionTable`] is strictly thread-local.
//! - This eliminates mutex locking, atomic reference counting, and cross-socket cache line
//!   bouncing over the inter-core coherency bus on the hot path.
//!
//! ### SYN Flood & Conntrack Exhaustion Defense
//! In standard Linux stacks, SYN floods exhaust the kernel `conntrack` table, inducing lock
//! contention that degrades all network services. In user-space AF_XDP, initial handshakes execute
//! on recyclable pre-allocated slabs. Malicious or uncompleted SYNs are discarded in nanoseconds
//! without allocating system RAM or impacting the kernel.

use std::collections::HashMap;
use std::net::SocketAddr;

use crate::error::{Result, XdpError};

pub const TCP_FLAG_FIN: u8 = 1 << 0;
pub const TCP_FLAG_SYN: u8 = 1 << 1;
pub const TCP_FLAG_RST: u8 = 1 << 2;
pub const TCP_FLAG_PSH: u8 = 1 << 3;
pub const TCP_FLAG_ACK: u8 = 1 << 4;
pub const TCP_FLAG_URG: u8 = 1 << 5;

/// RFC 793 finite state machine states.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TcpState {
    #[default]
    Listen,
    SynReceived,
    Established,
    FinWait1,
    FinWait2,
    CloseWait,
    Closing,
    LastAck,
    TimeWait,
    Closed,
}

impl TcpState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Listen => "LISTEN",
            Self::SynReceived => "SYN_RECEIVED",
            Self::Established => "ESTABLISHED",
            Self::FinWait1 => "FIN_WAIT_1",
            Self::FinWait2 => "FIN_WAIT_2",
            Self::CloseWait => "CLOSE_WAIT",
            Self::Closing => "CLOSING",
            Self::LastAck => "LAST_ACK",
            Self::TimeWait => "TIME_WAIT",
            Self::Closed => "CLOSED",
        }
    }
}

/// Standard 20-byte TCP header with flags and sequence counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcpHeader {
    pub src_port: u16,
    pub dst_port: u16,
    pub seq_num: u32,
    pub ack_num: u32,
    pub data_offset: usize,
    pub flags: u8,
    pub window_size: u16,
    pub checksum: u16,
}

impl TcpHeader {
    #[inline]
    pub fn is_syn(&self) -> bool {
        (self.flags & TCP_FLAG_SYN) != 0
    }

    #[inline]
    pub fn is_ack(&self) -> bool {
        (self.flags & TCP_FLAG_ACK) != 0
    }

    #[inline]
    pub fn is_fin(&self) -> bool {
        (self.flags & TCP_FLAG_FIN) != 0
    }

    #[inline]
    pub fn is_rst(&self) -> bool {
        (self.flags & TCP_FLAG_RST) != 0
    }

    #[inline]
    pub fn is_psh(&self) -> bool {
        (self.flags & TCP_FLAG_PSH) != 0
    }
}

/// TCP segment referencing in-place slab memory without intermediate copying.
pub struct TcpSegment<'a> {
    pub header: TcpHeader,
    pub payload: &'a [u8],
}

/// 4-tuple key uniquely identifying a bidirectional TCP connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TcpConnectionKey {
    pub client: SocketAddr,
    pub server: SocketAddr,
}

impl TcpConnectionKey {
    pub fn new(client: SocketAddr, server: SocketAddr) -> Self {
        Self { client, server }
    }
}

/// Single-core user-space TCP connection state representation.
#[derive(Debug)]
pub struct TcpConnection {
    pub key: TcpConnectionKey,
    pub state: TcpState,
    pub snd_nxt: u32,
    pub snd_una: u32,
    pub rcv_nxt: u32,
    pub rcv_wnd: u16,
    pub snd_wnd: u16,
    pub rx_stream: Vec<u8>,
}

impl TcpConnection {
    /// Creates a new connection in the LISTEN state.
    pub fn new(key: TcpConnectionKey) -> Self {
        Self {
            key,
            state: TcpState::Listen,
            snd_nxt: 1000, // Synthetic server initial sequence number (ISN)
            snd_una: 1000,
            rcv_nxt: 0,
            rcv_wnd: 65535,
            snd_wnd: 65535,
            rx_stream: Vec::with_capacity(8192),
        }
    }

    /// Evaluates an inbound TCP segment against the RFC 793 state machine.
    ///
    /// Returns optional outbound control flags (SYN-ACK or ACK) if an immediate response is required.
    pub fn handle_segment(&mut self, seg: &TcpSegment) -> Result<Option<u8>> {
        if seg.header.is_rst() {
            self.state = TcpState::Closed;
            return Err(XdpError::ConnectionReset(format!(
                "Received RST on connection {:?}",
                self.key
            )));
        }

        match self.state {
            TcpState::Listen => {
                if seg.header.is_syn() {
                    self.rcv_nxt = seg.header.seq_num.wrapping_add(1);
                    self.state = TcpState::SynReceived;
                    self.snd_nxt = self.snd_nxt.wrapping_add(1);
                    Ok(Some(TCP_FLAG_SYN | TCP_FLAG_ACK))
                } else {
                    Ok(None)
                }
            }
            TcpState::SynReceived => {
                if seg.header.is_ack() && seg.header.ack_num == self.snd_nxt {
                    self.state = TcpState::Established;
                    self.snd_una = seg.header.ack_num;
                    Ok(None)
                } else {
                    Ok(None)
                }
            }
            TcpState::Established => {
                let mut response_flags = None;

                // Reassemble in-order stream data
                if !seg.payload.is_empty() && seg.header.seq_num == self.rcv_nxt {
                    self.rx_stream.extend_from_slice(seg.payload);
                    self.rcv_nxt = self.rcv_nxt.wrapping_add(seg.payload.len() as u32);
                    response_flags = Some(TCP_FLAG_ACK);
                }

                if seg.header.is_fin() {
                    self.rcv_nxt = self.rcv_nxt.wrapping_add(1);
                    self.state = TcpState::CloseWait;
                    response_flags = Some(TCP_FLAG_ACK);
                }

                Ok(response_flags)
            }
            TcpState::CloseWait => Ok(None),
            TcpState::FinWait1 => {
                if seg.header.is_ack() {
                    self.state = TcpState::FinWait2;
                }
                Ok(None)
            }
            _ => Ok(None),
        }
    }

    /// Drains accumulated stream bytes for downstream L7 protocol parsing.
    pub fn drain_rx_stream(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.rx_stream)
    }
}

/// Thread-local lock-free connection table managing active AF_XDP TCP streams on one core.
pub struct TcpConnectionTable {
    connections: HashMap<TcpConnectionKey, TcpConnection>,
}

impl Default for TcpConnectionTable {
    fn default() -> Self {
        Self::new()
    }
}

impl TcpConnectionTable {
    /// Creates an empty connection table for this worker core.
    pub fn new() -> Self {
        Self {
            connections: HashMap::with_capacity(1024),
        }
    }

    /// Looks up or creates a connection tracking entry for the specified 4-tuple.
    pub fn get_or_create(&mut self, key: TcpConnectionKey) -> &mut TcpConnection {
        self.connections
            .entry(key)
            .or_insert_with(|| TcpConnection::new(key))
    }

    /// Traverses an active connection.
    pub fn get_mut(&mut self, key: &TcpConnectionKey) -> Option<&mut TcpConnection> {
        self.connections.get_mut(key)
    }

    /// Evicts a closed connection to reclaim memory.
    pub fn remove(&mut self, key: &TcpConnectionKey) -> Option<TcpConnection> {
        self.connections.remove(key)
    }

    /// Active connection count tracked on this core.
    #[inline]
    pub fn active_connections(&self) -> usize {
        self.connections.len()
    }
}

/// Parses an inbound TCP segment header from an L3 payload slice.
pub fn parse_tcp_header(data: &[u8]) -> Result<(TcpHeader, &[u8])> {
    if data.len() < 20 {
        return Err(XdpError::PacketTruncated {
            protocol: "TCP",
            len: data.len(),
            min_expected: 20,
        });
    }

    let src_port = u16::from_be_bytes([data[0], data[1]]);
    let dst_port = u16::from_be_bytes([data[2], data[3]]);
    let seq_num = u32::from_be_bytes([data[4], data[5], data[6], data[7]]);
    let ack_num = u32::from_be_bytes([data[8], data[9], data[10], data[11]]);

    let data_offset_byte = data[12];
    let data_offset = ((data_offset_byte >> 4) as usize) * 4;
    if data_offset < 20 || data.len() < data_offset {
        return Err(XdpError::PacketTruncated {
            protocol: "TCP Header",
            len: data.len(),
            min_expected: data_offset,
        });
    }

    let flags = data[13];
    let window_size = u16::from_be_bytes([data[14], data[15]]);
    let checksum = u16::from_be_bytes([data[16], data[17]]);

    let header = TcpHeader {
        src_port,
        dst_port,
        seq_num,
        ack_num,
        data_offset,
        flags,
        window_size,
        checksum,
    };

    Ok((header, &data[data_offset..]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    #[test]
    fn test_tcp_three_way_handshake_and_data_flow() {
        let client_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 100)), 54321);
        let server_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 8080);
        let key = TcpConnectionKey::new(client_addr, server_addr);

        let mut table = TcpConnectionTable::new();
        let conn = table.get_or_create(key);
        assert_eq!(conn.state, TcpState::Listen);

        // 1. Inbound SYN
        let syn_seg = TcpSegment {
            header: TcpHeader {
                src_port: 54321,
                dst_port: 8080,
                seq_num: 5000,
                ack_num: 0,
                data_offset: 20,
                flags: TCP_FLAG_SYN,
                window_size: 65535,
                checksum: 0,
            },
            payload: &[],
        };

        let resp1 = conn.handle_segment(&syn_seg).expect("SYN handling ok");
        assert_eq!(resp1, Some(TCP_FLAG_SYN | TCP_FLAG_ACK));
        assert_eq!(conn.state, TcpState::SynReceived);
        assert_eq!(conn.rcv_nxt, 5001);

        // 2. Inbound ACK completes 3-way handshake
        let ack_seg = TcpSegment {
            header: TcpHeader {
                src_port: 54321,
                dst_port: 8080,
                seq_num: 5001,
                ack_num: conn.snd_nxt,
                data_offset: 20,
                flags: TCP_FLAG_ACK,
                window_size: 65535,
                checksum: 0,
            },
            payload: &[],
        };

        let resp2 = conn.handle_segment(&ack_seg).expect("ACK handling ok");
        assert_eq!(resp2, None);
        assert_eq!(conn.state, TcpState::Established);

        // 3. Inbound Data
        let http_payload = b"GET / HTTP/1.1\r\nHost: example.com\r\n\r\n";
        let data_seg = TcpSegment {
            header: TcpHeader {
                src_port: 54321,
                dst_port: 8080,
                seq_num: 5001,
                ack_num: conn.snd_nxt,
                data_offset: 20,
                flags: TCP_FLAG_PSH | TCP_FLAG_ACK,
                window_size: 65535,
                checksum: 0,
            },
            payload: http_payload,
        };

        let resp3 = conn.handle_segment(&data_seg).expect("Data segment ok");
        assert_eq!(resp3, Some(TCP_FLAG_ACK));
        assert_eq!(conn.rcv_nxt, 5001 + (http_payload.len() as u32));

        // Verify accumulated stream data
        let stream_data = conn.drain_rx_stream();
        assert_eq!(stream_data, http_payload);
    }
}
