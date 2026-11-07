//! # End-to-End Integration & Architectural Invariant Tests for `velda-xdp`
//!
//! Validates:
//! 1. Declarative Opt-in: `Skb` mode binds cleanly on `lo` on standard Linux.
//! 2. Fail-Fast & LKG Protection: Requesting `Drv` on loopback or `Hw` on non-offloaded NIC
//!    fails immediately with strongly typed [`XdpError::DriverNotSupported`], preserving LKG.
//! 3. Zero-Copy Ingestion: Ethernet + IPv4 + UDP framing directly on [`XdpSlabLease`]
//!    with in-place L7 [`XdpSlabLease::header_workspace`] tracking.
//! 4. Decoupled Read/Write Streams: [`XdpStream::into_split`] round-trip and frame recycling.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use velda_xdp::config::DEFAULT_XDP_HEADER_WORKSPACE;
use velda_xdp::stack::L3Packet;
use velda_xdp::stack::frame::{
    ETHER_TYPE_IPV4, IP_PROTO_UDP, compute_internet_checksum, parse_ethernet_frame, parse_l3_packet,
};
use velda_xdp::stack::udp::parse_inbound_datagram;
use velda_xdp::{XdpAttachMode, XdpConfig, XdpEngineKind, XdpError, XdpListener, XdpSlabArena};

#[test]
fn test_listener_bind_skb_mode_on_loopback() {
    let config = XdpConfig::new("lo", 0)
        .with_attach_mode(XdpAttachMode::Skb)
        .with_engine_kind(XdpEngineKind::AfXdp)
        .with_frame_count(128);

    let listener = XdpListener::bind(config).expect("Binding Skb mode on loopback should succeed");
    assert_eq!(listener.profile().name, "lo");
    assert_eq!(listener.arena().frame_count(), 128);
    assert_eq!(listener.arena().available_count(), 128);
}

#[test]
fn test_listener_fail_fast_on_invalid_driver_for_lkg() {
    // Requesting Native Driver mode (Drv) on loopback must fail fast
    // to prevent invalid configurations from displacing the LKG runtime.
    let config_drv_on_lo = XdpConfig::new("lo", 0)
        .with_attach_mode(XdpAttachMode::Drv)
        .with_engine_kind(XdpEngineKind::AfXdp);

    match XdpListener::bind(config_drv_on_lo) {
        Err(XdpError::DriverNotSupported {
            interface, mode, ..
        }) => {
            assert_eq!(interface, "lo");
            assert_eq!(mode, "drv");
        }
        other => panic!("Expected DriverNotSupported error, got: {:?}", other),
    }

    // Requesting Hardware Offload (Hw) on loopback must also fail fast
    let config_hw_on_lo = XdpConfig::new("lo", 0)
        .with_attach_mode(XdpAttachMode::Hw)
        .with_engine_kind(XdpEngineKind::AfXdp);

    match XdpListener::bind(config_hw_on_lo) {
        Err(XdpError::DriverNotSupported {
            interface, mode, ..
        }) => {
            assert_eq!(interface, "lo");
            assert_eq!(mode, "hw");
        }
        other => panic!("Expected DriverNotSupported error, got: {:?}", other),
    }
}

#[test]
fn test_zero_copy_udp_datagram_and_header_workspace_pipeline() {
    let config = XdpConfig::new("lo", 0).with_frame_count(64);
    let arena = XdpSlabArena::new(&config).expect("UMEM arena creation ok");

    let mut lease = arena.lease().expect("Lease frame ok");

    // Synthesize an inbound Ethernet (14B) + IPv4 (20B) + UDP (8B) + HTTP/3 QUIC packet (30B)
    let payload_bytes = b"QUIC-INITIAL-PACKET-PAYLOAD-01";
    let total_len = 14 + 20 + 8 + payload_bytes.len();
    lease.set_payload_len(total_len);

    let buf = lease.payload_mut();

    // 1. Ethernet Header (14 bytes)
    buf[0..6].copy_from_slice(&[0x02, 0x00, 0x00, 0x00, 0x00, 0x01]); // Dst MAC
    buf[6..12].copy_from_slice(&[0x02, 0x00, 0x00, 0x00, 0x00, 0x02]); // Src MAC
    buf[12..14].copy_from_slice(&ETHER_TYPE_IPV4.to_be_bytes());

    // 2. IPv4 Header (20 bytes)
    buf[14] = 0x45; // Version 4, IHL 5
    buf[15] = 0x00; // DSCP/ECN
    let ipv4_total_len = (20 + 8 + payload_bytes.len()) as u16;
    buf[16..18].copy_from_slice(&ipv4_total_len.to_be_bytes());
    buf[23] = IP_PROTO_UDP;
    buf[26..30].copy_from_slice(&[192, 168, 1, 50]); // Client IP
    buf[30..34].copy_from_slice(&[127, 0, 0, 1]); // Server IP
    let ip_checksum = compute_internet_checksum(&buf[14..34]);
    buf[24..26].copy_from_slice(&ip_checksum.to_be_bytes());

    // 3. UDP Header (8 bytes)
    buf[34..36].copy_from_slice(&44333u16.to_be_bytes()); // Client port
    buf[36..38].copy_from_slice(&443u16.to_be_bytes()); // Server port (HTTP/3)
    let udp_len = (8 + payload_bytes.len()) as u16;
    buf[38..40].copy_from_slice(&udp_len.to_be_bytes());
    buf[40..42].copy_from_slice(&[0x00, 0x00]); // Checksum optional for test

    // 4. UDP Payload
    buf[42..42 + payload_bytes.len()].copy_from_slice(payload_bytes);

    // --- EXECUTE PIPELINE ---
    // A. Parse Ethernet L2
    let (eth, l3_slice) = parse_ethernet_frame(lease.payload()).expect("Ethernet ok");
    assert_eq!(eth.ether_type, ETHER_TYPE_IPV4);

    // B. Parse IPv4 L3
    let l3 = parse_l3_packet(eth.ether_type, l3_slice).expect("IPv4 ok");
    let (src_ip, dst_ip) = match l3 {
        L3Packet::Ipv4 { header, .. } => {
            assert_eq!(header.protocol, IP_PROTO_UDP);
            (IpAddr::V4(header.src_ip), IpAddr::V4(header.dst_ip))
        }
        _ => panic!("Expected IPv4"),
    };

    // C. Parse UDP L4 Datagram
    let datagram =
        parse_inbound_datagram(src_ip, dst_ip, 14 + 20, lease).expect("UDP datagram parse ok");

    assert_eq!(
        datagram.src,
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 50)), 44333)
    );
    assert_eq!(
        datagram.dst,
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 443)
    );
    assert_eq!(datagram.payload(), payload_bytes);

    // D. Use HeaderWorkspace within the same physical slab page for zero-copy token storage
    let workspace = datagram.lease.header_workspace();
    assert_eq!(workspace.len(), DEFAULT_XDP_HEADER_WORKSPACE);
}

#[test]
fn test_channel_stream_into_split_and_reclaim() {
    use velda_xdp::channel::{XdpReceiver, XdpSender, XdpStream};
    use velda_xdp::ffi::xdp_desc;
    use velda_xdp::rings::{CompletionRing, FillRing, RxRing, TxRing};

    let config = XdpConfig::new("lo", 0).with_frame_count(64);
    let arena = XdpSlabArena::new(&config).expect("UMEM arena ok");

    // Mock ring memory
    let ring_size = 16u32;
    let mut fill_prod = 0u32;
    let fill_cons = 0u32;
    let mut fill_storage = vec![0u64; ring_size as usize];

    let fill_ring = unsafe {
        FillRing::from_raw(
            &mut fill_prod as *mut u32,
            &fill_cons as *const u32,
            fill_storage.as_mut_ptr(),
            std::ptr::null(),
            ring_size,
        )
    };

    let rx_prod = 0u32;
    let mut rx_cons = 0u32;
    let rx_storage = vec![xdp_desc::default(); ring_size as usize];

    let rx_ring = unsafe {
        RxRing::from_raw(
            &rx_prod as *const u32,
            &mut rx_cons as *mut u32,
            rx_storage.as_ptr(),
            std::ptr::null(),
            ring_size,
        )
    };

    let mut tx_prod = 0u32;
    let tx_cons = 0u32;
    let mut tx_storage = vec![xdp_desc::default(); ring_size as usize];

    let tx_ring = unsafe {
        TxRing::from_raw(
            &mut tx_prod as *mut u32,
            &tx_cons as *const u32,
            tx_storage.as_mut_ptr(),
            std::ptr::null(),
            ring_size,
        )
    };

    let comp_prod = 0u32;
    let mut comp_cons = 0u32;
    let comp_storage = vec![0u64; ring_size as usize];

    let comp_ring = unsafe {
        CompletionRing::from_raw(
            &comp_prod as *const u32,
            &mut comp_cons as *mut u32,
            comp_storage.as_ptr(),
            std::ptr::null(),
            ring_size,
        )
    };

    let receiver = XdpReceiver::new(rx_ring, arena.clone());
    let sender = XdpSender::new(tx_ring, comp_ring, fill_ring);
    let stream = XdpStream::new(receiver, sender);

    // Split stream into independent read and write halves
    let (read_half, write_half) = stream.into_split();

    // Verify Read Half
    let mut recv_batch = Vec::new();
    let read_count = read_half
        .recv_batch(&mut recv_batch)
        .expect("Read batch ok");
    assert_eq!(read_count, 0);

    // Verify Write Half
    let lease1 = arena.lease().expect("Lease 1 ok");
    let lease2 = arena.lease().expect("Lease 2 ok");
    let sent = write_half.send_batch(&[lease1, lease2]);
    assert_eq!(sent, 2);
    assert_eq!(tx_prod, 2);

    // Verify Reclaim
    let reclaimed = write_half.reclaim_and_refill();
    assert_eq!(reclaimed, 0);
}
