//! Layer 7 gRPC over UDP pipeline strategies.
//!
//! ### Canonical Monorepo Vocabulary: Standard Pipe vs. Acceleration Pipe
//! - **Standard Bridge Pipe (`*_udp_tcp`)**: Downstream UDP datagrams bridged to upstream Linux kernel TCP stack.
//! - **Native Datagram Pipe (`*_udp_udp`)**: Native end-to-end UDP/QUIC datagram forwarding via LPM state machine.
//! - **Acceleration Bridge Pipe (`*_udp_rdma`)**: Downstream UDP datagrams bridged directly to upstream RDMA
//!   hardware Queue Pairs, completely bypassing the OS kernel network stack on the upstream path.
//!
//! Provides independent, self-contained pipeline strategies:
//! - 4 Native UDP -> UDP strategies (buffered, server_stream, client_stream, duplex)
//! - 4 Standard Bridge UDP -> TCP strategies (buffered, server_stream, client_stream, duplex)
//! - 4 Hardware Acceleration Bridge UDP -> RDMA strategies (buffered, server_stream, client_stream, duplex)

pub mod buffered_udp_rdma;
pub mod buffered_udp_tcp;
pub mod buffered_udp_udp;
pub mod client_stream_udp_rdma;
pub mod client_stream_udp_tcp;
pub mod client_stream_udp_udp;
pub mod duplex_udp_rdma;
pub mod duplex_udp_tcp;
pub mod duplex_udp_udp;
pub mod server_stream_udp_rdma;
pub mod server_stream_udp_tcp;
pub mod server_stream_udp_udp;
