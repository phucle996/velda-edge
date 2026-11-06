//! Dedicated gRPC wire pipe modules over TCP (HTTP/2 binary framing).
//!
//! Submodules:
//! - Native TCP to TCP: `buffered_tcp_tcp`, `server_stream_tcp_tcp`, `client_stream_tcp_tcp`, `duplex_tcp_tcp`.
//! - Bridge TCP to UDP: `buffered_tcp_udp`, `server_stream_tcp_udp`, `client_stream_tcp_udp`, `duplex_tcp_udp`.
//! - Kernel-Bypass TCP to RDMA: `buffered_tcp_rdma`, `server_stream_tcp_rdma`, `client_stream_tcp_rdma`, `duplex_tcp_rdma`.

pub mod buffered_tcp_rdma;
pub mod buffered_tcp_tcp;
pub mod buffered_tcp_udp;
pub mod client_stream_tcp_rdma;
pub mod client_stream_tcp_tcp;
pub mod client_stream_tcp_udp;
pub mod duplex_tcp_rdma;
pub mod duplex_tcp_tcp;
pub mod duplex_tcp_udp;
pub mod server_stream_tcp_rdma;
pub mod server_stream_tcp_tcp;
pub mod server_stream_tcp_udp;
