//! Dedicated gRPC wire pipe modules over TCP (HTTP/2 binary framing).
//!
//! ### Canonical Monorepo Vocabulary: Standard Pipe vs. Acceleration Pipe
//! - **Standard Pipe (`*_tcp_tcp`)**: Outbound traffic is managed by the Linux kernel TCP stack,
//!   accelerated in kernel space via `TcpAccelerationPath` / `GrpcAccelerationPath` (`TCP_NODELAY`,
//!   `TCP_FASTOPEN_CONNECT`, `SO_BUSY_POLL`, `TCP_QUICKACK`, `BBR`).
//! - **Acceleration Pipe (`*_tcp_rdma`)**: Outbound traffic completely bypasses the OS kernel using
//!   `velda-rdma` hardware Verbs (RoCEv2 / InfiniBand). Memory is DMA-transferred directly between host
//!   user-space RAM and backend hardware with zero syscalls and sub-microsecond latency.
//! - **Bridge Pipe (`*_tcp_udp`)**: Cross-transport bridging between downstream HTTP/2 binary framing
//!   and upstream UDP/QUIC datagram length-prefixed message framing.
//!
//! Submodules:
//! - Standard Native TCP to TCP: `buffered_tcp_tcp`, `server_stream_tcp_tcp`, `client_stream_tcp_tcp`, `duplex_tcp_tcp`.
//! - Bridge TCP to UDP: `buffered_tcp_udp`, `server_stream_tcp_udp`, `client_stream_tcp_udp`, `duplex_tcp_udp`.
//! - Hardware Acceleration TCP to RDMA: `buffered_tcp_rdma`, `server_stream_tcp_rdma`, `client_stream_tcp_rdma`, `duplex_tcp_rdma`.

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
