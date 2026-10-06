//! Layer 7 gRPC Upstream: isolated transport bindings for TCP (HTTP/2) and UDP (QUIC).
//!
//! Strictly separated by base transport:
//! - `tcp`: gRPC over TCP (`GrpcTcpUpstream`) via HTTP/2 binary framing and multiplexed pool.
//! - `udp`: gRPC over UDP (`GrpcUdpUpstream`) via QUIC datagrams and multiplexed client.

pub mod tcp;
pub mod udp;

pub use tcp::{GrpcClientResource, GrpcTcpClientResource, GrpcTcpUpstream, GrpcUpstream};
pub use udp::{GrpcUdpClientResource, GrpcUdpUpstream};
