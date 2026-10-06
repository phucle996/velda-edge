//! Layer 7 gRPC protocol pipeline: isolated transport bindings for TCP and UDP.
//!
//! gRPC operates as a first-class RPC protocol with two explicit transport bindings:
//! - `tcp`: gRPC over TCP (HTTP/2 binary framing, full bidirectional streaming).
//! - `udp`: gRPC over UDP (QUIC state machine, packet-driven RPC handoff).

pub mod tcp;
pub mod udp;

pub use tcp::handle_grpc_tcp;
pub use udp::{clear_grpc_udp_engines, handle_grpc_udp, has_grpc_udp_engine, init_grpc_udp_engine};
