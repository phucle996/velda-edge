//! Layer 7 gRPC over UDP downstream datagram dispatch and pipeline strategies.

pub mod downstream;
pub mod engine;
pub mod pipe;

pub use downstream::handle_grpc_udp;
pub use engine::{clear_grpc_udp_engines, has_grpc_udp_engine, init_grpc_udp_engine};
