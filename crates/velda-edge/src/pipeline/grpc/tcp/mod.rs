//! Layer 7 gRPC over TCP downstream stream worker and pipeline strategies.

pub mod downstream;
pub mod pipe;

pub use downstream::handle_grpc_tcp;
