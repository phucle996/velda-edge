//! Upstream gRPC client modules over UDP.
//!
//! Submodules:
//! - `connector`: Upstream UDP client connector and Unary dispatcher.

pub mod connector;

pub use connector::GrpcUdpUpstreamConnector;
