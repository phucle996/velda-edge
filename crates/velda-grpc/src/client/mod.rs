//! Upstream gRPC client modules.
//!
//! Submodules:
//! - `connector`: Upstream HTTP/2 client connection management, stream opening, and Unary forwarding.

pub mod connector;

pub use connector::GrpcUpstreamConnector;
