//! Upstream gRPC client modules.
//!
//! Submodules:
//! - `connector`: Upstream HTTP/2 client connection management, stream opening, and Unary forwarding.

pub mod connector;
pub mod header;
pub mod request;
pub mod response;

pub use connector::{GrpcAccelerationPath, GrpcUpstreamConnector};
pub use header::{build_headers, sanitize_headers};
pub use request::{GrpcClientRequest, GrpcClientRequestHead};
pub use response::{GrpcClientResponse, GrpcClientResponseHead};
