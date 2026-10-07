//! Upstream gRPC client modules over UDP / QUIC.
//!
//! Submodules:
//! - `connector`: Upstream QUIC client connector and multiplexed connection handle.
//! - `driver`: Background QUIC connection driver loop.

pub mod connector;
pub mod driver;
pub mod header;
pub mod request;
pub mod response;

pub use connector::{GrpcUdpClient, connect, default_client_config};
pub use header::{build_headers, sanitize_headers};
pub use request::{GrpcUdpClientRequest, GrpcUdpClientRequestHead};
pub use response::{GrpcUdpClientResponse, GrpcUdpClientResponseHead};
