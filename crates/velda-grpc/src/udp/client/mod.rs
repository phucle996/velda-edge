//! Upstream gRPC client modules over UDP / QUIC.
//!
//! Submodules:
//! - `connector`: Upstream QUIC client connector and multiplexed connection handle.
//! - `driver`: Background QUIC connection driver loop.

pub mod connector;
pub(crate) mod driver;

pub use connector::{GrpcUdpClient, connect, default_client_config};
