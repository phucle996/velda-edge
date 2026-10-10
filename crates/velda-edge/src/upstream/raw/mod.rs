//! Layer 4 (Raw) Upstreams: pure TCP streams and UDP datagram endpoints.
//!
//! Subsystems are strictly partitioned:
//! - Upstream entities (`tcp.rs`, `udp.rs`) manage endpoint topology, health tracking, and load balancing (WHEN & WHO).
//! - Upstream connectors (`tcp_connector.rs`, `udp_connector.rs`) manage socket allocation, Linux setsockopt, and transport connection (HOW).

pub mod tcp;
pub mod tcp_connector;
pub mod udp;
pub mod udp_connector;

pub use tcp::TcpUpstream;
pub use tcp_connector::{TcpAccelerationPath, connect_tcp_stream, notsent_lowat_for_mem_tier};
pub use udp::UdpUpstream;
pub use udp_connector::{UdpAccelerationPath, buffer_sizes_for_mem_tier, connect_udp_socket};
