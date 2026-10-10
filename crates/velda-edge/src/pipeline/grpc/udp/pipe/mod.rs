//! Layer 7 gRPC over UDP pipeline strategies.
//!
//! Provides 8 independent, self-contained pipeline strategies:
//! - 4 Native UDP -> UDP strategies (buffered, server_stream, client_stream, duplex)
//! - 4 Cross-transport Bridge UDP -> TCP strategies (buffered, server_stream, client_stream, duplex)

pub mod buffered_udp_tcp;
pub mod buffered_udp_udp;
pub mod client_stream_udp_tcp;
pub mod client_stream_udp_udp;
pub mod duplex_udp_tcp;
pub mod duplex_udp_udp;
pub mod server_stream_udp_tcp;
pub mod server_stream_udp_udp;
