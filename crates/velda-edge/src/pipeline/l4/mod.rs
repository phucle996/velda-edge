//! Layer 4 (L4) Raw Transport Pipelines.
//!
//! Provides raw stream (TCP) and datagram (UDP) forwarding between downstream
//! clients and backend targets with zero TLS termination or application parsing.

pub mod tcp;
pub mod udp;

pub use tcp::handle_l4_tcp;
pub use udp::{
    SessionAcquisition, UdpSessionKey, UdpSessionShard, UdpSessionTable, get_udp_session_table,
    handle_l4_udp,
};
