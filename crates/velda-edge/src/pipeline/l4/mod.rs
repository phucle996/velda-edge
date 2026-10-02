//! Layer 4 (L4) transport proxying pipelines.
//!
//! Direct byte and datagram forwarding without L7 parsing or TLS termination.

pub mod tcp;
pub mod udp;

pub use tcp::handle_l4_tcp;
pub use udp::{UdpSessionKey, UdpSessionTable, get_udp_session_table, handle_l4_udp};
