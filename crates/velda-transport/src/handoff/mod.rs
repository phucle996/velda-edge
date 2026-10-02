//! Protocol handoff envelopes for L7 application layers (Composer, HTTP, gRPC).

pub mod l7;

pub use l7::{TcpL7Handoff, UdpL7Handoff};
