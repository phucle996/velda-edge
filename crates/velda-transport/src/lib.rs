//! Crate `velda-transport`
//!
//! Edge Traffic Engine for Velda Edge.
//!
//! Owns traffic ingress, connection lifecycle, TCP/UDP sockets,
//! path classification, L4 fast-path forwarding, and L7 protocol handoff.
//!
//! Architectural principles:
//! - Flat workflows: Top-to-bottom readable execution paths
//! - Zero HTTP semantics in L4 transport: Moves raw bytes
//! - In-memory hot paths: Zero file I/O or JSON parsing during traffic serving

pub mod connection;
pub mod engine;
pub mod error;
pub mod forwarding;
pub mod ingress;
pub mod tcp;
pub mod udp;

pub use connection::{Connection, ConnectionReader, ConnectionWriter, next_connection_id};
pub use engine::{EngineHandle, TrafficEngine};
pub use error::{Result, TransportError};
pub use forwarding::{
    L7Handoff, UdpL7Handoff, forward_tcp_direct, forward_tcp_stream, forward_udp_direct,
};
pub use ingress::{IngressBinding, IngressListener, PathKind, classify_bytes, peek_and_classify};
pub use tcp::{
    TcpListener, TcpListenerConfig, TransferStats, connect_and_forward, forward_bidirectional,
    forward_connection,
};
pub use udp::{Datagram, UdpSocket, UdpSocketConfig, forward_datagram, forward_udp_flow};
