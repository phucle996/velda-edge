//! Crate `velda-ingress`
//!
//! Top Ingress Orchestration Engine for Velda Edge.
//!
//! Coordinates multi-port ingress traffic intake across OS kernel sockets (`velda-sock`)
//! and kernel-bypass drivers (`velda-xdp`), driving core-local worker runtimes and atomic
//! declarative reconciliation with strict LKG preservation.

pub mod binding;
pub mod engine;
pub mod error;
pub mod sock;
pub mod xdp;

// Backward-compatible module alias for callers expecting `velda_ingress::ingress::*`
pub mod ingress {
    pub use crate::binding::*;
    pub use crate::sock::*;
    pub use crate::xdp::*;
}

pub use binding::{IngressBinding, XdpConfig};
pub use engine::{
    EngineConfig, EngineHandle, IngressEngine, IngressHandle, ShardContext, TrafficEngine,
    run_ingress_shard,
};
pub use error::{IngressError, Result, TransportError};
pub use sock::{TcpBinding, TcpIngress, UdpBinding, UdpIngress};
pub use xdp::XdpIngress;

// Re-export core OS socket primitives and modules so downstream consumers remain ergonomic and unified
pub use velda_sock::{
    Connection, ConnectionReader, ConnectionWriter, Datagram, TcpListener, TcpListenerConfig,
    TransferStats, UdpSocket, UdpSocketConfig, connection, forward_bidirectional,
    forward_connection, next_connection_id, tcp, udp,
};
