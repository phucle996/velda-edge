//! Crate `velda-sock`
//!
//! Linux OS Kernel Socket Network Stack Mechanics for Velda Edge.
//!
//! Encapsulates standard Linux socket subsystem interactions (`sk_buff` path, `epoll`,
//! `SO_REUSEPORT`, `TCP_FASTOPEN`, `BBR`, `TCP_DEFER_ACCEPT`, `UDP_GRO`/`GSO`, and zero-copy `splice`),
//! leaving higher-level traffic scheduling and kernel-bypass dispatch to `velda-ingress`.

pub mod connection;
pub mod error;
pub mod tcp;
pub mod udp;

pub use connection::{Connection, ConnectionReader, ConnectionWriter, next_connection_id};
pub use error::{Result, SockError};
pub use tcp::{
    TcpListener, TcpListenerConfig, TransferStats, forward_bidirectional, forward_connection,
};
pub use udp::{Datagram, UdpSocket, UdpSocketConfig};
