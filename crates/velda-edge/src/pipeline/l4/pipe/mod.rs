//! # Layer 4 (L4) Raw Transport Pipe Strategies
//!
//! ## Architectural Invariants & Pipe Strategy Matrix
//!
//! In accordance with the Velda Edge Hourglass Architecture (`AGENTS.md` Sec 2.1–2.8):
//! - **Downstream Ingress Engines** (`tcp.rs`, `udp.rs`) own connection admission, resource limit
//!   enforcement, bogon filtering, and lock-free $O(1)$ route resolution.
//! - **Pipe Strategy Modules** (`pipe/*`) own the end-to-end wire forwarding workflow top-to-bottom
//!   without relying on cross-module parent glue.
//!
//! ### Strategy Matrix:
//! - **TCP Ingress**:
//!   - [`tcp_tcp`]: **Standard Pipe** routing downstream TCP streams through the Linux kernel socket
//!     stack to upstream TCP backends via zero-copy `splice` / user-space buffer fallback.
//!   - [`tcp_rdma`]: **Acceleration Pipe** bridging downstream TCP streams directly to hardware RDMA
//!     Queue Pairs (`velda-rdma`), bypassing the host kernel on the upstream transport leg.
//! - **UDP Ingress**:
//!   - [`udp_udp`]: **Standard Pipe** routing downstream UDP datagrams to upstream UDP backends with
//!     unidirectional fire-and-forget or bidirectional conntrack session tracking (`UdpSessionTable`).
//!   - [`udp_rdma`]: **Acceleration Pipe** bridging downstream UDP datagrams to hardware RDMA
//!     Queue Pairs, enabling kernel-bypass datagram transport across RoCEv2/InfiniBand fabrics.

pub mod tcp_rdma;
pub mod tcp_tcp;
pub mod udp_rdma;
pub mod udp_udp;
