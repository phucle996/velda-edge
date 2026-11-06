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
//!
//! ## Connection Identity Pre-Resolution & Zero-Copy Buffer Invariant (Bit 1 vs Bit 0)
//!
//! A fundamental architectural guarantee of the forwarding pipelines is that **no intermediate
//! `memcpy` ever occurs between Core Slabs and RDMA Slabs**:
//!
//! 1. **Temporal Separation (Handshake vs Forwarding)**:
//!    - Target physical endpoint resolution, health checks, and connection acquisition occur
//!      **before** reading downstream client payload.
//!    - The connection handle is retrieved by the frontline worker from the core's local lane
//!      shard, where it was pre-warmed by the companion shadow logistics worker and already
//!      resides warm in the physical CPU core's private L1/L2 data cache.
//!
//! 2. **Deterministic Buffer Selection via Immutable Connection Identity**:
//!    - **Bit 1 (RDMA Acceleration Pipe - `tcp_rdma` / `udp_rdma`)**:
//!      - The acquired connection is an active RoCEv2 Queue Pair (`RdmaLease`).
//!      - The pipeline leases contiguous spans directly from the pre-registered [`velda_rdma::RdmaSlabArena`].
//!      - Downstream bytes are read **directly from the client socket into the RDMA slab**
//!        (`client_reader.read(&mut slab)`), which is already memory-pinned (`mlock`) and registered
//!        with the hardware NIC via a single MTT `lkey`.
//!      - The NIC performs direct DMA transmission onto the wire. Core Slabs are never touched.
//!    - **Bit 0 (Standard Socket Pipe - `tcp_tcp` / `udp_udp`)**:
//!      - The acquired connection is a standard kernel socket (`TcpStream`).
//!      - The pipeline invokes Linux [`velda_transport::splice_bidirectional`], transferring byte pages
//!        directly between the client socket file descriptor and backend socket file descriptor inside
//!        the Linux kernel pipe buffer cache.
//!      - Data never touches user-space memory, and RDMA Slabs are never touched.
//!
//! Under no circumstances does the pipeline perform speculative double-buffering or cross-arena copies.

pub mod tcp_rdma;
pub mod tcp_tcp;
pub mod udp_rdma;
pub mod udp_udp;
