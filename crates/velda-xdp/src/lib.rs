//! # Velda XDP — Kernel-Bypass Ingress Transport Engine
//!
//! ## Architectural Foundations
//!
//! `velda-xdp` forms the top cone (pre-bound Ingress) of the Velda Edge Hourglass Architecture,
//! serving as the ingress counterpart to [`velda-rdma`](../velda-rdma) at the bottom cone:
//!
//! ### Kernel Bypass Mechanics
//! Standard Linux socket stacks (`epoll`, default `io_uring`) incur significant overhead per packet:
//! `sk_buff` allocation (>200B metadata), connection tracking (`conntrack`), firewall hooks (Netfilter),
//! and user/kernel context switches (`copy_to_user`).
//!
//! XDP intercepts raw Ethernet frames directly at the network interface card (NIC) driver ring buffer
//! prior to `sk_buff` allocation, bypassing the OS network stack.
//!
//! ### Operational Acceleration Modes
//! - **`PassFilter`**: Line-rate L4 packet filtering, rate limiting, and SYN flood mitigation directly
//!   at the NIC ring buffer. Clean traffic is passed (`XDP_PASS`) to the standard Linux stack.
//! - **`Sockmap`**: In-kernel stream redirection between downstream and upstream sockets via
//!   `BPF_MAP_TYPE_SOCKMAP` (`sockops` + `sk_msg`), bypassing socket read/write buffers.
//! - **`AfXdp` (Full Kernel Bypass)**: Incoming packets are DMA-transferred directly into pre-allocated
//!   user-space memory (UMEM) slabs. The data path from physical wire to L7 protocol parsers
//!   (HTTP/3, gRPC, TCP) operates with zero memory copies.
//!
//! ### Declarative Opt-in & LKG Resilience
//! XDP is enabled declaratively per listener. If the underlying host kernel, driver, or privileges
//! do not satisfy the requested mode, initialization fails immediately with typed errors to preserve
//! the running Last Known Good (LKG) deployment without silent performance degradation.

pub mod bpf;
pub mod channel;
pub mod config;
pub mod error;
pub mod ffi;
pub mod listener;
pub mod pool;
pub mod probe;
pub mod rings;
pub mod stack;

// Re-export core root types for the crate facade
pub use config::{XdpAttachMode, XdpConfig, XdpEngineKind};
pub use error::{Result, XdpError};
pub use listener::XdpListener;
pub use pool::{XdpSlabArena, XdpSlabLease};
