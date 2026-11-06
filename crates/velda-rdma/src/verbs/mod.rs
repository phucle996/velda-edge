//! # Safe Rust Wrappers around Low-Level RDMA Verbs Primitives
//!
//! ## Architectural Invariants & Object Hierarchy
//!
//! The InfiniBand Architecture (IBA) and RoCE specifications define a strict resource ownership
//! hierarchy enforced by Host Channel Adapter (HCA) hardware:
//!
//! ```text
//!              +-------------------+
//!              |    RdmaDevice     | (Physical HCA device in sysfs)
//!              +---------+---------+
//!                        |
//!              +---------v---------+
//!              |    RdmaContext    | (Open device context: ibv_open_device)
//!              +----+---------+----+
//!                   |         |
//!    +--------------v--+   +--v---------------+
//!    | ProtectionDomain|   | CompletionQueue  | (Hardware completion ring)
//!    +-------+---------+   +--------+---------+
//!            |                      |
//!    +-------+---------+            |
//!    |  MemoryRegion   |            |
//!    +-------+---------+            |
//!            |                      |
//!            +----------+-----------+
//!                       |
//!              +--------v--------+
//!              |    QueuePair    | (Send Queue + Receive Queue: ibv_qp)
//!              +-----------------+
//! ```
//!
//! ### RAII Deallocation Invariant
//! Hardware Verbs objects must be destroyed in strict reverse order of creation:
//! `QP -> (MR, CQ) -> PD -> Context`. Violating this order results in `EBUSY` kernel errors
//! or driver panics. Safe Rust wrappers in this module leverage standard RAII `Drop` implementations
//! and `Arc` reference counting to ensure parent contexts remain valid throughout the lifetime of
//! dependent objects.

pub mod cq;
pub mod device;
pub mod mr;
pub mod pd;
pub mod qp;

pub use cq::CompletionQueue;
pub use device::{RdmaContext, RdmaDevice};
pub use mr::MemoryRegion;
pub use pd::ProtectionDomain;
pub use qp::{QpAddress, QueuePair};
