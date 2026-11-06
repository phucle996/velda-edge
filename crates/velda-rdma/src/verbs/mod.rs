//! Safe Rust wrappers around low-level RDMA Verbs primitives.

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
