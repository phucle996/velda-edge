//! # Strongly-Typed Error Taxonomy for RDMA Subsystem
//!
//! ## Architectural Invariants & Failure Domains
//!
//! Kernel-bypass RDMA introduces failure modes that do not exist in conventional socket-based
//! network stacks: physical page locking failures (`ENOMEM` / `EPERM` from `mlock`), hardware state
//! machine rejections (`RESET -> INIT -> RTR -> RTS`), out-of-buffer Receiver Not Ready (RNR) drops,
//! and unannounced peer disconnection without TCP FIN packets.
//!
//! `RdmaError` partitions these failures into explicit, actionable error categories:
//! - **FFI & Driver Errors (`Library`)**: Dynamic library discovery failures for `libibverbs.so.1`
//!   or missing symbols.
//! - **Hardware Verbs Errors (`Verbs`)**: Low-level operational failures directly returned by the
//!   verbs provider driver, preserving the original OS `errno` for diagnostic triage.
//! - **Lifecycle & State Machine Errors (`StateTransition`)**: Detailed transition failures during
//!   Queue Pair modification.
//! - **Flow Control Failures (`RnrExhausted`, `InsufficientCredits`)**: Credit exhaustion or
//!   unresponsive receiver buffer allocation.
//! - **Stream Adaptation Errors (`BufferTooSmall`, `Timeout`, `Disconnected`)**: Failures surfaced
//!   during `AsyncRead` and `AsyncWrite` streaming bridging.

use thiserror::Error;

/// RDMA subsystem errors.
#[derive(Debug, Error)]
pub enum RdmaError {
    /// Standard I/O errors.
    #[error("RDMA I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Dynamic library loading / FFI resolution errors.
    #[error("RDMA library / FFI error: {0}")]
    Library(String),

    /// Verbs hardware operational error.
    #[error("Verbs hardware error: {0} (errno: {1})")]
    Verbs(String, i32),

    /// Requested RDMA device was not found.
    #[error("RDMA device '{0}' not found")]
    DeviceNotFound(String),

    /// Queue Pair state transition error.
    #[error("QP state transition failed from {from} to {to}: {reason}")]
    StateTransition {
        from: &'static str,
        to: &'static str,
        reason: String,
    },

    /// Receiver Not Ready (RNR) hardware retries exhausted.
    #[error("RDMA Receiver Not Ready (RNR) exhausted")]
    RnrExhausted,

    /// Insufficient flow control credits to post Send WR.
    #[error(
        "Insufficient RDMA flow control credits (available: {available}, required: {required})"
    )]
    InsufficientCredits { available: u32, required: u32 },

    /// RDMA slab arena exhausted for the requested slot class.
    #[error("RDMA slab arena exhausted for class {class:?}")]
    ArenaExhausted { class: &'static str },

    /// Operation timed out.
    #[error("RDMA operation timed out: {0}")]
    Timeout(String),

    /// Remote endpoint disconnected or closed connection.
    #[error("RDMA connection closed: {0}")]
    Disconnected(String),

    /// Provided buffer was too small to receive incoming payload.
    #[error("RDMA buffer too small (capacity: {capacity}, needed: {needed})")]
    BufferTooSmall { capacity: usize, needed: usize },

    /// Software mock simulation error.
    #[error("RDMA mock error: {0}")]
    Mock(String),
}
