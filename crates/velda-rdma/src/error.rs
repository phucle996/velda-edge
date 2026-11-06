//! Error types for RDMA subsystem.

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
