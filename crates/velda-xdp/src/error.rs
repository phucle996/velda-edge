//! # Strongly-Typed Error Taxonomy for the XDP & eBPF Subsystem
//!
//! ## Failure Domains & Last Known Good (LKG) Rollback Triggers
//!
//! Unlike standard socket programming which typically produces `ECONNREFUSED` or `EAGAIN`,
//! kernel-bypass networking introduces failure domains tied to hardware ASIC limitations,
//! driver capabilities, and kernel locked memory limits (`RLIMIT_MEMLOCK`).
//!
//! ### Categorized Failure Boundaries:
//! 1. **Capability Qualification & Driver Incompatibility (`CapabilityMismatch`, `DriverNotSupported`)**:
//!    - Occurs when a listener requests capabilities exceeding host limits (e.g. `Drv` mode on loopback).
//!    - Triggers immediate error returns, enabling orchestrators to abort deployments and preserve LKG.
//! 2. **Privilege Violations (`PermissionDenied`)**:
//!    - Raised when the process lacks `CAP_NET_ADMIN` (interface attachment) or `CAP_BPF` (bytecode loading).
//! 3. **Queue & Memory Exhaustion (`RingFull`, `UmemAllocation`)**:
//!    - Occurs when incoming traffic rates exceed hardware queue or UMEM buffer capacity.
//! 4. **User-Space Protocol Violations (`StateTransition`, `ChecksumMismatch`, `PacketTruncated`)**:
//!    - Surfaced when userspace TCP/UDP state machines encounter corrupted checksums or invalid sequences.

use thiserror::Error;
use velda_core::{Error as CoreError, ErrorKind};

/// Strongly-typed error taxonomy for XDP and eBPF operations.
#[derive(Debug, Error)]
pub enum XdpError {
    /// Standard OS I/O failure.
    #[error("XDP I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Requested XDP mode does not match host kernel or driver capabilities.
    ///
    /// Triggers an immediate rollback to the running Last Known Good (LKG) deployment.
    #[error(
        "XDP capability mismatch on interface '{interface}': required {required}, actual {actual} ({reason})"
    )]
    CapabilityMismatch {
        interface: String,
        required: String,
        actual: String,
        reason: &'static str,
    },

    /// Missing required privileges (CAP_NET_ADMIN, CAP_BPF, or RLIMIT_MEMLOCK ceiling exceeded).
    #[error(
        "XDP permission denied for '{operation}': required capabilities '{required_caps}' ({reason})"
    )]
    PermissionDenied {
        operation: &'static str,
        required_caps: &'static str,
        reason: &'static str,
    },

    /// Target network interface does not exist on the host system.
    #[error("Network interface '{0}' not found")]
    InterfaceNotFound(String),

    /// Network driver lacks support for native driver or hardware offload modes.
    #[error(
        "Driver '{driver}' of interface '{interface}' does not support XDP mode '{mode}' ({reason})"
    )]
    DriverNotSupported {
        interface: String,
        driver: String,
        mode: &'static str,
        reason: &'static str,
    },

    /// Memory allocation or UMEM kernel registration failure.
    #[error("UMEM allocation or registration failed: {0}")]
    UmemAllocation(String),

    /// SPSC ring buffer capacity saturated.
    #[error("XDP ring buffer '{ring_name}' is full (capacity: {capacity})")]
    RingFull {
        ring_name: &'static str,
        capacity: u32,
    },

    /// SPSC ring buffer has no descriptors available.
    #[error("XDP ring buffer '{ring_name}' is empty")]
    RingEmpty { ring_name: &'static str },

    /// Failure executing `setsockopt` or `getsockopt` on an AF_XDP socket.
    #[error("XDP socket option '{option}' failed with errno {errno}: {details}")]
    SocketOption {
        option: &'static str,
        errno: i32,
        details: String,
    },

    /// eBPF bytecode loading or interface attachment failure.
    #[error("eBPF program '{program}' failed to attach to interface '{interface}': {reason}")]
    BpfProgram {
        program: &'static str,
        interface: String,
        reason: String,
    },

    /// State transition failure in the user-space RFC 793 TCP state machine.
    #[error("TCP state transition failed from {from} to {to}: {reason}")]
    StateTransition {
        from: &'static str,
        to: &'static str,
        reason: String,
    },

    /// Layer 3 or Layer 4 checksum verification failure.
    #[error("Checksum verification failed for protocol {protocol}")]
    ChecksumMismatch { protocol: &'static str },

    /// Packet frame shorter than protocol header minimum length.
    #[error(
        "Packet truncated: length {len} bytes is below minimum required {min_expected} bytes for {protocol}"
    )]
    PacketTruncated {
        protocol: &'static str,
        len: usize,
        min_expected: usize,
    },

    /// Connection reset by peer via TCP RST flag.
    #[error("Connection reset by peer: {0}")]
    ConnectionReset(String),

    /// XDP operation timed out.
    #[error("XDP operation '{0}' timed out")]
    Timeout(&'static str),
}

impl From<XdpError> for CoreError {
    fn from(err: XdpError) -> Self {
        match err {
            XdpError::Io(e) => CoreError::with_source(ErrorKind::Connection, "XDP I/O error", e),
            XdpError::CapabilityMismatch { reason, .. } => CoreError::new(
                ErrorKind::Connection,
                format!("XDP capability mismatch: {reason}"),
            ),
            XdpError::PermissionDenied {
                operation, reason, ..
            } => CoreError::new(
                ErrorKind::Internal,
                format!("XDP permission denied for '{operation}': {reason}"),
            ),
            other => CoreError::new(ErrorKind::Connection, other.to_string()),
        }
    }
}

/// Convenience result alias for XDP operations.
pub type Result<T> = std::result::Result<T, XdpError>;
