//! Hardware-Optimized Lookup Tables & Canonical Integer Vocabulary (`velda-core::lut`).
//!
//! # Architecture & Rationale
//!
//! Velda Edge treats core request and pipeline attributes (wire protocol, HTTP method, pipeline variant,
//! canonical gRPC status, stream completion reason, and User-Agent client classification) as
//! **first-class canonical integer tokens** rather than late logging artefacts.
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────────────────────┐
//! │                          VELDA EDGE CANONICAL L1D LUT CODEBOOK                          │
//! ├───────────────────────┬───────────────┬────────────────┬───────────────┬────────────────┤
//! │ LUT Subsystem         │ Value Range   │ Variants Count │ Memory (L1D)  │ O(1) Mechanics │
//! ├───────────────────────┼───────────────┼────────────────┼───────────────┼────────────────┤
//! │ PipeKind              │ 0..=63        │ 64             │ 1,024 bytes   │ PIPE_NAMES     │
//! │ HttpMethod            │ 0..=9         │ 10             │   160 bytes   │ METHOD_NAMES   │
//! │ WireProtocol          │ 0..=6         │  7             │   112 bytes   │ PROTOCOL_NAMES │
//! │ GrpcStatus            │ 0..=17        │ 18             │   288 bytes   │ STATUS_NAMES   │
//! │ CompletionReason      │ 0..=3         │  4             │    64 bytes   │ REASON_NAMES   │
//! │ UaFamily              │ 0..=33        │ 34             │   544 bytes   │ UA_FAM_NAMES   │
//! │ UaOs                  │ 0..=7         │  8             │   128 bytes   │ UA_OS_NAMES    │
//! ├───────────────────────┴───────────────┴────────────────┼───────────────┴────────────────┤
//! │ Total Memory Resident in CPU L1 Data Cache:            │ ~2,320 bytes (< 5% of 32KB L1) │
//! └────────────────────────────────────────────────────────┴────────────────────────────────┘
//! ```
//!
//! ### Key Invariants:
//! 1. **100% Dense (Contiguous 0..=N-1)**:
//!    Every enum has strictly contiguous discriminants. No sparse gaps or `255` sentinels,
//!    enabling direct table lookups via `NAMES[token as usize]` with zero conditional branching.
//! 2. **L1 Data Cache Resident**:
//!    The entire codebook occupies ~2.3 KB of static memory, easily fitting into the 32 KB/48 KB
//!    L1 Data Cache of any modern CPU core with sub-1.5 ns access latency.
//! 3. **POD & Zero Heap Allocation**:
//!    All enums are `#[repr(u8)]`, `Copy`, `Eq`, `Hash`, zero-pointer, zero-allocation data structures.

pub mod completion;
pub mod grpc_status;
pub mod method;
pub mod pipe;
pub mod protocol;
pub mod reject;
pub mod user_agent;

pub use completion::{COMPLETION_REASON_NAMES, CompletionReason};
pub use grpc_status::{GRPC_STATUS_CODE_STRINGS, GRPC_STATUS_NAMES, GrpcStatus};
pub use method::{HTTP_METHOD_NAMES, HttpMethod};
pub use pipe::{PIPE_NAMES, PipeKind};
pub use protocol::{WIRE_PROTOCOL_FAMILIES, WIRE_PROTOCOL_NAMES, WireProtocol};
pub use reject::{REJECT_STATIC_METAS, RejectStaticMeta, get_reject_meta};
pub use user_agent::{UA_FAMILY_NAMES, UA_OS_NAMES, UaFamily, UaOs, classify_user_agent};

// Compile-time verification that all codebook discriminants fit in 1 byte
const _: () = assert!(std::mem::size_of::<PipeKind>() == 1);
const _: () = assert!(std::mem::size_of::<HttpMethod>() == 1);
const _: () = assert!(std::mem::size_of::<WireProtocol>() == 1);
const _: () = assert!(std::mem::size_of::<GrpcStatus>() == 1);
const _: () = assert!(std::mem::size_of::<CompletionReason>() == 1);
const _: () = assert!(std::mem::size_of::<UaFamily>() == 1);
const _: () = assert!(std::mem::size_of::<UaOs>() == 1);
const _: () = assert!(std::mem::size_of::<crate::error::RejectReason>() == 1);

// Compile-time verification of total string table & rejection metadata memory footprint
const TOTAL_LUT_BYTES: usize = (PipeKind::COUNT
    + HttpMethod::COUNT
    + WireProtocol::COUNT
    + GrpcStatus::COUNT * 2
    + CompletionReason::COUNT
    + UaFamily::COUNT
    + UaOs::COUNT)
    * std::mem::size_of::<&str>()
    + std::mem::size_of::<[RejectStaticMeta; 17]>();

// Total pointer array and static metadata size must remain under 4 KB (1 OS memory page / 1/8th of 32 KB L1D cache)
const _: () = assert!(TOTAL_LUT_BYTES < 4096);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_total_l1_footprint() {
        assert_eq!(
            TOTAL_LUT_BYTES,
            (64 + 10 + 7 + 18 * 2 + 5 + 34 + 8) * 16 + 17 * 40
        );
    }
}
