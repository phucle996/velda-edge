//! Hardware-Optimized Lookup Tables & Canonical Integer Vocabulary (`velda-core::lut`).
//!
//! # Architecture & Rationale
//!
//! Velda Edge treats core request and pipeline attributes (wire protocol, HTTP method, pipeline variant,
//! canonical internal status, and User-Agent client classification) as
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
//! │ InternalStatus        │ 0..=40        │ 41             │   656 bytes   │ STATUS_SPECS   │
//! │ UaFamily              │ 0..=33        │ 34             │   544 bytes   │ UA_FAM_NAMES   │
//! │ UaOs                  │ 0..=7         │  8             │   128 bytes   │ UA_OS_NAMES    │
//! │ OctetTable (IPv4)     │ 0..=255       │ 256            │ 1,024 bytes   │ OCTET_TABLE    │
//! │ HexOctetTable (IPv6)  │ 0..=255       │ 256            │   512 bytes   │ HEX_OCTET_TABLE│
//! │ DecimalPairs (Itoa)   │ 00..=99       │ 100            │   200 bytes   │ DIGIT_PAIRS    │
//! ├───────────────────────┴───────────────┴────────────────┼───────────────┴────────────────┤
//! │ Total Memory Resident in CPU L1 Data Cache:            │ ~4,040 bytes (< 13% of 32KB L1)│
//! └────────────────────────────────────────────────────────┴────────────────────────────────┘
//! ```
//!
//! ### Key Invariants:
//! 1. **100% Dense (Contiguous 0..=N-1)**:
//!    Every enum has strictly contiguous discriminants. No sparse gaps or `255` sentinels,
//!    enabling direct table lookups via `NAMES[token as usize]` with zero conditional branching.
//! 2. **L1 Data Cache Resident**:
//!    The pointer codebook occupies ~2.3 KB of static memory, easily fitting into the 32 KB/48 KB
//!    L1 Data Cache of any modern CPU core with sub-1.5 ns access latency.
//! 3. **POD & Zero Heap Allocation**:
//!    All enums are `#[repr(u8)]`, `Copy`, `Eq`, `Hash`, zero-pointer, zero-allocation data structures.

pub mod decimal;
pub mod ip;
pub mod method;
pub mod pipe;
pub mod protocol;
pub mod state_machine;
pub mod user_agent;

pub use ip::{
    encode_ip_u128, encode_ipv4_u128, encode_upstream_ip_u32, fast_write_ip, format_ip_bytes,
    is_ipv4_mapped_u128, parse_ip_u128, parse_ipv4_bytes, parse_ipv6_u128,
};
pub use method::{HTTP_METHOD_NAMES, HttpMethod};
pub use pipe::{PIPE_NAMES, PipeKind};
pub use protocol::{WIRE_PROTOCOL_FAMILIES, WIRE_PROTOCOL_NAMES, WireProtocol};
pub use state_machine::{INTERNAL_STATUS_NAMES, InternalStatus, STATUS_SPECS, StatusSpec};
pub use user_agent::{UA_FAMILY_NAMES, UA_OS_NAMES, UaFamily, UaOs, classify_user_agent};

// Compile-time verification that all codebook discriminants fit in 1 byte
const _: () = assert!(std::mem::size_of::<PipeKind>() == 1);
const _: () = assert!(std::mem::size_of::<HttpMethod>() == 1);
const _: () = assert!(std::mem::size_of::<WireProtocol>() == 1);
const _: () = assert!(std::mem::size_of::<InternalStatus>() == 1);
const _: () = assert!(std::mem::size_of::<UaFamily>() == 1);
const _: () = assert!(std::mem::size_of::<UaOs>() == 1);

// Compile-time verification of total string table memory footprint
const TOTAL_LUT_BYTES: usize = (PipeKind::COUNT
    + HttpMethod::COUNT
    + WireProtocol::COUNT
    + InternalStatus::COUNT
    + UaFamily::COUNT
    + UaOs::COUNT)
    * std::mem::size_of::<&str>();

// Total pointer array size must remain under 4 KB (1 OS memory page / 1/8th of 32 KB L1D cache)
const _: () = assert!(TOTAL_LUT_BYTES < 4096);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_total_l1_footprint() {
        assert_eq!(TOTAL_LUT_BYTES, (64 + 10 + 7 + 41 + 34 + 8) * 16);
    }
}
