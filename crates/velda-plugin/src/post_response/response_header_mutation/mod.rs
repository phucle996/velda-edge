//! Response Header Mutation Post-Response Plugin Module (`response_header_mutation`).
//!
//! Adds, mutates, appends, or strips HTTP response headers before downstream frame encoding:
//! - **Hardware Acceleration**: Single-instruction ALU bitmask execution and SIMD fingerprint probes.
//! - **$O(1)$ Status Code Resolution**: Compile-time `.rodata` indexing for all HTTP codes (100..=599).
//! - **High-Frequency Epoch Caching**: Thread-local Unix epoch caching minimizing string formatting.

pub mod config;
pub mod plugin;
pub mod simd;
pub mod var;

pub use config::{
    OP_DYNAMIC_ADD, OP_DYNAMIC_ANY, OP_DYNAMIC_SET, OP_DYNAMIC_SET_IF_MISSING, OP_REMOVE,
    OP_STATIC_ADD, OP_STATIC_SET, OP_STATIC_SET_IF_MISSING, ResponseHeaderMutationConfig,
};
pub use plugin::ResponseHeaderMutationPlugin;
pub use simd::{compute_header_fingerprint, fingerprint_matches_simd, validate_header_value_simd};
pub use var::{ResponseDynamicVar, ResponseHeaderValueSpec};
