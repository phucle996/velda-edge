//! Response Header Mutation Post-Response Plugin Module (`response_header_mutation`).
//!
//! Adds, mutates, appends, or strips HTTP response headers before downstream frame encoding:
//! - **Hardware Acceleration**: Single-instruction ALU bitmask execution (`op_mask: u8`) and SIMD fingerprint probes.
//! - **Deferred Arithmetic Variables**: `$request_id`, `$status`, and `$timestamp_epoch` write raw integers into `HeaderWorkspace`.

pub mod config;
pub mod fingerprint;
pub mod plugin;
pub mod var;

pub use config::{
    OP_DYNAMIC_ADD, OP_DYNAMIC_ANY, OP_DYNAMIC_SET, OP_DYNAMIC_SET_IF_MISSING, OP_REMOVE,
    OP_STATIC_ADD, OP_STATIC_SET, OP_STATIC_SET_IF_MISSING, ResponseHeaderMutationConfig,
};
pub use fingerprint::{
    compute_header_fingerprint, fingerprint_matches_simd, validate_header_value_simd,
};
pub use plugin::ResponseHeaderMutationPlugin;
pub use var::{ResponseDynamicVar, ResponseHeaderValueSpec};

pub mod validation;
