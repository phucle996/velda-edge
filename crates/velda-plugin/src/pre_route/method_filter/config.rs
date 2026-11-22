//! Configuration Domain Models for `method_filter`.
//!
//! ```text
//!  +─────────────────────────────────────────────────────────────────────────+
//!  |                     METHOD_FILTER CONFIGURATION MODEL                   |
//!  +─────────────────────────────────────────────────────────────────────────+
//!  |                                                                         |
//!  |  MethodFilterConfig                                                     |
//!  |    └─ allowed_methods: Vec<String>  (Explicit RFC 9110 method allowlist)|
//!  |                                                                         |
//!  |  Compilation into MethodFilterPlugin:                                   |
//!  |    ├─ Standard Methods (GET..TRACE, 1..=9) ──► 16-bit bitmask (u16)     |
//!  |    └─ Custom / WebDAV Methods (Unknown, 0) ──► custom_methods Vec       |
//!  +─────────────────────────────────────────────────────────────────────────+
//! ```

use serde::{Deserialize, Serialize};

/// Declarative configuration for the `method_filter` pre-route plugin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MethodFilterConfig {
    /// Non-empty allowlist of permitted HTTP methods (e.g. `["GET", "POST", "OPTIONS"]`).
    pub allowed_methods: Vec<String>,
}
