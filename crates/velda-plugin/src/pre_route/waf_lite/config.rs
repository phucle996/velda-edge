//! Configuration Domain Models for `waf_lite`.
//!
//! ```text
//!  +─────────────────────────────────────────────────────────────────────────+
//!  |                        WAF_LITE CONFIGURATION MODEL                     |
//!  +─────────────────────────────────────────────────────────────────────────+
//!  |                                                                         |
//!  |  Hardcoded Engine Invariants:                                           |
//!  |    ├─ URI Inspection (Path + Query): Always Enabled                     |
//!  |    └─ Payload Inspection (Body):     Always Enabled (Capped at 32 KB)   |
//!  |                                                                         |
//!  |  User Config (WafLiteConfig):                                           |
//!  |    └─ patterns: Vec<String>  (Exact attack signatures to match)         |
//!  +─────────────────────────────────────────────────────────────────────────+
//! ```

use serde::{Deserialize, Serialize};

/// Fixed maximum number of leading payload bytes inspected per request (32 KB).
pub const MAX_BODY_SCAN_BYTES: usize = 32_768;

/// Deserializable configuration for the `waf_lite` pre-route plugin.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WafLiteConfig {
    #[serde(default)]
    pub patterns: Vec<String>,
}
