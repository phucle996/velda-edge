//! Pre-Route Web Application Firewall Lite Submodule (`waf_lite`).
//!
//! ```text
//!  +─────────────────────────────────────────────────────────────────────────+
//!  |                      WAF_LITE MODULE BOUNDARIES                         |
//!  +─────────────────────────────────────────────────────────────────────────+
//!  |  config.rs     -> Domain types (WafLiteConfig, MAX_BODY_SCAN_BYTES)     |
//!  |  scanner.rs    -> Byte stream normalization & SIMD Aho-Corasick         |
//!  |  plugin.rs     -> WafLitePlugin implementing PreRoutePlugin             |
//!  |  validation.rs -> Declarative schema & pattern deduplication            |
//!  +─────────────────────────────────────────────────────────────────────────+
//! ```

pub mod config;
pub mod plugin;
pub mod scanner;
pub mod validation;

pub use config::{MAX_BODY_SCAN_BYTES, WafLiteConfig};
pub use plugin::WafLitePlugin;
pub use scanner::WafMatcher;
