//! Pre-Route HTTP Method Filter Submodule (`method_filter`).
//!
//! ```text
//!  +─────────────────────────────────────────────────────────────────────────+
//!  |                    METHOD_FILTER MODULE BOUNDARIES                      |
//!  +─────────────────────────────────────────────────────────────────────────+
//!  |  config.rs     -> Domain model (MethodFilterConfig)                     |
//!  |  plugin.rs     -> MethodFilterPlugin (16-bit bitmask PreRoutePlugin)    |
//!  |  validation.rs -> RFC 9110 token validation & deduplication             |
//!  +─────────────────────────────────────────────────────────────────────────+
//! ```

pub mod config;
pub mod plugin;
pub mod validation;

pub use config::MethodFilterConfig;
pub use plugin::MethodFilterPlugin;
