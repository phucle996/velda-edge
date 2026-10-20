//! Pre-Route Plugin Configuration Schemas & Validation.
//!
//! Submodules for plugins executed during the `pre_route` phase:
//! - [`ip_filter`]: Client IP and CIDR subnet access control.
//! - [`cors_preflight`]: Low-latency OPTIONS preflight short-circuiting.
//! - [`resource_limit`]: URI length, header count, and header byte size boundary limits.
//! - [`waf_lite`]: High-speed signature and malicious scanner perimeter inspection.

pub mod cors_preflight;
pub mod ip_filter;
pub mod resource_limit;
pub mod waf_lite;

pub use cors_preflight::*;
pub use ip_filter::*;
pub use resource_limit::*;
pub use waf_lite::*;
