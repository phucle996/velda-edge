//! Post-Response Plugin Configuration Schemas & Validation.
//!
//! Submodules for plugins executed during the `post_response` phase:
//! - [`access_log`]: Non-blocking access logging and telemetry.
//! - [`cors`]: Cross-Origin Resource Sharing response headers injection.
//! - [`http_cache_store`]: HTTP response caching and storage.
//! - [`response_compression`]: Payload compression (gzip, brotli, zstd).
//! - [`response_header_mutation`]: Outbound response header mutation.
//! - [`security_headers`]: Defensive HTTP security headers (HSTS, CSP, etc.).

pub mod access_log;
pub mod cors;
pub mod http_cache_store;
pub mod response_compression;
pub mod response_header_mutation;
pub mod security_headers;

pub use access_log::*;
pub use cors::*;
pub use http_cache_store::*;
pub use response_compression::*;
pub use response_header_mutation::*;
pub use security_headers::*;
