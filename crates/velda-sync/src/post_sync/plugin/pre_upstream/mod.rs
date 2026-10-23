//! Pre-Upstream Plugin Configuration Schemas & Validation.
//!
//! Submodules for plugins executed during the `pre_upstream` phase:
//! - [`api_key_auth`]: API Key extraction, consumer identity, and in-memory dual caching.
//! - [`jwt_auth`]: JWT signature validation, claims assertion, and claim-to-header forwarding.
//! - [`rate_limit`]: Token bucket and sliding window rate limiting.
//! - [`canary`]: Traffic splitting, percentage rollouts, and rule-based traffic steering.
//! - [`request_mirror`]: Asynchronous shadow traffic mirroring.
//! - [`url_rewrite`]: Path and query string manipulation.
//! - [`http_fault_injection`]: HTTP chaos synthetic latency and abort injection.
//! - [`grpc_fault_injection`]: Dedicated gRPC synthetic latency and abort injection.
//! - [`request_termination`]: Synthetic custom HTTP response short-circuiting.
//! - [`header_mutation`]: Upstream request header manipulation and sanitization.

pub mod api_key_auth;
pub mod canary;
pub mod grpc_fault_injection;
pub mod header_mutation;
pub mod hmac_auth;
pub mod http_cache_lookup;
pub mod http_fault_injection;
pub mod jwt_auth;
pub mod oauth2_introspection;
pub mod rate_limit;
pub mod request_mirror;
pub mod request_termination;
pub mod url_rewrite;
pub mod w3c_trace_context;

pub use api_key_auth::*;
pub use canary::*;
pub use grpc_fault_injection::*;
pub use header_mutation::*;
pub use hmac_auth::*;
pub use http_cache_lookup::*;
pub use http_fault_injection::*;
pub use jwt_auth::*;
pub use oauth2_introspection::*;
pub use rate_limit::*;
pub use request_mirror::*;
pub use request_termination::*;
pub use url_rewrite::*;
pub use w3c_trace_context::*;
