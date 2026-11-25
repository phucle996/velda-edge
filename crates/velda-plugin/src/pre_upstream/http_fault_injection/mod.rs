//! HTTP Fault Injection Plugin (`http_fault_injection`).
//!
//! Provides Chaos Engineering and Resilience Testing specifically engineered
//! for HTTP traffic (HTTP/1.1, HTTP/2 REST/Web, and HTTP/3):
//! - `condition`: Disjoint interval IP CIDR matching, pre-hashed header matching, and override extraction.
//! - `plugin`: Unified `HttpDelayConfig`, `HttpAbortConfig`, single TLS XorShift64* PRNG, and `HttpFaultInjectionPlugin`.
//! - `validation`: Strict JSON schema and bounds validation.

pub mod condition;
pub mod plugin;
pub mod validation;

pub use condition::{HeaderMatchRule, HttpFaultCondition, IpRangeMatcher};
pub use plugin::{HttpAbortConfig, HttpDelayConfig, HttpFaultInjectionPlugin};
pub use validation::*;
