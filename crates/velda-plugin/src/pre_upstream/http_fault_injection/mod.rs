//! HTTP Fault Injection Plugin (`http_fault_injection`).
//!
//! Provides enterprise-grade Chaos Engineering and Microservice Resilience Testing
//! specifically engineered for HTTP traffic (HTTP/1.1, HTTP/2 REST/Web, and HTTP/3).
//!
//! # Architecture & Submodule Organization
//! - [`condition`]: Disjoint interval IP CIDR matching, header matching, and safe dynamic overrides.
//! - [`delay`]: Range `[min_ms, max_ms]` delay modeling with ~1ns thread-local PRNG.
//! - [`abort`]: Pre-allocated plain-text response builder and header injection.
//! - [`plugin`]: Lean sequential workflow owner (`HttpFaultInjectionPlugin`).

pub mod abort;
pub mod condition;
pub mod delay;
pub mod plugin;

pub use abort::HttpAbortConfig;
pub use condition::{HeaderMatchRule, HttpFaultCondition, IpRangeMatcher};
pub use delay::HttpDelayConfig;
pub use plugin::HttpFaultInjectionPlugin;
