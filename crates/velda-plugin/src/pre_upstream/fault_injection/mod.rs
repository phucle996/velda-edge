//! Protocol-Agnostic Fault Injection Plugin (`fault_injection`).
//!
//! ```text
//!   L7 Request (HTTP/1.1, HTTP/2, HTTP/3, gRPC)
//!     └─► FaultInjectionPlugin::on_upstream_request
//!           ├─► strip `x-fault-delay-ms` / `x-fault-abort-status`
//!           ├─► Delay + Action::Status(InternalStatus)  ─► Protocol crate renders HTTP/gRPC wire status
//!           └─► Action::Status(InternalStatus)          ─► Bypass header `x-velda-fault: abort` preserved
//! ```
//!
//! Provides unified latency injection and deterministic abort injection across all L7 protocols
//! without constructing protocol-specific `L7Response` payloads.

pub mod condition;
pub mod plugin;
pub mod validation;

pub use condition::{FaultCondition, FaultHeaderMatchRule, IpRangeMatcher};
pub use plugin::{FaultAbortConfig, FaultDelayConfig, FaultInjectionPlugin};
pub use validation::*;
