//! CORS Preflight Pre-Route Plugin Module (`phase: "pre_route"`).
//!
//! Intercepts HTTP `OPTIONS` preflight requests at the edge, returning an RFC-compliant
//! `204 No Content` response with pre-configured CORS permissions.

pub mod origin;
pub mod plugin;

pub use origin::OriginPattern;
pub use plugin::CorsPreflightPlugin;
