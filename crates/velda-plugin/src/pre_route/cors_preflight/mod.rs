//! CORS Preflight Pre-Route Plugin Module.
//!
//! Intercepts HTTP `OPTIONS` preflight requests at the edge, returning an RFC-compliant
//! `204 No Content` response with pre-configured CORS permissions.

pub mod origin;
pub mod plugin;

pub use origin::OriginPattern;
pub use plugin::CorsPreflightPlugin;

pub mod validation;
pub use validation::*;
