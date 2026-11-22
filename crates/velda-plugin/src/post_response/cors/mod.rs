//! Post-Response Cross-Origin Resource Sharing (CORS) Submodule (`cors`).

pub mod origin;
pub mod plugin;
pub mod validation;

pub use origin::CorsOriginPattern;
pub use plugin::{CorsConfig, CorsResponsePlugin};
pub use validation::*;
