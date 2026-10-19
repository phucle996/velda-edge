//! Post-Response Cross-Origin Resource Sharing (CORS) Submodule (`cors`).

pub mod plugin;

pub use plugin::{CorsConfig, CorsResponsePlugin};
