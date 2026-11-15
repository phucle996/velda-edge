//! JWKS Background Key Rotator Plugin (`phase: "background"`).

pub mod config;
pub mod fetcher;
pub mod parser;
pub mod plugin;

pub use config::JwksRotatorConfig;
pub use plugin::JwksRotatorPlugin;

pub mod validation;
