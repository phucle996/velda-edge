//! Request Termination Pre-Upstream Plugin (`request_termination`).
//!
//! Short-circuits matched HTTP routes with a pre-compiled synthetic [`velda_core::L7Response`].

pub mod config;
pub mod plugin;
pub mod validation;

pub use config::RequestTerminationConfig;
pub use plugin::RequestTerminationPlugin;
