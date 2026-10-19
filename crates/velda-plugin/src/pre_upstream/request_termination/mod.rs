//! Request Termination Pre-Upstream Plugin (`request_termination`).
//!
//! Immediately terminates incoming HTTP requests at the edge and returns a synthetic response
//! without contacting upstream backends.

pub mod plugin;

pub use plugin::{RequestTerminationConfig, RequestTerminationPlugin};
