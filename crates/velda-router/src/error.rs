//! Error types for velda-router.

use thiserror::Error;

/// Errors produced during router compilation or route resolution.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum RouterError {
    /// Attempted to register a duplicate route identifier or listener conflict.
    #[error("duplicate route definition for listener '{listener}' and protocol '{protocol}'")]
    DuplicateRoute { listener: String, protocol: String },

    /// Route configuration was invalid or missing required parameters.
    #[error("invalid route configuration: {detail}")]
    InvalidRoute { detail: String },

    /// Request URI path was unsafe or invalid (e.g. null bytes, root escape).
    #[error("invalid URI path: {detail}")]
    InvalidPath { detail: String },
}
