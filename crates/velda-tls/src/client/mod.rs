//! Upstream TLS client connector module (for Upstream / Egress).
//!
//! Provides stateless TLS connection initiation and pre-compiled client connectors.

mod config;
mod engine;

pub use config::ClientTlsConfig;
pub use engine::{TlsClientEngine, connect};
