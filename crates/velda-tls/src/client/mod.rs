//! Upstream TLS client connector module (for Upstream / Egress).
//!
//! Provides stateless TLS connection initiation and pre-compiled client connectors.

mod config;
mod engine;
pub mod quic;
pub mod verifier;

pub use config::ClientTlsConfig;
pub use engine::{TlsClientEngine, connect};
pub use quic::build_quic_client_config;
pub use verifier::{
    InsecureCertVerifier, build_insecure_client_config, build_insecure_client_config_with_versions,
    build_insecure_tls13_client_config,
};
