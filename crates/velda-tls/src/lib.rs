//! Crate `velda-tls`
//!
//! Stateless, in-memory TLS Execution Engine for Velda Edge.
//!
//! Separates downstream (server termination) and upstream (client initiation)
//! into pure execution modules operating directly on RAM-allocated data.
//!
//! Zero IO on request serving hot path, zero global state.

pub mod client;
pub mod engine;
pub mod error;
pub mod pem;
pub mod server;
pub mod version;

pub use client::{
    ClientTlsConfig, InsecureCertVerifier, TlsClientEngine, build_insecure_client_config,
    build_insecure_client_config_with_versions, build_insecure_tls13_client_config,
    build_quic_client_config,
};
pub use engine::TlsEngine;
pub use error::TlsError;
pub use server::{
    ServerTlsConfig, SniConfigResolver, SniResolver, TlsHandshakeInfo, TlsServerEngine,
    TlsServerParams, WILDCARD_PREFIX, build_quic_server_config, extract_handshake_info,
    is_alpn_compatible, is_protocol_alpn_compatible, normalize_alpn_bytes,
};
pub use tokio_rustls::client::TlsStream;
pub use version::resolve_protocol_versions;
