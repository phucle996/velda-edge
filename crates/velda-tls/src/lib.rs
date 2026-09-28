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

pub use client::{ClientTlsConfig, TlsClientEngine};
pub use engine::TlsEngine;
pub use error::TlsError;
pub use server::{
    MAX_SESSION_CACHE_CAPACITY, MIN_SESSION_CACHE_CAPACITY, ServerTlsConfig, SniResolver,
    TlsHandshakeInfo, TlsServerEngine, build_quic_server_config, optimal_session_cache_capacity,
    probed_session_cache_capacity,
};
