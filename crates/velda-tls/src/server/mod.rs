//! Downstream TLS termination module (for Ingress / Composer).
//!
//! Provides stateless TLS termination functions and in-memory SNI resolving.

mod config;
mod engine;
mod handshake;
pub mod quic;
mod resolver;

pub use config::{
    MAX_SESSION_CACHE_CAPACITY, MIN_SESSION_CACHE_CAPACITY, ServerTlsConfig,
    optimal_session_cache_capacity, probed_session_cache_capacity,
};
pub use engine::{TlsServerEngine, accept};
pub use handshake::TlsHandshakeInfo;
pub use quic::build_quic_server_config;
pub use resolver::SniResolver;
