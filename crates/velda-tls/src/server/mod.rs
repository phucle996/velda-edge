//! Downstream TLS termination module for traffic ingress.
//!
//! Provides stateless TLS termination functions and in-memory SNI resolving.

mod config;
mod engine;
mod handshake;
pub mod quic;
mod resolver;
pub mod session;

pub use config::{ServerTlsConfig, TlsServerParams};
pub use engine::{TlsServerEngine, accept};
pub use handshake::{
    TlsHandshakeInfo, extract_handshake_info, is_alpn_compatible, is_protocol_alpn_compatible,
    normalize_alpn_bytes,
};
pub use quic::build_quic_server_config;
pub use resolver::{SniResolver, WILDCARD_PREFIX};
pub use session::ShardedServerSessionCache;
