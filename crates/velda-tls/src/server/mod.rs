//! Downstream TLS termination module (for Ingress / Composer).
//!
//! Provides stateless TLS termination functions and in-memory SNI resolving.

mod config;
mod engine;
mod handshake;
pub mod quic;
mod resolver;

pub use config::{ServerTlsConfig, TlsServerParams};
pub use engine::{TlsServerEngine, accept};
pub use handshake::{
    TlsHandshakeInfo, is_alpn_compatible, is_protocol_alpn_compatible, normalize_alpn_bytes,
};
pub use quic::build_quic_server_config;
pub use resolver::{SniResolver, WILDCARD_PREFIX};
