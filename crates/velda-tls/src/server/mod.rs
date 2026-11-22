//! Downstream TLS termination module for traffic ingress.
//!
//! Provides stateless TLS termination functions and in-memory SNI resolving.

mod config;
mod engine;
mod handshake;
pub mod ja4;
pub mod quic;
mod resolver;
pub mod session;

pub use config::{ServerTlsConfig, TlsServerParams};
pub use engine::{TlsServerEngine, accept};
pub use handshake::{
    TlsHandshakeInfo, extract_alpn, extract_handshake_info, is_alpn_compatible,
    is_protocol_alpn_compatible, normalize_alpn_bytes,
};
pub use ja4::{compute_ja4_string, compute_ja4_u64};
pub use quic::build_quic_server_config;
pub use resolver::{SniConfigResolver, SniResolver, WILDCARD_PREFIX};
pub use session::ShardedServerSessionCache;
