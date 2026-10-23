//! Pre-Upstream OAuth 2.0 Token Introspection Plugin Subsystem (`oauth2_introspection`).
//!
//! Conforms to RFC 7662 OAuth 2.0 Token Introspection specification.
//! Provides zero-IO in-memory 16-shard caching, persistent HTTP keep-alive connection pooling,
//! and flexible multi-source token extraction.

pub mod cache;
pub mod client;
pub mod plugin;
pub mod scope;

pub use cache::IntrospectionCache;
pub use client::IdpKeepAliveClient;
pub use plugin::Oauth2IntrospectionPlugin;
pub use velda_sync::{ClaimToHeaderConfig, Oauth2IntrospectionConfig};
