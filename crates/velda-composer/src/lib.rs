//! # velda-composer
//!
//! Protocol composition and runtime coordination layer for Velda Edge.
//!
//! Bridges `velda-transport` with application protocol engines (`velda-http`)
//! and TLS termination (`velda-tls`).
//!
//! ## Core Principle
//!
//! - `velda-transport`: moves the raw connection bytes (L4 accept / I/O).
//! - `velda-composer`: composes how the connection is processed (TLS? which protocol?).
//! - `velda-tls`: secures the stream and exposes TLS metadata (SNI, ALPN).
//! - `velda-http`: parses and serializes application messages.

pub mod composer;
pub mod config;
pub mod context;
pub mod error;

pub use composer::{ComposedDatagram, ComposedStream, Composer};
pub use config::{ApplicationProtocol, CompiledListenerComposition};
pub use context::{ComposerContext, TlsMetadata};
pub use error::ComposerError;
