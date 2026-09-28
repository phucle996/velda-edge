//! Crate `velda-http`
//!
//! High-performance Layer 7 HTTP Protocol Engine for Velda Edge.
//!
//! Provides zero-copy parsing, serialization, and connection lifecycle
//! management across HTTP versions (HTTP/1.1, HTTP/2, HTTP/3).
//!
//! ## Core Design
//! - Input: Any `AsyncRead + AsyncWrite + Unpin` stream (cleartext TCP or TLS).
//! - Protocol hint: Concrete [`HttpVersion`] (resolved upstream by `velda-composer`).
//! - Output: Strongly-typed, flat [`velda_core::L7Request`] and [`velda_core::L7Response`].
//! - Zero external framework abstractions, zero unnecessary heap allocations.

pub mod error;
pub mod h1;
pub mod h2;
pub mod h3;
pub mod server;
pub mod version;

pub use error::HttpError;
pub use h1::{Http1Connection, decode_request, encode_response};
pub use h2::{Http2Connection, Http2Responder};
pub use h3::{Http3Engine, Http3Frame, decode_frame, encode_frame};
pub use server::{HttpConnection, bind_connection};
pub use version::HttpVersion;
