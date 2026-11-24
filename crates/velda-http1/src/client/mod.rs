//! Upstream HTTP/1.1 Client Subsystem.
//!
//! Architectural Perspective (Góc nhìn Ingress):
//! Owns the upstream communication lifecycle (Gateway <-> Backend Microservice):
//! - `request`: Request entity model & wire serialization towards upstream targets.
//! - `response`: Response entity model & stream decoding from backend targets.
//! - `connector`: Upstream plain TCP and encrypted TLS stream handling and connection initiation.
//! - `header`: Outbound hop-by-hop header sanitization.

pub mod connector;
pub mod header;
pub mod request;
pub mod response;
