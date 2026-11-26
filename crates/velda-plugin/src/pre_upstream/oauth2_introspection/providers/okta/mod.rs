//! Okta Introspection Provider (`providers::okta`).
//!
//! ```text
//!   providers::okta
//!        ├─ client.rs   ──► OktaEtp StreamLease HTTP/1.1 Keep-Alive POST
//!        └─ response.rs ──► RFC 7662 JSON parse, policy eval, direct cache commit
//! ```

pub mod client;
pub mod response;

pub use response::{execute, revalidate};
