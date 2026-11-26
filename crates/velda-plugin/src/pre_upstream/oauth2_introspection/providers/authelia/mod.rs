//! Authelia Introspection Provider (`providers::authelia`).
//!
//! ```text
//!   providers::authelia
//!        ├─ client.rs   ──► AutheliaEtp StreamLease HTTP/1.1 Keep-Alive POST
//!        └─ response.rs ──► RFC 7662 JSON parse, policy eval, direct cache commit
//! ```

pub mod client;
pub mod response;

pub use response::{execute, revalidate};
